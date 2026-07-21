use crate::localization_io::{
    BlockingRequirements, BufferedBytes, CancellationGuard, LOCALIZATION_CHUNK_BYTES,
    LocalizationIoRuntime, cancellation_requested, cancelled_error,
};
use crate::process::{
    authenticate_client, authenticate_server, data_transport_budget, endpoint_id, receive_message,
    send_message, validate_secret,
};
use crate::{ResourceLocalizer, WorkerFuture};
use mutsuki_distributed_contracts::{
    DirectDataRef, DistributedError, DistributedErrorKind, NodeId, WorkerFailure,
};
use mutsuki_link::local::{LocalAddress, LocalConnection, LocalListener};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{OnceCell, mpsc, oneshot};

#[derive(Clone, Debug)]
pub struct FileContentSource {
    pub content_id: mutsuki_runtime_contracts::ContentId,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContentRequest {
    content_id: mutsuki_runtime_contracts::ContentId,
    #[serde(default)]
    offset: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContentManifestReply {
    result: Result<ContentManifest, WorkerFailure>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContentManifest {
    content_id: mutsuki_runtime_contracts::ContentId,
    chunk_bytes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContentTransferAck {
    content_id: mutsuki_runtime_contracts::ContentId,
}

pub struct FileContentServer {
    local_node: NodeId,
    worker_node: NodeId,
    address: String,
    secret: Arc<[u8]>,
    sources: BTreeMap<String, FileContentSource>,
    timeout: Duration,
    io: LocalizationIoRuntime,
}

impl FileContentServer {
    pub async fn open(
        local_node: NodeId,
        worker_node: NodeId,
        address: impl Into<String>,
        secret: Arc<[u8]>,
        sources: Vec<FileContentSource>,
        timeout: Duration,
        io: LocalizationIoRuntime,
    ) -> Result<Self, DistributedError> {
        validate_secret(&secret)?;
        if timeout.is_zero() {
            return Err(invalid_config("content server timeout must be positive"));
        }
        let mut indexed = BTreeMap::new();
        for source in sources {
            validate_sha256_content_id(&source.content_id)?;
            if source.content_id.size > io.budget().max_content_bytes {
                return Err(capacity_error());
            }
            let checked = source.clone();
            let cancelled = Arc::new(AtomicBool::new(false));
            io.run_blocking(
                BlockingRequirements::READ_HASH,
                source.content_id.size,
                cancelled,
                move || validate_content_file(&checked, DistributedErrorKind::InvalidConfig),
            )
            .await?;
            io.record_validation_read();
            if indexed
                .insert(source.content_id.digest.clone(), source)
                .is_some()
            {
                return Err(invalid_config("duplicate content digest"));
            }
        }
        if indexed.is_empty() {
            return Err(invalid_config("content server requires a source"));
        }
        Ok(Self {
            local_node,
            worker_node,
            address: address.into(),
            secret,
            sources: indexed,
            timeout,
            io,
        })
    }

    pub async fn serve_once(self) -> Result<(), DistributedError> {
        let (server, listener) = self.bind()?;
        server.serve_bound(listener).await
    }

    pub(crate) fn bind(self) -> Result<(Self, LocalListener), DistributedError> {
        let listener = LocalListener::bind(
            &LocalAddress(self.address.clone()),
            endpoint_id(&self.local_node),
            data_transport_budget(),
        )
        .map_err(|error| map_transport(&error))?;
        Ok((self, listener))
    }

    pub(crate) async fn serve_bound(self, listener: LocalListener) -> Result<(), DistributedError> {
        let mut connection = listener
            .accept(endpoint_id(&self.worker_node))
            .await
            .map_err(|error| map_transport(&error))?;
        authenticate_server(
            &mut connection,
            &self.local_node,
            &self.worker_node,
            &self.secret,
            self.timeout,
        )
        .await?;
        let request: ContentRequest =
            serde_json::from_slice(&receive_message(&mut connection, self.timeout).await?)
                .map_err(|_| protocol_error("content request is invalid"))?;
        let Some(source) = self.sources.get(&request.content_id.digest) else {
            let error = DistributedError::new(
                DistributedErrorKind::LocalizationFailed,
                "requested content is unavailable at the direct endpoint",
            );
            send_manifest(
                &mut connection,
                Err(WorkerFailure::from(&error)),
                self.timeout,
            )
            .await?;
            return Ok(());
        };
        if source.content_id != request.content_id {
            return Err(protocol_error(
                "content descriptor does not match direct source",
            ));
        }
        if request.offset > source.content_id.size {
            return Err(protocol_error("content request offset exceeds source size"));
        }
        send_manifest(
            &mut connection,
            Ok(ContentManifest {
                content_id: source.content_id.clone(),
                chunk_bytes: LOCALIZATION_CHUNK_BYTES,
            }),
            self.timeout,
        )
        .await?;

        let cancelled = Arc::new(AtomicBool::new(false));
        let guard = CancellationGuard::new(cancelled.clone());
        let (sender, mut receiver) = mpsc::channel(channel_capacity(&self.io));
        let path = source.path.clone();
        let offset = request.offset;
        let expected_bytes = source.content_id.size.saturating_sub(offset);
        let reader_io = self.io.clone();
        let reader_cancelled = cancelled.clone();
        let handle = tokio::runtime::Handle::current();
        let reader = self.io.run_blocking(
            BlockingRequirements::READ,
            expected_bytes,
            cancelled.clone(),
            move || {
                reader_io.record_source_read();
                let mut file = File::open(path).map_err(|_| storage_error())?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|_| storage_error())?;
                loop {
                    if cancellation_requested(&reader_io, &reader_cancelled) {
                        return Err(cancelled_error());
                    }
                    let mut chunk =
                        handle.block_on(reader_io.acquire_buffer(LOCALIZATION_CHUNK_BYTES))?;
                    chunk.bytes_mut().resize(LOCALIZATION_CHUNK_BYTES, 0);
                    let read = file.read(chunk.bytes_mut()).map_err(|_| storage_error())?;
                    if read == 0 {
                        break;
                    }
                    chunk.bytes_mut().truncate(read);
                    sender.blocking_send(chunk).map_err(|_| cancelled_error())?;
                }
                Ok(())
            },
        );
        let sender_cancelled = cancelled.clone();
        let network = async {
            while let Some(chunk) = receiver.recv().await {
                self.io.shape_network().await;
                if let Err(error) =
                    send_message(&mut connection, chunk.bytes(), false, self.timeout).await
                {
                    sender_cancelled.store(true, Ordering::Release);
                    return Err(error);
                }
            }
            Ok(())
        };
        let (reader_result, network_result) = tokio::join!(reader, network);
        reader_result?;
        network_result?;
        guard.complete();

        let ack: ContentTransferAck =
            serde_json::from_slice(&receive_message(&mut connection, self.timeout).await?)
                .map_err(|_| protocol_error("content transfer acknowledgement is invalid"))?;
        if ack.content_id != source.content_id {
            return Err(protocol_error(
                "content transfer acknowledgement does not match the source",
            ));
        }
        Ok(())
    }
}

async fn send_manifest(
    connection: &mut LocalConnection,
    result: Result<ContentManifest, WorkerFailure>,
    timeout: Duration,
) -> Result<(), DistributedError> {
    let reply = ContentManifestReply { result };
    send_message(
        connection,
        &serde_json::to_vec(&reply)
            .map_err(|_| protocol_error("content manifest encode failed"))?,
        true,
        timeout,
    )
    .await
}

struct SharedLocalization {
    result: OnceCell<Result<(), DistributedError>>,
}

impl SharedLocalization {
    fn new() -> Self {
        Self {
            result: OnceCell::new(),
        }
    }
}

pub struct LinkResourceLocalizer {
    worker_node: NodeId,
    secret: Arc<[u8]>,
    destination: PathBuf,
    timeout: Duration,
    io: LocalizationIoRuntime,
    in_flight: Mutex<BTreeMap<String, Weak<SharedLocalization>>>,
}

impl LinkResourceLocalizer {
    pub async fn open(
        worker_node: NodeId,
        secret: Arc<[u8]>,
        destination: impl Into<PathBuf>,
        timeout: Duration,
        io: LocalizationIoRuntime,
    ) -> Result<Self, DistributedError> {
        validate_secret(&secret)?;
        if timeout.is_zero() {
            return Err(invalid_config(
                "content localization timeout must be positive",
            ));
        }
        let destination = destination.into();
        let create = destination.clone();
        io.run_blocking(
            BlockingRequirements::WRITE,
            0,
            Arc::new(AtomicBool::new(false)),
            move || fs::create_dir_all(create).map_err(|_| storage_error()),
        )
        .await?;
        Ok(Self {
            worker_node,
            secret,
            destination,
            timeout,
            io,
            in_flight: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn io_metrics(&self) -> crate::LocalizationIoMetricsSnapshot {
        self.io.metrics()
    }

    pub async fn shutdown(&self) -> Result<(), DistributedError> {
        self.io.shutdown();
        let deadline = Instant::now() + self.timeout;
        loop {
            let metrics = self.io.metrics();
            if metrics.queued_jobs == 0
                && metrics.active_reads == 0
                && metrics.active_writes == 0
                && metrics.active_hash_jobs == 0
                && metrics.buffered_bytes == 0
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(cancelled_error());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn localize_one(&self, resource: &DirectDataRef) -> Result<(), DistributedError> {
        validate_sha256_content_id(&resource.content_id)?;
        if resource.content_id.size > self.io.budget().max_content_bytes {
            return Err(capacity_error());
        }
        let key = format!(
            "{}:{}:{}:{}",
            resource.content_id.algorithm,
            resource.content_id.digest,
            resource.content_id.size,
            resource.content_id.format
        );
        let shared = {
            let mut in_flight = self.in_flight.lock().expect("localization in-flight mutex");
            in_flight.retain(|_, value| value.strong_count() > 0);
            if let Some(existing) = in_flight.get(&key).and_then(Weak::upgrade) {
                existing
            } else {
                let shared = Arc::new(SharedLocalization::new());
                in_flight.insert(key, Arc::downgrade(&shared));
                shared
            }
        };
        shared
            .result
            .get_or_init(|| self.localize_one_inner(resource))
            .await
            .clone()
    }

    async fn localize_one_inner(&self, resource: &DirectDataRef) -> Result<(), DistributedError> {
        let cancelled = Arc::new(AtomicBool::new(false));
        let guard = CancellationGuard::new(cancelled.clone());
        let final_path = self.destination.join(&resource.content_id.digest);
        if self
            .validate_existing(resource, final_path.clone(), cancelled.clone())
            .await?
        {
            guard.complete();
            return Ok(());
        }
        let temporary = final_path.with_extension("partial");
        let prepared = self
            .prepare_partial(resource, temporary.clone(), cancelled.clone())
            .await?;
        let resume_offset = prepared.resume_offset;
        let address = resource
            .endpoint_hint
            .strip_prefix("link-local://")
            .filter(|address| !address.is_empty())
            .ok_or_else(|| {
                DistributedError::new(
                    DistributedErrorKind::LocalizationFailed,
                    "direct resource endpoint is not a Link local stream",
                )
            })?;
        let (sender, receiver) = mpsc::channel(channel_capacity(&self.io));
        let (ready_sender, ready_receiver) = oneshot::channel();
        let writer_io = self.io.clone();
        let writer_cancelled = cancelled.clone();
        let expected = resource.content_id.clone();
        let writer_temporary = temporary.clone();
        let writer_final = final_path.clone();
        let writer = self.io.run_blocking(
            BlockingRequirements::WRITE_HASH,
            resource.content_id.size.saturating_sub(resume_offset),
            cancelled.clone(),
            move || {
                ready_sender.send(()).map_err(|()| cancelled_error())?;
                write_and_commit(
                    prepared,
                    receiver,
                    &writer_io,
                    &writer_cancelled,
                    &expected,
                    &writer_temporary,
                    &writer_final,
                )
            },
        );
        let receiver_cancelled = cancelled.clone();
        let receiver_io = self.io.clone();
        let receive = async {
            let result = async {
                ready_receiver.await.map_err(|_| cancelled_error())?;
                let mut connection = connect_resource(
                    address,
                    &self.worker_node,
                    &resource.owner_node,
                    &self.secret,
                    self.timeout,
                )
                .await?;
                let request = ContentRequest {
                    content_id: resource.content_id.clone(),
                    offset: resume_offset,
                };
                send_message(
                    &mut connection,
                    &serde_json::to_vec(&request)
                        .map_err(|_| protocol_error("content request encode failed"))?,
                    true,
                    self.timeout,
                )
                .await?;
                let reply: ContentManifestReply =
                    serde_json::from_slice(&receive_message(&mut connection, self.timeout).await?)
                        .map_err(|_| protocol_error("content manifest is invalid"))?;
                let manifest = reply.result.map_err(|failure| {
                    DistributedError::new(
                        failure.kind,
                        "direct content endpoint rejected the request",
                    )
                })?;
                if manifest.content_id != resource.content_id
                    || manifest.chunk_bytes == 0
                    || manifest.chunk_bytes > LOCALIZATION_CHUNK_BYTES
                {
                    return Err(protocol_error("direct content manifest is incompatible"));
                }
                self.io.record_download();
                let mut remaining = resource.content_id.size.saturating_sub(resume_offset);
                while remaining > 0 {
                    let chunk = receive_message(&mut connection, self.timeout).await?;
                    if chunk.is_empty()
                        || chunk.len() > manifest.chunk_bytes
                        || u64::try_from(chunk.len()).unwrap_or(u64::MAX) > remaining
                    {
                        return Err(protocol_error("direct content chunk violates the manifest"));
                    }
                    remaining -= u64::try_from(chunk.len()).expect("chunk fits u64");
                    let mut buffered = receiver_io.acquire_buffer(chunk.len()).await?;
                    buffered.replace(chunk);
                    sender.send(buffered).await.map_err(|_| cancelled_error())?;
                }
                drop(sender);
                Ok(connection)
            }
            .await;
            if result.is_err() {
                receiver_cancelled.store(true, Ordering::Release);
            }
            result
        };
        let (writer_result, receive_result) = tokio::join!(writer, receive);
        if let Err(error) = &receive_result
            && error.kind == DistributedErrorKind::Protocol
        {
            self.remove_partial(temporary, Arc::new(AtomicBool::new(false)))
                .await?;
            return Err(error.clone());
        }
        writer_result?;
        let mut connection = receive_result?;
        guard.complete();
        send_message(
            &mut connection,
            &serde_json::to_vec(&ContentTransferAck {
                content_id: resource.content_id.clone(),
            })
            .map_err(|_| protocol_error("content transfer acknowledgement encode failed"))?,
            true,
            self.timeout,
        )
        .await?;
        match receive_message(&mut connection, self.timeout).await {
            Err(error) if error.kind == DistributedErrorKind::TransportClosed => Ok(()),
            Ok(_) => Err(protocol_error(
                "content endpoint sent data after transfer completion",
            )),
            Err(error) => Err(error),
        }
    }

    async fn validate_existing(
        &self,
        resource: &DirectDataRef,
        path: PathBuf,
        cancelled: Arc<AtomicBool>,
    ) -> Result<bool, DistributedError> {
        let source = FileContentSource {
            content_id: resource.content_id.clone(),
            path,
        };
        let io = self.io.clone();
        let exists = self
            .io
            .run_blocking(
                BlockingRequirements::READ_HASH,
                resource.content_id.size,
                cancelled,
                move || match fs::metadata(&source.path) {
                    Ok(_) => {
                        io.record_validation_read();
                        validate_content_file(&source, DistributedErrorKind::Corrupt)?;
                        Ok(true)
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                    Err(_) => Err(storage_error()),
                },
            )
            .await?;
        Ok(exists)
    }

    async fn prepare_partial(
        &self,
        resource: &DirectDataRef,
        path: PathBuf,
        cancelled: Arc<AtomicBool>,
    ) -> Result<PreparedPartial, DistributedError> {
        let size = resource.content_id.size;
        let io = self.io.clone();
        self.io
            .run_blocking(
                BlockingRequirements::READ_WRITE_HASH,
                size,
                cancelled,
                move || {
                    let resume_offset = match fs::metadata(&path) {
                        Ok(metadata) if metadata.len() <= size => metadata.len(),
                        Ok(_) => {
                            fs::remove_file(&path).map_err(|_| storage_error())?;
                            0
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                        Err(_) => return Err(storage_error()),
                    };
                    let mut hasher = Sha256::new();
                    if resume_offset > 0 {
                        io.record_validation_read();
                        let mut input = File::open(&path).map_err(|_| storage_error())?;
                        let mut buffer = vec![0; LOCALIZATION_CHUNK_BYTES];
                        loop {
                            let read = input.read(&mut buffer).map_err(|_| storage_error())?;
                            if read == 0 {
                                break;
                            }
                            hasher.update(&buffer[..read]);
                        }
                    }
                    let output = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&path)
                        .map_err(|_| storage_error())?;
                    Ok(PreparedPartial {
                        output,
                        hasher,
                        resume_offset,
                    })
                },
            )
            .await
    }

    async fn remove_partial(
        &self,
        path: PathBuf,
        cancelled: Arc<AtomicBool>,
    ) -> Result<(), DistributedError> {
        self.io
            .run_blocking(
                BlockingRequirements::WRITE,
                0,
                cancelled,
                move || match fs::remove_file(path) {
                    Ok(()) => Ok(()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(_) => Err(storage_error()),
                },
            )
            .await
    }

    pub fn content_path(&self, content_id: &mutsuki_runtime_contracts::ContentId) -> PathBuf {
        self.destination.join(&content_id.digest)
    }
}

impl ResourceLocalizer for LinkResourceLocalizer {
    fn localize<'a>(&'a self, resources: &'a [DirectDataRef]) -> WorkerFuture<'a, ()> {
        Box::pin(async move {
            for resource in resources {
                self.localize_one(resource).await?;
            }
            Ok(())
        })
    }

    fn shutdown(&self) -> WorkerFuture<'_, ()> {
        Box::pin(LinkResourceLocalizer::shutdown(self))
    }
}

struct PreparedPartial {
    output: File,
    hasher: Sha256,
    resume_offset: u64,
}

fn write_and_commit(
    mut prepared: PreparedPartial,
    mut receiver: mpsc::Receiver<BufferedBytes>,
    io: &LocalizationIoRuntime,
    cancelled: &AtomicBool,
    expected: &mutsuki_runtime_contracts::ContentId,
    temporary: &Path,
    final_path: &Path,
) -> Result<(), DistributedError> {
    let mut written = prepared.resume_offset;
    while let Some(chunk) = receiver.blocking_recv() {
        if cancellation_requested(io, cancelled) {
            return Err(cancelled_error());
        }
        prepared
            .output
            .write_all(chunk.bytes())
            .map_err(|_| storage_error())?;
        prepared.hasher.update(chunk.bytes());
        written = written
            .checked_add(u64::try_from(chunk.bytes().len()).expect("chunk fits u64"))
            .ok_or_else(|| protocol_error("localized content size overflowed"))?;
    }
    if cancellation_requested(io, cancelled) {
        return Err(cancelled_error());
    }
    if written != expected.size {
        return Err(DistributedError::new(
            DistributedErrorKind::TransportClosed,
            "direct content transfer ended before the declared size",
        ));
    }
    prepared.output.sync_all().map_err(|_| storage_error())?;
    let digest = format!("{:x}", prepared.hasher.finalize());
    drop(prepared.output);
    if digest != expected.digest {
        let _ = fs::remove_file(temporary);
        return Err(DistributedError::new(
            DistributedErrorKind::Corrupt,
            "direct content digest verification failed",
        ));
    }
    fs::rename(temporary, final_path).map_err(|_| storage_error())
}

fn validate_content_file(
    source: &FileContentSource,
    mismatch_kind: DistributedErrorKind,
) -> Result<(), DistributedError> {
    let metadata = fs::metadata(&source.path).map_err(|_| storage_error())?;
    if !metadata.is_file() || metadata.len() != source.content_id.size {
        return Err(DistributedError::new(
            mismatch_kind,
            "direct content file size does not match its descriptor",
        ));
    }
    let mut file = File::open(&source.path).map_err(|_| storage_error())?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; LOCALIZATION_CHUNK_BYTES];
    loop {
        let read = file.read(&mut buffer).map_err(|_| storage_error())?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    if format!("{:x}", hash.finalize()) != source.content_id.digest {
        return Err(DistributedError::new(
            mismatch_kind,
            "direct content file digest does not match its descriptor",
        ));
    }
    Ok(())
}

fn validate_sha256_content_id(
    content_id: &mutsuki_runtime_contracts::ContentId,
) -> Result<(), DistributedError> {
    if content_id.algorithm != "sha256"
        || content_id.digest.len() != 64
        || !content_id
            .digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid_config(
            "direct content requires a canonical sha256 digest",
        ));
    }
    Ok(())
}

async fn connect_resource(
    address: &str,
    worker_node: &NodeId,
    owner_node: &NodeId,
    secret: &[u8],
    timeout: Duration,
) -> Result<LocalConnection, DistributedError> {
    let deadline = Instant::now() + timeout;
    let mut connection = loop {
        let context = mutsuki_link::ConnectContext {
            deadline: Some(deadline),
            ..mutsuki_link::ConnectContext::default()
        };
        match mutsuki_link::local::connect(
            &LocalAddress(address.into()),
            endpoint_id(worker_node),
            endpoint_id(owner_node),
            data_transport_budget(),
            &context,
        )
        .await
        {
            Ok(connection) => break connection,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(map_transport(&error)),
        }
    };
    authenticate_client(&mut connection, worker_node, owner_node, secret, timeout).await?;
    Ok(connection)
}

fn channel_capacity(io: &LocalizationIoRuntime) -> usize {
    (io.budget().max_buffered_bytes / LOCALIZATION_CHUNK_BYTES).clamp(1, 16)
}

fn map_transport(error: &mutsuki_link::TransportError) -> DistributedError {
    let kind = match error.kind {
        mutsuki_link::TransportErrorKind::TimedOut => DistributedErrorKind::WorkerUnavailable,
        _ => DistributedErrorKind::TransportClosed,
    };
    DistributedError::new(kind, "authenticated Link transport failed")
}

fn storage_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Storage,
        "direct content storage operation failed",
    )
}

fn capacity_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::CapacityExceeded,
        "direct content exceeds the localization I/O budget",
    )
}

fn invalid_config(message: &'static str) -> DistributedError {
    DistributedError::new(DistributedErrorKind::InvalidConfig, message)
}

fn protocol_error(message: &'static str) -> DistributedError {
    DistributedError::new(DistributedErrorKind::Protocol, message)
}

#[cfg(all(test, feature = "localization-testkit"))]
mod tests {
    use super::*;
    use crate::{LocalizationIoBudget, LocalizationIoInjectedFault};
    use mutsuki_runtime_contracts::ContentId;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn runtime(max_content_bytes: u64) -> LocalizationIoRuntime {
        LocalizationIoRuntime::new(LocalizationIoBudget {
            max_active_reads: 2,
            max_active_writes: 2,
            max_active_hash_jobs: 2,
            max_queued_jobs: 8,
            max_buffered_bytes: 1024 * 1024,
            max_content_bytes,
        })
        .unwrap()
    }

    fn identity(bytes: &[u8]) -> ContentId {
        ContentId::new(
            "sha256",
            format!("{:x}", Sha256::digest(bytes)),
            u64::try_from(bytes.len()).unwrap(),
            "blob",
        )
    }

    fn unique(label: &str) -> String {
        format!(
            "issue24-{label}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }

    async fn localizer(
        worker: &NodeId,
        secret: Arc<[u8]>,
        destination: &Path,
        io: LocalizationIoRuntime,
    ) -> LinkResourceLocalizer {
        LinkResourceLocalizer::open(
            worker.clone(),
            secret,
            destination,
            Duration::from_millis(100),
            io,
        )
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn storage_faults_and_blocking_panic_preserve_partial_without_publishing() {
        let bytes = vec![7_u8; 512 * 1024];
        let content_id = identity(&bytes);
        let worker = NodeId(unique("worker"));
        let owner = NodeId(unique("owner"));
        let secret: Arc<[u8]> = Arc::from(unique("secret-at-least-thirty-two-bytes").into_bytes());

        for fault in [
            LocalizationIoInjectedFault::DiskFull,
            LocalizationIoInjectedFault::PermissionDenied,
        ] {
            let destination = tempfile::tempdir().unwrap();
            let io = runtime(content_id.size);
            let localizer =
                localizer(&worker, secret.clone(), destination.path(), io.clone()).await;
            let partial = localizer
                .content_path(&content_id)
                .with_extension("partial");
            fs::write(&partial, &bytes[..LOCALIZATION_CHUNK_BYTES]).unwrap();
            io.testkit().inject_next_write_fault(fault);
            let resource = DirectDataRef {
                owner_node: owner.clone(),
                content_id: content_id.clone(),
                endpoint_hint: format!("link-local://{}", unique("offline")),
            };
            let error = localizer.localize(&[resource]).await.unwrap_err();
            assert_eq!(error.kind, DistributedErrorKind::Storage);
            assert_eq!(
                fs::read(&partial).unwrap(),
                &bytes[..LOCALIZATION_CHUNK_BYTES]
            );
            assert!(!localizer.content_path(&content_id).exists());
        }

        let destination = tempfile::tempdir().unwrap();
        let io = runtime(content_id.size);
        let localizer = localizer(&worker, secret, destination.path(), io.clone()).await;
        let partial = localizer
            .content_path(&content_id)
            .with_extension("partial");
        fs::write(&partial, &bytes[..LOCALIZATION_CHUNK_BYTES]).unwrap();
        io.testkit().panic_next_blocking_job();
        let resource = DirectDataRef {
            owner_node: owner,
            content_id: content_id.clone(),
            endpoint_hint: format!("link-local://{}", unique("offline-panic")),
        };
        let error = localizer.localize(&[resource]).await.unwrap_err();
        assert_eq!(error.kind, DistributedErrorKind::LocalizationFailed);
        assert!(!localizer.content_path(&content_id).exists());
        assert_eq!(io.metrics().panicked_jobs, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timeout_and_cancellation_preserve_legal_partial_and_shutdown_joins_workers() {
        let bytes = vec![17_u8; 2 * 1024 * 1024];
        let content_id = identity(&bytes);
        let worker = NodeId(unique("worker"));
        let owner = NodeId(unique("owner"));
        let secret: Arc<[u8]> = Arc::from(unique("secret-at-least-thirty-two-bytes").into_bytes());
        let destination = tempfile::tempdir().unwrap();
        let timeout_io = runtime(content_id.size);
        let timeout_localizer =
            localizer(&worker, secret.clone(), destination.path(), timeout_io).await;
        let timeout_partial = timeout_localizer
            .content_path(&content_id)
            .with_extension("partial");
        fs::write(&timeout_partial, &bytes[..LOCALIZATION_CHUNK_BYTES]).unwrap();
        let offline = DirectDataRef {
            owner_node: owner.clone(),
            content_id: content_id.clone(),
            endpoint_hint: format!("link-local://{}", unique("offline-timeout")),
        };
        assert!(timeout_localizer.localize(&[offline]).await.is_err());
        assert_eq!(
            fs::read(&timeout_partial).unwrap(),
            &bytes[..LOCALIZATION_CHUNK_BYTES]
        );
        assert!(!timeout_localizer.content_path(&content_id).exists());

        let address = unique("cancel-source");
        let source = tempfile::NamedTempFile::new().unwrap();
        fs::write(source.path(), &bytes).unwrap();
        let server_io = runtime(content_id.size);
        server_io
            .testkit()
            .set_network_delay(Duration::from_millis(50));
        let server = FileContentServer::open(
            owner.clone(),
            worker.clone(),
            address.clone(),
            secret.clone(),
            vec![FileContentSource {
                content_id: content_id.clone(),
                path: source.path().to_owned(),
            }],
            Duration::from_secs(2),
            server_io,
        )
        .await
        .unwrap();
        let server = tokio::spawn(server.serve_once());
        let cancelled_destination = tempfile::tempdir().unwrap();
        let cancelled_io = runtime(content_id.size);
        let cancelled_localizer = LinkResourceLocalizer::open(
            worker,
            secret,
            cancelled_destination.path(),
            Duration::from_secs(2),
            cancelled_io.clone(),
        )
        .await
        .unwrap();
        let resource = DirectDataRef {
            owner_node: owner,
            content_id: content_id.clone(),
            endpoint_hint: format!("link-local://{address}"),
        };
        assert!(
            tokio::time::timeout(
                Duration::from_millis(25),
                cancelled_localizer.localize(&[resource])
            )
            .await
            .is_err()
        );
        cancelled_localizer.shutdown().await.unwrap();
        let metrics = cancelled_io.metrics();
        assert_eq!(metrics.queued_jobs, 0);
        assert_eq!(metrics.active_reads, 0);
        assert_eq!(metrics.active_writes, 0);
        assert_eq!(metrics.active_hash_jobs, 0);
        assert_eq!(metrics.buffered_bytes, 0);
        assert!(metrics.peak_buffered_bytes <= cancelled_io.budget().max_buffered_bytes);
        assert!(!cancelled_localizer.content_path(&content_id).exists());
        let partial = cancelled_localizer
            .content_path(&content_id)
            .with_extension("partial");
        if partial.exists() {
            assert!(fs::metadata(partial).unwrap().len() <= content_id.size);
        }
        let _ = server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hash_mismatch_removes_partial_and_never_publishes_final() {
        let bytes = vec![11_u8; 512 * 1024];
        let content_id = identity(&bytes);
        let worker = NodeId(unique("worker"));
        let owner = NodeId(unique("owner"));
        let address = unique("corrupt-source");
        let secret: Arc<[u8]> = Arc::from(unique("secret-at-least-thirty-two-bytes").into_bytes());
        let listener = LocalListener::bind(
            &LocalAddress(address.clone()),
            endpoint_id(&owner),
            data_transport_budget(),
        )
        .unwrap();
        let server_secret = secret.clone();
        let server_owner = owner.clone();
        let server_worker = worker.clone();
        let server_content = content_id.clone();
        let server = tokio::spawn(async move {
            let mut connection = listener.accept(endpoint_id(&server_worker)).await.unwrap();
            authenticate_server(
                &mut connection,
                &server_owner,
                &server_worker,
                &server_secret,
                Duration::from_secs(2),
            )
            .await
            .unwrap();
            let _: ContentRequest = serde_json::from_slice(
                &receive_message(&mut connection, Duration::from_secs(2))
                    .await
                    .unwrap(),
            )
            .unwrap();
            send_manifest(
                &mut connection,
                Ok(ContentManifest {
                    content_id: server_content,
                    chunk_bytes: LOCALIZATION_CHUNK_BYTES,
                }),
                Duration::from_secs(2),
            )
            .await
            .unwrap();
            for chunk in bytes.chunks(LOCALIZATION_CHUNK_BYTES) {
                let corrupt = vec![chunk[0].wrapping_add(1); chunk.len()];
                send_message(&mut connection, &corrupt, false, Duration::from_secs(2))
                    .await
                    .unwrap();
            }
            let _ = receive_message(&mut connection, Duration::from_secs(2)).await;
        });
        let destination = tempfile::tempdir().unwrap();
        let io = runtime(content_id.size);
        let localizer = localizer(&worker, secret, destination.path(), io).await;
        let resource = DirectDataRef {
            owner_node: owner,
            content_id: content_id.clone(),
            endpoint_hint: format!("link-local://{address}"),
        };
        let error = localizer.localize(&[resource]).await.unwrap_err();
        assert_eq!(error.kind, DistributedErrorKind::Corrupt);
        server.await.unwrap();
        assert!(!localizer.content_path(&content_id).exists());
        assert!(
            !localizer
                .content_path(&content_id)
                .with_extension("partial")
                .exists()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn final_path_is_atomically_visible_after_slow_transfer() {
        let bytes = vec![23_u8; 2 * 1024 * 1024];
        let content_id = identity(&bytes);
        let worker = NodeId(unique("worker"));
        let owner = NodeId(unique("owner"));
        let address = unique("slow-source");
        let secret: Arc<[u8]> = Arc::from(unique("secret-at-least-thirty-two-bytes").into_bytes());
        let source = tempfile::NamedTempFile::new().unwrap();
        fs::write(source.path(), &bytes).unwrap();
        let server_io = runtime(content_id.size);
        server_io
            .testkit()
            .set_network_delay(Duration::from_millis(5));
        let server = FileContentServer::open(
            owner.clone(),
            worker.clone(),
            address.clone(),
            secret.clone(),
            vec![FileContentSource {
                content_id: content_id.clone(),
                path: source.path().to_owned(),
            }],
            Duration::from_secs(2),
            server_io,
        )
        .await
        .unwrap();
        let server = tokio::spawn(server.serve_once());
        let destination = tempfile::tempdir().unwrap();
        let localizer = Arc::new(
            LinkResourceLocalizer::open(
                worker,
                secret,
                destination.path(),
                Duration::from_secs(2),
                runtime(content_id.size),
            )
            .await
            .unwrap(),
        );
        let final_path = localizer.content_path(&content_id);
        let resource = DirectDataRef {
            owner_node: owner,
            content_id,
            endpoint_hint: format!("link-local://{address}"),
        };
        let task_localizer = localizer.clone();
        let task = tokio::spawn(async move { task_localizer.localize(&[resource]).await });
        while !task.is_finished() && !final_path.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        if final_path.exists() {
            assert_eq!(fs::read(&final_path).unwrap(), bytes);
        }
        task.await.unwrap().unwrap();
        server.await.unwrap().unwrap();
        assert_eq!(fs::read(final_path).unwrap(), bytes);
    }
}
