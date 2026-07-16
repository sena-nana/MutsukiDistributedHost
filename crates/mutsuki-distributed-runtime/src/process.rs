use crate::{
    Coordinator, ResourceLocalizer, WireRemoteWorker, WorkerRegistry, WorkerRequestDispatcher,
    WorkerTransport,
};
use hmac::{Hmac, Mac};
use mutsuki_distributed_contracts::{
    ClusterCommand, ClusterReply, ClusterReplyBody, ClusterRequest, ControllerCommand,
    ControllerReply, ControllerReplyBody, ControllerRequest, ControllerSubmit, DistributedError,
    DistributedErrorKind, GlobalTaskId, LocalTaskOutcome, NodeId, SidecarCapabilityProof,
    TaskPlacement, WorkerAdvertisement, WorkerFailure, WorkerHealth, WorkerPulse, decode_control,
    encode_control,
};
use mutsuki_distributed_host_adapter::HostAdapter;
use mutsuki_link::{
    Connection, ConnectionQuality, EndpointAddress, EndpointId, ForwardSecrecyPolicy,
    IdentityEvidence, IdentityStatus, LocalPeerCredentialPolicy, PeerId, ProtocolSelection,
    ProtocolVersion, RemoteSecurityPolicy, SecurityExpectation, SecurityLevel, SecurityPolicy,
    SessionContinuity, SessionId, SessionInfo, TransportBudget, TransportErrorKind, TransportKind,
    TransportSecurityEvidence, authenticate_session,
    local::{LocalAddress, LocalConnection, LocalListener},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as AsyncMutex;

type HmacSha256 = Hmac<Sha256>;
const AUTH_CONTEXT: &[u8] = b"mutsuki.distributed.local-session.v1";
const DATA_CHUNK_BYTES: usize = 256 * 1024;
static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerConnectionConfig {
    pub node_id: NodeId,
    pub address: String,
}

#[derive(Clone, Debug)]
pub struct FileContentSource {
    pub content_id: mutsuki_runtime_contracts::ContentId,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContentRequest {
    content_id: mutsuki_runtime_contracts::ContentId,
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

pub struct FileContentServer {
    local_node: NodeId,
    worker_node: NodeId,
    address: String,
    secret: Arc<[u8]>,
    sources: BTreeMap<String, FileContentSource>,
    timeout: Duration,
}

impl FileContentServer {
    pub fn new(
        local_node: NodeId,
        worker_node: NodeId,
        address: impl Into<String>,
        secret: Arc<[u8]>,
        sources: Vec<FileContentSource>,
        timeout: Duration,
    ) -> Result<Self, DistributedError> {
        validate_secret(&secret)?;
        let mut indexed = BTreeMap::new();
        for source in sources {
            validate_content_file(&source)?;
            if indexed
                .insert(source.content_id.digest.clone(), source)
                .is_some()
            {
                return Err(invalid_process_config("duplicate content digest"));
            }
        }
        if indexed.is_empty() {
            return Err(invalid_process_config("content server requires a source"));
        }
        Ok(Self {
            local_node,
            worker_node,
            address: address.into(),
            secret,
            sources: indexed,
            timeout,
        })
    }

    pub async fn serve_once(self) -> Result<(), DistributedError> {
        let listener = LocalListener::bind(
            &LocalAddress(self.address),
            endpoint_id(&self.local_node),
            data_transport_budget(),
        )
        .map_err(map_transport)?;
        let mut connection = listener
            .accept(endpoint_id(&self.worker_node))
            .await
            .map_err(map_transport)?;
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
            let reply = ContentManifestReply {
                result: Err(WorkerFailure::from(&error)),
            };
            send_message(
                &mut connection,
                &serde_json::to_vec(&reply)
                    .map_err(|_| protocol_error("content manifest encode failed"))?,
                true,
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
        let reply = ContentManifestReply {
            result: Ok(ContentManifest {
                content_id: source.content_id.clone(),
                chunk_bytes: DATA_CHUNK_BYTES,
            }),
        };
        send_message(
            &mut connection,
            &serde_json::to_vec(&reply)
                .map_err(|_| protocol_error("content manifest encode failed"))?,
            true,
            self.timeout,
        )
        .await?;
        let mut file = File::open(&source.path).map_err(|_| content_io_error())?;
        let mut chunk = vec![0; DATA_CHUNK_BYTES];
        loop {
            let read = file.read(&mut chunk).map_err(|_| content_io_error())?;
            if read == 0 {
                break;
            }
            send_message(&mut connection, &chunk[..read], false, self.timeout).await?;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    }
}

pub struct LinkResourceLocalizer {
    worker_node: NodeId,
    secret: Arc<[u8]>,
    destination: PathBuf,
    max_content_bytes: u64,
    timeout: Duration,
}

impl LinkResourceLocalizer {
    pub fn new(
        worker_node: NodeId,
        secret: Arc<[u8]>,
        destination: impl Into<PathBuf>,
        max_content_bytes: u64,
        timeout: Duration,
    ) -> Result<Self, DistributedError> {
        validate_secret(&secret)?;
        if max_content_bytes == 0 {
            return Err(invalid_process_config(
                "content localization budget must be positive",
            ));
        }
        let destination = destination.into();
        fs::create_dir_all(&destination).map_err(|_| content_io_error())?;
        Ok(Self {
            worker_node,
            secret,
            destination,
            max_content_bytes,
            timeout,
        })
    }

    async fn localize_one(
        &self,
        resource: &mutsuki_distributed_contracts::DirectDataRef,
    ) -> Result<(), DistributedError> {
        validate_sha256_content_id(&resource.content_id)?;
        if resource.content_id.size > self.max_content_bytes {
            return Err(DistributedError::new(
                DistributedErrorKind::CapacityExceeded,
                "direct content exceeds the Worker localization budget",
            ));
        }
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
        let deadline = Instant::now() + self.timeout;
        let mut connection = loop {
            let context = mutsuki_link::ConnectContext {
                deadline: Some(deadline),
                ..mutsuki_link::ConnectContext::default()
            };
            match mutsuki_link::local::connect(
                &LocalAddress(address.into()),
                endpoint_id(&self.worker_node),
                endpoint_id(&resource.owner_node),
                data_transport_budget(),
                &context,
            )
            .await
            {
                Ok(connection) => break connection,
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => return Err(map_transport(error)),
            }
        };
        authenticate_client(
            &mut connection,
            &self.worker_node,
            &resource.owner_node,
            &self.secret,
            self.timeout,
        )
        .await?;
        let request = ContentRequest {
            content_id: resource.content_id.clone(),
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
            DistributedError::new(failure.kind, "direct content endpoint rejected the request")
        })?;
        if manifest.content_id != resource.content_id
            || manifest.chunk_bytes == 0
            || manifest.chunk_bytes > DATA_CHUNK_BYTES
        {
            return Err(protocol_error("direct content manifest is incompatible"));
        }
        let final_path = self.destination.join(&resource.content_id.digest);
        let temporary = final_path.with_extension("partial");
        let mut output = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|_| content_io_error())?;
        let mut remaining = resource.content_id.size;
        let mut hasher = Sha256::new();
        while remaining > 0 {
            let chunk = receive_message(&mut connection, self.timeout).await?;
            if chunk.is_empty()
                || chunk.len() > manifest.chunk_bytes
                || u64::try_from(chunk.len()).unwrap_or(u64::MAX) > remaining
            {
                let _ = fs::remove_file(&temporary);
                return Err(protocol_error("direct content chunk violates the manifest"));
            }
            output.write_all(&chunk).map_err(|_| content_io_error())?;
            hasher.update(&chunk);
            remaining -= u64::try_from(chunk.len()).expect("chunk fits u64");
        }
        output.sync_all().map_err(|_| content_io_error())?;
        let digest = format!("{:x}", hasher.finalize());
        if resource.content_id.algorithm != "sha256" || digest != resource.content_id.digest {
            drop(output);
            let _ = fs::remove_file(&temporary);
            return Err(DistributedError::new(
                DistributedErrorKind::Corrupt,
                "direct content digest verification failed",
            ));
        }
        drop(output);
        fs::rename(&temporary, &final_path).map_err(|_| content_io_error())?;
        Ok(())
    }

    pub fn content_path(&self, content_id: &mutsuki_runtime_contracts::ContentId) -> PathBuf {
        self.destination.join(&content_id.digest)
    }
}

impl ResourceLocalizer for LinkResourceLocalizer {
    fn localize<'a>(
        &'a self,
        resources: &'a [mutsuki_distributed_contracts::DirectDataRef],
    ) -> crate::WorkerFuture<'a, ()> {
        Box::pin(async move {
            for resource in resources {
                self.localize_one(resource).await?;
            }
            Ok(())
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuthHello {
    node_id: NodeId,
    nonce: [u8; 32],
    proof: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuthWelcome {
    node_id: NodeId,
    nonce: [u8; 32],
    proof: [u8; 32],
}

// A cancelled request may have written its frame without consuming the reply.
// Only a complete request/reply exchange leaves the connection reusable.
struct InFlightConnection<'a> {
    state: &'a mut Option<LocalConnection>,
    reusable: bool,
}

impl<'a> InFlightConnection<'a> {
    fn new(state: &'a mut Option<LocalConnection>) -> Self {
        Self {
            state,
            reusable: false,
        }
    }

    fn connection(&mut self) -> &mut LocalConnection {
        self.state.as_mut().expect("connection initialized")
    }

    fn keep(mut self) {
        self.reusable = true;
    }
}

impl Drop for InFlightConnection<'_> {
    fn drop(&mut self) {
        if !self.reusable {
            if let Some(connection) = self.state.as_mut() {
                connection.abort();
            }
            *self.state = None;
        }
    }
}

#[derive(Default)]
struct TaskStop {
    requested: AtomicBool,
    notify: tokio::sync::Notify,
}

impl TaskStop {
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

struct ScopedBackgroundTask {
    stop: Arc<TaskStop>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl ScopedBackgroundTask {
    fn new(stop: Arc<TaskStop>, handle: tokio::task::JoinHandle<()>) -> Self {
        Self {
            stop,
            handle: Some(handle),
        }
    }

    async fn shutdown(mut self) {
        self.stop.request();
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for ScopedBackgroundTask {
    fn drop(&mut self) {
        self.stop.request();
    }
}

pub struct LinkWorkerTransport {
    local_node: NodeId,
    remote_node: NodeId,
    address: String,
    secret: Arc<[u8]>,
    timeout: Duration,
    connection: AsyncMutex<Option<LocalConnection>>,
    next_request_id: AtomicU64,
}

impl LinkWorkerTransport {
    pub fn new(
        local_node: NodeId,
        remote_node: NodeId,
        address: impl Into<String>,
        secret: Arc<[u8]>,
        timeout: Duration,
    ) -> Result<Self, DistributedError> {
        validate_secret(&secret)?;
        if timeout.is_zero() {
            return Err(invalid_process_config("request timeout must be positive"));
        }
        Ok(Self {
            local_node,
            remote_node,
            address: address.into(),
            secret,
            timeout,
            connection: AsyncMutex::new(None),
            next_request_id: AtomicU64::new(1),
        })
    }

    async fn connect(&self) -> Result<LocalConnection, DistributedError> {
        let context = mutsuki_link::ConnectContext {
            deadline: Some(Instant::now() + self.timeout),
            ..mutsuki_link::ConnectContext::default()
        };
        let mut connection = mutsuki_link::local::connect(
            &LocalAddress(self.address.clone()),
            endpoint_id(&self.local_node),
            endpoint_id(&self.remote_node),
            transport_budget(),
            &context,
        )
        .await
        .map_err(map_transport)?;
        authenticate_client(
            &mut connection,
            &self.local_node,
            &self.remote_node,
            &self.secret,
            self.timeout,
        )
        .await?;
        Ok(connection)
    }

    async fn request(&self, command: ClusterCommand) -> Result<ClusterReplyBody, DistributedError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let payload = encode_control(&ClusterRequest {
            request_id,
            command,
        })?;
        let mut state = self.connection.lock().await;
        if state.is_none() {
            *state = Some(self.connect().await?);
        }
        let mut in_flight = InFlightConnection::new(&mut state);
        let result = async {
            let connection = in_flight.connection();
            send_message(connection, &payload, true, self.timeout).await?;
            let bytes = receive_message(connection, self.timeout).await?;
            let reply: ClusterReply = decode_control(&bytes)?;
            if reply.request_id != request_id {
                return Err(protocol_error("cluster reply request id does not match"));
            }
            reply.result.map_err(|failure| {
                DistributedError::new(failure.kind, "remote Worker rejected cluster request")
            })
        }
        .await;
        match result {
            Ok(reply) => {
                in_flight.keep();
                Ok(reply)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn describe(&self) -> Result<WorkerAdvertisement, DistributedError> {
        match self.request(ClusterCommand::DescribeWorker).await? {
            ClusterReplyBody::Worker(advertisement) => Ok(*advertisement),
            _ => Err(protocol_error("Worker describe reply has the wrong type")),
        }
    }

    pub async fn pulse(&self) -> Result<WorkerPulse, DistributedError> {
        match self.request(ClusterCommand::WorkerPulse).await? {
            ClusterReplyBody::Pulse(pulse) => Ok(pulse),
            _ => Err(protocol_error("Worker pulse reply has the wrong type")),
        }
    }

    pub async fn drain(&self) -> Result<(), DistributedError> {
        match self.request(ClusterCommand::DrainWorker).await? {
            ClusterReplyBody::Draining => Ok(()),
            _ => Err(protocol_error("Worker drain reply has the wrong type")),
        }
    }

    pub async fn stop(&self) -> Result<(), DistributedError> {
        match self.request(ClusterCommand::StopWorker).await? {
            ClusterReplyBody::Stopping => Ok(()),
            _ => Err(protocol_error("Worker stop reply has the wrong type")),
        }
    }
}

impl WorkerTransport for LinkWorkerTransport {
    fn round_trip(&self, request: Vec<u8>) -> crate::WorkerFuture<'_, Vec<u8>> {
        Box::pin(async move {
            match self.request(ClusterCommand::Worker(request)).await? {
                ClusterReplyBody::WorkerReply(reply) => Ok(reply),
                _ => Err(protocol_error("Worker task reply has the wrong type")),
            }
        })
    }
}

pub struct WorkerProcess {
    node_id: NodeId,
    controller_node: NodeId,
    address: String,
    secret: Arc<[u8]>,
    advertisement: WorkerAdvertisement,
    host: Arc<dyn HostAdapter>,
    dispatcher: WorkerRequestDispatcher,
    timeout: Duration,
}

impl WorkerProcess {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        controller_node: NodeId,
        address: impl Into<String>,
        secret: Arc<[u8]>,
        advertisement: WorkerAdvertisement,
        host: Arc<dyn HostAdapter>,
        localizer: Arc<dyn ResourceLocalizer>,
        timeout: Duration,
    ) -> Result<Self, DistributedError> {
        validate_secret(&secret)?;
        if advertisement.node_id != node_id || timeout.is_zero() {
            return Err(invalid_process_config(
                "Worker identity, advertisement, or timeout is invalid",
            ));
        }
        let endpoint = Arc::new(crate::WorkerEndpoint::new(
            node_id.clone(),
            host.clone(),
            localizer,
        ));
        Ok(Self {
            node_id,
            controller_node,
            address: address.into(),
            secret,
            advertisement,
            host,
            dispatcher: WorkerRequestDispatcher::new(endpoint),
            timeout,
        })
    }

    pub async fn run(self) -> Result<(), DistributedError> {
        let listener = LocalListener::bind(
            &LocalAddress(self.address.clone()),
            endpoint_id(&self.node_id),
            transport_budget(),
        )
        .map_err(map_transport)?;
        loop {
            let mut connection = listener
                .accept(endpoint_id(&self.controller_node))
                .await
                .map_err(map_transport)?;
            if authenticate_server(
                &mut connection,
                &self.node_id,
                &self.controller_node,
                &self.secret,
                self.timeout,
            )
            .await
            .is_err()
            {
                connection.abort();
                continue;
            }
            match self.serve_connection(&mut connection).await {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(error)
                    if matches!(
                        error.kind,
                        DistributedErrorKind::TransportClosed
                            | DistributedErrorKind::WorkerUnavailable
                    ) => {}
                Err(error) => return Err(error),
            }
        }
    }

    async fn serve_connection(
        &self,
        connection: &mut LocalConnection,
    ) -> Result<bool, DistributedError> {
        loop {
            let bytes = match receive_message(connection, Duration::from_secs(24 * 60 * 60)).await {
                Ok(bytes) => bytes,
                Err(error) if error.kind == DistributedErrorKind::TransportClosed => {
                    return Ok(false);
                }
                Err(error) => return Err(error),
            };
            let request: ClusterRequest = decode_control(&bytes)?;
            let (result, stopping) = match request.command {
                ClusterCommand::DescribeWorker => (
                    Ok(ClusterReplyBody::Worker(Box::new(
                        self.advertisement.clone(),
                    ))),
                    false,
                ),
                ClusterCommand::WorkerPulse => {
                    let pulse = self.worker_pulse().await;
                    (pulse.map(ClusterReplyBody::Pulse), false)
                }
                ClusterCommand::Worker(request) => (
                    self.dispatcher
                        .dispatch(&request)
                        .await
                        .map(ClusterReplyBody::WorkerReply),
                    false,
                ),
                ClusterCommand::DrainWorker => (
                    self.host
                        .begin_drain()
                        .await
                        .map(|()| ClusterReplyBody::Draining),
                    false,
                ),
                ClusterCommand::StopWorker => (
                    self.host
                        .begin_drain()
                        .await
                        .map(|()| ClusterReplyBody::Stopping),
                    true,
                ),
            };
            let reply = encode_control(&ClusterReply {
                request_id: request.request_id,
                result: result.map_err(|error| WorkerFailure::from(&error)),
            })?;
            send_message(connection, &reply, true, self.timeout).await?;
            if stopping {
                // The Link transport owns the socket writer task; allow the
                // queued terminal control reply to reach the peer before the
                // process drops the connection.
                tokio::time::sleep(Duration::from_millis(10)).await;
                return Ok(true);
            }
        }
    }

    async fn worker_pulse(&self) -> Result<WorkerPulse, DistributedError> {
        self.host.health().await?;
        let snapshots = self.host.snapshots().await?;
        let running_tasks = snapshots
            .iter()
            .filter(|snapshot| snapshot.status == "running")
            .count();
        let queue_depth = snapshots
            .iter()
            .filter(|snapshot| matches!(snapshot.status.as_str(), "ready" | "waiting" | "blocked"))
            .count();
        Ok(WorkerPulse {
            node_id: self.node_id.clone(),
            snapshot_version: self.advertisement.snapshot_version,
            health: if running_tasks == 0 {
                WorkerHealth::Ready
            } else {
                WorkerHealth::Busy
            },
            running_tasks,
            queue_depth,
        })
    }
}

pub struct ControllerProcess {
    coordinator: Arc<Coordinator>,
    registry: Arc<std::sync::Mutex<WorkerRegistry>>,
    workers: BTreeMap<NodeId, Arc<LinkWorkerTransport>>,
    pulse_gate: AsyncMutex<()>,
    stopping: AtomicBool,
}

impl ControllerProcess {
    pub async fn connect(
        origin_node: NodeId,
        origin_host: Arc<dyn HostAdapter>,
        workers: Vec<WorkerConnectionConfig>,
        secret: Arc<[u8]>,
        max_tasks: usize,
        timeout: Duration,
    ) -> Result<Self, DistributedError> {
        if workers.is_empty() {
            return Err(invalid_process_config("at least one Worker is required"));
        }
        let mut registry = WorkerRegistry::new(workers.len())?;
        let mut transports = BTreeMap::new();
        let mut remotes: BTreeMap<NodeId, Arc<dyn crate::RemoteWorker>> = BTreeMap::new();
        for worker in workers {
            let transport = Arc::new(LinkWorkerTransport::new(
                origin_node.clone(),
                worker.node_id.clone(),
                worker.address,
                secret.clone(),
                timeout,
            )?);
            let advertisement = transport.describe().await?;
            if advertisement.node_id != worker.node_id {
                return Err(protocol_error(
                    "authenticated Worker advertised another node id",
                ));
            }
            registry.register(advertisement)?;
            remotes.insert(
                worker.node_id.clone(),
                Arc::new(WireRemoteWorker::new(transport.clone())),
            );
            transports.insert(worker.node_id, transport);
        }
        let registry = Arc::new(std::sync::Mutex::new(registry));
        let coordinator = Arc::new(Coordinator::new(
            origin_node,
            origin_host,
            registry.clone(),
            remotes,
            max_tasks,
            transports.len().saturating_sub(1),
        )?);
        Ok(Self {
            coordinator,
            registry,
            workers: transports,
            pulse_gate: AsyncMutex::new(()),
            stopping: AtomicBool::new(false),
        })
    }

    pub fn coordinator(&self) -> &Arc<Coordinator> {
        &self.coordinator
    }

    pub async fn pulse_once(&self) -> Vec<(NodeId, DistributedError)> {
        // Keep the transport result and its registry update in the same order.
        // Otherwise an older failed pulse can overwrite a newer successful one.
        let _pulse = self.pulse_gate.lock().await;
        let mut failures = Vec::new();
        for (node_id, worker) in &self.workers {
            match worker.pulse().await {
                Ok(pulse) => {
                    if let Err(error) = self
                        .registry
                        .lock()
                        .expect("Worker registry mutex")
                        .pulse(pulse)
                    {
                        failures.push((node_id.clone(), error));
                    }
                }
                Err(error) => {
                    let mut registry = self.registry.lock().expect("Worker registry mutex");
                    if let Some(snapshot_version) =
                        registry.get(node_id).map(|worker| worker.snapshot_version)
                    {
                        let _ = registry.pulse(WorkerPulse {
                            node_id: node_id.clone(),
                            snapshot_version,
                            health: WorkerHealth::Unreachable,
                            running_tasks: 0,
                            queue_depth: 0,
                        });
                    }
                    failures.push((node_id.clone(), error));
                }
            }
        }
        failures
    }

    pub async fn shutdown(&self) -> Vec<(NodeId, DistributedError)> {
        let mut failures = Vec::new();
        for (node_id, worker) in &self.workers {
            if let Err(error) = worker.stop().await {
                failures.push((node_id.clone(), error));
            }
        }
        failures
    }

    pub async fn serve_management(
        self: Arc<Self>,
        client_node: NodeId,
        address: impl Into<String>,
        secret: Arc<[u8]>,
        pulse_interval: Duration,
        timeout: Duration,
    ) -> Result<(), DistributedError> {
        validate_secret(&secret)?;
        if pulse_interval.is_zero() {
            return Err(invalid_process_config("pulse interval must be positive"));
        }
        let address = address.into();
        let listener = LocalListener::bind(
            &LocalAddress(address),
            endpoint_id(&NodeId("controller-management".into())),
            transport_budget(),
        )
        .map_err(map_transport)?;
        let pulse_controller = self.clone();
        let pulse_stop = Arc::new(TaskStop::default());
        let task_stop = pulse_stop.clone();
        let pulse_task = ScopedBackgroundTask::new(
            pulse_stop,
            tokio::spawn(async move {
                while !pulse_controller.stopping.load(Ordering::Acquire)
                    && !task_stop.is_requested()
                {
                    pulse_controller.pulse_once().await;
                    if task_stop.is_requested() {
                        break;
                    }
                    tokio::select! {
                        () = tokio::time::sleep(pulse_interval) => {}
                        () = task_stop.notify.notified() => {}
                    }
                }
            }),
        );
        while !self.stopping.load(Ordering::Acquire) {
            let mut connection = listener
                .accept(endpoint_id(&client_node))
                .await
                .map_err(map_transport)?;
            if authenticate_server(
                &mut connection,
                &NodeId("controller-management".into()),
                &client_node,
                &secret,
                timeout,
            )
            .await
            .is_err()
            {
                connection.abort();
                continue;
            }
            self.serve_management_connection(&mut connection, timeout)
                .await?;
        }
        pulse_task.shutdown().await;
        let failures = self.shutdown().await;
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DistributedError::new(
                DistributedErrorKind::WorkerUnavailable,
                "one or more Workers did not drain during controller shutdown",
            ))
        }
    }

    async fn serve_management_connection(
        &self,
        connection: &mut LocalConnection,
        timeout: Duration,
    ) -> Result<(), DistributedError> {
        loop {
            let bytes = match receive_message(connection, Duration::from_secs(24 * 60 * 60)).await {
                Ok(bytes) => bytes,
                Err(error) if error.kind == DistributedErrorKind::TransportClosed => return Ok(()),
                Err(error) => return Err(error),
            };
            let request: ControllerRequest = decode_control(&bytes)?;
            let (result, shutdown) = match request.command {
                ControllerCommand::Capabilities => (
                    Ok(ControllerReplyBody::Capabilities(
                        SidecarCapabilityProof::current(),
                    )),
                    false,
                ),
                ControllerCommand::Submit(submit) => {
                    let ControllerSubmit {
                        global_task_id,
                        portable,
                        requirements,
                        direct_inputs,
                    } = *submit;
                    (
                        self.coordinator
                            .submit(global_task_id, portable, requirements, direct_inputs)
                            .await
                            .map(ControllerReplyBody::Placement),
                        false,
                    )
                }
                ControllerCommand::Cancel(global_task_id) => (
                    self.coordinator
                        .cancel(&global_task_id)
                        .await
                        .map(|()| ControllerReplyBody::Cancelled),
                    false,
                ),
                ControllerCommand::Outcome(global_task_id) => (
                    self.coordinator
                        .outcome(&global_task_id)
                        .await
                        .map(ControllerReplyBody::Outcome),
                    false,
                ),
                ControllerCommand::Health => {
                    let failures = self.pulse_once().await;
                    if failures.is_empty() {
                        (Ok(ControllerReplyBody::Health("healthy".into())), false)
                    } else {
                        (
                            Err(DistributedError::new(
                                DistributedErrorKind::WorkerUnavailable,
                                "one or more Worker sessions are unavailable",
                            )),
                            false,
                        )
                    }
                }
                ControllerCommand::Shutdown => {
                    self.stopping.store(true, Ordering::Release);
                    (Ok(ControllerReplyBody::ShuttingDown), true)
                }
            };
            let reply = encode_control(&ControllerReply {
                request_id: request.request_id,
                result: result.map_err(|error| WorkerFailure::from(&error)),
            })?;
            send_message(connection, &reply, true, timeout).await?;
            if shutdown {
                tokio::time::sleep(Duration::from_millis(10)).await;
                return Ok(());
            }
        }
    }
}

pub struct ControllerClient {
    local_node: NodeId,
    address: String,
    secret: Arc<[u8]>,
    timeout: Duration,
    connection: AsyncMutex<Option<LocalConnection>>,
    next_request_id: AtomicU64,
}

impl ControllerClient {
    pub fn new(
        local_node: NodeId,
        address: impl Into<String>,
        secret: Arc<[u8]>,
        timeout: Duration,
    ) -> Result<Self, DistributedError> {
        validate_secret(&secret)?;
        Ok(Self {
            local_node,
            address: address.into(),
            secret,
            timeout,
            connection: AsyncMutex::new(None),
            next_request_id: AtomicU64::new(1),
        })
    }

    async fn request(
        &self,
        command: ControllerCommand,
    ) -> Result<ControllerReplyBody, DistributedError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let payload = encode_control(&ControllerRequest {
            request_id,
            command,
        })?;
        let mut state = self.connection.lock().await;
        if state.is_none() {
            let context = mutsuki_link::ConnectContext {
                deadline: Some(Instant::now() + self.timeout),
                ..mutsuki_link::ConnectContext::default()
            };
            let mut connection = mutsuki_link::local::connect(
                &LocalAddress(self.address.clone()),
                endpoint_id(&self.local_node),
                endpoint_id(&NodeId("controller-management".into())),
                transport_budget(),
                &context,
            )
            .await
            .map_err(map_transport)?;
            authenticate_client(
                &mut connection,
                &self.local_node,
                &NodeId("controller-management".into()),
                &self.secret,
                self.timeout,
            )
            .await?;
            *state = Some(connection);
        }
        let result = async {
            let connection = state.as_mut().expect("controller connection initialized");
            send_message(connection, &payload, true, self.timeout).await?;
            let bytes = receive_message(connection, self.timeout).await?;
            let reply: ControllerReply = decode_control(&bytes)?;
            if reply.request_id != request_id {
                return Err(protocol_error("controller reply request id does not match"));
            }
            reply.result.map_err(|failure| {
                DistributedError::new(failure.kind, "controller rejected management request")
            })
        }
        .await;
        if result.is_err() {
            if let Some(connection) = state.as_mut() {
                connection.abort();
            }
            *state = None;
        }
        result
    }

    pub async fn capabilities(&self) -> Result<SidecarCapabilityProof, DistributedError> {
        match self.request(ControllerCommand::Capabilities).await? {
            ControllerReplyBody::Capabilities(proof) => Ok(proof),
            _ => Err(protocol_error(
                "controller capability reply has the wrong type",
            )),
        }
    }

    pub async fn submit(
        &self,
        global_task_id: GlobalTaskId,
        portable: mutsuki_runtime_contracts::PortableTask,
        requirements: mutsuki_runtime_contracts::RequirementSet,
        direct_inputs: Vec<mutsuki_distributed_contracts::DirectDataRef>,
    ) -> Result<TaskPlacement, DistributedError> {
        match self
            .request(ControllerCommand::Submit(Box::new(ControllerSubmit {
                global_task_id,
                portable,
                requirements,
                direct_inputs,
            })))
            .await?
        {
            ControllerReplyBody::Placement(placement) => Ok(placement),
            _ => Err(protocol_error("controller submit reply has the wrong type")),
        }
    }

    pub async fn cancel(&self, global_task_id: GlobalTaskId) -> Result<(), DistributedError> {
        match self
            .request(ControllerCommand::Cancel(global_task_id))
            .await?
        {
            ControllerReplyBody::Cancelled => Ok(()),
            _ => Err(protocol_error("controller cancel reply has the wrong type")),
        }
    }

    pub async fn outcome(
        &self,
        global_task_id: GlobalTaskId,
    ) -> Result<Option<LocalTaskOutcome>, DistributedError> {
        match self
            .request(ControllerCommand::Outcome(global_task_id))
            .await?
        {
            ControllerReplyBody::Outcome(outcome) => Ok(outcome),
            _ => Err(protocol_error(
                "controller outcome reply has the wrong type",
            )),
        }
    }

    pub async fn health(&self) -> Result<String, DistributedError> {
        match self.request(ControllerCommand::Health).await? {
            ControllerReplyBody::Health(health) => Ok(health),
            _ => Err(protocol_error("controller health reply has the wrong type")),
        }
    }

    pub async fn shutdown(&self) -> Result<(), DistributedError> {
        match self.request(ControllerCommand::Shutdown).await? {
            ControllerReplyBody::ShuttingDown => Ok(()),
            _ => Err(protocol_error(
                "controller shutdown reply has the wrong type",
            )),
        }
    }
}

async fn authenticate_client(
    connection: &mut LocalConnection,
    local: &NodeId,
    remote: &NodeId,
    secret: &[u8],
    timeout: Duration,
) -> Result<crate::LinkSessionBinding, DistributedError> {
    let nonce = nonce(local);
    let hello = AuthHello {
        node_id: local.clone(),
        nonce,
        proof: auth_proof(secret, b"hello", local, remote, &nonce, &[0; 32])?,
    };
    send_message(
        connection,
        &serde_json::to_vec(&hello).map_err(|_| auth_error())?,
        true,
        timeout,
    )
    .await?;
    let welcome: AuthWelcome = serde_json::from_slice(&receive_message(connection, timeout).await?)
        .map_err(|_| auth_error())?;
    if &welcome.node_id != remote
        || welcome.proof != auth_proof(secret, b"welcome", remote, local, &welcome.nonce, &nonce)?
    {
        return Err(auth_error());
    }
    authenticated_binding(connection, local, remote, secret, &nonce, &welcome.nonce)
}

async fn authenticate_server(
    connection: &mut LocalConnection,
    local: &NodeId,
    remote: &NodeId,
    secret: &[u8],
    timeout: Duration,
) -> Result<crate::LinkSessionBinding, DistributedError> {
    let hello: AuthHello = serde_json::from_slice(&receive_message(connection, timeout).await?)
        .map_err(|_| auth_error())?;
    if &hello.node_id != remote
        || hello.proof != auth_proof(secret, b"hello", remote, local, &hello.nonce, &[0; 32])?
    {
        return Err(auth_error());
    }
    let nonce = nonce(local);
    let welcome = AuthWelcome {
        node_id: local.clone(),
        nonce,
        proof: auth_proof(secret, b"welcome", local, remote, &nonce, &hello.nonce)?,
    };
    send_message(
        connection,
        &serde_json::to_vec(&welcome).map_err(|_| auth_error())?,
        true,
        timeout,
    )
    .await?;
    authenticated_binding(connection, local, remote, secret, &hello.nonce, &nonce)
}

fn authenticated_binding(
    connection: &LocalConnection,
    local: &NodeId,
    remote: &NodeId,
    secret: &[u8],
    client_nonce: &[u8; 32],
    server_nonce: &[u8; 32],
) -> Result<crate::LinkSessionBinding, DistributedError> {
    let transcript = transcript_hash(local, remote, client_nonce, server_nonce);
    let remote_peer = peer_id(remote);
    let local_address = EndpointAddress {
        scheme: "local".into(),
        address: local.0.clone(),
    };
    let remote_address = EndpointAddress {
        scheme: "local".into(),
        address: remote.0.clone(),
    };
    let session = SessionInfo {
        session_id: SessionId::from_bytes(transcript[..16].try_into().expect("SHA prefix")),
        peer_id: remote_peer,
        protocols: vec![ProtocolSelection {
            namespace: mutsuki_distributed_contracts::DISTRIBUTED_PROTOCOL_ID.into(),
            version: ProtocolVersion::new(1, 0),
        }],
        continuity: SessionContinuity::default(),
        quality: ConnectionQuality::default(),
        close_reason: None,
    };
    let evidence = TransportSecurityEvidence {
        transport: TransportKind::Local,
        security_level: SecurityLevel::AuthenticatedEncrypted,
        mutually_authenticated: true,
        local_peer_credential_verified: connection.peer_credentials().is_some(),
        development_plaintext: false,
        identity: IdentityEvidence {
            peer_id: remote_peer,
            public_key_fingerprint: identity_fingerprint(secret, remote),
            key_epoch: 1,
            status: IdentityStatus::Active {
                valid_until_unix_ms: u64::MAX,
            },
        },
        session_key: None,
    };
    let expected = SecurityExpectation {
        peer_id: remote_peer,
        public_key_fingerprint: identity_fingerprint(secret, remote),
        minimum_key_epoch: 1,
        handshake_transcript_hash: transcript,
        local_endpoint: local_address,
        remote_endpoint: remote_address,
        link_version: ProtocolVersion::new(1, 0),
        now_unix_ms: now_millis(),
    };
    let authenticated = authenticate_session(
        &session,
        &evidence,
        &expected,
        SecurityPolicy {
            remote: RemoteSecurityPolicy::AuthenticatedEncrypted,
            forward_secrecy: ForwardSecrecyPolicy::Optional,
            local_peer_credential: LocalPeerCredentialPolicy::Required,
        },
    )
    .map_err(|_| auth_error())?;
    crate::LinkSessionBinding::from_authenticated(authenticated)
}

async fn send_message(
    connection: &mut LocalConnection,
    bytes: &[u8],
    control: bool,
    timeout: Duration,
) -> Result<(), DistributedError> {
    let deadline = Instant::now() + timeout;
    loop {
        let result = if control {
            connection.try_send_control(bytes)
        } else {
            connection.try_send(bytes)
        };
        match result {
            Ok(()) => return Ok(()),
            Err(error) if error.kind == TransportErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(transport_timeout());
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(error) => return Err(map_transport(error)),
        }
    }
}

async fn receive_message(
    connection: &mut LocalConnection,
    timeout: Duration,
) -> Result<Vec<u8>, DistributedError> {
    let deadline = Instant::now() + timeout;
    loop {
        match connection.try_receive() {
            Ok(Some(bytes)) => return Ok(bytes),
            Ok(None) => {}
            Err(error) if error.kind == TransportErrorKind::WouldBlock => {}
            Err(error) => return Err(map_transport(error)),
        }
        if Instant::now() >= deadline {
            return Err(transport_timeout());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn transport_budget() -> TransportBudget {
    TransportBudget {
        max_frame_bytes: mutsuki_distributed_contracts::MAX_CONTROL_FRAME_BYTES,
        idle_timeout: None,
        ..TransportBudget::default()
    }
}

fn data_transport_budget() -> TransportBudget {
    TransportBudget {
        max_frame_bytes: DATA_CHUNK_BYTES + 1024,
        control_queue_capacity: 16,
        data_queue_capacity: 16,
        receive_queue_capacity: 16,
        idle_timeout: None,
        ..TransportBudget::default()
    }
}

fn validate_content_file(source: &FileContentSource) -> Result<(), DistributedError> {
    validate_sha256_content_id(&source.content_id)?;
    let metadata = fs::metadata(&source.path).map_err(|_| content_io_error())?;
    if !metadata.is_file() || metadata.len() != source.content_id.size {
        return Err(invalid_process_config(
            "direct content file size does not match its descriptor",
        ));
    }
    let mut file = File::open(&source.path).map_err(|_| content_io_error())?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; DATA_CHUNK_BYTES];
    loop {
        let read = file.read(&mut buffer).map_err(|_| content_io_error())?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    if format!("{:x}", hash.finalize()) != source.content_id.digest {
        return Err(invalid_process_config(
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
        return Err(invalid_process_config(
            "direct content requires a canonical sha256 digest",
        ));
    }
    Ok(())
}

fn endpoint_id(node: &NodeId) -> EndpointId {
    let digest = Sha256::digest(node.0.as_bytes());
    EndpointId::from_bytes(digest[..16].try_into().expect("SHA prefix"))
}

fn peer_id(node: &NodeId) -> PeerId {
    PeerId::from_bytes(Sha256::digest(node.0.as_bytes()).into())
}

fn identity_fingerprint(secret: &[u8], node: &NodeId) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(AUTH_CONTEXT);
    hash.update(secret);
    hash.update(node.0.as_bytes());
    hash.finalize().into()
}

fn nonce(node: &NodeId) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(AUTH_CONTEXT);
    hash.update(node.0.as_bytes());
    hash.update(now_millis().to_le_bytes());
    hash.update(std::process::id().to_le_bytes());
    hash.update(NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    hash.finalize().into()
}

fn transcript_hash(
    local: &NodeId,
    remote: &NodeId,
    client_nonce: &[u8; 32],
    server_nonce: &[u8; 32],
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(AUTH_CONTEXT);
    hash.update(local.0.as_bytes());
    hash.update(remote.0.as_bytes());
    hash.update(client_nonce);
    hash.update(server_nonce);
    hash.finalize().into()
}

fn auth_proof(
    secret: &[u8],
    role: &[u8],
    local: &NodeId,
    remote: &NodeId,
    nonce: &[u8; 32],
    peer_nonce: &[u8; 32],
) -> Result<[u8; 32], DistributedError> {
    let mut mac = HmacSha256::new_from_slice(secret).map_err(|_| auth_error())?;
    mac.update(AUTH_CONTEXT);
    mac.update(role);
    mac.update(local.0.as_bytes());
    mac.update(remote.0.as_bytes());
    mac.update(nonce);
    mac.update(peer_nonce);
    Ok(mac.finalize().into_bytes().into())
}

fn validate_secret(secret: &[u8]) -> Result<(), DistributedError> {
    if secret.len() < 32 {
        return Err(invalid_process_config(
            "cluster secret must contain at least 32 bytes",
        ));
    }
    Ok(())
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[allow(clippy::needless_pass_by_value)]
fn map_transport(error: mutsuki_link::TransportError) -> DistributedError {
    let kind = match error.kind {
        TransportErrorKind::Closed | TransportErrorKind::Aborted => {
            DistributedErrorKind::TransportClosed
        }
        TransportErrorKind::TimedOut => DistributedErrorKind::WorkerUnavailable,
        _ => DistributedErrorKind::TransportClosed,
    };
    DistributedError::new(kind, "authenticated Link transport failed")
}

fn transport_timeout() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::WorkerUnavailable,
        "authenticated Link request timed out",
    )
}

fn auth_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Protocol,
        "authenticated Link session could not be established",
    )
}

fn protocol_error(message: &'static str) -> DistributedError {
    DistributedError::new(DistributedErrorKind::Protocol, message)
}

fn invalid_process_config(message: &'static str) -> DistributedError {
    DistributedError::new(DistributedErrorKind::InvalidConfig, message)
}

fn content_io_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Storage,
        "direct content storage operation failed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use mutsuki_distributed_contracts::{
        DirectDataRef, LocalTaskOutcome, LocalTaskSnapshot, RunnerGeneration,
    };
    use mutsuki_distributed_host_adapter::HostFuture;
    use mutsuki_runtime_contracts::{
        CancelPolicy, CapabilitySet, ContentId, ExecutionMobility, PortabilityCapability,
        PortabilityCatalog, PortableTask, RequirementSet, RetrySafety, RuntimeEvent,
        SchemaIdentity, Task, TaskAcceptanceDurability, TaskBatch, TaskHandle,
        TaskPortabilityDescriptor,
    };
    use serde_json::json;
    use std::process::{Command, Stdio};
    use std::sync::Mutex;

    #[derive(Default)]
    struct ProcessFakeHost {
        tasks: Mutex<Vec<Task>>,
        cancelled: Mutex<Vec<String>>,
    }

    impl HostAdapter for ProcessFakeHost {
        fn submit_batch(&self, batch: TaskBatch) -> HostFuture<'_, Vec<TaskHandle>> {
            Box::pin(async move {
                let handles = batch
                    .tasks
                    .iter()
                    .map(|task| TaskHandle {
                        task_id: task.task_id.clone(),
                        protocol_id: task.protocol_id.clone(),
                        target_binding_id: task.target_binding_id.clone(),
                        cancel_policy: CancelPolicy::Cascade,
                        trace_id: task.trace_id.clone(),
                        correlation_id: task.correlation_id.clone(),
                    })
                    .collect();
                self.tasks.lock().expect("tasks mutex").extend(batch.tasks);
                Ok(handles)
            })
        }

        fn cancel(&self, handle: &TaskHandle) -> HostFuture<'_, ()> {
            let task_id = handle.task_id.clone();
            Box::pin(async move {
                self.cancelled.lock().expect("cancel mutex").push(task_id);
                Ok(())
            })
        }

        fn snapshots(&self) -> HostFuture<'_, Vec<LocalTaskSnapshot>> {
            Box::pin(async move {
                Ok(self
                    .tasks
                    .lock()
                    .expect("tasks mutex")
                    .iter()
                    .map(|task| LocalTaskSnapshot {
                        task_id: task.task_id.clone(),
                        protocol_id: task.protocol_id.clone(),
                        status: "ready".into(),
                        registry_generation: task.registry_generation,
                        runner_id: task.runner_hint.clone(),
                        lease_id: None,
                    })
                    .collect())
            })
        }

        fn outcome(&self, handle: &TaskHandle) -> HostFuture<'_, Option<LocalTaskOutcome>> {
            let task_id = handle.task_id.clone();
            Box::pin(async move {
                Ok(Some(LocalTaskOutcome {
                    task_id,
                    status: "completed".into(),
                    output_ref: Some("content:child-process-result".into()),
                    reason: None,
                    error_code: None,
                }))
            })
        }

        fn events_after(&self, _sequence: u64, _limit: usize) -> HostFuture<'_, Vec<RuntimeEvent>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn begin_drain(&self) -> HostFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn health(&self) -> HostFuture<'_, String> {
            Box::pin(async { Ok("healthy".into()) })
        }
    }

    fn test_portable() -> PortableTask {
        let mut task = Task::new("source", "example.process", json!({ "value": 9 }));
        task.runner_hint = Some("process-runner".into());
        PortableTask::new(
            task,
            SchemaIdentity::new("example.process", "1.0.0"),
            ContentId::new("sha256", "input", 8, "json"),
            PortabilityCapability {
                mobility: ExecutionMobility::Restartable,
                retry_safety: RetrySafety::Idempotent,
                task_acceptance: TaskAcceptanceDurability::Volatile,
                ..PortabilityCapability::default()
            },
        )
    }

    fn test_advertisement() -> WorkerAdvertisement {
        let portable = test_portable();
        WorkerAdvertisement {
            node_id: NodeId("worker-process".into()),
            protocol_major: mutsuki_distributed_contracts::DISTRIBUTED_PROTOCOL_MAJOR,
            snapshot_version: 1,
            capabilities: CapabilitySet::default(),
            portability: PortabilityCatalog {
                tasks: vec![TaskPortabilityDescriptor {
                    protocol_id: portable.task.protocol_id.clone(),
                    task_schema: portable.task_schema.clone(),
                    checkpoint_schema: None,
                    capability: portable.capability.clone(),
                }],
                resources: Vec::new(),
            },
            runners: vec![RunnerGeneration {
                runner_id: "process-runner".into(),
                plugin_id: "process-plugin".into(),
                runner_generation: 1,
                plugin_generation: 1,
            }],
            localized_content: std::collections::BTreeSet::default(),
            health: WorkerHealth::Ready,
        }
    }

    #[test]
    fn process_worker_child() {
        let Ok(address) = std::env::var("MUTSUKI_TEST_WORKER_ADDRESS") else {
            return;
        };
        let secret = std::env::var("MUTSUKI_TEST_CLUSTER_SECRET").expect("child secret");
        let destination =
            std::env::var("MUTSUKI_TEST_CONTENT_DESTINATION").expect("child content destination");
        let runtime = tokio::runtime::Runtime::new().expect("child runtime");
        runtime
            .block_on(
                WorkerProcess::new(
                    NodeId("worker-process".into()),
                    NodeId("controller-process".into()),
                    address,
                    Arc::from(secret.clone().into_bytes()),
                    test_advertisement(),
                    Arc::new(ProcessFakeHost::default()),
                    Arc::new(
                        LinkResourceLocalizer::new(
                            NodeId("worker-process".into()),
                            Arc::from(secret.clone().into_bytes()),
                            destination,
                            16 * 1024 * 1024,
                            Duration::from_secs(2),
                        )
                        .expect("child resource localizer"),
                    ),
                    Duration::from_secs(2),
                )
                .expect("child Worker config")
                .run(),
            )
            .expect("child Worker run");
    }

    #[test]
    fn process_content_server_child() {
        let Ok(address) = std::env::var("MUTSUKI_TEST_CONTENT_ADDRESS") else {
            return;
        };
        let secret = std::env::var("MUTSUKI_TEST_CLUSTER_SECRET").expect("content secret");
        let path = PathBuf::from(
            std::env::var("MUTSUKI_TEST_CONTENT_SOURCE").expect("content source path"),
        );
        let digest = std::env::var("MUTSUKI_TEST_CONTENT_DIGEST").expect("content digest");
        let size = fs::metadata(&path).expect("content metadata").len();
        let runtime = tokio::runtime::Runtime::new().expect("content runtime");
        runtime
            .block_on(
                FileContentServer::new(
                    NodeId("content-origin".into()),
                    NodeId("worker-process".into()),
                    address,
                    Arc::from(secret.into_bytes()),
                    vec![FileContentSource {
                        content_id: ContentId::new("sha256", digest, size, "blob"),
                        path,
                    }],
                    Duration::from_secs(2),
                )
                .expect("content server config")
                .serve_once(),
            )
            .expect("content server run");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independent_process_worker_submits_queries_cancels_pulses_and_drains() {
        let unique = now_millis();
        let address = format!("mutsuki-distributed-issue20-{unique}");
        let content_address = format!("mutsuki-distributed-content-{unique}");
        let secret = format!("issue20-test-secret-at-least-thirty-two-bytes-{unique}");
        let temporary = tempfile::tempdir().expect("content tempdir");
        let source_path = temporary.path().join("source.bin");
        let destination = temporary.path().join("worker-content");
        let content_bytes = vec![0x5a; 2 * 1024 * 1024 + 17];
        fs::write(&source_path, &content_bytes).expect("write content source");
        let content_digest = format!("{:x}", Sha256::digest(&content_bytes));
        let content_id = ContentId::new(
            "sha256",
            content_digest.clone(),
            u64::try_from(content_bytes.len()).unwrap(),
            "blob",
        );
        let executable = std::env::current_exe().expect("current test executable");
        let mut child = Command::new(executable)
            .args([
                "--exact",
                "process::tests::process_worker_child",
                "--nocapture",
            ])
            .env("MUTSUKI_TEST_WORKER_ADDRESS", &address)
            .env("MUTSUKI_TEST_CLUSTER_SECRET", &secret)
            .env("MUTSUKI_TEST_CONTENT_DESTINATION", &destination)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn independent Worker process");

        let mut content_child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process::tests::process_content_server_child",
                "--nocapture",
            ])
            .env("MUTSUKI_TEST_CONTENT_ADDRESS", &content_address)
            .env("MUTSUKI_TEST_CLUSTER_SECRET", &secret)
            .env("MUTSUKI_TEST_CONTENT_SOURCE", &source_path)
            .env("MUTSUKI_TEST_CONTENT_DIGEST", &content_digest)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn independent content origin process");

        let deadline = Instant::now() + Duration::from_secs(5);
        let controller = Arc::new(loop {
            match ControllerProcess::connect(
                NodeId("controller-process".into()),
                Arc::new(ProcessFakeHost::default()),
                vec![WorkerConnectionConfig {
                    node_id: NodeId("worker-process".into()),
                    address: address.clone(),
                }],
                Arc::from(secret.clone().into_bytes()),
                16,
                Duration::from_secs(2),
            )
            .await
            {
                Ok(controller) => break controller,
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("connect independent Worker: {error:?}"),
            }
        });

        let management_address = format!("mutsuki-distributed-management-{unique}");
        let mut server = tokio::spawn(controller.clone().serve_management(
            NodeId("management-client".into()),
            management_address.clone(),
            Arc::from(secret.clone().into_bytes()),
            Duration::from_millis(20),
            Duration::from_secs(2),
        ));
        let capability_client = mutsuki_distributed_control_client::DistributedControlClient::new(
            NodeId("management-client".into()),
            management_address.clone(),
            Arc::from(secret.clone().into_bytes()),
            Duration::from_secs(2),
        )
        .expect("capability client config");
        let proof = loop {
            match capability_client.capabilities().await {
                Ok(proof) => break proof,
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("capability handshake: {error:?}"),
            }
        };
        assert_eq!(
            proof.distributed_host_revision,
            mutsuki_distributed_contracts::DISTRIBUTED_HOST_REVISION
        );
        assert_eq!(
            proof
                .feature_proof
                .get(&mutsuki_distributed_contracts::DistributedFeature::Clustered),
            Some(&mutsuki_distributed_contracts::CapabilityMaturity::Deployable)
        );
        server.abort();
        assert!(
            server
                .await
                .expect_err("aborted management server")
                .is_cancelled()
        );
        assert!(capability_client.health().await.is_err());
        server = tokio::spawn(controller.clone().serve_management(
            NodeId("management-client".into()),
            management_address.clone(),
            Arc::from(secret.clone().into_bytes()),
            Duration::from_millis(20),
            Duration::from_secs(2),
        ));
        let recovery_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match capability_client.capabilities().await {
                Ok(recovered) => {
                    assert_eq!(recovered, proof);
                    break;
                }
                Err(_) if Instant::now() < recovery_deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("recover capability session: {error:?}"),
            }
        }
        drop(capability_client);
        let deadline = Instant::now() + Duration::from_secs(5);
        let client = loop {
            let candidate = ControllerClient::new(
                NodeId("management-client".into()),
                management_address.clone(),
                Arc::from(secret.clone().into_bytes()),
                Duration::from_secs(2),
            )
            .expect("management client config");
            match candidate.health().await {
                Ok(health) => {
                    assert_eq!(health, "healthy");
                    break candidate;
                }
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("connect management client: {error:?}"),
            }
        };

        let global = mutsuki_distributed_contracts::GlobalTaskId("process-task".into());
        let placement = client
            .submit(
                global.clone(),
                test_portable(),
                RequirementSet::default(),
                vec![DirectDataRef {
                    owner_node: NodeId("content-origin".into()),
                    content_id: content_id.clone(),
                    endpoint_hint: format!("link-local://{content_address}"),
                }],
            )
            .await
            .expect("submit to child Worker");
        assert_eq!(placement.node_id, NodeId("worker-process".into()));
        assert_eq!(
            placement.kind,
            mutsuki_distributed_contracts::PlacementKind::Remote
        );
        assert_eq!(
            fs::read(destination.join(&content_id.digest)).expect("localized content"),
            content_bytes
        );
        assert_eq!(
            client
                .outcome(global.clone())
                .await
                .expect("query child Worker")
                .expect("child outcome")
                .output_ref
                .as_deref(),
            Some("content:child-process-result")
        );
        client
            .cancel(global)
            .await
            .expect("cancel child Worker task");
        assert!(controller.pulse_once().await.is_empty());

        client.shutdown().await.expect("shutdown controller");
        server
            .await
            .expect("management server task")
            .expect("management server");

        let status = child.wait().expect("wait for Worker process");
        assert!(status.success());
        let content_status = content_child
            .wait()
            .expect("wait for content origin process");
        assert!(content_status.success());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independent_process_link_loss_is_structured_and_marks_worker_unreachable() {
        let unique = now_millis();
        let address = format!("mutsuki-distributed-disconnect-{unique}");
        let secret = format!("issue20-disconnect-secret-at-least-thirty-two-{unique}");
        let destination = tempfile::tempdir().expect("disconnect tempdir");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process::tests::process_worker_child",
                "--nocapture",
            ])
            .env("MUTSUKI_TEST_WORKER_ADDRESS", &address)
            .env("MUTSUKI_TEST_CLUSTER_SECRET", &secret)
            .env("MUTSUKI_TEST_CONTENT_DESTINATION", destination.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn disconnect Worker");
        let deadline = Instant::now() + Duration::from_secs(5);
        let controller = loop {
            match ControllerProcess::connect(
                NodeId("controller-process".into()),
                Arc::new(ProcessFakeHost::default()),
                vec![WorkerConnectionConfig {
                    node_id: NodeId("worker-process".into()),
                    address: address.clone(),
                }],
                Arc::from(secret.clone().into_bytes()),
                4,
                Duration::from_secs(2),
            )
            .await
            {
                Ok(controller) => break controller,
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("connect disconnect Worker: {error:?}"),
            }
        };
        child.kill().expect("kill Worker process");
        child.wait().expect("reap killed Worker");
        assert_eq!(controller.pulse_once().await.len(), 1);
        assert_eq!(
            controller
                .registry
                .lock()
                .expect("Worker registry")
                .get(&NodeId("worker-process".into()))
                .expect("Worker snapshot")
                .health,
            WorkerHealth::Unreachable
        );
    }
}
