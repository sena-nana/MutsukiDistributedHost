use mutsuki_distributed_contracts::{
    ChunkDescriptor, ContentManifest, DistributedError, DistributedErrorKind, ResourcePolicy,
};
use mutsuki_runtime_contracts::ContentId;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const DEFAULT_MAX_CHUNKS: usize = 1_048_576;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataLane {
    Resource,
    Result,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataTransferBudget {
    pub max_concurrent: usize,
    pub max_queued_bytes: u64,
    pub max_chunk_bytes: usize,
}

pub struct TransferChunk {
    pub lane: DataLane,
    pub source: mutsuki_distributed_contracts::NodeId,
    pub target: mutsuki_distributed_contracts::NodeId,
    pub content_id: ContentId,
    pub index: u32,
    pub bytes: Vec<u8>,
}

pub struct DataTransferQueue {
    budget: DataTransferBudget,
    queued: VecDeque<TransferChunk>,
    queued_bytes: u64,
    in_flight: usize,
}

impl DataTransferQueue {
    pub fn new(budget: DataTransferBudget) -> Result<Self, DistributedError> {
        if budget.max_concurrent == 0 || budget.max_queued_bytes == 0 || budget.max_chunk_bytes == 0
        {
            return Err(DistributedError::new(
                DistributedErrorKind::InvalidConfig,
                "data transfer budget must be positive and bounded",
            ));
        }
        Ok(Self {
            budget,
            queued: VecDeque::new(),
            queued_bytes: 0,
            in_flight: 0,
        })
    }

    pub fn enqueue(&mut self, chunk: TransferChunk) -> Result<(), DistributedError> {
        let chunk_bytes = chunk.bytes.len();
        let next_bytes = self
            .queued_bytes
            .checked_add(chunk_bytes as u64)
            .ok_or_else(capacity_error)?;
        if chunk_bytes > self.budget.max_chunk_bytes || next_bytes > self.budget.max_queued_bytes {
            return Err(capacity_error());
        }
        self.queued.push_back(chunk);
        self.queued_bytes = next_bytes;
        Ok(())
    }

    pub fn start_next(&mut self) -> Option<TransferChunk> {
        if self.in_flight >= self.budget.max_concurrent {
            return None;
        }
        let chunk = self.queued.pop_front()?;
        self.queued_bytes = self.queued_bytes.saturating_sub(chunk.bytes.len() as u64);
        self.in_flight += 1;
        Some(chunk)
    }

    pub fn complete_one(&mut self) -> Result<(), DistributedError> {
        if self.in_flight == 0 {
            return Err(DistributedError::new(
                DistributedErrorKind::InvalidTransition,
                "no data transfer is in flight",
            ));
        }
        self.in_flight -= 1;
        Ok(())
    }

    pub const fn queued_bytes(&self) -> u64 {
        self.queued_bytes
    }

    pub const fn in_flight(&self) -> usize {
        self.in_flight
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UploadStats {
    pub uploaded_chunks: usize,
    pub reused_chunks: usize,
    pub bytes_uploaded: u64,
}

#[derive(Clone, Debug)]
pub struct ContentStore {
    root: PathBuf,
    chunk_size: usize,
    max_chunks: usize,
}

impl ContentStore {
    pub fn open(root: impl Into<PathBuf>, chunk_size: usize) -> Result<Self, DistributedError> {
        if chunk_size == 0 {
            return Err(invalid_manifest());
        }
        let store = Self {
            root: root.into(),
            chunk_size,
            max_chunks: DEFAULT_MAX_CHUNKS,
        };
        fs::create_dir_all(store.chunk_dir()).map_err(|_| storage_error())?;
        fs::create_dir_all(store.manifest_dir()).map_err(|_| storage_error())?;
        fs::create_dir_all(store.upload_dir()).map_err(|_| storage_error())?;
        Ok(store)
    }

    pub const fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub fn build_manifest(
        &self,
        bytes: &[u8],
        format: impl Into<String>,
        policy: ResourcePolicy,
    ) -> Result<ContentManifest, DistributedError> {
        let chunks: Vec<_> = bytes
            .chunks(self.chunk_size)
            .enumerate()
            .map(|(index, chunk)| ChunkDescriptor {
                index: u32::try_from(index).unwrap_or(u32::MAX),
                digest: sha256_hex(chunk),
                size: chunk.len() as u64,
            })
            .collect();
        if chunks.len() > self.max_chunks || chunks.iter().any(|chunk| chunk.index == u32::MAX) {
            return Err(capacity_error());
        }
        Ok(ContentManifest {
            content_id: ContentId::new("sha256", sha256_hex(bytes), bytes.len() as u64, format),
            chunk_size: self.chunk_size as u64,
            chunks,
            policy,
        })
    }

    pub fn begin_upload(&self, manifest: &ContentManifest) -> Result<Vec<u32>, DistributedError> {
        self.validate_manifest(manifest)?;
        if self.manifest_path(&manifest.content_id.digest).exists() {
            if self.missing_chunks(manifest)?.is_empty() {
                return Ok(Vec::new());
            }
            return Err(corrupt_content());
        }
        atomic_json_write(
            &self.upload_path(&manifest.content_id.digest),
            manifest,
            false,
        )?;
        self.missing_chunks(manifest)
    }

    pub fn missing_upload_bytes(
        &self,
        manifest: &ContentManifest,
    ) -> Result<u64, DistributedError> {
        self.validate_manifest(manifest)?;
        let missing: BTreeSet<_> = self.missing_chunks(manifest)?.into_iter().collect();
        Ok(manifest
            .chunks
            .iter()
            .filter(|chunk| missing.contains(&chunk.index))
            .map(|chunk| chunk.size)
            .sum())
    }

    pub fn write_chunk(
        &self,
        content_digest: &str,
        index: u32,
        bytes: &[u8],
    ) -> Result<bool, DistributedError> {
        validate_digest(content_digest)?;
        let manifest = self.pending_manifest(content_digest)?;
        let descriptor = manifest
            .chunks
            .get(index as usize)
            .filter(|chunk| chunk.index == index)
            .ok_or_else(invalid_manifest)?;
        if descriptor.size != bytes.len() as u64 || descriptor.digest != sha256_hex(bytes) {
            return Err(corrupt_content());
        }
        let path = self.chunk_path(&descriptor.digest);
        if path.exists() {
            verify_file(&path, descriptor)?;
            return Ok(false);
        }
        atomic_bytes_write(&path, bytes, false)?;
        Ok(true)
    }

    pub fn complete_upload(
        &self,
        content_digest: &str,
    ) -> Result<ContentManifest, DistributedError> {
        validate_digest(content_digest)?;
        let manifest = self.pending_manifest(content_digest)?;
        if !self.missing_chunks(&manifest)?.is_empty() {
            return Err(DistributedError::new(
                DistributedErrorKind::DurabilityUnavailable,
                "content upload is incomplete",
            ));
        }
        let mut hasher = Sha256::new();
        let mut total = 0_u64;
        for chunk in &manifest.chunks {
            let mut file =
                File::open(self.chunk_path(&chunk.digest)).map_err(|_| storage_error())?;
            let copied = std::io::copy(&mut file, &mut HashWriter(&mut hasher))
                .map_err(|_| storage_error())?;
            total = total.saturating_add(copied);
        }
        if total != manifest.content_id.size
            || hex_digest(hasher.finalize().as_slice()) != manifest.content_id.digest
        {
            return Err(corrupt_content());
        }
        atomic_json_write(&self.manifest_path(content_digest), &manifest, true)?;
        match fs::remove_file(self.upload_path(content_digest)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(storage_error()),
        }
        Ok(manifest)
    }

    pub fn put_bytes(
        &self,
        bytes: &[u8],
        format: impl Into<String>,
        policy: ResourcePolicy,
    ) -> Result<(ContentManifest, UploadStats), DistributedError> {
        let manifest = self.build_manifest(bytes, format, policy)?;
        if self.has_content(&manifest.content_id) {
            return Ok((
                manifest.clone(),
                UploadStats {
                    uploaded_chunks: 0,
                    reused_chunks: manifest.chunks.len(),
                    bytes_uploaded: 0,
                },
            ));
        }
        let missing: BTreeSet<_> = self.begin_upload(&manifest)?.into_iter().collect();
        let mut stats = UploadStats::default();
        for (index, chunk) in bytes.chunks(self.chunk_size).enumerate() {
            let index = u32::try_from(index).map_err(|_| capacity_error())?;
            if !missing.contains(&index) {
                stats.reused_chunks += 1;
                continue;
            }
            if self.write_chunk(&manifest.content_id.digest, index, chunk)? {
                stats.uploaded_chunks += 1;
                stats.bytes_uploaded = stats.bytes_uploaded.saturating_add(chunk.len() as u64);
            } else {
                stats.reused_chunks += 1;
            }
        }
        self.complete_upload(&manifest.content_id.digest)?;
        Ok((manifest, stats))
    }

    pub fn has_content(&self, content_id: &ContentId) -> bool {
        self.manifest_path(&content_id.digest).exists()
    }

    pub fn manifest(&self, content_id: &ContentId) -> Result<ContentManifest, DistributedError> {
        let manifest = self.committed_manifest(&content_id.digest)?;
        if manifest.content_id != *content_id {
            return Err(corrupt_content());
        }
        Ok(manifest)
    }

    pub fn read_content(&self, content_id: &ContentId) -> Result<Vec<u8>, DistributedError> {
        let manifest = self.committed_manifest(&content_id.digest)?;
        if manifest.content_id != *content_id {
            return Err(corrupt_content());
        }
        let capacity = usize::try_from(content_id.size).map_err(|_| capacity_error())?;
        let mut bytes = Vec::with_capacity(capacity);
        for chunk in &manifest.chunks {
            let path = self.chunk_path(&chunk.digest);
            verify_file(&path, chunk)?;
            File::open(path)
                .map_err(|_| storage_error())?
                .read_to_end(&mut bytes)
                .map_err(|_| storage_error())?;
        }
        if sha256_hex(&bytes) != content_id.digest {
            return Err(corrupt_content());
        }
        Ok(bytes)
    }

    pub fn remove_content(&self, content_id: &ContentId) -> Result<(), DistributedError> {
        let path = self.manifest_path(&content_id.digest);
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(storage_error()),
        }
        let referenced = self.referenced_chunks()?;
        for entry in fs::read_dir(self.chunk_dir()).map_err(|_| storage_error())? {
            let entry = entry.map_err(|_| storage_error())?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if !referenced.contains(&name) {
                fs::remove_file(entry.path()).map_err(|_| storage_error())?;
            }
        }
        Ok(())
    }

    fn missing_chunks(&self, manifest: &ContentManifest) -> Result<Vec<u32>, DistributedError> {
        let mut missing = Vec::new();
        for chunk in &manifest.chunks {
            let path = self.chunk_path(&chunk.digest);
            if path.exists() {
                verify_file(&path, chunk)?;
            } else {
                missing.push(chunk.index);
            }
        }
        Ok(missing)
    }

    fn validate_manifest(&self, manifest: &ContentManifest) -> Result<(), DistributedError> {
        validate_digest(&manifest.content_id.digest)?;
        if manifest.content_id.algorithm != "sha256"
            || manifest.chunk_size != self.chunk_size as u64
            || manifest.chunks.len() > self.max_chunks
        {
            return Err(invalid_manifest());
        }
        let mut total = 0_u64;
        for (index, chunk) in manifest.chunks.iter().enumerate() {
            validate_digest(&chunk.digest)?;
            if chunk.index as usize != index || chunk.size == 0 || chunk.size > manifest.chunk_size
            {
                return Err(invalid_manifest());
            }
            total = total.checked_add(chunk.size).ok_or_else(capacity_error)?;
        }
        if total != manifest.content_id.size
            || (manifest.content_id.size == 0 && !manifest.chunks.is_empty())
        {
            return Err(invalid_manifest());
        }
        Ok(())
    }

    fn pending_manifest(&self, digest: &str) -> Result<ContentManifest, DistributedError> {
        read_json(&self.upload_path(digest))
    }

    fn committed_manifest(&self, digest: &str) -> Result<ContentManifest, DistributedError> {
        read_json(&self.manifest_path(digest))
    }

    fn referenced_chunks(&self) -> Result<BTreeSet<String>, DistributedError> {
        let mut referenced = BTreeSet::new();
        for entry in fs::read_dir(self.manifest_dir()).map_err(|_| storage_error())? {
            let manifest: ContentManifest = read_json(&entry.map_err(|_| storage_error())?.path())?;
            referenced.extend(manifest.chunks.into_iter().map(|chunk| chunk.digest));
        }
        Ok(referenced)
    }

    fn chunk_dir(&self) -> PathBuf {
        self.root.join("chunks")
    }

    fn manifest_dir(&self) -> PathBuf {
        self.root.join("manifests")
    }

    fn upload_dir(&self) -> PathBuf {
        self.root.join("uploads")
    }

    fn chunk_path(&self, digest: &str) -> PathBuf {
        self.chunk_dir().join(digest)
    }

    fn manifest_path(&self, digest: &str) -> PathBuf {
        self.manifest_dir().join(format!("{digest}.json"))
    }

    fn upload_path(&self, digest: &str) -> PathBuf {
        self.upload_dir().join(format!("{digest}.json"))
    }
}

struct HashWriter<'a>(&'a mut Sha256);

impl Write for HashWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.update(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn atomic_json_write<T: serde::Serialize>(
    path: &Path,
    value: &T,
    sync: bool,
) -> Result<(), DistributedError> {
    let bytes = serde_json::to_vec(value).map_err(|_| corrupt_content())?;
    atomic_bytes_write(path, &bytes, sync)
}

fn atomic_bytes_write(path: &Path, bytes: &[u8], sync: bool) -> Result<(), DistributedError> {
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|_| storage_error())?;
    file.write_all(bytes).map_err(|_| storage_error())?;
    if sync {
        file.sync_all().map_err(|_| storage_error())?;
    }
    fs::rename(temporary, path).map_err(|_| storage_error())
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, DistributedError> {
    let bytes = fs::read(path).map_err(|_| storage_error())?;
    serde_json::from_slice(&bytes).map_err(|_| corrupt_content())
}

fn verify_file(path: &Path, descriptor: &ChunkDescriptor) -> Result<(), DistributedError> {
    let bytes = fs::read(path).map_err(|_| storage_error())?;
    if bytes.len() as u64 != descriptor.size || sha256_hex(&bytes) != descriptor.digest {
        return Err(corrupt_content());
    }
    Ok(())
}

fn validate_digest(digest: &str) -> Result<(), DistributedError> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid_manifest());
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_digest(Sha256::digest(bytes).as_slice())
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(value, "{byte:02x}");
    }
    value
}

const fn storage_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Storage,
        "content storage operation failed",
    )
}

const fn corrupt_content() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Corrupt,
        "content hash or manifest validation failed",
    )
}

const fn invalid_manifest() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Protocol,
        "content manifest is invalid",
    )
}

const fn capacity_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::CapacityExceeded,
        "content manifest exceeds bounded limits",
    )
}
