use crate::ContentStore;
use mutsuki_distributed_contracts::{
    AcceptanceMode, AcceptanceReceipt, ConflictOutput, DistributedError, DistributedErrorKind,
    DurableAttempt, DurableTaskRecord, DurableTaskSpec, GlobalTaskId, GlobalTaskState, NodeId,
    ReplicaHealth, ReplicaRecord, ReplicaTarget, ResourceCatalogRecord, ResourcePolicy,
    StagedOutput,
};
use mutsuki_runtime_contracts::{ContentId, TaskHandle};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const DEFAULT_MAX_REGISTRY_RECORD_BYTES: usize = 64 * 1024;

pub trait MetadataReplica: Send + Sync {
    fn append(&self, record: &[u8], sync: bool) -> Result<(), DistributedError>;
}

pub struct FileMetadataReplica {
    file: Mutex<File>,
}

impl FileMetadataReplica {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DistributedError> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent).map_err(|_| registry_storage_error())?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|_| registry_storage_error())?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }
}

impl MetadataReplica for FileMetadataReplica {
    fn append(&self, record: &[u8], sync: bool) -> Result<(), DistributedError> {
        let mut file = self.file.lock().expect("metadata replica file mutex");
        file.write_all(record)
            .and_then(|()| file.write_all(b"\n"))
            .map_err(|_| registry_storage_error())?;
        if sync {
            file.sync_data().map_err(|_| registry_storage_error())?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "mutation", content = "payload", rename_all = "snake_case")]
enum RegistryMutation {
    Submit(Box<DurableTaskRecord>),
    Assign {
        global_task_id: GlobalTaskId,
        attempt: DurableAttempt,
    },
    Running {
        global_task_id: GlobalTaskId,
        attempt: u32,
    },
    StageOutput {
        global_task_id: GlobalTaskId,
        output: StagedOutput,
    },
    ConflictOutput {
        global_task_id: GlobalTaskId,
        output: ConflictOutput,
    },
    CommitOutput {
        global_task_id: GlobalTaskId,
        content_id: ContentId,
    },
    SetFailure {
        global_task_id: GlobalTaskId,
        state: GlobalTaskState,
        reason: String,
    },
    Cancel {
        global_task_id: GlobalTaskId,
    },
}

pub struct PersistentRegistry {
    file: Mutex<File>,
    records: Mutex<BTreeMap<GlobalTaskId, DurableTaskRecord>>,
    replicas: Vec<Arc<dyn MetadataReplica>>,
    max_tasks: usize,
    max_record_bytes: usize,
}

impl PersistentRegistry {
    pub fn open(
        path: impl AsRef<Path>,
        replicas: Vec<Arc<dyn MetadataReplica>>,
        max_tasks: usize,
    ) -> Result<Self, DistributedError> {
        if max_tasks == 0 {
            return Err(registry_config_error());
        }
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent).map_err(|_| registry_storage_error())?;
        }
        let records = replay_registry(path.as_ref(), max_tasks)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|_| registry_storage_error())?;
        Ok(Self {
            file: Mutex::new(file),
            records: Mutex::new(records),
            replicas,
            max_tasks,
            max_record_bytes: DEFAULT_MAX_REGISTRY_RECORD_BYTES,
        })
    }

    pub fn submit(
        &self,
        spec: DurableTaskSpec,
        resources: &ResourceCatalog,
    ) -> Result<AcceptanceReceipt, DistributedError> {
        let required_copies = spec.requested_acceptance.minimum_metadata_copies();
        if required_copies == 0
            || matches!(
                spec.requested_acceptance,
                AcceptanceMode::Critical {
                    minimum_replicas: 0 | 1
                }
            )
            || required_copies > self.replicas.len().saturating_add(1)
        {
            return Err(durability_unavailable());
        }
        {
            let records = self.records.lock().expect("persistent registry mutex");
            if records.contains_key(&spec.global_task_id) {
                return Err(DistributedError::new(
                    DistributedErrorKind::Conflict,
                    "global task id already exists",
                ));
            }
            if records.len() >= self.max_tasks {
                return Err(registry_capacity_error());
            }
        }

        let input_copies = resources.minimum_healthy_copies(&spec.required_inputs)?;
        match spec.requested_acceptance {
            AcceptanceMode::Fast => {}
            AcceptanceMode::Durable => {
                if !resources.inputs_recoverable(&spec.required_inputs) {
                    return Err(durability_unavailable());
                }
            }
            AcceptanceMode::Critical { minimum_replicas } => {
                if !resources.inputs_recoverable(&spec.required_inputs)
                    || (!spec.required_inputs.is_empty()
                        && input_copies < minimum_replicas as usize)
                {
                    return Err(durability_unavailable());
                }
            }
        }

        let state = match spec.requested_acceptance {
            AcceptanceMode::Fast => GlobalTaskState::Submitted,
            AcceptanceMode::Durable | AcceptanceMode::Critical { .. } => GlobalTaskState::Persisted,
        };
        let receipt = AcceptanceReceipt {
            global_task_id: spec.global_task_id.clone(),
            requested: spec.requested_acceptance,
            actual: spec.requested_acceptance,
            state,
            metadata_copies: required_copies,
            input_copies,
        };
        self.append(
            RegistryMutation::Submit(Box::new(DurableTaskRecord {
                spec,
                state,
                acceptance: receipt.actual,
                metadata_copies: required_copies,
                attempts: Vec::new(),
                staged_output: None,
                committed_output: None,
                conflicts: Vec::new(),
                failure: None,
            })),
            required_copies,
            receipt.actual != AcceptanceMode::Fast,
        )?;
        Ok(receipt)
    }

    pub fn assign(
        &self,
        global_task_id: &GlobalTaskId,
        attempt: u32,
        node_id: NodeId,
        local_handle: Option<TaskHandle>,
        runner_generation: u64,
    ) -> Result<(), DistributedError> {
        if attempt == 0 || runner_generation == 0 {
            return Err(registry_config_error());
        }
        let record = self.require(global_task_id)?;
        if record.state.is_terminal()
            || record.state == GlobalTaskState::OutputStaged
            || record
                .attempts
                .iter()
                .any(|current| current.attempt >= attempt)
        {
            return Err(invalid_transition());
        }
        self.append_for_task(
            &record,
            RegistryMutation::Assign {
                global_task_id: global_task_id.clone(),
                attempt: DurableAttempt {
                    attempt,
                    node_id,
                    local_handle,
                    runner_generation,
                    active: true,
                },
            },
        )
    }

    pub fn mark_running(
        &self,
        global_task_id: &GlobalTaskId,
        attempt: u32,
    ) -> Result<(), DistributedError> {
        let record = self.require(global_task_id)?;
        if record.state != GlobalTaskState::Assigned
            || record.active_attempt().map(|active| active.attempt) != Some(attempt)
        {
            return Err(invalid_transition());
        }
        self.append_for_task(
            &record,
            RegistryMutation::Running {
                global_task_id: global_task_id.clone(),
                attempt,
            },
        )
    }

    pub fn stage_output(
        &self,
        global_task_id: &GlobalTaskId,
        attempt: u32,
        worker_node: NodeId,
        content_id: ContentId,
        resources: &ResourceCatalog,
    ) -> Result<(), DistributedError> {
        let record = self.require(global_task_id)?;
        let active = record.active_attempt().ok_or_else(invalid_transition)?;
        if active.attempt != attempt || active.node_id != worker_node {
            let conflict = ConflictOutput {
                attempt,
                worker_node,
                content_id,
                reason: "output belongs to a stale attempt".into(),
            };
            self.append_for_task(
                &record,
                RegistryMutation::ConflictOutput {
                    global_task_id: global_task_id.clone(),
                    output: conflict,
                },
            )?;
            return Err(DistributedError::new(
                DistributedErrorKind::AttemptStale,
                "stale attempt output was quarantined",
            ));
        }
        if !matches!(
            record.state,
            GlobalTaskState::Assigned | GlobalTaskState::Running
        ) {
            return Err(invalid_transition());
        }
        let verified_copies = resources
            .get(&content_id)
            .ok_or_else(durability_unavailable)?
            .healthy_copies();
        if !resources.is_commit_ready(&content_id, record.acceptance) {
            return Err(durability_unavailable());
        }
        self.append_for_task(
            &record,
            RegistryMutation::StageOutput {
                global_task_id: global_task_id.clone(),
                output: StagedOutput {
                    attempt,
                    worker_node,
                    content_id,
                    verified_copies,
                },
            },
        )
    }

    pub fn commit_output(
        &self,
        global_task_id: &GlobalTaskId,
        resources: &ResourceCatalog,
    ) -> Result<ContentId, DistributedError> {
        let record = self.require(global_task_id)?;
        let staged = record
            .staged_output
            .clone()
            .ok_or_else(invalid_transition)?;
        if record.state != GlobalTaskState::OutputStaged
            || !resources.is_commit_ready(&staged.content_id, record.acceptance)
        {
            return Err(durability_unavailable());
        }
        self.append_for_task(
            &record,
            RegistryMutation::CommitOutput {
                global_task_id: global_task_id.clone(),
                content_id: staged.content_id.clone(),
            },
        )?;
        Ok(staged.content_id)
    }

    pub fn fail(
        &self,
        global_task_id: &GlobalTaskId,
        reason: impl Into<String>,
        recovery_required: bool,
    ) -> Result<(), DistributedError> {
        let record = self.require(global_task_id)?;
        if record.state.is_terminal() {
            return Err(invalid_transition());
        }
        self.append_for_task(
            &record,
            RegistryMutation::SetFailure {
                global_task_id: global_task_id.clone(),
                state: if recovery_required {
                    GlobalTaskState::RecoveryRequired
                } else {
                    GlobalTaskState::Failed
                },
                reason: reason.into(),
            },
        )
    }

    pub fn cancel(&self, global_task_id: &GlobalTaskId) -> Result<(), DistributedError> {
        let record = self.require(global_task_id)?;
        if record.state.is_terminal() {
            return Err(invalid_transition());
        }
        self.append_for_task(
            &record,
            RegistryMutation::Cancel {
                global_task_id: global_task_id.clone(),
            },
        )
    }

    pub fn query(&self, global_task_id: &GlobalTaskId) -> Option<DurableTaskRecord> {
        self.records
            .lock()
            .expect("persistent registry mutex")
            .get(global_task_id)
            .cloned()
    }

    fn require(
        &self,
        global_task_id: &GlobalTaskId,
    ) -> Result<DurableTaskRecord, DistributedError> {
        self.query(global_task_id).ok_or_else(|| {
            DistributedError::new(DistributedErrorKind::TaskUnknown, "global task is unknown")
        })
    }

    fn append_for_task(
        &self,
        record: &DurableTaskRecord,
        mutation: RegistryMutation,
    ) -> Result<(), DistributedError> {
        self.append(
            mutation,
            record.acceptance.minimum_metadata_copies(),
            record.acceptance != AcceptanceMode::Fast,
        )
    }

    fn append(
        &self,
        mutation: RegistryMutation,
        required_copies: usize,
        sync: bool,
    ) -> Result<(), DistributedError> {
        let encoded = serde_json::to_vec(&mutation).map_err(|_| registry_corrupt_error())?;
        if encoded.len() > self.max_record_bytes {
            return Err(DistributedError::new(
                DistributedErrorKind::CapacityExceeded,
                "registry control record exceeds the bounded limit",
            ));
        }
        let required_replica_acks = required_copies.saturating_sub(1);
        let replica_acks = self
            .replicas
            .iter()
            .take(required_replica_acks)
            .filter(|replica| replica.append(&encoded, sync).is_ok())
            .count();
        if replica_acks < required_replica_acks {
            return Err(durability_unavailable());
        }
        {
            let mut file = self.file.lock().expect("persistent registry file mutex");
            file.write_all(&encoded)
                .and_then(|()| file.write_all(b"\n"))
                .map_err(|_| registry_storage_error())?;
            if sync {
                file.sync_data().map_err(|_| registry_storage_error())?;
            }
        }
        apply_mutation(
            &mut self.records.lock().expect("persistent registry mutex"),
            mutation,
            self.max_tasks,
        )
    }
}

fn replay_registry(
    path: &Path,
    max_tasks: usize,
) -> Result<BTreeMap<GlobalTaskId, DurableTaskRecord>, DistributedError> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let file = File::open(path).map_err(|_| registry_storage_error())?;
    let mut records = BTreeMap::new();
    for line in BufReader::new(file).lines() {
        let line = line.map_err(|_| registry_storage_error())?;
        if line.is_empty() {
            continue;
        }
        if line.len() > DEFAULT_MAX_REGISTRY_RECORD_BYTES {
            return Err(registry_corrupt_error());
        }
        let mutation: RegistryMutation =
            serde_json::from_str(&line).map_err(|_| registry_corrupt_error())?;
        apply_mutation(&mut records, mutation, max_tasks)?;
    }
    Ok(records)
}

#[allow(clippy::too_many_lines)]
fn apply_mutation(
    records: &mut BTreeMap<GlobalTaskId, DurableTaskRecord>,
    mutation: RegistryMutation,
    max_tasks: usize,
) -> Result<(), DistributedError> {
    match mutation {
        RegistryMutation::Submit(record) => {
            let expected_state = match record.acceptance {
                AcceptanceMode::Fast => GlobalTaskState::Submitted,
                AcceptanceMode::Durable | AcceptanceMode::Critical { .. } => {
                    GlobalTaskState::Persisted
                }
            };
            if records.len() >= max_tasks
                || records.contains_key(&record.spec.global_task_id)
                || record.state != expected_state
                || !record.attempts.is_empty()
                || record.staged_output.is_some()
                || record.committed_output.is_some()
            {
                return Err(registry_corrupt_error());
            }
            records.insert(record.spec.global_task_id.clone(), *record);
        }
        RegistryMutation::Assign {
            global_task_id,
            attempt,
        } => {
            let record = records
                .get_mut(&global_task_id)
                .ok_or_else(registry_corrupt_error)?;
            if record.state.is_terminal()
                || record.state == GlobalTaskState::OutputStaged
                || attempt.attempt == 0
                || attempt.runner_generation == 0
                || record
                    .attempts
                    .iter()
                    .any(|current| current.attempt >= attempt.attempt)
            {
                return Err(registry_corrupt_error());
            }
            for previous in &mut record.attempts {
                previous.active = false;
            }
            record.attempts.push(attempt);
            record.state = GlobalTaskState::Assigned;
        }
        RegistryMutation::Running {
            global_task_id,
            attempt,
        } => {
            let record = records
                .get_mut(&global_task_id)
                .ok_or_else(registry_corrupt_error)?;
            if record.state != GlobalTaskState::Assigned
                || record.active_attempt().map(|active| active.attempt) != Some(attempt)
            {
                return Err(registry_corrupt_error());
            }
            record.state = GlobalTaskState::Running;
        }
        RegistryMutation::StageOutput {
            global_task_id,
            output,
        } => {
            let record = records
                .get_mut(&global_task_id)
                .ok_or_else(registry_corrupt_error)?;
            let active = record.active_attempt().ok_or_else(registry_corrupt_error)?;
            if !matches!(
                record.state,
                GlobalTaskState::Assigned | GlobalTaskState::Running
            ) || active.attempt != output.attempt
                || active.node_id != output.worker_node
            {
                return Err(registry_corrupt_error());
            }
            record.staged_output = Some(output);
            record.state = GlobalTaskState::OutputStaged;
        }
        RegistryMutation::ConflictOutput {
            global_task_id,
            output,
        } => records
            .get_mut(&global_task_id)
            .ok_or_else(registry_corrupt_error)?
            .conflicts
            .push(output),
        RegistryMutation::CommitOutput {
            global_task_id,
            content_id,
        } => {
            let record = records
                .get_mut(&global_task_id)
                .ok_or_else(registry_corrupt_error)?;
            if record.state != GlobalTaskState::OutputStaged
                || record
                    .staged_output
                    .as_ref()
                    .is_none_or(|staged| staged.content_id != content_id)
            {
                return Err(registry_corrupt_error());
            }
            record.committed_output = Some(content_id);
            record.state = GlobalTaskState::Committed;
            for attempt in &mut record.attempts {
                attempt.active = false;
            }
        }
        RegistryMutation::SetFailure {
            global_task_id,
            state,
            reason,
        } => {
            let record = records
                .get_mut(&global_task_id)
                .ok_or_else(registry_corrupt_error)?;
            if record.state.is_terminal()
                || !matches!(
                    state,
                    GlobalTaskState::Failed | GlobalTaskState::RecoveryRequired
                )
            {
                return Err(registry_corrupt_error());
            }
            record.state = state;
            record.failure = Some(reason);
            for attempt in &mut record.attempts {
                attempt.active = false;
            }
        }
        RegistryMutation::Cancel { global_task_id } => {
            let record = records
                .get_mut(&global_task_id)
                .ok_or_else(registry_corrupt_error)?;
            if record.state.is_terminal() {
                return Err(registry_corrupt_error());
            }
            record.state = GlobalTaskState::Cancelled;
            for attempt in &mut record.attempts {
                attempt.active = false;
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairJob {
    pub content_id: ContentId,
    pub missing_copies: usize,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CatalogSnapshot {
    records: BTreeMap<String, ResourceCatalogRecord>,
}

pub struct ResourceCatalog {
    path: PathBuf,
    records: BTreeMap<String, ResourceCatalogRecord>,
    max_records: usize,
    max_repair_jobs: usize,
}

impl ResourceCatalog {
    pub fn open(
        path: impl Into<PathBuf>,
        max_records: usize,
        max_repair_jobs: usize,
    ) -> Result<Self, DistributedError> {
        if max_records == 0 || max_repair_jobs == 0 {
            return Err(registry_config_error());
        }
        let path = path.into();
        let records = if path.exists() {
            let bytes = fs::read(&path).map_err(|_| registry_storage_error())?;
            let snapshot: CatalogSnapshot =
                serde_json::from_slice(&bytes).map_err(|_| registry_corrupt_error())?;
            snapshot.records
        } else {
            BTreeMap::new()
        };
        if records.len() > max_records {
            return Err(registry_capacity_error());
        }
        Ok(Self {
            path,
            records,
            max_records,
            max_repair_jobs,
        })
    }

    pub fn register(
        &mut self,
        manifest: mutsuki_distributed_contracts::ContentManifest,
        replicas: Vec<ReplicaRecord>,
        reference_count: u64,
        retain_until_epoch: u64,
    ) -> Result<(), DistributedError> {
        if !self.records.contains_key(&manifest.content_id.digest)
            && self.records.len() >= self.max_records
        {
            return Err(registry_capacity_error());
        }
        let mut record = ResourceCatalogRecord {
            manifest,
            replicas,
            reference_count,
            retain_until_epoch,
            repair_required: false,
        };
        record.repair_required = needs_repair(&record);
        self.records
            .insert(record.manifest.content_id.digest.clone(), record);
        self.persist()
    }

    pub fn record_replica(
        &mut self,
        content_id: &ContentId,
        target: ReplicaTarget,
        health: ReplicaHealth,
        verified_at_epoch: u64,
    ) -> Result<(), DistributedError> {
        let record = self
            .records
            .get_mut(&content_id.digest)
            .ok_or_else(durability_unavailable)?;
        if let Some(replica) = record
            .replicas
            .iter_mut()
            .find(|replica| replica.target == target)
        {
            replica.health = health;
            replica.verified_at_epoch = verified_at_epoch;
        } else {
            record.replicas.push(ReplicaRecord {
                target,
                health,
                verified_at_epoch,
            });
        }
        record.repair_required = needs_repair(record);
        self.persist()
    }

    pub fn add_reference(&mut self, content_id: &ContentId) -> Result<(), DistributedError> {
        let record = self
            .records
            .get_mut(&content_id.digest)
            .ok_or_else(durability_unavailable)?;
        record.reference_count = record
            .reference_count
            .checked_add(1)
            .ok_or_else(registry_capacity_error)?;
        self.persist()
    }

    pub fn release_reference(
        &mut self,
        content_id: &ContentId,
        retain_until_epoch: u64,
    ) -> Result<(), DistributedError> {
        let record = self
            .records
            .get_mut(&content_id.digest)
            .ok_or_else(durability_unavailable)?;
        record.reference_count = record.reference_count.saturating_sub(1);
        record.retain_until_epoch = record.retain_until_epoch.max(retain_until_epoch);
        self.persist()
    }

    pub fn get(&self, content_id: &ContentId) -> Option<&ResourceCatalogRecord> {
        self.records.get(&content_id.digest)
    }

    pub fn inputs_recoverable(&self, inputs: &[ContentId]) -> bool {
        inputs.iter().all(|input| {
            self.get(input)
                .is_some_and(ResourceCatalogRecord::is_recoverable)
        })
    }

    pub fn minimum_healthy_copies(&self, inputs: &[ContentId]) -> Result<usize, DistributedError> {
        if inputs.is_empty() {
            return Ok(0);
        }
        let mut minimum = usize::MAX;
        for input in inputs {
            minimum = minimum.min(
                self.get(input)
                    .map(ResourceCatalogRecord::healthy_copies)
                    .ok_or_else(durability_unavailable)?,
            );
        }
        Ok(minimum)
    }

    pub fn is_commit_ready(&self, content_id: &ContentId, mode: AcceptanceMode) -> bool {
        let Some(record) = self.get(content_id) else {
            return false;
        };
        match mode {
            AcceptanceMode::Fast => record.healthy_copies() >= 1,
            AcceptanceMode::Durable => record.is_recoverable() && record.healthy_copies() >= 1,
            AcceptanceMode::Critical { minimum_replicas } => {
                record.is_recoverable() && record.healthy_copies() >= minimum_replicas as usize
            }
        }
    }

    pub fn plan_repairs(&self, max_bytes: u64) -> Vec<RepairJob> {
        let mut bytes = 0_u64;
        self.records
            .values()
            .filter(|record| record.repair_required)
            .filter_map(|record| {
                let missing_copies = missing_replica_count(record);
                let estimated = record
                    .manifest
                    .content_id
                    .size
                    .saturating_mul(missing_copies as u64);
                if missing_copies == 0
                    || bytes.saturating_add(estimated) > max_bytes
                    || estimated == 0
                {
                    return None;
                }
                bytes = bytes.saturating_add(estimated);
                Some(RepairJob {
                    content_id: record.manifest.content_id.clone(),
                    missing_copies,
                    estimated_bytes: estimated,
                })
            })
            .take(self.max_repair_jobs)
            .collect()
    }

    pub fn collect_garbage(
        &mut self,
        now_epoch: u64,
        store: &ContentStore,
        max_items: usize,
    ) -> Result<Vec<ContentId>, DistributedError> {
        let candidates: Vec<_> = self
            .records
            .values()
            .filter(|record| record.reference_count == 0 && record.retain_until_epoch <= now_epoch)
            .take(max_items)
            .map(|record| record.manifest.content_id.clone())
            .collect();
        for content_id in &candidates {
            store.remove_content(content_id)?;
            self.records.remove(&content_id.digest);
        }
        if !candidates.is_empty() {
            self.persist()?;
        }
        Ok(candidates)
    }

    fn persist(&self) -> Result<(), DistributedError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|_| registry_storage_error())?;
        }
        let temporary = self.path.with_extension("tmp");
        let bytes = serde_json::to_vec(&CatalogSnapshot {
            records: self.records.clone(),
        })
        .map_err(|_| registry_corrupt_error())?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|_| registry_storage_error())?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| registry_storage_error())?;
        fs::rename(temporary, &self.path).map_err(|_| registry_storage_error())
    }
}

fn needs_repair(record: &ResourceCatalogRecord) -> bool {
    match record.manifest.policy {
        ResourcePolicy::Ephemeral | ResourcePolicy::Reconstructible => false,
        ResourcePolicy::Replicated { minimum_replicas } => {
            record.healthy_copies() < minimum_replicas as usize
        }
        ResourcePolicy::ExternalDurable => !record.replicas.iter().any(|replica| {
            matches!(replica.target, ReplicaTarget::External(_))
                && replica.health == ReplicaHealth::Healthy
        }),
    }
}

fn required_replica_count(record: &ResourceCatalogRecord) -> usize {
    match record.manifest.policy {
        ResourcePolicy::Ephemeral | ResourcePolicy::Reconstructible => 0,
        ResourcePolicy::Replicated { minimum_replicas } => minimum_replicas as usize,
        ResourcePolicy::ExternalDurable => 1,
    }
}

fn missing_replica_count(record: &ResourceCatalogRecord) -> usize {
    match record.manifest.policy {
        ResourcePolicy::ExternalDurable => usize::from(!record.replicas.iter().any(|replica| {
            matches!(replica.target, ReplicaTarget::External(_))
                && replica.health == ReplicaHealth::Healthy
        })),
        _ => required_replica_count(record).saturating_sub(record.healthy_copies()),
    }
}

const fn registry_storage_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Storage,
        "persistent registry storage operation failed",
    )
}

const fn registry_corrupt_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Corrupt,
        "persistent registry log is corrupt",
    )
}

const fn registry_config_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::InvalidConfig,
        "persistent registry limits or generations are invalid",
    )
}

const fn registry_capacity_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::CapacityExceeded,
        "persistent registry capacity exceeded",
    )
}

const fn durability_unavailable() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::DurabilityUnavailable,
        "requested durability cannot be proven",
    )
}

const fn invalid_transition() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::InvalidTransition,
        "global task state transition is invalid",
    )
}
