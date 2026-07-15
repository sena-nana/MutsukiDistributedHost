use mutsuki_distributed_contracts::{
    AcceptanceMode, AcceptanceReceipt, ConflictOutput, DistributedError, DistributedErrorKind,
    DurableAttempt, DurableTaskRecord, DurableTaskSpec, GlobalTaskId, GlobalTaskState,
    MetadataCommitProof, NodeId, StagedOutput,
};
use mutsuki_runtime_contracts::{ContentId, TaskHandle};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::ResourceCatalog;

const WAL_MAGIC: &[u8; 4] = b"MRW1";
const WAL_VERSION: u16 = 1;
const FRAME_FIXED_BYTES: usize = 4 + 2 + 1 + 1 + 8 + 4 + 32;
const DEFAULT_MAX_REGISTRY_RECORD_BYTES: usize = 64 * 1024;
const DEFAULT_COMPACT_AFTER_RECORDS: u64 = 16_384;
const DEFAULT_COMPACT_AFTER_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegistryOptions {
    pub max_tasks: usize,
    pub max_record_bytes: usize,
    pub compact_after_records: u64,
    pub compact_after_bytes: u64,
}

impl RegistryOptions {
    pub const fn for_max_tasks(max_tasks: usize) -> Self {
        Self {
            max_tasks,
            max_record_bytes: DEFAULT_MAX_REGISTRY_RECORD_BYTES,
            compact_after_records: DEFAULT_COMPACT_AFTER_RECORDS,
            compact_after_bytes: DEFAULT_COMPACT_AFTER_BYTES,
        }
    }

    fn validate(self) -> Result<Self, DistributedError> {
        if self.max_tasks == 0
            || self.max_record_bytes == 0
            || self.compact_after_records == 0
            || self.compact_after_bytes == 0
        {
            return Err(registry_config_error());
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataTransaction {
    pub transaction_id: String,
    pub log_index: u64,
    pub previous_version: u64,
    pub prepare_payload: Vec<u8>,
    pub prepare_frame: Vec<u8>,
    pub commit_frame: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaCommitAck {
    pub replica_id: String,
    pub transaction_id: String,
    pub log_index: u64,
}

pub trait MetadataReplica: Send + Sync {
    fn replica_id(&self) -> &str;
    fn commit(
        &self,
        transaction: &MetadataTransaction,
        sync: bool,
    ) -> Result<ReplicaCommitAck, DistributedError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaCommitOutcome {
    Committed(MetadataCommitProof),
    Failed(DistributedErrorKind),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaCommitAttempt {
    pub replica_id: String,
    pub log_index: u64,
    pub outcome: ReplicaCommitOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryCommitReport {
    pub transaction_id: String,
    pub log_index: u64,
    pub previous_version: u64,
    pub local_committed: bool,
    pub required_replica_acks: usize,
    pub replica_attempts: Vec<ReplicaCommitAttempt>,
    pub maintenance_error: Option<DistributedErrorKind>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryStats {
    pub last_log_index: u64,
    pub wal_transactions: u64,
    pub wal_bytes: u64,
    pub snapshot_path: PathBuf,
    pub recovered_tail_bytes: u64,
}

pub struct FileMetadataReplica {
    id: String,
    state: Mutex<FileReplicaState>,
}

struct FileReplicaState {
    wal: WalFile,
    committed: BTreeMap<u64, String>,
}

impl FileMetadataReplica {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DistributedError> {
        let path = path.as_ref().to_path_buf();
        ensure_parent(&path)?;
        let bytes = read_existing(&path)?;
        let (committed, truncate_at) =
            scan_transaction_ids(&bytes, DEFAULT_MAX_REGISTRY_RECORD_BYTES)?;
        let mut wal = WalFile::open(path.clone())?;
        if let Some(length) = truncate_at {
            wal.truncate(length)?;
        }
        Ok(Self {
            id: format!("file:{}", path.display()),
            state: Mutex::new(FileReplicaState { wal, committed }),
        })
    }
}

impl MetadataReplica for FileMetadataReplica {
    fn replica_id(&self) -> &str {
        &self.id
    }

    fn commit(
        &self,
        transaction: &MetadataTransaction,
        sync: bool,
    ) -> Result<ReplicaCommitAck, DistributedError> {
        let mut state = self.state.lock().expect("metadata replica mutex");
        if let Some(existing) = state.committed.get(&transaction.log_index) {
            if existing == &transaction.transaction_id {
                return Ok(ReplicaCommitAck {
                    replica_id: self.id.clone(),
                    transaction_id: existing.clone(),
                    log_index: transaction.log_index,
                });
            }
            return Err(replica_conflict_error(transaction.log_index));
        }
        state
            .wal
            .append_pair(&transaction.prepare_frame, &transaction.commit_frame, sync)?;
        state
            .committed
            .insert(transaction.log_index, transaction.transaction_id.clone());
        Ok(ReplicaCommitAck {
            replica_id: self.id.clone(),
            transaction_id: transaction.transaction_id.clone(),
            log_index: transaction.log_index,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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

impl RegistryMutation {
    fn task_id(&self) -> &GlobalTaskId {
        match self {
            Self::Submit(record) => &record.spec.global_task_id,
            Self::Assign { global_task_id, .. }
            | Self::Running { global_task_id, .. }
            | Self::StageOutput { global_task_id, .. }
            | Self::ConflictOutput { global_task_id, .. }
            | Self::CommitOutput { global_task_id, .. }
            | Self::SetFailure { global_task_id, .. }
            | Self::Cancel { global_task_id } => global_task_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PreparedPayload {
    transaction_id: String,
    log_index: u64,
    previous_transaction_id: String,
    global_task_id: GlobalTaskId,
    previous_version: u64,
    new_version: u64,
    mutation: RegistryMutation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RegistrySnapshot {
    format_version: u16,
    last_included_log_index: u64,
    last_transaction_id: String,
    records: BTreeMap<GlobalTaskId, DurableTaskRecord>,
    versions: BTreeMap<GlobalTaskId, u64>,
}

#[derive(Clone)]
struct PendingTransaction {
    metadata: MetadataTransaction,
    prepared: PreparedPayload,
    resulting_record: DurableTaskRecord,
    proofs: BTreeMap<String, MetadataCommitProof>,
    attempts: BTreeMap<String, ReplicaCommitOutcome>,
}

struct RegistryState {
    wal: WalFile,
    records: BTreeMap<GlobalTaskId, DurableTaskRecord>,
    versions: BTreeMap<GlobalTaskId, u64>,
    last_log_index: u64,
    last_transaction_id: String,
    wal_transactions: u64,
    recovered_tail_bytes: u64,
    pending: Option<PendingTransaction>,
    last_report: Option<RegistryCommitReport>,
}

pub struct PersistentRegistry {
    snapshot_path: PathBuf,
    state: Mutex<RegistryState>,
    replicas: Vec<Arc<dyn MetadataReplica>>,
    options: RegistryOptions,
    #[cfg(test)]
    fail_local_write_after: std::sync::atomic::AtomicUsize,
}

impl PersistentRegistry {
    pub fn open(
        path: impl AsRef<Path>,
        replicas: Vec<Arc<dyn MetadataReplica>>,
        max_tasks: usize,
    ) -> Result<Self, DistributedError> {
        Self::open_with_options(path, replicas, RegistryOptions::for_max_tasks(max_tasks))
    }

    pub fn open_with_options(
        path: impl AsRef<Path>,
        replicas: Vec<Arc<dyn MetadataReplica>>,
        options: RegistryOptions,
    ) -> Result<Self, DistributedError> {
        let options = options.validate()?;
        let path = path.as_ref().to_path_buf();
        ensure_parent(&path)?;
        validate_replica_ids(&replicas)?;
        let snapshot_path = snapshot_path_for(&path);
        let snapshot = load_snapshot(&snapshot_path, options.max_record_bytes)?;
        let bytes = read_existing(&path)?;
        let recovered = replay_wal(&bytes, snapshot, options)?;
        let mut wal = WalFile::open(path.clone())?;
        if let Some(length) = recovered.truncate_at {
            wal.truncate(length)?;
        }
        let recovered_tail_bytes = recovered.recovered_tail_bytes;
        Ok(Self {
            snapshot_path,
            state: Mutex::new(RegistryState {
                wal,
                records: recovered.records,
                versions: recovered.versions,
                last_log_index: recovered.last_log_index,
                last_transaction_id: recovered.last_transaction_id,
                wal_transactions: recovered.wal_transactions,
                recovered_tail_bytes,
                pending: recovered.pending,
                last_report: None,
            }),
            replicas,
            options,
            #[cfg(test)]
            fail_local_write_after: std::sync::atomic::AtomicUsize::new(usize::MAX),
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
                        && input_copies < usize::from(minimum_replicas))
                {
                    return Err(durability_unavailable());
                }
            }
        }
        let state_value = match spec.requested_acceptance {
            AcceptanceMode::Fast => GlobalTaskState::Submitted,
            AcceptanceMode::Durable | AcceptanceMode::Critical { .. } => GlobalTaskState::Persisted,
        };
        let acceptance = spec.requested_acceptance;
        let mutation = RegistryMutation::Submit(Box::new(DurableTaskRecord {
            spec: spec.clone(),
            state: state_value,
            acceptance,
            metadata_copies: required_copies,
            attempts: Vec::new(),
            staged_output: None,
            committed_output: None,
            conflicts: Vec::new(),
            failure: None,
        }));
        let proof = self.transact(
            mutation,
            required_copies,
            acceptance != AcceptanceMode::Fast,
        )?;
        Ok(AcceptanceReceipt {
            global_task_id: spec.global_task_id,
            requested: acceptance,
            actual: acceptance,
            state: state_value,
            metadata_copies: proof.replica_commits.len() + 1,
            input_copies,
            transaction_id: proof.transaction_id,
            log_index: proof.log_index,
            replica_commits: proof.replica_commits,
        })
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
        let mutation = RegistryMutation::Assign {
            global_task_id: global_task_id.clone(),
            attempt: DurableAttempt {
                attempt,
                node_id,
                local_handle,
                runner_generation,
                active: true,
            },
        };
        self.transact_for_existing(mutation).map(|_| ())
    }

    pub fn mark_running(
        &self,
        global_task_id: &GlobalTaskId,
        attempt: u32,
    ) -> Result<(), DistributedError> {
        self.transact_for_existing(RegistryMutation::Running {
            global_task_id: global_task_id.clone(),
            attempt,
        })
        .map(|_| ())
    }

    pub fn stage_output(
        &self,
        global_task_id: &GlobalTaskId,
        attempt: u32,
        worker_node: NodeId,
        content_id: ContentId,
        resources: &ResourceCatalog,
    ) -> Result<(), DistributedError> {
        let mut state = self
            .state
            .lock()
            .expect("persistent registry transaction mutex");
        let record = state
            .records
            .get(global_task_id)
            .ok_or_else(task_unknown)?
            .clone();
        let active = record.active_attempt().ok_or_else(invalid_transition)?;
        let (mutation, stale_output) = if active.attempt != attempt || active.node_id != worker_node
        {
            (
                RegistryMutation::ConflictOutput {
                    global_task_id: global_task_id.clone(),
                    output: ConflictOutput {
                        attempt,
                        worker_node,
                        content_id,
                        reason: "output belongs to a stale attempt".into(),
                    },
                },
                true,
            )
        } else {
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
            (
                RegistryMutation::StageOutput {
                    global_task_id: global_task_id.clone(),
                    output: StagedOutput {
                        attempt,
                        worker_node,
                        content_id,
                        verified_copies,
                    },
                },
                false,
            )
        };
        let required = record.acceptance.minimum_metadata_copies();
        self.transact_locked(
            &mut state,
            mutation,
            required,
            record.acceptance != AcceptanceMode::Fast,
        )?;
        if stale_output {
            return Err(DistributedError::new(
                DistributedErrorKind::AttemptStale,
                "stale attempt output was quarantined",
            ));
        }
        Ok(())
    }

    pub fn commit_output(
        &self,
        global_task_id: &GlobalTaskId,
        resources: &ResourceCatalog,
    ) -> Result<ContentId, DistributedError> {
        let mut state = self
            .state
            .lock()
            .expect("persistent registry transaction mutex");
        let record = state
            .records
            .get(global_task_id)
            .ok_or_else(task_unknown)?
            .clone();
        let staged = record
            .staged_output
            .clone()
            .ok_or_else(invalid_transition)?;
        if record.state != GlobalTaskState::OutputStaged
            || !resources.is_commit_ready(&staged.content_id, record.acceptance)
        {
            return Err(durability_unavailable());
        }
        self.transact_locked(
            &mut state,
            RegistryMutation::CommitOutput {
                global_task_id: global_task_id.clone(),
                content_id: staged.content_id.clone(),
            },
            record.acceptance.minimum_metadata_copies(),
            record.acceptance != AcceptanceMode::Fast,
        )?;
        Ok(staged.content_id)
    }

    pub fn fail(
        &self,
        global_task_id: &GlobalTaskId,
        reason: impl Into<String>,
        recovery_required: bool,
    ) -> Result<(), DistributedError> {
        self.transact_for_existing(RegistryMutation::SetFailure {
            global_task_id: global_task_id.clone(),
            state: if recovery_required {
                GlobalTaskState::RecoveryRequired
            } else {
                GlobalTaskState::Failed
            },
            reason: reason.into(),
        })
        .map(|_| ())
    }

    pub fn cancel(&self, global_task_id: &GlobalTaskId) -> Result<(), DistributedError> {
        self.transact_for_existing(RegistryMutation::Cancel {
            global_task_id: global_task_id.clone(),
        })
        .map(|_| ())
    }

    pub fn query(&self, global_task_id: &GlobalTaskId) -> Option<DurableTaskRecord> {
        self.state
            .lock()
            .expect("persistent registry transaction mutex")
            .records
            .get(global_task_id)
            .cloned()
    }

    pub fn last_commit_report(&self) -> Option<RegistryCommitReport> {
        self.state
            .lock()
            .expect("persistent registry transaction mutex")
            .last_report
            .clone()
    }

    pub fn stats(&self) -> Result<RegistryStats, DistributedError> {
        let state = self
            .state
            .lock()
            .expect("persistent registry transaction mutex");
        Ok(RegistryStats {
            last_log_index: state.last_log_index,
            wal_transactions: state.wal_transactions,
            wal_bytes: state.wal.len()?,
            snapshot_path: self.snapshot_path.clone(),
            recovered_tail_bytes: state.recovered_tail_bytes,
        })
    }

    pub fn compact(&self) -> Result<(), DistributedError> {
        let mut state = self
            .state
            .lock()
            .expect("persistent registry transaction mutex");
        if state.pending.is_some() {
            return Err(DistributedError::new(
                DistributedErrorKind::Conflict,
                "registry transaction is still prepared",
            ));
        }
        self.compact_locked(&mut state)
    }

    #[cfg(test)]
    pub(crate) fn inject_next_local_write_failure(&self) {
        self.inject_local_write_failure_after(0);
    }

    #[cfg(test)]
    pub(crate) fn inject_local_write_failure_after(&self, successful_writes: usize) {
        self.fail_local_write_after
            .store(successful_writes, std::sync::atomic::Ordering::Release);
    }

    fn transact_for_existing(
        &self,
        mutation: RegistryMutation,
    ) -> Result<CommitProof, DistributedError> {
        let mut state = self
            .state
            .lock()
            .expect("persistent registry transaction mutex");
        let record = state
            .records
            .get(mutation.task_id())
            .ok_or_else(task_unknown)?;
        let required = record.acceptance.minimum_metadata_copies();
        let sync = record.acceptance != AcceptanceMode::Fast;
        self.transact_locked(&mut state, mutation, required, sync)
    }

    fn transact(
        &self,
        mutation: RegistryMutation,
        required_copies: usize,
        sync: bool,
    ) -> Result<CommitProof, DistributedError> {
        let mut state = self
            .state
            .lock()
            .expect("persistent registry transaction mutex");
        self.transact_locked(&mut state, mutation, required_copies, sync)
    }

    fn transact_locked(
        &self,
        state: &mut RegistryState,
        mutation: RegistryMutation,
        required_copies: usize,
        sync: bool,
    ) -> Result<CommitProof, DistributedError> {
        let task_id = mutation.task_id().clone();
        let previous_version = state.versions.get(&task_id).copied().unwrap_or(0);
        let resulting_record = prepare_mutation(&state.records, &mutation, self.options.max_tasks)?;
        let mutation_bytes = serde_json::to_vec(&mutation).map_err(|_| registry_corrupt_error())?;
        if mutation_bytes.len() > self.options.max_record_bytes {
            return Err(DistributedError::new(
                DistributedErrorKind::CapacityExceeded,
                "registry control record exceeds the bounded limit",
            ));
        }

        let mut pending = self.prepare_pending_transaction(
            state,
            mutation,
            &mutation_bytes,
            resulting_record,
            previous_version,
            sync,
        )?;

        let required_replica_acks = required_copies.saturating_sub(1);
        self.replicate_transaction(&mut pending, required_replica_acks, sync);

        if pending.proofs.len() < required_replica_acks {
            state.last_report = Some(report_for(&pending, required_replica_acks, false, None));
            state.pending = Some(pending);
            return Err(durability_unavailable());
        }

        if let Err(error) = self.append_local(&mut state.wal, &pending.metadata.commit_frame, sync)
        {
            state.last_report = Some(report_for(&pending, required_replica_acks, false, None));
            state.pending = Some(pending);
            return Err(error);
        }

        state.records.insert(
            pending.prepared.global_task_id.clone(),
            pending.resulting_record.clone(),
        );
        state.versions.insert(
            pending.prepared.global_task_id.clone(),
            pending.prepared.new_version,
        );
        state.last_log_index = pending.metadata.log_index;
        state
            .last_transaction_id
            .clone_from(&pending.metadata.transaction_id);
        state.wal_transactions = state.wal_transactions.saturating_add(1);

        let maintenance_error = if self.compaction_due(state)? {
            self.compact_locked(state).err().map(|error| error.kind)
        } else {
            None
        };
        let report = report_for(&pending, required_replica_acks, true, maintenance_error);
        state.last_report = Some(report);
        let proof = CommitProof {
            transaction_id: pending.metadata.transaction_id,
            log_index: pending.metadata.log_index,
            replica_commits: pending.proofs.into_values().collect(),
        };
        state.pending = None;
        Ok(proof)
    }

    fn prepare_pending_transaction(
        &self,
        state: &mut RegistryState,
        mutation: RegistryMutation,
        mutation_bytes: &[u8],
        resulting_record: DurableTaskRecord,
        previous_version: u64,
        sync: bool,
    ) -> Result<PendingTransaction, DistributedError> {
        if let Some(existing) = state.pending.take() {
            if existing.prepared.mutation != mutation
                || existing.prepared.previous_version != previous_version
            {
                state.pending = Some(existing);
                return Err(DistributedError::new(
                    DistributedErrorKind::Conflict,
                    "another registry transaction is still prepared",
                ));
            }
            return Ok(existing);
        }
        let log_index = state
            .last_log_index
            .checked_add(1)
            .ok_or_else(registry_capacity_error)?;
        let transaction_id = transaction_id(
            &state.last_transaction_id,
            log_index,
            previous_version,
            mutation_bytes,
        );
        let prepared = PreparedPayload {
            transaction_id: transaction_id.clone(),
            log_index,
            previous_transaction_id: state.last_transaction_id.clone(),
            global_task_id: mutation.task_id().clone(),
            previous_version,
            new_version: previous_version
                .checked_add(1)
                .ok_or_else(registry_capacity_error)?,
            mutation,
        };
        let prepare_payload =
            serde_json::to_vec(&prepared).map_err(|_| registry_corrupt_error())?;
        if prepare_payload.len() > self.options.max_record_bytes {
            return Err(DistributedError::new(
                DistributedErrorKind::CapacityExceeded,
                "registry transaction exceeds the bounded limit",
            ));
        }
        let prepare_frame = encode_frame(FrameKind::Prepare, log_index, &prepare_payload)?;
        let commit_frame = encode_frame(FrameKind::Commit, log_index, transaction_id.as_bytes())?;
        self.append_local(&mut state.wal, &prepare_frame, sync)?;
        Ok(PendingTransaction {
            metadata: MetadataTransaction {
                transaction_id,
                log_index,
                previous_version,
                prepare_payload,
                prepare_frame,
                commit_frame,
            },
            prepared,
            resulting_record,
            proofs: BTreeMap::new(),
            attempts: BTreeMap::new(),
        })
    }

    fn replicate_transaction(
        &self,
        pending: &mut PendingTransaction,
        required_replica_acks: usize,
        sync: bool,
    ) {
        for replica in &self.replicas {
            if pending.proofs.len() >= required_replica_acks {
                break;
            }
            let replica_id = replica.replica_id().to_owned();
            if pending.proofs.contains_key(&replica_id) {
                continue;
            }
            match replica.commit(&pending.metadata, sync) {
                Ok(ack)
                    if ack.replica_id == replica_id
                        && ack.transaction_id == pending.metadata.transaction_id
                        && ack.log_index == pending.metadata.log_index =>
                {
                    let proof = MetadataCommitProof {
                        replica_id: ack.replica_id,
                        transaction_id: ack.transaction_id,
                        log_index: ack.log_index,
                    };
                    pending.attempts.insert(
                        replica_id.clone(),
                        ReplicaCommitOutcome::Committed(proof.clone()),
                    );
                    pending.proofs.insert(replica_id, proof);
                }
                Ok(_) => {
                    pending.attempts.insert(
                        replica_id,
                        ReplicaCommitOutcome::Failed(DistributedErrorKind::Corrupt),
                    );
                }
                Err(error) => {
                    pending
                        .attempts
                        .insert(replica_id, ReplicaCommitOutcome::Failed(error.kind));
                }
            }
        }
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn append_local(
        &self,
        wal: &mut WalFile,
        frame: &[u8],
        sync: bool,
    ) -> Result<(), DistributedError> {
        #[cfg(test)]
        {
            let remaining = self
                .fail_local_write_after
                .load(std::sync::atomic::Ordering::Acquire);
            if remaining != usize::MAX {
                if remaining == 0 {
                    self.fail_local_write_after
                        .store(usize::MAX, std::sync::atomic::Ordering::Release);
                    return Err(registry_storage_error());
                }
                self.fail_local_write_after
                    .store(remaining - 1, std::sync::atomic::Ordering::Release);
            }
        }
        wal.append(frame, sync)
    }

    fn compaction_due(&self, state: &RegistryState) -> Result<bool, DistributedError> {
        Ok(state.wal_transactions >= self.options.compact_after_records
            || state.wal.len()? >= self.options.compact_after_bytes)
    }

    fn compact_locked(&self, state: &mut RegistryState) -> Result<(), DistributedError> {
        let snapshot = RegistrySnapshot {
            format_version: WAL_VERSION,
            last_included_log_index: state.last_log_index,
            last_transaction_id: state.last_transaction_id.clone(),
            records: state.records.clone(),
            versions: state.versions.clone(),
        };
        persist_snapshot(
            &self.snapshot_path,
            &snapshot,
            self.options.max_record_bytes,
        )?;
        // The durable snapshot is already authoritative. Truncating the open
        // handle avoids Windows' prohibition on replacing an open file; a
        // crash before, during, or after this point replays either the old WAL
        // (skipping included indices) or the empty WAL from the snapshot.
        state.wal.truncate(0)?;
        state.wal_transactions = 0;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct CommitProof {
    transaction_id: String,
    log_index: u64,
    replica_commits: Vec<MetadataCommitProof>,
}

fn report_for(
    pending: &PendingTransaction,
    required_replica_acks: usize,
    local_committed: bool,
    maintenance_error: Option<DistributedErrorKind>,
) -> RegistryCommitReport {
    RegistryCommitReport {
        transaction_id: pending.metadata.transaction_id.clone(),
        log_index: pending.metadata.log_index,
        previous_version: pending.metadata.previous_version,
        local_committed,
        required_replica_acks,
        replica_attempts: pending
            .attempts
            .iter()
            .map(|(replica_id, outcome)| ReplicaCommitAttempt {
                replica_id: replica_id.clone(),
                log_index: pending.metadata.log_index,
                outcome: outcome.clone(),
            })
            .collect(),
        maintenance_error,
    }
}

fn prepare_mutation(
    records: &BTreeMap<GlobalTaskId, DurableTaskRecord>,
    mutation: &RegistryMutation,
    max_tasks: usize,
) -> Result<DurableTaskRecord, DistributedError> {
    if let RegistryMutation::Submit(record) = mutation {
        let expected_state = match record.acceptance {
            AcceptanceMode::Fast => GlobalTaskState::Submitted,
            AcceptanceMode::Durable | AcceptanceMode::Critical { .. } => GlobalTaskState::Persisted,
        };
        if records.len() >= max_tasks
            || records.contains_key(&record.spec.global_task_id)
            || record.state != expected_state
            || !record.attempts.is_empty()
            || record.staged_output.is_some()
            || record.committed_output.is_some()
        {
            return Err(if records.contains_key(&record.spec.global_task_id) {
                DistributedError::new(
                    DistributedErrorKind::Conflict,
                    "global task id already exists",
                )
            } else if records.len() >= max_tasks {
                registry_capacity_error()
            } else {
                invalid_transition()
            });
        }
        return Ok((**record).clone());
    }
    prepare_existing_mutation(records, mutation)
}

fn prepare_existing_mutation(
    records: &BTreeMap<GlobalTaskId, DurableTaskRecord>,
    mutation: &RegistryMutation,
) -> Result<DurableTaskRecord, DistributedError> {
    let mut record = records
        .get(mutation.task_id())
        .cloned()
        .ok_or_else(task_unknown)?;
    match mutation {
        RegistryMutation::Assign { attempt, .. } => {
            if record.state.is_terminal()
                || record.state == GlobalTaskState::OutputStaged
                || attempt.attempt == 0
                || attempt.runner_generation == 0
                || record
                    .attempts
                    .iter()
                    .any(|current| current.attempt >= attempt.attempt)
            {
                return Err(invalid_transition());
            }
            for previous in &mut record.attempts {
                previous.active = false;
            }
            record.attempts.push(attempt.clone());
            record.state = GlobalTaskState::Assigned;
        }
        RegistryMutation::Running { attempt, .. } => {
            if record.state != GlobalTaskState::Assigned
                || record.active_attempt().map(|active| active.attempt) != Some(*attempt)
            {
                return Err(invalid_transition());
            }
            record.state = GlobalTaskState::Running;
        }
        RegistryMutation::StageOutput { output, .. } => {
            let active = record.active_attempt().ok_or_else(invalid_transition)?;
            if !matches!(
                record.state,
                GlobalTaskState::Assigned | GlobalTaskState::Running
            ) || active.attempt != output.attempt
                || active.node_id != output.worker_node
            {
                return Err(invalid_transition());
            }
            record.staged_output = Some(output.clone());
            record.state = GlobalTaskState::OutputStaged;
        }
        RegistryMutation::ConflictOutput { output, .. } => {
            record.conflicts.push(output.clone());
        }
        RegistryMutation::CommitOutput { content_id, .. } => {
            if record.state != GlobalTaskState::OutputStaged
                || record
                    .staged_output
                    .as_ref()
                    .is_none_or(|staged| &staged.content_id != content_id)
            {
                return Err(invalid_transition());
            }
            record.committed_output = Some(content_id.clone());
            record.state = GlobalTaskState::Committed;
            deactivate_attempts(&mut record);
        }
        RegistryMutation::SetFailure { state, reason, .. } => {
            if record.state.is_terminal()
                || !matches!(
                    state,
                    GlobalTaskState::Failed | GlobalTaskState::RecoveryRequired
                )
            {
                return Err(invalid_transition());
            }
            record.state = *state;
            record.failure = Some(reason.clone());
            deactivate_attempts(&mut record);
        }
        RegistryMutation::Cancel { .. } => {
            if record.state.is_terminal() {
                return Err(invalid_transition());
            }
            record.state = GlobalTaskState::Cancelled;
            deactivate_attempts(&mut record);
        }
        RegistryMutation::Submit(_) => unreachable!(),
    }
    Ok(record)
}

fn deactivate_attempts(record: &mut DurableTaskRecord) {
    for attempt in &mut record.attempts {
        attempt.active = false;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum FrameKind {
    Prepare = 1,
    Commit = 2,
    Snapshot = 3,
}

impl TryFrom<u8> for FrameKind {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Prepare),
            2 => Ok(Self::Commit),
            3 => Ok(Self::Snapshot),
            _ => Err(()),
        }
    }
}

fn encode_frame(
    kind: FrameKind,
    log_index: u64,
    payload: &[u8],
) -> Result<Vec<u8>, DistributedError> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| registry_capacity_error())?;
    let mut protected = Vec::with_capacity(FRAME_FIXED_BYTES - 4 + payload.len());
    protected.extend_from_slice(WAL_MAGIC);
    protected.extend_from_slice(&WAL_VERSION.to_le_bytes());
    protected.push(kind as u8);
    protected.push(0);
    protected.extend_from_slice(&log_index.to_le_bytes());
    protected.extend_from_slice(&payload_len.to_le_bytes());
    protected.extend_from_slice(payload);
    let checksum = Sha256::digest(&protected);
    let body_len = protected
        .len()
        .checked_add(checksum.len())
        .ok_or_else(registry_capacity_error)?;
    let body_len = u32::try_from(body_len).map_err(|_| registry_capacity_error())?;
    let mut frame = Vec::with_capacity(4 + usize::try_from(body_len).unwrap_or(usize::MAX));
    frame.extend_from_slice(&body_len.to_le_bytes());
    frame.extend_from_slice(&protected);
    frame.extend_from_slice(&checksum);
    Ok(frame)
}

pub(crate) fn committed_frames_from_prepare_payload(
    prepare_payload: &[u8],
) -> Result<(u64, String, Vec<u8>, Vec<u8>), DistributedError> {
    let prepared: PreparedPayload =
        serde_json::from_slice(prepare_payload).map_err(|_| registry_corrupt_error())?;
    if transaction_id_for_prepared(&prepared)? != prepared.transaction_id {
        return Err(registry_corrupt_error());
    }
    let prepare = encode_frame(FrameKind::Prepare, prepared.log_index, prepare_payload)?;
    let commit = encode_frame(
        FrameKind::Commit,
        prepared.log_index,
        prepared.transaction_id.as_bytes(),
    )?;
    Ok((prepared.log_index, prepared.transaction_id, prepare, commit))
}

#[derive(Debug)]
struct DecodedFrame<'a> {
    kind: FrameKind,
    log_index: u64,
    payload: &'a [u8],
    end: usize,
}

fn decode_frame_at(
    bytes: &[u8],
    offset: usize,
    max_payload_bytes: usize,
) -> Result<Option<DecodedFrame<'_>>, DistributedError> {
    let remaining = &bytes[offset..];
    if remaining.is_empty() {
        return Ok(None);
    }
    if remaining.len() < 4 {
        return Ok(None);
    }
    let body_len = usize::try_from(u32::from_le_bytes(remaining[..4].try_into().unwrap()))
        .map_err(|_| registry_capacity_error())?;
    if remaining.len() < 4 + body_len {
        return Ok(None);
    }
    if body_len < FRAME_FIXED_BYTES - 4 {
        return Err(registry_corrupt_at(offset));
    }
    let body = &remaining[4..4 + body_len];
    let protected_len = body_len - 32;
    let (protected, checksum) = body.split_at(protected_len);
    if Sha256::digest(protected).as_slice() != checksum {
        return Err(registry_corrupt_at(offset));
    }
    if &protected[..4] != WAL_MAGIC || protected[4..6] != WAL_VERSION.to_le_bytes() {
        return Err(registry_corrupt_at(offset));
    }
    let kind = FrameKind::try_from(protected[6]).map_err(|()| registry_corrupt_at(offset))?;
    if protected[7] != 0 {
        return Err(registry_corrupt_at(offset));
    }
    let log_index = u64::from_le_bytes(protected[8..16].try_into().unwrap());
    let payload_len = usize::try_from(u32::from_le_bytes(protected[16..20].try_into().unwrap()))
        .map_err(|_| registry_capacity_error())?;
    if payload_len > max_payload_bytes || protected.len() != 20 + payload_len {
        return Err(registry_corrupt_at(offset));
    }
    Ok(Some(DecodedFrame {
        kind,
        log_index,
        payload: &protected[20..],
        end: offset + 4 + body_len,
    }))
}

struct RecoveredRegistry {
    records: BTreeMap<GlobalTaskId, DurableTaskRecord>,
    versions: BTreeMap<GlobalTaskId, u64>,
    last_log_index: u64,
    last_transaction_id: String,
    wal_transactions: u64,
    pending: Option<PendingTransaction>,
    truncate_at: Option<u64>,
    recovered_tail_bytes: u64,
}

fn replay_wal(
    bytes: &[u8],
    snapshot: Option<RegistrySnapshot>,
    options: RegistryOptions,
) -> Result<RecoveredRegistry, DistributedError> {
    if bytes.starts_with(b"{\"") {
        return Err(registry_corrupt_at(0).with_detail(
            "legacy newline JSON WAL is not a recoverable production framing format",
        ));
    }
    let snapshot = snapshot.unwrap_or_else(|| RegistrySnapshot {
        format_version: WAL_VERSION,
        last_included_log_index: 0,
        last_transaction_id: String::new(),
        records: BTreeMap::new(),
        versions: BTreeMap::new(),
    });
    if snapshot.format_version != WAL_VERSION || snapshot.records.len() > options.max_tasks {
        return Err(registry_corrupt_at(0));
    }
    let mut records = snapshot.records;
    let mut versions = snapshot.versions;
    let mut last_log_index = snapshot.last_included_log_index;
    let mut last_transaction_id = snapshot.last_transaction_id;
    let mut pending: BTreeMap<u64, (PreparedPayload, usize)> = BTreeMap::new();
    let mut offset = 0;
    let mut wal_transactions = 0_u64;
    while offset < bytes.len() {
        let Some(frame) = decode_frame_at(bytes, offset, options.max_record_bytes)? else {
            break;
        };
        if frame.kind == FrameKind::Snapshot {
            return Err(registry_corrupt_at(offset));
        }
        if frame.log_index <= snapshot.last_included_log_index {
            offset = frame.end;
            continue;
        }
        match frame.kind {
            FrameKind::Prepare => {
                let prepared: PreparedPayload = serde_json::from_slice(frame.payload)
                    .map_err(|_| registry_corrupt_at(offset))?;
                if prepared.log_index != frame.log_index {
                    return Err(registry_corrupt_at(offset));
                }
                if let Some((existing, _)) = pending.get(&frame.log_index) {
                    if existing.transaction_id != prepared.transaction_id {
                        return Err(registry_corrupt_at(offset));
                    }
                } else {
                    pending.insert(frame.log_index, (prepared, offset));
                }
            }
            FrameKind::Commit => {
                let transaction_id =
                    std::str::from_utf8(frame.payload).map_err(|_| registry_corrupt_at(offset))?;
                let Some((prepared, _)) = pending.remove(&frame.log_index) else {
                    return Err(registry_corrupt_at(offset));
                };
                if prepared.transaction_id != transaction_id
                    || prepared.previous_transaction_id != last_transaction_id
                    || prepared.log_index != last_log_index.saturating_add(1)
                    || prepared.previous_version
                        != versions.get(&prepared.global_task_id).copied().unwrap_or(0)
                    || prepared.new_version != prepared.previous_version.saturating_add(1)
                {
                    return Err(registry_corrupt_at(offset));
                }
                let expected_tx = transaction_id_for_prepared(&prepared)?;
                if expected_tx != prepared.transaction_id {
                    return Err(registry_corrupt_at(offset));
                }
                let resulting = prepare_mutation(&records, &prepared.mutation, options.max_tasks)
                    .map_err(|_| registry_corrupt_at(offset))?;
                records.insert(prepared.global_task_id.clone(), resulting);
                versions.insert(prepared.global_task_id, prepared.new_version);
                last_log_index = prepared.log_index;
                last_transaction_id = prepared.transaction_id;
                wal_transactions = wal_transactions.saturating_add(1);
            }
            FrameKind::Snapshot => unreachable!(),
        }
        offset = frame.end;
    }
    let recovered_pending = recover_pending_transaction(
        pending,
        &records,
        &versions,
        last_log_index,
        &last_transaction_id,
        options.max_tasks,
        offset,
    )?;
    let recovered_tail_bytes = bytes.len().saturating_sub(offset) as u64;
    let truncate_at = (offset < bytes.len()).then_some(offset as u64);
    Ok(RecoveredRegistry {
        records,
        versions,
        last_log_index,
        last_transaction_id,
        wal_transactions,
        pending: recovered_pending,
        truncate_at,
        recovered_tail_bytes,
    })
}

fn recover_pending_transaction(
    pending: BTreeMap<u64, (PreparedPayload, usize)>,
    records: &BTreeMap<GlobalTaskId, DurableTaskRecord>,
    versions: &BTreeMap<GlobalTaskId, u64>,
    last_log_index: u64,
    last_transaction_id: &str,
    max_tasks: usize,
    end_offset: usize,
) -> Result<Option<PendingTransaction>, DistributedError> {
    let recovered = match pending.len() {
        0 => None,
        1 => {
            let (_, (prepared, prepared_offset)) = pending.into_iter().next().unwrap();
            validate_prepared_tail(
                &prepared,
                records,
                versions,
                last_log_index,
                last_transaction_id,
                max_tasks,
                prepared_offset,
            )?;
            let prepare_payload =
                serde_json::to_vec(&prepared).map_err(|_| registry_corrupt_at(prepared_offset))?;
            let prepare_frame =
                encode_frame(FrameKind::Prepare, prepared.log_index, &prepare_payload)?;
            let commit_frame = encode_frame(
                FrameKind::Commit,
                prepared.log_index,
                prepared.transaction_id.as_bytes(),
            )?;
            let resulting_record = prepare_mutation(records, &prepared.mutation, max_tasks)
                .map_err(|_| registry_corrupt_at(prepared_offset))?;
            Some(PendingTransaction {
                metadata: MetadataTransaction {
                    transaction_id: prepared.transaction_id.clone(),
                    log_index: prepared.log_index,
                    previous_version: prepared.previous_version,
                    prepare_payload,
                    prepare_frame,
                    commit_frame,
                },
                prepared,
                resulting_record,
                proofs: BTreeMap::new(),
                attempts: BTreeMap::new(),
            })
        }
        _ => return Err(registry_corrupt_at(end_offset)),
    };
    Ok(recovered)
}

fn validate_prepared_tail(
    prepared: &PreparedPayload,
    records: &BTreeMap<GlobalTaskId, DurableTaskRecord>,
    versions: &BTreeMap<GlobalTaskId, u64>,
    last_log_index: u64,
    last_transaction_id: &str,
    max_tasks: usize,
    offset: usize,
) -> Result<(), DistributedError> {
    if prepared.previous_transaction_id != last_transaction_id
        || prepared.log_index != last_log_index.saturating_add(1)
        || prepared.previous_version != versions.get(&prepared.global_task_id).copied().unwrap_or(0)
        || prepared.new_version != prepared.previous_version.saturating_add(1)
        || transaction_id_for_prepared(prepared)? != prepared.transaction_id
        || prepare_mutation(records, &prepared.mutation, max_tasks).is_err()
    {
        return Err(registry_corrupt_at(offset));
    }
    Ok(())
}

fn scan_transaction_ids(
    bytes: &[u8],
    max_record_bytes: usize,
) -> Result<(BTreeMap<u64, String>, Option<u64>), DistributedError> {
    if bytes.starts_with(b"{\"") {
        return Err(registry_corrupt_at(0));
    }
    let mut committed = BTreeMap::new();
    let mut pending: BTreeMap<u64, (String, usize)> = BTreeMap::new();
    let mut offset = 0;
    while offset < bytes.len() {
        let Some(frame) = decode_frame_at(bytes, offset, max_record_bytes)? else {
            break;
        };
        match frame.kind {
            FrameKind::Prepare => {
                let prepared: PreparedPayload = serde_json::from_slice(frame.payload)
                    .map_err(|_| registry_corrupt_at(offset))?;
                if prepared.log_index != frame.log_index {
                    return Err(registry_corrupt_at(offset));
                }
                pending
                    .entry(frame.log_index)
                    .or_insert((prepared.transaction_id, offset));
            }
            FrameKind::Commit => {
                let transaction_id =
                    std::str::from_utf8(frame.payload).map_err(|_| registry_corrupt_at(offset))?;
                if committed
                    .get(&frame.log_index)
                    .is_some_and(|id| id == transaction_id)
                {
                    offset = frame.end;
                    continue;
                }
                let Some((prepared_id, _)) = pending.remove(&frame.log_index) else {
                    return Err(registry_corrupt_at(offset));
                };
                if prepared_id != transaction_id {
                    return Err(registry_corrupt_at(offset));
                }
                committed.insert(frame.log_index, transaction_id.to_owned());
            }
            FrameKind::Snapshot => return Err(registry_corrupt_at(offset)),
        }
        offset = frame.end;
    }
    let truncate_offset = pending
        .values()
        .map(|(_, prepared_offset)| *prepared_offset)
        .min()
        .unwrap_or(offset);
    Ok((
        committed,
        (truncate_offset < bytes.len()).then_some(truncate_offset as u64),
    ))
}

fn transaction_id(
    previous_transaction_id: &str,
    log_index: u64,
    previous_version: u64,
    mutation_bytes: &[u8],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(previous_transaction_id.as_bytes());
    hasher.update(log_index.to_le_bytes());
    hasher.update(previous_version.to_le_bytes());
    hasher.update(mutation_bytes);
    to_hex(&hasher.finalize())
}

fn transaction_id_for_prepared(prepared: &PreparedPayload) -> Result<String, DistributedError> {
    let mutation_bytes =
        serde_json::to_vec(&prepared.mutation).map_err(|_| registry_corrupt_error())?;
    Ok(transaction_id(
        &prepared.previous_transaction_id,
        prepared.log_index,
        prepared.previous_version,
        &mutation_bytes,
    ))
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn snapshot_path_for(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.snapshot", path.display()))
}

fn persist_snapshot(
    path: &Path,
    snapshot: &RegistrySnapshot,
    max_record_bytes: usize,
) -> Result<(), DistributedError> {
    let payload = serde_json::to_vec(snapshot).map_err(|_| registry_corrupt_error())?;
    let snapshot_limit = max_record_bytes
        .checked_mul(snapshot.records.len().max(1))
        .ok_or_else(registry_capacity_error)?;
    if payload.len() > snapshot_limit {
        return Err(registry_capacity_error());
    }
    let frame = encode_frame(
        FrameKind::Snapshot,
        snapshot.last_included_log_index,
        &payload,
    )?;
    let temp = PathBuf::from(format!("{}.tmp", path.display()));
    ensure_parent(path)?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)
        .map_err(|_| registry_storage_error())?;
    file.write_all(&frame)
        .and_then(|()| file.sync_all())
        .map_err(|_| registry_storage_error())?;
    fs::rename(&temp, path).map_err(|_| registry_storage_error())?;
    #[cfg(unix)]
    sync_parent(path)?;
    // Windows cannot open a directory through the ordinary File API used by
    // std, and FlushFileBuffers is specified for file handles rather than
    // directory metadata. The snapshot file itself was synced before the
    // atomic rename, so there is no supported parent-directory flush to add.
    Ok(())
}

fn load_snapshot(
    path: &Path,
    max_record_bytes: usize,
) -> Result<Option<RegistrySnapshot>, DistributedError> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).map_err(|_| registry_storage_error())?;
    let max_snapshot_bytes = bytes.len().max(max_record_bytes);
    let frame =
        decode_frame_at(&bytes, 0, max_snapshot_bytes)?.ok_or_else(|| registry_corrupt_at(0))?;
    if frame.kind != FrameKind::Snapshot || frame.end != bytes.len() {
        return Err(registry_corrupt_at(0));
    }
    let snapshot: RegistrySnapshot =
        serde_json::from_slice(frame.payload).map_err(|_| registry_corrupt_at(0))?;
    if snapshot.last_included_log_index != frame.log_index {
        return Err(registry_corrupt_at(0));
    }
    Ok(Some(snapshot))
}

struct WalFile {
    file: File,
}

impl WalFile {
    fn open(path: PathBuf) -> Result<Self, DistributedError> {
        ensure_parent(&path)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|_| registry_storage_error())?;
        Ok(Self { file })
    }

    fn append(&mut self, bytes: &[u8], sync: bool) -> Result<(), DistributedError> {
        let original = self.len()?;
        if self
            .file
            .seek(SeekFrom::End(0))
            .and_then(|_| self.file.write_all(bytes))
            .and_then(|()| if sync { self.file.sync_data() } else { Ok(()) })
            .is_err()
        {
            let _ = self.file.set_len(original);
            let _ = self.file.seek(SeekFrom::End(0));
            return Err(registry_storage_error());
        }
        Ok(())
    }

    fn append_pair(
        &mut self,
        first: &[u8],
        second: &[u8],
        sync: bool,
    ) -> Result<(), DistributedError> {
        let original = self.len()?;
        if self
            .file
            .seek(SeekFrom::End(0))
            .and_then(|_| self.file.write_all(first))
            .and_then(|()| self.file.write_all(second))
            .and_then(|()| if sync { self.file.sync_data() } else { Ok(()) })
            .is_err()
        {
            let _ = self.file.set_len(original);
            let _ = self.file.seek(SeekFrom::End(0));
            return Err(registry_storage_error());
        }
        Ok(())
    }

    fn len(&self) -> Result<u64, DistributedError> {
        self.file
            .metadata()
            .map(|metadata| metadata.len())
            .map_err(|_| registry_storage_error())
    }

    fn truncate(&mut self, length: u64) -> Result<(), DistributedError> {
        self.file
            .set_len(length)
            .and_then(|()| self.file.seek(SeekFrom::End(0)).map(|_| ()))
            .and_then(|()| self.file.sync_all())
            .map_err(|_| registry_storage_error())
    }
}

fn read_existing(path: &Path) -> Result<Vec<u8>, DistributedError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut file = File::open(path).map_err(|_| registry_storage_error())?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|_| registry_storage_error())?;
    Ok(bytes)
}

fn ensure_parent(path: &Path) -> Result<(), DistributedError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|_| registry_storage_error())?;
    }
    Ok(())
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), DistributedError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|_| registry_storage_error())
}

fn validate_replica_ids(replicas: &[Arc<dyn MetadataReplica>]) -> Result<(), DistributedError> {
    let mut ids = BTreeSet::new();
    for replica in replicas {
        if replica.replica_id().is_empty() || !ids.insert(replica.replica_id()) {
            return Err(registry_config_error());
        }
    }
    Ok(())
}

const fn registry_config_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::InvalidConfig,
        "persistent registry configuration is invalid",
    )
}

const fn registry_capacity_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::CapacityExceeded,
        "persistent registry capacity is exhausted",
    )
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
        "persistent registry data is corrupt",
    )
}

fn registry_corrupt_at(offset: usize) -> DistributedError {
    registry_corrupt_error().with_detail(format!("WAL corruption at byte offset {offset}"))
}

fn replica_conflict_error(log_index: u64) -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Conflict,
        "replica log index belongs to another transaction",
    )
    .with_detail(format!("conflicting replica log index {log_index}"))
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

const fn task_unknown() -> DistributedError {
    DistributedError::new(DistributedErrorKind::TaskUnknown, "global task is unknown")
}
