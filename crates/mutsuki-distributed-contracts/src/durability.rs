use mutsuki_runtime_contracts::{ContentId, PortableTask, RequirementSet, TaskHandle};
use serde::{Deserialize, Serialize};

use crate::{GlobalTaskId, NodeId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum AcceptanceMode {
    Fast,
    Durable,
    Critical { minimum_replicas: u8 },
}

impl AcceptanceMode {
    pub const fn minimum_metadata_copies(self) -> usize {
        match self {
            Self::Fast => 1,
            Self::Durable => 2,
            Self::Critical { minimum_replicas } => minimum_replicas as usize,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GlobalTaskState {
    Submitted,
    Persisted,
    Assigned,
    Running,
    OutputStaged,
    Committed,
    Failed,
    Cancelled,
    RecoveryRequired,
}

impl GlobalTaskState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Committed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DurableTaskSpec {
    pub global_task_id: GlobalTaskId,
    pub portable: PortableTask,
    pub requirements: RequirementSet,
    /// Content descriptors only. Bytes are never embedded in registry records.
    pub required_inputs: Vec<ContentId>,
    pub requested_acceptance: AcceptanceMode,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DurableAttempt {
    pub attempt: u32,
    pub node_id: NodeId,
    pub local_handle: Option<TaskHandle>,
    pub runner_generation: u64,
    pub active: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StagedOutput {
    pub attempt: u32,
    pub worker_node: NodeId,
    pub content_id: ContentId,
    pub verified_copies: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConflictOutput {
    pub attempt: u32,
    pub worker_node: NodeId,
    pub content_id: ContentId,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DurableTaskRecord {
    pub spec: DurableTaskSpec,
    pub state: GlobalTaskState,
    pub acceptance: AcceptanceMode,
    pub metadata_copies: usize,
    pub attempts: Vec<DurableAttempt>,
    pub staged_output: Option<StagedOutput>,
    pub committed_output: Option<ContentId>,
    pub conflicts: Vec<ConflictOutput>,
    pub failure: Option<String>,
}

impl DurableTaskRecord {
    pub fn active_attempt(&self) -> Option<&DurableAttempt> {
        self.attempts.iter().rev().find(|attempt| attempt.active)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AcceptanceReceipt {
    pub global_task_id: GlobalTaskId,
    pub requested: AcceptanceMode,
    pub actual: AcceptanceMode,
    pub state: GlobalTaskState,
    pub metadata_copies: usize,
    pub input_copies: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "policy")]
pub enum ResourcePolicy {
    Ephemeral,
    Reconstructible,
    Replicated { minimum_replicas: u8 },
    ExternalDurable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChunkDescriptor {
    pub index: u32,
    pub digest: String,
    pub size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ContentManifest {
    pub content_id: ContentId,
    pub chunk_size: u64,
    pub chunks: Vec<ChunkDescriptor>,
    pub policy: ResourcePolicy,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum ReplicaTarget {
    Node(NodeId),
    External(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaHealth {
    Pending,
    Healthy,
    Damaged,
    Missing,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicaRecord {
    pub target: ReplicaTarget,
    pub health: ReplicaHealth,
    pub verified_at_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceCatalogRecord {
    pub manifest: ContentManifest,
    pub replicas: Vec<ReplicaRecord>,
    pub reference_count: u64,
    pub retain_until_epoch: u64,
    pub repair_required: bool,
}

impl ResourceCatalogRecord {
    pub fn healthy_copies(&self) -> usize {
        self.replicas
            .iter()
            .filter(|replica| replica.health == ReplicaHealth::Healthy)
            .count()
    }

    pub fn is_recoverable(&self) -> bool {
        match self.manifest.policy {
            ResourcePolicy::Ephemeral => false,
            ResourcePolicy::Reconstructible => true,
            ResourcePolicy::Replicated { minimum_replicas } => {
                self.healthy_copies() >= minimum_replicas as usize
            }
            ResourcePolicy::ExternalDurable => self.replicas.iter().any(|replica| {
                matches!(replica.target, ReplicaTarget::External(_))
                    && replica.health == ReplicaHealth::Healthy
            }),
        }
    }
}
