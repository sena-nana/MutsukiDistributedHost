use mutsuki_runtime_contracts::{ContentId, RetrySafety, SchemaIdentity};
use serde::{Deserialize, Serialize};

use crate::{GlobalTaskId, NodeId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryTier {
    Ephemeral,
    Restartable,
    Checkpointed,
    Mirrored,
    NonRecoverable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureConfirmation {
    Suspect,
    Dead,
    LeaseExpired,
    ExplicitSpeculative,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecoveryPolicy {
    pub tier: RecoveryTier,
    pub max_attempts: u32,
    pub base_backoff_ticks: u64,
    pub max_backoff_ticks: u64,
    pub deadline_tick: Option<u64>,
    pub allow_speculative: bool,
    pub minimum_quality: f64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointArtifactManifest {
    pub global_task_id: GlobalTaskId,
    pub source_attempt: u32,
    pub sequence: u64,
    pub checkpoint_schema: SchemaIdentity,
    pub task_schema: SchemaIdentity,
    pub plugin_generation: u64,
    pub input_content_id: ContentId,
    pub checkpoint_content_id: ContentId,
    pub baseline_content_id: ContentId,
    pub previous_content_id: Option<ContentId>,
    pub changed_chunks: Vec<u32>,
    pub complete_baseline: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecoveryTarget {
    pub node_id: NodeId,
    pub runner_generation: u64,
    pub plugin_generation: u64,
    pub quality: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum RecoveryAction {
    Wait,
    Fail {
        reason: String,
    },
    RecoveryRequired {
        reason: String,
    },
    Restart {
        attempt: u32,
        not_before_tick: u64,
        target: RecoveryTarget,
    },
    RestoreCheckpoint {
        attempt: u32,
        not_before_tick: u64,
        target: RecoveryTarget,
        checkpoint: Box<CheckpointArtifactManifest>,
    },
    PromoteStandby {
        node_id: NodeId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RealtimeSession {
    pub session_id: String,
    pub primary_node: NodeId,
    pub standby_node: Option<NodeId>,
    pub execution_variant: String,
    pub last_checkpoint: Option<CheckpointArtifactManifest>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct MigrationEstimate {
    pub future_benefit: f64,
    pub state_transfer_cost: f64,
    pub cold_start_cost: f64,
    pub interruption_cost: f64,
    pub failure_risk_cost: f64,
    pub safety_margin: f64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionMigrationDecision {
    Stay,
    MigrateWholeSession { target: NodeId },
    PromoteStandby { target: NodeId },
    Degrade { reason: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectRecoveryAction {
    RetryWithIdempotencyKey,
    VerifyExternalState,
    CompensateThenRetry,
    TransactionalOutbox,
    RecoveryRequired,
    RejectWhileQuorumLost,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EffectRecoveryCapability {
    pub retry_safety: RetrySafety,
    pub idempotency_key: Option<String>,
    pub external_verifier: bool,
    pub transactional_outbox: bool,
    pub compensation_hook: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MirrorBudget {
    pub max_sessions: usize,
    pub max_compute_units: u64,
    pub max_memory_bytes: u64,
    pub max_network_bytes: u64,
}
