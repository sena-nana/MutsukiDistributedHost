use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::{IdentityStatus, MemberHealth, NodeId};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyClass {
    ClusterSafety,
    HardRealtime,
    SoftRealtime,
    Interactive,
    Batch,
    Background,
}

impl LatencyClass {
    pub const fn rank(self) -> u8 {
        match self {
            Self::ClusterSafety => 6,
            Self::HardRealtime => 5,
            Self::SoftRealtime => 4,
            Self::Interactive => 3,
            Self::Batch => 2,
            Self::Background => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkOrigin {
    Local,
    Remote,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LexicographicPriority {
    pub safety_critical: bool,
    pub recovery_critical: bool,
    pub latency_class: LatencyClass,
    pub deadline_risk: u8,
    pub dag_criticality: u8,
    pub unlock_value: u8,
    pub business_priority: i16,
    pub age_ticks: u64,
    pub fair_share_credit: i64,
    pub origin: WorkOrigin,
}

impl LexicographicPriority {
    pub const fn scheduling_key(self) -> (u8, u8, u8, u8, u8, u8, u8, i16, u64, i64) {
        (
            self.safety_critical as u8,
            self.recovery_critical as u8,
            self.latency_class.rank(),
            matches!(self.origin, WorkOrigin::Local) as u8,
            self.deadline_risk,
            self.dag_criticality,
            self.unlock_value,
            self.business_priority,
            self.age_ticks,
            self.fair_share_credit,
        )
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CapabilityBits(pub u128);

impl CapabilityBits {
    pub const CPU: Self = Self(1 << 0);
    pub const AVX2: Self = Self(1 << 1);
    pub const CUDA: Self = Self(1 << 2);
    pub const METAL: Self = Self(1 << 3);
    pub const VULKAN: Self = Self(1 << 4);
    pub const FP16: Self = Self(1 << 5);
    pub const BF16: Self = Self(1 << 6);
    pub const TRUSTED_EXECUTION: Self = Self(1 << 7);

    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn contains(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlacementFlags(pub u8);

impl PlacementFlags {
    pub const LOCAL_ONLY: Self = Self(1 << 0);
    pub const FRAME_BOUND: Self = Self(1 << 1);
    pub const LOCAL_DEVICE_BOUND: Self = Self(1 << 2);

    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn contains(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualityPolicy {
    Exact,
    AllowDegraded,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EndToEndCost {
    pub queue: f64,
    pub rtt: f64,
    pub input_transfer: f64,
    pub prewarm: f64,
    pub execution: f64,
    pub output_transfer: f64,
    pub commit: f64,
    pub jitter: f64,
    pub recovery: f64,
    pub energy: f64,
    pub ttft: Option<f64>,
    pub steady_latency: Option<f64>,
}

impl EndToEndCost {
    pub fn total(&self) -> f64 {
        self.queue
            + self.rtt
            + self.input_transfer
            + self.prewarm
            + self.execution
            + self.output_transfer
            + self.commit
            + self.jitter
            + self.recovery
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionVariant {
    pub variant_id: String,
    pub required_capabilities: CapabilityBits,
    pub runner_id: String,
    pub runner_generation: u64,
    pub plugin_id: String,
    pub plugin_generation: u64,
    pub quality: f64,
    pub peak_memory_bytes: u64,
    pub peak_vram_bytes: u64,
    pub failure_probability: f64,
    pub base_cost: EndToEndCost,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SchedulingNodeSnapshot {
    pub node_id: NodeId,
    pub capability_version: u64,
    pub resource_version: u64,
    pub capabilities: CapabilityBits,
    pub os: String,
    pub abi: String,
    pub trust_level: u8,
    pub identity_status: IdentityStatus,
    pub integrity_verified: bool,
    pub health: MemberHealth,
    pub pressure_bucket: u8,
    pub available_cpu_units: u32,
    pub available_memory_bytes: u64,
    pub available_vram_bytes: u64,
    pub localized_content: BTreeSet<String>,
    pub variants: Vec<ExecutionVariant>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlacementSlo {
    pub deadline_ticks: f64,
    pub max_p95_ticks: f64,
    pub max_p99_ticks: f64,
    pub max_jitter_ticks: f64,
    pub max_failure_probability: f64,
    pub minimum_quality: f64,
    pub streaming: bool,
    pub max_ttft_ticks: Option<f64>,
    pub max_steady_latency_ticks: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskPlacementRequest {
    pub task_id: String,
    pub task_type: String,
    pub input_bucket: u8,
    pub local_node: NodeId,
    pub priority: LexicographicPriority,
    pub required_capabilities: CapabilityBits,
    pub required_os: Option<String>,
    pub required_abi: Option<String>,
    pub minimum_trust: u8,
    pub required_memory_bytes: u64,
    pub required_vram_bytes: u64,
    pub required_plugin: Option<(String, u64)>,
    pub required_content: BTreeSet<String>,
    pub flags: PlacementFlags,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub local_estimated_cost: f64,
    pub safety_margin: f64,
    pub small_task_threshold: f64,
    pub quality_policy: QualityPolicy,
    pub session_node: Option<NodeId>,
    pub migration_cost: f64,
    pub dag_cross_node_cost: f64,
    pub dag_parallel_benefit: f64,
    pub slo: PlacementSlo,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulingEvent {
    NewTask,
    NodeStateChanged,
    CapabilityChanged,
    AdmissionRejected,
    SessionMigrationRequested,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlacementCandidate {
    pub node_id: NodeId,
    pub variant_id: String,
    pub capability_version: u64,
    pub resource_version: u64,
    pub predicted_p50: f64,
    pub predicted_p95: f64,
    pub predicted_p99: f64,
    pub risk_adjusted_cost: f64,
    pub remote_cost: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlacementPlan {
    pub selected: PlacementCandidate,
    pub fallbacks: Vec<PlacementCandidate>,
    pub evaluated_candidates: usize,
    pub profitability_margin: f64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReservationRequest {
    pub reservation_id: String,
    pub origin: WorkOrigin,
    pub capability_version: u64,
    pub cpu_units: u32,
    pub memory_bytes: u64,
    pub vram_bytes: u64,
    pub threads: u32,
    pub valid_until_tick: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum AdmissionOutcome {
    Accept { reservation_id: String },
    RetryAfter { tick: u64 },
    Overloaded,
    InsufficientMemory,
    CapabilityChanged { current_version: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalResourceBudget {
    pub total_cpu_units: u32,
    pub total_memory_bytes: u64,
    pub total_vram_bytes: u64,
    pub total_threads: u32,
    pub reserved_local_cpu_units: u32,
    pub reserved_local_memory_bytes: u64,
    pub reserved_local_vram_bytes: u64,
    pub reserved_local_threads: u32,
    pub max_remote_pressure_bucket: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DistributedResourceBudget {
    pub max_cpu_share_percent: u8,
    pub max_memory_bytes: u64,
    pub max_hash_bytes_per_tick: u64,
    pub max_disk_bytes_per_tick: u64,
    pub max_scheduler_operations_per_tick: u32,
    pub max_telemetry_events_per_tick: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NetworkBudget {
    pub max_bytes_per_tick: u64,
    pub max_concurrent_transfers: usize,
    pub max_queued_bytes: u64,
    pub control_reserve_bytes_per_tick: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkDegradation {
    Normal,
    StopPreReplication,
    ReduceCheckpointing,
    PauseRemoteBatch,
    RejectLargeRemote,
    ControlOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteLoadAction {
    Continue,
    ReduceConcurrency,
    PauseCheckpointableBatch,
    CancelRemoteBackground,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryClass {
    Correctness,
    SchedulingSummary,
    Discardable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CombinedPulse {
    pub node_id: NodeId,
    pub capability_version: u64,
    pub resource_version: u64,
    pub pressure_bucket: u8,
    pub health: MemberHealth,
    pub next_sample_after_ticks: u64,
    pub accepted_events: BTreeMap<String, u64>,
}
