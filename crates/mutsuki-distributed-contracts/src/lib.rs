//! Distributed control/data-plane descriptors. These contracts never enter
//! the plugin ABI or the ordinary local Host execution context.

#![forbid(unsafe_code)]
#![allow(clippy::missing_errors_doc, clippy::must_use_candidate)]

use mutsuki_runtime_contracts::{
    CapabilitySet, ContentId, ExecutionMobility, PortabilityCatalog, PortableTask, RequirementSet,
    RetrySafety, TaskHandle,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

mod durability;
pub use durability::*;
mod ha;
pub use ha::*;
mod recovery;
pub use recovery::*;
mod scheduler;
pub use scheduler::*;

pub const DISTRIBUTED_PROTOCOL_ID: &str = "mutsuki.distributed.cluster";
pub const DISTRIBUTED_PROTOCOL_MAJOR: u16 = 1;
pub const MAX_CONTROL_FRAME_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributionMode {
    #[default]
    Disabled,
    LocalObservable,
    Clustered,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct NodeId(pub String);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct GlobalTaskId(pub String);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerHealth {
    #[default]
    Ready,
    Busy,
    Draining,
    Unreachable,
    Incompatible,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunnerGeneration {
    pub runner_id: String,
    pub plugin_id: String,
    pub runner_generation: u64,
    pub plugin_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerAdvertisement {
    pub node_id: NodeId,
    pub protocol_major: u16,
    pub snapshot_version: u64,
    pub capabilities: CapabilitySet,
    pub portability: PortabilityCatalog,
    pub runners: Vec<RunnerGeneration>,
    pub localized_content: BTreeSet<String>,
    pub health: WorkerHealth,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerPulse {
    pub node_id: NodeId,
    pub snapshot_version: u64,
    pub health: WorkerHealth,
    pub running_tasks: usize,
    pub queue_depth: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectDataRef {
    pub owner_node: NodeId,
    pub content_id: ContentId,
    pub endpoint_hint: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RemoteTaskEnvelope {
    pub global_task_id: GlobalTaskId,
    pub attempt: u32,
    pub origin_node: NodeId,
    pub requirements: RequirementSet,
    pub portable: PortableTask,
    /// Descriptors only. Resource bytes travel directly between origin and Worker.
    pub direct_inputs: Vec<DirectDataRef>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RemoteAccepted {
    pub global_task_id: GlobalTaskId,
    pub attempt: u32,
    pub worker_node: NodeId,
    pub local_handle: TaskHandle,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RemoteResult {
    pub global_task_id: GlobalTaskId,
    pub attempt: u32,
    pub worker_node: NodeId,
    pub outcome: Option<LocalTaskOutcome>,
    pub direct_outputs: Vec<DirectDataRef>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerRequest {
    pub request_id: u64,
    pub command: WorkerCommand,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", content = "payload", rename_all = "snake_case")]
pub enum WorkerCommand {
    Submit(Box<RemoteTaskEnvelope>),
    Cancel(TaskHandle),
    Outcome(TaskHandle),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerReply {
    pub request_id: u64,
    pub result: Result<WorkerReplyBody, WorkerFailure>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "reply", content = "payload", rename_all = "snake_case")]
pub enum WorkerReplyBody {
    Accepted(RemoteAccepted),
    Cancelled,
    Outcome(Option<LocalTaskOutcome>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerFailure {
    pub kind: DistributedErrorKind,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalTaskSnapshot {
    pub task_id: String,
    pub protocol_id: String,
    pub status: String,
    pub registry_generation: u64,
    pub runner_id: Option<String>,
    pub lease_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalTaskOutcome {
    pub task_id: String,
    pub status: String,
    pub output_ref: Option<String>,
    pub reason: Option<String>,
    pub error_code: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementKind {
    Local,
    Remote,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskPlacement {
    pub kind: PlacementKind,
    pub global_task_id: GlobalTaskId,
    pub attempt: u32,
    pub node_id: NodeId,
    pub local_handle: TaskHandle,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AttemptRecord {
    pub attempt: u32,
    pub node_id: NodeId,
    pub local_handle: TaskHandle,
    pub active: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GlobalTaskRecord {
    pub global_task_id: GlobalTaskId,
    pub portable: PortableTask,
    pub requirements: RequirementSet,
    pub direct_inputs: Vec<DirectDataRef>,
    pub attempts: Vec<AttemptRecord>,
}

impl GlobalTaskRecord {
    pub fn active_attempt(&self) -> Option<&AttemptRecord> {
        self.attempts.iter().rev().find(|attempt| attempt.active)
    }
}

pub fn can_restart_from_input(portable: &PortableTask) -> bool {
    matches!(
        portable.capability.mobility,
        ExecutionMobility::Restartable | ExecutionMobility::Checkpointable
    ) && matches!(
        portable.capability.retry_safety,
        RetrySafety::Idempotent | RetrySafety::Verifiable | RetrySafety::Compensatable
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributedErrorKind {
    Disabled,
    InvalidConfig,
    CapacityExceeded,
    HostUnavailable,
    WorkerUnavailable,
    WorkerRejected,
    Incompatible,
    LocalizationFailed,
    TransportClosed,
    AttemptStale,
    RetryUnsafe,
    TaskUnknown,
    Protocol,
    Storage,
    Corrupt,
    DurabilityUnavailable,
    InvalidTransition,
    Conflict,
    QuorumLost,
    Fenced,
    GrantExpired,
    NotLeader,
    ControlLeaseExpired,
}

impl From<&DistributedError> for WorkerFailure {
    fn from(error: &DistributedError) -> Self {
        Self {
            kind: error.kind,
            message: error.public_message.to_owned(),
        }
    }
}

pub fn encode_control<T: Serialize>(value: &T) -> Result<Vec<u8>, DistributedError> {
    let bytes = serde_json::to_vec(value).map_err(|_| protocol_error())?;
    if bytes.len() > MAX_CONTROL_FRAME_BYTES {
        return Err(DistributedError::new(
            DistributedErrorKind::CapacityExceeded,
            "distributed control frame exceeds the bounded limit",
        ));
    }
    Ok(bytes)
}

pub fn decode_control<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, DistributedError> {
    if bytes.len() > MAX_CONTROL_FRAME_BYTES {
        return Err(DistributedError::new(
            DistributedErrorKind::CapacityExceeded,
            "distributed control frame exceeds the bounded limit",
        ));
    }
    serde_json::from_slice(bytes).map_err(|_| protocol_error())
}

const fn protocol_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Protocol,
        "distributed control frame is invalid",
    )
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
#[error("{public_message}")]
pub struct DistributedError {
    pub kind: DistributedErrorKind,
    pub public_message: &'static str,
}

impl DistributedError {
    pub const fn new(kind: DistributedErrorKind, public_message: &'static str) -> Self {
        Self {
            kind,
            public_message,
        }
    }
}

pub fn runner_generations(advertisement: &WorkerAdvertisement) -> BTreeMap<&str, u64> {
    advertisement
        .runners
        .iter()
        .map(|runner| (runner.runner_id.as_str(), runner.runner_generation))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mutsuki_runtime_contracts::{
        PortabilityCapability, SchemaIdentity, Task, TaskAcceptanceDurability,
    };
    use serde_json::json;

    #[test]
    fn large_data_never_enters_control_envelope() {
        let input = ContentId::new("sha256", "abc", 8 * 1024 * 1024 * 1024, "blob");
        let portable = PortableTask::new(
            Task::new(
                "local-task",
                "example.compute",
                json!({ "input": "content:abc" }),
            ),
            SchemaIdentity::new("example.compute", "1.0.0"),
            input.clone(),
            PortabilityCapability {
                mobility: ExecutionMobility::Restartable,
                retry_safety: RetrySafety::Idempotent,
                task_acceptance: TaskAcceptanceDurability::Volatile,
                ..PortabilityCapability::default()
            },
        );
        let envelope = RemoteTaskEnvelope {
            global_task_id: GlobalTaskId("global-1".into()),
            attempt: 1,
            origin_node: NodeId("origin".into()),
            requirements: RequirementSet::default(),
            portable,
            direct_inputs: vec![DirectDataRef {
                owner_node: NodeId("origin".into()),
                content_id: input,
                endpoint_hint: "link://origin/resource/abc".into(),
            }],
        };
        let encoded = encode_control(&envelope).unwrap();
        assert!(encoded.len() < 4 * 1024);
        assert!(can_restart_from_input(&envelope.portable));
    }

    #[test]
    fn oversized_control_frames_are_rejected_before_decode() {
        let bytes = vec![b'x'; MAX_CONTROL_FRAME_BYTES + 1];
        assert_eq!(
            decode_control::<WorkerRequest>(&bytes).unwrap_err().kind,
            DistributedErrorKind::CapacityExceeded
        );
    }
}
