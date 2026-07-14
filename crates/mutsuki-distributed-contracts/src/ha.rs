use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{GlobalTaskId, NodeId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlNodeKind {
    Full,
    Witness,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlRole {
    Leader,
    Follower,
    Witness,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberHealth {
    Healthy,
    Overloaded,
    Draining,
    Suspect,
    Isolated,
    Dead,
    Incompatible,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterAvailability {
    Healthy,
    Impaired,
    Degraded,
    QuorumLost,
    Isolated,
    SafeStop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlOperation {
    Query,
    LocalWork,
    ContinueGranted,
    DurableWrite,
    MembershipChange,
    GenerationSwitch,
    IrreversibleEffect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlRecordKind {
    Membership,
    GlobalTask,
    Assignment,
    PluginGeneration,
    ResourceMetadata,
    ManagementConfig,
    ExecutionGrant,
    ResultCommit,
    Reconciliation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControlRecord {
    pub record_id: String,
    pub kind: ControlRecordKind,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommittedControlRecord {
    pub index: u64,
    pub term: u64,
    pub epoch: u64,
    pub record: ControlRecord,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionGrant {
    pub global_task_id: GlobalTaskId,
    pub attempt: u32,
    pub worker_node: NodeId,
    pub term: u64,
    pub epoch: u64,
    pub issued_tick: u64,
    pub valid_until_tick: u64,
    pub irreversible_effects: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControlLease {
    pub leader_node: NodeId,
    pub term: u64,
    pub issued_tick: u64,
    pub valid_until_tick: u64,
}

impl ControlLease {
    pub const fn is_valid_at(&self, tick: u64) -> bool {
        tick >= self.issued_tick && tick <= self.valid_until_tick
    }
}

impl ExecutionGrant {
    pub const fn is_valid_at(&self, tick: u64) -> bool {
        tick >= self.issued_tick && tick <= self.valid_until_tick
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutedUncommittedResult {
    pub global_task_id: GlobalTaskId,
    pub attempt: u32,
    pub worker_node: NodeId,
    pub grant_term: u64,
    pub grant_epoch: u64,
    pub output_digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationDecision {
    Accept,
    Reexecute,
    Compensate,
    Reject,
    ManualReview,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemberPulseSummary {
    pub node_id: NodeId,
    pub capability_version: u64,
    pub resource_version: u64,
    pub pressure_bucket: u8,
    pub health: MemberHealth,
}
