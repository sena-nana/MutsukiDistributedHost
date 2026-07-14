use mutsuki_runtime_contracts::ContentId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::{GlobalTaskId, NodeId};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustMode {
    #[default]
    TrustedLan,
    AuditedLan,
    RestrictedWorkers,
    ByzantineResistant,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeTrustLevel {
    Quarantined,
    Untrusted,
    Restricted,
    Managed,
    Trusted,
}

impl NodeTrustLevel {
    pub const fn rank(self) -> u8 {
        match self {
            Self::Quarantined => 0,
            Self::Untrusted => 1,
            Self::Restricted => 2,
            Self::Managed => 3,
            Self::Trusted => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityStatus {
    Pending,
    Active,
    Expired,
    Revoked,
    Quarantined,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub node_id: NodeId,
    pub key_id: String,
    pub key_generation: u64,
    pub certificate_fingerprint: String,
    pub valid_from_tick: u64,
    pub valid_until_tick: u64,
    pub status: IdentityStatus,
    pub trust_level: NodeTrustLevel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataSensitivity {
    Public,
    Internal,
    Confidential,
    Restricted,
    Credential,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskTrustFlags(pub u8);

impl TaskTrustFlags {
    pub const ALLOW_EXTERNAL_WORKERS: Self = Self(1 << 0);
    pub const ALLOW_PERSISTENT_CACHE: Self = Self(1 << 1);
    pub const REQUIRE_ATTESTATION: Self = Self(1 << 2);
    pub const IRREVERSIBLE_EFFECTS: Self = Self(1 << 3);

    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn contains(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "level")]
pub enum ResultVerificationPolicy {
    None,
    HashOnly,
    DeterministicReplay,
    SpotCheck { rate_basis_points: u16 },
    NOfM { required: u8, total: u8 },
    DomainVerifier { protocol_id: String },
    Replayable,
    ProofCarrying { proof_type: String },
    ManualReview,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskTrustPolicy {
    pub sensitivity: DataSensitivity,
    pub minimum_trust: NodeTrustLevel,
    pub flags: TaskTrustFlags,
    pub verification: ResultVerificationPolicy,
    pub task_value: u8,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArtifactIdentity {
    pub artifact_id: String,
    pub artifact_kind: String,
    pub version: String,
    pub generation: u64,
    pub content_id: ContentId,
    pub signer_key_id: String,
    pub integrity_tag: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AttestationEvidence {
    pub provider: String,
    pub node_id: NodeId,
    pub identity_key_id: String,
    pub host_content_id: ContentId,
    pub artifact_content_ids: Vec<ContentId>,
    pub issued_tick: u64,
    pub valid_until_tick: u64,
    pub evidence_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AttestationVerdict {
    pub accepted: bool,
    pub verifier_id: String,
    pub node_id: NodeId,
    pub valid_until_tick: u64,
    pub environment_digest: String,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceAuthorization {
    pub authorization_id: String,
    pub global_task_id: GlobalTaskId,
    pub attempt: u32,
    pub node_id: NodeId,
    pub subject_identity_generation: u64,
    pub term: u64,
    pub epoch: u64,
    pub content_ids: Vec<ContentId>,
    pub scopes: BTreeSet<String>,
    pub allow_persistent_cache: bool,
    pub issued_tick: u64,
    pub valid_until_tick: u64,
    pub key_id: String,
    pub authorization_tag: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReceipt {
    pub global_task_id: GlobalTaskId,
    pub attempt: u32,
    pub term: u64,
    pub epoch: u64,
    pub node_id: NodeId,
    pub task_schema: String,
    pub input_content_id: ContentId,
    pub output_content_id: ContentId,
    pub runner_id: String,
    pub runner_generation: u64,
    pub plugin_id: String,
    pub plugin_generation: u64,
    pub execution_variant: String,
    pub policy_digest: String,
    pub quality: f64,
    pub degraded_flags: BTreeSet<String>,
    pub environment_digest: String,
    pub identity_key_id: String,
    pub receipt_tag: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitProof {
    pub log_index: u64,
    pub term: u64,
    pub epoch: u64,
    pub quorum_certificate_digest: Option<String>,
    pub audit_segment: Option<u64>,
    pub audit_leaf: Option<u32>,
    pub audit_root: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustBoundObjectKind {
    AssignmentLease,
    ExecutionGrant,
    ResultReceipt,
    ResourceManifest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedStateBinding {
    pub kind: TrustBoundObjectKind,
    pub object_digest: String,
    pub subject_node_id: NodeId,
    pub signer_node_id: NodeId,
    pub global_task_id: Option<GlobalTaskId>,
    pub attempt: Option<u32>,
    pub term: u64,
    pub epoch: u64,
    pub key_id: String,
    pub authentication_tag: String,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    Pending,
    Accepted,
    Rejected,
    Quarantined,
    ManualReview,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationAction {
    AcceptDigest,
    ReplayOnIndependentNode,
    Sample { rate_basis_points: u16 },
    CollectIndependentResults { required: u8, total: u8 },
    InvokeDomainVerifier { protocol_id: String },
    ValidateReplayArtifact,
    ValidateProof { proof_type: String },
    RequireManualReview,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResultVerificationRecord {
    pub global_task_id: GlobalTaskId,
    pub attempt: u32,
    pub policy: ResultVerificationPolicy,
    pub status: VerificationStatus,
    pub verifier_id: String,
    pub expected_digest: Option<String>,
    pub observed_digest: String,
    pub tolerance: Option<f64>,
    pub evidence: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceAction {
    LeaderFence,
    VotingMembershipChange,
    TrustRootChange,
    ForceAcceptQuarantinedResult,
    OverrideUncertainEffect,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GovernanceCertificate {
    pub action: GovernanceAction,
    pub action_digest: String,
    pub term: u64,
    pub epoch: u64,
    pub required_signers: u8,
    pub signer_tags: BTreeMap<NodeId, String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventKind {
    Membership,
    Permission,
    Assignment,
    Lease,
    ResultCommit,
    Verification,
    Revocation,
    ManualDecision,
    ResourceAccess,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub sequence: u64,
    pub tick: u64,
    pub kind: AuditEventKind,
    pub global_task_id: Option<GlobalTaskId>,
    pub attempt: Option<u32>,
    pub node_id: Option<NodeId>,
    pub metadata: BTreeMap<String, String>,
    pub previous_hash: String,
    pub event_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuditSegment {
    pub segment_id: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub previous_segment_root: Option<String>,
    pub merkle_root: String,
    pub leaf_hashes: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuditInclusionProof {
    pub segment_id: u64,
    pub leaf_index: u32,
    pub leaf_hash: String,
    pub siblings: Vec<(String, bool)>,
    pub merkle_root: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TrustPlaneBudget {
    pub max_signatures_per_tick: u32,
    pub max_verifications_per_tick: u32,
    pub max_replays_per_tick: u32,
    pub max_audit_events_per_segment: usize,
    pub max_audit_metadata_entries: usize,
    pub max_audit_bytes_per_tick: u64,
    pub max_reputation_updates_per_tick: u32,
    pub max_attestations_per_tick: u32,
    pub max_compute_units_per_tick: u64,
    pub max_network_bytes_per_tick: u64,
    pub max_storage_bytes_per_tick: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyKind {
    ForgedCapability,
    ResultMismatch,
    ResourceCorruption,
    AbnormalLatency,
    CancelRefused,
    UnauthorizedAccess,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReputationSnapshot {
    pub node_id: NodeId,
    pub capability: String,
    pub plugin_generation: u64,
    pub samples: u32,
    pub reliability_ewma: f64,
    pub timeout_ewma: f64,
    pub mismatch_ewma: f64,
    pub corruption_ewma: f64,
    pub uncertainty_penalty: f64,
    pub anomalies: BTreeSet<AnomalyKind>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompromiseImpact {
    pub node_id: NodeId,
    pub new_fencing_epoch: u64,
    pub revoked_authorizations: Vec<String>,
    pub quarantined_content: Vec<ContentId>,
    pub affected_tasks: BTreeSet<GlobalTaskId>,
    pub required_actions: Vec<String>,
}
