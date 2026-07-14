use hmac::{Hmac, Mac};
use mutsuki_distributed_contracts::{
    ArtifactIdentity, AttestationEvidence, AttestationVerdict, CommitProof, DataSensitivity,
    DistributedError, DistributedErrorKind, ExecutionReceipt, GlobalTaskId, GovernanceCertificate,
    IdentityStatus, NodeId, NodeIdentity, NodeTrustLevel, ResourceAuthorization,
    ResultVerificationPolicy, ResultVerificationRecord, SignedStateBinding, TaskTrustFlags,
    TaskTrustPolicy, TrustBoundObjectKind, TrustMode, TrustPlaneBudget, VerificationAction,
    VerificationStatus,
};
use mutsuki_runtime_contracts::ContentId;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::LinkSessionBinding;

type HmacSha256 = Hmac<Sha256>;
const MIN_KEY_BYTES: usize = 32;

#[derive(Clone)]
struct IdentityEntry {
    identity: NodeIdentity,
    secret: Vec<u8>,
}

#[derive(Default)]
pub struct NodeIdentityRegistry {
    entries: BTreeMap<NodeId, IdentityEntry>,
}

impl NodeIdentityRegistry {
    pub fn approve(
        &mut self,
        mut identity: NodeIdentity,
        secret: Vec<u8>,
        now_tick: u64,
    ) -> Result<(), DistributedError> {
        validate_identity(&identity, &secret, now_tick)?;
        if self.entries.contains_key(&identity.node_id) {
            return Err(DistributedError::new(
                DistributedErrorKind::Conflict,
                "node identity already exists and requires rotation",
            ));
        }
        identity.status = IdentityStatus::Active;
        self.entries
            .insert(identity.node_id.clone(), IdentityEntry { identity, secret });
        Ok(())
    }

    pub fn rotate(
        &mut self,
        node_id: &NodeId,
        key_id: String,
        certificate_fingerprint: String,
        secret: Vec<u8>,
        valid_from_tick: u64,
        valid_until_tick: u64,
        now_tick: u64,
    ) -> Result<NodeIdentity, DistributedError> {
        let current = self.entries.get(node_id).ok_or_else(identity_unknown)?;
        if !identity_active_at(&current.identity, now_tick) || secret.len() < MIN_KEY_BYTES {
            return Err(identity_unavailable());
        }
        let next_generation = current.identity.key_generation.saturating_add(1);
        let trust_level = current.identity.trust_level;
        let identity = NodeIdentity {
            node_id: node_id.clone(),
            key_id,
            key_generation: next_generation,
            certificate_fingerprint,
            valid_from_tick,
            valid_until_tick,
            status: IdentityStatus::Active,
            trust_level,
        };
        validate_identity(&identity, &secret, now_tick)?;
        if let Some(current) = self.entries.get_mut(node_id) {
            current.secret.fill(0);
        }
        self.entries.insert(
            node_id.clone(),
            IdentityEntry {
                identity: identity.clone(),
                secret,
            },
        );
        Ok(identity)
    }

    pub fn revoke(&mut self, node_id: &NodeId) -> Result<(), DistributedError> {
        let entry = self.entries.get_mut(node_id).ok_or_else(identity_unknown)?;
        entry.identity.status = IdentityStatus::Revoked;
        entry.secret.fill(0);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn readmit(
        &mut self,
        node_id: &NodeId,
        key_id: String,
        certificate_fingerprint: String,
        secret: Vec<u8>,
        trust_level: NodeTrustLevel,
        valid_from_tick: u64,
        valid_until_tick: u64,
        attestation: &AttestationVerdict,
        now_tick: u64,
    ) -> Result<NodeIdentity, DistributedError> {
        let previous = self.entries.get(node_id).ok_or_else(identity_unknown)?;
        if !matches!(
            previous.identity.status,
            IdentityStatus::Revoked | IdentityStatus::Quarantined
        ) || !attestation.accepted
            || attestation.node_id != *node_id
            || attestation.valid_until_tick < now_tick
        {
            return Err(trust_policy_error());
        }
        let identity = NodeIdentity {
            node_id: node_id.clone(),
            key_id,
            key_generation: previous.identity.key_generation.saturating_add(1),
            certificate_fingerprint,
            valid_from_tick,
            valid_until_tick,
            status: IdentityStatus::Active,
            trust_level,
        };
        validate_identity(&identity, &secret, now_tick)?;
        self.entries.insert(
            node_id.clone(),
            IdentityEntry {
                identity: identity.clone(),
                secret,
            },
        );
        Ok(identity)
    }

    pub fn quarantine(&mut self, node_id: &NodeId) -> Result<(), DistributedError> {
        let entry = self.entries.get_mut(node_id).ok_or_else(identity_unknown)?;
        entry.identity.status = IdentityStatus::Quarantined;
        entry.identity.trust_level = NodeTrustLevel::Quarantined;
        entry.secret.fill(0);
        Ok(())
    }

    pub fn set_trust_level(
        &mut self,
        node_id: &NodeId,
        trust_level: NodeTrustLevel,
    ) -> Result<(), DistributedError> {
        let entry = self.entries.get_mut(node_id).ok_or_else(identity_unknown)?;
        if entry.identity.status != IdentityStatus::Active {
            return Err(identity_unavailable());
        }
        entry.identity.trust_level = trust_level;
        Ok(())
    }

    pub fn identity(&self, node_id: &NodeId) -> Option<&NodeIdentity> {
        self.entries.get(node_id).map(|entry| &entry.identity)
    }

    pub fn eligible(&self, node_id: &NodeId, minimum_trust: NodeTrustLevel, now_tick: u64) -> bool {
        self.entries.get(node_id).is_some_and(|entry| {
            identity_active_at(&entry.identity, now_tick)
                && entry.identity.trust_level >= minimum_trust
        })
    }

    pub fn sign(
        &self,
        node_id: &NodeId,
        payload: &[u8],
        now_tick: u64,
    ) -> Result<(String, String), DistributedError> {
        let entry = self.entries.get(node_id).ok_or_else(identity_unknown)?;
        if !identity_active_at(&entry.identity, now_tick) {
            return Err(identity_unavailable());
        }
        Ok((
            entry.identity.key_id.clone(),
            mac_hex(&entry.secret, payload)?,
        ))
    }

    pub fn verify(
        &self,
        node_id: &NodeId,
        key_id: &str,
        payload: &[u8],
        tag: &str,
        now_tick: u64,
    ) -> bool {
        self.entries.get(node_id).is_some_and(|entry| {
            identity_active_at(&entry.identity, now_tick)
                && entry.identity.key_id == key_id
                && verify_mac(&entry.secret, payload, tag)
        })
    }
}

pub fn require_authenticated_encrypted_link(
    binding: &LinkSessionBinding,
) -> Result<(), DistributedError> {
    if binding.security_level < mutsuki_link_core::SecurityLevel::AuthenticatedEncrypted {
        return Err(DistributedError::new(
            DistributedErrorKind::Incompatible,
            "distributed trust plane requires authenticated encrypted transport",
        ));
    }
    Ok(())
}

pub struct TrustPolicyEngine {
    mode: TrustMode,
}

impl TrustPolicyEngine {
    pub const fn new(mode: TrustMode) -> Self {
        Self { mode }
    }

    pub fn authorize_node(
        &self,
        registry: &NodeIdentityRegistry,
        node_id: &NodeId,
        policy: &TaskTrustPolicy,
        integrity_verified: bool,
        attestation: Option<&AttestationVerdict>,
        now_tick: u64,
    ) -> Result<(), DistributedError> {
        if !registry.eligible(node_id, policy.minimum_trust, now_tick) {
            return Err(trust_policy_error());
        }
        let identity = registry.identity(node_id).ok_or_else(identity_unknown)?;
        if !policy
            .flags
            .contains(TaskTrustFlags::ALLOW_EXTERNAL_WORKERS)
            && identity.trust_level < NodeTrustLevel::Managed
        {
            return Err(trust_policy_error());
        }
        if matches!(
            policy.sensitivity,
            DataSensitivity::Confidential
                | DataSensitivity::Restricted
                | DataSensitivity::Credential
        ) && identity.trust_level < NodeTrustLevel::Managed
        {
            return Err(trust_policy_error());
        }
        if policy.sensitivity == DataSensitivity::Credential
            && identity.trust_level != NodeTrustLevel::Trusted
        {
            return Err(trust_policy_error());
        }
        if !integrity_verified {
            return Err(trust_policy_error());
        }
        if policy.flags.contains(TaskTrustFlags::REQUIRE_ATTESTATION)
            && !attestation.is_some_and(|verdict| {
                verdict.accepted
                    && verdict.node_id == *node_id
                    && verdict.valid_until_tick >= now_tick
            })
        {
            return Err(trust_policy_error());
        }
        if self.mode == TrustMode::RestrictedWorkers
            && identity.trust_level == NodeTrustLevel::Untrusted
            && policy.sensitivity != DataSensitivity::Public
        {
            return Err(trust_policy_error());
        }
        Ok(())
    }
}

pub struct ArtifactVerifier {
    authority_keys: BTreeMap<String, Vec<u8>>,
    allowlist: BTreeMap<(String, u64), ArtifactIdentity>,
}

impl ArtifactVerifier {
    pub fn new(authority_keys: BTreeMap<String, Vec<u8>>) -> Result<Self, DistributedError> {
        if authority_keys.is_empty() || authority_keys.values().any(|key| key.len() < MIN_KEY_BYTES)
        {
            return Err(trust_config_error());
        }
        Ok(Self {
            authority_keys,
            allowlist: BTreeMap::new(),
        })
    }

    pub fn sign_and_allow(
        &mut self,
        mut artifact: ArtifactIdentity,
    ) -> Result<ArtifactIdentity, DistributedError> {
        let key = self
            .authority_keys
            .get(&artifact.signer_key_id)
            .ok_or_else(trust_policy_error)?;
        if artifact.generation == 0 {
            return Err(trust_policy_error());
        }
        artifact.integrity_tag.clear();
        let payload = canonical_json(&artifact)?;
        artifact.integrity_tag = mac_hex(key, &payload)?;
        self.allowlist.insert(
            (artifact.artifact_id.clone(), artifact.generation),
            artifact.clone(),
        );
        Ok(artifact)
    }

    pub fn verify(&self, artifact: &ArtifactIdentity) -> bool {
        let Some(allowed) = self
            .allowlist
            .get(&(artifact.artifact_id.clone(), artifact.generation))
        else {
            return false;
        };
        if allowed != artifact {
            return false;
        }
        let Some(key) = self.authority_keys.get(&artifact.signer_key_id) else {
            return false;
        };
        let mut unsigned = artifact.clone();
        let tag = std::mem::take(&mut unsigned.integrity_tag);
        canonical_json(&unsigned).is_ok_and(|payload| verify_mac(key, &payload, &tag))
    }
}

pub struct AttestationVerifier {
    provider_keys: BTreeMap<String, Vec<u8>>,
}

impl AttestationVerifier {
    pub fn new(provider_keys: BTreeMap<String, Vec<u8>>) -> Result<Self, DistributedError> {
        if provider_keys.is_empty() || provider_keys.values().any(|key| key.len() < MIN_KEY_BYTES) {
            return Err(trust_config_error());
        }
        Ok(Self { provider_keys })
    }

    pub fn sign_evidence(
        &self,
        mut evidence: AttestationEvidence,
    ) -> Result<AttestationEvidence, DistributedError> {
        let key = self
            .provider_keys
            .get(&evidence.provider)
            .ok_or_else(trust_policy_error)?;
        evidence.evidence_digest.clear();
        evidence.evidence_digest = mac_hex(key, &canonical_json(&evidence)?)?;
        Ok(evidence)
    }

    pub fn verify(
        &self,
        evidence: &AttestationEvidence,
        identity: &NodeIdentity,
        required_artifacts: &[ContentId],
        now_tick: u64,
    ) -> AttestationVerdict {
        let accepted = self
            .provider_keys
            .get(&evidence.provider)
            .is_some_and(|key| {
                let mut unsigned = evidence.clone();
                let tag = std::mem::take(&mut unsigned.evidence_digest);
                canonical_json(&unsigned).is_ok_and(|payload| verify_mac(key, &payload, &tag))
                    && evidence.node_id == identity.node_id
                    && evidence.identity_key_id == identity.key_id
                    && evidence.issued_tick <= now_tick
                    && evidence.valid_until_tick >= now_tick
                    && required_artifacts
                        .iter()
                        .all(|required| evidence.artifact_content_ids.contains(required))
            });
        AttestationVerdict {
            accepted,
            verifier_id: format!("attestation:{}", evidence.provider),
            node_id: evidence.node_id.clone(),
            valid_until_tick: evidence.valid_until_tick,
            environment_digest: digest_hex(
                format!(
                    "{}:{}:{}",
                    evidence.node_id.0, evidence.identity_key_id, evidence.host_content_id.digest
                )
                .as_bytes(),
            ),
            reason: (!accepted).then(|| "attestation evidence or binding is invalid".into()),
        }
    }
}

pub struct ResourceAuthorizer {
    authorizations: BTreeMap<String, ResourceAuthorization>,
    authority_key_id: String,
    authority_secret: Vec<u8>,
}

impl ResourceAuthorizer {
    pub fn new(
        authority_key_id: String,
        authority_secret: Vec<u8>,
    ) -> Result<Self, DistributedError> {
        if authority_key_id.is_empty() || authority_secret.len() < MIN_KEY_BYTES {
            return Err(trust_config_error());
        }
        Ok(Self {
            authorizations: BTreeMap::new(),
            authority_key_id,
            authority_secret,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn grant(
        &mut self,
        registry: &NodeIdentityRegistry,
        node_id: &NodeId,
        global_task_id: GlobalTaskId,
        attempt: u32,
        term: u64,
        epoch: u64,
        content_ids: Vec<ContentId>,
        scopes: BTreeSet<String>,
        allow_persistent_cache: bool,
        issued_tick: u64,
        valid_until_tick: u64,
    ) -> Result<ResourceAuthorization, DistributedError> {
        if attempt == 0
            || term == 0
            || epoch == 0
            || content_ids.is_empty()
            || scopes.is_empty()
            || valid_until_tick <= issued_tick
            || !registry.eligible(node_id, NodeTrustLevel::Untrusted, issued_tick)
        {
            return Err(trust_policy_error());
        }
        let authorization_id = digest_hex(
            format!(
                "{}:{}:{}:{}:{}",
                global_task_id.0, attempt, node_id.0, epoch, valid_until_tick
            )
            .as_bytes(),
        );
        let subject_identity_generation = registry
            .identity(node_id)
            .ok_or_else(identity_unknown)?
            .key_generation;
        let mut authorization = ResourceAuthorization {
            authorization_id: authorization_id.clone(),
            global_task_id,
            attempt,
            node_id: node_id.clone(),
            subject_identity_generation,
            term,
            epoch,
            content_ids,
            scopes,
            allow_persistent_cache,
            issued_tick,
            valid_until_tick,
            key_id: String::new(),
            authorization_tag: String::new(),
        };
        let payload = canonical_json(&authorization)?;
        authorization.key_id.clone_from(&self.authority_key_id);
        authorization.authorization_tag = mac_hex(&self.authority_secret, &payload)?;
        self.authorizations
            .insert(authorization_id, authorization.clone());
        Ok(authorization)
    }

    pub fn validate(
        &self,
        registry: &NodeIdentityRegistry,
        authorization: &ResourceAuthorization,
        current_term: u64,
        current_epoch: u64,
        now_tick: u64,
    ) -> bool {
        if authorization.term != current_term
            || authorization.epoch != current_epoch
            || authorization.issued_tick > now_tick
            || authorization.valid_until_tick < now_tick
            || !self
                .authorizations
                .contains_key(&authorization.authorization_id)
        {
            return false;
        }
        let mut unsigned = authorization.clone();
        let key_id = std::mem::take(&mut unsigned.key_id);
        let tag = std::mem::take(&mut unsigned.authorization_tag);
        key_id == self.authority_key_id
            && registry.eligible(&authorization.node_id, NodeTrustLevel::Untrusted, now_tick)
            && registry
                .identity(&authorization.node_id)
                .is_some_and(|identity| {
                    identity.key_generation == authorization.subject_identity_generation
                })
            && canonical_json(&unsigned)
                .is_ok_and(|payload| verify_mac(&self.authority_secret, &payload, &tag))
    }

    pub fn revoke_for_node(&mut self, node_id: &NodeId) -> Vec<String> {
        let revoked = self
            .authorizations
            .iter()
            .filter(|(_, authorization)| &authorization.node_id == node_id)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in &revoked {
            self.authorizations.remove(id);
        }
        revoked
    }

    pub fn revoke_task(&mut self, global_task_id: &GlobalTaskId) -> Vec<String> {
        let revoked = self
            .authorizations
            .iter()
            .filter(|(_, authorization)| &authorization.global_task_id == global_task_id)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in &revoked {
            self.authorizations.remove(id);
        }
        revoked
    }
}

pub fn sign_execution_receipt(
    registry: &NodeIdentityRegistry,
    mut receipt: ExecutionReceipt,
    now_tick: u64,
) -> Result<ExecutionReceipt, DistributedError> {
    if receipt.attempt == 0
        || receipt.term == 0
        || receipt.epoch == 0
        || receipt.runner_generation == 0
        || receipt.plugin_generation == 0
        || !receipt.quality.is_finite()
    {
        return Err(trust_policy_error());
    }
    receipt.identity_key_id.clear();
    receipt.receipt_tag.clear();
    let payload = canonical_json(&receipt)?;
    let (key_id, tag) = registry.sign(&receipt.node_id, &payload, now_tick)?;
    receipt.identity_key_id = key_id;
    receipt.receipt_tag = tag;
    Ok(receipt)
}

pub fn verify_execution_receipt(
    registry: &NodeIdentityRegistry,
    receipt: &ExecutionReceipt,
    commit: Option<&CommitProof>,
    expected_attempt: u32,
    expected_term: u64,
    expected_epoch: u64,
    now_tick: u64,
) -> Result<(), DistributedError> {
    if receipt.attempt != expected_attempt
        || receipt.term != expected_term
        || receipt.epoch != expected_epoch
        || commit.is_some_and(|proof| {
            proof.term != expected_term || proof.epoch != expected_epoch || proof.log_index == 0
        })
    {
        return Err(DistributedError::new(
            DistributedErrorKind::Fenced,
            "execution receipt belongs to a stale term, epoch, or attempt",
        ));
    }
    let mut unsigned = receipt.clone();
    let key_id = std::mem::take(&mut unsigned.identity_key_id);
    let tag = std::mem::take(&mut unsigned.receipt_tag);
    let payload = canonical_json(&unsigned)?;
    if !registry.verify(&receipt.node_id, &key_id, &payload, &tag, now_tick) {
        return Err(identity_unavailable());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn sign_state_binding(
    registry: &NodeIdentityRegistry,
    kind: TrustBoundObjectKind,
    object_digest: String,
    subject_node_id: NodeId,
    signer_node_id: &NodeId,
    global_task_id: Option<GlobalTaskId>,
    attempt: Option<u32>,
    term: u64,
    epoch: u64,
    now_tick: u64,
) -> Result<SignedStateBinding, DistributedError> {
    if object_digest.is_empty() || term == 0 || epoch == 0 {
        return Err(trust_policy_error());
    }
    let mut binding = SignedStateBinding {
        kind,
        object_digest,
        subject_node_id,
        signer_node_id: signer_node_id.clone(),
        global_task_id,
        attempt,
        term,
        epoch,
        key_id: String::new(),
        authentication_tag: String::new(),
    };
    let payload = canonical_json(&binding)?;
    let (key_id, tag) = registry.sign(signer_node_id, &payload, now_tick)?;
    binding.key_id = key_id;
    binding.authentication_tag = tag;
    Ok(binding)
}

pub fn verify_state_binding(
    registry: &NodeIdentityRegistry,
    binding: &SignedStateBinding,
    expected_term: u64,
    expected_epoch: u64,
    now_tick: u64,
) -> bool {
    if binding.term != expected_term || binding.epoch != expected_epoch {
        return false;
    }
    let mut unsigned = binding.clone();
    let key_id = std::mem::take(&mut unsigned.key_id);
    let tag = std::mem::take(&mut unsigned.authentication_tag);
    canonical_json(&unsigned).is_ok_and(|payload| {
        registry.verify(&binding.signer_node_id, &key_id, &payload, &tag, now_tick)
    })
}

pub fn verify_governance_certificate(
    registry: &NodeIdentityRegistry,
    certificate: &GovernanceCertificate,
    voting_nodes: &BTreeSet<NodeId>,
    now_tick: u64,
) -> bool {
    if certificate.required_signers == 0
        || certificate.signer_tags.len() < usize::from(certificate.required_signers)
    {
        return false;
    }
    let payload = format!(
        "{:?}:{}:{}:{}",
        certificate.action, certificate.action_digest, certificate.term, certificate.epoch
    );
    certificate.signer_tags.iter().all(|(node_id, tag)| {
        voting_nodes.contains(node_id)
            && registry.identity(node_id).is_some_and(|identity| {
                registry.verify(node_id, &identity.key_id, payload.as_bytes(), tag, now_tick)
            })
    })
}

pub trait DomainResultVerifier {
    fn verifier_id(&self) -> &str;
    fn verify(&self, expected: &[u8], observed: &[u8]) -> bool;
}

pub fn verify_deterministic_result(
    global_task_id: GlobalTaskId,
    attempt: u32,
    expected: &[u8],
    observed: &[u8],
    verifier_id: &str,
) -> ResultVerificationRecord {
    let expected_digest = digest_hex(expected);
    let observed_digest = digest_hex(observed);
    ResultVerificationRecord {
        global_task_id,
        attempt,
        policy: ResultVerificationPolicy::DeterministicReplay,
        status: if expected_digest == observed_digest {
            VerificationStatus::Accepted
        } else {
            VerificationStatus::Quarantined
        },
        verifier_id: verifier_id.into(),
        expected_digest: Some(expected_digest),
        observed_digest,
        tolerance: None,
        evidence: BTreeMap::new(),
    }
}

pub fn verify_approximate_result(
    global_task_id: GlobalTaskId,
    attempt: u32,
    expected: &[f64],
    observed: &[f64],
    absolute_tolerance: f64,
    verifier_id: &str,
) -> ResultVerificationRecord {
    let valid = absolute_tolerance.is_finite()
        && absolute_tolerance >= 0.0
        && expected.len() == observed.len()
        && expected.iter().zip(observed).all(|(left, right)| {
            left.is_finite() && right.is_finite() && (left - right).abs() <= absolute_tolerance
        });
    let expected_bytes = canonical_json(&expected).unwrap_or_default();
    let observed_bytes = canonical_json(&observed).unwrap_or_default();
    ResultVerificationRecord {
        global_task_id,
        attempt,
        policy: ResultVerificationPolicy::DomainVerifier {
            protocol_id: verifier_id.into(),
        },
        status: if valid {
            VerificationStatus::Accepted
        } else {
            VerificationStatus::Quarantined
        },
        verifier_id: verifier_id.into(),
        expected_digest: Some(digest_hex(&expected_bytes)),
        observed_digest: digest_hex(&observed_bytes),
        tolerance: Some(absolute_tolerance),
        evidence: BTreeMap::new(),
    }
}

pub fn adaptive_verification_policy(
    requested: &ResultVerificationPolicy,
    task_value: u8,
    trust_level: NodeTrustLevel,
    irreversible_effects: bool,
) -> ResultVerificationPolicy {
    if irreversible_effects || task_value >= 90 {
        return match requested {
            ResultVerificationPolicy::None | ResultVerificationPolicy::HashOnly => {
                ResultVerificationPolicy::ManualReview
            }
            policy => policy.clone(),
        };
    }
    if trust_level <= NodeTrustLevel::Restricted && task_value >= 50 {
        return match requested {
            ResultVerificationPolicy::None => ResultVerificationPolicy::SpotCheck {
                rate_basis_points: 1000,
            },
            policy => policy.clone(),
        };
    }
    requested.clone()
}

pub fn plan_result_verification(
    policy: &ResultVerificationPolicy,
) -> Result<Vec<VerificationAction>, DistributedError> {
    let actions = match policy {
        ResultVerificationPolicy::None | ResultVerificationPolicy::HashOnly => {
            vec![VerificationAction::AcceptDigest]
        }
        ResultVerificationPolicy::DeterministicReplay => {
            vec![VerificationAction::ReplayOnIndependentNode]
        }
        ResultVerificationPolicy::SpotCheck { rate_basis_points }
            if *rate_basis_points > 0 && *rate_basis_points <= 10_000 =>
        {
            vec![VerificationAction::Sample {
                rate_basis_points: *rate_basis_points,
            }]
        }
        ResultVerificationPolicy::NOfM { required, total }
            if *required > 0 && *total >= *required =>
        {
            vec![VerificationAction::CollectIndependentResults {
                required: *required,
                total: *total,
            }]
        }
        ResultVerificationPolicy::DomainVerifier { protocol_id } if !protocol_id.is_empty() => {
            vec![VerificationAction::InvokeDomainVerifier {
                protocol_id: protocol_id.clone(),
            }]
        }
        ResultVerificationPolicy::Replayable => {
            vec![VerificationAction::ValidateReplayArtifact]
        }
        ResultVerificationPolicy::ProofCarrying { proof_type } if !proof_type.is_empty() => {
            vec![VerificationAction::ValidateProof {
                proof_type: proof_type.clone(),
            }]
        }
        ResultVerificationPolicy::ManualReview => {
            vec![VerificationAction::RequireManualReview]
        }
        _ => return Err(trust_policy_error()),
    };
    Ok(actions)
}

pub fn verify_n_of_m_against_trusted_digest(
    global_task_id: GlobalTaskId,
    attempt: u32,
    observed_digests: &[String],
    required: u8,
    total: u8,
    trusted_expected_digest: Option<&str>,
) -> ResultVerificationRecord {
    let mut evidence = BTreeMap::new();
    evidence.insert("observations".into(), observed_digests.len().to_string());
    let matching = trusted_expected_digest.map_or(0, |expected| {
        observed_digests
            .iter()
            .filter(|digest| digest.as_str() == expected)
            .count()
    });
    let valid_shape = required > 0
        && total >= required
        && observed_digests.len() <= usize::from(total)
        && observed_digests.len() >= usize::from(required);
    let status = if trusted_expected_digest.is_none() {
        VerificationStatus::ManualReview
    } else if valid_shape && matching >= usize::from(required) {
        VerificationStatus::Accepted
    } else {
        VerificationStatus::Quarantined
    };
    ResultVerificationRecord {
        global_task_id,
        attempt,
        policy: ResultVerificationPolicy::NOfM { required, total },
        status,
        verifier_id: "n-of-m-against-trusted-digest".into(),
        expected_digest: trusted_expected_digest.map(str::to_owned),
        observed_digest: digest_hex(observed_digests.join(":").as_bytes()),
        tolerance: None,
        evidence,
    }
}

pub struct TrustBudgetMeter {
    budget: TrustPlaneBudget,
    signatures: u32,
    verifications: u32,
    replays: u32,
    audit_bytes: u64,
    reputation_updates: u32,
    attestations: u32,
    compute_units: u64,
    network_bytes: u64,
    storage_bytes: u64,
}

impl TrustBudgetMeter {
    pub fn new(budget: TrustPlaneBudget) -> Result<Self, DistributedError> {
        if budget.max_signatures_per_tick == 0
            || budget.max_verifications_per_tick == 0
            || budget.max_replays_per_tick == 0
            || budget.max_audit_events_per_segment == 0
            || budget.max_audit_metadata_entries == 0
            || budget.max_audit_bytes_per_tick == 0
            || budget.max_reputation_updates_per_tick == 0
            || budget.max_attestations_per_tick == 0
            || budget.max_compute_units_per_tick == 0
            || budget.max_network_bytes_per_tick == 0
            || budget.max_storage_bytes_per_tick == 0
        {
            return Err(trust_config_error());
        }
        Ok(Self {
            budget,
            signatures: 0,
            verifications: 0,
            replays: 0,
            audit_bytes: 0,
            reputation_updates: 0,
            attestations: 0,
            compute_units: 0,
            network_bytes: 0,
            storage_bytes: 0,
        })
    }

    pub fn admit_signature(&mut self) -> bool {
        admit_count(&mut self.signatures, self.budget.max_signatures_per_tick)
    }

    pub fn admit_verification(&mut self) -> bool {
        admit_count(
            &mut self.verifications,
            self.budget.max_verifications_per_tick,
        )
    }

    pub fn admit_replay(&mut self) -> bool {
        admit_count(&mut self.replays, self.budget.max_replays_per_tick)
    }

    pub fn admit_audit_bytes(&mut self, bytes: u64) -> bool {
        let next = self.audit_bytes.saturating_add(bytes);
        if next > self.budget.max_audit_bytes_per_tick {
            return false;
        }
        self.audit_bytes = next;
        true
    }

    pub fn admit_reputation_update(&mut self) -> bool {
        admit_count(
            &mut self.reputation_updates,
            self.budget.max_reputation_updates_per_tick,
        )
    }

    pub fn admit_attestation(&mut self) -> bool {
        admit_count(
            &mut self.attestations,
            self.budget.max_attestations_per_tick,
        )
    }

    pub fn admit_heavy_work(
        &mut self,
        compute_units: u64,
        network_bytes: u64,
        storage_bytes: u64,
    ) -> bool {
        let next_compute = self.compute_units.saturating_add(compute_units);
        let next_network = self.network_bytes.saturating_add(network_bytes);
        let next_storage = self.storage_bytes.saturating_add(storage_bytes);
        if next_compute > self.budget.max_compute_units_per_tick
            || next_network > self.budget.max_network_bytes_per_tick
            || next_storage > self.budget.max_storage_bytes_per_tick
        {
            return false;
        }
        self.compute_units = next_compute;
        self.network_bytes = next_network;
        self.storage_bytes = next_storage;
        true
    }

    pub fn next_tick(&mut self) {
        self.signatures = 0;
        self.verifications = 0;
        self.replays = 0;
        self.audit_bytes = 0;
        self.reputation_updates = 0;
        self.attestations = 0;
        self.compute_units = 0;
        self.network_bytes = 0;
        self.storage_bytes = 0;
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TrustPlaneFeatureFlags(pub u8);

impl TrustPlaneFeatureFlags {
    pub const MINIMUM_IDENTITY_AUTHENTICATION: Self = Self(1 << 0);
    pub const SIGNED_AUDIT: Self = Self(1 << 1);
    pub const ATTESTATION: Self = Self(1 << 2);
    pub const RESULT_REEXECUTION: Self = Self(1 << 3);

    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn contains(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrustPlaneRuntimeProfile {
    pub enabled: bool,
    pub features: TrustPlaneFeatureFlags,
}

impl TrustPlaneRuntimeProfile {
    pub const fn background_features_active(self) -> bool {
        self.enabled
            && (self.features.contains(TrustPlaneFeatureFlags::SIGNED_AUDIT)
                || self.features.contains(TrustPlaneFeatureFlags::ATTESTATION)
                || self
                    .features
                    .contains(TrustPlaneFeatureFlags::RESULT_REEXECUTION))
    }
}

fn validate_identity(
    identity: &NodeIdentity,
    secret: &[u8],
    now_tick: u64,
) -> Result<(), DistributedError> {
    if identity.key_id.is_empty()
        || identity.key_generation == 0
        || identity.certificate_fingerprint.is_empty()
        || identity.valid_from_tick > now_tick
        || identity.valid_until_tick <= now_tick
        || secret.len() < MIN_KEY_BYTES
        || identity.trust_level == NodeTrustLevel::Quarantined
    {
        return Err(trust_config_error());
    }
    Ok(())
}

fn identity_active_at(identity: &NodeIdentity, tick: u64) -> bool {
    identity.status == IdentityStatus::Active
        && identity.valid_from_tick <= tick
        && identity.valid_until_tick >= tick
}

fn canonical_json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, DistributedError> {
    serde_json::to_vec(value).map_err(|_| {
        DistributedError::new(
            DistributedErrorKind::Protocol,
            "trust plane canonical payload is invalid",
        )
    })
}

pub(crate) fn digest_hex(bytes: &[u8]) -> String {
    hex_bytes(&Sha256::digest(bytes))
}

fn mac_hex(key: &[u8], payload: &[u8]) -> Result<String, DistributedError> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| trust_config_error())?;
    mac.update(payload);
    Ok(hex_bytes(&mac.finalize().into_bytes()))
}

fn verify_mac(key: &[u8], payload: &[u8], tag: &str) -> bool {
    let Ok(tag) = decode_hex(tag) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(key) else {
        return false;
    };
    mac.update(payload);
    mac.verify_slice(&tag).is_ok()
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn decode_hex(value: &str) -> Result<Vec<u8>, ()> {
    if !value.len().is_multiple_of(2) {
        return Err(());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).map_err(|_| ())?;
            u8::from_str_radix(text, 16).map_err(|_| ())
        })
        .collect()
}

fn admit_count(current: &mut u32, maximum: u32) -> bool {
    if *current >= maximum {
        return false;
    }
    *current += 1;
    true
}

const fn trust_config_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::InvalidConfig,
        "trust plane configuration or key material is invalid",
    )
}

const fn trust_policy_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::WorkerRejected,
        "trust policy rejected identity, environment, or data access",
    )
}

const fn identity_unknown() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::WorkerUnavailable,
        "node identity is unknown",
    )
}

const fn identity_unavailable() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Fenced,
        "node identity is expired, revoked, rotated, or quarantined",
    )
}
