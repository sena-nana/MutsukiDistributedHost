use super::*;
use mutsuki_distributed_contracts::*;
use mutsuki_link_core::{ConnectionQuality, PeerId, SecurityLevel};
use mutsuki_runtime_contracts::ContentId;
use std::collections::{BTreeMap, BTreeSet};
use tempfile::TempDir;

const NODE_KEY: [u8; 32] = [7; 32];
const ROTATED_KEY: [u8; 32] = [8; 32];
const AUTHORITY_KEY: [u8; 32] = [9; 32];

fn identity(node: &str, trust: NodeTrustLevel) -> NodeIdentity {
    NodeIdentity {
        node_id: NodeId(node.into()),
        key_id: format!("{node}-key-1"),
        key_generation: 1,
        certificate_fingerprint: format!("cert-{node}"),
        valid_from_tick: 1,
        valid_until_tick: 100,
        status: IdentityStatus::Pending,
        trust_level: trust,
    }
}

fn registry(node: &str, trust: NodeTrustLevel) -> NodeIdentityRegistry {
    let mut registry = NodeIdentityRegistry::default();
    registry
        .approve(identity(node, trust), NODE_KEY.to_vec(), 1)
        .unwrap();
    registry
}

fn authorizer() -> ResourceAuthorizer {
    ResourceAuthorizer::new("cluster-authority".into(), AUTHORITY_KEY.to_vec()).unwrap()
}

fn content(digest: &str) -> ContentId {
    ContentId::new("sha256", digest, 64, "blob")
}

fn budget() -> TrustPlaneBudget {
    TrustPlaneBudget {
        max_signatures_per_tick: 4,
        max_verifications_per_tick: 4,
        max_replays_per_tick: 2,
        max_audit_events_per_segment: 2,
        max_audit_metadata_entries: 8,
        max_audit_bytes_per_tick: 64 * 1024,
        max_reputation_updates_per_tick: 4,
        max_attestations_per_tick: 2,
        max_compute_units_per_tick: 100,
        max_network_bytes_per_tick: 1024,
        max_storage_bytes_per_tick: 1024,
    }
}

#[test]
fn encrypted_identity_rotation_revocation_and_resource_leases_are_fenced() {
    let plain = LinkSessionBinding {
        peer_id: PeerId::from_bytes([1; 32]),
        quality: ConnectionQuality::default(),
        security_level: SecurityLevel::Authenticated,
    };
    assert!(require_authenticated_encrypted_link(&plain).is_err());
    let encrypted = LinkSessionBinding {
        security_level: SecurityLevel::AuthenticatedEncrypted,
        ..plain
    };
    require_authenticated_encrypted_link(&encrypted).unwrap();

    let node_id = NodeId("worker".into());
    let mut registry = registry("worker", NodeTrustLevel::Managed);
    let mut authorizer = authorizer();
    let authorization = authorizer
        .grant(
            &registry,
            &node_id,
            GlobalTaskId("task".into()),
            1,
            3,
            5,
            vec![content("input")],
            BTreeSet::from(["read".into()]),
            false,
            2,
            10,
        )
        .unwrap();
    assert!(authorizer.validate(&registry, &authorization, 3, 5, 3));
    assert!(!authorizer.validate(&registry, &authorization, 3, 6, 3));

    let rotated = registry
        .rotate(
            &node_id,
            "worker-key-2".into(),
            "cert-worker-2".into(),
            ROTATED_KEY.to_vec(),
            3,
            200,
            3,
        )
        .unwrap();
    assert_eq!(rotated.key_generation, 2);
    assert!(!authorizer.validate(&registry, &authorization, 3, 5, 4));
    registry.revoke(&node_id).unwrap();
    assert!(!registry.eligible(&node_id, NodeTrustLevel::Untrusted, 4));
    assert!(
        authorizer
            .grant(
                &registry,
                &node_id,
                GlobalTaskId("task-2".into()),
                1,
                3,
                5,
                vec![content("input-2")],
                BTreeSet::from(["read".into()]),
                false,
                4,
                10,
            )
            .is_err()
    );
}

#[test]
fn sensitive_policy_artifact_integrity_and_attestation_are_hard_filters() {
    let node_id = NodeId("restricted".into());
    let registry = registry("restricted", NodeTrustLevel::Restricted);
    let engine = TrustPolicyEngine::new(TrustMode::RestrictedWorkers);
    let policy = TaskTrustPolicy {
        sensitivity: DataSensitivity::Confidential,
        minimum_trust: NodeTrustLevel::Restricted,
        flags: TaskTrustFlags::ALLOW_EXTERNAL_WORKERS,
        verification: ResultVerificationPolicy::HashOnly,
        task_value: 50,
    };
    assert!(
        engine
            .authorize_node(&registry, &node_id, &policy, true, None, 2)
            .is_err()
    );

    let mut authority =
        ArtifactVerifier::new(BTreeMap::from([("release".into(), AUTHORITY_KEY.to_vec())]))
            .unwrap();
    let artifact = authority
        .sign_and_allow(ArtifactIdentity {
            artifact_id: "runner".into(),
            artifact_kind: "runner".into(),
            version: "1.0.0".into(),
            generation: 7,
            content_id: content("runner"),
            signer_key_id: "release".into(),
            integrity_tag: String::new(),
        })
        .unwrap();
    assert!(authority.verify(&artifact));
    let mut replaced = artifact.clone();
    replaced.content_id = content("replaced");
    assert!(!authority.verify(&replaced));

    let attestation =
        AttestationVerifier::new(BTreeMap::from([("tpm".into(), AUTHORITY_KEY.to_vec())])).unwrap();
    let evidence = attestation
        .sign_evidence(AttestationEvidence {
            provider: "tpm".into(),
            node_id: node_id.clone(),
            identity_key_id: "restricted-key-1".into(),
            host_content_id: content("host"),
            artifact_content_ids: vec![artifact.content_id.clone()],
            issued_tick: 1,
            valid_until_tick: 20,
            evidence_digest: String::new(),
        })
        .unwrap();
    let verdict = attestation.verify(
        &evidence,
        registry.identity(&node_id).unwrap(),
        &[artifact.content_id],
        2,
    );
    assert!(verdict.accepted);
}

fn unsigned_receipt(node_id: NodeId) -> ExecutionReceipt {
    ExecutionReceipt {
        global_task_id: GlobalTaskId("receipt-task".into()),
        attempt: 2,
        term: 3,
        epoch: 5,
        node_id,
        task_schema: "example.task@1".into(),
        input_content_id: content("input"),
        output_content_id: content("output"),
        runner_id: "runner".into(),
        runner_generation: 2,
        plugin_id: "plugin".into(),
        plugin_generation: 7,
        execution_variant: "cuda-fp16".into(),
        policy_digest: "policy".into(),
        quality: 0.99,
        degraded_flags: BTreeSet::new(),
        environment_digest: "environment".into(),
        identity_key_id: String::new(),
        receipt_tag: String::new(),
    }
}

#[test]
fn receipts_bind_identity_implementation_attempt_epoch_and_commit() {
    let node_id = NodeId("worker".into());
    let registry = registry("worker", NodeTrustLevel::Trusted);
    let receipt = sign_execution_receipt(&registry, unsigned_receipt(node_id.clone()), 2).unwrap();
    let commit = CommitProof {
        log_index: 10,
        term: 3,
        epoch: 5,
        quorum_certificate_digest: Some("quorum".into()),
        audit_segment: Some(1),
        audit_leaf: Some(0),
        audit_root: Some("root".into()),
    };
    verify_execution_receipt(&registry, &receipt, Some(&commit), 2, 3, 5, 2).unwrap();
    assert_eq!(
        verify_execution_receipt(&registry, &receipt, Some(&commit), 1, 3, 5, 2)
            .unwrap_err()
            .kind,
        DistributedErrorKind::Fenced
    );
    let mut tampered = receipt;
    tampered.output_content_id = content("forged-output");
    assert!(verify_execution_receipt(&registry, &tampered, Some(&commit), 2, 3, 5, 2).is_err());

    let action_digest = "trust-root-change";
    let payload = format!(
        "{:?}:{}:{}:{}",
        GovernanceAction::TrustRootChange,
        action_digest,
        3,
        5
    );
    let (_, tag) = registry.sign(&node_id, payload.as_bytes(), 2).unwrap();
    let certificate = GovernanceCertificate {
        action: GovernanceAction::TrustRootChange,
        action_digest: action_digest.into(),
        term: 3,
        epoch: 5,
        required_signers: 1,
        signer_tags: BTreeMap::from([(node_id.clone(), tag)]),
    };
    assert!(verify_governance_certificate(
        &registry,
        &certificate,
        &BTreeSet::from([node_id]),
        2
    ));

    let binding = sign_state_binding(
        &registry,
        TrustBoundObjectKind::ExecutionGrant,
        "grant-digest".into(),
        NodeId("worker".into()),
        &NodeId("worker".into()),
        Some(GlobalTaskId("receipt-task".into())),
        Some(2),
        3,
        5,
        2,
    )
    .unwrap();
    assert!(verify_state_binding(&registry, &binding, 3, 5, 2));
    assert!(!verify_state_binding(&registry, &binding, 3, 6, 2));
}

#[test]
fn deterministic_and_approximate_verification_quarantine_wrong_results() {
    let accepted = verify_deterministic_result(
        GlobalTaskId("deterministic".into()),
        1,
        b"canonical",
        b"canonical",
        "replay-node",
    );
    assert_eq!(accepted.status, VerificationStatus::Accepted);
    let rejected = verify_deterministic_result(
        GlobalTaskId("deterministic".into()),
        1,
        b"canonical",
        b"wrong",
        "replay-node",
    );
    assert_eq!(rejected.status, VerificationStatus::Quarantined);
    let approximate = verify_approximate_result(
        GlobalTaskId("float".into()),
        1,
        &[1.0, 2.0],
        &[1.001, 1.999],
        0.01,
        "vector-tolerance-v1",
    );
    assert_eq!(approximate.status, VerificationStatus::Accepted);
    assert_eq!(approximate.tolerance, Some(0.01));
    assert_eq!(
        adaptive_verification_policy(
            &ResultVerificationPolicy::None,
            95,
            NodeTrustLevel::Trusted,
            false,
        ),
        ResultVerificationPolicy::ManualReview
    );
    assert_eq!(
        plan_result_verification(&ResultVerificationPolicy::NOfM {
            required: 2,
            total: 3,
        })
        .unwrap(),
        vec![VerificationAction::CollectIndependentResults {
            required: 2,
            total: 3,
        }]
    );
    let no_truth = verify_n_of_m_against_trusted_digest(
        GlobalTaskId("valuable".into()),
        1,
        &["fast-majority".into(), "fast-majority".into()],
        2,
        3,
        None,
    );
    assert_eq!(no_truth.status, VerificationStatus::ManualReview);
    let trusted = verify_n_of_m_against_trusted_digest(
        GlobalTaskId("valuable".into()),
        1,
        &["expected".into(), "expected".into(), "wrong".into()],
        2,
        3,
        Some("expected"),
    );
    assert_eq!(trusted.status, VerificationStatus::Accepted);
}

#[test]
fn persistent_audit_chain_merkle_proofs_and_task_trace_are_verifiable() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("audit.jsonl");
    let task_id = GlobalTaskId("audit-task".into());
    let node_id = NodeId("worker".into());
    let mut audit = PersistentAuditLog::open(&path, budget()).unwrap();
    for (kind, phase) in [
        (AuditEventKind::Assignment, "assigned"),
        (AuditEventKind::Lease, "executing"),
        (AuditEventKind::ResultCommit, "committed"),
        (AuditEventKind::Verification, "verified"),
    ] {
        audit
            .append(
                1,
                kind,
                Some(task_id.clone()),
                Some(1),
                Some(node_id.clone()),
                BTreeMap::from([
                    ("phase".into(), phase.into()),
                    ("input_digest".into(), "sha256:input".into()),
                    ("output_digest".into(), "sha256:output".into()),
                    ("plugin_generation".into(), "7".into()),
                ]),
            )
            .unwrap();
    }
    let first = audit.build_segment(1, 1, None).unwrap();
    let second = audit
        .build_segment(2, 3, Some(first.merkle_root.clone()))
        .unwrap();
    let proof = PersistentAuditLog::inclusion_proof(&first, 1).unwrap();
    assert!(PersistentAuditLog::verify_inclusion(&proof));
    assert!(PersistentAuditLog::verify_segment_consistency(
        &first, &second
    ));
    assert_eq!(audit.trace_task(&task_id).len(), 4);
    drop(audit);
    let reopened = PersistentAuditLog::open(&path, budget()).unwrap();
    assert_eq!(reopened.event_count(), 4);
    assert_eq!(reopened.trace_attempt(&task_id, 1).len(), 4);
    assert_eq!(reopened.trace_node(&node_id).len(), 4);
}

#[test]
fn compromise_drill_revokes_access_quarantines_provenance_and_fences_epoch() {
    let node_id = NodeId("compromised".into());
    let task_id = GlobalTaskId("affected".into());
    let output = content("suspect-output");
    let mut registry = registry("compromised", NodeTrustLevel::Managed);
    let mut authorizer = authorizer();
    let authorization = authorizer
        .grant(
            &registry,
            &node_id,
            task_id.clone(),
            1,
            1,
            5,
            vec![content("sensitive-input")],
            BTreeSet::from(["read".into()]),
            false,
            2,
            20,
        )
        .unwrap();
    let mut tracker = CompromiseTracker::new(5).unwrap();
    tracker.record_output(node_id.clone(), task_id.clone(), output.clone());
    let impact = tracker
        .isolate(&mut registry, &mut authorizer, &node_id)
        .unwrap();
    assert_eq!(impact.new_fencing_epoch, 6);
    assert!(impact.affected_tasks.contains(&task_id));
    assert!(impact.quarantined_content.contains(&output));
    assert!(
        impact
            .revoked_authorizations
            .contains(&authorization.authorization_id)
    );
    assert!(!authorizer.validate(&registry, &authorization, 1, 5, 3));
    assert_eq!(
        registry.identity(&node_id).unwrap().status,
        IdentityStatus::Quarantined
    );
    let verdict = AttestationVerdict {
        accepted: true,
        verifier_id: "fresh-integrity-check".into(),
        node_id: node_id.clone(),
        valid_until_tick: 100,
        environment_digest: "fresh-environment".into(),
        reason: None,
    };
    let readmitted = registry
        .readmit(
            &node_id,
            "compromised-key-2".into(),
            "new-certificate".into(),
            ROTATED_KEY.to_vec(),
            NodeTrustLevel::Restricted,
            4,
            100,
            &verdict,
            4,
        )
        .unwrap();
    assert_eq!(readmitted.key_generation, 2);
}

#[test]
fn reputation_is_bounded_slow_and_never_enables_disabled_background_work() {
    let mut reputation = ReputationModel::new(2, 2).unwrap();
    let observation = ReputationObservation {
        flags: ReputationObservationFlags::RESULT_MISMATCH,
        latency_ratio: 5.0,
    };
    let mut snapshot = None;
    for _ in 0..5 {
        snapshot = Some(
            reputation
                .record(NodeId("node".into()), "cuda".into(), 7, observation)
                .unwrap(),
        );
    }
    let snapshot = snapshot.unwrap();
    assert!(snapshot.anomalies.contains(&AnomalyKind::ResultMismatch));
    assert!(snapshot.anomalies.contains(&AnomalyKind::AbnormalLatency));
    assert!(reputation.scheduling_risk(&NodeId("node".into()), "cuda", 7) > 0.0);

    let mut meter = TrustBudgetMeter::new(budget()).unwrap();
    assert!(meter.admit_signature());
    assert!(meter.admit_verification());
    assert!(meter.admit_replay());
    assert!(meter.admit_audit_bytes(1024));
    assert!(meter.admit_heavy_work(50, 512, 512));
    assert!(!meter.admit_heavy_work(51, 0, 0));
    let disabled = TrustPlaneRuntimeProfile {
        enabled: false,
        features: TrustPlaneFeatureFlags::MINIMUM_IDENTITY_AUTHENTICATION
            .union(TrustPlaneFeatureFlags::SIGNED_AUDIT)
            .union(TrustPlaneFeatureFlags::ATTESTATION)
            .union(TrustPlaneFeatureFlags::RESULT_REEXECUTION),
    };
    assert!(!disabled.background_features_active());
}
