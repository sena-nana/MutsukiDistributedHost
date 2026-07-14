use super::*;
use mutsuki_distributed_contracts::{
    AcceptanceMode, ClusterAvailability, ControlNodeKind, ControlOperation, ControlRecord,
    ControlRecordKind, ControlRole, DistributedErrorKind, DurableTaskSpec,
    ExecutedUncommittedResult, GlobalTaskId, GlobalTaskState, MemberHealth, MemberPulseSummary,
    NodeId, ReconciliationDecision,
};
use mutsuki_runtime_contracts::{
    ContentId, ExecutionMobility, PortabilityCapability, PortableTask, RequirementSet, RetrySafety,
    SchemaIdentity, Task, TaskAcceptanceDurability,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

fn node(id: &str) -> NodeId {
    NodeId(id.into())
}

fn specs(temp: &TempDir, kinds: &[(&str, ControlNodeKind)]) -> Vec<ControlNodeSpec> {
    kinds
        .iter()
        .map(|(id, kind)| ControlNodeSpec {
            node_id: node(id),
            kind: *kind,
            storage_path: temp.path().join(format!("{id}.json")),
        })
        .collect()
}

fn three_full(temp: &TempDir) -> Vec<ControlNodeSpec> {
    specs(
        temp,
        &[
            ("node-a", ControlNodeKind::Full),
            ("node-b", ControlNodeKind::Full),
            ("node-c", ControlNodeKind::Full),
        ],
    )
}

fn record(id: &str, kind: ControlRecordKind) -> ControlRecord {
    ControlRecord {
        record_id: id.into(),
        kind,
        metadata: BTreeMap::from([
            ("global_task_id".into(), "global-1".into()),
            ("content_id".into(), "sha256:abc".into()),
        ]),
    }
}

fn durable_spec(id: &str) -> DurableTaskSpec {
    let input = ContentId::new(
        "sha256",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        0,
        "empty",
    );
    DurableTaskSpec {
        global_task_id: GlobalTaskId(id.into()),
        portable: PortableTask::new(
            Task::new("source", "example.ha", json!({ "input": "content:empty" })),
            SchemaIdentity::new("example.ha", "1.0.0"),
            input,
            PortabilityCapability {
                mobility: ExecutionMobility::Restartable,
                retry_safety: RetrySafety::Idempotent,
                task_acceptance: TaskAcceptanceDurability::Persisted,
                ..PortabilityCapability::default()
            },
        ),
        requirements: RequirementSet::default(),
        required_inputs: Vec::new(),
        requested_acceptance: AcceptanceMode::Durable,
    }
}

#[test]
fn leader_kill_elects_successor_and_fences_the_recovered_old_leader() {
    let temp = TempDir::new().unwrap();
    let cluster_specs = three_full(&temp);
    let mut cluster = ReplicatedControlPlane::open(cluster_specs.clone()).unwrap();
    assert_eq!(cluster.elect(&node("node-a")).unwrap(), 1);
    cluster
        .renew_control_lease(&node("node-a"), 0, 100)
        .unwrap();
    let committed = cluster
        .propose_from(
            &node("node-c"),
            record("task-submit", ControlRecordKind::GlobalTask),
            1,
        )
        .unwrap();
    assert_eq!(committed.term, 1);
    assert_eq!(committed.index, 1);
    for id in ["node-a", "node-b", "node-c"] {
        assert_eq!(
            cluster.committed_records(&node(id)).unwrap(),
            vec![committed.clone()]
        );
    }

    let old_grant = cluster
        .issue_grant(
            &node("node-a"),
            GlobalTaskId("global-1".into()),
            1,
            node("node-b"),
            10,
            100,
            false,
        )
        .unwrap();
    cluster.set_alive(&node("node-a"), false).unwrap();
    assert_eq!(cluster.elect(&node("node-b")).unwrap(), 2);
    cluster
        .authorize(
            &node("node-b"),
            ControlOperation::ContinueGranted,
            Some(&old_grant),
            20,
        )
        .unwrap();
    cluster
        .renew_control_lease(&node("node-b"), 20, 100)
        .unwrap();

    cluster.recover(&node("node-a")).unwrap();
    assert_eq!(
        cluster.node_role(&node("node-a")).unwrap(),
        ControlRole::Follower
    );
    assert_eq!(
        cluster
            .propose(
                &node("node-a"),
                record("old-leader-write", ControlRecordKind::ManagementConfig),
            )
            .unwrap_err()
            .kind,
        DistributedErrorKind::NotLeader
    );

    let renewed = cluster
        .issue_grant(
            &node("node-b"),
            GlobalTaskId("global-1".into()),
            1,
            node("node-b"),
            30,
            100,
            false,
        )
        .unwrap();
    assert_eq!(renewed.term, 2);
    assert!(renewed.epoch > old_grant.epoch);
    assert_eq!(
        cluster.validate_result(&old_grant, 31).unwrap_err().kind,
        DistributedErrorKind::Fenced
    );
    cluster.validate_result(&renewed, 31).unwrap();

    drop(cluster);
    let mut reopened = ReplicatedControlPlane::open(cluster_specs).unwrap();
    reopened.validate_result(&renewed, 31).unwrap();
    assert!(reopened.elect(&node("node-b")).unwrap() >= 3);
    assert!(
        reopened
            .committed_records(&node("node-c"))
            .unwrap()
            .iter()
            .any(|entry| entry.record.record_id == "task-submit")
    );
}

#[test]
fn quorum_loss_blocks_global_writes_but_valid_grants_continue_and_reconcile() {
    let temp = TempDir::new().unwrap();
    let mut cluster = ReplicatedControlPlane::open(three_full(&temp)).unwrap();
    cluster.elect(&node("node-a")).unwrap();
    cluster
        .renew_control_lease(&node("node-a"), 0, 100)
        .unwrap();
    let grant = cluster
        .issue_grant(
            &node("node-a"),
            GlobalTaskId("isolated-task".into()),
            1,
            node("node-c"),
            10,
            20,
            false,
        )
        .unwrap();
    cluster.isolate(&node("node-b"), true).unwrap();
    cluster.isolate(&node("node-c"), true).unwrap();
    assert_eq!(
        cluster.availability(&node("node-a"), 20),
        ClusterAvailability::QuorumLost
    );
    assert_eq!(
        cluster.availability(&node("node-c"), 20),
        ClusterAvailability::Isolated
    );
    for operation in [
        ControlOperation::DurableWrite,
        ControlOperation::MembershipChange,
        ControlOperation::GenerationSwitch,
        ControlOperation::IrreversibleEffect,
    ] {
        assert_eq!(
            cluster
                .authorize(&node("node-a"), operation, None, 20)
                .unwrap_err()
                .kind,
            DistributedErrorKind::QuorumLost
        );
    }
    cluster
        .authorize(
            &node("node-c"),
            ControlOperation::ContinueGranted,
            Some(&grant),
            20,
        )
        .unwrap();
    cluster
        .record_uncommitted_result(
            &node("node-c"),
            ExecutedUncommittedResult {
                global_task_id: grant.global_task_id.clone(),
                attempt: grant.attempt,
                worker_node: grant.worker_node.clone(),
                grant_term: grant.term,
                grant_epoch: grant.epoch,
                output_digest: "sha256:isolated-output".into(),
            },
            20,
        )
        .unwrap();

    cluster.isolate(&node("node-b"), false).unwrap();
    cluster.isolate(&node("node-c"), false).unwrap();
    assert_eq!(
        cluster.availability(&node("node-a"), 21),
        ClusterAvailability::Healthy
    );
    let reconciled = cluster
        .reconcile(
            &node("node-a"),
            &grant.global_task_id,
            ReconciliationDecision::ManualReview,
            21,
        )
        .unwrap();
    assert_eq!(reconciled.output_digest, "sha256:isolated-output");
    assert_eq!(
        cluster
            .authorize(
                &node("node-c"),
                ControlOperation::ContinueGranted,
                Some(&grant),
                31,
            )
            .unwrap_err()
            .kind,
        DistributedErrorKind::GrantExpired
    );
}

#[test]
fn two_full_nodes_and_witness_form_quorum_without_allowing_witness_leadership() {
    let temp = TempDir::new().unwrap();
    let mut cluster = ReplicatedControlPlane::open(specs(
        &temp,
        &[
            ("node-a", ControlNodeKind::Full),
            ("node-b", ControlNodeKind::Full),
            ("witness", ControlNodeKind::Witness),
        ],
    ))
    .unwrap();
    assert!(cluster.elect(&node("witness")).is_err());
    cluster.elect(&node("node-a")).unwrap();
    cluster.set_alive(&node("node-a"), false).unwrap();
    assert_eq!(cluster.elect(&node("node-b")).unwrap(), 2);
    assert_eq!(cluster.quorum_size(), 2);
}

#[test]
fn short_control_lease_expires_before_longer_execution_grant() {
    let temp = TempDir::new().unwrap();
    let mut cluster = ReplicatedControlPlane::open(three_full(&temp)).unwrap();
    cluster.elect(&node("node-a")).unwrap();
    cluster.renew_control_lease(&node("node-a"), 0, 5).unwrap();
    let grant = cluster
        .issue_grant(
            &node("node-a"),
            GlobalTaskId("long-compute".into()),
            1,
            node("node-b"),
            1,
            50,
            false,
        )
        .unwrap();
    assert_eq!(
        cluster
            .authorize(&node("node-a"), ControlOperation::DurableWrite, None, 6,)
            .unwrap_err()
            .kind,
        DistributedErrorKind::ControlLeaseExpired
    );
    cluster
        .authorize(
            &node("node-b"),
            ControlOperation::ContinueGranted,
            Some(&grant),
            6,
        )
        .unwrap();
}

#[test]
fn isolated_node_cannot_self_elect_from_its_own_log() {
    let temp = TempDir::new().unwrap();
    let mut cluster = ReplicatedControlPlane::open(three_full(&temp)).unwrap();
    cluster.elect(&node("node-a")).unwrap();
    cluster.isolate(&node("node-c"), true).unwrap();
    assert_eq!(
        cluster.elect(&node("node-c")).unwrap_err().kind,
        DistributedErrorKind::QuorumLost
    );
}

#[test]
fn control_log_is_bounded_and_queries_are_available_on_every_management_node() {
    let temp = TempDir::new().unwrap();
    let mut cluster = ReplicatedControlPlane::open(three_full(&temp)).unwrap();
    cluster.elect(&node("node-a")).unwrap();
    cluster
        .renew_control_lease(&node("node-a"), 0, 100)
        .unwrap();
    let committed = cluster
        .propose_from(
            &node("node-b"),
            record("resource-meta", ControlRecordKind::ResourceMetadata),
            1,
        )
        .unwrap();
    assert_eq!(committed.epoch, 1);
    assert_eq!(
        cluster.committed_records(&node("node-c")).unwrap()[0],
        committed
    );

    let mut oversized = record("oversized", ControlRecordKind::ResourceMetadata);
    oversized
        .metadata
        .insert("bytes".into(), "x".repeat(70 * 1024));
    assert_eq!(
        cluster
            .propose_from(&node("node-b"), oversized, 1)
            .unwrap_err()
            .kind,
        DistributedErrorKind::CapacityExceeded
    );
}

#[test]
fn phase4_registry_records_roundtrip_through_the_cft_backend() {
    let temp = TempDir::new().unwrap();
    let mut control = ReplicatedControlPlane::open(three_full(&temp)).unwrap();
    control.elect(&node("node-a")).unwrap();
    control
        .renew_control_lease(&node("node-a"), 0, 100)
        .unwrap();
    let control = Arc::new(Mutex::new(control));
    let replica = Arc::new(CftRegistryReplica::new(control.clone(), node("node-b"), 1));
    let resources = ResourceCatalog::open(temp.path().join("catalog.json"), 8, 2).unwrap();
    let primary =
        PersistentRegistry::open(temp.path().join("primary-registry.wal"), vec![replica], 8)
            .unwrap();
    let task_id = GlobalTaskId("registry-over-cft".into());
    let receipt = primary
        .submit(durable_spec(&task_id.0), &resources)
        .unwrap();
    assert_eq!(receipt.state, GlobalTaskState::Persisted);
    drop(primary);

    let restored_path = temp.path().join("restored-registry.wal");
    CftRegistryReplica::restore_registry_wal(
        &control.lock().unwrap(),
        &node("node-c"),
        &restored_path,
    )
    .unwrap();
    let restored = PersistentRegistry::open(restored_path, Vec::new(), 8).unwrap();
    assert_eq!(
        restored.query(&task_id).unwrap().state,
        GlobalTaskState::Persisted
    );
}

#[test]
fn failure_detector_uses_versioned_pulses_and_suspect_before_dead() {
    let mut detector = FailureDetector::new(8, 10, 20, 2, 30).unwrap();
    detector
        .register_full(
            MemberPulseSummary {
                node_id: node("worker"),
                capability_version: 1,
                resource_version: 1,
                pressure_bucket: 0,
                health: MemberHealth::Healthy,
            },
            0,
        )
        .unwrap();
    assert_eq!(detector.next_pulse_after(&node("worker")), Some(30));
    assert_eq!(
        detector
            .pulse(
                &MemberPulseSummary {
                    node_id: node("worker"),
                    capability_version: 1,
                    resource_version: 1,
                    pressure_bucket: 9,
                    health: MemberHealth::Overloaded,
                },
                2,
            )
            .unwrap(),
        PulseDisposition::Accepted
    );
    assert_eq!(detector.next_pulse_after(&node("worker")), Some(30));
    assert_eq!(
        detector
            .pulse(
                &MemberPulseSummary {
                    node_id: node("worker"),
                    capability_version: 2,
                    resource_version: 1,
                    pressure_bucket: 0,
                    health: MemberHealth::Healthy,
                },
                3,
            )
            .unwrap(),
        PulseDisposition::FullSnapshotRequired
    );
    assert_eq!(
        detector.health(&node("worker")),
        Some(MemberHealth::Incompatible)
    );
    detector
        .register_full(
            MemberPulseSummary {
                node_id: node("worker"),
                capability_version: 2,
                resource_version: 1,
                pressure_bucket: 0,
                health: MemberHealth::Healthy,
            },
            4,
        )
        .unwrap();
    detector.advance(14);
    assert_eq!(
        detector.health(&node("worker")),
        Some(MemberHealth::Suspect)
    );
    detector.advance(24);
    assert_eq!(detector.health(&node("worker")), Some(MemberHealth::Dead));
}

#[test]
fn leadership_preference_requires_health_margin_and_sustained_hysteresis() {
    let current = LeaderMetrics {
        p95_network_ms: 10.0,
        storage_healthy: true,
        sleep_risk: 0.1,
        control_capacity: 60.0,
        trusted: true,
    };
    let candidate = LeaderMetrics {
        p95_network_ms: 2.0,
        storage_healthy: true,
        sleep_risk: 0.0,
        control_capacity: 100.0,
        trusted: true,
    };
    let mut preference = LeadershipPreference::new(10.0, 5).unwrap();
    assert!(!preference.should_transfer((&node("a"), current), (&node("b"), candidate), 10));
    assert!(!preference.should_transfer((&node("a"), current), (&node("b"), candidate), 14));
    assert!(preference.should_transfer((&node("a"), current), (&node("b"), candidate), 15));

    let unhealthy = LeaderMetrics {
        storage_healthy: false,
        ..candidate
    };
    assert!(!preference.should_transfer((&node("a"), current), (&node("b"), unhealthy), 30));
}

#[test]
fn availability_distinguishes_impaired_degraded_quorum_lost_and_safe_stop() {
    let temp = TempDir::new().unwrap();
    let mut cluster = ReplicatedControlPlane::open(specs(
        &temp,
        &[
            ("a", ControlNodeKind::Full),
            ("b", ControlNodeKind::Full),
            ("c", ControlNodeKind::Full),
            ("d", ControlNodeKind::Full),
            ("e", ControlNodeKind::Full),
        ],
    ))
    .unwrap();
    cluster.elect(&node("a")).unwrap();
    assert_eq!(
        cluster.availability(&node("a"), 0),
        ClusterAvailability::Healthy
    );
    cluster.set_alive(&node("e"), false).unwrap();
    assert_eq!(
        cluster.availability(&node("a"), 0),
        ClusterAvailability::Impaired
    );
    cluster.set_alive(&node("d"), false).unwrap();
    assert_eq!(
        cluster.availability(&node("a"), 0),
        ClusterAvailability::Degraded
    );
    cluster.set_alive(&node("c"), false).unwrap();
    assert_eq!(
        cluster.availability(&node("a"), 0),
        ClusterAvailability::QuorumLost
    );
    cluster.isolate(&node("b"), true).unwrap();
    assert_eq!(
        cluster.availability(&node("b"), 0),
        ClusterAvailability::SafeStop
    );
}
