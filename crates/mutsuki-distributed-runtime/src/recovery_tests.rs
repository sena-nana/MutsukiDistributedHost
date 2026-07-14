use super::*;
use mutsuki_distributed_contracts::*;
use mutsuki_runtime_contracts::*;
use serde_json::json;
use std::collections::BTreeSet;
use tempfile::TempDir;

fn capability(retry: RetrySafety) -> PortabilityCapability {
    PortabilityCapability {
        mobility: ExecutionMobility::Restartable,
        retry_safety: retry,
        task_acceptance: TaskAcceptanceDurability::Persisted,
        resource_persistence: ResourcePersistence::ContentAddressed,
        recovery: RecoveryMode::RestartFromInput,
    }
}

fn portable(input: ContentId, retry: RetrySafety) -> PortableTask {
    let mut task = Task::new("source", "example.recovery", json!({"input":"content"}));
    task.runner_hint = Some("recovery-runner".into());
    PortableTask::new(
        task,
        SchemaIdentity::new("example.recovery", "1.0.0"),
        input,
        capability(retry),
    )
}

fn checkpoint(input: ContentId, sequence: u64, fill: u8) -> TaskCheckpoint {
    TaskCheckpoint::new(
        SchemaIdentity::new("example.checkpoint", "1.0.0"),
        7,
        sequence,
        portable(input, RetrySafety::Idempotent),
        vec![fill; 1024],
    )
}

#[test]
fn checkpoint_requires_full_baseline_then_deduplicates_and_restores() {
    let temp = TempDir::new().unwrap();
    let input = ContentId::new("sha256", "input", 10, "json");
    let store = ContentStore::open(temp.path(), 128).unwrap();
    let mut manager = CheckpointManager::new(
        store,
        CheckpointBudget {
            max_checkpoint_bytes: 16 * 1024,
            max_uploaded_bytes: 32 * 1024,
            max_operations: 4,
        },
    )
    .unwrap();
    let first_checkpoint = checkpoint(input.clone(), 1, 1);
    let first = manager
        .persist(
            GlobalTaskId("task".into()),
            1,
            &first_checkpoint,
            7,
            None,
            ResourcePolicy::Replicated {
                minimum_replicas: 1,
            },
        )
        .unwrap();
    assert!(first.complete_baseline);
    assert_eq!(first.baseline_content_id, first.checkpoint_content_id);
    let uploaded_after_first = manager.uploaded_bytes();

    let second_checkpoint = checkpoint(input.clone(), 2, 2);
    let second = manager
        .persist(
            GlobalTaskId("task".into()),
            1,
            &second_checkpoint,
            7,
            Some(&first),
            ResourcePolicy::Replicated {
                minimum_replicas: 1,
            },
        )
        .unwrap();
    assert!(!second.complete_baseline);
    assert_eq!(second.baseline_content_id, first.checkpoint_content_id);
    assert_eq!(
        second.previous_content_id,
        Some(first.checkpoint_content_id)
    );
    assert!(!second.changed_chunks.is_empty());
    assert!(manager.uploaded_bytes() > uploaded_after_first);
    assert!(manager.uploaded_bytes() - uploaded_after_first < second.checkpoint_content_id.size);
    let restored = manager
        .restore(&second, &second_checkpoint.task_schema, 7, &input)
        .unwrap();
    assert_eq!(restored.sequence, 2);
    assert_eq!(restored.payload, vec![2; 1024]);
    assert_eq!(
        manager
            .restore(&second, &second_checkpoint.task_schema, 8, &input,)
            .unwrap_err()
            .kind,
        DistributedErrorKind::Incompatible
    );
    assert!(CheckpointManager::adaptive_interval(5, 2, 100, 0.2).is_some());
    assert_eq!(CheckpointManager::adaptive_interval(5, 2, 100, 0.9), None);
}

fn recovery_fixture(
    temp: &TempDir,
    retry: RetrySafety,
) -> (
    DurableTaskRecord,
    ResourceCatalog,
    WorkerAdvertisement,
    RecoveryTarget,
) {
    let store = ContentStore::open(temp.path().join("input"), 64).unwrap();
    let (manifest, _) = store
        .put_bytes(
            b"input",
            "json",
            ResourcePolicy::Replicated {
                minimum_replicas: 1,
            },
        )
        .unwrap();
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 8, 2).unwrap();
    catalog
        .register(
            manifest.clone(),
            vec![ReplicaRecord {
                target: ReplicaTarget::Node(NodeId("worker".into())),
                health: ReplicaHealth::Healthy,
                verified_at_epoch: 1,
            }],
            1,
            0,
        )
        .unwrap();
    let portable = portable(manifest.content_id.clone(), retry);
    let record = DurableTaskRecord {
        spec: DurableTaskSpec {
            global_task_id: GlobalTaskId("recover".into()),
            portable: portable.clone(),
            requirements: RequirementSet::default(),
            required_inputs: vec![manifest.content_id],
            requested_acceptance: AcceptanceMode::Durable,
        },
        state: GlobalTaskState::RecoveryRequired,
        acceptance: AcceptanceMode::Durable,
        metadata_copies: 2,
        attempts: vec![DurableAttempt {
            attempt: 1,
            node_id: NodeId("dead".into()),
            local_handle: None,
            runner_generation: 1,
            active: false,
        }],
        staged_output: None,
        committed_output: None,
        conflicts: Vec::new(),
        failure: Some("worker dead".into()),
    };
    let advertisement = WorkerAdvertisement {
        node_id: NodeId("worker".into()),
        protocol_major: DISTRIBUTED_PROTOCOL_MAJOR,
        snapshot_version: 1,
        capabilities: CapabilitySet::default(),
        portability: PortabilityCatalog {
            tasks: vec![TaskPortabilityDescriptor {
                protocol_id: portable.task.protocol_id.clone(),
                task_schema: portable.task_schema.clone(),
                checkpoint_schema: Some(SchemaIdentity::new("example.checkpoint", "1.0.0")),
                capability: portable.capability.clone(),
            }],
            resources: Vec::new(),
        },
        runners: vec![RunnerGeneration {
            runner_id: "recovery-runner".into(),
            plugin_id: "plugin".into(),
            runner_generation: 2,
            plugin_generation: 7,
        }],
        localized_content: BTreeSet::new(),
        health: WorkerHealth::Ready,
    };
    let target = RecoveryTarget {
        node_id: NodeId("worker".into()),
        runner_generation: 2,
        plugin_generation: 7,
        quality: 1.0,
    };
    (record, catalog, advertisement, target)
}

#[test]
fn recovery_waits_for_dead_applies_backoff_and_refuses_unsafe_retry() {
    let temp = TempDir::new().unwrap();
    let (record, catalog, advertisement, target) = recovery_fixture(&temp, RetrySafety::Idempotent);
    let policy = RecoveryPolicy {
        tier: RecoveryTier::Restartable,
        max_attempts: 3,
        base_backoff_ticks: 5,
        max_backoff_ticks: 20,
        deadline_tick: Some(100),
        allow_speculative: false,
        minimum_quality: 0.9,
    };
    assert_eq!(
        RecoveryPlanner::plan(
            &record,
            &policy,
            FailureConfirmation::Suspect,
            &advertisement,
            target.clone(),
            &catalog,
            None,
            None,
            10,
        ),
        RecoveryAction::Wait
    );
    match RecoveryPlanner::plan(
        &record,
        &policy,
        FailureConfirmation::Dead,
        &advertisement,
        target,
        &catalog,
        None,
        None,
        10,
    ) {
        RecoveryAction::Restart {
            attempt,
            not_before_tick,
            ..
        } => {
            assert_eq!(attempt, 2);
            assert_eq!(not_before_tick, 20);
        }
        action => panic!("unexpected action: {action:?}"),
    }

    let (unsafe_record, unsafe_catalog, unsafe_ad, unsafe_target) =
        recovery_fixture(&TempDir::new().unwrap(), RetrySafety::Unsafe);
    assert!(matches!(
        RecoveryPlanner::plan(
            &unsafe_record,
            &policy,
            FailureConfirmation::Dead,
            &unsafe_ad,
            unsafe_target,
            &unsafe_catalog,
            None,
            None,
            10,
        ),
        RecoveryAction::RecoveryRequired { .. }
    ));
}

#[test]
fn effects_sessions_and_mirroring_are_explicit_and_budgeted() {
    for (capability, expected) in [
        (
            EffectRecoveryCapability {
                retry_safety: RetrySafety::Idempotent,
                idempotency_key: Some("key".into()),
                external_verifier: false,
                transactional_outbox: false,
                compensation_hook: false,
            },
            EffectRecoveryAction::RetryWithIdempotencyKey,
        ),
        (
            EffectRecoveryCapability {
                retry_safety: RetrySafety::Verifiable,
                idempotency_key: None,
                external_verifier: true,
                transactional_outbox: false,
                compensation_hook: false,
            },
            EffectRecoveryAction::VerifyExternalState,
        ),
        (
            EffectRecoveryCapability {
                retry_safety: RetrySafety::Compensatable,
                idempotency_key: None,
                external_verifier: false,
                transactional_outbox: false,
                compensation_hook: true,
            },
            EffectRecoveryAction::CompensateThenRetry,
        ),
    ] {
        assert_eq!(effect_recovery_action(&capability, true), expected);
    }
    assert_eq!(
        effect_recovery_action(
            &EffectRecoveryCapability {
                retry_safety: RetrySafety::Unsafe,
                idempotency_key: None,
                external_verifier: false,
                transactional_outbox: false,
                compensation_hook: false,
            },
            true,
        ),
        EffectRecoveryAction::RecoveryRequired
    );
    assert_eq!(
        effect_recovery_action(
            &EffectRecoveryCapability {
                retry_safety: RetrySafety::Idempotent,
                idempotency_key: Some("effect-key".into()),
                external_verifier: false,
                transactional_outbox: false,
                compensation_hook: false,
            },
            false,
        ),
        EffectRecoveryAction::RejectWhileQuorumLost
    );

    let session = RealtimeSession {
        session_id: "live".into(),
        primary_node: NodeId("primary".into()),
        standby_node: Some(NodeId("standby".into())),
        execution_variant: "quality".into(),
        last_checkpoint: None,
    };
    let estimate = MigrationEstimate {
        future_benefit: 100.0,
        state_transfer_cost: 10.0,
        cold_start_cost: 10.0,
        interruption_cost: 10.0,
        failure_risk_cost: 5.0,
        safety_margin: 5.0,
    };
    assert_eq!(
        decide_session_migration(&session, NodeId("target".into()), estimate, true, false),
        SessionMigrationDecision::Stay
    );
    assert_eq!(
        decide_session_migration(&session, NodeId("target".into()), estimate, false, true),
        SessionMigrationDecision::PromoteStandby {
            target: NodeId("standby".into())
        }
    );

    let mut mirrors = MirrorAdmission::new(MirrorBudget {
        max_sessions: 1,
        max_compute_units: 10,
        max_memory_bytes: 1024,
        max_network_bytes: 2048,
    })
    .unwrap();
    assert!(mirrors.admit("ordinary", false, 1, 1, 1).is_err());
    mirrors.admit("critical", true, 10, 1024, 2048).unwrap();
    assert!(mirrors.admit("second", true, 1, 1, 1).is_err());
    assert!(mirrors.release("critical"));
}
