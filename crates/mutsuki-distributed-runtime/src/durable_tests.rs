use super::*;
use mutsuki_distributed_contracts::{
    AcceptanceMode, DurableTaskSpec, GlobalTaskId, GlobalTaskState, NodeId, ReplicaHealth,
    ReplicaRecord, ReplicaTarget, ResourcePolicy,
};
use mutsuki_runtime_contracts::{
    CancelPolicy, ContentId, ExecutionMobility, PortabilityCapability, PortableTask,
    RequirementSet, RetrySafety, SchemaIdentity, Task, TaskAcceptanceDurability, TaskHandle,
};
use serde_json::json;
use std::fs;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use tempfile::TempDir;

struct ScriptedReplica {
    id: String,
    failures_remaining: AtomicUsize,
    calls: Mutex<Vec<(String, u64)>>,
}

impl ScriptedReplica {
    fn new(id: &str, failures: usize) -> Self {
        Self {
            id: id.into(),
            failures_remaining: AtomicUsize::new(failures),
            calls: Mutex::new(Vec::new()),
        }
    }
}

impl MetadataReplica for ScriptedReplica {
    fn replica_id(&self) -> &str {
        &self.id
    }

    fn commit(
        &self,
        transaction: &MetadataTransaction,
        _sync: bool,
    ) -> Result<ReplicaCommitAck, DistributedError> {
        self.calls
            .lock()
            .unwrap()
            .push((transaction.transaction_id.clone(), transaction.log_index));
        if self
            .failures_remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(DistributedError::new(
                DistributedErrorKind::Storage,
                "injected replica failure",
            ));
        }
        Ok(ReplicaCommitAck {
            replica_id: self.id.clone(),
            transaction_id: transaction.transaction_id.clone(),
            log_index: transaction.log_index,
        })
    }
}

fn portable(input: ContentId) -> PortableTask {
    let mut task = Task::new(
        "local-source",
        "example.durable",
        json!({ "input": format!("content:{}", input.digest) }),
    );
    task.runner_hint = Some("durable-runner".into());
    PortableTask::new(
        task,
        SchemaIdentity::new("example.durable", "1.0.0"),
        input,
        PortabilityCapability {
            mobility: ExecutionMobility::Restartable,
            retry_safety: RetrySafety::Idempotent,
            task_acceptance: TaskAcceptanceDurability::Persisted,
            ..PortabilityCapability::default()
        },
    )
}

fn spec(id: &str, input: ContentId, acceptance: AcceptanceMode) -> DurableTaskSpec {
    DurableTaskSpec {
        global_task_id: GlobalTaskId(id.into()),
        portable: portable(input.clone()),
        requirements: RequirementSet::default(),
        required_inputs: vec![input],
        requested_acceptance: acceptance,
    }
}

fn healthy_node(node: &str) -> ReplicaRecord {
    ReplicaRecord {
        target: ReplicaTarget::Node(NodeId(node.into())),
        health: ReplicaHealth::Healthy,
        verified_at_epoch: 1,
    }
}

fn local_handle(task_id: &str) -> TaskHandle {
    TaskHandle {
        task_id: task_id.into(),
        protocol_id: "example.durable".into(),
        target_binding_id: None,
        cancel_policy: CancelPolicy::Cascade,
        trace_id: None,
        correlation_id: None,
    }
}

fn two_copy_resource(
    temp: &TempDir,
    name: &str,
    bytes: &[u8],
) -> (
    mutsuki_distributed_contracts::ContentManifest,
    ContentStore,
    ContentStore,
) {
    let chunk_size = if bytes.len() > 1024 { 64 * 1024 } else { 4 };
    let first = ContentStore::open(temp.path().join(format!("{name}-node-a")), chunk_size).unwrap();
    let second =
        ContentStore::open(temp.path().join(format!("{name}-node-b")), chunk_size).unwrap();
    let (manifest, _) = first
        .put_bytes(
            bytes,
            "application/octet-stream",
            ResourcePolicy::Replicated {
                minimum_replicas: 2,
            },
        )
        .unwrap();
    let (replica_manifest, _) = second
        .put_bytes(
            bytes,
            "application/octet-stream",
            ResourcePolicy::Replicated {
                minimum_replicas: 2,
            },
        )
        .unwrap();
    assert_eq!(manifest, replica_manifest);
    (manifest, first, second)
}

#[test]
fn chunk_upload_resumes_after_reopen_and_deduplicates_content() {
    let temp = TempDir::new().unwrap();
    let bytes = b"abcdefghijkl";
    let store = ContentStore::open(temp.path(), 4).unwrap();
    let manifest = store
        .build_manifest(
            bytes,
            "application/octet-stream",
            ResourcePolicy::Reconstructible,
        )
        .unwrap();
    assert_eq!(store.begin_upload(&manifest).unwrap(), vec![0, 1, 2]);
    assert!(
        store
            .write_chunk(&manifest.content_id.digest, 0, &bytes[..4])
            .unwrap()
    );
    drop(store);

    let store = ContentStore::open(temp.path(), 4).unwrap();
    assert_eq!(store.begin_upload(&manifest).unwrap(), vec![1, 2]);
    assert!(
        store
            .write_chunk(&manifest.content_id.digest, 1, &bytes[4..8])
            .unwrap()
    );
    assert!(
        store
            .write_chunk(&manifest.content_id.digest, 2, &bytes[8..])
            .unwrap()
    );
    store.complete_upload(&manifest.content_id.digest).unwrap();
    assert_eq!(store.read_content(&manifest.content_id).unwrap(), bytes);

    let (_, stats) = store
        .put_bytes(
            bytes,
            "application/octet-stream",
            ResourcePolicy::Reconstructible,
        )
        .unwrap();
    assert_eq!(stats.uploaded_chunks, 0);
    assert_eq!(stats.reused_chunks, 3);
    assert_eq!(stats.bytes_uploaded, 0);

    fs::write(
        temp.path().join("chunks").join(&manifest.chunks[0].digest),
        b"xxxx",
    )
    .unwrap();
    assert_eq!(
        store.read_content(&manifest.content_id).unwrap_err().kind,
        DistributedErrorKind::Corrupt
    );
}

#[test]
fn resource_catalog_plans_bounded_repair_and_retention_aware_gc() {
    let temp = TempDir::new().unwrap();
    let store = ContentStore::open(temp.path().join("content"), 4).unwrap();
    let (manifest, _) = store
        .put_bytes(
            b"abcdefgh",
            "blob",
            ResourcePolicy::Replicated {
                minimum_replicas: 2,
            },
        )
        .unwrap();
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 32, 4).unwrap();
    catalog
        .register(manifest.clone(), vec![healthy_node("node-a")], 1, 0)
        .unwrap();
    assert!(catalog.get(&manifest.content_id).unwrap().repair_required);
    assert!(catalog.plan_repairs(7).is_empty());
    let repairs = catalog.plan_repairs(8);
    assert_eq!(repairs.len(), 1);
    assert_eq!(repairs[0].missing_copies, 1);

    catalog
        .record_replica(
            &manifest.content_id,
            ReplicaTarget::Node(NodeId("node-b".into())),
            ReplicaHealth::Healthy,
            2,
        )
        .unwrap();
    assert!(!catalog.get(&manifest.content_id).unwrap().repair_required);
    assert!(catalog.plan_repairs(u64::MAX).is_empty());

    catalog.release_reference(&manifest.content_id, 10).unwrap();
    assert!(catalog.collect_garbage(9, &store, 1).unwrap().is_empty());
    assert_eq!(catalog.collect_garbage(10, &store, 1).unwrap().len(), 1);
    assert!(!store.has_content(&manifest.content_id));
}

#[test]
fn garbage_collection_preserves_chunks_referenced_by_another_manifest() {
    let temp = TempDir::new().unwrap();
    let store = ContentStore::open(temp.path(), 4).unwrap();
    let (first, _) = store
        .put_bytes(b"AAAABBBB", "blob", ResourcePolicy::Reconstructible)
        .unwrap();
    let (second, _) = store
        .put_bytes(b"AAAACCCC", "blob", ResourcePolicy::Reconstructible)
        .unwrap();
    assert_eq!(first.chunks[0].digest, second.chunks[0].digest);
    store.remove_content(&first.content_id).unwrap();
    assert_eq!(store.read_content(&second.content_id).unwrap(), b"AAAACCCC");
    store.remove_content(&second.content_id).unwrap();
    assert!(!store.has_content(&second.content_id));
}

#[test]
fn external_durable_policy_requires_a_verified_external_location() {
    let temp = TempDir::new().unwrap();
    let store = ContentStore::open(temp.path().join("content"), 4).unwrap();
    let (manifest, _) = store
        .put_bytes(b"external", "blob", ResourcePolicy::ExternalDurable)
        .unwrap();
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 8, 2).unwrap();
    catalog
        .register(manifest.clone(), vec![healthy_node("node-a")], 1, 0)
        .unwrap();
    assert!(!catalog.get(&manifest.content_id).unwrap().is_recoverable());
    assert_eq!(catalog.plan_repairs(u64::MAX).len(), 1);
    catalog
        .record_replica(
            &manifest.content_id,
            ReplicaTarget::External("s3://durable-bucket/object".into()),
            ReplicaHealth::Healthy,
            2,
        )
        .unwrap();
    assert!(catalog.get(&manifest.content_id).unwrap().is_recoverable());
    assert!(catalog.plan_repairs(u64::MAX).is_empty());
}

#[test]
fn acceptance_modes_are_explicit_and_durable_records_survive_entry_loss() {
    let temp = TempDir::new().unwrap();
    let (input_manifest, input_a, input_b) = two_copy_resource(&temp, "input", b"durable-input");
    assert!(input_a.has_content(&input_manifest.content_id));
    assert!(input_b.has_content(&input_manifest.content_id));
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 32, 4).unwrap();
    catalog
        .register(
            input_manifest.clone(),
            vec![healthy_node("node-a"), healthy_node("node-b")],
            1,
            0,
        )
        .unwrap();

    let follower_path = temp.path().join("follower.wal");
    let replica = Arc::new(FileMetadataReplica::open(&follower_path).unwrap());
    let primary =
        PersistentRegistry::open(temp.path().join("primary.wal"), vec![replica.clone()], 32)
            .unwrap();
    let receipt = primary
        .submit(
            spec(
                "critical-task",
                input_manifest.content_id.clone(),
                AcceptanceMode::Critical {
                    minimum_replicas: 2,
                },
            ),
            &catalog,
        )
        .unwrap();
    assert_eq!(receipt.actual, receipt.requested);
    assert_eq!(receipt.state, GlobalTaskState::Persisted);
    assert_eq!(receipt.metadata_copies, 2);
    assert_eq!(receipt.input_copies, 2);
    assert_eq!(receipt.log_index, 1);
    assert_eq!(receipt.replica_commits.len(), 1);
    assert_eq!(
        receipt.replica_commits[0].transaction_id,
        receipt.transaction_id
    );
    drop(primary);
    drop(replica);

    let follower = PersistentRegistry::open(&follower_path, Vec::new(), 32).unwrap();
    assert_eq!(
        follower
            .query(&GlobalTaskId("critical-task".into()))
            .unwrap()
            .state,
        GlobalTaskState::Persisted
    );
}

#[test]
fn durable_acceptance_fails_without_real_metadata_or_input_redundancy() {
    let temp = TempDir::new().unwrap();
    let store = ContentStore::open(temp.path().join("content"), 4).unwrap();
    let (manifest, _) = store
        .put_bytes(b"ephemeral", "blob", ResourcePolicy::Ephemeral)
        .unwrap();
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 16, 4).unwrap();
    catalog
        .register(manifest.clone(), vec![healthy_node("node-a")], 1, 0)
        .unwrap();
    let registry =
        PersistentRegistry::open(temp.path().join("registry.wal"), Vec::new(), 16).unwrap();
    let fast = registry
        .submit(
            spec(
                "best-effort",
                manifest.content_id.clone(),
                AcceptanceMode::Fast,
            ),
            &catalog,
        )
        .unwrap();
    assert_eq!(fast.actual, AcceptanceMode::Fast);
    assert_eq!(fast.state, GlobalTaskState::Submitted);
    assert_eq!(fast.metadata_copies, 1);
    assert_eq!(
        registry
            .submit(
                spec("not-durable", manifest.content_id, AcceptanceMode::Durable,),
                &catalog,
            )
            .unwrap_err()
            .kind,
        DistributedErrorKind::DurabilityUnavailable
    );
}

#[test]
fn task_failure_recovery_and_cancellation_have_explicit_terminal_states() {
    let temp = TempDir::new().unwrap();
    let store = ContentStore::open(temp.path().join("content"), 4).unwrap();
    let (manifest, _) = store
        .put_bytes(b"input", "blob", ResourcePolicy::Reconstructible)
        .unwrap();
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 16, 4).unwrap();
    catalog
        .register(manifest.clone(), vec![healthy_node("origin")], 3, 0)
        .unwrap();
    let registry =
        PersistentRegistry::open(temp.path().join("registry.wal"), Vec::new(), 16).unwrap();

    for id in ["failed", "recovery", "cancelled"] {
        registry
            .submit(
                spec(id, manifest.content_id.clone(), AcceptanceMode::Fast),
                &catalog,
            )
            .unwrap();
    }
    registry
        .fail(&GlobalTaskId("failed".into()), "worker error", false)
        .unwrap();
    registry
        .fail(
            &GlobalTaskId("recovery".into()),
            "external effect uncertain",
            true,
        )
        .unwrap();
    registry.cancel(&GlobalTaskId("cancelled".into())).unwrap();
    assert_eq!(
        registry
            .query(&GlobalTaskId("failed".into()))
            .unwrap()
            .state,
        GlobalTaskState::Failed
    );
    assert_eq!(
        registry
            .query(&GlobalTaskId("recovery".into()))
            .unwrap()
            .state,
        GlobalTaskState::RecoveryRequired
    );
    assert_eq!(
        registry
            .query(&GlobalTaskId("cancelled".into()))
            .unwrap()
            .state,
        GlobalTaskState::Cancelled
    );
}

#[test]
fn direct_resource_and_result_lanes_obey_independent_data_budget() {
    let content_id = ContentId::new(
        "sha256",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        8,
        "blob",
    );
    let mut queue = DataTransferQueue::new(DataTransferBudget {
        max_concurrent: 1,
        max_queued_bytes: 8,
        max_chunk_bytes: 4,
    })
    .unwrap();
    for (lane, index) in [(DataLane::Resource, 0), (DataLane::Result, 1)] {
        queue
            .enqueue(TransferChunk {
                lane,
                source: NodeId("origin".into()),
                target: NodeId("worker".into()),
                content_id: content_id.clone(),
                index,
                bytes: vec![u8::try_from(index).unwrap(); 4],
            })
            .unwrap();
    }
    assert_eq!(queue.queued_bytes(), 8);
    assert_eq!(
        queue
            .enqueue(TransferChunk {
                lane: DataLane::Resource,
                source: NodeId("origin".into()),
                target: NodeId("worker".into()),
                content_id,
                index: 2,
                bytes: vec![2; 1],
            })
            .unwrap_err()
            .kind,
        DistributedErrorKind::CapacityExceeded
    );
    let first = queue.start_next().unwrap();
    assert_eq!(first.lane, DataLane::Resource);
    assert_eq!(first.source, NodeId("origin".into()));
    assert_eq!(first.target, NodeId("worker".into()));
    assert!(queue.start_next().is_none());
    queue.complete_one().unwrap();
    assert_eq!(queue.start_next().unwrap().lane, DataLane::Result);
}

#[test]
fn output_is_staged_then_committed_by_another_management_node() {
    let temp = TempDir::new().unwrap();
    let (input, _input_a, _input_b) = two_copy_resource(&temp, "input", b"input");
    let (output, output_a, output_b) = two_copy_resource(&temp, "output", b"final-output");
    assert!(output_a.has_content(&output.content_id));
    assert!(output_b.has_content(&output.content_id));
    let catalog_path = temp.path().join("catalog.json");
    let mut catalog = ResourceCatalog::open(&catalog_path, 32, 4).unwrap();
    catalog
        .register(
            input.clone(),
            vec![healthy_node("node-a"), healthy_node("node-b")],
            1,
            0,
        )
        .unwrap();
    catalog
        .register(
            output.clone(),
            vec![healthy_node("node-a"), healthy_node("node-b")],
            1,
            0,
        )
        .unwrap();

    let follower_path = temp.path().join("follower.wal");
    let replica = Arc::new(FileMetadataReplica::open(&follower_path).unwrap());
    let primary =
        PersistentRegistry::open(temp.path().join("primary.wal"), vec![replica.clone()], 32)
            .unwrap();
    let task_id = GlobalTaskId("two-phase".into());
    primary
        .submit(
            spec("two-phase", input.content_id, AcceptanceMode::Durable),
            &catalog,
        )
        .unwrap();
    primary
        .assign(
            &task_id,
            1,
            NodeId("worker-a".into()),
            Some(local_handle("worker-local-attempt-1")),
            1,
        )
        .unwrap();
    assert_eq!(
        primary
            .query(&task_id)
            .unwrap()
            .active_attempt()
            .unwrap()
            .local_handle
            .as_ref()
            .unwrap()
            .task_id,
        "worker-local-attempt-1"
    );
    primary.mark_running(&task_id, 1).unwrap();
    primary
        .stage_output(
            &task_id,
            1,
            NodeId("worker-a".into()),
            output.content_id.clone(),
            &catalog,
        )
        .unwrap();
    assert_eq!(
        primary.query(&task_id).unwrap().state,
        GlobalTaskState::OutputStaged
    );
    assert!(primary.query(&task_id).unwrap().committed_output.is_none());
    drop(primary);
    drop(replica);

    let third = Arc::new(FileMetadataReplica::open(temp.path().join("third.wal")).unwrap());
    let successor = PersistentRegistry::open(&follower_path, vec![third], 32).unwrap();
    let reopened_catalog = ResourceCatalog::open(&catalog_path, 32, 4).unwrap();
    assert_eq!(
        successor
            .commit_output(&task_id, &reopened_catalog)
            .unwrap(),
        output.content_id
    );
    assert_eq!(
        successor.query(&task_id).unwrap().state,
        GlobalTaskState::Committed
    );
}

#[test]
fn stale_attempt_output_is_preserved_as_conflict_and_never_committed() {
    let temp = TempDir::new().unwrap();
    let (input, _) = ContentStore::open(temp.path().join("input"), 4)
        .unwrap()
        .put_bytes(b"input", "blob", ResourcePolicy::Reconstructible)
        .unwrap();
    let (output, _) = ContentStore::open(temp.path().join("output"), 4)
        .unwrap()
        .put_bytes(b"output", "blob", ResourcePolicy::Reconstructible)
        .unwrap();
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 16, 4).unwrap();
    catalog
        .register(input.clone(), vec![healthy_node("origin")], 1, 0)
        .unwrap();
    catalog
        .register(output.clone(), vec![healthy_node("worker")], 1, 0)
        .unwrap();
    let registry =
        PersistentRegistry::open(temp.path().join("registry.wal"), Vec::new(), 16).unwrap();
    let task_id = GlobalTaskId("stale-output".into());
    registry
        .submit(
            spec("stale-output", input.content_id, AcceptanceMode::Fast),
            &catalog,
        )
        .unwrap();
    registry
        .assign(&task_id, 1, NodeId("worker-old".into()), None, 1)
        .unwrap();
    registry
        .assign(&task_id, 2, NodeId("worker-new".into()), None, 1)
        .unwrap();
    assert_eq!(
        registry
            .stage_output(
                &task_id,
                1,
                NodeId("worker-old".into()),
                output.content_id,
                &catalog,
            )
            .unwrap_err()
            .kind,
        DistributedErrorKind::AttemptStale
    );
    let record = registry.query(&task_id).unwrap();
    assert_eq!(record.state, GlobalTaskState::Assigned);
    assert_eq!(record.conflicts.len(), 1);
    assert!(record.committed_output.is_none());
}

#[test]
fn large_input_bytes_never_enter_the_registry_log() {
    let temp = TempDir::new().unwrap();
    let bytes = vec![b'Z'; 2 * 1024 * 1024];
    let (manifest, _a, _b) = two_copy_resource(&temp, "large", &bytes);
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 16, 4).unwrap();
    catalog
        .register(
            manifest.clone(),
            vec![healthy_node("node-a"), healthy_node("node-b")],
            1,
            0,
        )
        .unwrap();
    let replica = Arc::new(FileMetadataReplica::open(temp.path().join("replica.wal")).unwrap());
    let registry_path = temp.path().join("registry.wal");
    let registry = PersistentRegistry::open(&registry_path, vec![replica], 16).unwrap();
    registry
        .submit(
            spec("large", manifest.content_id, AcceptanceMode::Durable),
            &catalog,
        )
        .unwrap();
    let log = fs::read(&registry_path).unwrap();
    assert!(log.len() < 64 * 1024);
    assert!(
        !log.windows(1024)
            .any(|window| window.iter().all(|byte| *byte == b'Z'))
    );
}

#[test]
fn half_written_registry_tail_is_truncated_without_losing_committed_state() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("registry.wal");
    let store = ContentStore::open(temp.path().join("content"), 4).unwrap();
    let (manifest, _) = store
        .put_bytes(b"tail", "blob", ResourcePolicy::Reconstructible)
        .unwrap();
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 8, 2).unwrap();
    catalog
        .register(manifest.clone(), vec![healthy_node("origin")], 1, 0)
        .unwrap();
    let registry = PersistentRegistry::open(&path, Vec::new(), 8).unwrap();
    registry
        .submit(
            spec("tail", manifest.content_id, AcceptanceMode::Fast),
            &catalog,
        )
        .unwrap();
    drop(registry);
    let committed = fs::read(&path).unwrap();
    let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&committed[..committed.len() / 3]).unwrap();
    file.sync_all().unwrap();

    let reopened = PersistentRegistry::open(&path, Vec::new(), 8).unwrap();
    assert!(reopened.query(&GlobalTaskId("tail".into())).is_some());
    assert!(reopened.stats().unwrap().recovered_tail_bytes > 0);
    assert_eq!(fs::metadata(path).unwrap().len(), committed.len() as u64);
}

#[test]
fn checksum_corruption_reports_the_exact_frame_offset() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("registry.wal");
    let store = ContentStore::open(temp.path().join("content"), 4).unwrap();
    let (manifest, _) = store
        .put_bytes(b"checksum", "blob", ResourcePolicy::Reconstructible)
        .unwrap();
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 8, 2).unwrap();
    catalog
        .register(manifest.clone(), vec![healthy_node("origin")], 1, 0)
        .unwrap();
    PersistentRegistry::open(&path, Vec::new(), 8)
        .unwrap()
        .submit(
            spec("checksum", manifest.content_id, AcceptanceMode::Fast),
            &catalog,
        )
        .unwrap();
    let mut bytes = fs::read(&path).unwrap();
    bytes[32] ^= 0x5a;
    fs::write(&path, bytes).unwrap();
    let error = PersistentRegistry::open(path, Vec::new(), 8).err().unwrap();
    assert_eq!(error.kind, DistributedErrorKind::Corrupt);
    assert_eq!(
        error.detail.as_deref(),
        Some("WAL corruption at byte offset 0")
    );
}

#[test]
fn failed_local_prepare_or_commit_never_publishes_memory_or_replay_state() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("registry.wal");
    let store = ContentStore::open(temp.path().join("content"), 4).unwrap();
    let (manifest, _) = store
        .put_bytes(b"local-failure", "blob", ResourcePolicy::Reconstructible)
        .unwrap();
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 8, 2).unwrap();
    catalog
        .register(manifest.clone(), vec![healthy_node("origin")], 1, 0)
        .unwrap();

    let registry = PersistentRegistry::open(&path, Vec::new(), 8).unwrap();
    registry.inject_next_local_write_failure();
    assert_eq!(
        registry
            .submit(
                spec(
                    "prepare-fails",
                    manifest.content_id.clone(),
                    AcceptanceMode::Fast
                ),
                &catalog,
            )
            .unwrap_err()
            .kind,
        DistributedErrorKind::Storage
    );
    assert!(
        registry
            .query(&GlobalTaskId("prepare-fails".into()))
            .is_none()
    );

    registry.inject_local_write_failure_after(1);
    let commit_failure_spec = spec("commit-fails", manifest.content_id, AcceptanceMode::Fast);
    assert_eq!(
        registry
            .submit(commit_failure_spec.clone(), &catalog)
            .unwrap_err()
            .kind,
        DistributedErrorKind::Storage
    );
    assert!(
        registry
            .query(&GlobalTaskId("commit-fails".into()))
            .is_none()
    );
    drop(registry);
    let reopened = PersistentRegistry::open(path, Vec::new(), 8).unwrap();
    assert!(
        reopened
            .query(&GlobalTaskId("prepare-fails".into()))
            .is_none()
    );
    assert!(
        reopened
            .query(&GlobalTaskId("commit-fails".into()))
            .is_none()
    );
    reopened.submit(commit_failure_spec, &catalog).unwrap();
    assert!(
        reopened
            .query(&GlobalTaskId("commit-fails".into()))
            .is_some()
    );
}

#[test]
fn replica_selection_skips_failures_and_retry_reuses_the_transaction() {
    let temp = TempDir::new().unwrap();
    let (input, _a, _b) = two_copy_resource(&temp, "replica-input", b"replica-input");
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 8, 2).unwrap();
    catalog
        .register(
            input.clone(),
            vec![healthy_node("a"), healthy_node("b")],
            1,
            0,
        )
        .unwrap();
    let first = Arc::new(ScriptedReplica::new("first", usize::MAX));
    let second = Arc::new(ScriptedReplica::new("second", 0));
    let registry = PersistentRegistry::open(
        temp.path().join("registry.wal"),
        vec![first.clone(), second.clone()],
        8,
    )
    .unwrap();
    let receipt = registry
        .submit(
            spec(
                "fallback",
                input.content_id.clone(),
                AcceptanceMode::Durable,
            ),
            &catalog,
        )
        .unwrap();
    assert_eq!(receipt.replica_commits.len(), 1);
    assert_eq!(receipt.replica_commits[0].replica_id, "second");
    let report = registry.last_commit_report().unwrap();
    assert_eq!(report.replica_attempts.len(), 2);
    assert!(matches!(
        report.replica_attempts[0].outcome,
        ReplicaCommitOutcome::Failed(DistributedErrorKind::Storage)
    ));

    let flaky = Arc::new(ScriptedReplica::new("flaky", 1));
    let retry_registry =
        PersistentRegistry::open(temp.path().join("retry.wal"), vec![flaky.clone()], 8).unwrap();
    let retry_spec = spec(
        "retry-same-transaction",
        input.content_id,
        AcceptanceMode::Durable,
    );
    let first_error = retry_registry
        .submit(retry_spec.clone(), &catalog)
        .unwrap_err();
    assert_eq!(
        first_error.kind,
        DistributedErrorKind::DurabilityUnavailable
    );
    let receipt = retry_registry.submit(retry_spec, &catalog).unwrap();
    let calls = flaky.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], calls[1]);
    assert_eq!(receipt.transaction_id, calls[0].0);
}

#[test]
fn replica_leading_local_commit_failure_recovers_the_prepared_transaction() {
    let temp = TempDir::new().unwrap();
    let (input, _a, _b) = two_copy_resource(&temp, "leading-input", b"leading-input");
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 8, 2).unwrap();
    catalog
        .register(
            input.clone(),
            vec![healthy_node("a"), healthy_node("b")],
            1,
            0,
        )
        .unwrap();
    let primary_path = temp.path().join("primary.wal");
    let replica_path = temp.path().join("replica.wal");
    let replica = Arc::new(FileMetadataReplica::open(&replica_path).unwrap());
    let registry = PersistentRegistry::open(&primary_path, vec![replica], 8).unwrap();
    let durable_spec = spec("replica-leading", input.content_id, AcceptanceMode::Durable);
    registry.inject_local_write_failure_after(1);
    assert_eq!(
        registry
            .submit(durable_spec.clone(), &catalog)
            .unwrap_err()
            .kind,
        DistributedErrorKind::Storage
    );
    let failed_report = registry.last_commit_report().unwrap();
    assert!(!failed_report.local_committed);
    assert!(matches!(
        failed_report.replica_attempts[0].outcome,
        ReplicaCommitOutcome::Committed(_)
    ));
    drop(registry);

    let replica = Arc::new(FileMetadataReplica::open(&replica_path).unwrap());
    let recovered = PersistentRegistry::open(&primary_path, vec![replica], 8).unwrap();
    assert!(
        recovered
            .query(&GlobalTaskId("replica-leading".into()))
            .is_none()
    );
    let receipt = recovered.submit(durable_spec, &catalog).unwrap();
    assert_eq!(receipt.transaction_id, failed_report.transaction_id);
    assert_eq!(receipt.log_index, failed_report.log_index);
    assert!(
        recovered
            .query(&GlobalTaskId("replica-leading".into()))
            .is_some()
    );
}

#[test]
fn concurrent_transitions_linearize_and_replay_to_the_same_record() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("registry.wal");
    let store = ContentStore::open(temp.path().join("content"), 4).unwrap();
    let (manifest, _) = store
        .put_bytes(b"race", "blob", ResourcePolicy::Reconstructible)
        .unwrap();
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 8, 2).unwrap();
    catalog
        .register(manifest.clone(), vec![healthy_node("origin")], 1, 0)
        .unwrap();
    let registry = Arc::new(PersistentRegistry::open(&path, Vec::new(), 8).unwrap());
    let task_id = GlobalTaskId("linearized".into());
    registry
        .submit(
            spec("linearized", manifest.content_id, AcceptanceMode::Fast),
            &catalog,
        )
        .unwrap();
    let barrier = Arc::new(Barrier::new(33));
    let mut workers = Vec::new();
    for _ in 0..32 {
        let registry = Arc::clone(&registry);
        let barrier = Arc::clone(&barrier);
        let task_id = task_id.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            registry.assign(&task_id, 1, NodeId("worker".into()), None, 1)
        }));
    }
    barrier.wait();
    let successes = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .count();
    assert_eq!(successes, 1);
    let before = registry.query(&task_id).unwrap();
    drop(registry);
    let reopened = PersistentRegistry::open(path, Vec::new(), 8).unwrap();
    assert_eq!(reopened.query(&task_id), Some(before));
    assert_eq!(reopened.stats().unwrap().last_log_index, 2);
}

#[test]
fn snapshot_compaction_recovers_before_and_after_wal_truncation() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("registry.wal");
    let store = ContentStore::open(temp.path().join("content"), 4).unwrap();
    let (manifest, _) = store
        .put_bytes(b"snapshot", "blob", ResourcePolicy::Reconstructible)
        .unwrap();
    let mut catalog = ResourceCatalog::open(temp.path().join("catalog.json"), 8, 2).unwrap();
    catalog
        .register(manifest.clone(), vec![healthy_node("origin")], 1, 0)
        .unwrap();
    let options = RegistryOptions {
        max_tasks: 8,
        max_record_bytes: 64 * 1024,
        compact_after_records: 2,
        compact_after_bytes: u64::MAX,
    };
    let registry = PersistentRegistry::open_with_options(&path, Vec::new(), options).unwrap();
    registry
        .submit(
            spec(
                "snapshot-a",
                manifest.content_id.clone(),
                AcceptanceMode::Fast,
            ),
            &catalog,
        )
        .unwrap();
    let pre_compaction_wal = fs::read(&path).unwrap();
    registry
        .submit(
            spec("snapshot-b", manifest.content_id, AcceptanceMode::Fast),
            &catalog,
        )
        .unwrap();
    let stats = registry.stats().unwrap();
    assert_eq!(stats.wal_transactions, 0);
    assert_eq!(stats.wal_bytes, 0);
    assert!(stats.snapshot_path.exists());
    drop(registry);

    fs::write(&path, pre_compaction_wal).unwrap();
    fs::write(
        format!("{}.snapshot.tmp", path.display()),
        b"interrupted snapshot",
    )
    .unwrap();
    let reopened = PersistentRegistry::open_with_options(&path, Vec::new(), options).unwrap();
    assert!(reopened.query(&GlobalTaskId("snapshot-a".into())).is_some());
    assert!(reopened.query(&GlobalTaskId("snapshot-b".into())).is_some());
    assert_eq!(reopened.stats().unwrap().last_log_index, 2);
}
