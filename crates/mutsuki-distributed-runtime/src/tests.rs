use super::*;
use mutsuki_distributed_contracts::RunnerGeneration;
use mutsuki_runtime_contracts::{
    CancelPolicy, CapabilitySet, ContentId, PortabilityCapability, PortableResourceDescriptor,
    ResourcePersistence, RetrySafety, SchemaIdentity, Task, TaskAcceptanceDurability,
    TaskPortabilityDescriptor,
};
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct FakeHost {
    submitted: Mutex<Vec<Task>>,
    cancelled: Mutex<Vec<String>>,
    draining: AtomicBool,
}

impl HostAdapter for FakeHost {
    fn submit_batch(&self, batch: TaskBatch) -> HostFuture<'_, Vec<TaskHandle>> {
        Box::pin(async move {
            if self.draining.load(Ordering::Acquire) {
                return Err(DistributedError::new(
                    DistributedErrorKind::HostUnavailable,
                    "fake Host is draining",
                ));
            }
            let handles = batch.tasks.iter().map(task_handle).collect();
            self.submitted
                .lock()
                .expect("submitted mutex")
                .extend(batch.tasks);
            Ok(handles)
        })
    }

    fn cancel(&self, handle: &TaskHandle) -> HostFuture<'_, ()> {
        let task_id = handle.task_id.clone();
        Box::pin(async move {
            self.cancelled
                .lock()
                .expect("cancelled mutex")
                .push(task_id);
            Ok(())
        })
    }

    fn snapshots(&self) -> HostFuture<'_, Vec<LocalTaskSnapshot>> {
        Box::pin(async move {
            Ok(self
                .submitted
                .lock()
                .expect("submitted mutex")
                .iter()
                .map(|task| LocalTaskSnapshot {
                    task_id: task.task_id.clone(),
                    protocol_id: task.protocol_id.clone(),
                    status: "ready".into(),
                    registry_generation: task.registry_generation,
                    runner_id: task.runner_hint.clone(),
                    lease_id: None,
                })
                .collect())
        })
    }

    fn outcome(&self, handle: &TaskHandle) -> HostFuture<'_, Option<LocalTaskOutcome>> {
        let task_id = handle.task_id.clone();
        Box::pin(async move {
            Ok(Some(LocalTaskOutcome {
                task_id,
                status: "completed".into(),
                output_ref: Some("content:result".into()),
                reason: None,
                error_code: None,
            }))
        })
    }

    fn events_after(&self, _sequence: u64, _limit: usize) -> HostFuture<'_, Vec<RuntimeEvent>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn begin_drain(&self) -> HostFuture<'_, ()> {
        Box::pin(async move {
            self.draining.store(true, Ordering::Release);
            Ok(())
        })
    }

    fn health(&self) -> HostFuture<'_, String> {
        Box::pin(async { Ok("ok".into()) })
    }
}

struct FakeLocalizer {
    calls: AtomicUsize,
    fail: AtomicBool,
}

impl FakeLocalizer {
    fn working() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
        }
    }
}

impl ResourceLocalizer for FakeLocalizer {
    fn localize<'a>(&'a self, _resources: &'a [DirectDataRef]) -> WorkerFuture<'a, ()> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::AcqRel);
            if self.fail.load(Ordering::Acquire) {
                Err(DistributedError::new(
                    DistributedErrorKind::LocalizationFailed,
                    "resource localization failed before local submission",
                ))
            } else {
                Ok(())
            }
        })
    }
}

struct LoopbackTransport {
    dispatcher: Arc<WorkerRequestDispatcher>,
}

impl WorkerTransport for LoopbackTransport {
    fn round_trip(&self, request: Vec<u8>) -> WorkerFuture<'_, Vec<u8>> {
        Box::pin(async move { self.dispatcher.dispatch(&request).await })
    }
}

fn wire(worker: Arc<dyn RemoteWorker>) -> Arc<dyn RemoteWorker> {
    let dispatcher = Arc::new(WorkerRequestDispatcher::new(worker));
    Arc::new(WireRemoteWorker::new(Arc::new(LoopbackTransport {
        dispatcher,
    })))
}

fn task_handle(task: &Task) -> TaskHandle {
    TaskHandle {
        task_id: task.task_id.clone(),
        protocol_id: task.protocol_id.clone(),
        target_binding_id: task.target_binding_id.clone(),
        cancel_policy: CancelPolicy::Cascade,
        trace_id: task.trace_id.clone(),
        correlation_id: task.correlation_id.clone(),
    }
}

fn capability(mobility: ExecutionMobility, retry_safety: RetrySafety) -> PortabilityCapability {
    PortabilityCapability {
        mobility,
        retry_safety,
        task_acceptance: TaskAcceptanceDurability::Volatile,
        ..PortabilityCapability::default()
    }
}

fn portable(mobility: ExecutionMobility, retry_safety: RetrySafety) -> PortableTask {
    let mut task = Task::new("source-task", "example.compute", json!({ "value": 7 }));
    task.runner_hint = Some("same-plugin-runner".into());
    PortableTask::new(
        task,
        SchemaIdentity::new("example.compute", "1.0.0"),
        ContentId::new("sha256", "input", 1024, "json"),
        capability(mobility, retry_safety),
    )
}

fn advertisement(node: &str, portable: &PortableTask) -> WorkerAdvertisement {
    WorkerAdvertisement {
        node_id: NodeId(node.into()),
        protocol_major: DISTRIBUTED_PROTOCOL_MAJOR,
        snapshot_version: 1,
        capabilities: CapabilitySet::default(),
        portability: mutsuki_runtime_contracts::PortabilityCatalog {
            tasks: vec![TaskPortabilityDescriptor {
                protocol_id: portable.task.protocol_id.clone(),
                task_schema: portable.task_schema.clone(),
                checkpoint_schema: None,
                capability: portable.capability.clone(),
            }],
            resources: Vec::new(),
        },
        runners: vec![RunnerGeneration {
            runner_id: "same-plugin-runner".into(),
            plugin_id: "same-plugin".into(),
            runner_generation: 1,
            plugin_generation: 1,
        }],
        localized_content: BTreeSet::new(),
        health: WorkerHealth::Ready,
    }
}

struct Fixture {
    coordinator: Coordinator,
    origin: Arc<FakeHost>,
    worker_one_host: Arc<FakeHost>,
    worker_two_host: Arc<FakeHost>,
    worker_one: Arc<WorkerEndpoint>,
    worker_two: Arc<WorkerEndpoint>,
}

fn fixture(portable: &PortableTask) -> Fixture {
    let origin = Arc::new(FakeHost::default());
    let worker_one_host = Arc::new(FakeHost::default());
    let worker_two_host = Arc::new(FakeHost::default());
    let worker_one = Arc::new(WorkerEndpoint::new(
        NodeId("worker-1".into()),
        worker_one_host.clone(),
        Arc::new(FakeLocalizer::working()),
    ));
    let worker_two = Arc::new(WorkerEndpoint::new(
        NodeId("worker-2".into()),
        worker_two_host.clone(),
        Arc::new(FakeLocalizer::working()),
    ));
    let mut registry = WorkerRegistry::new(4).unwrap();
    registry
        .register(advertisement("worker-1", portable))
        .unwrap();
    registry
        .register(advertisement("worker-2", portable))
        .unwrap();
    let workers: BTreeMap<NodeId, Arc<dyn RemoteWorker>> = [
        (
            NodeId("worker-1".into()),
            wire(worker_one.clone() as Arc<dyn RemoteWorker>),
        ),
        (
            NodeId("worker-2".into()),
            wire(worker_two.clone() as Arc<dyn RemoteWorker>),
        ),
    ]
    .into_iter()
    .collect();
    let coordinator = Coordinator::new(
        NodeId("origin".into()),
        origin.clone(),
        Arc::new(Mutex::new(registry)),
        workers,
        32,
        1,
    )
    .unwrap();
    Fixture {
        coordinator,
        origin,
        worker_one_host,
        worker_two_host,
        worker_one,
        worker_two,
    }
}

#[test]
fn registry_uses_full_snapshots_and_compact_versioned_pulses() {
    let portable = portable(ExecutionMobility::Portable, RetrySafety::Idempotent);
    let mut registry = WorkerRegistry::new(1).unwrap();
    registry
        .register(advertisement("worker-1", &portable))
        .unwrap();
    registry
        .pulse(WorkerPulse {
            node_id: NodeId("worker-1".into()),
            snapshot_version: 1,
            health: WorkerHealth::Busy,
            running_tasks: 1,
            queue_depth: 2,
        })
        .unwrap();
    assert_eq!(
        registry.get(&NodeId("worker-1".into())).unwrap().health,
        WorkerHealth::Busy
    );
    assert_eq!(
        registry
            .latest_pulse(&NodeId("worker-1".into()))
            .unwrap()
            .queue_depth,
        2
    );
    assert_eq!(
        registry
            .pulse(WorkerPulse {
                node_id: NodeId("worker-1".into()),
                snapshot_version: 2,
                health: WorkerHealth::Ready,
                running_tasks: 0,
                queue_depth: 0,
            })
            .unwrap_err()
            .kind,
        DistributedErrorKind::Incompatible
    );
}

#[tokio::test]
async fn submit_cancel_result_and_worker_rejection_use_the_same_local_task_path() {
    let portable = portable(ExecutionMobility::Restartable, RetrySafety::Idempotent);
    let fixture = fixture(&portable);
    fixture.worker_one.reject_next();
    let global = GlobalTaskId("global-1".into());
    let placement = fixture
        .coordinator
        .submit(
            global.clone(),
            portable,
            RequirementSet::default(),
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(placement.kind, PlacementKind::Remote);
    assert_eq!(placement.node_id, NodeId("worker-2".into()));
    {
        let submitted = fixture
            .worker_two_host
            .submitted
            .lock()
            .expect("submitted mutex");
        assert_eq!(submitted.len(), 1);
        assert_eq!(submitted[0].protocol_id, "example.compute");
        assert_eq!(
            submitted[0].runner_hint.as_deref(),
            Some("same-plugin-runner")
        );
        assert_eq!(submitted[0].payload, json!({ "value": 7 }));
    }

    assert_eq!(
        fixture
            .coordinator
            .outcome(&global)
            .await
            .unwrap()
            .unwrap()
            .status,
        "completed"
    );
    fixture.coordinator.cancel(&global).await.unwrap();
    assert_eq!(
        fixture
            .worker_two_host
            .cancelled
            .lock()
            .expect("cancelled mutex")
            .len(),
        1
    );
    assert!(fixture.origin.submitted.lock().unwrap().is_empty());
}

#[tokio::test]
async fn local_only_and_no_eligible_worker_fall_back_to_origin() {
    let local = portable(ExecutionMobility::LocalOnly, RetrySafety::Unsafe);
    let fixture = fixture(&local);
    let placement = fixture
        .coordinator
        .submit(
            GlobalTaskId("local-only".into()),
            local,
            RequirementSet::default(),
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(placement.kind, PlacementKind::Local);
    assert_eq!(placement.node_id, NodeId("origin".into()));
    assert_eq!(fixture.origin.submitted.lock().unwrap().len(), 1);
    assert!(fixture.worker_one_host.submitted.lock().unwrap().is_empty());
}

#[tokio::test]
async fn incompatible_runner_or_resource_keeps_execution_local() {
    let mut portable = portable(ExecutionMobility::Portable, RetrySafety::Idempotent);
    portable.task.runner_hint = Some("missing-runner".into());
    portable.resources = vec![PortableResourceDescriptor {
        task_ref_id: "input:model".into(),
        content_id: ContentId::new("sha256", "model", 2048, "blob"),
        resource_kind: "model".into(),
        schema: SchemaIdentity::new("example.model", "1.0.0"),
        persistence: ResourcePersistence::ContentAddressed,
    }];
    let fixture = fixture(&portable);
    let placement = fixture
        .coordinator
        .submit(
            GlobalTaskId("incompatible".into()),
            portable,
            RequirementSet::default(),
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(placement.kind, PlacementKind::Local);
    assert_eq!(fixture.origin.submitted.lock().unwrap().len(), 1);
    assert!(fixture.worker_one_host.submitted.lock().unwrap().is_empty());
    assert!(fixture.worker_two_host.submitted.lock().unwrap().is_empty());
}

#[tokio::test]
async fn localization_failure_is_structured_and_never_reaches_local_host() {
    let portable = portable(ExecutionMobility::Portable, RetrySafety::Idempotent);
    let host = Arc::new(FakeHost::default());
    let localizer = Arc::new(FakeLocalizer::working());
    localizer.fail.store(true, Ordering::Release);
    let endpoint = Arc::new(WorkerEndpoint::new(
        NodeId("worker".into()),
        host.clone(),
        localizer.clone(),
    ));
    let remote = wire(endpoint);
    let error = remote
        .submit(RemoteTaskEnvelope {
            global_task_id: GlobalTaskId("localize-failure".into()),
            attempt: 1,
            origin_node: NodeId("origin".into()),
            requirements: RequirementSet::default(),
            portable,
            direct_inputs: vec![DirectDataRef {
                owner_node: NodeId("origin".into()),
                content_id: ContentId::new("sha256", "blob", 4096, "blob"),
                endpoint_hint: "link://origin/resource/blob".into(),
            }],
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind, DistributedErrorKind::LocalizationFailed);
    assert_eq!(localizer.calls.load(Ordering::Acquire), 1);
    assert!(host.submitted.lock().unwrap().is_empty());
}

#[tokio::test]
async fn disconnect_restarts_safe_work_with_new_attempt_and_rejects_stale_result() {
    let portable = portable(ExecutionMobility::Restartable, RetrySafety::Idempotent);
    let fixture = fixture(&portable);
    let global = GlobalTaskId("restartable".into());
    let first = fixture
        .coordinator
        .submit(
            global.clone(),
            portable,
            RequirementSet::default(),
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(first.node_id, NodeId("worker-1".into()));
    fixture.worker_one.set_connected(false);
    let second = fixture
        .coordinator
        .restart_after_disconnect(&global)
        .await
        .unwrap();
    assert_eq!(second.attempt, 2);
    assert_eq!(second.node_id, NodeId("worker-2".into()));
    assert_eq!(
        fixture
            .coordinator
            .accept_result(RemoteResult {
                global_task_id: global,
                attempt: 1,
                worker_node: NodeId("worker-1".into()),
                outcome: None,
                direct_outputs: Vec::new(),
            })
            .unwrap_err()
            .kind,
        DistributedErrorKind::AttemptStale
    );
}

#[tokio::test]
async fn unsafe_remote_work_never_restarts_automatically() {
    let portable = portable(ExecutionMobility::Portable, RetrySafety::Unsafe);
    let fixture = fixture(&portable);
    let global = GlobalTaskId("unsafe".into());
    fixture
        .coordinator
        .submit(
            global.clone(),
            portable,
            RequirementSet::default(),
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .coordinator
            .restart_after_disconnect(&global)
            .await
            .unwrap_err()
            .kind,
        DistributedErrorKind::RetryUnsafe
    );
}

#[tokio::test]
async fn fallback_attempts_are_strictly_bounded() {
    let portable = portable(ExecutionMobility::Portable, RetrySafety::Idempotent);
    let fixture = fixture(&portable);
    fixture.worker_one.reject_next();
    fixture.worker_two.reject_next();
    let placement = fixture
        .coordinator
        .submit(
            GlobalTaskId("bounded-fallback".into()),
            portable,
            RequirementSet::default(),
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(placement.kind, PlacementKind::Local);
    assert_eq!(fixture.origin.submitted.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn disabled_sidecar_is_inert_and_does_not_own_local_host_lifecycle() {
    let sidecar = Sidecar::disabled();
    assert_eq!(sidecar.mode(), DistributionMode::Disabled);
    assert_eq!(sidecar.background_tasks(), 0);
    assert!(!sidecar.opens_network_on_construction());
    assert_eq!(
        sidecar.health().await.unwrap_err().kind,
        DistributedErrorKind::Disabled
    );

    let host = Arc::new(FakeHost::default());
    let observable = Sidecar::local_observable(host.clone());
    assert_eq!(observable.health().await.unwrap(), "ok");
    assert!(observable.task_snapshots().await.unwrap().is_empty());
    drop(observable);
    assert!(!host.draining.load(Ordering::Acquire));

    let observable = Sidecar::local_observable(host.clone());
    observable.begin_local_drain().await.unwrap();
    assert!(host.draining.load(Ordering::Acquire));
}

#[test]
fn distributed_protocol_separates_control_from_large_data_streams() {
    let descriptor = distributed_protocol_descriptor();
    let debug_identity = descriptor
        .debug_identity
        .as_ref()
        .expect("distributed protocol debug identity");
    assert_eq!(debug_identity.authority, "mutsuki.distributed");
    assert_eq!(debug_identity.name, "cluster");
    assert_eq!(debug_identity.stable_id(), descriptor.stable_id);
    assert_eq!(descriptor.channels.len(), 3);
    assert_eq!(descriptor.channels[0].id.0, 1);
    assert_eq!(
        descriptor.channels[0].debug_name.as_deref(),
        Some("control")
    );
    assert_eq!(descriptor.channels[0].mode, ChannelMode::RequestResponse);
    assert_eq!(descriptor.channels[1].mode, ChannelMode::Stream);
    assert_eq!(descriptor.channels[2].mode, ChannelMode::Stream);
}
