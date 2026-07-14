use super::*;
use mutsuki_distributed_contracts::*;
use std::collections::BTreeSet;

fn cost(execution: f64, transfer: f64) -> EndToEndCost {
    EndToEndCost {
        queue: 1.0,
        rtt: 1.0,
        input_transfer: transfer,
        prewarm: 1.0,
        execution,
        output_transfer: transfer / 4.0,
        commit: 1.0,
        jitter: 1.0,
        recovery: 10.0,
        energy: 1.0,
        ttft: Some(2.0),
        steady_latency: Some(1.0),
    }
}

fn variant(id: &str, bits: CapabilityBits, execution: f64) -> ExecutionVariant {
    ExecutionVariant {
        variant_id: id.into(),
        required_capabilities: bits,
        runner_id: "runner".into(),
        runner_generation: 2,
        plugin_id: "compute".into(),
        plugin_generation: 7,
        quality: 1.0,
        peak_memory_bytes: 128,
        peak_vram_bytes: if bits.contains(CapabilityBits::CUDA)
            || bits.contains(CapabilityBits::METAL)
        {
            128
        } else {
            0
        },
        failure_probability: 0.01,
        base_cost: cost(execution, 2.0),
    }
}

fn node(
    id: &str,
    bits: CapabilityBits,
    pressure: u8,
    variant: ExecutionVariant,
) -> SchedulingNodeSnapshot {
    SchedulingNodeSnapshot {
        node_id: NodeId(id.into()),
        capability_version: 1,
        resource_version: 1,
        capabilities: bits,
        os: if bits.contains(CapabilityBits::METAL) {
            "macos".into()
        } else {
            "linux".into()
        },
        abi: "aarch64".into(),
        trust_level: 3,
        identity_status: IdentityStatus::Active,
        integrity_verified: true,
        health: MemberHealth::Healthy,
        pressure_bucket: pressure,
        available_cpu_units: 8,
        available_memory_bytes: 4096,
        available_vram_bytes: 4096,
        localized_content: BTreeSet::new(),
        variants: vec![variant],
    }
}

fn priority(latency_class: LatencyClass, origin: WorkOrigin) -> LexicographicPriority {
    LexicographicPriority {
        safety_critical: false,
        recovery_critical: false,
        latency_class,
        deadline_risk: 0,
        dag_criticality: 0,
        unlock_value: 0,
        business_priority: 0,
        age_ticks: 0,
        fair_share_credit: 0,
        origin,
    }
}

fn request(bits: CapabilityBits) -> TaskPlacementRequest {
    TaskPlacementRequest {
        task_id: "task".into(),
        task_type: "image.infer".into(),
        input_bucket: 2,
        local_node: NodeId("local".into()),
        priority: priority(LatencyClass::Interactive, WorkOrigin::Local),
        required_capabilities: bits,
        required_os: None,
        required_abi: None,
        minimum_trust: 1,
        required_memory_bytes: 64,
        required_vram_bytes: 0,
        required_plugin: Some(("compute".into(), 7)),
        required_content: BTreeSet::new(),
        flags: PlacementFlags::default(),
        input_bytes: 1024,
        output_bytes: 64,
        local_estimated_cost: 100.0,
        safety_margin: 5.0,
        small_task_threshold: 5.0,
        quality_policy: QualityPolicy::Exact,
        session_node: None,
        migration_cost: 0.0,
        dag_cross_node_cost: 0.0,
        dag_parallel_benefit: 0.0,
        slo: PlacementSlo {
            deadline_ticks: 200.0,
            max_p95_ticks: 150.0,
            max_p99_ticks: 180.0,
            max_jitter_ticks: 10.0,
            max_failure_probability: 0.1,
            minimum_quality: 0.9,
            streaming: false,
            max_ttft_ticks: None,
            max_steady_latency_ticks: None,
        },
    }
}

#[test]
fn lexical_priority_preserves_safety_latency_and_local_work() {
    let mut queue = TaskPriorityQueue::new(4).unwrap();
    queue
        .push(
            priority(LatencyClass::Batch, WorkOrigin::Remote),
            "remote-batch",
        )
        .unwrap();
    queue
        .push(
            priority(LatencyClass::HardRealtime, WorkOrigin::Local),
            "local-realtime",
        )
        .unwrap();
    let mut safety = priority(LatencyClass::Background, WorkOrigin::Remote);
    safety.safety_critical = true;
    queue.push(safety, "cluster-safety").unwrap();
    assert_eq!(queue.pop(), Some("cluster-safety"));
    assert_eq!(queue.pop(), Some("local-realtime"));
}

#[test]
fn performance_model_is_bounded_and_adds_uncertainty() {
    let mut model = PerformanceModel::new(2, 4).unwrap();
    model
        .record(
            "task-a",
            "cpu",
            1,
            PerformanceObservation {
                latency_ticks: 8.0,
                peak_memory_bytes: 100,
                failed: false,
            },
        )
        .unwrap();
    let prediction = model.predict("task-a", "cpu", 1).unwrap();
    assert_eq!(prediction.samples, 1);
    assert!(prediction.uncertainty_penalty > 0.0);
    for task in ["task-b", "task-c"] {
        model
            .record(
                task,
                "cpu",
                1,
                PerformanceObservation {
                    latency_ticks: 16.0,
                    peak_memory_bytes: 120,
                    failed: false,
                },
            )
            .unwrap();
    }
    assert_eq!(model.profile_count(), 2);
}

#[test]
fn indexed_top_k_selects_heterogeneous_variant_and_precomputes_fallback() {
    let mut scheduler = PlacementScheduler::new(3, 6, 16, 4).unwrap();
    scheduler
        .update_node(
            SchedulingEvent::CapabilityChanged,
            node(
                "local",
                CapabilityBits::CPU,
                10,
                variant("cpu", CapabilityBits::CPU, 90.0),
            ),
        )
        .unwrap();
    scheduler
        .update_node(
            SchedulingEvent::CapabilityChanged,
            node(
                "cuda-a",
                CapabilityBits::CPU.union(CapabilityBits::CUDA),
                10,
                variant("cuda", CapabilityBits::CUDA, 10.0),
            ),
        )
        .unwrap();
    scheduler
        .update_node(
            SchedulingEvent::CapabilityChanged,
            node(
                "cuda-b",
                CapabilityBits::CPU.union(CapabilityBits::CUDA),
                20,
                variant("cuda", CapabilityBits::CUDA, 12.0),
            ),
        )
        .unwrap();
    for index in 0..32 {
        scheduler
            .update_node(
                SchedulingEvent::NodeStateChanged,
                node(
                    &format!("cpu-{index}"),
                    CapabilityBits::CPU,
                    1,
                    variant("cpu", CapabilityBits::CPU, 30.0),
                ),
            )
            .unwrap();
    }

    let plan = scheduler
        .schedule(SchedulingEvent::NewTask, &request(CapabilityBits::CUDA))
        .unwrap();
    assert_eq!(plan.selected.node_id, NodeId("cuda-a".into()));
    assert_eq!(plan.selected.variant_id, "cuda");
    assert_eq!(plan.fallbacks[0].node_id, NodeId("cuda-b".into()));
    assert!(plan.evaluated_candidates <= 3);
    assert!(plan.profitability_margin > 0.0);

    scheduler
        .update_node(
            SchedulingEvent::CapabilityChanged,
            node(
                "metal-a",
                CapabilityBits::CPU.union(CapabilityBits::METAL),
                5,
                variant("metal", CapabilityBits::METAL, 9.0),
            ),
        )
        .unwrap();
    let metal = scheduler
        .schedule(SchedulingEvent::NewTask, &request(CapabilityBits::METAL))
        .unwrap();
    assert_eq!(metal.selected.node_id, NodeId("metal-a".into()));
    assert_eq!(metal.selected.variant_id, "metal");
}

#[test]
fn profitability_gate_keeps_short_and_frame_tasks_local() {
    let mut scheduler = PlacementScheduler::new(2, 4, 8, 2).unwrap();
    scheduler
        .update_node(
            SchedulingEvent::CapabilityChanged,
            node(
                "local",
                CapabilityBits::CPU,
                5,
                variant("cpu", CapabilityBits::CPU, 2.0),
            ),
        )
        .unwrap();
    scheduler
        .update_node(
            SchedulingEvent::CapabilityChanged,
            node(
                "remote",
                CapabilityBits::CPU,
                0,
                variant("remote-cpu", CapabilityBits::CPU, 1.0),
            ),
        )
        .unwrap();
    let mut short = request(CapabilityBits::CPU);
    short.local_estimated_cost = 3.0;
    short.flags = PlacementFlags::FRAME_BOUND;
    let plan = scheduler
        .schedule(SchedulingEvent::NewTask, &short)
        .unwrap();
    assert_eq!(plan.selected.node_id, NodeId("local".into()));

    short.flags = PlacementFlags::default();
    short.local_estimated_cost = 20.0;
    short.safety_margin = 50.0;
    let plan = scheduler
        .schedule(SchedulingEvent::NewTask, &short)
        .unwrap();
    assert_eq!(plan.selected.node_id, NodeId("local".into()));
}

#[test]
fn local_admission_corrects_stale_state_and_protects_local_reserve() {
    let mut admission = LocalAdmissionController::new(
        LocalResourceBudget {
            total_cpu_units: 8,
            total_memory_bytes: 1000,
            total_vram_bytes: 500,
            total_threads: 8,
            reserved_local_cpu_units: 4,
            reserved_local_memory_bytes: 400,
            reserved_local_vram_bytes: 200,
            reserved_local_threads: 4,
            max_remote_pressure_bucket: 70,
        },
        2,
    )
    .unwrap();
    let reservation = ReservationRequest {
        reservation_id: "remote-1".into(),
        origin: WorkOrigin::Remote,
        capability_version: 1,
        cpu_units: 1,
        memory_bytes: 100,
        vram_bytes: 0,
        threads: 1,
        valid_until_tick: 10,
    };
    assert_eq!(
        admission.admit(reservation.clone(), 1),
        AdmissionOutcome::CapabilityChanged { current_version: 2 }
    );
    let mut current = reservation;
    current.capability_version = 2;
    current.cpu_units = 5;
    assert_eq!(
        admission.admit(current.clone(), 1),
        AdmissionOutcome::Overloaded
    );
    current.cpu_units = 2;
    assert!(matches!(
        admission.admit(current, 1),
        AdmissionOutcome::Accept { .. }
    ));
    admission.update_local_state(2, 90);
    assert_eq!(
        admission.remote_load_action(),
        RemoteLoadAction::CancelRemoteBackground
    );
}

#[test]
fn scheduler_rejects_revoked_identity_and_unverified_runtime() {
    let mut scheduler = PlacementScheduler::new(2, 4, 8, 2).unwrap();
    let mut revoked = node(
        "local",
        CapabilityBits::CPU,
        0,
        variant("cpu", CapabilityBits::CPU, 2.0),
    );
    revoked.identity_status = IdentityStatus::Revoked;
    scheduler
        .update_node(SchedulingEvent::CapabilityChanged, revoked)
        .unwrap();
    let mut unverified = node(
        "remote",
        CapabilityBits::CPU,
        0,
        variant("cpu", CapabilityBits::CPU, 1.0),
    );
    unverified.integrity_verified = false;
    scheduler
        .update_node(SchedulingEvent::CapabilityChanged, unverified)
        .unwrap();
    assert_eq!(
        scheduler
            .schedule(SchedulingEvent::NewTask, &request(CapabilityBits::CPU))
            .unwrap_err()
            .kind,
        DistributedErrorKind::WorkerUnavailable
    );
}

#[test]
fn telemetry_and_network_work_only_on_events_and_degrade_by_budget() {
    let mut telemetry = TelemetrySampler::new(NodeId("node".into()), 1, 2, 32).unwrap();
    assert!(telemetry.record(TelemetryClass::Correctness, "grant", 1, 99));
    assert!(!telemetry.record(TelemetryClass::Discardable, "sample", 1, 99));
    let first = telemetry.pulse(1, 1, 10, MemberHealth::Healthy, false);
    let second = telemetry.pulse(1, 1, 10, MemberHealth::Healthy, false);
    assert!(second.next_sample_after_ticks > first.next_sample_after_ticks);

    let mut network = NetworkBudgetController::new(NetworkBudget {
        max_bytes_per_tick: 1000,
        max_concurrent_transfers: 1,
        max_queued_bytes: 1000,
        control_reserve_bytes_per_tick: 100,
    })
    .unwrap();
    assert!(network.admit_control(50));
    assert!(!network.enqueue_data(500, true));
    assert!(network.enqueue_data(500, false));
    network.update_pressure(80, false);
    assert_eq!(network.degradation(), NetworkDegradation::PauseRemoteBatch);
    assert!(network.start_data(500));
    assert!(!network.start_data(1));
    assert!(network.complete_data());

    let mut resources = DistributedBudgetMeter::new(DistributedResourceBudget {
        max_cpu_share_percent: 10,
        max_memory_bytes: 1024,
        max_hash_bytes_per_tick: 100,
        max_disk_bytes_per_tick: 100,
        max_scheduler_operations_per_tick: 1,
        max_telemetry_events_per_tick: 1,
    })
    .unwrap();
    assert!(resources.admit_hash_and_disk(50, 50));
    assert!(!resources.admit_hash_and_disk(51, 0));
    assert!(resources.admit_scheduler_operation());
    assert!(!resources.admit_scheduler_operation());
    resources.next_tick();
    assert!(resources.admit_hash_and_disk(100, 100));
}
