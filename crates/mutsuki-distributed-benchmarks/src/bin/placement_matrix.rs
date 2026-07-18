use mutsuki_distributed_contracts::{
    CapabilityBits, EndToEndCost, ExecutionVariant, IdentityStatus, LatencyClass,
    LexicographicPriority, MemberHealth, NodeId, PlacementFlags, PlacementSlo, QualityPolicy,
    SchedulingEvent, SchedulingNodeSnapshot, TaskPlacementRequest, WorkOrigin,
};
use mutsuki_distributed_runtime::PlacementScheduler;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Serialize)]
struct RawReport {
    schema_version: &'static str,
    benchmark: &'static str,
    node_counts: Vec<usize>,
    variants_per_node: Vec<usize>,
    top_k_values: Vec<usize>,
    decisions_per_case: usize,
    cases: Vec<RawCase>,
    correctness: Correctness,
}

#[derive(Serialize)]
struct RawCase {
    case_id: String,
    workload: &'static str,
    latency_class: &'static str,
    nodes: usize,
    variants_per_node: usize,
    top_k: usize,
    index_build_ns: u128,
    decision_ns: Vec<u128>,
    evaluated_candidates: Vec<usize>,
    fallback_candidates: Vec<usize>,
}

#[derive(Default, Serialize)]
struct Correctness {
    scheduling_errors: u64,
    incompatible_selections: u64,
    local_selections: u64,
    operation_budget_violations: u64,
}

#[derive(Clone, Copy)]
struct Workload {
    name: &'static str,
    latency_name: &'static str,
    latency: LatencyClass,
    required: CapabilityBits,
}

fn main() {
    let node_counts = env_list("MUTSUKI_PLACEMENT_NODES", &[1, 4, 16, 64, 256]);
    let variants_per_node = env_list("MUTSUKI_PLACEMENT_VARIANTS", &[1, 4, 16]);
    let top_k_values = env_list("MUTSUKI_PLACEMENT_TOP_K", &[1, 4, 8, 16]);
    let decisions = env::var("MUTSUKI_PLACEMENT_DECISIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100);
    assert!(
        decisions > 0,
        "MUTSUKI_PLACEMENT_DECISIONS must be positive"
    );
    let workloads = [
        Workload {
            name: "cpu",
            latency_name: "interactive",
            latency: LatencyClass::Interactive,
            required: CapabilityBits::CPU,
        },
        Workload {
            name: "cuda",
            latency_name: "batch",
            latency: LatencyClass::Batch,
            required: CapabilityBits::CUDA,
        },
        Workload {
            name: "metal",
            latency_name: "soft_realtime",
            latency: LatencyClass::SoftRealtime,
            required: CapabilityBits::METAL,
        },
    ];
    let mut cases = Vec::new();
    let mut correctness = Correctness::default();
    for &nodes in &node_counts {
        for &variant_count in &variants_per_node {
            for &top_k in &top_k_values {
                for workload in workloads {
                    cases.push(run_case(
                        nodes,
                        variant_count,
                        top_k,
                        decisions,
                        workload,
                        &mut correctness,
                    ));
                }
            }
        }
    }
    assert_eq!(correctness.scheduling_errors, 0);
    assert_eq!(correctness.incompatible_selections, 0);
    assert_eq!(correctness.local_selections, 0);
    assert_eq!(correctness.operation_budget_violations, 0);
    let report = RawReport {
        schema_version: "mutsuki.distributed.placement-matrix.raw.v1",
        benchmark: "placement-matrix",
        node_counts,
        variants_per_node,
        top_k_values,
        decisions_per_case: decisions,
        cases,
        correctness,
    };
    let output = env::var_os("MUTSUKI_BENCH_OUTPUT").map_or_else(
        || PathBuf::from("target/mutsuki-benchmarks/placement-matrix.raw.json"),
        PathBuf::from,
    );
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).expect("create placement benchmark output directory");
    }
    fs::write(
        &output,
        serde_json::to_vec_pretty(&report).expect("encode placement benchmark report"),
    )
    .expect("write placement benchmark report");
    println!("{}", output.display());
}

fn run_case(
    nodes: usize,
    variant_count: usize,
    top_k: usize,
    decisions: usize,
    workload: Workload,
    correctness: &mut Correctness,
) -> RawCase {
    assert!(nodes > 0 && variant_count > 0 && top_k > 0);
    let max_operations = top_k.saturating_mul(variant_count).max(1);
    let mut scheduler =
        PlacementScheduler::new(top_k, max_operations, 32, 4).expect("create placement scheduler");
    let mut node_capabilities = BTreeMap::new();
    let build_started = Instant::now();
    for index in 0..nodes {
        let capabilities = capabilities(index);
        let node_id = NodeId(format!("node-{index:04}"));
        node_capabilities.insert(node_id.clone(), capabilities);
        scheduler
            .update_node(
                SchedulingEvent::NodeStateChanged,
                snapshot(node_id, capabilities, index, variant_count, workload),
            )
            .expect("index scheduling node");
    }
    let index_build_ns = build_started.elapsed().as_nanos();
    let request = request(workload);
    let mut decision_ns = Vec::with_capacity(decisions);
    let mut evaluated_candidates = Vec::with_capacity(decisions);
    let mut fallback_candidates = Vec::with_capacity(decisions);
    for _ in 0..decisions {
        let started = Instant::now();
        let result = scheduler.schedule(SchedulingEvent::NewTask, black_box(&request));
        decision_ns.push(started.elapsed().as_nanos());
        let Ok(plan) = result else {
            correctness.scheduling_errors += 1;
            continue;
        };
        let selected_capabilities = node_capabilities
            .get(&plan.selected.node_id)
            .copied()
            .unwrap_or_default();
        if !selected_capabilities.contains(workload.required)
            || !plan.selected.variant_id.starts_with(workload.name)
        {
            correctness.incompatible_selections += 1;
        }
        if plan.selected.node_id == request.local_node {
            correctness.local_selections += 1;
        }
        if plan.evaluated_candidates > max_operations {
            correctness.operation_budget_violations += 1;
        }
        evaluated_candidates.push(plan.evaluated_candidates);
        fallback_candidates.push(plan.fallbacks.len());
    }
    RawCase {
        case_id: format!(
            "placement.{}.nodes-{nodes}.variants-{variant_count}.top-k-{top_k}",
            workload.name
        ),
        workload: workload.name,
        latency_class: workload.latency_name,
        nodes,
        variants_per_node: variant_count,
        top_k,
        index_build_ns,
        decision_ns,
        evaluated_candidates,
        fallback_candidates,
    }
}

fn capabilities(index: usize) -> CapabilityBits {
    let mut bits = CapabilityBits::CPU;
    if index == 0 || index.is_multiple_of(3) {
        bits = bits.union(CapabilityBits::CUDA);
    }
    if index == 0 || index.is_multiple_of(5) {
        bits = bits.union(CapabilityBits::METAL);
    }
    bits
}

fn snapshot(
    node_id: NodeId,
    capabilities: CapabilityBits,
    index: usize,
    variant_count: usize,
    workload: Workload,
) -> SchedulingNodeSnapshot {
    SchedulingNodeSnapshot {
        node_id,
        capability_version: 1,
        resource_version: 1,
        capabilities,
        os: if capabilities.contains(CapabilityBits::METAL) {
            "macos".into()
        } else {
            "linux".into()
        },
        abi: "aarch64".into(),
        trust_level: 3,
        identity_status: IdentityStatus::Active,
        integrity_verified: true,
        health: MemberHealth::Healthy,
        pressure_bucket: u8::try_from(index % 40).expect("pressure bucket"),
        available_cpu_units: 16,
        available_memory_bytes: 16 * 1024 * 1024 * 1024,
        available_vram_bytes: 8 * 1024 * 1024 * 1024,
        localized_content: BTreeSet::new(),
        variants: (0..variant_count)
            .map(|variant| execution_variant(workload, variant))
            .collect(),
    }
}

fn execution_variant(workload: Workload, index: usize) -> ExecutionVariant {
    let execution_offset = u32::try_from(index).expect("variant index must fit in u32");
    ExecutionVariant {
        variant_id: format!("{}-{index:02}", workload.name),
        required_capabilities: workload.required,
        runner_id: format!("{}-runner", workload.name),
        runner_generation: 1,
        plugin_id: "compute".into(),
        plugin_generation: 1,
        quality: 1.0,
        peak_memory_bytes: 256 * 1024 * 1024,
        peak_vram_bytes: if workload.required == CapabilityBits::CPU {
            0
        } else {
            512 * 1024 * 1024
        },
        failure_probability: 0.001,
        base_cost: EndToEndCost {
            queue: 1.0,
            rtt: 1.0,
            input_transfer: 2.0,
            prewarm: 1.0,
            execution: 10.0 + f64::from(execution_offset),
            output_transfer: 1.0,
            commit: 1.0,
            jitter: 1.0,
            recovery: 10.0,
            energy: 1.0,
            ttft: Some(2.0),
            steady_latency: Some(1.0),
        },
    }
}

fn request(workload: Workload) -> TaskPlacementRequest {
    TaskPlacementRequest {
        task_id: format!("{}-task", workload.name),
        task_type: format!("benchmark.{}", workload.name),
        input_bucket: 2,
        local_node: NodeId("origin-not-in-cluster".into()),
        priority: LexicographicPriority {
            safety_critical: false,
            recovery_critical: false,
            latency_class: workload.latency,
            deadline_risk: 0,
            dag_criticality: 0,
            unlock_value: 0,
            business_priority: 0,
            age_ticks: 0,
            fair_share_credit: 0,
            origin: WorkOrigin::Local,
        },
        required_capabilities: workload.required,
        required_os: None,
        required_abi: None,
        minimum_trust: 1,
        required_memory_bytes: 128 * 1024 * 1024,
        required_vram_bytes: if workload.required == CapabilityBits::CPU {
            0
        } else {
            256 * 1024 * 1024
        },
        required_plugin: Some(("compute".into(), 1)),
        required_content: BTreeSet::new(),
        flags: PlacementFlags::default(),
        input_bytes: 4 * 1024 * 1024,
        output_bytes: 1024,
        local_estimated_cost: 1_000.0,
        safety_margin: 5.0,
        small_task_threshold: 1.0,
        quality_policy: QualityPolicy::Exact,
        session_node: None,
        migration_cost: 0.0,
        dag_cross_node_cost: 0.0,
        dag_parallel_benefit: 0.0,
        slo: PlacementSlo {
            deadline_ticks: 500.0,
            max_p95_ticks: 400.0,
            max_p99_ticks: 450.0,
            max_jitter_ticks: 20.0,
            max_failure_probability: 0.1,
            minimum_quality: 1.0,
            streaming: false,
            max_ttft_ticks: None,
            max_steady_latency_ticks: None,
        },
    }
}

fn env_list(name: &str, default: &[usize]) -> Vec<usize> {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|item| {
                    item.parse()
                        .expect("benchmark matrix value must be an integer")
                })
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty() && values.iter().all(|value| *value > 0))
        .unwrap_or_else(|| default.to_vec())
}
