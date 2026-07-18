use mutsuki_distributed_contracts::{
    AcceptanceMode, DurableTaskSpec, EffectRecoveryAction, EffectRecoveryCapability, GlobalTaskId,
    GlobalTaskState, NodeId, ReplicaHealth, ReplicaRecord, ReplicaTarget, ResourcePolicy,
};
use mutsuki_distributed_runtime::{
    ContentStore, PersistentRegistry, ResourceCatalog, effect_recovery_action,
};
use mutsuki_runtime_contracts::{
    ExecutionMobility, PortabilityCapability, PortableTask, RequirementSet, RetrySafety,
    SchemaIdentity, Task, TaskAcceptanceDurability,
};
use serde::Serialize;
use serde_json::json;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::TempDir;

#[derive(Serialize)]
struct RawReport {
    schema_version: &'static str,
    benchmark: &'static str,
    samples: usize,
    cases: Vec<RawCase>,
    correctness: Correctness,
}

#[derive(Serialize)]
struct RawCase {
    stage: &'static str,
    transition_ns: Vec<u128>,
    reopen_ns: Vec<u128>,
}

#[derive(Default, Serialize)]
struct Correctness {
    lost_states_after_reopen: u64,
    stale_outputs_accepted: u64,
    duplicate_commits_accepted: u64,
    committed_output_changes: u64,
    unsafe_automatic_retries: u64,
}

#[allow(clippy::too_many_lines)]
fn main() {
    let samples = env::var("MUTSUKI_FAULT_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(20);
    assert!(samples > 0, "MUTSUKI_FAULT_SAMPLES must be positive");
    let root = TempDir::new().expect("create fault benchmark directory");
    let mut running_transition = Vec::with_capacity(samples);
    let mut running_reopen = Vec::with_capacity(samples);
    let mut staged_transition = Vec::with_capacity(samples);
    let mut staged_reopen = Vec::with_capacity(samples);
    let mut commit_transition = Vec::with_capacity(samples);
    let mut commit_reopen = Vec::with_capacity(samples);
    let mut correctness = Correctness::default();

    for sample in 0..samples {
        let running = fixture(&root.path().join(format!("running-{sample}")), "running");
        let started = Instant::now();
        running
            .registry
            .mark_running(&running.task_id, 1)
            .expect("mark task running");
        running_transition.push(started.elapsed().as_nanos());
        let registry_path = running.registry_path.clone();
        drop(running.registry);
        let started = Instant::now();
        let reopened = PersistentRegistry::open(&registry_path, Vec::new(), 8)
            .expect("reopen running registry");
        running_reopen.push(started.elapsed().as_nanos());
        if reopened.query(&running.task_id).map(|record| record.state)
            != Some(GlobalTaskState::Running)
        {
            correctness.lost_states_after_reopen += 1;
        }
        let action = effect_recovery_action(
            &EffectRecoveryCapability {
                retry_safety: RetrySafety::Unsafe,
                idempotency_key: None,
                external_verifier: false,
                transactional_outbox: false,
                compensation_hook: false,
            },
            true,
        );
        if action != EffectRecoveryAction::RecoveryRequired {
            correctness.unsafe_automatic_retries += 1;
        }

        let staged = fixture(&root.path().join(format!("staged-{sample}")), "staged");
        staged
            .registry
            .mark_running(&staged.task_id, 1)
            .expect("mark staged task running");
        let stale = staged.registry.stage_output(
            &staged.task_id,
            2,
            NodeId("stale-worker".into()),
            staged.output.clone(),
            &staged.catalog,
        );
        if stale.is_ok() {
            correctness.stale_outputs_accepted += 1;
        }
        let before = staged.registry.query(&staged.task_id).expect("staged task");
        if before.staged_output.is_some() || before.committed_output.is_some() {
            correctness.stale_outputs_accepted += 1;
        }
        let started = Instant::now();
        staged
            .registry
            .stage_output(
                &staged.task_id,
                1,
                NodeId("worker".into()),
                staged.output.clone(),
                &staged.catalog,
            )
            .expect("stage current output");
        staged_transition.push(started.elapsed().as_nanos());
        let registry_path = staged.registry_path.clone();
        drop(staged.registry);
        let started = Instant::now();
        let reopened = PersistentRegistry::open(&registry_path, Vec::new(), 8)
            .expect("reopen output-staged registry");
        staged_reopen.push(started.elapsed().as_nanos());
        let record = reopened
            .query(&staged.task_id)
            .expect("reopened staged task");
        if record.state != GlobalTaskState::OutputStaged
            || record
                .staged_output
                .as_ref()
                .map(|output| &output.content_id)
                != Some(&staged.output)
            || record.committed_output.is_some()
        {
            correctness.lost_states_after_reopen += 1;
        }

        let committed = fixture(&root.path().join(format!("commit-{sample}")), "commit");
        committed
            .registry
            .mark_running(&committed.task_id, 1)
            .expect("mark commit task running");
        committed
            .registry
            .stage_output(
                &committed.task_id,
                1,
                NodeId("worker".into()),
                committed.output.clone(),
                &committed.catalog,
            )
            .expect("stage commit output");
        let started = Instant::now();
        let output = committed
            .registry
            .commit_output(&committed.task_id, &committed.catalog)
            .expect("commit output");
        commit_transition.push(started.elapsed().as_nanos());
        assert_eq!(output, committed.output);
        let registry_path = committed.registry_path.clone();
        drop(committed.registry);
        let started = Instant::now();
        let reopened = PersistentRegistry::open(&registry_path, Vec::new(), 8)
            .expect("reopen committed registry");
        commit_reopen.push(started.elapsed().as_nanos());
        let before_duplicate = reopened
            .query(&committed.task_id)
            .expect("reopened committed task");
        if before_duplicate.state != GlobalTaskState::Committed
            || before_duplicate.committed_output.as_ref() != Some(&committed.output)
        {
            correctness.lost_states_after_reopen += 1;
        }
        if reopened
            .commit_output(&committed.task_id, &committed.catalog)
            .is_ok()
        {
            correctness.duplicate_commits_accepted += 1;
        }
        if reopened
            .query(&committed.task_id)
            .and_then(|record| record.committed_output)
            != Some(committed.output)
        {
            correctness.committed_output_changes += 1;
        }
    }

    assert_eq!(correctness.lost_states_after_reopen, 0);
    assert_eq!(correctness.stale_outputs_accepted, 0);
    assert_eq!(correctness.duplicate_commits_accepted, 0);
    assert_eq!(correctness.committed_output_changes, 0);
    assert_eq!(correctness.unsafe_automatic_retries, 0);
    let report = RawReport {
        schema_version: "mutsuki.distributed.durability-faults.raw.v1",
        benchmark: "durability-faults",
        samples,
        cases: vec![
            RawCase {
                stage: "running",
                transition_ns: running_transition,
                reopen_ns: running_reopen,
            },
            RawCase {
                stage: "output_staged",
                transition_ns: staged_transition,
                reopen_ns: staged_reopen,
            },
            RawCase {
                stage: "committed",
                transition_ns: commit_transition,
                reopen_ns: commit_reopen,
            },
        ],
        correctness,
    };
    let output = env::var_os("MUTSUKI_BENCH_OUTPUT").map_or_else(
        || PathBuf::from("target/mutsuki-benchmarks/durability-faults.raw.json"),
        PathBuf::from,
    );
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).expect("create fault benchmark output directory");
    }
    fs::write(
        &output,
        serde_json::to_vec_pretty(&report).expect("encode fault benchmark report"),
    )
    .expect("write fault benchmark report");
    println!("{}", output.display());
}

struct Fixture {
    registry: PersistentRegistry,
    registry_path: PathBuf,
    catalog: ResourceCatalog,
    task_id: GlobalTaskId,
    output: mutsuki_runtime_contracts::ContentId,
}

fn fixture(path: &Path, id: &str) -> Fixture {
    fs::create_dir_all(path).expect("create fixture directory");
    let store = ContentStore::open(path.join("content"), 4).expect("open content store");
    let (input, _) = store
        .put_bytes(b"fault-input", "blob", ResourcePolicy::Reconstructible)
        .expect("store fault input");
    let (output, _) = store
        .put_bytes(b"fault-output", "blob", ResourcePolicy::Reconstructible)
        .expect("store fault output");
    let mut catalog = ResourceCatalog::open(path.join("catalog.json"), 8, 2)
        .expect("open fault resource catalog");
    catalog
        .register(input.clone(), vec![healthy("origin")], 1, 0)
        .expect("register fault input");
    catalog
        .register(output.clone(), vec![healthy("worker")], 1, 0)
        .expect("register fault output");
    let registry_path = path.join("registry.wal");
    let registry =
        PersistentRegistry::open(&registry_path, Vec::new(), 8).expect("open fault registry");
    let task_id = GlobalTaskId(format!("fault-{id}"));
    let mut task = Task::new(
        format!("local-{id}"),
        "benchmark.fault",
        json!({"input": input.content_id.digest}),
    );
    task.runner_hint = Some("fault-runner".into());
    registry
        .submit(
            DurableTaskSpec {
                global_task_id: task_id.clone(),
                portable: PortableTask::new(
                    task,
                    SchemaIdentity::new("benchmark.fault", "1.0.0"),
                    input.content_id.clone(),
                    PortabilityCapability {
                        mobility: ExecutionMobility::Restartable,
                        retry_safety: RetrySafety::Unsafe,
                        task_acceptance: TaskAcceptanceDurability::Volatile,
                        ..PortabilityCapability::default()
                    },
                ),
                requirements: RequirementSet::default(),
                required_inputs: vec![input.content_id],
                requested_acceptance: AcceptanceMode::Fast,
            },
            &catalog,
        )
        .expect("submit fault task");
    registry
        .assign(&task_id, 1, NodeId("worker".into()), None, 1)
        .expect("assign fault task");
    Fixture {
        registry,
        registry_path,
        catalog,
        task_id,
        output: output.content_id,
    }
}

fn healthy(node: &str) -> ReplicaRecord {
    ReplicaRecord {
        target: ReplicaTarget::Node(NodeId(node.into())),
        health: ReplicaHealth::Healthy,
        verified_at_epoch: 1,
    }
}
