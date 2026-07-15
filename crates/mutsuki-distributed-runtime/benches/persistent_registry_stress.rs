use mutsuki_distributed_contracts::{AcceptanceMode, DurableTaskSpec, GlobalTaskId, NodeId};
use mutsuki_distributed_runtime::{PersistentRegistry, RegistryOptions, ResourceCatalog};
use mutsuki_runtime_contracts::{
    ContentId, ExecutionMobility, PortabilityCapability, PortableTask, RequirementSet, RetrySafety,
    SchemaIdentity, Task, TaskAcceptanceDurability,
};
use serde_json::json;
use std::env;
use std::time::Instant;
use tempfile::TempDir;

fn spec(id: usize, input: &ContentId) -> DurableTaskSpec {
    let task_id = format!("stress-{id}");
    DurableTaskSpec {
        global_task_id: GlobalTaskId(task_id.clone()),
        portable: PortableTask::new(
            Task::new(task_id, "stress.registry", json!({})),
            SchemaIdentity::new("stress.registry", "1.0.0"),
            input.clone(),
            PortabilityCapability {
                mobility: ExecutionMobility::Restartable,
                retry_safety: RetrySafety::Idempotent,
                task_acceptance: TaskAcceptanceDurability::Volatile,
                ..PortabilityCapability::default()
            },
        ),
        requirements: RequirementSet::default(),
        required_inputs: Vec::new(),
        requested_acceptance: AcceptanceMode::Fast,
    }
}

fn main() {
    let mutations = env::var("MUTSUKI_REGISTRY_STRESS_MUTATIONS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(100_000);
    assert!(mutations > 0);
    let task_count = mutations.div_ceil(4);
    let max_tasks = usize::try_from(task_count).expect("stress task count fits usize");
    let temp = TempDir::new().expect("create stress directory");
    let wal_path = temp.path().join("registry.wal");
    let catalog =
        ResourceCatalog::open(temp.path().join("catalog.json"), 1, 1).expect("open empty catalog");
    let registry = PersistentRegistry::open_with_options(
        &wal_path,
        Vec::new(),
        RegistryOptions {
            max_tasks,
            max_record_bytes: 64 * 1024,
            compact_after_records: (mutations / 4).max(1),
            compact_after_bytes: u64::MAX,
        },
    )
    .expect("open registry");
    let input = ContentId::new("sha256", "stress-input", 0, "none");
    let started = Instant::now();
    let mut committed = 0_u64;
    for task_number in 0..task_count {
        let task_number = usize::try_from(task_number).expect("task number fits usize");
        let task_id = GlobalTaskId(format!("stress-{task_number}"));
        registry
            .submit(spec(task_number, &input), &catalog)
            .expect("submit stress task");
        committed += 1;
        if committed == mutations {
            break;
        }
        registry
            .assign(&task_id, 1, NodeId("stress-worker".into()), None, 1)
            .expect("assign stress task");
        committed += 1;
        if committed == mutations {
            break;
        }
        registry.mark_running(&task_id, 1).expect("run stress task");
        committed += 1;
        if committed == mutations {
            break;
        }
        registry.cancel(&task_id).expect("cancel stress task");
        committed += 1;
    }
    assert_eq!(committed, mutations);
    let mutation_elapsed = started.elapsed();
    registry.compact().expect("final stress snapshot");
    let compacted = registry.stats().expect("registry stats");
    assert_eq!(compacted.last_log_index, mutations);
    assert_eq!(compacted.wal_bytes, 0);
    drop(registry);

    let reopen_started = Instant::now();
    let reopened = PersistentRegistry::open_with_options(
        &wal_path,
        Vec::new(),
        RegistryOptions {
            max_tasks,
            max_record_bytes: 64 * 1024,
            compact_after_records: (mutations / 4).max(1),
            compact_after_bytes: u64::MAX,
        },
    )
    .expect("reopen compacted registry");
    let reopen_elapsed = reopen_started.elapsed();
    let mutations_per_second =
        u128::from(mutations) * 1_000_000_000 / mutation_elapsed.as_nanos().max(1);
    assert_eq!(reopened.stats().unwrap().last_log_index, mutations);
    assert!(reopened.query(&GlobalTaskId("stress-0".into())).is_some());
    assert!(
        reopened
            .query(&GlobalTaskId(format!("stress-{}", task_count - 1)))
            .is_some()
    );

    println!(
        "{{\"mutations\":{mutations},\"tasks\":{task_count},\"mutation_seconds\":{:.6},\"mutations_per_second\":{mutations_per_second},\"reopen_seconds\":{:.6},\"wal_bytes_after_compaction\":{}}}",
        mutation_elapsed.as_secs_f64(),
        reopen_elapsed.as_secs_f64(),
        compacted.wal_bytes,
    );
}
