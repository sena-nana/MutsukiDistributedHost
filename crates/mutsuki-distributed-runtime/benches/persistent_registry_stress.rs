use mutsuki_distributed_contracts::{AcceptanceMode, DurableTaskSpec, GlobalTaskId, NodeId};
use mutsuki_distributed_runtime::{
    FileMetadataReplica, MetadataReplica, PersistentRegistry, RegistryOptions, ResourceCatalog,
};
use mutsuki_runtime_contracts::{
    ContentId, ExecutionMobility, PortabilityCapability, PortableTask, RequirementSet, RetrySafety,
    SchemaIdentity, Task, TaskAcceptanceDurability,
};
use serde_json::json;
use std::env;
use std::fs;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

fn spec(id: usize, input: &ContentId, acceptance: AcceptanceMode) -> DurableTaskSpec {
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
        requested_acceptance: acceptance,
    }
}

#[allow(clippy::too_many_lines)]
fn main() {
    let mutations = env::var("MUTSUKI_REGISTRY_STRESS_MUTATIONS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(100_000);
    let acceptance_name = env::var("MUTSUKI_REGISTRY_ACCEPTANCE").unwrap_or_else(|_| "fast".into());
    let acceptance = match acceptance_name.as_str() {
        "fast" => AcceptanceMode::Fast,
        "durable" => AcceptanceMode::Durable,
        "critical" => AcceptanceMode::Critical {
            minimum_replicas: 3,
        },
        _ => panic!("MUTSUKI_REGISTRY_ACCEPTANCE must be fast, durable, or critical"),
    };
    assert!(mutations > 0);
    let task_count = mutations.div_ceil(4);
    let max_tasks = usize::try_from(task_count).expect("stress task count fits usize");
    let temp = TempDir::new().expect("create stress directory");
    let wal_path = temp.path().join("registry.wal");
    let catalog =
        ResourceCatalog::open(temp.path().join("catalog.json"), 1, 1).expect("open empty catalog");
    let replica_count = acceptance.minimum_metadata_copies().saturating_sub(1);
    let replicas = (0..replica_count)
        .map(|index| {
            FileMetadataReplica::open(temp.path().join(format!("replica-{index}.wal")))
                .map(|replica| Arc::new(replica) as Arc<dyn MetadataReplica>)
                .expect("open metadata replica")
        })
        .collect::<Vec<_>>();
    let registry = PersistentRegistry::open_with_options(
        &wal_path,
        replicas,
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
            .submit(spec(task_number, &input, acceptance), &catalog)
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
    let pre_compact = registry.stats().expect("pre-compact registry stats");
    let compact_started = Instant::now();
    registry.compact().expect("final stress snapshot");
    let compact_elapsed = compact_started.elapsed();
    let compacted = registry.stats().expect("registry stats");
    assert_eq!(compacted.last_log_index, mutations);
    assert_eq!(compacted.wal_bytes, 0);
    drop(registry);

    let reopen_started = Instant::now();
    let reopened_replicas = (0..replica_count)
        .map(|index| {
            FileMetadataReplica::open(temp.path().join(format!("replica-{index}.wal")))
                .map(|replica| Arc::new(replica) as Arc<dyn MetadataReplica>)
                .expect("reopen metadata replica")
        })
        .collect::<Vec<_>>();
    let reopened = PersistentRegistry::open_with_options(
        &wal_path,
        reopened_replicas,
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

    let report = json!({
        "schema": "mutsuki.distributed.registry.raw/v1",
        "acceptance": acceptance_name,
        "mutations": mutations,
        "tasks": task_count,
        "mutation_ns": mutation_elapsed.as_nanos(),
        "mutations_per_second": mutations_per_second,
        "compact_ns": compact_elapsed.as_nanos(),
        "reopen_ns": reopen_elapsed.as_nanos(),
        "wal_bytes_before_compaction": pre_compact.wal_bytes,
        "wal_bytes_after_compaction": compacted.wal_bytes,
        "snapshot_bytes": fs::metadata(&compacted.snapshot_path).map_or(0, |metadata| metadata.len()),
        "replica_count": replica_count,
        "last_log_index": reopened.stats().unwrap().last_log_index,
        "correctness": {
            "mutations_committed": committed,
            "tail_recovered_bytes": compacted.recovered_tail_bytes,
            "first_task_present": reopened.query(&GlobalTaskId("stress-0".into())).is_some(),
            "last_task_present": reopened.query(&GlobalTaskId(format!("stress-{}", task_count - 1))).is_some(),
        },
    });
    let bytes = serde_json::to_vec_pretty(&report).expect("serialize registry report");
    if let Ok(path) = env::var("MUTSUKI_BENCH_OUTPUT") {
        if let Some(parent) = std::path::Path::new(&path).parent() {
            fs::create_dir_all(parent).expect("create registry report directory");
        }
        fs::write(path, &bytes).expect("write registry report");
    }
    println!(
        "{}",
        String::from_utf8(bytes).expect("registry report UTF-8")
    );
}
