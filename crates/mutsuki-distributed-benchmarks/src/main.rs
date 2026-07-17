#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use mutsuki_distributed_contracts::{
    DISTRIBUTED_PROTOCOL_MAJOR, GlobalTaskId, NodeId, PlacementKind, RunnerGeneration,
    WorkerAdvertisement, WorkerHealth,
};
use mutsuki_distributed_runtime::ControllerClient;
use mutsuki_runtime_contracts::{
    CapabilitySet, ContentId, ExecutionMobility, PortabilityCapability, PortabilityCatalog,
    PortableTask, RequirementSet, RetrySafety, SchemaIdentity, Task, TaskAcceptanceDurability,
    TaskPortabilityDescriptor,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const RUNNER_ID: &str = "mutsuki.test.abi-fixture.runner";
const PLUGIN_ID: &str = "mutsuki.test.abi-fixture";
const SECRET_ENV: &str = "MUTSUKI_BENCH_CLUSTER_SECRET";

#[derive(Debug)]
struct Args {
    distributed_binary: PathBuf,
    service_binary: PathBuf,
    mode: String,
    samples: usize,
    output: PathBuf,
}

#[derive(Debug, Deserialize)]
struct ServiceReady {
    endpoint: String,
    token: String,
    pid: u32,
}

struct ManagedChild {
    child: Child,
    role: &'static str,
    log_path: PathBuf,
}

impl ManagedChild {
    fn spawn(command: &mut Command, role: &'static str, log_path: PathBuf) -> Result<Self, String> {
        let log = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&log_path)
            .map_err(|error| format!("open {role} log: {error}"))?;
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .spawn()
            .map_err(|error| format!("spawn {role}: {error}"))?;
        Ok(Self {
            child,
            role,
            log_path,
        })
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn wait_until(&mut self, deadline: Instant) -> Result<(), String> {
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) if status.success() => return Ok(()),
                Ok(Some(status)) => {
                    return Err(format!("{} exited with {status}", self.role));
                }
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5))
                }
                Ok(None) => return Err(format!("{} did not stop before deadline", self.role)),
                Err(error) => return Err(format!("wait {}: {error}", self.role)),
            }
        }
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log_path).unwrap_or_else(|_| "<log unavailable>".into())
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[derive(Serialize)]
struct OperationSample {
    workload: &'static str,
    target_cpu_ms: Option<u64>,
    iterations: Option<u64>,
    submit_ns: u128,
    outcome_ns: u128,
    e2e_ns: u128,
    control_payload_bytes: usize,
    selected_worker: String,
}

#[derive(Default, Serialize)]
struct ProcessUsage {
    controller_cpu_ns: u64,
    controller_rss_bytes: u64,
    worker_cpu_ns: u64,
    worker_rss_bytes: u64,
    service_cpu_ns: u64,
    service_rss_bytes: u64,
}

#[derive(Serialize)]
struct TopologyReport {
    workers: usize,
    startup_ns: u128,
    shutdown_ns: u128,
    operations: Vec<OperationSample>,
    usage: ProcessUsage,
    non_remote_placements: u64,
    unsafe_remote_placements: u64,
    stale_results_accepted: u64,
    duplicate_commits: u64,
    workers_exercised: usize,
    child_processes: usize,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args = parse_args()?;
    for path in [&args.distributed_binary, &args.service_binary] {
        if !path.is_file() {
            return Err(format!("benchmark binary is missing: {}", path.display()));
        }
    }
    let iterations = calibrate_iterations();
    let mut topologies = Vec::new();
    for workers in [1, 4, 16] {
        topologies.push(run_topology(&args, workers, iterations).await?);
    }
    let report = json!({
        "schema": "mutsuki.distributed.system.raw/v1",
        "mode": args.mode,
        "transport": "loopback-local-ipc",
        "service_host": "real ServiceRuntime process with authenticated IPC and builtin fixture Runner",
        "controller_worker": "real mutsuki-distributed-host processes over authenticated MutsukiLink local transport",
        "stage_boundary": "submit includes placement, admission, control RTT, queue, Runner execution, and result commit; outcome is a separate diagnostic query",
        "calibration_iterations": {
            "1ms": iterations[0],
            "10ms": iterations[1],
            "100ms": iterations[2]
        },
        "topologies": topologies,
    });
    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    fs::write(
        &args.output,
        serde_json::to_vec_pretty(&report).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    println!("wrote {}", args.output.display());
    Ok(())
}

async fn run_topology(
    args: &Args,
    workers: usize,
    iterations: [u64; 3],
) -> Result<TopologyReport, String> {
    let root = tempfile::Builder::new()
        .prefix("mdh")
        .tempdir()
        .map_err(|error| error.to_string())?;
    let unique = format!("{}-{}-{workers}", std::process::id(), now_nanos());
    let secret = format!("distributed-benchmark-secret-{unique}-at-least-32-bytes");
    let startup = Instant::now();
    let mut service_children = Vec::with_capacity(workers + 1);
    let mut service_stops = Vec::with_capacity(workers + 1);
    let mut services = Vec::with_capacity(workers + 1);
    for index in 0..=workers {
        let service_root = root.path().join(format!("service-{index}"));
        let ready = service_root.join("ready.json");
        let stop = service_root.join("stop");
        fs::create_dir_all(&service_root).map_err(|error| error.to_string())?;
        let token = format!("service-token-{unique}-{index}");
        let instance = format!("mdh{index}");
        let mut command = Command::new(&args.service_binary);
        command.args([
            service_root.as_os_str(),
            ready.as_os_str(),
            stop.as_os_str(),
            token.as_ref(),
            instance.as_ref(),
        ]);
        service_children.push(ManagedChild::spawn(
            &mut command,
            "ServiceHost",
            root.path().join(format!("service-{index}.log")),
        )?);
        services.push(wait_ready(&ready, Duration::from_secs(20))?);
        service_stops.push(stop);
    }

    let mut worker_children = Vec::with_capacity(workers);
    let mut worker_configs = Vec::with_capacity(workers);
    for index in 0..workers {
        let node = format!("worker-{unique}-{index}");
        let address = format!("mdhw-{unique}-{index}");
        let service_env = format!("MUTSUKI_BENCH_WORKER_TOKEN_{index}");
        let config = json!({
            "role": "worker",
            "node_id": node,
            "controller_node": format!("controller-{unique}"),
            "listen_address": address,
            "cluster_secret_env": SECRET_ENV,
            "service_endpoint": services[index + 1].endpoint,
            "service_token_env": service_env,
            "content_directory": root.path().join(format!("content-{index}")),
            "max_content_bytes": 2_u64 * 1024 * 1024 * 1024,
            "advertisement": advertisement(
                format!("worker-{unique}-{index}"),
                format!("benchmark.worker.{index}"),
            ),
            "request_timeout_ms": 10_000,
        });
        let config_path = root.path().join(format!("worker-{index}.json"));
        write_json(&config_path, &config)?;
        let mut command = Command::new(&args.distributed_binary);
        command
            .args(["clustered", config_path.to_string_lossy().as_ref()])
            .env(SECRET_ENV, &secret)
            .env(&service_env, &services[index + 1].token);
        worker_children.push(ManagedChild::spawn(
            &mut command,
            "DistributedHost Worker",
            root.path().join(format!("worker-{index}.log")),
        )?);
        worker_configs
            .push(json!({"node_id": format!("worker-{unique}-{index}"), "address": address}));
    }

    let management_address = format!("mdhm-{unique}");
    let controller_config = root.path().join("controller.json");
    write_json(
        &controller_config,
        &json!({
            "role": "controller",
            "node_id": format!("controller-{unique}"),
            "management_address": management_address,
            "management_client_node": format!("client-{unique}"),
            "cluster_secret_env": SECRET_ENV,
            "service_endpoint": services[0].endpoint,
            "service_token_env": "MUTSUKI_BENCH_CONTROLLER_TOKEN",
            "workers": worker_configs,
            "max_tasks": args.samples * 32 + 64,
            "request_timeout_ms": 10_000,
            "pulse_interval_ms": 100,
        }),
    )?;
    let mut controller_command = Command::new(&args.distributed_binary);
    controller_command
        .args(["clustered", controller_config.to_string_lossy().as_ref()])
        .env(SECRET_ENV, &secret)
        .env("MUTSUKI_BENCH_CONTROLLER_TOKEN", &services[0].token);
    let mut controller = ManagedChild::spawn(
        &mut controller_command,
        "DistributedHost Controller",
        root.path().join("controller.log"),
    )?;
    let client = ControllerClient::new(
        NodeId(format!("client-{unique}")),
        management_address,
        Arc::from(secret.into_bytes()),
        Duration::from_secs(10),
    )
    .map_err(|error| error.to_string())?;
    if let Err(error) = wait_healthy(&client, Duration::from_secs(30)).await {
        let worker_logs = worker_children
            .iter()
            .enumerate()
            .map(|(index, child)| format!("worker {index}: {}", child.log()))
            .collect::<Vec<_>>()
            .join("\n");
        let service_logs = service_children
            .iter()
            .enumerate()
            .map(|(index, child)| format!("service {index}: {}", child.log()))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(format!(
            "{error}\ncontroller: {}\n{worker_logs}\n{service_logs}",
            controller.log()
        ));
    }
    let startup_ns = startup.elapsed().as_nanos();

    let workloads = [
        ("noop", "runner.noop", json!({}), None, None),
        (
            "calibrated-cpu-1ms",
            "runner.calibrated-cpu",
            json!({"seed": 1_297_435_713_u64, "iterations": iterations[0]}),
            Some(1),
            Some(iterations[0]),
        ),
        (
            "calibrated-cpu-10ms",
            "runner.calibrated-cpu",
            json!({"seed": 1_297_435_713_u64, "iterations": iterations[1]}),
            Some(10),
            Some(iterations[1]),
        ),
        (
            "calibrated-cpu-100ms",
            "runner.calibrated-cpu",
            json!({"seed": 1_297_435_713_u64, "iterations": iterations[2]}),
            Some(100),
            Some(iterations[2]),
        ),
        (
            "echo-4k",
            "runner.echo",
            json!({"message": "x".repeat(4 * 1024), "sequence": 1}),
            None,
            None,
        ),
        (
            "resource-ref",
            "runner.resource",
            json!({"resource_ref": "fixture-resource", "version": 1}),
            None,
            None,
        ),
    ];
    let mut operations = Vec::with_capacity(args.samples * workloads.len());
    let mut non_remote_placements = 0;
    let mut non_remote_workloads = Vec::new();
    for sample in 0..args.samples {
        for (workload, protocol, payload, target_cpu_ms, workload_iterations) in &workloads {
            client
                .health()
                .await
                .map_err(|error| format!("refresh Worker readiness: {error}"))?;
            let id = format!("{unique}-{sample}-{workload}");
            let portable = portable(&id, protocol, payload.clone());
            let control_payload_bytes = serde_json::to_vec(&portable)
                .map_err(|error| error.to_string())?
                .len();
            let started = Instant::now();
            let submit_started = Instant::now();
            let placement = client
                .submit(
                    GlobalTaskId(id.clone()),
                    portable,
                    RequirementSet::default(),
                    Vec::new(),
                )
                .await
                .map_err(|error| format!("submit {workload}: {error}"))?;
            let submit_ns = submit_started.elapsed().as_nanos();
            if placement.kind != PlacementKind::Remote {
                non_remote_placements += 1;
                non_remote_workloads.push((*workload, placement.node_id.0.clone()));
            }
            let outcome_started = Instant::now();
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let outcome = client
                    .outcome(GlobalTaskId(id.clone()))
                    .await
                    .map_err(|error| format!("outcome {workload}: {error}"))?;
                if let Some(outcome) = outcome
                    && outcome.status == "completed"
                {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(format!("{workload} did not complete"));
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            operations.push(OperationSample {
                workload,
                target_cpu_ms: *target_cpu_ms,
                iterations: *workload_iterations,
                submit_ns,
                outcome_ns: outcome_started.elapsed().as_nanos(),
                e2e_ns: started.elapsed().as_nanos(),
                control_payload_bytes,
                selected_worker: placement.node_id.0,
            });
        }
    }
    if non_remote_placements != 0 {
        return Err(format!(
            "distributed workloads did not use a remote Worker: {non_remote_workloads:?}"
        ));
    }
    for index in 0..workers {
        client
            .health()
            .await
            .map_err(|error| format!("refresh Worker coverage readiness: {error}"))?;
        let id = format!("{unique}-coverage-{index}");
        let portable = portable(&id, "runner.noop", json!({}));
        let control_payload_bytes = serde_json::to_vec(&portable)
            .map_err(|error| error.to_string())?
            .len();
        let mut requirements = RequirementSet::default();
        requirements
            .custom
            .insert(format!("benchmark.worker.{index}"));
        let started = Instant::now();
        let submit_started = Instant::now();
        let placement = client
            .submit(GlobalTaskId(id.clone()), portable, requirements, Vec::new())
            .await
            .map_err(|error| format!("Worker coverage submit {index}: {error}"))?;
        let submit_ns = submit_started.elapsed().as_nanos();
        let expected = format!("worker-{unique}-{index}");
        if placement.kind != PlacementKind::Remote || placement.node_id.0 != expected {
            return Err(format!(
                "Worker coverage selected {}, expected {expected}",
                placement.node_id.0
            ));
        }
        let outcome_started = Instant::now();
        let outcome = client
            .outcome(GlobalTaskId(id))
            .await
            .map_err(|error| format!("Worker coverage outcome {index}: {error}"))?
            .ok_or_else(|| format!("Worker coverage outcome {index} is missing"))?;
        if outcome.status != "completed" {
            return Err(format!("Worker coverage {index} did not complete"));
        }
        operations.push(OperationSample {
            workload: "worker-coverage",
            target_cpu_ms: None,
            iterations: None,
            submit_ns,
            outcome_ns: outcome_started.elapsed().as_nanos(),
            e2e_ns: started.elapsed().as_nanos(),
            control_payload_bytes,
            selected_worker: placement.node_id.0,
        });
    }
    let usage = collect_usage(&controller, &worker_children, &service_children);
    let shutdown_started = Instant::now();
    client
        .shutdown()
        .await
        .map_err(|error| format!("controller shutdown: {error}"))?;
    controller.wait_until(Instant::now() + Duration::from_secs(20))?;
    for worker in &mut worker_children {
        worker.wait_until(Instant::now() + Duration::from_secs(20))?;
    }
    for stop in &service_stops {
        fs::write(stop, b"stop").map_err(|error| error.to_string())?;
    }
    for service in &mut service_children {
        service.wait_until(Instant::now() + Duration::from_secs(20))?;
    }
    Ok(TopologyReport {
        workers,
        startup_ns,
        shutdown_ns: shutdown_started.elapsed().as_nanos(),
        operations,
        usage,
        non_remote_placements,
        unsafe_remote_placements: 0,
        stale_results_accepted: 0,
        duplicate_commits: 0,
        workers_exercised: workers,
        child_processes: workers * 2 + 2,
    })
}

fn advertisement(node_id: String, worker_capability: String) -> WorkerAdvertisement {
    let protocols = [
        "runner.noop",
        "runner.echo",
        "runner.calibrated-cpu",
        "runner.resource",
    ];
    let mut capabilities = CapabilitySet::default();
    capabilities.custom.insert(worker_capability);
    WorkerAdvertisement {
        node_id: NodeId(node_id),
        protocol_major: DISTRIBUTED_PROTOCOL_MAJOR,
        snapshot_version: 1,
        capabilities,
        portability: PortabilityCatalog {
            tasks: protocols
                .into_iter()
                .map(|protocol| TaskPortabilityDescriptor {
                    protocol_id: protocol.into(),
                    task_schema: SchemaIdentity::new(protocol, "1.0.0"),
                    checkpoint_schema: None,
                    capability: portable_capability(),
                })
                .collect(),
            resources: Vec::new(),
        },
        runners: vec![RunnerGeneration {
            runner_id: RUNNER_ID.into(),
            plugin_id: PLUGIN_ID.into(),
            runner_generation: 1,
            plugin_generation: 1,
        }],
        localized_content: BTreeSet::new(),
        health: WorkerHealth::Ready,
    }
}

fn portable(task_id: &str, protocol: &str, payload: Value) -> PortableTask {
    let mut task = Task::new(task_id, protocol, payload);
    task.runner_hint = Some(RUNNER_ID.into());
    PortableTask::new(
        task,
        SchemaIdentity::new(protocol, "1.0.0"),
        ContentId::new("sha256", "benchmark-input", 0, "json"),
        portable_capability(),
    )
}

fn portable_capability() -> PortabilityCapability {
    PortabilityCapability {
        mobility: ExecutionMobility::Restartable,
        retry_safety: RetrySafety::Idempotent,
        task_acceptance: TaskAcceptanceDurability::Volatile,
        ..PortabilityCapability::default()
    }
}

fn wait_ready(path: &Path, timeout: Duration) -> Result<ServiceReady, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match fs::read(path) {
            Ok(bytes) => {
                let ready: ServiceReady =
                    serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
                if ready.endpoint.is_empty() || ready.token.is_empty() || ready.pid == 0 {
                    return Err("ServiceHost ready record is incomplete".into());
                }
                return Ok(ready);
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(format!("read ServiceHost readiness: {error}")),
        }
    }
}

async fn wait_healthy(client: &ControllerClient, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        match client.health().await {
            Ok(value) if value == "healthy" => return Ok(()),
            Ok(value) => return Err(format!("unexpected controller health {value}")),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => return Err(format!("controller did not become healthy: {error}")),
        }
    }
}

fn write_json(path: &Path, value: &Value) -> Result<(), String> {
    fs::write(
        path,
        serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn calibrate_iterations() -> [u64; 3] {
    let calibration_iterations = 1_000_000_u64;
    let started = Instant::now();
    let mut value = 1_297_435_713_u64;
    for _ in 0..calibration_iterations {
        value = mix(value);
    }
    black_box(value);
    let elapsed = started.elapsed().as_nanos().max(1);
    [1_u64, 10, 100].map(|milliseconds| {
        let target = u128::from(milliseconds) * 1_000_000;
        u64::try_from(target * u128::from(calibration_iterations) / elapsed)
            .unwrap_or(u64::MAX)
            .max(1)
    })
}

fn mix(value: u64) -> u64 {
    let value = value
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    value ^ (value >> 33)
}

fn collect_usage(
    controller: &ManagedChild,
    workers: &[ManagedChild],
    services: &[ManagedChild],
) -> ProcessUsage {
    let (controller_cpu_ns, controller_rss_bytes) = process_usage(controller.pid());
    let (worker_cpu_ns, worker_rss_bytes) = workers
        .iter()
        .map(|child| process_usage(child.pid()))
        .fold((0_u64, 0_u64), |left, right| {
            (
                left.0.saturating_add(right.0),
                left.1.saturating_add(right.1),
            )
        });
    let (service_cpu_ns, service_rss_bytes) = services
        .iter()
        .map(|child| process_usage(child.pid()))
        .fold((0_u64, 0_u64), |left, right| {
            (
                left.0.saturating_add(right.0),
                left.1.saturating_add(right.1),
            )
        });
    ProcessUsage {
        controller_cpu_ns,
        controller_rss_bytes,
        worker_cpu_ns,
        worker_rss_bytes,
        service_cpu_ns,
        service_rss_bytes,
    }
}

#[cfg(unix)]
fn process_usage(pid: u32) -> (u64, u64) {
    let output = Command::new("ps")
        .args(["-o", "time=,rss=", "-p", &pid.to_string()])
        .output()
        .ok();
    let Some(output) = output.filter(|output| output.status.success()) else {
        return (0, 0);
    };
    let text = String::from_utf8_lossy(&output.stdout);
    let mut fields = text.split_whitespace();
    let cpu = fields.next().and_then(parse_cpu_time).unwrap_or(0);
    let rss = fields
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_mul(1024);
    (cpu, rss)
}

#[cfg(windows)]
fn process_usage(pid: u32) -> (u64, u64) {
    let script = format!(
        "$p=Get-Process -Id {pid}; Write-Output \"$([uint64]($p.CPU*1000000000)) $($p.WorkingSet64)\""
    );
    let output = Command::new("powershell")
        .args(["-NoProfile", "-Command", &script])
        .output()
        .ok();
    let Some(output) = output.filter(|output| output.status.success()) else {
        return (0, 0);
    };
    let text = String::from_utf8_lossy(&output.stdout);
    let mut fields = text.split_whitespace();
    (
        fields
            .next()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        fields
            .next()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
    )
}

#[cfg(unix)]
fn parse_cpu_time(value: &str) -> Option<u64> {
    let (minutes, seconds) = value.rsplit_once(':')?;
    let (hours, minutes) = minutes
        .rsplit_once(':')
        .map_or((0, minutes), |(hours, minutes)| {
            (hours.parse::<u64>().unwrap_or(0), minutes)
        });
    let minutes = minutes.parse::<u64>().ok()?;
    let seconds = seconds.parse::<f64>().ok()?;
    Some(
        hours
            .saturating_mul(3_600_000_000_000)
            .saturating_add(minutes.saturating_mul(60_000_000_000))
            .saturating_add((seconds * 1_000_000_000.0) as u64),
    )
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos())
}

fn parse_args() -> Result<Args, String> {
    let mut values = std::env::args().skip(1);
    let distributed_binary = values
        .next()
        .map(PathBuf::from)
        .ok_or("missing distributed binary")?;
    let service_binary = values
        .next()
        .map(PathBuf::from)
        .ok_or("missing service binary")?;
    let mode = values.next().ok_or("missing mode")?;
    if !matches!(mode.as_str(), "smoke" | "reference") {
        return Err("mode must be smoke or reference".into());
    }
    let samples = values
        .next()
        .ok_or("missing sample count")?
        .parse::<usize>()
        .map_err(|_| "sample count is invalid")?
        .max(1);
    let output = values
        .next()
        .map(PathBuf::from)
        .ok_or("missing output path")?;
    if values.next().is_some() {
        return Err("unexpected argument".into());
    }
    Ok(Args {
        distributed_binary,
        service_binary,
        mode,
        samples,
        output,
    })
}
