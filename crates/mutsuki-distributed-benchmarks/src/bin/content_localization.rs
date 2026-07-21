use mutsuki_distributed_contracts::{DirectDataRef, NodeId};
use mutsuki_distributed_runtime::{
    FileContentServer, FileContentSource, LinkResourceLocalizer, LocalizationIoBudget,
    LocalizationIoMetricsSnapshot, LocalizationIoRuntime, ResourceLocalizer,
};
use mutsuki_runtime_contracts::ContentId;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::env;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::task::JoinSet;

const CHUNK_BYTES: usize = 256 * 1024;

#[derive(Serialize)]
struct RawReport {
    schema_version: &'static str,
    benchmark: &'static str,
    boundary: &'static str,
    content_bytes: u64,
    aggregate_content_bytes: u64,
    chunk_bytes: usize,
    concurrency: usize,
    samples: usize,
    warmup_samples: usize,
    lane: String,
    resume_percent: u64,
    coalesced: bool,
    cases: Vec<RawCase>,
    correctness: Correctness,
}

#[derive(Serialize)]
struct RawCase {
    name: &'static str,
    elapsed_ns: Vec<u128>,
    operations: usize,
    ipc_bytes_per_sample: u64,
    disk_read_bytes_per_sample: u64,
    disk_write_bytes_per_sample: u64,
    duplicate_bytes_avoided_per_sample: u64,
    evidence: Vec<SampleEvidence>,
}

#[derive(Serialize)]
struct SampleEvidence {
    elapsed_ns: u128,
    reactor_heartbeat_p50_ns: u64,
    reactor_heartbeat_p95_ns: u64,
    reactor_heartbeat_p99_ns: u64,
    reactor_heartbeat_max_ns: u64,
    origin_io: Option<LocalizationIoMetricsSnapshot>,
    worker_io: LocalizationIoMetricsSnapshot,
}

#[derive(Serialize)]
struct Correctness {
    digest_mismatches: u64,
    incomplete_files: u64,
    unexpected_origin_contacts_on_hit: u64,
}

#[derive(Clone, Copy)]
enum TransferKind {
    Miss,
    Resume,
}

#[derive(Clone, Copy)]
struct SampleMode {
    concurrency: usize,
    resume_percent: u64,
    coalesced: bool,
}

#[tokio::main(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn main() {
    let concurrency = env_usize("MUTSUKI_CONTENT_CONCURRENCY", 1);
    let samples = env_usize("MUTSUKI_CONTENT_SAMPLES", 5);
    let warmup_samples = env_usize("MUTSUKI_CONTENT_WARMUP_SAMPLES", 0);
    let lane = env::var("MUTSUKI_CONTENT_LANE").unwrap_or_else(|_| "per-transfer".into());
    let resume_percent = env_u64("MUTSUKI_CONTENT_RESUME_PERCENT", 50);
    let coalesced = env_bool("MUTSUKI_CONTENT_COALESCED", false);
    assert!(resume_percent <= 100, "resume percent must not exceed 100");
    let content_bytes = match env::var("MUTSUKI_CONTENT_AGGREGATE_BYTES") {
        Ok(value) => {
            let aggregate = value
                .parse::<u64>()
                .expect("MUTSUKI_CONTENT_AGGREGATE_BYTES must be an integer");
            let concurrency_u64 = u64::try_from(concurrency).expect("concurrency fits u64");
            assert!(
                aggregate > 0 && aggregate % concurrency_u64 == 0,
                "MUTSUKI_CONTENT_AGGREGATE_BYTES must be positive and divisible by concurrency"
            );
            aggregate / concurrency_u64
        }
        Err(_) => env_u64("MUTSUKI_CONTENT_BYTES", 1024 * 1024),
    };
    assert!(content_bytes > 0, "MUTSUKI_CONTENT_BYTES must be positive");
    assert!(
        concurrency > 0,
        "MUTSUKI_CONTENT_CONCURRENCY must be positive"
    );
    assert!(samples > 0, "MUTSUKI_CONTENT_SAMPLES must be positive");
    let mode = SampleMode {
        concurrency,
        resume_percent,
        coalesced,
    };

    let output = env::var_os("MUTSUKI_BENCH_OUTPUT").map_or_else(
        || PathBuf::from("target/mutsuki-benchmarks/content-localization.raw.json"),
        PathBuf::from,
    );
    let output_directory = output.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(output_directory).expect("create benchmark output directory");
    let temp = TempDir::new_in(output_directory).expect("create content benchmark directory");
    let source_path = temp.path().join("source.bin");
    let content_id = write_source(&source_path, content_bytes);
    let secret: Arc<[u8]> =
        Arc::from(format!("content-localization-benchmark-secret-{}", unique()).into_bytes());
    let mut miss = Vec::with_capacity(samples);
    let mut hit = Vec::with_capacity(samples);
    let mut resume = Vec::with_capacity(samples);

    for sample in 0..warmup_samples {
        run_sample(
            sample,
            &source_path,
            &content_id,
            content_bytes,
            secret.clone(),
            mode,
        )
        .await;
    }

    for sample in 0..samples {
        let timings = run_sample(
            sample,
            &source_path,
            &content_id,
            content_bytes,
            secret.clone(),
            mode,
        )
        .await;
        miss.push(timings.0);
        hit.push(timings.1);
        resume.push(timings.2);
    }

    let concurrency_u64 = u64::try_from(concurrency).expect("concurrency fits u64");
    let full_bytes = content_bytes
        .checked_mul(concurrency_u64)
        .expect("benchmark byte count");
    let resume_offset = content_bytes.saturating_mul(resume_percent) / 100;
    let physical_transfers = if coalesced { 1 } else { concurrency_u64 };
    let physical_full_bytes = content_bytes
        .checked_mul(physical_transfers)
        .expect("physical benchmark byte count");
    let resume_bytes = (content_bytes - resume_offset)
        .checked_mul(physical_transfers)
        .expect("resume byte count");
    let cases = vec![
        RawCase {
            name: "content.localization.miss",
            elapsed_ns: miss.iter().map(|sample| sample.elapsed_ns).collect(),
            operations: concurrency,
            ipc_bytes_per_sample: physical_full_bytes,
            disk_read_bytes_per_sample: physical_full_bytes,
            disk_write_bytes_per_sample: physical_full_bytes,
            duplicate_bytes_avoided_per_sample: full_bytes - physical_full_bytes,
            evidence: miss,
        },
        RawCase {
            name: "content.localization.verified_hit",
            elapsed_ns: hit.iter().map(|sample| sample.elapsed_ns).collect(),
            operations: concurrency,
            ipc_bytes_per_sample: 0,
            disk_read_bytes_per_sample: physical_full_bytes,
            disk_write_bytes_per_sample: 0,
            duplicate_bytes_avoided_per_sample: physical_full_bytes,
            evidence: hit,
        },
        RawCase {
            name: "content.localization.resume_half",
            elapsed_ns: resume.iter().map(|sample| sample.elapsed_ns).collect(),
            operations: concurrency,
            ipc_bytes_per_sample: resume_bytes,
            disk_read_bytes_per_sample: physical_full_bytes,
            disk_write_bytes_per_sample: resume_bytes,
            duplicate_bytes_avoided_per_sample: physical_full_bytes - resume_bytes,
            evidence: resume,
        },
    ];
    let report = RawReport {
        schema_version: "mutsuki.distributed.content-localization.raw.v2",
        benchmark: "content-localization",
        boundary: "authenticated-link-local-ipc-and-real-filesystem",
        content_bytes,
        aggregate_content_bytes: full_bytes,
        chunk_bytes: CHUNK_BYTES,
        concurrency,
        samples,
        warmup_samples,
        lane,
        resume_percent,
        coalesced,
        cases,
        correctness: Correctness {
            digest_mismatches: 0,
            incomplete_files: 0,
            unexpected_origin_contacts_on_hit: 0,
        },
    };
    fs::write(
        &output,
        serde_json::to_vec_pretty(&report).expect("encode content benchmark report"),
    )
    .expect("write content benchmark report");
    println!("{}", output.display());
}

async fn run_sample(
    sample: usize,
    source_path: &Path,
    content_id: &ContentId,
    content_bytes: u64,
    secret: Arc<[u8]>,
    mode: SampleMode,
) -> (SampleEvidence, SampleEvidence, SampleEvidence) {
    let destination_count = if mode.coalesced { 1 } else { mode.concurrency };
    let destinations = destinations(
        source_path.parent().expect("content benchmark root"),
        "sample",
        sample,
        destination_count,
    );
    let miss = transfer_group(
        TransferKind::Miss,
        sample,
        source_path,
        content_id,
        &destinations,
        secret.clone(),
        mode,
    )
    .await;

    let heartbeat = Heartbeat::start();
    let hit_started = Instant::now();
    let mut hit_tasks = JoinSet::new();
    let hit_io = localization_io(content_bytes);
    for (index, destination) in destinations.iter().enumerate() {
        let localizer = LinkResourceLocalizer::open(
            NodeId(format!("content-worker-{sample}-{index}")),
            secret.clone(),
            destination,
            Duration::from_secs(120),
            hit_io.clone(),
        )
        .await
        .expect("cache-hit localizer");
        let resource = DirectDataRef {
            owner_node: NodeId(format!("offline-origin-{sample}-{index}")),
            content_id: content_id.clone(),
            endpoint_hint: "link-local://origin-must-not-be-contacted".into(),
        };
        hit_tasks.spawn(async move {
            localizer
                .localize(std::slice::from_ref(&resource))
                .await
                .expect("verified cache hit");
        });
    }
    while let Some(result) = hit_tasks.join_next().await {
        result.expect("cache-hit task");
    }
    let hit_elapsed = hit_started.elapsed().as_nanos();
    let hit_heartbeat = heartbeat.stop().await;
    let hit = sample_evidence(hit_elapsed, hit_heartbeat, None, hit_io.metrics());

    let resume_offset = content_bytes.saturating_mul(mode.resume_percent) / 100;
    for destination in &destinations {
        let final_path = destination.join(&content_id.digest);
        fs::remove_file(final_path).expect("remove cold localization result");
        seed_partial(
            source_path,
            &destination.join(format!("{}.partial", content_id.digest)),
            resume_offset,
        );
    }
    let resume = transfer_group(
        TransferKind::Resume,
        sample,
        source_path,
        content_id,
        &destinations,
        secret,
        mode,
    )
    .await;
    for destination in destinations {
        fs::remove_dir_all(&destination).expect("remove completed sample destination");
        assert!(
            !destination.exists(),
            "completed sample destination must not be retained"
        );
    }
    (miss, hit, resume)
}

async fn transfer_group(
    kind: TransferKind,
    sample: usize,
    source_path: &Path,
    content_id: &ContentId,
    destinations: &[PathBuf],
    secret: Arc<[u8]>,
    mode: SampleMode,
) -> SampleEvidence {
    let origin_io = localization_io(content_id.size);
    let worker_io = localization_io(content_id.size);
    let mut transfers = Vec::with_capacity(destinations.len());
    for (index, destination) in destinations.iter().enumerate() {
        let suffix = match kind {
            TransferKind::Miss => "miss",
            TransferKind::Resume => "resume",
        };
        let origin = NodeId(format!("content-origin-{sample}-{index}"));
        let worker = NodeId(format!("content-worker-{sample}-{index}"));
        let address = format!("content-{suffix}-{sample}-{index}-{}", unique());
        let server = FileContentServer::open(
            origin.clone(),
            worker.clone(),
            address.clone(),
            secret.clone(),
            vec![FileContentSource {
                content_id: content_id.clone(),
                path: source_path.to_owned(),
            }],
            Duration::from_secs(120),
            origin_io.clone(),
        )
        .await
        .expect("content benchmark server");
        let localizer = LinkResourceLocalizer::open(
            worker,
            secret.clone(),
            destination,
            Duration::from_secs(120),
            worker_io.clone(),
        )
        .await
        .expect("content benchmark localizer");
        let resource = DirectDataRef {
            owner_node: origin,
            content_id: content_id.clone(),
            endpoint_hint: format!("link-local://{address}"),
        };
        transfers.push((server, localizer, resource));
    }
    let heartbeat = Heartbeat::start();
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for (server, localizer, resource) in transfers {
        tasks.spawn(async move {
            let server_task = tokio::spawn(server.serve_once());
            if mode.coalesced {
                let localizer = Arc::new(localizer);
                let mut followers = JoinSet::new();
                for _ in 0..mode.concurrency {
                    let follower = localizer.clone();
                    let resource = resource.clone();
                    followers.spawn(async move {
                        follower
                            .localize(std::slice::from_ref(&resource))
                            .await
                            .expect("coalesced content localization");
                    });
                }
                while let Some(result) = followers.join_next().await {
                    result.expect("coalesced localization follower");
                }
            } else {
                localizer
                    .localize(std::slice::from_ref(&resource))
                    .await
                    .expect("content localization");
            }
            server_task
                .await
                .expect("content server task")
                .expect("content server transfer");
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("content transfer task");
    }
    let elapsed = started.elapsed().as_nanos();
    let heartbeat = heartbeat.stop().await;
    sample_evidence(
        elapsed,
        heartbeat,
        Some(origin_io.metrics()),
        worker_io.metrics(),
    )
}

fn localization_io(max_content_bytes: u64) -> LocalizationIoRuntime {
    let active_jobs = env_usize("MUTSUKI_CONTENT_ACTIVE_JOBS", 4);
    let io = LocalizationIoRuntime::new(LocalizationIoBudget {
        max_active_reads: active_jobs,
        max_active_writes: active_jobs,
        max_active_hash_jobs: active_jobs,
        max_queued_jobs: 64,
        max_buffered_bytes: 192 * 1024 * 1024,
        max_content_bytes,
    })
    .expect("content benchmark I/O budget");
    let read_delay = Duration::from_millis(env_u64("MUTSUKI_CONTENT_READ_DELAY_MS", 0));
    let write_delay = Duration::from_millis(env_u64("MUTSUKI_CONTENT_WRITE_DELAY_MS", 0));
    let hash_delay = Duration::from_millis(env_u64("MUTSUKI_CONTENT_HASH_DELAY_MS", 0));
    io.testkit()
        .set_stage_delays(read_delay, write_delay, hash_delay);
    io.testkit()
        .set_network_delay(Duration::from_millis(env_u64(
            "MUTSUKI_CONTENT_NETWORK_DELAY_MS",
            0,
        )));
    io
}

struct Heartbeat {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<Vec<u64>>,
}

impl Heartbeat {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let task_stop = stop.clone();
        let task = tokio::spawn(async move {
            let period = Duration::from_millis(1);
            let mut deadline = tokio::time::Instant::now() + period;
            let mut delays = Vec::new();
            while !task_stop.load(Ordering::Acquire) {
                tokio::time::sleep_until(deadline).await;
                let now = tokio::time::Instant::now();
                delays.push(duration_ns(
                    now.saturating_duration_since(deadline).as_nanos(),
                ));
                deadline += period;
            }
            delays
        });
        Self { stop, task }
    }

    async fn stop(self) -> Vec<u64> {
        self.stop.store(true, Ordering::Release);
        self.task.await.expect("reactor heartbeat task")
    }
}

fn sample_evidence(
    elapsed_ns: u128,
    mut heartbeat: Vec<u64>,
    origin_io: Option<LocalizationIoMetricsSnapshot>,
    worker_io: LocalizationIoMetricsSnapshot,
) -> SampleEvidence {
    heartbeat.sort_unstable();
    SampleEvidence {
        elapsed_ns,
        reactor_heartbeat_p50_ns: percentile(&heartbeat, 50),
        reactor_heartbeat_p95_ns: percentile(&heartbeat, 95),
        reactor_heartbeat_p99_ns: percentile(&heartbeat, 99),
        reactor_heartbeat_max_ns: heartbeat.last().copied().unwrap_or(0),
        origin_io,
        worker_io,
    }
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let index = (values.len() - 1).saturating_mul(percentile) / 100;
    values[index]
}

fn duration_ns(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn write_source(path: &Path, content_bytes: u64) -> ContentId {
    let mut file = File::create(path).expect("create content source");
    let mut hasher = Sha256::new();
    let mut offset = 0_u64;
    let mut chunk = vec![0_u8; CHUNK_BYTES];
    while offset < content_bytes {
        let remaining = usize::try_from((content_bytes - offset).min(CHUNK_BYTES as u64))
            .expect("remaining chunk size");
        for (index, byte) in chunk[..remaining].iter_mut().enumerate() {
            *byte = u8::try_from((offset + u64::try_from(index).expect("index")) % 251)
                .expect("bounded byte");
        }
        file.write_all(&chunk[..remaining]).expect("write content");
        hasher.update(&chunk[..remaining]);
        offset += u64::try_from(remaining).expect("chunk length");
    }
    file.sync_all().expect("sync content source");
    ContentId::new(
        "sha256",
        format!("{:x}", hasher.finalize()),
        content_bytes,
        "blob",
    )
}

fn seed_partial(source: &Path, destination: &Path, bytes: u64) {
    let mut input = File::open(source)
        .expect("open source for partial")
        .take(bytes);
    let mut output = File::create(destination).expect("create partial content");
    std::io::copy(&mut input, &mut output).expect("seed partial content");
    output.sync_all().expect("sync partial content");
}

fn destinations(root: &Path, prefix: &str, sample: usize, count: usize) -> Vec<PathBuf> {
    (0..count)
        .map(|index| {
            let path = root.join(format!("{prefix}-{sample}-{index}"));
            fs::create_dir_all(&path).expect("create content destination");
            path
        })
        .collect()
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn unique() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos()
}
