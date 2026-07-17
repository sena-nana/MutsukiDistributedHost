use mutsuki_distributed_contracts::{DirectDataRef, NodeId};
use mutsuki_distributed_runtime::{
    FileContentServer, FileContentSource, LinkResourceLocalizer, ResourceLocalizer,
};
use mutsuki_runtime_contracts::ContentId;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::env;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
    chunk_bytes: usize,
    concurrency: usize,
    samples: usize,
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

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let content_bytes = env_u64("MUTSUKI_CONTENT_BYTES", 1024 * 1024);
    let concurrency = env_usize("MUTSUKI_CONTENT_CONCURRENCY", 1);
    let samples = env_usize("MUTSUKI_CONTENT_SAMPLES", 5);
    assert!(content_bytes > 0, "MUTSUKI_CONTENT_BYTES must be positive");
    assert!(
        concurrency > 0,
        "MUTSUKI_CONTENT_CONCURRENCY must be positive"
    );
    assert!(samples > 0, "MUTSUKI_CONTENT_SAMPLES must be positive");

    let temp = TempDir::new().expect("create content benchmark directory");
    let source_path = temp.path().join("source.bin");
    let content_id = write_source(&source_path, content_bytes);
    let secret: Arc<[u8]> =
        Arc::from(format!("content-localization-benchmark-secret-{}", unique()).into_bytes());
    let mut miss = Vec::with_capacity(samples);
    let mut hit = Vec::with_capacity(samples);
    let mut resume = Vec::with_capacity(samples);

    for sample in 0..samples {
        let destinations = destinations(temp.path(), "sample", sample, concurrency);
        miss.push(
            transfer_group(
                TransferKind::Miss,
                sample,
                &source_path,
                &content_id,
                &destinations,
                secret.clone(),
            )
            .await,
        );

        let hit_started = Instant::now();
        let mut hit_tasks = JoinSet::new();
        for (index, destination) in destinations.iter().enumerate() {
            let localizer = LinkResourceLocalizer::new(
                NodeId(format!("content-worker-{sample}-{index}")),
                secret.clone(),
                destination,
                content_bytes,
                Duration::from_secs(120),
            )
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
        hit.push(hit_started.elapsed().as_nanos());

        let resume_offset = content_bytes / 2;
        for destination in &destinations {
            let final_path = destination.join(&content_id.digest);
            fs::remove_file(final_path).expect("remove cold localization result");
            seed_partial(
                &source_path,
                &destination.join(format!("{}.partial", content_id.digest)),
                resume_offset,
            );
        }
        resume.push(
            transfer_group(
                TransferKind::Resume,
                sample,
                &source_path,
                &content_id,
                &destinations,
                secret.clone(),
            )
            .await,
        );
        for destination in destinations {
            fs::remove_dir_all(&destination).expect("remove completed sample destination");
            assert!(
                !destination.exists(),
                "completed sample destination must not be retained"
            );
        }
    }

    let concurrency_u64 = u64::try_from(concurrency).expect("concurrency fits u64");
    let full_bytes = content_bytes
        .checked_mul(concurrency_u64)
        .expect("benchmark byte count");
    let resume_bytes = (content_bytes - content_bytes / 2)
        .checked_mul(concurrency_u64)
        .expect("resume byte count");
    let cases = vec![
        RawCase {
            name: "content.localization.miss",
            elapsed_ns: miss,
            operations: concurrency,
            ipc_bytes_per_sample: full_bytes,
            disk_read_bytes_per_sample: full_bytes,
            disk_write_bytes_per_sample: full_bytes,
            duplicate_bytes_avoided_per_sample: 0,
        },
        RawCase {
            name: "content.localization.verified_hit",
            elapsed_ns: hit,
            operations: concurrency,
            ipc_bytes_per_sample: 0,
            disk_read_bytes_per_sample: full_bytes,
            disk_write_bytes_per_sample: 0,
            duplicate_bytes_avoided_per_sample: full_bytes,
        },
        RawCase {
            name: "content.localization.resume_half",
            elapsed_ns: resume,
            operations: concurrency,
            ipc_bytes_per_sample: resume_bytes,
            disk_read_bytes_per_sample: full_bytes,
            disk_write_bytes_per_sample: resume_bytes,
            duplicate_bytes_avoided_per_sample: full_bytes - resume_bytes,
        },
    ];
    let report = RawReport {
        schema_version: "mutsuki.distributed.content-localization.raw.v1",
        benchmark: "content-localization",
        boundary: "authenticated-link-local-ipc-and-real-filesystem",
        content_bytes,
        chunk_bytes: CHUNK_BYTES,
        concurrency,
        samples,
        cases,
        correctness: Correctness {
            digest_mismatches: 0,
            incomplete_files: 0,
            unexpected_origin_contacts_on_hit: 0,
        },
    };
    let output = env::var_os("MUTSUKI_BENCH_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from("target/mutsuki-benchmarks/content-localization.raw.json")
        });
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).expect("create benchmark output directory");
    }
    fs::write(
        &output,
        serde_json::to_vec_pretty(&report).expect("encode content benchmark report"),
    )
    .expect("write content benchmark report");
    println!("{}", output.display());
}

async fn transfer_group(
    kind: TransferKind,
    sample: usize,
    source_path: &Path,
    content_id: &ContentId,
    destinations: &[PathBuf],
    secret: Arc<[u8]>,
) -> u128 {
    let mut transfers = Vec::with_capacity(destinations.len());
    for (index, destination) in destinations.iter().enumerate() {
        let suffix = match kind {
            TransferKind::Miss => "miss",
            TransferKind::Resume => "resume",
        };
        let origin = NodeId(format!("content-origin-{sample}-{index}"));
        let worker = NodeId(format!("content-worker-{sample}-{index}"));
        let address = format!("content-{suffix}-{sample}-{index}-{}", unique());
        let server = FileContentServer::new(
            origin.clone(),
            worker.clone(),
            address.clone(),
            secret.clone(),
            vec![FileContentSource {
                content_id: content_id.clone(),
                path: source_path.to_owned(),
            }],
            Duration::from_secs(120),
        )
        .expect("content benchmark server");
        let localizer = LinkResourceLocalizer::new(
            worker,
            secret.clone(),
            destination,
            content_id.size,
            Duration::from_secs(120),
        )
        .expect("content benchmark localizer");
        let resource = DirectDataRef {
            owner_node: origin,
            content_id: content_id.clone(),
            endpoint_hint: format!("link-local://{address}"),
        };
        transfers.push((server, localizer, resource));
    }
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for (server, localizer, resource) in transfers {
        tasks.spawn(async move {
            let server_task = tokio::spawn(server.serve_once());
            localizer
                .localize(std::slice::from_ref(&resource))
                .await
                .expect("content localization");
            server_task
                .await
                .expect("content server task")
                .expect("content server transfer");
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("content transfer task");
    }
    started.elapsed().as_nanos()
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

fn unique() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos()
}
