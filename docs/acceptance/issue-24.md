# Issue #24 acceptance evidence

## Scope and invariants

Issue #24 moves content-localization filesystem access and SHA-256 work off Tokio reactor
threads. It does not change MutsukiCore contracts, MutsukiLink wire messages, ContentId semantics,
or placement policy. The implementation is split between `content_localization.rs` and
`localization_io.rs`; test/benchmark fault and traffic shaping is available only through the
`localization-testkit` feature.

Worker deployment JSON now requires `localization_io` with active read/write/hash limits, an
exact bounded waiting queue, a global byte limit, and a content-size limit. Invalid values return
`InvalidConfig`; admission and size exhaustion return `CapacityExceeded`.

## Functional evidence

The runtime suite covers cold localization, verified cache hits, ordered size/EOF validation,
0/50/90 percent resume, overlong partial restart, hash mismatch removal, concurrent same-ContentId
coalescing, queue rejection, hard buffered-byte enforcement, slow-transfer atomic visibility,
timeout/cancellation, injected disk-full and permission-denied failures, blocking panic recovery,
and shutdown joining. Coalescing asserts one source read, one source validation read, and one
download. Cancellation and storage failures never expose the final path and preserve legal
partials; corrupt/hash-mismatched transfers remove the partial.

Local working-tree gates on macOS ARM64:

- `cargo fmt --all -- --check`: pass.
- `cargo metadata --locked --format-version 1 --no-deps`: pass.
- `cargo check --workspace --all-targets --all-features --locked`: pass.
- `cargo test --workspace --all-targets --all-features --locked`: pass, including 70 runtime tests.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: pass.
- `./scripts/check-boundaries.sh`: pass.

## Fixed-host performance evidence

Host: `zt-admindeMac-mini.local`, macOS 26.5.2 (25F84), ARM64, rustc 1.97.0.
Each full case uses one warmup, five samples, and three independent processes. Raw baseline and
optimized reports are under `artifacts/performance/issue-24/`; the Python sampler polls RSS and
context switches, and the Rust workload records 1 ms reactor heartbeat delays plus localization
queue/execution histograms, bytes, active jobs, buffer peaks, and physical I/O counts.

The pre-change baseline is revision `04d1fe2f432555b2c93dd241612fd2f1978fd17a`.
Optimized evidence is locked to clean remote revision
`dfe7e9545fb3c7ec6d7ab4b03f0147ac793f6f79` (`dirty: false`):

| Size | Baseline c1 median | Optimized c1 median | Relative throughput |
| --- | ---: | ---: | ---: |
| 1 MiB | 33.487 ms | 20.228 ms | 165.5% |
| 64 MiB | 181.329 ms | 152.970 ms | 118.5% |
| 512 MiB | 1.247 s | 1.095 s | 113.9% |

The full 27-scenario matrix passed all 1,230 generated gates. Maximum median-of-runs heartbeat
p99 was 16.64 ms, below 50 ms and below 10 percent of every blocking stage lasting at least
500 ms. The configured cross-pressure byte budget was 192 MiB; median 512 MiB/c16 minus median
64 MiB/c4 peak RSS growth was 195,526,656 bytes, below the allowed 218,103,808 bytes. Same-digest
c16 at both 64 and 512 MiB recorded exactly one physical source read, validation read, and
download. Correctness/fault counters were zero and observed pipeline bytes never exceeded the
configured limit.

`artifacts/performance/issue-24/optimized/core-report.json` passes MutsukiCore's
`scripts/performance/validate_report.py` validator (27 cases) with
`repository_revisions.MutsukiDistributedHost.dirty = false`.

## Limitations

The local transport is the deployable boundary exercised here; CI provides Linux/macOS/Windows
compile/test portability, while RSS acceptance is fixed-host macOS ARM64 evidence. Process RSS
includes fixture setup and Link IPC mappings. Resume deliberately performs a full prefix rehash;
no checkpointed SHA state or metadata format is introduced. Heartbeat and paused-network RSS
gates aggregate independent process runs with medians so a single OS scheduling spike cannot veto
an otherwise clean matrix.

MutsukiBotTemplate release manifests, deployment revisions, Cargo manifest pins, and lockfile may
now pin DistributedHost `dfe7e9545fb3c7ec6d7ab4b03f0147ac793f6f79` when a downstream rollout is
scheduled.
