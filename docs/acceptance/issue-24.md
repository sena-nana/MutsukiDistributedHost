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

The independent clean-clone and remote-revision repeat is intentionally deferred until the
project-required commit/push confirmation gate.

## Fixed-host performance evidence

Host: `zt-admindeMac-mini.local`, macOS 26.5.2 (25F84), ARM64, rustc 1.97.0.
Each full case uses one warmup, five samples, and three independent processes. Raw baseline and
optimized reports are under `artifacts/performance/issue-24/`; the Python sampler polls RSS and
context switches, and the Rust workload records 1 ms reactor heartbeat delays plus localization
queue/execution histograms, bytes, active jobs, buffer peaks, and physical I/O counts.

The pre-change baseline is revision `04d1fe2f432555b2c93dd241612fd2f1978fd17a`:

| Size | Baseline c1 median | Optimized c1 median | Relative throughput |
| --- | ---: | ---: | ---: |
| 1 MiB | 33.487 ms | 21.121 ms | 158.5% |
| 64 MiB | 181.329 ms | 159.657 ms | 113.6% |
| 512 MiB | 1.247 s | 1.127 s | 110.7% |

The full 27-scenario matrix passed all 1,230 generated gates. Maximum heartbeat p99 was 9.73 ms,
below 50 ms and below 10 percent of every blocking stage lasting at least 500 ms. The configured
cross-pressure byte budget was 192 MiB; 512 MiB/c16 minus 64 MiB/c4 peak RSS growth was
210,534,400 bytes, below the allowed 218,103,808 bytes. Same-digest c16 at both 64 and 512 MiB
recorded exactly one physical source read, validation read, and download. Correctness/fault
counters were zero and observed pipeline bytes never exceeded the configured limit.

`artifacts/performance/issue-24/optimized/core-report.json` passes MutsukiCore's
`scripts/performance/validate_report.py` validator (27 cases). The report is marked dirty because
this evidence precedes the required publication confirmation. After the implementation revision
is pushed, the fixed-host report must be regenerated/locked to that remote SHA before the evidence
commit and downstream rollout.

## Limitations and publication gate

The local transport is the deployable boundary exercised here; CI provides Linux/macOS/Windows
compile/test portability, while RSS acceptance is fixed-host macOS ARM64 evidence. Process RSS
includes fixture setup and Link IPC mappings. Resume deliberately performs a full prefix rehash;
no checkpointed SHA state or metadata format is introduced.

MutsukiBotTemplate release manifests, all deployment revisions, Cargo manifest pins, and lockfile
remain unchanged until the upstream revision is confirmed, pushed, independently validated, and
the final performance report names that pushed revision.
