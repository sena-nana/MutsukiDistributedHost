# Issue #24 acceptance evidence

## Scope

Issue #24 moves content-localization filesystem and SHA-256 work off Tokio reactor
threads without changing Core contracts, Link wire messages, ContentId semantics, or
placement. Runtime pieces live in `content_localization.rs` and `localization_io.rs`;
fault/traffic shaping is test-only via `localization-testkit`.

Worker deployment JSON requires `localization_io` (active read/write/hash limits, queued
jobs, buffered bytes, content size). Invalid values return `InvalidConfig`; admission or
size exhaustion returns `CapacityExceeded`. Same ContentId requests coalesce to one
physical transfer.

## Functional evidence

Runtime coverage includes cold miss, verified hit, ordered size/EOF checks, 0/50/90%
resume, overlong/corrupt partial handling, coalescing, queue rejection, hard byte budget,
atomic final visibility, timeout/cancel, injected disk-full/permission faults, blocking
panic recovery, and shutdown join. Cancellation and storage faults never publish the
final path; hash mismatch removes the partial.

## Fixed-host performance evidence

Host: `zt-admindeMac-mini.local`, macOS 26.5.2 (25F84), ARM64, rustc 1.97.0.
Matrix: one warmup, five samples, three processes. Raw reports live under
`artifacts/performance/issue-24/`.

Baseline revision `04d1fe2f432555b2c93dd241612fd2f1978fd17a`. Optimized evidence locked to
clean remote `dfe7e9545fb3c7ec6d7ab4b03f0147ac793f6f79` (`dirty: false`):

| Size | Baseline c1 median | Optimized c1 median | Relative throughput |
| --- | ---: | ---: | ---: |
| 1 MiB | 33.487 ms | 20.228 ms | 165.5% |
| 64 MiB | 181.329 ms | 152.970 ms | 118.5% |
| 512 MiB | 1.247 s | 1.095 s | 113.9% |

Full 27-scenario matrix: 1,230 gates passed. Median-of-runs heartbeat p99 max 16.64 ms
(< 50 ms and < 10% of blocking stages ≥ 500 ms). Cross-pressure RSS growth
195,526,656 bytes under the 218,103,808 byte limit. Same-digest c16 recorded one source
read, validation read, and download. `core-report.json` validates with MutsukiCore
`scripts/performance/validate_report.py`.

## Limitations

Evidence uses deployable Link local IPC on fixed-host macOS ARM64. Resume rehashes the
partial prefix (no checkpointed SHA state). Heartbeat/RSS gates use per-process medians
to resist single-run scheduling spikes. Downstream pins may reference DistributedHost
`dfe7e9545fb3c7ec6d7ab4b03f0147ac793f6f79` for that evidence lock.
