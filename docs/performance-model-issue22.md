# DistributedHost performance model for issue 22

This repository owns the distributed placement, durable registry, content-localization and real
Controller/Worker process evidence. It does not claim real-network performance. All process tests
use authenticated MutsukiLink local IPC; every report labels that boundary explicitly.

`scripts/run-performance-model.py` emits `mutsuki.performance.report/v1` and a sibling anomaly
analysis. The ServiceHost fixture binary and any additional repository revision sources are explicit
arguments so an independent checkout never relies on sibling paths:

```text
./scripts/run-performance-model.py \
  --mode smoke \
  --service-binary /absolute/path/to/mutsuki-benchmark-service \
  --repository MutsukiCore=/absolute/path/to/MutsukiCore \
  --repository MutsukiLink=/absolute/path/to/MutsukiLink \
  --repository MutsukiServiceHost=/absolute/path/to/MutsukiServiceHost \
  --output /absolute/path/to/distributed-report.json
```

Reference mode expands these fixed dimensions:

- real Controller/Worker/ServiceHost topologies: 1, 4 and 16 Workers;
- placement: 1, 4, 16, 64 and 256 nodes; 1, 4 and 16 variants; top-K 1, 4, 8 and 16;
- registry: fast mode at 10,000, 100,000 and 1,000,000 mutations; durable and critical modes at
  10,000 mutations each;
- content: 1 MiB, 64 MiB and 1 GiB at concurrency 1, 4 and 16, with 256 KiB chunks;
- content scaling regression: a fixed 4 GiB aggregate at concurrency 4 and 16;
- durability faults: process-state recovery after running, output-staged and committed transitions.

Content cases distinguish cold miss, verified offline hit and half-file resume. The production
localizer serializes concurrent requests for the same digest, validates existing and partial bytes,
requests only the remaining range and atomically publishes the final file. The report retains IPC,
disk and avoided-duplicate byte counts. Content throughput is reported in `bytes/s`; registry
throughput is reported in `mutations/s`; generic operation cases retain `units/s`.

Each content sample removes its completed destination set outside the timing window. This bounds
benchmark scratch storage to one source plus one sample's concurrent destinations; reference
sampling must not multiply retained 1 GiB files by the sample count.

The original per-transfer matrix is a capacity-stress lane: `1 GiB, c16` moves and persists 16 GiB
per sample, so it is not compared directly with the 4 GiB aggregate `1 GiB, c4` case. Issue 23 adds
a fixed-total lane that compares `4 x 1 GiB` with `16 x 256 MiB`, rotates concurrency order between
process runs and performs one unreported warmup. Only this equal-work lane applies the c16/c4
throughput regression threshold. Reports record aggregate bytes and aggregate-by-RAM pressure for
both lanes.

After a complete raw matrix has been retained, `--reuse-raw --skip-build` rebuilds only the report
and analysis. The command first verifies every expected process-run, registry, content and fault file
is present, so a partial or mixed matrix cannot silently become reference evidence.

Zero-tolerance counters cover non-remote and unsafe remote placement, stale results, stale output,
duplicate commit, changed committed output, incompatible selection, incomplete/corrupt content and
unsafe automatic retry. Any non-zero value is classified as a framework suspect. Invalid samples or
dimensions are benchmark implementation errors. Without correctness failures, MAD above 10% of the
median in more than 20% of cases is environmental noise; an isolated set is case-specific noise.
No regression conclusion is made without an explicitly approved baseline from the same environment.

Durable and critical acknowledgement perform a real storage synchronization for each registry
mutation (and critical also synchronizes metadata replicas). Ten thousand mutations already provide
ten thousand fsync observations per process run. Crossing 100,000/1,000,000 with those modes would
multiply wall time without adding an independent dimension, so the large-scale state/index/compaction
cases use fast acknowledgement while the 10,000 case compares all three acknowledgement contracts.
The mutation timing lane disables automatic compaction because snapshot cost is already measured as
an explicit stage. For the 10,000-mutation comparison it also reports fixed 100-mutation windows,
which provide median/p95/p99/MAD evidence instead of treating one complete process run as one fsync
sample. Durable records three expected synchronous points per mutation; critical records four.
