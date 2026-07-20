#!/usr/bin/env python3
"""Run DistributedHost's owner benchmarks and emit Mutsuki Performance Model v1."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def command(args: list[str], *, env: dict[str, str] | None = None) -> None:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    subprocess.run(args, cwd=ROOT, env=merged, check=True)


def load(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def rotated(values: list[int], offset: int) -> list[int]:
    if not values:
        return []
    position = offset % len(values)
    return values[position:] + values[:position]


def git(path: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(path), *args], text=True, stderr=subprocess.DEVNULL
    ).strip()


def repository_revision(path: Path) -> dict[str, Any]:
    revision = git(path, "rev-parse", "HEAD")
    dirty = bool(git(path, "status", "--porcelain"))
    try:
        remote = git(path, "config", "--get", "remote.origin.url")
    except subprocess.CalledProcessError:
        remote = "local-only"
    return {"revision": revision, "dirty": dirty, "remote": remote}


def parse_repositories(values: list[str]) -> dict[str, dict[str, Any]]:
    repositories = {"MutsukiDistributedHost": repository_revision(ROOT)}
    for value in values:
        if "=" not in value:
            raise SystemExit("--repository must use NAME=PATH")
        name, raw_path = value.split("=", 1)
        path = Path(raw_path).resolve()
        if not name or not path.is_dir():
            raise SystemExit(f"invalid repository revision source: {value}")
        repositories[name] = repository_revision(path)
    return dict(sorted(repositories.items()))


def sysctl(name: str, fallback: str) -> str:
    try:
        return subprocess.check_output(["sysctl", "-n", name], text=True).strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return fallback


def environment(mode: str, process_runs: int) -> dict[str, Any]:
    machine = platform.machine() or "unknown"
    cpu_count = os.cpu_count() or 1
    if sys.platform == "darwin":
        cpu_model = sysctl("machdep.cpu.brand_string", platform.processor() or machine)
        physical = sysctl("hw.physicalcpu", str(cpu_count))
        logical = sysctl("hw.logicalcpu", str(cpu_count))
        ram = int(sysctl("hw.memsize", "1"))
        topology = f"physical={physical},logical={logical}"
    else:
        cpu_model = platform.processor() or machine
        topology = f"logical={cpu_count}"
        try:
            ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        except (AttributeError, ValueError):
            ram = 1
    try:
        rustc = subprocess.check_output(["rustc", "--version"], text=True).strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        rustc = "unavailable"
    return {
        "cpu_model": cpu_model,
        "cpu_topology": topology,
        "ram_bytes": max(1, ram),
        "os": platform.platform(),
        "kernel": platform.release(),
        "architecture": machine,
        "target_triple": f"{machine}-{sys.platform}",
        "toolchains": {"rustc": rustc, "python": platform.python_version()},
        "release_profile": {"name": "release", "lto": False, "codegen_units": 16},
        "power_mode": os.environ.get("MUTSUKI_BENCH_POWER_MODE", "not-recorded"),
        "virtualization": os.environ.get("MUTSUKI_BENCH_VIRTUALIZATION", "not-recorded"),
        "runner_configuration": {
            "mode": mode,
            "process_runs": process_runs,
            "content_chunk_bytes": 256 * 1024,
            "content_warmup_samples": 1,
            "transport": "loopback-local-ipc",
        },
        "network": {
            "scope": "local IPC only",
            "real_network_claim": False,
        },
    }


def distribution(samples: list[float], unit: str) -> dict[str, Any]:
    if not samples or any(value < 0 or not (value < float("inf")) for value in samples):
        raise ValueError("distribution samples must be finite and non-negative")
    values = sorted(samples)
    median = statistics.median(values)
    deviations = sorted(abs(value - median) for value in values)

    def percentile(quantile: float) -> float:
        index = max(0, min(len(values) - 1, int(len(values) * quantile + 0.999999) - 1))
        return values[index]

    return {
        "median": median,
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "mad": statistics.median(deviations),
        "min": values[0],
        "max": values[-1],
        "unit": unit,
        "sample_count": len(values),
        "samples": values,
    }


def case(
    case_id: str,
    mode: str,
    dimensions: dict[str, Any],
    latency: list[float],
    *,
    units: float = 1.0,
    throughput_unit: str = "units/s",
    counters: dict[str, int] | None = None,
    metrics: dict[str, float] | None = None,
    stages: dict[str, float] | None = None,
) -> dict[str, Any]:
    counters = counters or {}
    result_metrics: dict[str, Any] = {
        "latency_ns": distribution(latency, "ns"),
        "throughput_per_second": distribution(
            [units * 1_000_000_000.0 / max(1.0, value) for value in latency], throughput_unit
        ),
    }
    if metrics:
        result_metrics.update(metrics)
    return {
        "case_id": case_id,
        "measurement_mode": mode,
        "dimensions": dimensions,
        "metrics": result_metrics,
        "correctness": {"passed": all(value == 0 for value in counters.values()), "counters": counters},
        **({"stage_breakdown": stages} if stages else {}),
    }


def run_raw(args: argparse.Namespace, raw: Path, process_run: int) -> list[Path]:
    generated: list[Path] = []
    system_path = raw / f"system-{process_run}.json"
    if not reusable_raw(args, system_path):
        command(
            [
                str(args.distributed_benchmark),
                str(args.distributed_binary),
                str(args.service_binary),
                args.mode,
                str(args.system_samples),
                str(system_path),
            ]
        )
    generated.append(system_path)

    placement_path = raw / f"placement-{process_run}.json"
    placement_env = {
        "MUTSUKI_BENCH_OUTPUT": str(placement_path),
        "MUTSUKI_PLACEMENT_DECISIONS": str(args.placement_decisions),
    }
    if args.mode == "smoke":
        placement_env.update(
            {
                "MUTSUKI_PLACEMENT_NODES": "1,4,16",
                "MUTSUKI_PLACEMENT_VARIANTS": "1,4",
                "MUTSUKI_PLACEMENT_TOP_K": "1,4",
            }
        )
    if not reusable_raw(args, placement_path):
        command([str(args.placement_binary)], env=placement_env)
    generated.append(placement_path)

    for mutations, acceptances in args.registry_matrix:
        for acceptance in acceptances:
            output = raw / f"registry-{mutations}-{acceptance}-{process_run}.json"
            if not reusable_raw(args, output):
                command(
                    ["cargo", "bench", "--quiet", "-p", "mutsuki-distributed-runtime", "--bench", "persistent_registry_stress"],
                    env={
                        "MUTSUKI_REGISTRY_STRESS_MUTATIONS": str(mutations),
                        "MUTSUKI_REGISTRY_ACCEPTANCE": acceptance,
                        "MUTSUKI_REGISTRY_WINDOW_MUTATIONS": str(
                            100 if mutations <= 10_000 else mutations
                        ),
                        "MUTSUKI_BENCH_OUTPUT": str(output),
                    },
                )
            generated.append(output)

    for size in args.content_sizes:
        for concurrency in rotated(args.content_concurrency, process_run):
            output = raw / f"content-{size}-{concurrency}-{process_run}.json"
            if not reusable_raw(args, output):
                command(
                    [str(args.content_binary)],
                    env={
                        "MUTSUKI_CONTENT_BYTES": str(size),
                        "MUTSUKI_CONTENT_CONCURRENCY": str(concurrency),
                        "MUTSUKI_CONTENT_SAMPLES": str(args.content_samples),
                        "MUTSUKI_CONTENT_WARMUP_SAMPLES": "1",
                        "MUTSUKI_CONTENT_LANE": "per-transfer-capacity-stress",
                        "MUTSUKI_BENCH_OUTPUT": str(output),
                    },
                )
            generated.append(output)

    for concurrency in rotated(args.content_scaling_concurrency, process_run):
        aggregate = args.content_scaling_total_bytes
        output = raw / f"content-fixed-total-{aggregate}-{concurrency}-{process_run}.json"
        if not reusable_raw(args, output):
            command(
                [str(args.content_binary)],
                env={
                    "MUTSUKI_CONTENT_AGGREGATE_BYTES": str(aggregate),
                    "MUTSUKI_CONTENT_CONCURRENCY": str(concurrency),
                    "MUTSUKI_CONTENT_SAMPLES": str(args.content_samples),
                    "MUTSUKI_CONTENT_WARMUP_SAMPLES": "1",
                    "MUTSUKI_CONTENT_LANE": "fixed-total-scaling",
                    "MUTSUKI_BENCH_OUTPUT": str(output),
                },
            )
        generated.append(output)

    faults_path = raw / f"faults-{process_run}.json"
    if not reusable_raw(args, faults_path):
        command(
            [str(args.fault_binary)],
            env={
                "MUTSUKI_FAULT_SAMPLES": str(args.fault_samples),
                "MUTSUKI_BENCH_OUTPUT": str(faults_path),
            },
        )
    generated.append(faults_path)
    return generated


def reusable_raw(args: argparse.Namespace, path: Path) -> bool:
    if not args.resume or not path.is_file():
        return False
    try:
        load(path)
    except (OSError, json.JSONDecodeError):
        return False
    return True


def merge_system(paths: list[Path]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    reports = [load(path) for path in paths]
    grouped: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    startup: dict[int, list[float]] = defaultdict(list)
    shutdown: dict[int, list[float]] = defaultdict(list)
    correctness: dict[str, int] = defaultdict(int)
    usage: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for report in reports:
        for topology in report["topologies"]:
            workers = topology["workers"]
            startup[workers].append(float(topology["startup_ns"]))
            shutdown[workers].append(float(topology["shutdown_ns"]))
            for name in ("non_remote_placements", "unsafe_remote_placements", "stale_results_accepted", "duplicate_commits"):
                correctness[name] += int(topology[name])
            if topology["workers_exercised"] != workers:
                correctness["worker_coverage_failures"] += 1
            for key, value in topology["usage"].items():
                usage[workers][key] = max(usage[workers][key], float(value))
            for operation in topology["operations"]:
                grouped[(workers, operation["workload"])].append(operation)
    cases: list[dict[str, Any]] = []
    for workers, samples in sorted(startup.items()):
        cases.append(case("distributed.system.startup", "system", {"workers": workers}, samples))
        cases.append(case("distributed.system.shutdown", "system", {"workers": workers}, shutdown[workers]))
    for (workers, workload), operations in sorted(grouped.items()):
        latency = [float(item["e2e_ns"]) for item in operations]
        counters = {
            "non_remote_placements": correctness["non_remote_placements"],
            "unsafe_remote_placements": correctness["unsafe_remote_placements"],
            "stale_results_accepted": correctness["stale_results_accepted"],
            "duplicate_commits": correctness["duplicate_commits"],
        }
        cases.append(
            case(
                f"distributed.system.{workload.replace('_', '-')}",
                "system",
                {"workers": workers, "workload": workload},
                latency,
                counters=counters,
                metrics={
                    "peak_rss_bytes": usage[workers]["controller_rss_bytes"]
                    + usage[workers]["worker_rss_bytes"]
                    + usage[workers]["service_rss_bytes"],
                    "cpu_time_ns": distribution(
                        [
                            usage[workers]["controller_cpu_ns"]
                            + usage[workers]["worker_cpu_ns"]
                            + usage[workers]["service_cpu_ns"]
                        ],
                        "ns",
                    ),
                    "ipc_bytes": float(sum(item["control_payload_bytes"] for item in operations)),
                },
                stages={
                    "submit_median_ns": statistics.median(float(item["submit_ns"]) for item in operations),
                    "outcome_median_ns": statistics.median(float(item["outcome_ns"]) for item in operations),
                },
            )
        )
    return cases, dict(correctness)


def merge_placement(paths: list[Path]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    correctness: dict[str, int] = defaultdict(int)
    for path in paths:
        report = load(path)
        for name, value in report["correctness"].items():
            correctness[name] += int(value)
        for item in report["cases"]:
            grouped[item["case_id"]].append(item)
    cases = []
    for case_id, items in sorted(grouped.items()):
        first = items[0]
        latency = [float(value) for item in items for value in item["decision_ns"]]
        evaluated = [value for item in items for value in item["evaluated_candidates"]]
        cases.append(
            case(
                case_id,
                "time",
                {
                    "workload": first["workload"],
                    "latency_class": first["latency_class"],
                    "nodes": first["nodes"],
                    "variants_per_node": first["variants_per_node"],
                    "top_k": first["top_k"],
                },
                latency,
                counters=dict(correctness),
                metrics={"evaluated_candidates": float(statistics.median(evaluated))},
            )
        )
    return cases, dict(correctness)


def merge_registry(paths: list[Path]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    windowed: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    correctness: dict[str, int] = defaultdict(int)
    for path in paths:
        report = load(path)
        grouped[(report["acceptance"], report["mutations"])].append(report)
        if report["mutations"] <= 10_000:
            windowed[(report["acceptance"], report["mutations"])].append(report)
        if report["correctness"]["mutations_committed"] != report["mutations"]:
            correctness["lost_mutations"] += 1
        if not report["correctness"].get("wal_present_before_compaction", False):
            correctness["missing_pre_compaction_wal"] += 1
        if not report["correctness"]["first_task_present"] or not report["correctness"]["last_task_present"]:
            correctness["missing_reopened_tasks"] += 1
    cases = []
    for (acceptance, mutations), items in sorted(grouped.items()):
        latency = [float(item["mutation_ns"]) for item in items]
        cases.append(
            case(
                "distributed.registry.mutate",
                "time",
                {
                    "acceptance": acceptance,
                    "mutations": mutations,
                    "compaction_during_mutation": False,
                },
                latency,
                units=float(mutations),
                throughput_unit="mutations/s",
                counters=dict(correctness),
                metrics={
                    "disk_bytes": float(max(item["snapshot_bytes"] for item in items)),
                    "replica_count": float(items[0]["replica_count"]),
                    "expected_sync_points_per_mutation": float(
                        items[0]["expected_sync_points_per_mutation"]
                    ),
                },
                stages={
                    "compact_median_ns": statistics.median(float(item["compact_ns"]) for item in items),
                    "reopen_median_ns": statistics.median(float(item["reopen_ns"]) for item in items),
                },
            )
        )
    for (acceptance, mutations), items in sorted(windowed.items()):
        elapsed = [
            float(value)
            for item in items
            for value in item["mutation_window_ns"]
        ]
        counts = [
            int(value)
            for item in items
            for value in item["mutation_window_counts"]
        ]
        if not elapsed or len(elapsed) != len(counts) or len(set(counts)) != 1:
            raise ValueError("registry mutation windows must be non-empty and fixed-size")
        cases.append(
            case(
                "distributed.registry.mutation-window",
                "time",
                {
                    "acceptance": acceptance,
                    "mutations": mutations,
                    "window_mutations": counts[0],
                    "compaction_during_mutation": False,
                },
                elapsed,
                units=float(counts[0]),
                throughput_unit="mutations/s",
                counters=dict(correctness),
                metrics={
                    "expected_sync_points_per_mutation": float(
                        items[0]["expected_sync_points_per_mutation"]
                    )
                },
            )
        )
    return cases, dict(correctness)


def merge_content(
    paths: list[Path], ram_bytes: int
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    grouped: dict[
        tuple[str, int, int, int, str],
        list[tuple[dict[str, Any], dict[str, Any]]],
    ] = defaultdict(list)
    correctness: dict[str, int] = defaultdict(int)
    for path in paths:
        report = load(path)
        for name, value in report["correctness"].items():
            correctness[name] += int(value)
        for item in report["cases"]:
            aggregate = int(
                report.get(
                    "aggregate_content_bytes",
                    report["content_bytes"] * report["concurrency"],
                )
            )
            grouped[
                (
                    report.get("lane", "per-transfer"),
                    report["content_bytes"],
                    aggregate,
                    report["concurrency"],
                    item["name"],
                )
            ].append((report, item))
    cases = []
    for (lane, size, aggregate, concurrency, name), items in sorted(grouped.items()):
        latency = [float(value) for _, item in items for value in item["elapsed_ns"]]
        first_report, first = items[0]
        case_id = (
            name.replace("content.localization.", "content.localization.fixed-total.")
            if lane == "fixed-total-scaling"
            else name
        )
        cases.append(
            case(
                case_id,
                "time",
                {
                    "lane": lane,
                    "content_bytes": size,
                    "aggregate_content_bytes": aggregate,
                    "ram_pressure_ratio": aggregate / max(1, ram_bytes),
                    "concurrency": concurrency,
                    "chunk_bytes": first_report["chunk_bytes"],
                },
                latency,
                units=float(aggregate),
                throughput_unit="bytes/s",
                counters=dict(correctness),
                metrics={
                    "aggregate_bytes_per_sample": float(aggregate),
                    "ipc_bytes": float(first["ipc_bytes_per_sample"]),
                    "disk_bytes": float(first["disk_read_bytes_per_sample"] + first["disk_write_bytes_per_sample"]),
                    "duplicate_bytes_avoided": float(first["duplicate_bytes_avoided_per_sample"]),
                },
            )
        )
    return cases, dict(correctness)


def merge_faults(paths: list[Path]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    correctness: dict[str, int] = defaultdict(int)
    for path in paths:
        report = load(path)
        for name, value in report["correctness"].items():
            correctness[name] += int(value)
        for item in report["cases"]:
            grouped[item["stage"]].append(item)
    cases = []
    for stage, items in sorted(grouped.items()):
        transitions = [float(value) for item in items for value in item["transition_ns"]]
        reopens = [float(value) for item in items for value in item["reopen_ns"]]
        cases.append(
            case(
                f"distributed.durability.{stage.replace('_', '-')}",
                "time",
                {"fault_stage": stage},
                transitions,
                counters=dict(correctness),
                stages={"reopen_median_ns": statistics.median(reopens)},
            )
        )
    return cases, dict(correctness)


def analyze(cases: list[dict[str, Any]], counters: dict[str, int]) -> dict[str, Any]:
    noisy = []
    for item in cases:
        latency = item["metrics"].get("latency_ns")
        if latency and latency["median"] > 0 and latency["mad"] / latency["median"] > 0.10:
            noisy.append({"case_id": item["case_id"], "dimensions": item["dimensions"], "mad_ratio": latency["mad"] / latency["median"]})
    fixed_total = {
        item["dimensions"]["concurrency"]: item
        for item in cases
        if item["case_id"] == "content.localization.fixed-total.miss"
    }
    content_diagnostic: dict[str, Any]
    if set(fixed_total) >= {4, 16}:
        c4 = fixed_total[4]["metrics"]["throughput_per_second"]["median"]
        c16 = fixed_total[16]["metrics"]["throughput_per_second"]["median"]
        ratio = c16 / max(1.0, c4)
        aggregate = fixed_total[4]["dimensions"]["aggregate_content_bytes"]
        evaluated = aggregate >= 4 * 1024 * 1024 * 1024
        content_diagnostic = {
            "aggregate_content_bytes": aggregate,
            "c4_median_bytes_per_second": c4,
            "c16_median_bytes_per_second": c16,
            "c16_to_c4_ratio": ratio,
            "minimum_ratio": 0.90,
            "evaluated": evaluated,
            "passed": not evaluated or ratio >= 0.90,
        }
        if not evaluated:
            content_diagnostic["reason"] = (
                "smoke covers fixed-total behavior; the scaling threshold applies to the 4 GiB reference lane"
            )
    else:
        content_diagnostic = {"passed": False, "reason": "fixed-total c4/c16 cases are missing"}
    registry_diagnostics = []
    for acceptance in ("durable", "critical"):
        candidates = [
            item
            for item in cases
            if item["case_id"] == "distributed.registry.mutation-window"
            and item["dimensions"]["acceptance"] == acceptance
        ]
        if not candidates:
            registry_diagnostics.append(
                {"acceptance": acceptance, "passed": False, "reason": "window case is missing"}
            )
            continue
        item = max(candidates, key=lambda value: value["dimensions"]["mutations"])
        latency = item["metrics"]["latency_ns"]
        ratio = latency["mad"] / max(1.0, latency["median"])
        evaluated = latency["sample_count"] >= 3
        registry_diagnostics.append(
            {
                "acceptance": acceptance,
                "mutations": item["dimensions"]["mutations"],
                "window_mutations": item["dimensions"]["window_mutations"],
                "expected_sync_points_per_mutation": item["metrics"][
                    "expected_sync_points_per_mutation"
                ],
                "mad_ratio": ratio,
                "maximum_mad_ratio": 0.10,
                "evaluated": evaluated,
                "passed": not evaluated or ratio <= 0.10,
            }
        )
    issue23_passed = content_diagnostic["passed"] and all(
        item["passed"] for item in registry_diagnostics
    )
    if any(counters.values()):
        classification = "framework-suspect"
        reason = "one or more zero-tolerance correctness counters are non-zero"
    elif not issue23_passed:
        classification = "issue23-regression"
        reason = "fixed-total content scaling or durable registry jitter exceeds its fixed-reference threshold"
    elif noisy and len(noisy) / max(1, len(cases)) > 0.20:
        classification = "environmental-noise"
        reason = "more than 20% of latency cases have MAD above 10% of median"
    elif noisy:
        classification = "case-specific-noise"
        reason = "isolated latency cases have MAD above 10% of median"
    else:
        classification = "no-obvious-anomaly"
        reason = "structure and correctness passed; no broad high-MAD pattern was observed"
    return {
        "schema_version": "mutsuki.performance.analysis/v1",
        "classification": classification,
        "reason": reason,
        "correctness_counters": counters,
        "noisy_cases": noisy,
        "issue23": {
            "passed": issue23_passed,
            "content_fixed_total": content_diagnostic,
            "registry_window_jitter": registry_diagnostics,
        },
        "limitations": [
            "No regression claim is made without an approved baseline from the same environment.",
            "Per-transfer 1 GiB concurrency cases are capacity stress; only the fixed-total lane is used for scaling regression decisions.",
            "Loopback local IPC does not represent real network latency or throughput.",
        ],
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("--mode", choices=("smoke", "reference"), default="smoke")
    result.add_argument("--service-binary", type=Path, required=True)
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--raw-dir", type=Path)
    result.add_argument("--repository", action="append", default=[], metavar="NAME=PATH")
    result.add_argument("--process-runs", type=int)
    result.add_argument("--skip-build", action="store_true")
    result.add_argument(
        "--resume",
        action="store_true",
        help="reuse only complete, parseable raw case files and run every missing case",
    )
    result.add_argument(
        "--reuse-raw",
        action="store_true",
        help="rebuild the report from the complete existing raw matrix without rerunning workloads",
    )
    return result


def expected_raw_paths(args: argparse.Namespace) -> list[Path]:
    paths: list[Path] = []
    for process_run in range(args.process_runs):
        paths.extend(
            [
                args.raw_dir / f"system-{process_run}.json",
                args.raw_dir / f"placement-{process_run}.json",
                *[
                    args.raw_dir / f"registry-{mutations}-{acceptance}-{process_run}.json"
                    for mutations, modes in args.registry_matrix
                    for acceptance in modes
                ],
                *[
                    args.raw_dir / f"content-{size}-{concurrency}-{process_run}.json"
                    for size in args.content_sizes
                    for concurrency in args.content_concurrency
                ],
                *[
                    args.raw_dir
                    / f"content-fixed-total-{args.content_scaling_total_bytes}-{concurrency}-{process_run}.json"
                    for concurrency in args.content_scaling_concurrency
                ],
                args.raw_dir / f"faults-{process_run}.json",
            ]
        )
    return paths


def main() -> None:
    args = parser().parse_args()
    args.output = args.output.resolve()
    args.raw_dir = (args.raw_dir or args.output.with_suffix("").with_name(args.output.stem + "-raw")).resolve()
    args.process_runs = args.process_runs or (1 if args.mode == "smoke" else 3)
    if args.process_runs < 1 or not args.service_binary.resolve().is_file():
        raise SystemExit("process runs must be positive and --service-binary must exist")
    args.service_binary = args.service_binary.resolve()
    args.distributed_binary = ROOT / "target/release/mutsuki-distributed-host"
    args.distributed_benchmark = ROOT / "target/release/mutsuki-distributed-benchmarks"
    args.placement_binary = ROOT / "target/release/placement_matrix"
    args.content_binary = ROOT / "target/release/content_localization"
    args.fault_binary = ROOT / "target/release/durability_faults"
    if os.name == "nt":
        args.distributed_binary = args.distributed_binary.with_suffix(".exe")
        args.distributed_benchmark = args.distributed_benchmark.with_suffix(".exe")
        args.placement_binary = args.placement_binary.with_suffix(".exe")
        args.content_binary = args.content_binary.with_suffix(".exe")
        args.fault_binary = args.fault_binary.with_suffix(".exe")
    args.system_samples = 2 if args.mode == "smoke" else 20
    args.placement_decisions = 10 if args.mode == "smoke" else 100
    args.registry_matrix = (
        [(100, ("fast", "durable", "critical"))]
        if args.mode == "smoke"
        else [
            (10_000, ("fast", "durable", "critical")),
            (100_000, ("fast",)),
            (1_000_000, ("fast",)),
        ]
    )
    args.content_sizes = [1024 * 1024] if args.mode == "smoke" else [1024 * 1024, 64 * 1024 * 1024, 1024 * 1024 * 1024]
    args.content_concurrency = [1, 4] if args.mode == "smoke" else [1, 4, 16]
    args.content_scaling_total_bytes = 64 * 1024 * 1024 if args.mode == "smoke" else 4 * 1024 * 1024 * 1024
    args.content_scaling_concurrency = [4, 16]
    args.content_samples = 2 if args.mode == "smoke" else 5
    args.fault_samples = 3 if args.mode == "smoke" else 20
    args.raw_dir.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if not args.skip_build:
        command(["cargo", "build", "--release", "-p", "mutsuki-distributed-host", "-p", "mutsuki-distributed-benchmarks", "--bins"])
        command(["cargo", "bench", "-p", "mutsuki-distributed-runtime", "--bench", "persistent_registry_stress", "--no-run"])
    expected = [args.distributed_binary, args.distributed_benchmark, args.placement_binary, args.content_binary, args.fault_binary]
    if any(not path.is_file() for path in expected):
        raise SystemExit("one or more release benchmark binaries are missing")

    expected_raw = expected_raw_paths(args)
    if args.reuse_raw:
        missing = [str(path) for path in expected_raw if not path.is_file()]
        if missing:
            raise SystemExit("raw matrix is incomplete:\n" + "\n".join(missing))
        generated = expected_raw
    else:
        generated = []
        for process_run in range(args.process_runs):
            generated.extend(run_raw(args, args.raw_dir, process_run))
    def by_name(prefix: str) -> list[Path]:
        return sorted(path for path in generated if path.name.startswith(prefix))

    environment_value = environment(args.mode, args.process_runs)
    groups = [
        merge_system(by_name("system-")),
        merge_placement(by_name("placement-")),
        merge_registry(by_name("registry-")),
        merge_content(by_name("content-"), int(environment_value["ram_bytes"])),
        merge_faults(by_name("faults-")),
    ]
    cases = [item for group, _ in groups for item in group]
    counters: dict[str, int] = defaultdict(int)
    for _, values in groups:
        for name, value in values.items():
            counters[name] += value
    revisions = parse_repositories(args.repository)
    generated_at = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    report = {
        "schema_version": "mutsuki.performance.report/v1",
        "suite_version": "mutsuki-distributed-host-issue22-v1",
        "workload_version": "mutsuki.performance.workload/v1",
        "report_id": f"distributed-{args.mode}-{generated_at}",
        "generated_at": generated_at,
        "revision_lock_hash": canonical_hash(revisions),
        "repository_revisions": revisions,
        "environment_id": canonical_hash(environment_value),
        "environment": environment_value,
        "feature_set": ["distributed", "durability", "content-localization", "placement"],
        "deployment": "real Controller, Worker, ServiceHost processes plus owner component benchmarks",
        "measurement_boundary": "loopback local IPC and real filesystem; no real-network claim",
        "sampling": {
            "warmup_iterations": 1,
            "samples_per_process": min(len(item["metrics"]["latency_ns"]["samples"]) for item in cases),
            "process_runs": args.process_runs,
        },
        "cases": cases,
        "correctness": {"passed": not any(counters.values()), "counters": dict(sorted(counters.items()))},
        "metadata": {
            "mode": args.mode,
            "raw_directory": args.raw_dir.name or "$OUTPUT_RAW",
        },
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    analysis_path = args.output.with_name(args.output.stem + "-analysis.json")
    analysis_path.write_text(json.dumps(analyze(cases, dict(counters)), indent=2) + "\n", encoding="utf-8")
    print(args.output)
    print(analysis_path)


if __name__ == "__main__":
    main()
