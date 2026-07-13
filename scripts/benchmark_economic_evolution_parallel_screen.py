#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.generator import generate_structural_population
from hydra.economic_evolution.parallel_screen import (
    run_ultra_cheap_screen_parallel,
    run_ultra_cheap_screen_processes,
)
from hydra.economic_evolution.screen import CheapScreenPolicy
from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.economic_evolution_pilot import (
    _load_preregistration,
    _validate_preregistration,
    _verify_data_fingerprint,
)
from hydra.research.turbo_feature_builder import build_or_open_turbo_feature_bundles


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preregistration", required=True)
    parser.add_argument("--contract-map", required=True)
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--baseline-run-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument(
        "--mode", choices=("threads", "processes"), default="threads"
    )
    args = parser.parse_args()

    prereg_path = Path(args.preregistration).resolve()
    prereg, source = _load_preregistration(prereg_path)
    _validate_preregistration(prereg, prereg_path)
    feature_build = build_or_open_turbo_feature_bundles(
        cache_root=args.cache_root,
        contract_map_path=args.contract_map,
    )
    matrices = {
        market: FeatureMatrix.open(path, mmap=True)
        for market, path in feature_build.market_paths.items()
    }
    _verify_data_fingerprint(
        prereg,
        feature_build.source_fingerprint,
        args.contract_map,
        feature_build.market_paths,
    )
    generated = generate_structural_population(
        campaign_id=str(prereg["campaign_id"]),
        raw_proposal_count=int(prereg["funnel"]["raw_proposals"]),
    )
    if generated.candidate_manifest_hash != str(
        prereg["structural_population"]["candidate_manifest_hash"]
    ):
        raise RuntimeError("frozen structural population drift")

    baseline_dir = Path(args.baseline_run_dir)
    baseline_summary = json.loads(
        (baseline_dir / "cheap_screen_summary.json").read_text(encoding="utf-8")
    )
    baseline_rows = _read_jsonl(baseline_dir / "cheap_screen_results.jsonl")
    before_cpu = time.process_time()
    child_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    before_wall = time.perf_counter()
    policy = CheapScreenPolicy(**prereg["cheap_screen_policy"])
    if args.mode == "threads":
        candidate = run_ultra_cheap_screen_parallel(
            generated.sleeves,
            matrices,
            policy=policy,
            worker_count=args.workers,
        )
    else:
        candidate = run_ultra_cheap_screen_processes(
            generated.sleeves,
            feature_build.market_paths,
            policy=policy,
            worker_count=args.workers,
        )
    wall = time.perf_counter() - before_wall
    parent_cpu = time.process_time() - before_cpu
    child_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    child_cpu = (
        child_after.ru_utime
        + child_after.ru_stime
        - child_before.ru_utime
        - child_before.ru_stime
    )
    aggregate_cpu = parent_cpu + child_cpu
    candidate_rows = list(candidate.rows)
    rows_identical = candidate_rows == baseline_rows
    if not rows_identical:
        raise RuntimeError("parallel screen changed frozen row outputs")

    baseline_seconds = float(baseline_summary["elapsed_seconds"])
    report: dict[str, Any] = {
        "schema": "hydra_economic_evolution_parallel_screen_benchmark_v1",
        "campaign_id": prereg["campaign_id"],
        "preregistration_source": source,
        "data_fingerprint": feature_build.source_fingerprint,
        "candidate_manifest_hash": generated.candidate_manifest_hash,
        "worker_count": args.workers,
        "execution_mode": args.mode,
        "one_authoritative_writer": True,
        "writer_pid": os.getpid(),
        "baseline": {
            "elapsed_seconds": baseline_seconds,
            "screens_per_second": float(baseline_summary["screens_per_second"]),
            "result_sha256": _sha256(
                baseline_dir / "cheap_screen_results.jsonl"
            ),
        },
        "parallel": {
            "elapsed_seconds": wall,
            "coordinator_cpu_seconds": parent_cpu,
            "child_cpu_seconds": child_cpu,
            "aggregate_cpu_seconds": aggregate_cpu,
            "aggregate_cpu_utilization_pct_of_one_core": 100.0
            * aggregate_cpu
            / max(wall, 1e-12),
            "screens_per_second": candidate.screens_per_second,
            "coordinator_peak_rss_mb": resource.getrusage(
                resource.RUSAGE_SELF
            ).ru_maxrss
            / 1024.0,
            "maximum_single_child_rss_mb": child_after.ru_maxrss / 1024.0,
            "result_canonical_sha256": _canonical_rows_hash(candidate_rows),
        },
        "comparison": {
            "rows_identical": rows_identical,
            "row_count": len(candidate_rows),
            "unique_execution_paths": candidate.unique_execution_path_count,
            "survivor_count": len(candidate.survivors),
            "speedup": baseline_seconds / max(wall, 1e-12),
        },
        "governance": {
            "scientific_outcome_changed": False,
            "threshold_changed": False,
            "new_data_purchase": False,
            "q4_access": False,
            "broker_connections": 0,
            "orders": 0,
        },
        "CONTRE": (
            "The benchmark reuses a warm local cache and measures one frozen "
            "population; persistent-cycle speed may differ."
        ),
    }
    writer = AtomicResultWriter(args.output_dir)
    writer.write_json("parallel_screen_benchmark.json", report)
    print(json.dumps(report, sort_keys=True, indent=2))
    return 0


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_rows_hash(rows: list[dict[str, Any]]) -> str:
    return hashlib.sha256(
        json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
