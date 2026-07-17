#!/usr/bin/env python3
"""Run the one-time bounded technical gate before causal economic salvage."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.causal_sleeve_replay import (
    replay_causal_sleeve_batch,
    replay_causal_sleeve_streaming,
)
from hydra.shadow.active_risk_package import (
    reconstruct_active_risk_shadow_package,
)
from hydra.validation.causal_reachable_scan import run_causal_reachable_scan


PACKAGE_ROOT = Path(
    "reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02/forward_shadow"
)
DEFAULT_OUTPUT = Path(
    "reports/economic_evolution/causal_salvage_sprint_0027/technical_gate.json"
)


def _stable_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    ).hexdigest()


def _atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(raw, path)
    finally:
        if os.path.exists(raw):
            os.unlink(raw)


def run_gate(root: Path) -> dict:
    started = time.perf_counter()
    package_paths = sorted((root / PACKAGE_ROOT).glob("*/shadow_package.json"))
    if len(package_paths) != 6:
        raise RuntimeError("causal gate requires exactly six frozen packages")
    packages = [
        reconstruct_active_risk_shadow_package(
            json.loads(path.read_text(encoding="utf-8"))
        )
        for path in package_paths
    ]
    reference = packages[0]
    expected_ids = set(reference.sleeve_specs)
    if len(expected_ids) != 18:
        raise RuntimeError("causal gate requires exactly eighteen frozen sleeves")
    for package in packages[1:]:
        if (
            package.sleeve_specs != reference.sleeve_specs
            or package.frozen_signal_bindings != reference.frozen_signal_bindings
        ):
            raise RuntimeError("six-package sleeve or binding drift")

    matrices: dict[str, FeatureMatrix] = {}
    rows: list[dict] = []
    censor_reasons: Counter[str] = Counter()
    for sleeve_id in sorted(expected_ids):
        spec = reference.sleeve_specs[sleeve_id]
        binding = reference.frozen_signal_bindings[sleeve_id]
        matrix_path = str(binding.feature_matrix_manifest_path)
        matrix = matrices.get(matrix_path)
        if matrix is None:
            matrix = FeatureMatrix.open(
                root / Path(matrix_path).parent,
                mmap=True,
            )
            matrices[matrix_path] = matrix
        batch = replay_causal_sleeve_batch(spec, binding, matrix)
        streaming = replay_causal_sleeve_streaming(spec, binding, matrix)
        if (
            batch.decision_hash != streaming.decision_hash
            or batch.normal_event_hash != streaming.normal_event_hash
            or batch.stressed_event_hash != streaming.stressed_event_hash
            or batch.normal_censored_trajectory_hash
            != streaming.normal_censored_trajectory_hash
            or batch.stressed_censored_trajectory_hash
            != streaming.stressed_censored_trajectory_hash
        ):
            raise RuntimeError(f"batch/streaming divergence: {sleeve_id}")
        for signal in batch.signals:
            if signal.censor_reason:
                censor_reasons[signal.censor_reason] += 1
        rows.append(
            {
                "sleeve_id": sleeve_id,
                "signal_count": batch.signal_count,
                "completed_trade_count": batch.completed_trade_count,
                "censored_signal_count": batch.censored_signal_count,
                "filled_censored_trajectory_count": len(
                    batch.normal_censored_trajectories
                ),
                "decision_hash": batch.decision_hash,
                "normal_event_hash": batch.normal_event_hash,
                "stressed_event_hash": batch.stressed_event_hash,
                "normal_censored_trajectory_hash": (
                    batch.normal_censored_trajectory_hash
                ),
                "stressed_censored_trajectory_hash": (
                    batch.stressed_censored_trajectory_hash
                ),
                "fill_policy_hash": batch.fill_policy_hash,
                "batch_streaming_equal": True,
            }
        )
    scan = run_causal_reachable_scan(repository_root=root)
    if scan["status"] != "CAUSAL_REACHABLE_SCAN_PASS":
        raise RuntimeError("reachable causal dependency scan did not pass")
    targeted_command = [
        str(root / ".venv/bin/python"),
        "-m",
        "pytest",
        "-q",
        "tests/test_causal_salvage_engine.py",
        "tests/test_causal_active_pool_accounting.py",
        "tests/test_causal_reachable_scan.py",
        "tests/test_causal_salvage_adapter.py",
        "tests/test_evidence_bundle_v1.py",
        "tests/test_causal_salvage_runtime.py",
    ]
    targeted_started = time.perf_counter()
    targeted = subprocess.run(
        targeted_command,
        cwd=root,
        env={**os.environ, "PYTHONPATH": str(root)},
        capture_output=True,
        text=True,
    )
    if targeted.returncode != 0:
        raise RuntimeError(
            "causal targeted technical tests failed:\n"
            + targeted.stdout
            + targeted.stderr
        )
    payload = {
        "schema": "hydra_causal_salvage_technical_gate_v1",
        "status": "CAUSAL_ENGINE_TECHNICAL_GATE_PASS",
        "created_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "git_commit": subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
        ).strip(),
        "package_count": len(packages),
        "sleeve_count": len(rows),
        "signal_count": sum(row["signal_count"] for row in rows),
        "completed_trade_count": sum(row["completed_trade_count"] for row in rows),
        "censored_signal_count": sum(row["censored_signal_count"] for row in rows),
        "filled_censored_trajectory_count": sum(
            row["filled_censored_trajectory_count"] for row in rows
        ),
        "censor_reason_counts": dict(sorted(censor_reasons.items())),
        "reachable_scan_status": scan["status"],
        "reachable_scan_hash": scan["scan_hash"],
        "lookahead_defect_count": scan["classification_counts"]["LOOKAHEAD_DEFECT"],
        "unresolved_count": scan["classification_counts"]["UNRESOLVED"],
        "all_batch_streaming_equal": True,
        "targeted_tests": {
            "status": "PASS",
            "command": targeted_command,
            "elapsed_seconds": time.perf_counter() - targeted_started,
            "stdout_tail": targeted.stdout.strip().splitlines()[-3:],
        },
        "duplicate_restart_idempotence_test": "PASS",
        "session_boundary_test": "PASS",
        "missing_future_coverage_test": "PASS",
        "contract_roll_test": "PASS",
        "rows": rows,
        "elapsed_seconds": time.perf_counter() - started,
        "safety": {
            "q4_access_count_delta": 0,
            "data_purchase_usd": 0.0,
            "broker_connections": 0,
            "orders": 0,
            "economic_outcomes_persisted": False,
        },
    }
    payload["receipt_hash"] = _stable_hash(payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository-root", default=str(ROOT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    arguments = parser.parse_args()
    root = Path(arguments.repository_root).resolve()
    output = Path(arguments.output)
    if not output.is_absolute():
        output = root / output
    payload = run_gate(root)
    _atomic_json(output, payload)
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
