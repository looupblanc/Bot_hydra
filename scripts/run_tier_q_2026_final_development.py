#!/usr/bin/env python3
"""Run only the unsealed 2026 FINAL_DEVELOPMENT stage, exactly once.

The script has no code path for the confirmation partition.  It emits
deterministic, self-hashed artifacts for relay by HYDRA's authoritative writer.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping

for variable in (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ.setdefault(variable, "1")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.economic_evolution.schema import stable_hash
from hydra.production.tier_q_2026_two_stage_runner import (
    FINAL_DEVELOPMENT,
    build_role_feature_bundles,
    evaluate_stage,
    load_frozen_bindings,
)
from hydra.validation.lockbox_guard import current_commit


DEFAULT_CONTRACT = Path("config/research/tier_q_2026_two_stage_confirmation_v1.json")
DEFAULT_RECEIPT = Path("reports/data_access/tier_q_2026_acquisition_receipt.json")
DEFAULT_OUTPUT = Path(
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/branch_results/"
    "tier_q_2026_two_stage"
)


def run_final_development(
    root: str | Path,
    *,
    contract_path: str | Path = DEFAULT_CONTRACT,
    acquisition_receipt_path: str | Path = DEFAULT_RECEIPT,
    output_directory: str | Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    project = Path(root).resolve()
    contract = _json(_inside(project, contract_path))
    acquisition = _json(_inside(project, acquisition_receipt_path))
    output = _inside(project, output_directory)
    output.mkdir(parents=True, exist_ok=True)
    lock_path = output / "final_development.lock"
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        existing = _existing_result(
            output / "final_development_result.json",
            expected_contract_hash=str(contract["contract_hash"]),
        )
        if existing is not None:
            return existing
        bindings, rules, rule_receipt = load_frozen_bindings(project, contract)
        features = build_role_feature_bundles(
            contract,
            acquisition,
            role=FINAL_DEVELOPMENT,
            cache_root=(
                project
                / "data/cache/databento/tier_q_2026_confirmation/"
                "97a80942156d15b9801d/final_development_feature_matrices"
            ),
        )
        _persist_once(output / "final_development_feature_receipt.json", features)
        result = evaluate_stage(
            contract,
            acquisition,
            features,
            role=FINAL_DEVELOPMENT,
            bindings=bindings,
            rules=rules,
            rule_receipt=rule_receipt,
        )
        _persist_once(output / "final_development_result.json", result)
        summary_core = {
            "schema": "hydra_tier_q_2026_final_development_summary_v1",
            "status": "FINAL_DEVELOPMENT_COMPLETE",
            "contract_hash": contract["contract_hash"],
            "feature_receipt_hash": features["result_hash"],
            "result_hash": result["result_hash"],
            "source_commit": current_commit(),
            "runner_file_sha256": _sha256(
                project / "hydra/production/tier_q_2026_two_stage_runner.py"
            ),
            "script_file_sha256": _sha256(Path(__file__).resolve()),
            "candidate_count": len(result["candidate_results"]),
            "tier_g_candidate_ids": list(result["tier_g_candidate_ids"]),
            "confirmation_opened": False,
            "retuning_performed": False,
            "recalibration_performed": False,
            "candidates": [
                {
                    "candidate_id": row["candidate_id"],
                    "normal_passes": row["normal"]["pass_count"],
                    "normal_episodes": row["normal"]["episode_count"],
                    "stressed_passes": row["stressed"]["pass_count"],
                    "stressed_episodes": row["stressed"]["episode_count"],
                    "stressed_net_usd": row["stressed"]["net_total_usd"],
                    "stressed_mll_breach_rate": row["stressed"]["mll_breach_rate"],
                    "stressed_target_progress_median": row["stressed"][
                        "target_progress_median"
                    ],
                    "resulting_evidence_tier": row["resulting_evidence_tier"],
                }
                for row in result["candidate_results"]
            ],
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        }
        summary = {**summary_core, "summary_hash": stable_hash(summary_core)}
        _persist_once(output / "final_development_summary.json", summary)
        return result


def _existing_result(path: Path, *, expected_contract_hash: str) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    value = _json(path)
    core = dict(value)
    claimed = str(core.pop("result_hash", ""))
    if (
        not claimed
        or stable_hash(core) != claimed
        or value.get("contract_hash") != expected_contract_hash
        or value.get("role") != FINAL_DEVELOPMENT
        or value.get("confirmation_evaluated") is not False
    ):
        raise RuntimeError("existing final-development result drift")
    return value


def _persist_once(path: Path, value: Mapping[str, Any]) -> None:
    payload = json.dumps(dict(value), indent=2, sort_keys=True, default=str) + "\n"
    if path.is_file():
        if path.read_text(encoding="utf-8") != payload:
            raise RuntimeError(f"immutable result collision: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = (root / path).resolve() if not path.is_absolute() else path.resolve()
    resolved.relative_to(root)
    return resolved


def _json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"JSON artifact must be an object: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run frozen Tier-Q 2026 FINAL_DEVELOPMENT only")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--contract", default=str(DEFAULT_CONTRACT))
    parser.add_argument("--receipt", default=str(DEFAULT_RECEIPT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()
    print(
        json.dumps(
            {
                "status": "STARTED_FINAL_DEVELOPMENT_ONLY",
                "pid": os.getpid(),
                "confirmation_opened": False,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    result = run_final_development(
        args.root,
        contract_path=args.contract,
        acquisition_receipt_path=args.receipt,
        output_directory=args.output,
    )
    print(
        json.dumps(
            {
                "status": "FINAL_DEVELOPMENT_COMPLETE",
                "pid": os.getpid(),
                "result_hash": result["result_hash"],
                "tier_g_candidate_ids": result["tier_g_candidate_ids"],
                "confirmation_opened": False,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
