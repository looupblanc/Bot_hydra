#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.governance.proof_registry import (
    MULTIPLICITY_EVENT,
    append_entry,
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)


EVENT_ID = "hydra_economic_evolution_pilot_0001_multiplicity_reservation"
WORM_PATH = "config/v7/economic_evolution_pilot_0001.json"
WORM_SHA256 = "dced549b9df09381c2bd6108e55e680a9c8fed7d384a21d2a293a551917e3ab2"
WORM_COMMIT = "91380f91076dba7aece9dad6317e1270e5f00953"
EXPECTED_BEFORE = 266_954
DELTA_TRIALS = 52_000
EXPECTED_AFTER = EXPECTED_BEFORE + DELTA_TRIALS
OUTPUT_PATH = (
    "reports/economic_evolution/pilot_0001/"
    "multiplicity_reservation.json"
)


def reserve(
    *,
    project_root: str | Path,
    proof_registry_path: str | Path,
) -> dict[str, object]:
    root = Path(project_root).resolve()
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    worm = root / WORM_PATH
    if _sha256(worm) != WORM_SHA256:
        raise RuntimeError("economic-evolution WORM hash drift")
    manifest = json.loads(worm.read_text(encoding="utf-8"))
    if (
        int(manifest["multiplicity"]["prospective_global_reservation"])
        != DELTA_TRIALS
        or float(manifest["multiplicity"]["campaign_specific_inflation"])
        != 1.5
        or manifest["structural_population"]["outcome_feedback_used"] is not False
    ):
        raise RuntimeError("economic-evolution multiplicity policy drift")
    registry = load_and_verify(proof_path)
    if burned_window_ids(registry) != ("Q4_2024",):
        raise RuntimeError("unexpected proof-window state")
    existing = next(
        (row for row in registry["entries"] if row["event_id"] == EVENT_ID),
        None,
    )
    if existing is None:
        if multiplicity_trial_count(registry) != EXPECTED_BEFORE:
            raise RuntimeError("unexpected N_trials before pilot reservation")
        entry = append_entry(
            proof_path,
            {
                "event_id": EVENT_ID,
                "event_type": MULTIPLICITY_EVENT,
                "recorded_at_utc": datetime.now(UTC).isoformat(),
                "status": "RESERVED_BEFORE_ANY_ECONOMIC_EVOLUTION_OUTCOME",
                "scientific_role": (
                    "MULTIPLICITY_RESERVATION_ONLY_NO_PROOF_WINDOW_CONSUMED"
                ),
                "evidence": {
                    "campaign_id": manifest["campaign_id"],
                    "worm_path": WORM_PATH,
                    "worm_sha256": WORM_SHA256,
                    "worm_commit": WORM_COMMIT,
                    "candidate_manifest_hash": manifest["structural_population"][
                        "candidate_manifest_hash"
                    ],
                    "feature_results_seen": False,
                    "signal_results_seen": False,
                    "pnl_results_seen": False,
                    "account_results_seen": False,
                    "new_data_purchase": False,
                    "q4_access": False,
                    "outbound_orders": 0,
                },
                "multiplicity": {
                    "previous_N_trials": EXPECTED_BEFORE,
                    "delta_trials": DELTA_TRIALS,
                    "cumulative_N_trials": EXPECTED_AFTER,
                    "maximum_structural_proposals": 50_000,
                    "maximum_exact_component_replays": 600,
                    "maximum_incremental_value_evaluations": 400,
                    "maximum_account_policy_structures": 600,
                    "maximum_rolling_elites": 20,
                    "maximum_xfa_elites": 5,
                    "maximum_directed_mutations": 20,
                    "campaign_inflation_factor": 1.5,
                    "campaign_effective_N_trials": DELTA_TRIALS * 1.5,
                    "method": (
                        "Conservative upper-bound reservation before any real "
                        "screen, PnL, account, Combine or XFA outcome."
                    ),
                },
            },
        )
    else:
        if (
            int(existing["multiplicity"]["cumulative_N_trials"])
            != EXPECTED_AFTER
            or existing["evidence"]["worm_sha256"] != WORM_SHA256
        ):
            raise RuntimeError("existing pilot reservation drift")
        entry = existing
    after = load_and_verify(proof_path)
    result: dict[str, object] = {
        "schema": "hydra_economic_evolution_multiplicity_reservation_v1",
        "event_id": EVENT_ID,
        "status": "RESERVED_BEFORE_ANY_ECONOMIC_EVOLUTION_OUTCOME",
        "worm_path": WORM_PATH,
        "worm_sha256": WORM_SHA256,
        "worm_commit": WORM_COMMIT,
        "previous_N_trials": EXPECTED_BEFORE,
        "delta_trials": DELTA_TRIALS,
        "cumulative_N_trials": multiplicity_trial_count(after),
        "entry_hash": entry["entry_hash"],
        "burned_windows": list(burned_window_ids(after)),
        "new_data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "The upper-bound reservation counts many structures that will die "
            "before statistical testing; this is conservative and reduces final power."
        ),
        "prochaine_action": "run_fixed_validator_controls_then_pilot_funnel",
    }
    _write_once_json(root / OUTPUT_PATH, result)
    return result


def _write_once_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != raw:
            raise RuntimeError(f"write-once reservation drift: {path}")
        return
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with temporary.open("x", encoding="utf-8") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--proof-registry", default="mission/state/proof_registry.json"
    )
    args = parser.parse_args()
    print(
        json.dumps(
            reserve(
                project_root=args.project_root,
                proof_registry_path=args.proof_registry,
            ),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
