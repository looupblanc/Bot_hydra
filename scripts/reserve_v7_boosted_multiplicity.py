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


EVENT_ID = "v7_boosted_dual_track_tournament_0001_multiplicity_reservation"
WORM_PATH = "WORM/v7-boosted-dual-track-tournament-0001-2026-07-13.json"
WORM_SHA256 = "fb99e1304b59a50a7a38a1915d64cdace4dfa4c4b8e55b0ec22ec62a24e15969"
EXPECTED_BEFORE = 265_347
DELTA_TRIALS = 1_607
EXPECTED_AFTER = EXPECTED_BEFORE + DELTA_TRIALS
OUTPUT_PATH = (
    "reports/v7_boosted/tournament_0001/"
    "v7_boosted_multiplicity_reservation.json"
)


def reserve(
    *,
    project_root: str | Path,
    proof_registry_path: str | Path,
    preregistration_commit: str,
) -> dict[str, object]:
    root = Path(project_root).resolve()
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    worm = root / WORM_PATH
    if _sha256(worm) != WORM_SHA256:
        raise RuntimeError("boosted WORM hash drift")
    manifest = json.loads(worm.read_text(encoding="utf-8"))
    reservation = manifest["multiplicity_reservation"]
    if (
        int(reservation["delta_raw_trials"]) != DELTA_TRIALS
        or float(reservation["campaign_effective_N_trials"]) != 2_410.5
        or manifest["frozen_before_new_feature_signal_pnl_or_account_results"]
        is not True
    ):
        raise RuntimeError("boosted WORM multiplicity policy drift")
    registry = load_and_verify(proof_path)
    if burned_window_ids(registry) != ("Q4_2024",):
        raise RuntimeError("unexpected proof-window state before boosted reservation")
    existing = next(
        (row for row in registry["entries"] if row["event_id"] == EVENT_ID),
        None,
    )
    if existing is not None:
        if (
            int(existing["multiplicity"]["cumulative_N_trials"])
            != EXPECTED_AFTER
            or existing["evidence"]["worm_sha256"] != WORM_SHA256
        ):
            raise RuntimeError("existing boosted multiplicity reservation drift")
        entry = existing
    else:
        if multiplicity_trial_count(registry) != EXPECTED_BEFORE:
            raise RuntimeError("unexpected N_trials before boosted reservation")
        entry = append_entry(
            proof_path,
            {
                "event_id": EVENT_ID,
                "event_type": MULTIPLICITY_EVENT,
                "recorded_at_utc": datetime.now(UTC).isoformat(),
                "status": "RESERVED_BEFORE_ANY_BOOSTED_RESULT",
                "scientific_role": (
                    "MULTIPLICITY_RESERVATION_ONLY_NO_PROOF_WINDOW_CONSUMED"
                ),
                "evidence": {
                    "tournament_id": manifest["tournament_id"],
                    "worm_path": WORM_PATH,
                    "worm_sha256": WORM_SHA256,
                    "preregistration_commit": preregistration_commit,
                    "new_feature_results_seen": False,
                    "new_signal_results_seen": False,
                    "new_pnl_results_seen": False,
                    "new_account_results_seen": False,
                    "new_data_purchase": False,
                    "q4_access": False,
                    "outbound_orders": 0,
                },
                "multiplicity": {
                    "previous_N_trials": EXPECTED_BEFORE,
                    "delta_trials": DELTA_TRIALS,
                    "cumulative_N_trials": EXPECTED_AFTER,
                    "mechanism_structural_trials": 256,
                    "mechanism_real_and_null_world_trials": 1_024,
                    "basket_structure_trials": 320,
                    "rolling_diagnostic_trials": 7,
                    "campaign_inflation_factor": 1.5,
                    "campaign_effective_N_trials": 2_410.5,
                    "method": (
                        "All frozen boosted mechanism structures, their real/null "
                        "world evaluations, bounded baskets and rolling diagnostics "
                        "reserved before any new result."
                    ),
                },
            },
        )
    result: dict[str, object] = {
        "schema": "hydra_v7_boosted_multiplicity_reservation_v1",
        "event_id": EVENT_ID,
        "status": "RESERVED_BEFORE_ANY_BOOSTED_RESULT",
        "worm_path": WORM_PATH,
        "worm_sha256": WORM_SHA256,
        "preregistration_commit": preregistration_commit,
        "previous_N_trials": EXPECTED_BEFORE,
        "delta_trials": DELTA_TRIALS,
        "cumulative_N_trials": EXPECTED_AFTER,
        "entry_hash": entry["entry_hash"],
        "burned_windows": list(burned_window_ids(load_and_verify(proof_path))),
        "new_data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "Conservative reservation counts work that may be stopped early; this "
            "reduces power but prevents unlogged optional stopping."
        ),
        "prochaine_action": "build_outcome_free_boosted_feature_and_signal_manifests",
    }
    _write_once_json(root / OUTPUT_PATH, result)
    return result


def _write_once_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != raw:
            raise RuntimeError(f"write-once result drift: {path}")
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
    parser.add_argument("--proof-registry", default="mission/state/proof_registry.json")
    parser.add_argument("--preregistration-commit", required=True)
    args = parser.parse_args()
    result = reserve(
        project_root=args.project_root,
        proof_registry_path=args.proof_registry,
        preregistration_commit=args.preregistration_commit,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
