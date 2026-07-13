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
    ANNOTATION_EVENT,
    append_entry,
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)


RESERVATION_EVENT_ID = (
    "v7_boosted_dual_track_tournament_0001_multiplicity_reservation"
)
ANNOTATION_EVENT_ID = (
    "v7_boosted_dual_track_tournament_0001_superseded_before_result"
)
AMENDMENT_PATH = "MISSION_CONTRACT_AMENDMENT_004_ECONOMIC_EVOLUTION.md"
AMENDMENT_SHA256 = (
    "0e7c9da13f6f04b5cb0ae7deffca23a9af3af8143c9e4ba8bc17807bccd6747a"
)
AMENDMENT_COMMIT = "e82aaf61b1eb19ebc8fee488890b539a3ae97ba4"
EXPECTED_TRIAL_COUNT = 266_954
RESULT_DIR = "reports/v7_boosted/tournament_0001"
OUTPUT_PATH = f"{RESULT_DIR}/v7_boosted_supersession_annotation.json"
ALLOWED_PREEXISTING_FILES = frozenset(
    {
        "v7_boosted_multiplicity_reservation.json",
        "v7_boosted_supersession_annotation.json",
    }
)


def annotate_supersession(
    *,
    project_root: str | Path,
    proof_registry_path: str | Path,
    output_path: str | Path = OUTPUT_PATH,
) -> dict[str, object]:
    root = Path(project_root).resolve()
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    output = Path(output_path)
    if not output.is_absolute():
        output = root / output

    amendment = root / AMENDMENT_PATH
    if _sha256(amendment) != AMENDMENT_SHA256:
        raise RuntimeError("economic evolution amendment hash drift")
    _assert_no_boosted_results(root)

    before = load_and_verify(proof_path)
    trials_before = multiplicity_trial_count(before)
    if burned_window_ids(before) != ("Q4_2024",):
        raise RuntimeError("unexpected proof-window state before supersession")
    reservation = next(
        (
            row
            for row in before["entries"]
            if row["event_id"] == RESERVATION_EVENT_ID
        ),
        None,
    )
    if reservation is None:
        raise RuntimeError("boosted multiplicity reservation missing")
    if (
        reservation["status"] != "RESERVED_BEFORE_ANY_BOOSTED_RESULT"
        or int(reservation["multiplicity"]["cumulative_N_trials"])
        != EXPECTED_TRIAL_COUNT
    ):
        raise RuntimeError("boosted reservation status drift")

    existing = next(
        (
            row
            for row in before["entries"]
            if row["event_id"] == ANNOTATION_EVENT_ID
        ),
        None,
    )
    if existing is None:
        if trials_before != EXPECTED_TRIAL_COUNT:
            raise RuntimeError(
                "supersession cannot be added retroactively after later trials"
            )
        annotation = append_entry(
            proof_path,
            {
                "event_id": ANNOTATION_EVENT_ID,
                "event_type": ANNOTATION_EVENT,
                "recorded_at_utc": datetime.now(UTC).isoformat(),
                "references_event_id": RESERVATION_EVENT_ID,
                "status": "SUPERSEDED_BEFORE_ANY_RESULT",
                "correction": {
                    "execution_status": "SUPERSEDED_BEFORE_ANY_RESULT",
                    "replacement_engine": "ECONOMIC_EVOLUTION_ENGINE_V1",
                    "amendment_path": AMENDMENT_PATH,
                    "amendment_sha256": AMENDMENT_SHA256,
                    "amendment_commit": AMENDMENT_COMMIT,
                    "reservation_remains_in_global_N_trials": True,
                    "reserved_trial_count": 1_607,
                    "new_feature_results_seen": False,
                    "new_signal_results_seen": False,
                    "new_pnl_results_seen": False,
                    "new_account_results_seen": False,
                    "new_data_purchase": False,
                    "q4_access": False,
                    "outbound_orders": 0,
                },
            },
        )
    else:
        correction = existing.get("correction") or {}
        if (
            existing.get("references_event_id") != RESERVATION_EVENT_ID
            or correction.get("amendment_sha256") != AMENDMENT_SHA256
            or correction.get("reservation_remains_in_global_N_trials") is not True
        ):
            raise RuntimeError("existing supersession annotation drift")
        annotation = existing

    after = load_and_verify(proof_path)
    if multiplicity_trial_count(after) != trials_before:
        raise RuntimeError("supersession annotation changed multiplicity")
    if burned_window_ids(after) != ("Q4_2024",):
        raise RuntimeError("supersession annotation changed proof windows")

    result: dict[str, object] = {
        "schema": "hydra_v7_boosted_supersession_annotation_v1",
        "tournament_id": "hydra_v7_boosted_dual_track_tournament_0001",
        "status": "SUPERSEDED_BEFORE_ANY_RESULT",
        "reservation_event_id": RESERVATION_EVENT_ID,
        "annotation_event_id": ANNOTATION_EVENT_ID,
        "annotation_entry_hash": annotation["entry_hash"],
        "amendment_path": AMENDMENT_PATH,
        "amendment_sha256": AMENDMENT_SHA256,
        "amendment_commit": AMENDMENT_COMMIT,
        "global_N_trials": EXPECTED_TRIAL_COUNT,
        "reserved_trial_count_retained": 1_607,
        "new_feature_results_seen": False,
        "new_signal_results_seen": False,
        "new_pnl_results_seen": False,
        "new_account_results_seen": False,
        "new_data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "The conservative reservation lowers future power even though the "
            "superseded tournament produced no outcomes; retaining it prevents "
            "retroactive optional-stopping bias."
        ),
        "prochaine_action": "preregister_economic_evolution_engine_pilot",
    }
    _write_once_json(output, result)
    return result


def _assert_no_boosted_results(root: Path) -> None:
    report_dir = root / RESULT_DIR
    observed = {
        path.name for path in report_dir.iterdir() if path.is_file()
    } if report_dir.exists() else set()
    unexpected = observed - ALLOWED_PREEXISTING_FILES
    if unexpected:
        raise RuntimeError(f"boosted results already exist: {sorted(unexpected)}")
    cache_dir = root / "data/cache/v7_boosted"
    if cache_dir.exists() and any(path.is_file() for path in cache_dir.rglob("*")):
        raise RuntimeError("boosted feature or signal cache already exists")


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
    parser.add_argument(
        "--proof-registry", default="mission/state/proof_registry.json"
    )
    parser.add_argument("--output", default=OUTPUT_PATH)
    args = parser.parse_args()
    result = annotate_supersession(
        project_root=args.project_root,
        proof_registry_path=args.proof_registry,
        output_path=args.output,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
