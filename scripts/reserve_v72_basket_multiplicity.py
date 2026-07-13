#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.governance.proof_registry import (
    MULTIPLICITY_EVENT,
    append_entry,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.validation.v72_basket_search_freeze import (
    EXPECTED_GLOBAL_N_TRIALS_BEFORE,
    RESERVATION_EVENT_ID,
    SEARCH_MANIFEST_PATH,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--proof-registry", default="mission/state/proof_registry.json")
    parser.add_argument("--preregistration-commit", required=True)
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    registry_path = Path(args.proof_registry)
    if not registry_path.is_absolute():
        registry_path = root / registry_path
    manifest_path = root / SEARCH_MANIFEST_PATH
    manifest_sha = _sha256(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    registry = load_and_verify(registry_path)
    existing = next(
        (
            row
            for row in registry["entries"]
            if row["event_id"] == RESERVATION_EVENT_ID
        ),
        None,
    )
    expected_after = int(manifest["multiplicity"]["raw_global_N_trials_after"])
    if existing is not None:
        if (
            int(existing["multiplicity"]["cumulative_N_trials"]) != expected_after
            or existing["evidence"]["search_manifest_sha256"] != manifest_sha
        ):
            raise RuntimeError("existing V7.2 reservation drift")
        print(json.dumps(existing, indent=2, sort_keys=True))
        return 0
    if multiplicity_trial_count(registry) != EXPECTED_GLOBAL_N_TRIALS_BEFORE:
        raise RuntimeError("unexpected proof registry N_trials before V7.2 reservation")
    delta = int(manifest["structure_count"])
    entry = append_entry(
        registry_path,
        {
            "event_id": RESERVATION_EVENT_ID,
            "event_type": MULTIPLICITY_EVENT,
            "recorded_at_utc": datetime.now(UTC).isoformat(),
            "status": "RESERVED_BEFORE_V72_STATIC_BASKET_RESULTS",
            "scientific_role": "MULTIPLICITY_RESERVATION_ONLY_NO_PROOF_WINDOW_CONSUMED",
            "evidence": {
                "search_manifest_path": SEARCH_MANIFEST_PATH,
                "search_manifest_sha256": manifest_sha,
                "component_bank_sha256": manifest["component_bank_sha256"],
                "preregistration_commit": args.preregistration_commit,
                "held_out_information_read": False,
            },
            "multiplicity": {
                "previous_N_trials": EXPECTED_GLOBAL_N_TRIALS_BEFORE,
                "delta_trials": delta,
                "cumulative_N_trials": EXPECTED_GLOBAL_N_TRIALS_BEFORE + delta,
                "campaign_raw_basket_allocation_trials": delta,
                "campaign_inflation_factor": 1.5,
                "campaign_effective_N_trials": delta * 1.5,
                "method": (
                    "Every distinct preregistered V7.2 basket-allocation structure; "
                    "four leave-one-block-out rotations share the same structural family"
                ),
            },
        },
    )
    print(json.dumps(entry, indent=2, sort_keys=True))
    return 0


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
