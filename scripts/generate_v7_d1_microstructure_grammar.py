from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.v7_d1_microstructure_grammar import (
    GRAMMAR_ID,
    PREREGISTRATION_SHA256,
    candidate_specs,
    generate_signal_population,
    load_feature_store,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate frozen outcome-free D1 microstructure signals."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--output",
        default="reports/v7/data/d1_microstructure_grammar0001_signal_manifest.json",
    )
    args = parser.parse_args()
    minute, event = load_feature_store(args.project_root)
    signals = generate_signal_population(minute, event)
    payload = {
        "schema": "hydra_v7_d1_microstructure_signal_manifest_v1",
        "grammar_id": GRAMMAR_ID,
        "preregistration_sha256": PREREGISTRATION_SHA256,
        "candidate_specs": [row.to_dict() for row in candidate_specs()],
        "candidate_count": len(signals),
        "signal_count": sum(len(rows) for rows in signals.values()),
        "signals": {
            candidate_id: [row.to_dict() for row in rows]
            for candidate_id, rows in signals.items()
        },
        "contains_future_outcomes_or_pnl": False,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "Signals use only completed print features, but their economic "
            "value remains entirely unresolved until the separately committed "
            "new-dataset tripwire and tribunal run."
        ),
    }
    payload["manifest_hash"] = _stable_hash(payload)
    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, destination)
    print(
        json.dumps(
            {
                "path": str(destination),
                "sha256": _sha256(destination),
                "manifest_hash": payload["manifest_hash"],
                "candidate_count": payload["candidate_count"],
                "signal_count": payload["signal_count"],
                "signal_counts": {
                    candidate_id: len(rows)
                    for candidate_id, rows in signals.items()
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _stable_hash(payload: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
