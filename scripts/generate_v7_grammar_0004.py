from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.v7_hypothesis_grammar_0004 import (
    GRAMMAR_ID,
    PREREGISTRATION_SHA256,
    candidate_specs,
    generate_signal_population,
    load_v7_market_bars,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate the outcome-free V7 grammar 0004 signal manifest."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--output", default="reports/v7/phase4/grammar0004_signal_manifest.json"
    )
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    bars = load_v7_market_bars(root)
    signals = generate_signal_population(
        bars, graveyard_path=root / "mission/state/graveyard.db"
    )
    payload = {
        "schema": "hydra_v7_grammar_0004_signal_manifest_v1",
        "grammar_id": GRAMMAR_ID,
        "preregistration_sha256": PREREGISTRATION_SHA256,
        "candidate_specs": [row.to_dict() for row in candidate_specs()],
        "candidate_count": len(signals),
        "signal_count": sum(len(rows) for rows in signals.values()),
        "signals": {
            candidate_id: [row.to_dict() for row in rows]
            for candidate_id, rows in signals.items()
        },
        "market_bundle_hashes": {
            market: row.bundle_hash for market, row in sorted(bars.items())
        },
        "contains_outcomes_or_pnl": False,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "Outcome-free signals can still encode weak economic hypotheses; "
            "only the separately committed tribunal may inspect returns."
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
