from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.v7_hypothesis_grammar_0003 import (
    PREREGISTRATION_SHA256,
    candidate_specs,
    generate_signal_population,
    load_v7_market_bars,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate outcome-free V7 grammar 0003 signals."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--preregistration",
        default="WORM/v7-grammar-0003-hypotheses-2026-07-12.json",
    )
    parser.add_argument(
        "--output", default="reports/v7/phase4/grammar0003_signal_manifest.json"
    )
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    preregistration = root / args.preregistration
    if _sha256(preregistration) != PREREGISTRATION_SHA256:
        raise RuntimeError("grammar 0003 WORM hash mismatch")
    bars = load_v7_market_bars(root)
    signals = generate_signal_population(
        bars, graveyard_path=root / "mission/state/graveyard.db"
    )
    payload = {
        "schema": "hydra_v7_grammar_0003_signal_manifest_v1",
        "preregistration_sha256": PREREGISTRATION_SHA256,
        "candidate_count": len(signals),
        "specifications": [row.to_dict() for row in candidate_specs()],
        "signal_counts": {
            candidate_id: len(rows) for candidate_id, rows in signals.items()
        },
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
    }
    payload["manifest_hash"] = _stable_hash(payload)
    destination = root / args.output
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, destination)
    print(
        json.dumps(
            {
                "candidate_count": payload["candidate_count"],
                "signal_count": sum(payload["signal_counts"].values()),
                "signal_counts": payload["signal_counts"],
                "manifest_hash": payload["manifest_hash"],
                "output": str(destination),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _stable_hash(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
