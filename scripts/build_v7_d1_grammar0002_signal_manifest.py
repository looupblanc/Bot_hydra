from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.v7_d1_microstructure_grammar import load_feature_store
from hydra.research.v7_d1_microstructure_grammar_0002 import (
    candidate_specs,
    generate_signal_population,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build the outcome-free V7 D1 grammar 0002 signal manifest."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--output",
        default="reports/v7/data/d1_microstructure_grammar0002_signal_manifest.json",
    )
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    minute, _event = load_feature_store(root)
    signals = generate_signal_population(minute, project_root=root)
    payload = {
        "schema": "hydra_v7_d1_microstructure_grammar0002_signal_manifest_v1",
        "grammar_id": "hydra_v7_d1_microstructure_grammar_0002",
        "candidate_specs": [row.to_dict() for row in candidate_specs(root)],
        "signals": {
            candidate_id: [row.to_dict() for row in rows]
            for candidate_id, rows in signals.items()
        },
        "candidate_count": len(signals),
        "signal_count": sum(len(rows) for rows in signals.values()),
        "contains_outcomes_or_pnl": False,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "outbound_order_count": 0,
    }
    payload["manifest_hash"] = _stable_hash(payload)
    output = root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, output)
    print(
        json.dumps(
            {
                "candidate_count": payload["candidate_count"],
                "signal_count": payload["signal_count"],
                "counts": {
                    candidate_id: len(rows)
                    for candidate_id, rows in signals.items()
                },
                "output": str(output),
                "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _stable_hash(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
