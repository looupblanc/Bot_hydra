#!/usr/bin/env python3
"""Audit or explicitly run the frozen one-candidate MGC power replay."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.mgc_rates_target_vol_power_replay import (
    DEFAULT_CARD,
    DEFAULT_OUTPUT,
    audit_replay_inputs,
    persist_replay_artifacts,
    run_power_replay,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--card", default=str(DEFAULT_CARD))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument(
        "--execute-economic-replay",
        action="store_true",
        help="Decode the governed pre-Q4 data and read economic outcomes.",
    )
    args = parser.parse_args(argv)
    if not args.execute_economic_replay:
        audit = audit_replay_inputs(args.root, card_path=args.card)
        print(json.dumps(audit, sort_keys=True))
        return 0
    result = run_power_replay(args.root, card_path=args.card)
    artifacts = persist_replay_artifacts(
        args.root, result, output_root=args.output
    )
    print(
        json.dumps(
            {
                "decision": result["decision"],
                "candidate_id": result["candidate_result"]["candidate_id"],
                "power_preflight": result["power_preflight"],
                "control_power": result["control_power"],
                "branch_gate": result["branch_gate"],
                "result_hash": result["result_hash"],
                "artifacts": artifacts,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

