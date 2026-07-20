#!/usr/bin/env python3
"""Audit or explicitly run the bounded V73 ES recovery-speed tripwire."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


for _name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ[_name] = "1"

from hydra.research.v73_extreme_recovery_speed_tripwire import (  # noqa: E402
    DEFAULT_CARD,
    DEFAULT_OUTPUT,
    audit_only,
    persist_economic_result,
    run_economic_tripwire,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--decision-card", type=Path, default=DEFAULT_CARD)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--audit-only", action="store_true")
    mode.add_argument("--run-economic", action="store_true")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    if args.run_economic:
        result = run_economic_tripwire(
            args.root, decision_card_path=args.decision_card
        )
        artifact = persist_economic_result(
            args.root, result, output_root=args.output
        )
        payload = {"result": result, "artifact": artifact}
    else:
        # Safe default: metadata/hash audit only, with no parquet decoding and
        # no report or mission write.
        payload = audit_only(args.root, decision_card_path=args.decision_card)
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
