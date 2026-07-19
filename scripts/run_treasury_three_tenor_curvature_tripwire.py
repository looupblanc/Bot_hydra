#!/usr/bin/env python3
"""Audit or explicitly execute the bounded Treasury-curvature tripwire."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


for _name in (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ[_name] = "1"

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.treasury_three_tenor_curvature_tripwire import (  # noqa: E402
    DEFAULT_CARD,
    RUN_AUTHORIZATION,
    TreasuryCurvatureError,
    audit_inputs,
    run_economic_tripwire,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--card", default=DEFAULT_CARD)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--audit-only",
        action="store_true",
        help="hash and metadata audit only; this is the safe default",
    )
    mode.add_argument(
        "--run-economic-replay",
        action="store_true",
        help="decode outcomes and run the bounded replay after root authorization",
    )
    parser.add_argument(
        "--authorization",
        help=f"economic mode requires the exact token {RUN_AUTHORIZATION}",
    )
    parser.add_argument(
        "--output",
        help="optional local result JSON; rejected in audit-only mode",
    )
    args = parser.parse_args()
    if not args.run_economic_replay:
        if args.output:
            parser.error("--output is forbidden in read-only audit mode")
        result = audit_inputs(args.root, card_path=args.card)
    else:
        if args.authorization != RUN_AUTHORIZATION:
            parser.error("economic replay requires the exact root authorization token")
        result = run_economic_tripwire(
            args.root,
            authorization=args.authorization,
            card_path=args.card,
        )
    rendered = json.dumps(
        result,
        indent=2,
        sort_keys=True,
        allow_nan=False,
        default=str,
    ) + "\n"
    if args.output:
        destination = Path(args.output)
        if not destination.is_absolute():
            destination = Path(args.root).resolve() / destination
        project = Path(args.root).resolve()
        try:
            destination.resolve().relative_to(project)
        except ValueError as exc:
            raise TreasuryCurvatureError("output path escapes repository") from exc
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, destination)
        print(
            json.dumps(
                {
                    "status": result["status"],
                    "result_hash": result["result_hash"],
                    "output": str(destination),
                },
                sort_keys=True,
            )
        )
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
