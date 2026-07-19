#!/usr/bin/env python3
"""Audit or explicitly run the bounded cross-ecology analog router."""

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

from hydra.research.cross_ecology_session_path_analog_router import (  # noqa: E402
    DEFAULT_CARD,
    RUN_AUTHORIZATION,
    SessionPathAnalogError,
    audit_inputs,
    run_economic_tripwire,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--card", default=DEFAULT_CARD)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--audit-only", action="store_true")
    mode.add_argument("--run-economic-replay", action="store_true")
    parser.add_argument("--authorization")
    parser.add_argument(
        "--production-manifest",
        help="validated committed production manifest and multiplicity reservation",
    )
    parser.add_argument("--output")
    args = parser.parse_args()
    if not args.run_economic_replay:
        if args.output:
            parser.error("--output is forbidden in metadata-only audit mode")
        result = audit_inputs(args.root, card_path=args.card)
    else:
        if args.authorization != RUN_AUTHORIZATION:
            parser.error("economic replay requires the exact root authorization token")
        if not args.production_manifest:
            parser.error("economic replay requires --production-manifest")
        result = run_economic_tripwire(
            args.root,
            authorization=args.authorization,
            card_path=args.card,
            production_manifest_path=args.production_manifest,
        )
    rendered = json.dumps(result, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n"
    if args.output:
        project = Path(args.root).resolve()
        destination = Path(args.output)
        destination = destination.resolve() if destination.is_absolute() else (project / destination).resolve()
        try:
            destination.relative_to(project)
        except ValueError as exc:
            raise SessionPathAnalogError("output path escapes repository") from exc
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, destination)
        print(json.dumps({"status": result["status"], "result_hash": result["result_hash"], "output": str(destination)}, sort_keys=True))
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
