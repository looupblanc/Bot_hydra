#!/usr/bin/env python3
"""Run the frozen causal-salvage sprint from its immutable manifest."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.production.causal_salvage_runtime import (
    DEFAULT_MANIFEST,
    run_causal_salvage_sprint,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Execute the bounded causal-salvage sprint without mutation, XFA, "
            "forward activation, or data access; clean development promotion "
            "remains gated on the sealed causal EvidenceBundle."
        )
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="Frozen sprint manifest (repository-relative by default).",
    )
    parser.add_argument(
        "--repository-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="HYDRA repository root.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional repository-contained checkpoint directory override.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = run_causal_salvage_sprint(
        args.manifest,
        repository_root=args.repository_root,
        output_dir=args.output_dir,
    )
    print(json.dumps(result, sort_keys=True, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
