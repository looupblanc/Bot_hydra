#!/usr/bin/env python3
"""Run and seal the bounded cross-index dispersion/residual tripwire."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from hydra.research.intraday_cross_index_dispersion_residual import (
    DEFAULT_MANIFEST,
    DEFAULT_OUTPUT,
    persist_tripwire_result,
    run_intraday_cross_index_dispersion_residual,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--cell-id",
        action="append",
        dest="cell_ids",
        help="Optional non-decisional subset smoke; repeat for multiple cells.",
    )
    args = parser.parse_args()
    result = run_intraday_cross_index_dispersion_residual(
        args.root,
        manifest_path=args.manifest,
        cell_ids=args.cell_ids,
    )
    receipt = persist_tripwire_result(args.root, result, output_root=args.output)
    print(
        json.dumps(
            {
                "status": result["status"],
                "gate": result["gate"],
                "counts": result["counts"],
                "receipt": receipt,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
