#!/usr/bin/env python3
"""Audit, freeze, or execute the hash-bound autonomous graduation cohort."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

for variable in (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ[variable] = "1"

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.production.autonomous_graduation_cohort import (
    DEFAULT_MANIFEST,
    audit_autonomous_graduation_cohort,
    build_autonomous_graduation_preflight,
    execute_autonomous_graduation_cohort,
)


def _write(path: Path | None, value: dict[str, object]) -> None:
    rendered = json.dumps(value, sort_keys=True, indent=2) + "\n"
    if path is None:
        print(rendered, end="")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--mode", choices=("audit", "preflight", "execute"), required=True)
    parser.add_argument("--preflight-receipt", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--no-xfa", action="store_true")
    args = parser.parse_args()

    root = args.root.resolve()
    if args.mode == "audit":
        result = audit_autonomous_graduation_cohort(
            root, manifest_path=args.manifest
        )
    elif args.mode == "preflight":
        result = build_autonomous_graduation_preflight(
            root, manifest_path=args.manifest
        )
    else:
        if args.preflight_receipt is None:
            parser.error("--preflight-receipt is required in execute mode")
        preflight = json.loads(args.preflight_receipt.read_text(encoding="utf-8"))
        result = execute_autonomous_graduation_cohort(
            root,
            preflight,
            manifest_path=args.manifest,
            run_xfa=not args.no_xfa,
        )
    _write(args.output, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
