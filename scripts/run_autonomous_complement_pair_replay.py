#!/usr/bin/env python3
"""Freeze or execute the bounded complementary-pair replay."""

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

from hydra.production.autonomous_complement_pair_replay import (
    DEFAULT_MANIFEST,
    build_preflight,
    execute,
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
    parser.add_argument("--mode", choices=("preflight", "execute"), required=True)
    parser.add_argument("--preflight-receipt", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.mode == "preflight":
        result = build_preflight(args.root, manifest_path=args.manifest)
    else:
        if args.preflight_receipt is None:
            parser.error("--preflight-receipt is required in execute mode")
        result = execute(
            args.root,
            json.loads(args.preflight_receipt.read_text(encoding="utf-8")),
            manifest_path=args.manifest,
        )
    _write(args.output, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
