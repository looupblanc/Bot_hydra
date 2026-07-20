#!/usr/bin/env python3
from __future__ import annotations

"""Estimate or explicitly acquire the frozen options-IV teacher bundle."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.databento_loader import _import_databento, load_api_key
from hydra.data.options_iv_teacher_acquisition import (
    MANIFEST_PATH,
    OptionsTeacherAcquisitionError,
    default_project_root,
    estimate_or_acquire,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Officially re-estimate the fixed pre-Q4 ES.OPT/NQ.OPT "
            "statistics+definition teacher bundle; download only with --execute."
        )
    )
    parser.add_argument("--manifest", default=MANIFEST_PATH)
    parser.add_argument("--root", default=str(default_project_root()))
    parser.add_argument(
        "--execute",
        action="store_true",
        help="purchase and seal the bundle after all live guards pass",
    )
    args = parser.parse_args()

    key = load_api_key()
    if not key:
        raise OptionsTeacherAcquisitionError(
            "DATABENTO_API_KEY is required for the official cost recheck"
        )
    client = _import_databento().Historical(key)
    result = estimate_or_acquire(
        root=args.root,
        client=client,
        manifest_path=args.manifest,
        execute=bool(args.execute),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
