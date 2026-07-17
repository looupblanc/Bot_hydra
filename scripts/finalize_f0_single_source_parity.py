#!/usr/bin/env python3
"""Atomically seal the bounded, non-authorizing F0 contamination receipt."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from hydra.shadow.f0_single_source_parity import (
    DEFAULT_AUDIT_PATH,
    DEFAULT_OPERATING_MANIFEST_PATH,
    DEFAULT_OUTPUT_PATH,
    write_f0_contamination_receipt,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository-root", default=".")
    parser.add_argument("--audit", default=str(DEFAULT_AUDIT_PATH))
    parser.add_argument(
        "--operating-manifest", default=str(DEFAULT_OPERATING_MANIFEST_PATH)
    )
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument(
        "--created-at",
        help="Optional ISO-8601 timestamp for deterministic controlled runs.",
    )
    arguments = parser.parse_args()
    created_at = (
        datetime.fromisoformat(arguments.created_at.replace("Z", "+00:00"))
        if arguments.created_at
        else None
    )
    receipt = write_f0_contamination_receipt(
        repository_root=Path(arguments.repository_root),
        audit_path=arguments.audit,
        operating_manifest_path=arguments.operating_manifest,
        output_path=arguments.output,
        created_at=created_at,
    )
    print(json.dumps(receipt, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
