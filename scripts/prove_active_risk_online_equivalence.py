#!/usr/bin/env python3
"""Run the bounded Operating Package V1 parity proof."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from hydra.shadow.active_risk_online_equivalence import (
    DEFAULT_AUDIT_PATH,
    ONLINE_OFFLINE_EQUIVALENCE_PROVEN,
    prove_active_risk_online_equivalence,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository-root", default=".")
    parser.add_argument(
        "--raw-rebuild-root",
        default="mission/state/operating_package_v1_parity/raw_feature_rebuild",
    )
    parser.add_argument("--output", default=str(DEFAULT_AUDIT_PATH))
    parser.add_argument("--skip-accounts", action="store_true")
    parser.add_argument("--processor-uses-proven-engine", action="store_true")
    parser.add_argument("--persistent-processor-version")
    arguments = parser.parse_args()
    receipt = prove_active_risk_online_equivalence(
        repository_root=Path(arguments.repository_root),
        raw_rebuild_root=arguments.raw_rebuild_root,
        output_path=arguments.output,
        reconcile_accounts=not arguments.skip_accounts,
        processor_uses_proven_engine=arguments.processor_uses_proven_engine,
        persistent_processor_version=arguments.persistent_processor_version,
    )
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0 if receipt["status"] == ONLINE_OFFLINE_EQUIVALENCE_PROVEN else 2


if __name__ == "__main__":
    raise SystemExit(main())
