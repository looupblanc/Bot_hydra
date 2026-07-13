from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v72_trade_arrival_renewal_funnel import (
    run_trade_arrival_renewal_funnel,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run frozen V7.2 G11 Stage 0-2.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--proof-registry", default="mission/state/proof_registry.json")
    parser.add_argument("--output-dir", default="reports/v7_2/discovery_0011")
    args = parser.parse_args()
    result = run_trade_arrival_renewal_funnel(
        project_root=args.project_root,
        proof_registry_path=args.proof_registry,
        output_dir=args.output_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
