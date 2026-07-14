from __future__ import annotations

import argparse
import json
from pathlib import Path

from hydra.research.economic_evolution_coverage_union_campaign import (
    run_coverage_union_campaign,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--preregistration", required=True)
    parser.add_argument("--contract-map", required=True)
    parser.add_argument("--cache-root", required=True)
    args = parser.parse_args()
    result = run_coverage_union_campaign(
        Path(args.output_dir),
        preregistration_path=Path(args.preregistration),
        contract_map_path=Path(args.contract_map),
        cache_root=Path(args.cache_root),
    )
    print(
        json.dumps(
            {
                "campaign_id": result["campaign_id"],
                "scientific_status": result["scientific_status"],
                "result_sha256": result["result_sha256"],
                "broker_connections": 0,
                "orders": 0,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
