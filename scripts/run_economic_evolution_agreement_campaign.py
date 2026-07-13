#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.economic_evolution_agreement_campaign import (
    run_directional_agreement_campaign,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--preregistration", required=True)
    parser.add_argument("--contract-map", required=True)
    parser.add_argument("--cache-root", required=True)
    args = parser.parse_args()
    result = run_directional_agreement_campaign(
        args.output_dir,
        preregistration_path=args.preregistration,
        contract_map_path=args.contract_map,
        cache_root=args.cache_root,
    )
    print(
        json.dumps(
            {
                "campaign_id": result["campaign_id"],
                "scientific_status": result["scientific_status"],
                "account_research_candidate_count": result[
                    "account_research_candidate_count"
                ],
                "combine_path_diagnostic_count": result[
                    "combine_path_diagnostic_count"
                ],
                "result_sha256": result["result_sha256"],
                "orders": result["governance"]["orders"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
