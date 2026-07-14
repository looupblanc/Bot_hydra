from __future__ import annotations

import argparse
import json

from hydra.research.economic_evolution_opportunity_density_campaign import (
    run_opportunity_density_campaign,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the frozen HYDRA opportunity-density campaign."
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--preregistration", required=True)
    parser.add_argument("--contract-map", required=True)
    parser.add_argument("--cache-root", required=True)
    args = parser.parse_args()
    result = run_opportunity_density_campaign(
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
                "policy_pair_evaluated_count": result[
                    "policy_pair_evaluated_count"
                ],
                "combine_path_diagnostic_count": result[
                    "combine_path_diagnostic_count"
                ],
                "orders": result["governance"]["orders"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
