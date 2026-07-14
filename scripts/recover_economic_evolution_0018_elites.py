from __future__ import annotations

import argparse
import json

from hydra.research.economic_evolution_0018_elite_recovery import (
    run_0018_elite_recovery,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--preregistration", required=True)
    parser.add_argument("--contract-map", required=True)
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--worm-manifest", required=True)
    args = parser.parse_args()
    result = run_0018_elite_recovery(
        args.source_dir,
        args.output_dir,
        preregistration_path=args.preregistration,
        contract_map_path=args.contract_map,
        cache_root=args.cache_root,
        worm_manifest_path=args.worm_manifest,
    )
    print(json.dumps({
        "manifest_hash": result["manifest_hash"],
        "selected_policy_count": result["selected_policy_count"],
        "passing_policy_ids": result["passing_policy_ids"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
