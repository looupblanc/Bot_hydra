from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.budget import DatabentoBudgetConfig
from hydra.data.databento_loader import estimate_request, load_api_key
from hydra.data.databento_volume_front import acquire_volume_front, volume_front_request
from hydra.utils.config import project_path


TASK_SHA256 = "21e39bd03cacf5291cbd7191d23b85dcbab788dd714832a3b28c5d3c5bc660d5"
RECOVERY_TASK_SHA256 = "94b58ef3eed584171cf6601347a4b91f1b1a204b7f4feaa1434efe0cc77695d1"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Acquire governed GC/MGC volume-front development OHLCV."
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--max-cost-usd", type=float, default=5.0)
    parser.add_argument("--minimum-remaining-usd", type=float, default=30.0)
    parser.add_argument(
        "--base-roll-map",
        default=(
            "data/cache/contract_maps/"
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json"
        ),
    )
    parser.add_argument(
        "--report-dir", default="reports/data_repairs/gc_mgc_volume_front_v1"
    )
    args = parser.parse_args()
    task = project_path(
        "reports", "engineering", "hydra_gc_volume_front_data_repair_20260711.md"
    )
    if (
        not task.is_file()
        or hashlib.sha256(task.read_bytes()).hexdigest() != TASK_SHA256
    ):
        raise RuntimeError("Frozen volume-front engineering task is missing or changed.")
    recovery_task = project_path(
        "reports",
        "engineering",
        "hydra_gc_volume_front_definition_recovery_20260711.md",
    )
    if (
        not recovery_task.is_file()
        or hashlib.sha256(recovery_task.read_bytes()).hexdigest()
        != RECOVERY_TASK_SHA256
    ):
        raise RuntimeError("Frozen volume-front recovery task is missing or changed.")
    request = volume_front_request()
    if not args.execute:
        print(
            json.dumps(
                {
                    "execute": False,
                    "network_request_made": False,
                    "request": request.to_dict(),
                    "maximum_cost_usd": args.max_cost_usd,
                    "minimum_remaining_usd": args.minimum_remaining_usd,
                    "task_sha256": TASK_SHA256,
                    "recovery_task_sha256": RECOVERY_TASK_SHA256,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    key = load_api_key()
    if not key:
        raise RuntimeError("DATABENTO_API_KEY is unavailable.")
    estimate = estimate_request(request, key)
    result = acquire_volume_front(
        request,
        key=key,
        budget=DatabentoBudgetConfig(),
        base_roll_map_path=project_path(args.base_roll_map),
        output_report_dir=project_path(args.report_dir),
        estimate=estimate,
        maximum_cost_usd=args.max_cost_usd,
        minimum_remaining_usd=args.minimum_remaining_usd,
    )
    print(
        json.dumps(
            {
                "scientific_conclusion": result["scientific_conclusion"],
                "request_id": result["request_id"],
                "official_estimate": result["official_estimate"],
                "actual_spend_usd": result["actual_spend_usd"],
                "data_path": result["data_path"],
                "data_sha256": result["data_sha256"],
                "roll_map_path": result["roll_map_path"],
                "roll_map_hash": result["roll_map_hash"],
                "validation": result["validation"],
                "q4_access_count_delta": result["q4_access_count_delta"],
                "report_path": result["report_path"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
