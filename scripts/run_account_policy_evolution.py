from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.account_level_evolution_v6 import (
    run_account_level_evolution_v6,
)


DEFAULT_TASK = Path(
    "reports/engineering/hydra_account_level_evolution_v6_20260712.md"
)
DEFAULT_MAP = Path(
    "/root/hydra-bot/data/cache/contract_maps/"
    "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a bounded development-only HYDRA V6 account-policy generation."
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--generation-index", type=int, default=0)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--grammar-count", type=int, default=480)
    parser.add_argument("--grammar-exact-limit", type=int, default=72)
    parser.add_argument("--basket-count", type=int, default=600)
    parser.add_argument("--controller-basket-limit", type=int, default=40)
    parser.add_argument("--target-velocity-mutation-limit", type=int, default=24)
    parser.add_argument("--screening-starts", type=int, default=24)
    parser.add_argument("--promotion-starts", type=int, default=48)
    parser.add_argument(
        "--source-report-root",
        default="/root/hydra-bot/reports/mission_experiments",
    )
    parser.add_argument("--engineering-task", default=str(DEFAULT_TASK))
    parser.add_argument("--contract-map", default=str(DEFAULT_MAP))
    parser.add_argument("--skip-data-access-record", action="store_true")
    parser.add_argument("--allow-uncommitted-code", action="store_true")
    return parser.parse_args()


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    args = parse_args()
    task = Path(args.engineering_task)
    roll_map = Path(args.contract_map)
    commit = (
        "unknown"
        if args.allow_uncommitted_code
        else subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    )
    result = run_account_level_evolution_v6(
        args.output_dir,
        engineering_task_path=task,
        engineering_task_sha256=_sha(task),
        contract_map_path=roll_map,
        contract_map_sha256=_sha(roll_map),
        code_commit=commit,
        source_report_root=args.source_report_root,
        generation_index=args.generation_index,
        worker_count=args.workers,
        grammar_count=args.grammar_count,
        grammar_exact_limit=args.grammar_exact_limit,
        basket_count=args.basket_count,
        controller_basket_limit=args.controller_basket_limit,
        target_velocity_mutation_limit=args.target_velocity_mutation_limit,
        screening_starts=args.screening_starts,
        promotion_starts=args.promotion_starts,
        record_data_access=not args.skip_data_access_record,
    )
    print(f"scientific_conclusion={result['scientific_conclusion']}")
    print(f"result_hash={result['result_hash']}")
    print(f"report_path={result['report_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
