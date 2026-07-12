#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.combine_first_evolution_v5 import (
    run_combine_first_evolution_v5,
)


DEFAULT_TASK = Path(
    "reports/engineering/hydra_combine_first_evolution_v5_20260712.md"
)
DEFAULT_MAP = Path(
    "data/cache/contract_maps/"
    "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json"
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a bounded, no-order HYDRA Combine-First V5 tournament."
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--epoch-index", type=int, default=0)
    parser.add_argument("--proposals", type=int, default=10_000)
    parser.add_argument("--exact-limit", type=int, default=200)
    parser.add_argument("--mutation-limit", type=int, default=60)
    parser.add_argument("--episode-starts", type=int, default=24)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--engineering-task", type=Path, default=DEFAULT_TASK)
    parser.add_argument("--contract-map", type=Path, default=DEFAULT_MAP)
    parser.add_argument("--no-data-access-record", action="store_true")
    args = parser.parse_args()
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    result = run_combine_first_evolution_v5(
        args.output_dir,
        engineering_task_path=args.engineering_task,
        engineering_task_sha256=_sha256(args.engineering_task),
        contract_map_path=args.contract_map,
        contract_map_sha256=_sha256(args.contract_map),
        code_commit=commit,
        epoch_index=args.epoch_index,
        worker_count=args.workers,
        proposal_count=args.proposals,
        exact_limit=args.exact_limit,
        mutation_limit=args.mutation_limit,
        maximum_episode_starts=args.episode_starts,
        record_data_access=not args.no_data_access_record,
    )
    print(
        json.dumps(
            {
                "scientific_conclusion": result["scientific_conclusion"],
                "result_hash": result["result_hash"],
                "report_path": result["report_path"],
                "combine_elite_count": result["combine_elite_count"],
                "xfa_candidate_count": result["xfa_candidate_count"],
                "paper_shadow_ready": result["paper_shadow_ready"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
