#!/usr/bin/env python3
"""Run the bounded six-book XFA post-payout frontier."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from hydra.propfirm.xfa_post_payout_frontier import run_six_book_frontier


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=Path(__file__).resolve().parents[1])
    arguments = parser.parse_args()
    root = Path(arguments.project_root).resolve()
    result = run_six_book_frontier(
        project_root=root,
        selection_path="reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02/frozen_book_selection_revision_02.json",
        decision_report_path="reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02/decision_report_revision_02.json",
        halving_root="reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02/successive_halving",
        stage_cache_root="data/cache/economic_production/hydra_active_risk_pool_target_velocity_0026",
        evidence_bundle_path="data/cache/evidence_bundles/hydra_active_risk_pool_target_velocity_0026.evidence-v1",
        feature_cache_root="data/cache/economic_evolution/features",
        runtime_summaries_path="data/cache/economic_production/hydra_active_risk_pool_target_velocity_0026/component_runtime_summaries.jsonl",
        source_tape_output_dir="data/cache/operating/hydra_operating_package_v1/xfa_source_tape",
        payout_event_tape_path="data/cache/operating/hydra_operating_package_v1/xfa_post_payout_events.jsonl.gz",
        output_path="reports/operating/hydra_operating_package_v1/xfa_post_payout_frontier.json",
    )
    print(
        json.dumps(
            {
                "result_hash": result["result_hash"],
                "transition_count": result["transition_count"],
                "frontier_evaluation_count": result["frontier_evaluation_count"],
                "payout_event_count": result["canonical_payout_event_tape"][
                    "event_count"
                ],
                "selected_profiles": {
                    row["policy_id"]: row["selected_profile"]["policy_id"]
                    for row in result["books"]
                },
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
