from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hydra.mission.experiment_queue import experiment_counts
from hydra.mission.mission_state import connect_state_readonly, mission_paths, state_snapshot
from hydra.mission.watchdog import heartbeat_status, scheduler_health


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show HYDRA autonomous mission status.")
    parser.add_argument("--state-dir", default="mission/state")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = mission_paths(args.state_dir)
    heartbeat = {}
    if paths.heartbeat_path.exists():
        heartbeat = json.loads(paths.heartbeat_path.read_text(encoding="utf-8"))
    snapshot = {}
    counts = {"TOTAL": 0, "QUEUED": 0, "RUNNING": 0, "COMPLETED": 0, "FAILED": 0, "BLOCKED": 0}
    if paths.db_path.exists():
        conn = connect_state_readonly(paths)
        try:
            snapshot = state_snapshot(conn)
            counts = experiment_counts(conn)
        finally:
            conn.close()
    hb_status = heartbeat_status(paths)
    scheduler = scheduler_health(hb_status, snapshot, counts)
    status = {
        "heartbeat_path": str(paths.heartbeat_path),
        "heartbeat": heartbeat,
        "state": snapshot,
        "experiments": counts,
        "scheduler": scheduler,
    }
    if args.json:
        print(json.dumps(status, indent=2, sort_keys=True, default=str))
    else:
        conversion = dict(snapshot.get("evidence_conversion_v3_latest_metrics") or {})
        account_v6 = dict(
            heartbeat.get("account_level_v6_latest_metrics")
            or snapshot.get("account_level_v6_latest_metrics")
            or {}
        )
        print(f"mission_id: {heartbeat.get('mission_id') or snapshot.get('mission_id')}")
        print(f"phase: {heartbeat.get('current_phase') or snapshot.get('current_phase')}")
        print(f"action: {heartbeat.get('current_action') or snapshot.get('current_action')}")
        print(f"cycle_count: {heartbeat.get('cycle_count') or snapshot.get('cycle_count')}")
        print(f"heartbeat_at_utc: {heartbeat.get('heartbeat_at_utc')}")
        print(f"pid: {heartbeat.get('pid')}")
        print(f"checkpoint: {heartbeat.get('latest_checkpoint') or snapshot.get('last_successful_checkpoint')}")
        print(f"q4_access_count: {heartbeat.get('q4_access_count')}")
        print(f"remaining_budget: {heartbeat.get('remaining_databento_budget_usd') or snapshot.get('remaining_databento_budget_usd')}")
        print(f"current_engine: {heartbeat.get('foundry_current_engine') or snapshot.get('foundry_current_engine')}")
        print(f"v5_grammar_status: {heartbeat.get('combine_first_v5_grammar_status') or snapshot.get('combine_first_v5_grammar_status')}")
        print(f"account_v6_phase: {heartbeat.get('account_level_v6_phase') or snapshot.get('account_level_v6_phase')}")
        print(f"account_v6_generation: {heartbeat.get('account_level_v6_current_generation', snapshot.get('account_level_v6_current_generation'))}")
        print(f"account_v6_completed_generations: {heartbeat.get('account_level_v6_completed_generations', snapshot.get('account_level_v6_completed_generations', 0))}")
        print(f"account_v6_queues: {heartbeat.get('account_level_v6_queues') or snapshot.get('account_level_v6_queues', {})}")
        print(f"account_v6_components: {account_v6.get('component_bank_size', 0)}")
        print(f"account_v6_behavioral_clusters: {account_v6.get('behavioral_clusters', 0)}")
        print(f"account_v6_individuals: {account_v6.get('individuals_evaluated', 0)}")
        print(f"account_v6_baskets: {account_v6.get('baskets_evaluated', 0)}")
        print(f"account_v6_controllers: {account_v6.get('controllers_evaluated', 0)}")
        print(f"account_v6_target_velocity_mutations: {account_v6.get('target_velocity_mutations', 0)}")
        print(f"account_v6_target_velocity_improved: {account_v6.get('target_velocity_improved_children', 0)}")
        print(f"account_v6_episodes: {account_v6.get('total_rolling_combine_episodes', 0)}")
        print(f"account_v6_individual_elites: {account_v6.get('individual_elite_count', 0)}")
        print(f"account_v6_basket_elites: {account_v6.get('basket_elite_count', 0)}")
        print(f"account_v6_controller_elites: {account_v6.get('controller_elite_count', 0)}")
        print(f"account_v6_xfa_elites: {account_v6.get('xfa_elite_count', 0)}")
        print(f"account_v6_report: {account_v6.get('report_path')}")
        print(f"prototypes_generated: {heartbeat.get('strategy_prototypes_generated', snapshot.get('strategy_prototypes_generated', 0))}")
        print(f"strategies_screened: {heartbeat.get('strategies_screened', snapshot.get('strategies_screened', 0))}")
        print(f"promising_candidates: {heartbeat.get('promising_candidates', snapshot.get('promising_candidates', 0))}")
        print(f"shadow_candidates: {heartbeat.get('shadow_candidates', snapshot.get('shadow_candidates', 0))}")
        print(f"paper_shadow_ready: {heartbeat.get('paper_shadow_ready_candidates', snapshot.get('paper_shadow_ready_candidates', 0))}")
        print(f"shadow_active: {heartbeat.get('shadow_active_candidates', snapshot.get('shadow_active_candidates', 0))}")
        print(f"mechanisms_represented: {heartbeat.get('mechanisms_represented', snapshot.get('mechanisms_represented', 0))}")
        print(f"market_ecologies_represented: {heartbeat.get('market_ecologies_represented', snapshot.get('market_ecologies_represented', 0))}")
        print(f"timeframes_represented: {heartbeat.get('timeframes_represented', snapshot.get('timeframes_represented', 0))}")
        print(f"strategies_killed: {heartbeat.get('strategies_killed', snapshot.get('strategies_killed', 0))}")
        print(f"lineages_frozen: {heartbeat.get('lineages_frozen', snapshot.get('lineages_frozen', 0))}")
        print(f"topstep_path_candidates: {heartbeat.get('topstep_path_candidates', snapshot.get('topstep_path_candidates', 0))}")
        print(f"q4_candidates: {heartbeat.get('q4_candidates', snapshot.get('q4_candidates', 0))}")
        print(f"pre_holdout_ready: {snapshot.get('pre_holdout_candidate_count', 0)}")
        print(f"evidence_debt_remaining: {conversion.get('evidence_debt_queue_count', 0)}")
        print(f"full_economic_replays: {conversion.get('full_economic_replay_count', 0)}")
        print(f"full_risk_replays: {conversion.get('full_risk_replay_count', 0)}")
        print(f"full_promotion_validations: {conversion.get('full_promotion_validation_count', 0)}")
        print(f"evidence_conversion_statuses: {conversion.get('status_counts', {})}")
        print(f"evidence_conversion_report: {conversion.get('report_path')}")
        print(f"model_quota_state: {heartbeat.get('model_quota_state') or snapshot.get('model_quota_state')}")
        print(f"last_meaningful_progress: {heartbeat.get('last_meaningful_progress_at_utc') or snapshot.get('last_meaningful_progress_at_utc')}")
        print(f"next_planned_action: {heartbeat.get('foundry_next_planned_action') or snapshot.get('foundry_next_planned_action')}")
        print(f"scheduler_classification: {scheduler.get('classification')}")
        print(f"experiments: {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
