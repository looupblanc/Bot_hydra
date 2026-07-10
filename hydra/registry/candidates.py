from __future__ import annotations

import json
import sqlite3

from hydra.strategies.dsl import StrategyCandidate
from hydra.utils.time import utc_now_iso


def upsert_candidate(conn: sqlite3.Connection, candidate: StrategyCandidate, metrics: dict, prop: dict, status: str, rejection_reason: str | None, robustness: float, cluster: str | None) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO candidates (
            candidate_id, family, symbol, timeframe, parameters_json, risk_json,
            net_profit, max_drawdown, profit_factor, sharpe, trade_count, win_rate,
            mll_breached, mll_buffer, correlation_cluster, validation_status,
            rejection_reason, robustness_score, parent_candidate_id, mutation_type, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate.candidate_id, candidate.family, candidate.symbol, candidate.timeframe,
            json.dumps(candidate.parameters, sort_keys=True), json.dumps(candidate.risk_parameters, sort_keys=True),
            metrics.get("net_profit", 0.0), metrics.get("max_drawdown", 0.0), metrics.get("profit_factor", 0.0),
            metrics.get("sharpe", 0.0), int(metrics.get("trade_count", 0)), metrics.get("win_rate", 0.0),
            int(bool(prop.get("mll_breached", False))), prop.get("mll_buffer", 0.0), cluster,
            status, rejection_reason, robustness, candidate.parent_candidate_id, candidate.mutation_type, utc_now_iso(),
        ),
    )
    conn.commit()


def upsert_topstep_candidate(
    conn: sqlite3.Connection,
    candidate: StrategyCandidate,
    metrics: dict,
    status: str,
    rejection_reason: str | None,
    topstep: dict,
    robustness: float = 0.0,
    cluster: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO candidates (
            candidate_id, family, symbol, timeframe, parameters_json, risk_json,
            net_profit, max_drawdown, profit_factor, sharpe, trade_count, win_rate,
            mll_breached, mll_buffer, correlation_cluster, validation_status,
            rejection_reason, robustness_score,
            topstep_passed, topstep_score, combine_days_to_pass, combine_profit_target_hit,
            combine_mll_breached, combine_min_mll_buffer, combine_best_day_profit,
            combine_best_day_pct_of_total_profit, combine_consistency_ok, target_inflation_required,
            funded_sim_survived, payout_eligible, payout_days_to_eligibility, payout_cycles_survived,
            gross_payout_available, trader_net_payout, post_payout_mll_breach,
            internal_daily_stop_used, daily_profit_lock_used, worst_day_loss,
            max_consecutive_losing_days, winning_days_150_count, topstep_split_scores_json,
            parent_candidate_id, mutation_type, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate.candidate_id,
            candidate.family,
            candidate.symbol,
            candidate.timeframe,
            json.dumps(candidate.parameters, sort_keys=True),
            json.dumps(candidate.risk_parameters, sort_keys=True),
            metrics.get("net_profit", topstep.get("adjusted_net_profit", 0.0)),
            metrics.get("max_drawdown", 0.0),
            metrics.get("profit_factor", 0.0),
            metrics.get("sharpe", 0.0),
            int(metrics.get("trade_count", topstep.get("trade_count", 0))),
            metrics.get("win_rate", 0.0),
            int(bool(topstep.get("combine_mll_breached", False))),
            topstep.get("combine_min_mll_buffer", 0.0),
            cluster,
            status,
            rejection_reason,
            robustness,
            int(bool(topstep.get("topstep_passed", False))),
            topstep.get("topstep_score", 0.0),
            topstep.get("combine_days_to_pass"),
            int(bool(topstep.get("combine_profit_target_hit", False))),
            int(bool(topstep.get("combine_mll_breached", False))),
            topstep.get("combine_min_mll_buffer", 0.0),
            topstep.get("combine_best_day_profit", 0.0),
            topstep.get("combine_best_day_pct_of_total_profit", 0.0),
            int(bool(topstep.get("combine_consistency_ok", False))),
            int(bool(topstep.get("target_inflation_required", False))),
            int(bool(topstep.get("funded_sim_survived", False))),
            int(bool(topstep.get("payout_eligible", False))),
            topstep.get("payout_days_to_eligibility"),
            int(topstep.get("payout_cycles_survived", 0)),
            topstep.get("gross_payout_available", 0.0),
            topstep.get("trader_net_payout", 0.0),
            int(bool(topstep.get("post_payout_mll_breach", False))),
            topstep.get("internal_daily_stop_used", 0.0),
            topstep.get("daily_profit_lock_used", 0.0),
            topstep.get("worst_day_loss", 0.0),
            int(topstep.get("max_consecutive_losing_days", 0)),
            int(topstep.get("winning_days_150_count", 0)),
            json.dumps(topstep.get("split_scores", {}), sort_keys=True),
            candidate.parent_candidate_id,
            candidate.mutation_type,
            utc_now_iso(),
        ),
    )
    conn.commit()


def load_candidates(conn: sqlite3.Connection, status: str | None = None) -> list[sqlite3.Row]:
    if status:
        return list(conn.execute("SELECT * FROM candidates WHERE validation_status = ? ORDER BY net_profit DESC", (status,)))
    return list(conn.execute("SELECT * FROM candidates ORDER BY created_at DESC"))


def update_promotion_metadata(conn: sqlite3.Connection, candidate_id: str, promotion: dict) -> None:
    conn.execute(
        """
        UPDATE candidates SET
            validation_status=?,
            rejection_reason=?,
            strategy_fingerprint=?,
            parameter_zone=?,
            research_lane=?,
            promotion_stage=?,
            promotion_classification=?,
            promotion_score=?,
            economic_score=?,
            execution_readiness_score=?,
            gate_history_json=?,
            recommended_action=?,
            config_export_path=?,
            risk_export_path=?,
            branch_action=?,
            lineage_json=?,
            validation_provenance_json=?,
            validation_version=?,
            input_fingerprint=?,
            validation_computed_at=?,
            computation_mode=?,
            evidence_strength=?
        WHERE candidate_id=?
        """,
        (
            promotion.get("status"),
            promotion.get("rejection_reason"),
            promotion.get("strategy_fingerprint", ""),
            promotion.get("parameter_zone", ""),
            promotion.get("research_lane", ""),
            promotion.get("promotion_stage", "GENERATED"),
            promotion.get("classification", ""),
            promotion.get("promotion_score", 0.0),
            promotion.get("economic_score", 0.0),
            promotion.get("execution_readiness_score", 0.0),
            json.dumps(promotion.get("gate_history", []), sort_keys=True),
            promotion.get("recommended_action", ""),
            promotion.get("config_export_path"),
            promotion.get("risk_export_path"),
            promotion.get("branch_action", ""),
            json.dumps(promotion.get("lineage", {}), sort_keys=True),
            json.dumps(promotion.get("validation_provenance", {}), sort_keys=True),
            promotion.get("validation_version", ""),
            promotion.get("input_fingerprint", ""),
            promotion.get("validation_computed_at", ""),
            promotion.get("computation_mode", ""),
            promotion.get("evidence_strength", ""),
            candidate_id,
        ),
    )
    conn.commit()


def load_strategy_fingerprints(conn: sqlite3.Connection) -> set[str]:
    return {row["strategy_fingerprint"] for row in conn.execute("SELECT strategy_fingerprint FROM candidates WHERE strategy_fingerprint != ''")}
