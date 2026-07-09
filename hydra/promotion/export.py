from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hydra.utils.config import project_path


def export_research_config(candidate, promotion: dict[str, Any], tag: str) -> tuple[str | None, str | None]:
    if promotion.get("classification") != "TRADING_READY_CANDIDATE":
        return None, None
    folder = project_path("reports", "trading_ready_configs", tag)
    folder.mkdir(parents=True, exist_ok=True)
    strategy_path = folder / f"{candidate.candidate_id}_strategy.json"
    risk_path = folder / f"{candidate.candidate_id}_risk.json"
    strategy_path.write_text(
        json.dumps(
            {
                "candidate_id": candidate.candidate_id,
                "family": candidate.family,
                "symbol": candidate.symbol,
                "timeframe": candidate.timeframe,
                "parameters": candidate.parameters,
                "entry_logic": candidate.entry_logic,
                "exit_logic": candidate.exit_logic,
                "live_trading_allowed": False,
                "deployment_scope": "paper_shadow_controlled_prop_evaluation_research_only",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    risk_path.write_text(
        json.dumps(
            {
                "candidate_id": candidate.candidate_id,
                "risk_parameters": candidate.risk_parameters,
                "kill_switch_conditions": [
                    "mll_buffer_below_1000",
                    "daily_loss_exceeds_internal_stop",
                    "two_consecutive_rule_deviations",
                    "data_feed_gap_or_bad_timestamp",
                ],
                "do_not_trade_if": [
                    "live_trading_not_explicitly_enabled",
                    "current_session_not_allowed",
                    "daily_profit_lock_already_hit",
                    "internal_daily_stop_already_hit",
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return str(strategy_path), str(risk_path)
