from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any, Mapping


def replay_evidence_rows(
    replay: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    episodes: list[dict[str, Any]] = []
    paths: list[dict[str, Any]] = []
    failure = _failure_vector(replay)
    for scenario_key, scenario in (
        ("normal_episodes", "NORMAL"),
        ("stressed_episodes", "STRESSED_1_5X"),
    ):
        for raw in replay[scenario_key]:
            episode, daily = _convert_episode(raw, manifest, scenario, failure)
            episodes.append(episode)
            paths.extend(daily)
    return episodes, paths


def _convert_episode(
    raw: Mapping[str, Any],
    manifest: Mapping[str, Any],
    scenario: str,
    failure: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    campaign_id = str(raw["campaign_id"])
    policy_id = str(raw["policy_id"])
    start_day = int(raw["start_day"])
    horizon_days = int(raw["horizon_trading_days"])
    horizon = str(raw.get("horizon_label") or f"{horizon_days}_TRADING_DAYS")
    episode_id = f"{policy_id}:{start_day}"
    terminal = str(raw["terminal_classification"])
    consistency_ok = bool(raw["consistency_ok"])
    episode = {
        "campaign_id": campaign_id,
        "policy_id": policy_id,
        "episode_id": episode_id,
        "episode_start": _day_iso(start_day),
        "horizon": horizon,
        "temporal_block": _temporal_block(start_day, manifest),
        "duration_trading_days": int(raw["eligible_days"]),
        "target_reached": terminal == "TARGET_REACHED",
        "mll_breached": terminal == "MLL_BREACHED",
        "censored_state": terminal
        in {"DATA_CENSORED", "OPERATIONAL_HORIZON_NOT_REACHED"},
        "cost_scenario": scenario,
        "costs": float(raw["total_cost"]),
        "net_pnl": float(raw["net_pnl"]),
        "target_progress": float(raw["target_progress"]),
        "minimum_mll_buffer": float(raw["minimum_mll_buffer"]),
        "consistency_ok": consistency_ok,
        "days_to_target": (
            None if raw.get("days_to_target") is None else float(raw["days_to_target"])
        ),
        "failure_vector": [failure],
        "terminal_state": terminal,
        "component_contribution": {
            str(key): float(value)
            for key, value in (raw.get("component_contribution") or {}).items()
        },
    }
    raw_path = list(raw.get("daily_path") or ())
    output: list[dict[str, Any]] = []
    for index, day in enumerate(raw_path):
        realized = float(day["realized_pnl"])
        buffer = float(day["closing_mll_buffer"])
        consistency = bool(day["consistency_ok"])
        output.append(
            {
                "campaign_id": campaign_id,
                "policy_id": policy_id,
                "episode_id": episode_id,
                "trading_day": _day_date(int(day["session_day"])),
                "cost_scenario": scenario,
                "horizon": horizon,
                "realized_pnl": realized,
                "unrealized_pnl": float(day["unrealized_pnl"]),
                "daily_pnl": float(day["day_pnl"]),
                "equity": float(day["balance"]),
                "mll": float(day["mll_floor"]),
                "mll_buffer": buffer,
                "minimum_mll_buffer": float(day["minimum_mll_buffer"]),
                "consistency": 1.0 if consistency else 0.0,
                "consistency_ok": consistency,
                "target_progress": float(day["target_progress"]),
                "costs": float(day["costs"]),
                "conflicts": list(day["conflicts"]),
                "exposure": dict(day["exposure"]),
                "component_attribution": dict(day["component_attribution"]),
                "risk_allocation": list(day["routing_decisions"]),
                "closing_mll_buffer": float(day["closing_mll_buffer"]),
                "cumulative_costs": float(day["cumulative_costs"]),
            }
        )
    if abs(sum(float(row["costs"]) for row in output) - float(raw["total_cost"])) > 1e-8:
        raise ValueError("daily cost path does not reconcile")
    return episode, output


def _failure_vector(replay: Mapping[str, Any]) -> str:
    stress = replay["stressed_1_5x"]
    if float(stress["mll_breach_rate"]) > 0.10:
        return "MLL_BREACH"
    if float(stress["median_episode_net_pnl"]) <= 0.0:
        return "COST_FRAGILITY"
    if float(stress["consistency_pass_rate"]) < 0.75:
        return "CONSISTENCY_FAILURE"
    if float(stress["target_progress_median"]) < 0.35:
        return "TARGET_TOO_SLOW"
    return "NO_INCREMENTAL_VALUE"


def _temporal_block(epoch_day: int, manifest: Mapping[str, Any]) -> str:
    value = _day_date(epoch_day)
    for block in manifest["temporal_blocks"]["blocks"]:
        if str(block["start"]) <= value <= str(block["end"]):
            return str(block["block_id"])
    return "OUTSIDE_FROZEN_TEMPORAL_BLOCK"


def _day_date(epoch_day: int) -> str:
    return (date(1970, 1, 1) + timedelta(days=epoch_day)).isoformat()


def _day_iso(epoch_day: int) -> str:
    day = date(1970, 1, 1) + timedelta(days=epoch_day)
    return datetime(day.year, day.month, day.day, tzinfo=UTC).isoformat().replace(
        "+00:00", "Z"
    )


__all__ = ["replay_evidence_rows"]
