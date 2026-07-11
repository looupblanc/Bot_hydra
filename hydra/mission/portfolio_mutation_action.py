from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.portfolio.portfolio_role_search import search_portfolio_roles
from hydra.portfolio.shadow_shared_account import normalize_candidate_trades
from hydra.shadow.contract_resolver import discover_roll_maps, resolve_current_contracts
from hydra.shadow.feed_health import assess_feed_health
from hydra.shadow.forward_bar_store import CmeSessionCalendar, ForwardBarStore


class PortfolioMutationActionError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or _sha256(path) != expected:
        raise PortfolioMutationActionError(f"Frozen {label} is missing or changed: {path}")


def _write_json(path: Path, value: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)
    return path


def _load_shadow_ledgers(sources: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
    if len(sources) != 4 or len({str(row.get("candidate_id")) for row in sources}) != 4:
        raise PortfolioMutationActionError("Exactly four immutable active-shadow sources are required")
    ledgers: dict[str, pd.DataFrame] = {}
    for source in sorted(sources, key=lambda row: str(row["candidate_id"])):
        candidate_id = str(source["candidate_id"])
        result_path = Path(str(source["result_path"]))
        ledger_path = Path(str(source["ledger_path"]))
        _verify(result_path, str(source["result_sha256"]), f"{candidate_id} result")
        _verify(ledger_path, str(source["ledger_sha256"]), f"{candidate_id} ledger")
        result = json.loads(result_path.read_text(encoding="utf-8"))
        if str(result.get("result_hash") or "") != str(source.get("result_hash") or ""):
            raise PortfolioMutationActionError(f"Semantic result hash drift: {candidate_id}")
        matches = [
            row
            for row in result.get("candidates") or []
            if str(row.get("candidate_id") or "") == candidate_id
        ]
        if len(matches) != 1 or str(matches[0].get("status")) != "SHADOW_RESEARCH_CANDIDATE":
            raise PortfolioMutationActionError(f"Source is not one immutable shadow candidate: {candidate_id}")
        raw_rows = [
            json.loads(line)
            for line in ledger_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        frame = normalize_candidate_trades(candidate_id, raw_rows)
        expected = dict(source.get("expected_2024") or {})
        if len(frame) != int(expected.get("events", -1)) or not np.isclose(
            float(frame["net_pnl"].sum()), float(expected.get("net_pnl", np.nan)), atol=1e-8
        ):
            raise PortfolioMutationActionError(f"Frozen normalized totals changed: {candidate_id}")
        # The source strategies already passed candidate-level no-lookahead review.
        # For account replay, use entry as the conservative latest availability;
        # this bridge cannot create an earlier fill or a new strategy pass.
        frame = frame.copy()
        frame["strategy_id"] = candidate_id
        frame["contracts"] = 1
        frame["availability_timestamp"] = frame["entry_timestamp"]
        frame["source_bar_close"] = frame["entry_timestamp"]
        frame["provenance_bridge"] = "frozen_candidate_integrity_at_or_before_entry"
        ledgers[candidate_id] = frame
    return ledgers


def _daily_policy_table(base: dict[str, pd.DataFrame]) -> pd.DataFrame:
    events = pd.concat(base.values(), ignore_index=True).sort_values("entry_timestamp")
    sessions = (
        events.groupby("event_session_id", sort=True)
        .agg(
            first_entry=("entry_timestamp", "min"),
            last_exit=("exit_timestamp", "max"),
            shared_net=("net_pnl", "sum"),
            opportunities=("trade_id", "size"),
        )
        .reset_index()
    )
    sessions["prior_source_end"] = sessions["last_exit"].shift(1)
    sessions["prior_3_net"] = sessions["shared_net"].shift(1).rolling(3, min_periods=2).sum()
    equity = sessions["shared_net"].cumsum()
    sessions["prior_drawdown"] = equity.shift(1) - equity.cummax().shift(1).clip(lower=0.0)
    sessions["prior_5_loss_rate"] = (
        sessions["shared_net"].lt(0).astype(float).shift(1).rolling(5, min_periods=3).mean()
    )
    return sessions


def _policy_decisions(
    sessions: pd.DataFrame,
    *,
    policy_id: str,
    metric: str,
    quantile: float,
    deactivate_below: bool,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    training_count = min(30, max(6, len(sessions) // 3))
    training = sessions[metric].iloc[:training_count].dropna()
    threshold = float(training.quantile(quantile)) if len(training) else 0.0
    available = sessions[metric].notna()
    if deactivate_below:
        deactivate = sessions[metric].le(threshold) & available
    else:
        deactivate = sessions[metric].ge(threshold) & available
    deactivate.iloc[:training_count] = False
    rows: list[dict[str, Any]] = []
    for index, row in sessions.iterrows():
        decision = pd.Timestamp(row["first_entry"]) - pd.Timedelta(microseconds=1)
        source_end = row["prior_source_end"]
        if pd.isna(source_end):
            source_end = decision - pd.Timedelta(seconds=1)
        source_end = min(pd.Timestamp(source_end), decision)
        opportunity_bin = "multi" if int(row["opportunities"]) > 1 else "single"
        rows.append(
            {
                "decision_id": f"{policy_id}:{row['event_session_id']}",
                "event_session_id": str(row["event_session_id"]),
                "decision_timestamp": decision,
                "available_timestamp": source_end,
                "source_window_end": source_end,
                "deactivate": bool(deactivate.iloc[index]),
                "match_group": f"weekday_{decision.weekday()}:{opportunity_bin}",
                "policy_version": f"{policy_id}:{metric}:q{quantile}:{threshold:.12g}",
            }
        )
    return pd.DataFrame(rows), {
        "policy_id": policy_id,
        "metric": metric,
        "training_count": training_count,
        "quantile": quantile,
        "frozen_threshold": threshold,
        "deactivate_below": deactivate_below,
        "current_session_outcome_used": False,
        "decision_shift_periods": 1,
    }


def run_portfolio_role_research(
    output_dir: Path,
    *,
    engineering_task_path: Path,
    engineering_task_sha256: str,
    sources: list[dict[str, Any]],
    code_commit: str,
    defensive_control_count: int = 4096,
    inclusion_control_count: int = 255,
) -> dict[str, Any]:
    _verify(engineering_task_path, engineering_task_sha256, "engineering task")
    base = _load_shadow_ledgers(sources)
    sessions = _daily_policy_table(base)
    policy_definitions = (
        ("shared_intraday_loss_circuit_v1", "prior_3_net", 0.25, True),
        ("shared_mll_buffer_throttle_v1", "prior_drawdown", 0.25, True),
        ("shared_loss_cluster_deactivation_v1", "prior_5_loss_rate", 0.75, False),
    )
    policies: dict[str, pd.DataFrame] = {}
    policy_audit: list[dict[str, Any]] = []
    placeholder = next(iter(base.values())).iloc[:1].copy()
    defensive_ledgers: dict[str, pd.DataFrame] = {}
    defensive_specs: dict[str, dict[str, Any]] = {}
    for policy_id, metric, quantile, below in policy_definitions:
        decisions, audit = _policy_decisions(
            sessions,
            policy_id=policy_id,
            metric=metric,
            quantile=quantile,
            deactivate_below=below,
        )
        policies[policy_id] = decisions
        policy_audit.append(audit)
        defensive_ledgers[policy_id] = placeholder
        defensive_specs[policy_id] = {
            "candidate_id": policy_id,
            "portfolio_role": "defensive",
            "target_pool": "DEFENSIVE_ACCOUNT_POOL",
            "mechanism_family": "past_only_shared_account_deactivation",
        }
    defensive = search_portfolio_roles(
        base,
        defensive_ledgers,
        defensive_specs,
        defensive_policies=policies,
        control_count=defensive_control_count,
        seed=7112026,
    )

    by_id = {str(source["candidate_id"]): source for source in sources}
    ym_id = "strategy_open_gap_continuation_YM_v1"
    daily_id = "strategy_daily_cross_CL_to_YM_source_prior_trend_continuation_q80_h120_v1"
    inclusion_rows: list[dict[str, Any]] = []
    for candidate_id, pool in (
        (ym_id, "COMBINE_PASSER_POOL"),
        (daily_id, "XFA_PAYOUT_POOL"),
    ):
        comparison_base = {key: value for key, value in base.items() if key != candidate_id}
        search = search_portfolio_roles(
            comparison_base,
            {candidate_id: base[candidate_id]},
            {
                candidate_id: {
                    "candidate_id": candidate_id,
                    "portfolio_role": "alpha",
                    "target_pool": pool,
                    "mechanism_family": (
                        ((json.loads(Path(str(by_id[candidate_id]["result_path"])).read_text(encoding="utf-8")).get("candidates") or [{}])[0]).get("mechanism_family")
                    ),
                }
            },
            control_count=inclusion_control_count,
            seed=7112026,
        )
        inclusion_rows.extend(search.to_dict()["candidates"])

    candidates = defensive.to_dict()["candidates"] + inclusion_rows
    status_counts: dict[str, int] = {}
    pool_counts: dict[str, int] = {}
    for row in candidates:
        status_counts[str(row["research_status"])] = status_counts.get(str(row["research_status"]), 0) + 1
        pool_counts[str(row["target_pool"])] = pool_counts.get(str(row["target_pool"]), 0) + 1
    research_candidates = sum("RESEARCH_CANDIDATE" in str(row["research_status"]) for row in candidates)
    result: dict[str, Any] = {
        "schema": "hydra_portfolio_role_research_result_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "code_commit": code_commit,
        "source_candidate_count": len(base),
        "candidate_count": len(candidates),
        "structural_prototypes": len(candidates),
        "portfolio_role_candidates_generated": len(candidates),
        "defensive_role_candidates_generated": len(policy_definitions),
        "combine_pool_candidates_generated": 1,
        "xfa_pool_candidates_generated": 1,
        "research_candidate_count": research_candidates,
        "status_counts": dict(sorted(status_counts.items())),
        "pool_counts": dict(sorted(pool_counts.items())),
        "policy_audit": policy_audit,
        "candidates": candidates,
        "pareto_candidate_ids_by_pool": defensive.to_dict()["pareto_candidate_ids_by_pool"],
        "matched_deactivation_controls_per_policy": defensive_control_count,
        "matched_inclusion_controls_per_candidate": inclusion_control_count,
        "candidate_level_evidence_only": True,
        "promising_candidates": 0,
        "shadow_candidates": 0,
        "shadow_research_active": 0,
        "paper_shadow_ready": 0,
        "q4_access_count": 0,
        "network_requests": 0,
        "incremental_databento_spend_usd": 0.0,
        "broker_connections": 0,
        "outbound_orders": 0,
        "scientific_conclusion": (
            "PORTFOLIO_ROLE_RESEARCH_CANDIDATES_FOUND"
            if research_candidates
            else "PORTFOLIO_ROLE_EVIDENCE_INSUFFICIENT"
        ),
        "interpretation_boundary": (
            "Combine, XFA and defensive utilities are separate research objectives. "
            "No result is inherited, activated, Paper-ready or funded evidence."
        ),
    }
    result["result_hash"] = _stable_hash(result)
    output_dir.mkdir(parents=True, exist_ok=True)
    result_path = _write_json(output_dir / "portfolio_role_research_result.json", result)
    result["artifacts"] = {
        "result_json_path": str(result_path),
        "result_json_sha256": _sha256(result_path),
    }
    return result


def run_forward_feed_audit(
    output_dir: Path,
    *,
    engineering_task_path: Path,
    engineering_task_sha256: str,
    required_roots: list[str],
    contract_map_dir: Path,
    code_commit: str,
    as_of_utc: str | None = None,
) -> dict[str, Any]:
    _verify(engineering_task_path, engineering_task_sha256, "forward-feed engineering task")
    now = (
        datetime.fromisoformat(as_of_utc.replace("Z", "+00:00"))
        if as_of_utc
        else datetime.now(timezone.utc)
    )
    if now.tzinfo is None:
        raise PortfolioMutationActionError("Forward-feed audit timestamp must be timezone aware")
    now = now.astimezone(timezone.utc)
    maps = discover_roll_maps(contract_map_dir)
    resolution = resolve_current_contracts(maps, required_roots, as_of=now)
    store = ForwardBarStore(output_dir / "offline_forward_store_audit.db", calendar=CmeSessionCalendar.weekly_only())
    health = assess_feed_health(
        resolution,
        store,
        now=now,
        max_age_seconds=75,
        source_authorization_proven=False,
    )
    conclusion = (
        "FORWARD_DATA_SOURCE_REQUIRED"
        if health.status == "SOURCE_REQUIRED"
        else f"FORWARD_FEED_{health.status}"
    )
    result: dict[str, Any] = {
        "schema": "hydra_forward_feed_audit_result_v1",
        "generated_at_utc": now.isoformat(),
        "code_commit": code_commit,
        "scientific_conclusion": conclusion,
        "status": health.status,
        "mission_blocker": health.mission_blocker,
        "required_roots": sorted(set(required_roots)),
        "roll_maps_inspected": len(maps),
        "feed_health": health.to_dict(),
        "source_authorization_proven": False,
        "current_holiday_calendar_proven": False,
        "candidate_heartbeats_published": 0,
        "shadow_policy": "FAIL_CLOSED_NO_SIGNAL_NO_FILL",
        "next_action": resolution.next_action,
        "q4_access_count": 0,
        "network_requests": 0,
        "incremental_databento_spend_usd": 0.0,
        "broker_connections": 0,
        "outbound_orders": 0,
        "paid_data_allowed": False,
    }
    result["result_hash"] = _stable_hash(result)
    output_dir.mkdir(parents=True, exist_ok=True)
    result_path = _write_json(output_dir / "forward_feed_audit_result.json", result)
    result["artifacts"] = {
        "result_json_path": str(result_path),
        "result_json_sha256": _sha256(result_path),
    }
    return result
