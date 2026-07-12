from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.research.turbo_meta_screen import (
    MINIMUM_EXPLORATION_SHARE,
    MetaScreenError,
    MetaScreenFit,
    fit_temporal_meta_screen,
)
from hydra.strategies.turbo_dsl import StrategySpec


FEATURE_NAMES = (
    "family_code",
    "market_code",
    "timeframe_code",
    "parameter_count",
    "numeric_parameter_scale",
    "risk_parameter_count",
    "is_mutation",
)
OUTCOMES = (
    "positive_economics",
    "target_hit",
    "mll_survival",
    "consistency_success",
    "cost_resilient",
    "sufficient_opportunities",
    "non_duplicate",
    "topstep_success",
)


@dataclass(frozen=True)
class PropfirmMetaScreen:
    models: Mapping[str, MetaScreenFit]
    report_payload: dict[str, Any]
    allocation_enabled: bool
    exploration_share: float = MINIMUM_EXPLORATION_SHARE
    strategy_evidence: bool = False
    may_validate_or_promote: bool = False

    def predict(self, specs: Sequence[StrategySpec]) -> dict[str, dict[str, float]]:
        rows = [meta_features_for_spec(spec) for spec in specs]
        output = {spec.candidate_id: {} for spec in specs}
        for outcome, model in self.models.items():
            values = model.predict_proba(rows)
            for spec, value in zip(specs, values, strict=True):
                output[spec.candidate_id][outcome] = float(value)
        for spec in specs:
            row = output[spec.candidate_id]
            row["rolling_success_priority"] = float(
                np.mean(
                    [
                        row.get("positive_economics", 0.5),
                        row.get("target_hit", 0.5),
                        row.get("mll_survival", 0.5),
                        row.get("consistency_success", 0.5),
                    ]
                )
            )
            row["estimated_compute_cost"] = float(
                1.0 + spec.holding_events / 60.0 + int(spec.context_feature is not None)
            )
        return output

    def report(self) -> dict[str, Any]:
        return dict(self.report_payload)


def fit_registry_propfirm_meta_screen(
    registry_path: str | Path,
) -> PropfirmMetaScreen:
    path = Path(registry_path)
    if not path.is_file():
        return _cold("registry_missing")
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "SELECT candidate_id,family,symbol,timeframe,parameters_json,risk_json,"
            "net_profit,trade_count,combine_profit_target_hit,combine_mll_breached,"
            "combine_consistency_ok,topstep_passed,rejection_reason,parent_candidate_id,"
            "created_at FROM candidates ORDER BY created_at,candidate_id"
        ).fetchall()
    finally:
        connection.close()
    training: list[dict[str, Any]] = []
    for row in rows:
        parameters = _json_dict(row[4])
        risk = _json_dict(row[5])
        reason = str(row[12] or "").lower()
        training.append(
            {
                "candidate_id": str(row[0]),
                **_generic_features(
                    family=str(row[1]),
                    market=str(row[2]),
                    timeframe=str(row[3]),
                    parameters=parameters,
                    risk=risk,
                    is_mutation=row[13] is not None,
                ),
                "positive_economics": int(float(row[6] or 0.0) > 0),
                "target_hit": int(bool(row[8])),
                "mll_survival": int(not bool(row[9])),
                "consistency_success": int(bool(row[10])),
                "cost_resilient": int("cost" not in reason),
                "sufficient_opportunities": int(int(row[7] or 0) >= 8),
                "non_duplicate": int("duplicate" not in reason),
                "topstep_success": int(bool(row[11])),
            }
        )
    models: dict[str, MetaScreenFit] = {}
    reports: dict[str, Any] = {}
    for outcome in OUTCOMES:
        try:
            model = fit_temporal_meta_screen(
                training,
                feature_names=FEATURE_NAMES,
                target_name=outcome,
                minimum_rows=500,
            )
        except (MetaScreenError, ValueError) as exc:
            reports[outcome] = {
                "status": "UNAVAILABLE",
                "reason": f"{type(exc).__name__}:{exc}",
            }
            continue
        models[outcome] = model
        reports[outcome] = {"status": "OOS_EVALUATED", **model.report()}
    critical = ("positive_economics", "target_hit", "mll_survival")
    enabled = all(
        outcome in models
        and models[outcome].oos_recall_at_half_budget >= 0.80
        for outcome in critical
    )
    report = {
        "schema": "hydra_propfirm_meta_screen_v1",
        "registry_rows": len(training),
        "outcomes": reports,
        "allocation_enabled": enabled,
        "minimum_exploration_share": MINIMUM_EXPLORATION_SHARE,
        "false_negative_guard": (
            "enabled_only_when_each_critical_oos_recall_at_half_budget_gte_0_80"
        ),
        "interpretation_boundary": {
            "allocation_only": True,
            "strategy_evidence": False,
            "may_validate_or_promote": False,
        },
    }
    return PropfirmMetaScreen(models, report, enabled)


def meta_features_for_spec(spec: StrategySpec) -> dict[str, float]:
    return _generic_features(
        family=spec.family,
        market=spec.market,
        timeframe=spec.timeframe,
        parameters={
            "threshold": spec.threshold,
            "holding_events": spec.holding_events,
            "session_code": spec.session_code,
            "context_threshold": spec.context_threshold,
        },
        risk={"quantity": spec.quantity},
        is_mutation=spec.candidate_id.startswith("strategy_v5_"),
    )


def _generic_features(
    *,
    family: str,
    market: str,
    timeframe: str,
    parameters: Mapping[str, Any],
    risk: Mapping[str, Any],
    is_mutation: bool,
) -> dict[str, float]:
    numeric = [
        abs(float(value))
        for value in parameters.values()
        if isinstance(value, (int, float)) and math.isfinite(float(value))
    ]
    return {
        "family_code": _hash_unit(family),
        "market_code": _hash_unit(market),
        "timeframe_code": _hash_unit(timeframe),
        "parameter_count": float(len(parameters)),
        "numeric_parameter_scale": float(np.mean(numeric)) if numeric else 0.0,
        "risk_parameter_count": float(len(risk)),
        "is_mutation": float(bool(is_mutation)),
    }


def _cold(reason: str) -> PropfirmMetaScreen:
    return PropfirmMetaScreen(
        {},
        {
            "schema": "hydra_propfirm_meta_screen_v1",
            "status": "COLD_START",
            "reason": reason,
            "allocation_enabled": False,
            "minimum_exploration_share": MINIMUM_EXPLORATION_SHARE,
            "interpretation_boundary": {
                "allocation_only": True,
                "strategy_evidence": False,
                "may_validate_or_promote": False,
            },
        },
        False,
    )


def _hash_unit(value: str) -> float:
    integer = int(hashlib.sha256(value.encode()).hexdigest()[:13], 16)
    return integer / float(16**13 - 1)


def _json_dict(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


__all__ = [
    "FEATURE_NAMES",
    "OUTCOMES",
    "PropfirmMetaScreen",
    "fit_registry_propfirm_meta_screen",
    "meta_features_for_spec",
]
