from __future__ import annotations

import json
import sqlite3

from hydra.research.propfirm_meta_screen import (
    OUTCOMES,
    fit_registry_propfirm_meta_screen,
)
from hydra.strategies.turbo_dsl import (
    ComparisonOperator,
    StrategyRole,
    StrategySpec,
)


def _registry(path: str, rows: int = 800) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE candidates ("
        "candidate_id TEXT,family TEXT,symbol TEXT,timeframe TEXT,"
        "parameters_json TEXT,risk_json TEXT,net_profit REAL,trade_count INTEGER,"
        "combine_profit_target_hit INTEGER,combine_mll_breached INTEGER,"
        "combine_consistency_ok INTEGER,topstep_passed INTEGER,"
        "rejection_reason TEXT,parent_candidate_id TEXT,created_at TEXT)"
    )
    for index in range(rows):
        success = int(index % 4 in {0, 1})
        connection.execute(
            "INSERT INTO candidates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                f"candidate-{index:04d}",
                f"family-{index % 5}",
                ("NQ", "ES", "CL", "YM")[index % 4],
                ("1m", "1m|15m", "1m|60m")[index % 3],
                json.dumps({"threshold": (index % 17) / 10, "holding": 5 + index % 4}),
                json.dumps({"quantity": 1 + index % 3}),
                1000.0 if success else -500.0,
                20 if success else 4,
                success,
                0 if success else index % 2,
                success,
                success,
                "" if success else ("duplicate" if index % 3 == 0 else "cost_fragility"),
                None if index % 3 else f"parent-{index}",
                f"2026-01-{1 + index // 40:02d}T00:{index % 40:02d}:00Z",
            ),
        )
    connection.commit()
    connection.close()


def _spec() -> StrategySpec:
    return StrategySpec(
        candidate_id="new-candidate",
        lineage_id="new-lineage",
        family="family-1",
        market="NQ",
        timeframe="1m|15m",
        feature="path_efficiency",
        operator=ComparisonOperator.GREATER_EQUAL,
        threshold=0.8,
        side=1,
        holding_events=15,
        point_value=20.0,
        round_turn_cost=14.5,
        role=StrategyRole.COMBINE_PASSER,
    )


def test_propfirm_meta_screen_is_oos_allocation_only(tmp_path) -> None:
    path = tmp_path / "registry.db"
    _registry(str(path))
    fitted = fit_registry_propfirm_meta_screen(path)
    report = fitted.report()
    assert report["registry_rows"] == 800
    assert report["minimum_exploration_share"] >= 0.20
    assert not report["interpretation_boundary"]["strategy_evidence"]
    assert not report["interpretation_boundary"]["may_validate_or_promote"]
    assert set(report["outcomes"]) == set(OUTCOMES)
    predictions = fitted.predict([_spec()])
    assert "rolling_success_priority" in predictions["new-candidate"]
    assert 0.0 <= predictions["new-candidate"]["rolling_success_priority"] <= 1.0


def test_missing_registry_keeps_full_scientific_boundary(tmp_path) -> None:
    fitted = fit_registry_propfirm_meta_screen(tmp_path / "missing.db")
    assert not fitted.allocation_enabled
    assert fitted.exploration_share >= 0.20
    assert fitted.predict([_spec()])["new-candidate"]["rolling_success_priority"] == 0.5
