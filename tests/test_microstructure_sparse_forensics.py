from __future__ import annotations

import json
import math
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hydra.production.microstructure_sparse_forensics import (
    SparseForensicsConfig,
    analyze_sparse_forensics,
    audit_authoritative_0031,
)


SESSION = "2024-07-08"


def _manifest(sleeve_id: str, *, family: str, path: str) -> dict[str, object]:
    return {
        "sleeve_id": sleeve_id,
        "family": family,
        "market": "NQ",
        "execution_path": path,
        "deployability_tier": (
            "L1_DEPLOYABLE" if path == "AGGRESSIVE" else "MBO_TEACHER_ONLY"
        ),
    }


def _signal(
    sleeve_id: str,
    signal_id: str,
    feature_hash: str,
    *,
    decision_ns: int,
    direction: int = 1,
) -> dict[str, object]:
    return {
        "sleeve_id": sleeve_id,
        "signal_id": signal_id,
        "feature_hash": feature_hash,
        "market": "NQ",
        "session_id": SESSION,
        "direction": direction,
        "decision_time_ns": decision_ns,
    }


def _trade(
    sleeve_id: str,
    signal_id: str,
    trade_id: str,
    *,
    path: str,
    entry_ns: int,
    exit_ns: int,
    entry_price: float,
    exit_price: float,
    direction: int = 1,
    minimum_unrealized: float = -20.0,
) -> dict[str, object]:
    gross = direction * (exit_price - entry_price) * 20.0
    return {
        "sleeve_id": sleeve_id,
        "signal_id": signal_id,
        "trade_id": trade_id,
        "market": "NQ",
        "session_id": SESSION,
        "role": "DISCOVERY",
        "execution_path": path,
        "direction": direction,
        "requested_quantity": 1,
        "filled_quantity": 1,
        "entry_time_ns": entry_ns,
        "exit_time_ns": exit_ns,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "stop_price": entry_price - direction,
        "target_price": entry_price + direction * 2.0,
        "exit_reason": "MAXIMUM_HOLD",
        "gross_pnl_usd": gross,
        "base_slippage_cost_usd": 20.0,
        "normal_total_cost_usd": 23.8,
        "stressed_total_cost_usd": 33.8,
        "normal_costs_usd": 3.8,
        "normal_net_pnl_usd": gross - 3.8,
        "stressed_costs_usd": 13.8,
        "stressed_net_pnl_usd": gross - 13.8,
        "minimum_unrealized_pnl_usd": minimum_unrealized,
    }


def _snap(timestamp: int, bid: float, ask: float) -> dict[str, object]:
    return {
        "market": "NQ",
        "session_id": SESSION,
        "available_ns": timestamp,
        "bid_price": bid,
        "ask_price": ask,
    }


def _material() -> dict[str, object]:
    manifests = [
        _manifest("a", family="FLOW", path="AGGRESSIVE"),
        _manifest("b", family="FLOW", path="AGGRESSIVE"),
        _manifest("p", family="QUEUE", path="PASSIVE"),
    ]
    signals = [
        _signal("a", "sa", "fa", decision_ns=1_000_000_000),
        _signal("b", "sb", "fb", decision_ns=1_500_000_000),
        _signal(
            "p",
            "sp",
            "fp",
            decision_ns=1_700_000_000,
            direction=-1,
        ),
    ]
    trades = [
        _trade(
            "a",
            "sa",
            "ta",
            path="AGGRESSIVE",
            entry_ns=2_000_000_000,
            exit_ns=3_000_000_000,
            entry_price=100.75,
            exit_price=100.00,
        ),
        _trade(
            "b",
            "sb",
            "tb",
            path="AGGRESSIVE",
            entry_ns=2_100_000_000,
            exit_ns=3_100_000_000,
            entry_price=100.75,
            exit_price=100.00,
        ),
        _trade(
            "p",
            "sp",
            "tp",
            path="PASSIVE",
            entry_ns=2_200_000_000,
            exit_ns=3_200_000_000,
            entry_price=100.25,
            exit_price=100.75,
            direction=-1,
        ),
    ]
    snapshots = [
        _snap(1_000_000_000, 100.00, 100.25),
        _snap(1_500_000_000, 100.00, 100.25),
        _snap(1_700_000_000, 100.25, 100.50),
        _snap(2_000_000_000, 100.00, 100.25),
        _snap(2_100_000_000, 100.00, 100.25),
        _snap(2_200_000_000, 100.25, 100.50),
        _snap(3_000_000_000, 100.50, 100.75),
        _snap(3_100_000_000, 100.50, 100.75),
        _snap(3_200_000_000, 100.00, 100.25),
    ]
    features = [
        {
            "feature_hash": feature,
            "market": "NQ",
            "session_id": SESSION,
            "decision_ns": signal["decision_time_ns"],
            "available_ns": signal["decision_time_ns"],
            "spread_ticks": 1.0,
        }
        for signal, feature in zip(signals, ("fa", "fb", "fp"), strict=True)
    ]
    return {
        "sleeve_manifests": manifests,
        "signals": signals,
        "trades": trades,
        "feature_matrices": features,
        "book_snapshots": snapshots,
        "selected_sessions": [SESSION],
    }


def test_exact_bridge_separates_embedded_passive_slippage() -> None:
    result = analyze_sparse_forensics(**_material())
    bridge = result["gross_to_net"]
    trades = _material()["trades"]
    assert bridge["trade_count"] == 3
    assert bridge["stored_two_sided_base_slippage_usd"] == 60.0
    # Two aggressive trades embed $20; the passive trade embeds exit-only $10.
    assert bridge["marketable_slippage_usd"] == 50.0
    assert bridge["commission_usd"] == pytest.approx(11.4)
    assert bridge["realized_gross_usd"] == pytest.approx(
        sum(float(row["gross_pnl_usd"]) for row in trades)
    )
    assert bridge["normal_net_usd"] == pytest.approx(
        sum(float(row["normal_net_pnl_usd"]) for row in trades)
    )
    assert bridge["stressed_net_usd"] == pytest.approx(
        sum(float(row["stressed_net_pnl_usd"]) for row in trades)
    )
    assert bridge["normal_bridge_residual_usd"] == pytest.approx(0.0)
    assert bridge["stressed_bridge_residual_usd"] == pytest.approx(0.0)


def test_cluster_deduplicates_same_family_side_zone_and_exposes_conflict() -> None:
    result = analyze_sparse_forensics(**_material())
    flow = [
        row
        for row in result["opportunity_clusters"]
        if row["family"] == "FLOW"
    ]
    assert len(flow) == 1
    assert flow[0]["signal_count"] == 2
    assert flow[0]["trade_count"] == 2
    assert flow[0]["duplicate_trade_count"] == 1
    assert flow[0]["signal_ids"] == ["sa", "sb"]
    assert flow[0]["trade_ids"] == ["ta", "tb"]
    assert flow[0]["signal_to_trade_ids"] == {"sa": ["ta"], "sb": ["tb"]}
    assert result["opportunity_cluster_summary"]["duplicate_trade_count"] == 1
    conflict = result["governor_and_conflicts"]
    assert conflict["governor_evidence_status"] == "UNAVAILABLE_STANDALONE_SLEEVE_PILOT"
    assert conflict["actual_suppressed_signal_count"] is None
    assert conflict["potential_opposite_side_cluster_count"] >= 1


def test_episode_trade_count_reconciles_terminal_ledger() -> None:
    material = _material()
    first = material["trades"][0]
    episodes = [
        {
            "episode_id": "episode-normal",
            "sleeve_id": "a",
            "start_session": SESSION,
            "horizon_days": 5,
            "scenario": "NORMAL",
            "coverage_status": "FULL_COVERAGE",
            "full_coverage": True,
            "target_reached": False,
            "mll_breached": False,
            "net_pnl_usd": first["normal_net_pnl_usd"],
            "costs_usd": first["normal_costs_usd"],
        },
        {
            "episode_id": "episode-stress",
            "sleeve_id": "a",
            "start_session": SESSION,
            "horizon_days": 5,
            "scenario": "STRESSED_1_5X",
            "coverage_status": "FULL_COVERAGE",
            "full_coverage": True,
            "target_reached": False,
            "mll_breached": False,
            "net_pnl_usd": first["stressed_net_pnl_usd"],
            "costs_usd": first["stressed_costs_usd"],
        },
    ]
    result = analyze_sparse_forensics(**material, episodes=episodes)
    assert [row["trade_count"] for row in result["episode_trade_rows"]] == [1, 1]
    assert all(
        row["trade_occurrence_count"] == 1
        for row in result["episode_trade_summary"]
    )


def _write_dataset(root: Path, name: str, rows: list[dict[str, object]]) -> None:
    directory = root / "datasets" / name
    directory.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), directory / "part-000000.parquet")


def test_authoritative_loader_reads_partitioned_parquet(tmp_path: Path) -> None:
    material = _material()
    for name in (
        "sleeve_manifests",
        "signals",
        "trades",
        "feature_matrices",
        "book_snapshots",
    ):
        _write_dataset(tmp_path, name, material[name])
    _write_dataset(tmp_path, "episodes", [])
    _write_dataset(tmp_path, "account_daily_paths", [])
    (tmp_path / "decision_report.json").write_text(
        json.dumps({"selected_sessions": [SESSION]}), encoding="utf-8"
    )

    result = audit_authoritative_0031(tmp_path)
    assert result["counts"]["signals"] == 3
    assert result["counts"]["trades"] == 3
    assert result["source_paths"]["trades"][0].endswith("part-000000.parquet")
    assert len(result["audit_hash"]) == 64
    assert math.isfinite(result["gross_to_net"]["normal_net_usd"])
