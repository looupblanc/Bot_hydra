from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from hydra.research.role_conditioned_structural_epoch import (
    POOLS,
    PREREGISTRATION_SHA256,
    STRUCTURES,
    RoleConditionedEpochError,
    _canonical_hash,
    _event_contracts,
    _fit_parameters,
    _normalize_mutation_ledger,
    _prior_training,
    run_role_conditioned_structural_epoch,
)


TASK = Path("reports/engineering/hydra_role_conditioned_structural_epoch_20260711.md")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _event(
    candidate_id: str,
    timestamp: pd.Timestamp,
    pnl: float,
    *,
    symbol: str,
    mae: float = -80.0,
) -> dict[str, Any]:
    cost = 2.0
    return {
        "candidate_id": candidate_id,
        "parent_candidate_id": f"parent_{candidate_id}",
        "timestamp": timestamp.isoformat(),
        "event_session_id": timestamp.date().isoformat(),
        "symbol": symbol,
        "active_contract": f"{symbol}H4",
        "gross_pnl": float(pnl + cost),
        "cost": cost,
        "net_pnl": float(pnl),
        "mae_dollars": float(mae),
    }


def _standard_hash(value: dict[str, Any], field: str) -> str:
    payload = dict(value)
    payload.pop(field, None)
    return _canonical_hash(payload)


def _frozen_inputs(
    root: Path,
    *,
    q4_access_count: int = 0,
    change_2024: float = 0.0,
) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    candidate_ids = [
        "strategy_alpha_a_h120_v1",
        "strategy_alpha_b_h30_v1",
        "strategy_alpha_c_open_gap_v1",
    ]
    symbols = ["YM", "NQ", "GC"]
    rows: list[dict[str, Any]] = []
    for year, starts, count in (
        (2023, (1, 5, 9), 3),
        (2024, (2, 5, 8), 3),
    ):
        for month in starts:
            for day_offset in range(count):
                timestamp = pd.Timestamp(
                    year=year,
                    month=month,
                    day=2 + day_offset,
                    hour=14,
                    tz="UTC",
                )
                for index, (candidate_id, symbol) in enumerate(
                    zip(candidate_ids, symbols, strict=True)
                ):
                    pnl = (360.0 - index * 130.0) * (1.0 if day_offset != 1 else -0.35)
                    if year == 2024:
                        pnl += change_2024 * (index + 1)
                    rows.append(
                        _event(
                            candidate_id,
                            timestamp,
                            pnl,
                            symbol=symbol,
                            mae=-70.0 - 20.0 * index,
                        )
                    )
    ledger = root / "mutation_ledger.jsonl"
    ledger.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    candidates = [
        {
            "candidate_id": candidate_id,
            "parent_candidate_id": f"parent_{candidate_id}",
            "status": "RESEARCH_PROTOTYPE",
            "status_inherited": False,
            "inherited_passes": [],
            "q4_access_count": 0,
            "order_capability": False,
        }
        for candidate_id in candidate_ids
    ]
    mutation: dict[str, Any] = {
        "schema": "hydra_promising_lineage_mutation_result_v1",
        "code_commit": "frozen-mutation",
        "development_end_exclusive": "2024-10-01",
        "candidates": candidates,
        "artifacts": {"trade_ledger_sha256": _sha(ledger)},
        "q4_access_count": q4_access_count,
        "network_requests": 0,
        "paid_data_requests": 0,
        "order_capability": False,
        "paper_shadow_ready": 0,
        "shadow_research_active": 0,
    }
    mutation["result_hash"] = _standard_hash(mutation, "result_hash")
    mutation_path = root / "mutation_result.json"
    _write_json(mutation_path, mutation)

    evidence_path = root / "halving_evidence.jsonl"
    evidence_path.write_text(
        "".join(
            json.dumps(
                {
                    "candidate_id": candidate_id,
                    "status": "PROMISING_RESEARCH_CANDIDATE",
                    "selected_elite": True,
                    "status_inherited": False,
                },
                sort_keys=True,
            )
            + "\n"
            for candidate_id in candidate_ids
        ),
        encoding="utf-8",
    )
    manifest: dict[str, Any] = {
        "schema": "hydra_post_mutation_elite_manifest_v1",
        "selected_candidate_ids": candidate_ids,
        "selected_count": len(candidate_ids),
        "status_ceiling": "PROMISING_RESEARCH_CANDIDATE",
        "q4_access_count": 0,
        "order_capability": False,
        "paper_shadow_ready": 0,
        "shadow_research_active": 0,
    }
    manifest["manifest_hash"] = _standard_hash(manifest, "manifest_hash")
    manifest_path = root / "halving_manifest.json"
    _write_json(manifest_path, manifest)
    halving: dict[str, Any] = {
        "schema": "hydra_post_mutation_successive_halving_result_v1",
        "source_mutation_result_sha256": _sha(mutation_path),
        "selected_candidate_ids": candidate_ids,
        "selected_elite_count": len(candidate_ids),
        "artifacts": {
            "candidate_evidence": {
                "path": str(evidence_path),
                "sha256": _sha(evidence_path),
            },
            "elite_manifest": {
                "path": str(manifest_path),
                "sha256": _sha(manifest_path),
            },
        },
        "q4_access_count": 0,
        "network_requests": 0,
        "paid_data_requests": 0,
        "order_capability": False,
        "paper_shadow_ready": 0,
        "shadow_research_active": 0,
    }
    halving_hash_payload = dict(halving)
    halving_hash_payload["artifacts"] = {
        key: value["sha256"] for key, value in sorted(halving["artifacts"].items())
    }
    halving["result_hash"] = _canonical_hash(halving_hash_payload)
    halving_path = root / "halving_result.json"
    _write_json(halving_path, halving)

    portfolio: dict[str, Any] = {
        "schema": "hydra_portfolio_role_research_result_v1",
        "q4_access_count": 0,
        "network_requests": 0,
        "incremental_databento_spend_usd": 0.0,
        "broker_connections": 0,
        "outbound_orders": 0,
        "paper_shadow_ready": 0,
        "shadow_research_active": 0,
    }
    portfolio["result_hash"] = _standard_hash(portfolio, "result_hash")
    portfolio_path = root / "portfolio_result.json"
    _write_json(portfolio_path, portfolio)
    meta: dict[str, Any] = {
        "schema": "meta_failure_allocation_v1",
        "governance": {
            "q4_access_count_delta": 0,
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
        },
    }
    meta["result_hash"] = _standard_hash(meta, "result_hash")
    meta_path = root / "meta_result.json"
    _write_json(meta_path, meta)
    return {
        "engineering_task_path": TASK,
        "engineering_task_sha256": PREREGISTRATION_SHA256,
        "mutation_result_path": mutation_path,
        "mutation_result_sha256": _sha(mutation_path),
        "mutation_result_hash": mutation["result_hash"],
        "mutation_ledger_path": ledger,
        "mutation_ledger_sha256": _sha(ledger),
        "halving_result_path": halving_path,
        "halving_result_sha256": _sha(halving_path),
        "halving_result_hash": halving["result_hash"],
        "halving_evidence_path": evidence_path,
        "halving_evidence_sha256": _sha(evidence_path),
        "halving_manifest_path": manifest_path,
        "halving_manifest_sha256": _sha(manifest_path),
        "halving_manifest_hash": manifest["manifest_hash"],
        "portfolio_role_result_path": portfolio_path,
        "portfolio_role_result_sha256": _sha(portfolio_path),
        "portfolio_role_result_hash": portfolio["result_hash"],
        "meta_result_path": meta_path,
        "meta_result_sha256": _sha(meta_path),
        "meta_result_hash": meta["result_hash"],
        "code_commit": "role-epoch-test-code",
        "control_count": 3,
    }


def _run(output: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    return run_role_conditioned_structural_epoch(output, **inputs)


def test_exact_six_two_per_pool_aliases_and_absolute_immutable_artifacts(
    tmp_path: Path,
) -> None:
    inputs = _frozen_inputs(tmp_path / "inputs")
    result = _run(tmp_path / "output", inputs)

    assert result["policy_count"] == result["candidate_count"] == 6
    assert result["pool_counts"] == {pool: 2 for pool in POOLS}
    assert {row["policy_id"] for row in result["candidates"]} == {
        row["policy_id"] for row in STRUCTURES
    }
    assert result["promising_candidates"] == result["promising_candidate_count"]
    assert sum(result["status_counts"].values()) == 6
    assert result["matched_control_count"] == 18
    assert all(row["candidate_id"] == row["policy_id"] for row in result["candidates"])
    assert all(row["role"] == "PORTFOLIO" for row in result["candidates"])
    assert all(row["primary_market"] == "PORTFOLIO" for row in result["candidates"])
    assert all(not row["status_inherited"] and not row["inherited_passes"] for row in result["candidates"])
    assert all(str(row["mechanism_family"]).startswith("account_policy_") for row in result["candidates"])
    for artifact in result["artifacts"].values():
        path = Path(artifact["path"])
        assert path.is_absolute() and path.is_file()
        assert _sha(path) == artifact["sha256"]
    second = _run(tmp_path / "output", inputs)
    assert second["result_hash"] == result["result_hash"]
    assert second["candidates"] == result["candidates"]


def test_pending_outcome_is_not_visible_before_true_completion() -> None:
    first = pd.Timestamp("2023-01-03T14:00:00Z")
    rows = [
        _event("strategy_guard_h120_v1", first, 500.0, symbol="YM", mae=-900.0),
        _event(
            "strategy_guard_h120_v1",
            first + pd.Timedelta(minutes=30),
            -100.0,
            symbol="YM",
            mae=-50.0,
        ),
        _event(
            "strategy_guard_h120_v1",
            first + pd.Timedelta(minutes=121),
            10.0,
            symbol="YM",
            mae=-10.0,
        ),
    ]
    frame = _normalize_mutation_ledger(rows, {"strategy_guard_h120_v1"})
    before_completion = first + pd.Timedelta(minutes=30)

    assert _prior_training(frame, before_completion).empty
    fit = _fit_parameters(frame)
    latch = next(row for row in STRUCTURES if row["kind"] == "realized_qualifying_day_latch")
    latch_contracts, _, source_ends = _event_contracts(frame, latch, fit)
    throttle = next(row for row in STRUCTURES if row["kind"] == "prior_mae_quantile_throttle")
    throttle_contracts, _, _ = _event_contracts(frame, throttle, fit)

    assert latch_contracts.tolist() == [1, 1, 0]
    assert throttle_contracts[1] == 2
    assert throttle_contracts[2] == 1
    assert pd.Timestamp(source_ends[1]) < before_completion
    assert pd.Timestamp(source_ends[1]) != first + pd.Timedelta(minutes=120)


def test_2024_outcomes_cannot_change_frozen_2023_fit(tmp_path: Path) -> None:
    first_inputs = _frozen_inputs(tmp_path / "first-input", change_2024=0.0)
    second_inputs = _frozen_inputs(tmp_path / "second-input", change_2024=10_000.0)
    first = _run(tmp_path / "first-output", first_inputs)
    second = _run(tmp_path / "second-output", second_inputs)

    assert first["fit_hash"] == second["fit_hash"]
    first_manifest = json.loads(
        Path(first["artifacts"]["policy_manifest"]["path"]).read_text(encoding="utf-8")
    )
    second_manifest = json.loads(
        Path(second["artifacts"]["policy_manifest"]["path"]).read_text(encoding="utf-8")
    )
    assert first_manifest["fit_parameters"] == second_manifest["fit_parameters"]
    assert all(
        row["current_event_outcome_used_for_decision"] is False
        for row in (
            json.loads(line)
            for line in Path(
                first["artifacts"]["transformed_event_ledger"]["path"]
            ).read_text(encoding="utf-8").splitlines()
            if line
        )
    )


def test_security_zeros_controls_and_q4_source_fail_closed(tmp_path: Path) -> None:
    inputs = _frozen_inputs(tmp_path / "safe-input")
    result = _run(tmp_path / "safe-output", inputs)
    for field in (
        "q4_access_count",
        "network_requests",
        "paid_data_requests",
        "broker_connections",
        "outbound_orders",
        "paper_shadow_ready",
        "shadow_research_active",
    ):
        assert result[field] == 0
    assert result["incremental_databento_spend_usd"] == 0.0
    assert result["order_capability"] is False
    assert all(row["matched_controls"]["control_count"] == 3 for row in result["candidates"])
    assert all(row["matched_controls"]["operational_policy"] is False for row in result["candidates"])

    unsafe = _frozen_inputs(tmp_path / "unsafe-input", q4_access_count=1)
    with pytest.raises(RoleConditionedEpochError, match="prohibited q4_access_count"):
        _run(tmp_path / "unsafe-output", unsafe)


def test_horizon_derivation_is_conservative_and_costs_reconcile(tmp_path: Path) -> None:
    result = _run(tmp_path / "output", _frozen_inputs(tmp_path / "inputs"))
    ledger = [
        json.loads(line)
        for line in Path(result["artifacts"]["transformed_event_ledger"]["path"])
        .read_text(encoding="utf-8")
        .splitlines()
        if line
    ]
    horizons = {
        row["candidate_id"]: row["holding_minutes"]
        for row in ledger
    }
    assert horizons["strategy_alpha_a_h120_v1"] == 120
    assert horizons["strategy_alpha_b_h30_v1"] == 30
    assert horizons["strategy_alpha_c_open_gap_v1"] == 60
    assert all(pd.Timestamp(row["completion_timestamp"]) > pd.Timestamp(row["signal_timestamp"]) for row in ledger)
    assert all(
        np.isclose(row["gross_pnl_1_0x"] - row["cost_1_0x"], row["net_pnl_1_0x"])
        and np.isclose(row["gross_pnl_1_0x"] - row["cost_1_5x"], row["net_pnl_1_5x"])
        for row in ledger
    )

