from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from hydra.factory.post_mutation_successive_halving import (
    PREREGISTRATION_SHA256,
    PostMutationIntegrityError,
    _bootstrap,
    _canonical_hash,
    run_post_mutation_successive_halving,
)


TASK = Path("reports/engineering/hydra_post_mutation_successive_halving_20260711.md")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _candidate(
    candidate_id: str,
    *,
    pool: str = "COMBINE_PASSER_POOL",
    role: str = "ALPHA",
    defensive_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    parent = f"parent_{candidate_id}"
    row: dict[str, Any] = {
        "candidate_id": candidate_id,
        "parent_candidate_id": parent,
        "status": "RESEARCH_PROTOTYPE",
        "status_inherited": False,
        "inherited_passes": [],
        "role": role,
        "objective_pool": pool,
        "hypothesis": {
            "hypothesis_id": f"hyp_{candidate_id}",
            "parent_candidate_id": parent,
            "child_candidate_id": candidate_id,
            "mutation_class": "PAST_ONLY_GAP_STABILITY_BAND",
            "status_inheritance_allowed": False,
            "q4_access_allowed": False,
            "live_or_broker_allowed": False,
        },
        "guard": {"fit_year": 2023},
        "metrics": {"candidate_null_bh_adjusted_p": 0.10},
        "behaviorally_duplicate": False,
        "shadow_activation_allowed": False,
        "paper_shadow_ready": False,
        "q4_access_count": 0,
        "order_capability": False,
    }
    if defensive_evidence is not None:
        row["defensive_evidence"] = defensive_evidence
    return row


def _timestamps(offset_minutes: int = 0) -> list[str]:
    values = []
    for start in ("2023-06-01", "2024-02-01", "2024-05-01"):
        base = pd.Timestamp(start, tz="UTC")
        values.extend(
            (base + pd.Timedelta(days=index, minutes=offset_minutes)).isoformat()
            for index in range(4)
        )
    return values


def _rows(
    candidate: dict[str, Any],
    pnls: list[float],
    *,
    offset_minutes: int = 0,
    symbol: str = "ES",
    timestamps: list[str] | None = None,
    future_availability: bool = False,
) -> list[dict[str, Any]]:
    times = timestamps or _timestamps(offset_minutes)
    rows = []
    for index, (timestamp, pnl) in enumerate(zip(times, pnls, strict=True)):
        row = {
            "candidate_id": candidate["candidate_id"],
            "parent_candidate_id": candidate["parent_candidate_id"],
            "timestamp": timestamp,
            "event_session_id": timestamp[:10],
            "symbol": symbol,
            "active_contract": f"{symbol}M4",
            "net_pnl": float(pnl),
            "gross_pnl": float(pnl) + 2.0,
            "cost": 2.0,
            "mae_dollars": float(min(-20.0, pnl)),
        }
        if future_availability and index == 3:
            timestamp_value = pd.Timestamp(timestamp)
            row["availability_timestamp"] = (
                timestamp_value + pd.Timedelta(seconds=1)
            ).isoformat()
        rows.append(row)
    return rows


def _frozen_inputs(
    root: Path,
    candidates: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    ledger = root / "mutation_trades.jsonl"
    ledger.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    payload: dict[str, Any] = {
        "schema": "hydra_promising_lineage_mutation_result_v1",
        "code_commit": "frozen-mutation-code",
        "development_end_exclusive": "2024-10-01",
        "q4_access_count": 0,
        "network_requests": 0,
        "paid_data_requests": 0,
        "order_capability": False,
        "parent_audit": [
            {
                "candidate_id": row["parent_candidate_id"],
                "parent_unchanged": True,
            }
            for row in candidates
        ],
        "candidates": candidates,
        "artifacts": {"trade_ledger_sha256": _sha(ledger)},
    }
    payload["result_hash"] = _canonical_hash(payload)
    result = root / "mutation_result.json"
    result.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return result, ledger


def _run(output: Path, result: Path, ledger: Path) -> dict[str, Any]:
    return run_post_mutation_successive_halving(
        output,
        engineering_task_path=TASK,
        engineering_task_sha256=PREREGISTRATION_SHA256,
        mutation_result_path=result,
        mutation_result_sha256=_sha(result),
        mutation_trade_ledger_path=ledger,
        mutation_trade_ledger_sha256=_sha(ledger),
        code_commit="post-mutation-test-code",
    )


def test_one_weak_noncatastrophic_fold_is_not_universally_rejected(tmp_path: Path) -> None:
    candidate = _candidate("combine_weak_fold")
    pnls = [1_000.0] * 4 + [-250.0] * 4 + [1_000.0] * 4
    result, ledger = _frozen_inputs(tmp_path / "inputs", [candidate], _rows(candidate, pnls))

    output = _run(tmp_path / "output", result, ledger)
    evaluated = output["candidates"][0]

    assert evaluated["weak_fold_non_catastrophic"] is True
    assert evaluated["temporal_screen_pass"] is True
    assert evaluated["status"] == "PROMISING_RESEARCH_CANDIDATE"
    assert "WEAK_DEVELOPMENT_FOLD" in evaluated["uncertainty_flags"]
    assert not evaluated["paper_shadow_ready"]
    assert not evaluated["shadow_research_active"]


def test_catastrophic_fold_is_scientifically_rejected_not_integrity_fabricated(
    tmp_path: Path,
) -> None:
    candidate = _candidate("combine_catastrophic")
    pnls = [1_250.0] * 4 + [-1_750.0] * 4 + [1_250.0] * 4
    result, ledger = _frozen_inputs(tmp_path / "inputs", [candidate], _rows(candidate, pnls))

    output = _run(tmp_path / "output", result, ledger)
    evaluated = output["candidates"][0]

    assert evaluated["weak_fold_non_catastrophic"] is False
    assert evaluated["status"] == "RESEARCH_REJECTED"
    assert evaluated["disposition"] == "NEGATIVE_OR_CATASTROPHIC_DEVELOPMENT_EVIDENCE"
    assert output["q4_access_count"] == 0
    assert output["order_capability"] is False


def test_phase_specific_pools_and_maximum_feasible_archive(tmp_path: Path) -> None:
    defensive = {
        "pool_utility_delta": 0.7,
        "maximum_drawdown_reduction": 700.0,
        "min_mll_buffer_delta": 650.0,
        "shared_loss_days_reduction": 2,
        "matched_control_p": 0.05,
        "hard_risk_violation": False,
    }
    combine = _candidate("combine_path", pool="COMBINE_PASSER_POOL")
    xfa = _candidate("xfa_path", pool="XFA_PAYOUT_POOL")
    guard = _candidate(
        "defensive_path",
        pool="DEFENSIVE_ACCOUNT_POOL",
        role="DEFENSIVE",
        defensive_evidence=defensive,
    )
    rows = (
        _rows(combine, [1_000.0] * 12, offset_minutes=1, symbol="ES")
        + _rows(xfa, [1_000.0] * 12, offset_minutes=2, symbol="NQ")
        + _rows(guard, [0.0] * 12, offset_minutes=3, symbol="CL")
    )
    result, ledger = _frozen_inputs(tmp_path / "inputs", [combine, xfa, guard], rows)

    output = _run(tmp_path / "output", result, ledger)
    by_id = {row["candidate_id"]: row for row in output["candidates"]}

    assert by_id["combine_path"]["block_bootstrap"]["target_before_mll_probability"] > 0.0
    assert by_id["xfa_path"]["block_bootstrap"]["expected_payout_cycles_before_ruin"] >= 1.0
    assert (
        by_id["xfa_path"]["block_bootstrap"]["xfa_replay_method"]
        == "daily_block_bootstrap_simulate_funded_xfa"
    )
    assert by_id["xfa_path"]["block_bootstrap"]["qualifying_day_frequency"] == 1.0
    assert by_id["xfa_path"]["block_bootstrap"]["median_days_to_payout"] == 5.0
    assert by_id["defensive_path"]["status"] == "PROMISING_RESEARCH_CANDIDATE"
    assert by_id["defensive_path"]["pooled_net_pnl"] == 0.0
    assert set(output["objective_pool_counts"]) == {
        "COMBINE_PASSER_POOL",
        "XFA_PAYOUT_POOL",
        "DEFENSIVE_ACCOUNT_POOL",
    }
    assert output["selected_elite_count"] == 3
    assert output["selection_audit"]["maximum_feasible_achieved"] is True
    assert set(output["selection_audit"]["pool_counts"]) == set(output["objective_pool_counts"])


def test_xfa_bootstrap_does_not_turn_four_winning_days_into_a_payout() -> None:
    """A profit quotient would report one cycle; funded rules require five days."""

    candidate = _candidate("xfa_four_days", pool="XFA_PAYOUT_POOL")
    rows = _rows(
        candidate,
        [2_000.0] * 4,
        timestamps=[
            (
                pd.Timestamp("2023-06-01", tz="UTC")
                + pd.Timedelta(days=index)
            ).isoformat()
            for index in range(4)
        ],
    )
    frame = pd.DataFrame(rows)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)

    replay = _bootstrap(candidate["candidate_id"], frame, "XFA_PAYOUT_POOL")

    assert replay["xfa_replay_method"] == "daily_block_bootstrap_simulate_funded_xfa"
    assert replay["xfa_daily_path_length"] == 4
    assert replay["qualifying_day_frequency"] == 1.0
    assert replay["expected_payout_cycles_before_ruin"] == 0.0
    assert replay["payout_eligibility_probability"] == 0.0
    assert replay["median_days_to_payout"] is None
    assert replay["mll_survival_probability"] == 1.0


def test_behavioral_clone_is_rejected_and_lineage_cannot_inflate_elites(tmp_path: Path) -> None:
    first = _candidate("clone_a")
    second = _candidate("clone_b")
    timestamps = _timestamps()
    rows = _rows(first, [1_000.0] * 12, timestamps=timestamps) + _rows(
        second, [900.0] * 12, timestamps=timestamps
    )
    result, ledger = _frozen_inputs(tmp_path / "inputs", [first, second], rows)

    output = _run(tmp_path / "output", result, ledger)
    duplicates = [row for row in output["candidates"] if row["behaviorally_duplicate"]]

    assert len(duplicates) == 1
    assert duplicates[0]["status"] == "RESEARCH_REJECTED"
    assert duplicates[0]["duplicate_of"] == "clone_a"
    assert output["selected_elite_count"] == 1


def test_hash_q4_and_future_availability_are_hard_fail_closed(tmp_path: Path) -> None:
    candidate = _candidate("integrity")
    result, ledger = _frozen_inputs(
        tmp_path / "inputs",
        [candidate],
        _rows(candidate, [1_000.0] * 12, future_availability=True),
    )
    with pytest.raises(PostMutationIntegrityError, match="Future information"):
        _run(tmp_path / "future-output", result, ledger)

    with pytest.raises(PostMutationIntegrityError, match="mutation result hash drift"):
        run_post_mutation_successive_halving(
            tmp_path / "hash-output",
            engineering_task_path=TASK,
            engineering_task_sha256=PREREGISTRATION_SHA256,
            mutation_result_path=result,
            mutation_result_sha256="0" * 64,
            mutation_trade_ledger_path=ledger,
            mutation_trade_ledger_sha256=_sha(ledger),
            code_commit="test",
        )

    q4_rows = _rows(candidate, [1_000.0] * 12)
    q4_rows[-1]["timestamp"] = "2024-10-02T00:00:00+00:00"
    q4_result, q4_ledger = _frozen_inputs(tmp_path / "q4", [candidate], q4_rows)
    with pytest.raises(PostMutationIntegrityError, match="Q4-or-later"):
        _run(tmp_path / "q4-output", q4_result, q4_ledger)


def test_bootstrap_and_outputs_are_deterministic_with_status_ceiling(tmp_path: Path) -> None:
    candidate = _candidate("deterministic")
    pnls = [900.0, 700.0, -100.0, 800.0] * 3
    result, ledger = _frozen_inputs(tmp_path / "inputs", [candidate], _rows(candidate, pnls))

    first = _run(tmp_path / "first", result, ledger)
    second = _run(tmp_path / "second", result, ledger)

    assert first["result_hash"] == second["result_hash"]
    assert first["candidates"] == second["candidates"]
    for artifact in ("candidate_evidence", "elite_manifest", "report"):
        assert first["artifacts"][artifact]["sha256"] == second["artifacts"][artifact]["sha256"]
        assert Path(first["artifacts"][artifact]["path"]).is_absolute()
    assert Path(first["artifacts"]["result"]["path"]).is_absolute()
    assert first["status_ceiling"] == "PROMISING_RESEARCH_CANDIDATE"
    assert all(
        row["status"]
        in {
            "PROMISING_RESEARCH_CANDIDATE",
            "INSUFFICIENT_EVIDENCE",
            "RESEARCH_REJECTED",
            "HARD_INTEGRITY_REJECTED",
        }
        for row in first["candidates"]
    )
    assert first["paper_shadow_ready"] == 0
    assert first["shadow_research_active"] == 0


def test_status_inheritance_is_a_hard_failure(tmp_path: Path) -> None:
    candidate = _candidate("inherited")
    candidate["status_inherited"] = True
    result, ledger = _frozen_inputs(tmp_path / "inputs", [candidate], _rows(candidate, [100.0] * 12))

    with pytest.raises(PostMutationIntegrityError, match="inheritance"):
        _run(tmp_path / "output", result, ledger)
