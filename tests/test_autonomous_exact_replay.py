from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_exact_replay as exact
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.research.causal_sleeve_replay import CausalTradeMark, CausalTradeTrajectory
from hydra.research.causal_target_velocity import HazardOutcome


def _entry(
    candidate_id: str,
    *,
    market: str,
    mechanism: str,
    qd_cell: str,
    behavior: str,
    stressed: float,
    wave: int = 1,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "candidate": {
            "market": market,
            "mechanism": mechanism,
            "timeframe": "1m",
            "session_code": 1,
        },
        "candidate_fingerprint": f"fingerprint-{candidate_id}",
        "realized_behavioral_fingerprint": behavior,
        "qd_cell": qd_cell,
        "stressed_full_net": stressed,
        "normal_full_net": stressed + 10.0,
        "stressed_design_net": stressed / 2.0,
        "positive_stressed_block_count": 2,
        "completed_event_count": 1,
        "exact_hashes": {"decision_hash": f"hash-{candidate_id}"},
        "_source_wave": wave,
    }


def test_quality_diverse_selection_round_robins_market_mechanism() -> None:
    rows = [
        _entry(
            "nq-a",
            market="NQ",
            mechanism="BREAKOUT",
            qd_cell="q1",
            behavior="b1",
            stressed=100.0,
        ),
        _entry(
            "nq-b",
            market="NQ",
            mechanism="BREAKOUT",
            qd_cell="q2",
            behavior="b2",
            stressed=90.0,
        ),
        _entry(
            "ym-a",
            market="YM",
            mechanism="CROSS_ASSET",
            qd_cell="q3",
            behavior="b3",
            stressed=80.0,
        ),
        _entry(
            "cl-a",
            market="CL",
            mechanism="REVERSAL",
            qd_cell="q4",
            behavior="b4",
            stressed=70.0,
        ),
    ]

    selected = exact.select_quality_diverse_cohort(rows, maximum=3)

    assert {row["candidate_id"] for row in selected} == {"nq-a", "ym-a", "cl-a"}
    assert len({row["qd_cell"] for row in selected}) == 3
    assert len({row["realized_behavioral_fingerprint"] for row in selected}) == 3


def test_duplicate_candidate_with_different_exact_hashes_fails_closed() -> None:
    left = _entry(
        "same",
        market="NQ",
        mechanism="BREAKOUT",
        qd_cell="q1",
        behavior="b1",
        stressed=1.0,
    )
    right = {**left, "exact_hashes": {"decision_hash": "different"}}

    with pytest.raises(exact.AutonomousExactReplayError, match="evidence drift"):
        exact.select_quality_diverse_cohort([left, right], maximum=1)


def test_cohort_supports_deterministic_offset_and_explicit_ids() -> None:
    rows = [
        _entry(
            f"candidate-{index}",
            market=market,
            mechanism=f"M{index}",
            qd_cell=f"q{index}",
            behavior=f"b{index}",
            stressed=100.0 - index,
        )
        for index, market in enumerate(("NQ", "YM", "ES", "CL"), start=1)
    ]
    first = exact.select_quality_diverse_cohort(rows, maximum=2)
    second = exact.select_quality_diverse_cohort(rows, maximum=2, offset=2)
    explicit = exact.select_quality_diverse_cohort(
        rows,
        maximum=2,
        candidate_ids=("candidate-4", "candidate-2"),
    )

    assert not ({row["candidate_id"] for row in first} & {row["candidate_id"] for row in second})
    assert [row["candidate_id"] for row in explicit] == ["candidate-4", "candidate-2"]


def test_event_evidence_requires_compressed_and_content_hashes(tmp_path: Path) -> None:
    base = tmp_path / exact.DEFAULT_BANK_ROOT / "wave_01/stage2_event_evidence"
    base.mkdir(parents=True)
    path = base / "candidate.jsonl.gz"
    raw = b'{"event_id":"one"}\n'
    path.write_bytes(gzip.compress(raw, mtime=0))
    receipt = {
        "relative_path": "wave_01/stage2_event_evidence/candidate.jsonl.gz",
        "record_count": 1,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "uncompressed_sha256": hashlib.sha256(raw).hexdigest(),
    }

    rows, verified = exact._read_verified_event_evidence(tmp_path, receipt)
    assert rows == [{"event_id": "one"}]
    assert verified["record_count"] == 1

    path.write_bytes(path.read_bytes() + b"drift")
    with pytest.raises(exact.AutonomousExactReplayError, match="compressed SHA"):
        exact._read_verified_event_evidence(tmp_path, receipt)


def test_session_contract_fails_closed_after_mandatory_flatten() -> None:
    session_day = (date(2026, 7, 16) - date(1970, 1, 1)).days
    valid = TradePathEvent(
        event_id="valid",
        decision_ns=int(datetime(2026, 7, 16, 19, 59, tzinfo=UTC).timestamp() * 1e9),
        exit_ns=int(datetime(2026, 7, 16, 20, 9, tzinfo=UTC).timestamp() * 1e9),
        session_day=session_day,
        net_pnl=1.0,
        gross_pnl=2.0,
        worst_unrealized_pnl=-1.0,
        best_unrealized_pnl=2.0,
        quantity=1,
        mini_equivalent=0.1,
    )

    assert exact._event_session_compliant(valid) is True
    late_exit = replace(
        valid,
        event_id="late-exit",
        exit_ns=int(datetime(2026, 7, 16, 20, 11, tzinfo=UTC).timestamp() * 1e9),
    )
    after_close_entry = replace(
        valid,
        event_id="after-close-entry",
        decision_ns=int(datetime(2026, 7, 16, 20, 15, tzinfo=UTC).timestamp() * 1e9),
        exit_ns=int(datetime(2026, 7, 16, 20, 16, tzinfo=UTC).timestamp() * 1e9),
    )
    assert exact._event_session_compliant(late_exit) is False
    assert exact._event_session_compliant(after_close_entry) is False


def test_best_frontier_excludes_empty_or_noncompliant_cells() -> None:
    empty = {
        "candidate_id": "empty",
        "account_rule_compliant": True,
        "horizon_trading_days": 5,
        "account_size_usd": 50_000,
        "integer_quantity_tier": 1,
        "full_coverage_start_count": 0,
        "normal": {"episode_count": 0},
        "stressed": {"episode_count": 0},
    }
    noncompliant = {
        **empty,
        "candidate_id": "noncompliant",
        "account_rule_compliant": False,
        "normal": {"episode_count": 1, "pass_rate": 1.0},
        "stressed": {"episode_count": 1, "pass_rate": 1.0},
    }

    assert exact._best_frontier_point([empty, noncompliant]) is None


def test_exact_race_replays_both_scenarios_without_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = {
        "campaign_id": exact.SOURCE_CAMPAIGN_ID,
        "data": {},
        "evaluation_grid": {},
    }
    manifest["manifest_hash"] = stable_hash(manifest)
    manifest_path = tmp_path / "source_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    candidate_id = "candidate-exact"
    raw_event = json.dumps(
        {
            "event_id": "source",
            "fill_time_ns": 1,
            "quantity": 1,
            "risk_unit_price": 1.0,
        }
    ).encode("utf-8") + b"\n"
    event_path = (
        tmp_path
        / exact.DEFAULT_BANK_ROOT
        / "wave_01/stage2_event_evidence/candidate-exact.jsonl.gz"
    )
    event_path.parent.mkdir(parents=True)
    event_path.write_bytes(gzip.compress(raw_event, mtime=0))
    receipt = {
        "relative_path": "wave_01/stage2_event_evidence/candidate-exact.jsonl.gz",
        "record_count": 1,
        "sha256": hashlib.sha256(event_path.read_bytes()).hexdigest(),
        "uncompressed_sha256": hashlib.sha256(raw_event).hexdigest(),
    }
    entry = {
        **_entry(
            candidate_id,
            market="NQ",
            mechanism="BREAKOUT",
            qd_cell="qd-exact",
            behavior="behavior-exact",
            stressed=100.0,
        ),
        "candidate": {
            "market": "NQ",
            "execution_market": "MNQ",
            "mechanism": "BREAKOUT",
            "adverse_r": 1.0,
        },
        "eligible_session_days": list(range(100, 120)),
        "event_evidence": receipt,
        "exact_hashes": {"placeholder": "patched-reconstructor"},
    }
    bank_root = tmp_path / exact.DEFAULT_BANK_ROOT
    for wave, entries in ((1, [entry]), (2, [])):
        payload = {
            "schema": "hydra_fast_pass_qd_bank_v1",
            "campaign_id": exact.SOURCE_CAMPAIGN_ID,
            "wave": wave,
            "capacity": 150,
            "entries": entries,
            "bank_hash": stable_hash(entries),
        }
        path = bank_root / f"wave_{wave:02d}/causal_executable_bank.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    normal = _trajectory(candidate_id, scenario="NORMAL", net=100.0)
    stressed = _trajectory(candidate_id, scenario="STRESSED_1_5X", net=90.0)
    replay = SimpleNamespace(
        candidate=SimpleNamespace(
            candidate_id=candidate_id,
            structural_fingerprint=entry["candidate_fingerprint"],
        ),
        normal_trajectories=(normal,),
        stressed_trajectories=(stressed,),
        eligible_session_days=tuple(range(100, 120)),
        events=(
            SimpleNamespace(
                session_day=100,
                outcome=HazardOutcome.FAVORABLE_FIRST,
            ),
        ),
    )
    monkeypatch.setattr(exact, "reconstruct_exact_hazard_replay", lambda **_: replay)
    starts = {horizon: ((100, "B1"),) for horizon in exact.HORIZONS}
    monkeypatch.setattr(
        exact,
        "_load_frozen_grid",
        lambda *_: (
            tuple(range(100, 120)),
            starts,
            {
                "grid_hash": "fixture-grid",
                "account_calendar_session_count": 20,
                "headline_start_counts": {"5": 1, "10": 1, "20": 1},
                "feature_manifest_hashes": {},
            },
        ),
    )
    monkeypatch.setattr(
        exact,
        "_load_rule_snapshot",
        lambda *_: (_rules(), {"parsed_rule_hash": "fixture-rules"}),
    )
    before = {path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_file()}

    result = exact.run_exact_0029_account_size_race(
        tmp_path,
        cohort_maximum=1,
        integer_tiers=(1,),
        fast_pass_manifest_path=manifest_path,
        rule_snapshot_path=manifest_path,
    )

    after = {path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_file()}
    assert after == before
    assert result["status"] == "COMPLETE_EXACT_CAUSAL_ACCOUNT_SIZE_RACE"
    assert result["counters"]["exact_account_replays"] == 36
    assert result["counters"]["exact_normal_account_replays"] == 18
    assert result["counters"]["exact_stressed_account_replays"] == 18
    assert result["counters"]["summary_scaled_episode_screens"] == 0
    assert result["counters"]["promotion_count"] == 0
    assert result["best_exact_frontier_point"]["candidate_id"] == candidate_id
    assert all(
        cell["normal"]["episode_path_hash"]
        and cell["stressed"]["episode_path_hash"]
        for cell in result["results"][0]["frontier"]
    )
    hashed = dict(result)
    hashed.pop("result_hash")
    hashed.pop("runtime_seconds")
    assert stable_hash(hashed) == result["result_hash"]
    assert all(
        cell["normal"]["size_reduced_count"] == 0
        and cell["stressed"]["size_reduced_count"] == 0
        for cell in result["results"][0]["frontier"]
    )


def _trajectory(candidate_id: str, *, scenario: str, net: float) -> CausalTradeTrajectory:
    decision_ns = int((100 * 86_400 + 18 * 3_600) * 1_000_000_000)
    exit_ns = decision_ns + 1_000_000_000
    event = TradePathEvent(
        event_id=f"event:{scenario}",
        decision_ns=decision_ns,
        exit_ns=exit_ns,
        session_day=100,
        net_pnl=net,
        gross_pnl=net + 1.0,
        worst_unrealized_pnl=-20.0,
        best_unrealized_pnl=120.0,
        quantity=1,
        mini_equivalent=0.1,
    )
    return CausalTradeTrajectory(
        component_id=candidate_id,
        market="NQ",
        side=1,
        event=event,
        marks=(
            CausalTradeMark(
                availability_time_ns=decision_ns + 500_000_000,
                worst_unrealized_pnl=-20.0,
                best_unrealized_pnl=50.0,
                current_unrealized_pnl=10.0,
            ),
            CausalTradeMark(
                availability_time_ns=exit_ns,
                worst_unrealized_pnl=-20.0,
                best_unrealized_pnl=120.0,
                current_unrealized_pnl=net,
            ),
        ),
    )


def _rules() -> dict[str, dict[str, object]]:
    return {
        "50K": _rule("50K", 50_000, 3_000, 2_000, 5, 1_000),
        "100K": _rule("100K", 100_000, 6_000, 3_000, 10, 2_000),
        "150K": _rule("150K", 150_000, 9_000, 4_500, 15, 3_000),
    }


def _rule(
    label: str, size: int, target: int, mll: int, maximum_mini: int, dll: int
) -> dict[str, object]:
    return {
        "account_label": label,
        "account_size_usd": size,
        "profit_target_usd": target,
        "maximum_loss_limit_usd": mll,
        "maximum_mini_contracts": maximum_mini,
        "consistency_target_fraction": 0.5,
        "minimum_trading_days": 2,
        "optional_daily_loss_limit_usd": dll,
        "special_contract_caps": {},
    }
