from __future__ import annotations

import json
from pathlib import Path

import pytest
import pandas as pd

from hydra.economic_evolution.schema import stable_hash
from hydra.production import tier_q_2026_two_stage_runner as runner


def _contract() -> dict:
    core = {
        "schema": runner.CONTRACT_SCHEMA,
        "status": "FROZEN_AWAITING_ACQUISITION",
        "promotion_order": ["Q", "G", "C"],
        "outcome_accessed_at_freeze": False,
        "candidate_cohort": [{"candidate_id": "candidate_a"}],
        "temporal_roles": [
            {
                "role": runner.FINAL_DEVELOPMENT,
                "start": "2026-01-01",
                "end": "2026-05-01",
                "retuning_allowed": False,
            },
            {
                "role": runner.CONFIRMATION,
                "start": "2026-05-01",
                "end": "2026-07-19",
                "retuning_allowed": False,
            },
        ],
    }
    return {**core, "contract_hash": stable_hash(core)}


def _summary(*, passes: int, net: float, blocks: tuple[str, ...]) -> dict:
    return {
        "pass_count": passes,
        "net_total_usd": net,
        "mll_breach_rate": 0.0,
        "episode_path_hash": stable_hash([passes, net, blocks]),
        "passing_paths_consistency_compliant": passes > 0,
        "by_block": {
            block: {"pass_count": 1 if index < passes else 0}
            for index, block in enumerate(blocks)
        },
    }


def _concentration(*, share: float = 0.25) -> dict:
    return {
        "cleared": share <= 0.5,
        "worst_case_maximums": {
            "maximum_single_day_profit_share": share,
            "maximum_single_trade_profit_share": share,
            "maximum_single_event_profit_share": share,
        },
    }


def test_tier_g_gate_requires_passes_in_two_contexts() -> None:
    thresholds = {
        "minimum_normal_passes": 2,
        "minimum_stressed_passes": 2,
        "minimum_positive_temporal_contexts": 2,
        "maximum_stressed_mll_breach_rate": 0.1,
        "single_trade_or_day_profit_share_maximum": 0.5,
        "no_retuning": True,
    }
    normal = _summary(passes=2, net=1000.0, blocks=("FD_A", "FD_B"))
    stressed = _summary(passes=2, net=500.0, blocks=("FD_A", "FD_B"))
    result = runner.tier_g_gate(normal, stressed, _concentration(), thresholds)
    assert result["passed"] is True
    assert result["resulting_tier"] == "G"

    one_context = _summary(passes=2, net=500.0, blocks=("FD_A", "FD_A_COPY"))
    one_context["by_block"]["FD_A_COPY"]["pass_count"] = 0
    failed = runner.tier_g_gate(normal, one_context, _concentration(), thresholds)
    assert failed["passed"] is False
    assert failed["checks"]["minimum_positive_temporal_contexts"] is False


def test_tier_c_gate_is_prior_g_and_one_shot_concentration_bound() -> None:
    thresholds = {
        "minimum_normal_passes": 1,
        "minimum_stressed_passes": 1,
        "maximum_stressed_mll_breach_rate": 0.1,
        "single_trade_or_day_profit_share_maximum": 0.5,
        "no_retuning": True,
    }
    normal = _summary(passes=1, net=500.0, blocks=("C",))
    stressed = _summary(passes=1, net=250.0, blocks=("C",))
    passed = runner.tier_c_gate(
        normal,
        stressed,
        _concentration(),
        thresholds,
        prior_tier_g=True,
        selected_horizon_matches=True,
        full_coverage_start_count=2,
    )
    assert passed["passed"] is True
    assert passed["resulting_tier"] == "C"

    failed = runner.tier_c_gate(
        normal,
        stressed,
        _concentration(share=0.75),
        thresholds,
        prior_tier_g=True,
        selected_horizon_matches=True,
        full_coverage_start_count=2,
    )
    assert failed["passed"] is False
    assert failed["checks"]["concentration_complete_and_controlled"] is False


def test_confirmation_stays_sealed_until_self_hashed_tier_g_result() -> None:
    contract = _contract()
    try:
        runner._authorized_confirmation_ids(contract, None)
    except runner.TierQTwoStageError as exc:
        assert "sealed" in str(exc)
    else:  # pragma: no cover - explicit fail-closed assertion
        raise AssertionError("confirmation opened without final development")

    candidate = {
        "candidate_id": "candidate_a",
        "promotion_gate": {"passed": True},
        "resulting_evidence_tier": "G",
    }
    core = {
        "schema": runner.STAGE_SCHEMA,
        "role": runner.FINAL_DEVELOPMENT,
        "contract_hash": contract["contract_hash"],
        "candidate_results": [candidate],
        "tier_g_candidate_ids": ["candidate_a"],
        "retuning_performed": False,
        "recalibration_performed": False,
    }
    result = {**core, "result_hash": stable_hash(core)}
    assert runner._authorized_confirmation_ids(contract, result) == ("candidate_a",)

    drift = dict(result)
    drift["tier_g_candidate_ids"] = []
    try:
        runner._authorized_confirmation_ids(contract, drift)
    except runner.TierQTwoStageError as exc:
        assert "receipt drift" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("mutated final-development result authorized confirmation")


def test_non_overlapping_starts_never_cross_frozen_blocks() -> None:
    jan_1 = runner._date_ns("2026-01-01") // runner.DAY_NS
    days = tuple(jan_1 + value for value in range(12))
    starts = runner._non_overlapping_role_starts(
        days,
        blocks=(
            {"block_id": "A", "start": "2026-01-01", "end": "2026-01-07"},
            {"block_id": "B", "start": "2026-01-07", "end": "2026-01-13"},
        ),
        horizon=3,
    )
    assert starts == (
        (jan_1, "A"),
        (jan_1 + 3, "A"),
        (jan_1 + 6, "B"),
        (jan_1 + 9, "B"),
    )


def test_real_receipt_final_development_never_reads_sealed_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[1]
    contract = json.loads(
        (root / "config/research/tier_q_2026_two_stage_confirmation_v1.json").read_text()
    )
    receipt = json.loads(
        (root / "reports/data_access/tier_q_2026_acquisition_receipt.json").read_text()
    )
    assert receipt["receipt_hash"] == (
        "6e04534226a7e60c408db9576091c75a787d5428f440bed355cb9d7485093607"
    )
    original = runner._sha256
    opened: list[str] = []

    def guarded(path: Path) -> str:
        opened.append(str(path))
        if path.name == "confirmation_2026_sealed.parquet":
            raise AssertionError("sealed confirmation artifact was opened before Tier G")
        return original(path)

    monkeypatch.setattr(runner, "_sha256", guarded)
    verified = runner.verify_acquisition_receipt(
        contract, receipt, open_role=runner.FINAL_DEVELOPMENT
    )
    assert "SEALED_CONFIRMATION_PARQUET" in verified["_inventory"]
    assert not any("confirmation_2026_sealed.parquet" in value for value in opened)
    assert any("final_development_2026.parquet" in value for value in opened)


def test_real_final_development_coverage_excludes_partial_split_session() -> None:
    root = Path(__file__).resolve().parents[1]
    frame = pd.read_parquet(
        root
        / "data/cache/databento/tier_q_2026_confirmation/97a80942156d15b9801d/"
        "final_development_2026.parquet"
    )
    audit = runner.audit_final_development_coverage(frame)
    assert audit["raw_row_count"] == 1_136_722
    assert audit["true_trading_day_count"] == 85
    assert audit["partial_split_session"]["session_day"] == "2026-05-01"
    assert audit["partial_split_session"]["eligible_as_episode_start"] is False
    assert audit["good_friday_energy"]["CL_rows"] == 0
    assert audit["good_friday_energy"]["MCL_rows"] == 0
    assert audit["maximum_theoretical_non_overlapping_starts_before_warmup"] == {
        "5": 17,
        "10": 8,
        "20": 4,
    }
