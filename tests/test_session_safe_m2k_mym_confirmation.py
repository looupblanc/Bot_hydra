from __future__ import annotations

import copy
from pathlib import Path

import pytest

from hydra.production import session_safe_m2k_mym_confirmation as confirmation
from hydra.production.fresh_confirmation_lane import _date_ns, _open_bound_matrix
from hydra.research.causal_target_velocity import (
    HazardCandidate,
    discover_intents_batch,
    discover_intents_streaming,
    with_availability_safe_cross_asset_feature,
)


ROOT = Path(__file__).resolve().parents[1]


def test_frozen_card_is_self_hashed_untouched_and_non_promotional() -> None:
    card = confirmation.load_decision_card(ROOT / confirmation.DEFAULT_CARD)
    assert card["untouched_audit"]["period_is_unviewed"] is True
    assert card["untouched_audit"]["target_period_overlap_count"] == 0
    assert card["data_request"]["request_start_inclusive"] == "2021-12-20"
    assert card["data_request"]["confirmation_start_inclusive"] == "2022-01-03"
    assert card["data_request"]["end_exclusive"] == "2023-01-01"
    assert [row["candidate"]["execution_market"] for row in card["components"]] == [
        "M2K",
        "MYM",
    ]
    assert card["source"]["source_evidence_tier"] == "E"
    assert card["gate"]["evidence_ceiling_on_pass"] == (
        "FRESH_REPLICATION_SUCCESS_TIER_Q_ELIGIBLE_NOT_TIER_C"
    )
    assert card["official_cost_estimate"]["actual_spend_usd"] == 0.0


def test_card_mutation_fails_closed() -> None:
    card = confirmation.load_decision_card(ROOT / confirmation.DEFAULT_CARD)
    changed = copy.deepcopy(card)
    changed["components"][0]["quality_tier"] = 1.5
    with pytest.raises(confirmation.SessionSafeConfirmationError):
        confirmation.load_decision_card_from_mapping(changed)


def test_gate_requires_pass_stress_net_mll_and_consistency() -> None:
    card = confirmation.load_decision_card(ROOT / confirmation.DEFAULT_CARD)
    passing = {
        "full_coverage_start_count": 12,
        "normal": {"pass_count": 1},
        "stressed": {
            "pass_count": 1,
            "net_total_usd": 1.0,
            "mll_breach_rate": 0.0,
            "consistency_compliance_rate": 1.0,
        },
    }
    result = confirmation.confirmation_gate(passing, card["gate"])
    assert result["passed"] is True
    assert result["evidence_ceiling"].endswith("NOT_TIER_C")
    failing = copy.deepcopy(passing)
    failing["stressed"]["net_total_usd"] = -0.01
    assert confirmation.confirmation_gate(failing, card["gate"])["passed"] is False


def test_frozen_decision_path_reproduces_source_signal_inventory() -> None:
    """Prove the fresh replay is technically reconstructible before purchase."""

    import json

    card = confirmation.load_decision_card(ROOT / confirmation.DEFAULT_CARD)
    manifest = json.loads(
        (ROOT / "config/v7/fast_pass_factory_0029_revision_05.json").read_text()
    )
    bindings = manifest["data"]["feature_matrix_bindings"]
    matrices = {
        market: _open_bound_matrix(ROOT, bindings, market)
        for market in ("RTY", "YM", "ES")
    }
    expected = {
        "hazard_1478619b60a10a3b9bccef4f": 115,
        "hazard_19327ab34a21d623c654a6cc": 469,
    }
    for row in card["components"]:
        candidate = HazardCandidate(**row["candidate"])
        matrix = matrices[candidate.market]
        if candidate.cross_asset_reference_market:
            matrix = with_availability_safe_cross_asset_feature(
                matrix, matrices[candidate.cross_asset_reference_market]
            )
        calibrated = confirmation._calibrated(candidate, row["calibration"])
        kwargs = {
            "evaluation_start_ns": _date_ns(
                manifest["data"]["evaluation_start_inclusive"]
            ),
            "evaluation_end_exclusive_ns": _date_ns(
                manifest["data"]["evaluation_end_exclusive"]
            ),
        }
        batch = discover_intents_batch(calibrated, matrix, **kwargs)
        streaming = discover_intents_streaming(calibrated, matrix, **kwargs)
        assert len(batch) == expected[candidate.candidate_id]
        assert tuple((item.row_index, item.direction) for item in batch) == streaming
