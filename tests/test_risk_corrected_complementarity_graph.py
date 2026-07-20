from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.production.risk_corrected_complementarity_graph import (
    DEFAULT_MANIFEST,
    RiskCorrectedComplementarityError,
    _clusters,
    _load_manifest,
    _pearson,
    _policy,
    verify_result,
)


ROOT = Path(__file__).resolve().parents[1]
RESULT = Path(
    "reports/economic_evolution/risk_corrected_complementarity_graph_v1/"
    "economic_result.json"
)


def test_manifest_is_self_hashed_and_forbids_epsilon() -> None:
    manifest = _load_manifest(ROOT / DEFAULT_MANIFEST)
    assert manifest["selection_contract"]["design_blocks"] == ["B1", "B2"]
    assert manifest["selection_contract"]["b3_b4_used_for_membership"] is False
    assert manifest["governor_contract"]["epsilon_charge_forbidden"] is True
    assert manifest["governor_contract"]["risk_charge_derivation"] == (
        "MAX_CAUSAL_DECLARED_STOP_RISK_PER_MINI"
    )


def test_path_similarity_and_clustering_are_deterministic() -> None:
    assert _pearson([1.0, 2.0, 3.0], [2.0, 4.0, 6.0]) == pytest.approx(1.0)
    assert _pearson([1.0, 2.0, 3.0], [6.0, 4.0, 2.0]) == pytest.approx(-1.0)
    clusters = _clusters(
        ["a", "b", "c"],
        {("a", "b"): 0.95, ("a", "c"): 0.1, ("b", "c"): 0.1},
        0.9,
    )
    assert clusters["a"] == clusters["b"]
    assert clusters["a"] != clusters["c"]


def test_policy_fails_closed_on_epsilon_component() -> None:
    class Component:
        account_label = "50K"
        declared_risk_charge_per_mini = 1e-6

    class Context:
        components = {"a": Component(), "b": Component()}
        rules = {
            "50K": {
                "profit_target_usd": 3000.0,
                "maximum_loss_limit_usd": 2000.0,
                "maximum_mini_contracts": 5,
                "consistency_target_fraction": 0.5,
            }
        }

    manifest = _load_manifest(ROOT / DEFAULT_MANIFEST)
    with pytest.raises(RiskCorrectedComplementarityError, match="epsilon"):
        _policy(
            Context(),
            {"policy_id": "x", "account_label": "50K", "component_ids": ["a", "b"]},
            manifest,
        )


def test_sealed_result_has_no_side_effect_and_no_epsilon() -> None:
    if not (ROOT / RESULT).exists():
        pytest.skip("economic replay has not sealed its result yet")
    value = json.loads((ROOT / RESULT).read_text(encoding="utf-8"))
    verified = verify_result(value)
    assert verified["source_inventory"]["tier_q_candidate_count"] == 24
    assert verified["source_inventory"]["pass_observed_policy_count"] == 50
    assert verified["risk_charge_contract"]["epsilon_fallback_used"] is False
    assert min(verified["risk_charge_contract"]["candidate_charges"].values()) >= 1.0
    quarantine = verified["epsilon_status_quarantine"]
    assert quarantine["scope"] == (
        "THREE_LAYER_LEGACY_EPSILON_RISK_EVIDENCE_RECONCILIATION"
    )
    assert quarantine["layer_status_count"] == 13
    assert quarantine["unique_candidate_count"] == 10
    assert quarantine["unique_policy_count"] == 12
    assert quarantine["source_artifacts_mutated"] is False
    layers = quarantine["layers"]
    assert layers["exact_candidate_cells"]["status_count"] == 6
    assert layers["marginal_book_policies"]["status_count"] == 6
    assert layers["tier_g_books"]["status_count"] == 1
    assert set(layers["exact_candidate_cells"]["candidate_ids"]) == {
        "hazard_020ae195ccef8e39b1907e38",
        "hazard_1f3ff3e1f6d2b9d5e8eec1b3",
        "hazard_0a569f580a2540474116636c",
        "hazard_01340d634a288f435a06760c",
        "hazard_16a744e747cafb88a7e2c83b",
        "hazard_2afe13b4c912d4aa7f238626",
    }
    assert layers["tier_g_books"]["candidate_ids"] == [
        "hazard_020ae195ccef8e39b1907e38"
    ]
    assert quarantine["quarantined_not_replayed_candidate_ids"] == [
        "hazard_16a744e747cafb88a7e2c83b",
        "hazard_1f3ff3e1f6d2b9d5e8eec1b3",
    ]
    assert set(quarantine["risk_corrected_replay_candidate_ids"]).isdisjoint(
        quarantine["quarantined_not_replayed_candidate_ids"]
    )
    assert quarantine["old_promotional_evidence_status"] == (
        "QUARANTINED_NOT_DELETED_NOT_INHERITED"
    )
    assert verified["selection_contract"]["selection_completed_before_held_out_replay"] is True
