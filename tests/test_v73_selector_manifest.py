from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from hydra.selection.selector_manifest import (
    BASELINES,
    HARD_REQUIREMENTS,
    PARETO_OBJECTIVES,
    SCHEMA,
    SelectorManifestError,
    finalize_manifest,
    load_and_verify_selector_manifest,
    stable_hash,
    validate_manifest,
)


def _block(index: int) -> dict[str, object]:
    month = 1 + index * 2
    return {
        "block_id": f"block_{index + 1}",
        "start_date": f"2024-{month:02d}-01",
        "end_date_exclusive": f"2024-{month + 1:02d}-01",
        "contracts": [f"ES{index + 1}", f"MES{index + 1}"],
        "contract_and_roll_separation": {
            "contract_identity_recorded_by_market_and_session": True,
            "roll_transition_dates_explicit": True,
            "unsafe_roll_windows_explicit": True,
            "roll_boundaries_not_used_to_invent_independence": True,
            "block_may_span_contracts": False,
        },
        "event_count": 120 + index,
        "trading_days": 20,
        "markets": ["ES", "MES"],
        "sessions": ["CME_GLOBEX", "RTH"],
        "volatility_regime": f"REGIME_{index + 1}",
        "episode_starts": [
            f"2024-{month:02d}-05T14:30:00Z",
            f"2024-{month:02d}-12T14:30:00Z",
        ],
        "episode_starts_unique_across_blocks": True,
        "inference_unit": "TEMPORAL_BLOCK",
        "within_block_starts_independent": False,
        "overlapping_episode_starts_counted_as_independent": False,
        "primary_horizon_complete_for_every_start": True,
        "contamination_history": [],
        "provenance": {
            "trade_ledger": f"immutable/trades/block_{index + 1}.jsonl",
            "trade_ledger_sha256": f"{index + 1}" * 64,
        },
    }


def _payload() -> dict[str, object]:
    return {
        "schema": SCHEMA,
        "experiment_id": "hydra_v7_3_nested_selector_0001",
        "temporal_blocks": [_block(index) for index in range(4)],
        "outer_crossfit": {
            "method": "LEAVE_ONE_BLOCK_OUT",
            "held_out_each_block_exactly_once": True,
            "candidate_evidence_design_only": True,
            "primary_champions_per_fold": 1,
            "maximum_backups_per_fold": 1,
            "no_retuning_after_heldout": True,
            "headline_evidence_heldout_only": True,
        },
        "frozen_horizons": [20, 40, 60, 90, "full"],
        "risk_tiers": [0.75, 1.0, 1.15, 1.3, 1.45],
        "risk_selected_inside_design_set": True,
        "static_risk_only": True,
        "pareto_objectives": [
            {"metric": metric, "direction": direction}
            for metric, direction in PARETO_OBJECTIVES
        ],
        "hard_requirements": {name: True for name in HARD_REQUIREMENTS},
        "baselines": list(BASELINES),
        "random_selection_seeds": [7301, 7302, 7303, 7304],
        "decision_thresholds": {
            "SELECTOR_PROCEDURE_GREEN": {
                "minimum_aggregate_held_out_combine_passes": 3,
                "minimum_blocks_with_passes": 2,
                "minimum_positive_economic_blocks": 3,
                "positive_aggregate_stressed_net_required": True,
                "normal_and_stressed_improvement_over_best_parent_required": True,
                "held_out_target_progress_improvement_over_best_parent_required": True,
                "maximum_held_out_mll_breach_rate": 0.10,
                "acceptable_consistency_required": True,
                "improvement_over_equal_risk_required": True,
                "stronger_than_random_selection_required": True,
                "maximum_single_block_pass_share": 0.50,
                "maximum_single_component_profit_share": 0.65,
            },
            "SELECTOR_PROCEDURE_WEAK": {
                "green_requirements_not_met": True,
                "minimum_positive_economic_blocks": 1,
                "any_held_out_improvement_signal_required": True,
            },
            "SELECTOR_PROCEDURE_FALSIFIED": {
                "green_requirements_not_met": True,
                "weak_requirements_not_met": True,
                "terminate_static_basket_synthesis": True,
            },
        },
        "governance": {
            "q4_access_allowed": False,
            "new_data_purchase_allowed": False,
            "broker_or_orders_allowed": False,
        },
        "compute_workers": 3,
        "selector_frozen_before_heldout": True,
    }


def test_finalize_is_stable_idempotent_and_does_not_mutate_input() -> None:
    payload = _payload()
    original = copy.deepcopy(payload)

    first = finalize_manifest(payload)
    second = finalize_manifest(first)

    assert payload == original
    assert first == second
    unhashed = dict(first)
    claimed = unhashed.pop("manifest_hash")
    assert claimed == stable_hash(unhashed)


def test_load_and_verify_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "selector_manifest.json"
    finalized = finalize_manifest(_payload())
    path.write_text(json.dumps(finalized, sort_keys=True), encoding="utf-8")

    assert load_and_verify_selector_manifest(path) == finalized
    validate_manifest(finalized)


def test_load_rejects_unfinalized_and_hash_drift(tmp_path: Path) -> None:
    path = tmp_path / "selector_manifest.json"
    path.write_text(json.dumps(_payload()), encoding="utf-8")
    with pytest.raises(SelectorManifestError, match="not finalized"):
        load_and_verify_selector_manifest(path)

    finalized = finalize_manifest(_payload())
    finalized["compute_workers"] = 4
    path.write_text(json.dumps(finalized), encoding="utf-8")
    with pytest.raises(SelectorManifestError):
        load_and_verify_selector_manifest(path)


def test_temporal_blocks_require_four_chronological_independent_ranges() -> None:
    too_few = _payload()
    too_few["temporal_blocks"] = too_few["temporal_blocks"][:3]
    with pytest.raises(SelectorManifestError, match="four temporal blocks"):
        finalize_manifest(too_few)

    overlap = _payload()
    overlap["temporal_blocks"][1]["start_date"] = "2024-01-20"
    overlap["temporal_blocks"][1]["episode_starts"] = [
        "2024-01-21T14:30:00Z",
        "2024-01-22T14:30:00Z",
    ]
    with pytest.raises(SelectorManifestError, match="nonoverlapping"):
        finalize_manifest(overlap)

    reused = _payload()
    reused["temporal_blocks"][1]["episode_starts"] = list(
        reused["temporal_blocks"][0]["episode_starts"]
    )
    with pytest.raises(SelectorManifestError):
        finalize_manifest(reused)


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        (("frozen_horizons",), [20, 40, 60, "full"]),
        (("risk_selected_inside_design_set",), False),
        (("static_risk_only",), False),
        (("compute_workers",), 2),
        (("selector_frozen_before_heldout",), False),
        (("governance", "q4_access_allowed"), True),
        (("governance", "new_data_purchase_allowed"), True),
        (("governance", "broker_or_orders_allowed"), True),
        (
            (
                "decision_thresholds",
                "SELECTOR_PROCEDURE_GREEN",
                "minimum_aggregate_held_out_combine_passes",
            ),
            2,
        ),
        (
            (
                "decision_thresholds",
                "SELECTOR_PROCEDURE_GREEN",
                "maximum_held_out_mll_breach_rate",
            ),
            0.11,
        ),
        (
            (
                "decision_thresholds",
                "SELECTOR_PROCEDURE_GREEN",
                "maximum_single_component_profit_share",
            ),
            0.650001,
        ),
    ],
)
def test_frozen_policy_drift_is_rejected(
    path: tuple[str, ...], replacement: object
) -> None:
    payload = _payload()
    target = payload
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = replacement

    with pytest.raises(SelectorManifestError):
        finalize_manifest(payload)


def test_pareto_policy_rejects_weights_and_direction_drift() -> None:
    weighted = _payload()
    weighted["pareto_objectives"][0]["weight"] = 1.0
    with pytest.raises(SelectorManifestError, match="only metric and direction"):
        finalize_manifest(weighted)

    reversed_direction = _payload()
    reversed_direction["pareto_objectives"][5]["direction"] = "MAXIMIZE"
    with pytest.raises(SelectorManifestError, match="direction drift"):
        finalize_manifest(reversed_direction)


def test_all_baselines_and_fixed_random_seeds_are_required() -> None:
    missing_baseline = _payload()
    missing_baseline["baselines"] = missing_baseline["baselines"][:-1]
    with pytest.raises(SelectorManifestError, match="baseline policy drift"):
        finalize_manifest(missing_baseline)

    duplicate_seed = _payload()
    duplicate_seed["random_selection_seeds"] = [7301, 7301, 7303, 7304]
    with pytest.raises(SelectorManifestError, match="random-selection seeds"):
        finalize_manifest(duplicate_seed)
