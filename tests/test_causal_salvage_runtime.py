from __future__ import annotations

import json
from pathlib import Path
from copy import deepcopy

import pytest

from hydra.production.causal_salvage_runtime import (
    CausalSalvageRuntimeError,
    _select_stage,
    _standalone_policy,
    _validate_manifest,
)


def _summary(*, net: float, passes: int) -> dict[str, object]:
    return {
        "net_total": net,
        "pass_count": passes,
        "pass_block_count": min(passes, 2),
        "target_progress_p25": 0.25,
        "target_progress_median": 0.50,
        "mll_breach_rate": 0.0,
        "minimum_mll_buffer": 4_000.0,
        "maximum_single_block_pass_share": 0.5 if passes else 1.0,
        "maximum_single_sleeve_profit_share": 0.5,
        "maximum_best_day_profit_share": 0.25,
        "accepted_event_count": 10,
    }


def _row(policy_id: str, behavior: str, *, net: float) -> dict[str, object]:
    return {
        "policy_id": policy_id,
        "behavior_fingerprint": behavior,
        "scenarios": {
            "NORMAL": {"90_TRADING_DAYS": _summary(net=net, passes=1)},
            "STRESSED_1_5X": {
                "90_TRADING_DAYS": _summary(net=net - 1.0, passes=1)
            },
        },
    }


def test_select_stage_treats_halving_size_as_a_maximum() -> None:
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads(
        (root / "config/v7/causal_salvage_sprint_0027.json").read_text(
            encoding="utf-8"
        )
    )
    policies = {
        policy.policy_id: policy
        for policy in (
            _standalone_policy("component-a"),
            _standalone_policy("component-b"),
            _standalone_policy("component-c"),
        )
    }
    rows = [
        _row("causal-standalone:component-a", "behavior-1", net=30.0),
        _row("causal-standalone:component-b", "behavior-1", net=20.0),
        _row("causal-standalone:component-c", "behavior-2", net=10.0),
    ]

    selected, receipt = _select_stage(
        rows,
        policies,
        limit=512,
        manifest=manifest,
        final_stage=False,
    )

    assert len(selected) == 2
    assert receipt["output_limit"] == 512
    assert receipt["distinct_behavior_count"] == 2
    assert receipt["effective_output_target"] == 2
    assert receipt["selected_count"] == 2


def test_manifest_binds_horizon_resolved_frozen_v7_costs() -> None:
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads(
        (root / "config/v7/causal_salvage_sprint_0027.json").read_text(
            encoding="utf-8"
        )
    )
    _validate_manifest(manifest)

    drifted = deepcopy(manifest)
    drifted["causal_repairs"]["fill_policy"][
        "normal_slippage_ticks_per_side_by_current_horizon"
    ]["60m"] = 1.0
    with pytest.raises(CausalSalvageRuntimeError, match="cost contract drift"):
        _validate_manifest(drifted)
