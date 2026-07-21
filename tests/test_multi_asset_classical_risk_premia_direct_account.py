from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from hydra.economic_evolution.schema import stable_hash
from hydra.research.multi_asset_classical_risk_premia_direct_account import (
    BRANCH_ID,
    Policy,
    _config,
    frozen_policies,
    policy_events,
)


def test_frozen_lattice_is_bounded_and_unique() -> None:
    policies = frozen_policies()
    assert len(policies) == 32
    assert len({policy.policy_id for policy in policies}) == 32
    assert {policy.family for policy in policies} == {
        "TIME_SERIES_TREND",
        "CROSS_SECTIONAL_STRENGTH",
        "TREND_STRENGTH_BLEND",
    }


def test_account_snapshots_keep_mll_and_contract_limits_hard() -> None:
    expected = {"50K": (3000.0, 2000.0, 5.0), "100K": (6000.0, 3000.0, 10.0), "150K": (9000.0, 4500.0, 15.0)}
    for name, values in expected.items():
        config, maximum = _config(name)
        assert (config.combine_profit_target, config.combine_max_loss_limit, maximum) == values
        assert config.minimum_pass_days == 2
        assert not config.use_optional_daily_loss_limit


def test_next_session_event_never_precedes_decision() -> None:
    rows = []
    symbols = ("M2K", "MYM", "MGC", "MCL", "ZN")
    for index, day in enumerate(range(20230103, 20230228)):
        # Invalid calendar integers are harmless here because policy_events
        # only needs an ordered causal session key for this unit contract.
        if str(day)[-2:] > "28":
            continue
        for offset, symbol in enumerate(symbols):
            price = 100.0 + index + offset
            rows.append(
                {
                    "session_day": day,
                    "symbol": symbol,
                    "entry_ns": index * 1_000_000 + offset,
                    "exit_ns": index * 1_000_000 + 500_000 + offset,
                    "entry": price,
                    "exit": price + 1.0,
                    "high": price + 1.5,
                    "low": price - 0.5,
                    "vol_20": 0.01,
                    "ret_20": 0.20 if offset % 2 == 0 else -0.20,
                    "ret_60": 0.20 if offset % 2 == 0 else -0.20,
                    "ret_120": 0.20 if offset % 2 == 0 else -0.20,
                }
            )
    events = policy_events(
        Policy("TIME_SERIES_TREND", 20, 2, 0.10),
        pd.DataFrame(rows),
        maximum_loss_limit=4500.0,
        maximum_mini_equivalent=15.0,
        stressed=False,
    )
    assert events
    assert all(event.decision_ns < event.exit_ns for event in events)
    assert all(event.contract_limit_compliant for event in events)


def test_persisted_result_reconciles_when_present() -> None:
    path = Path("reports/research_tripwires/multi_asset_classical_risk_premia_direct_account_v1/economic_result.json")
    if not path.exists():
        return
    result = json.loads(path.read_text(encoding="utf-8"))
    core = dict(result)
    claimed = core.pop("result_hash")
    assert claimed == stable_hash(core)
    assert result["branch_id"] == BRANCH_ID
    assert result["policy_count"] == 32
    assert result["governance"]["q4_access_count_delta"] == 0
