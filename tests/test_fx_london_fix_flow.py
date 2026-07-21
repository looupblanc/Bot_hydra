from __future__ import annotations

import json

from hydra.research.fx_london_fix_flow import (
    _write_state,
    frozen_policies,
)


def test_frozen_policy_lattice_is_complete_and_unique() -> None:
    policies = frozen_policies()

    assert len(policies) == 128
    assert len({policy.policy_id for policy in policies}) == 128
    assert sum(policy.mechanism == "PRE_FIX_CONTINUATION" for policy in policies) == 96
    assert sum(policy.mechanism == "POST_FIX_INVENTORY_UNWIND" for policy in policies) == 32
    assert {
        policy.exit_parameter
        for policy in policies
        if policy.mechanism == "POST_FIX_INVENTORY_UNWIND"
    } == {"HOLD_15M", "HOLD_30M", "HOLD_60M"}


def test_production_state_advances_atomically(tmp_path) -> None:
    destination = tmp_path / "production_state.json"

    _write_state(destination, {"status": "RAW_LOAD_ACTIVE", "pid": 123})
    _write_state(destination, {"status": "COMPLETE", "verdict": "FALSIFIED"})

    assert json.loads(destination.read_text(encoding="utf-8")) == {
        "status": "COMPLETE",
        "verdict": "FALSIFIED",
    }
