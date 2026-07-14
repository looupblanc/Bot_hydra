from __future__ import annotations

import json
from dataclasses import replace

from hydra.economic_evolution.account_censored_horizon import (
    CENSORED_HORIZON_CLASS_ID,
    CONTROL_HORIZON_SESSIONS,
    DIAGNOSTIC_HORIZON_SESSIONS,
    CensoredHorizonPair,
)
from hydra.economic_evolution.account_elite_robustness import _parent_from_entry


def test_censored_horizon_pair_freezes_policy_behavior() -> None:
    manifest = json.loads(
        open(
            "WORM/economic-evolution-0018-canonical-elites-2026-07-14.json",
            encoding="utf-8",
        ).read()
    )
    parent = _parent_from_entry(manifest["policies"][0])
    child = replace(
        parent,
        policy_id="diagnostic-policy",
        exact_change=(("diagnostic_horizon_sessions", 90),),
    )
    pair = CensoredHorizonPair(
        pair_id="diagnostic-pair",
        parent_policy_id=parent.policy_id,
        mutation_family="CENSORED_HORIZON",
        failure_target="RESEARCH_HORIZON_CENSORING_VS_TARGET_VELOCITY",
        real_policy=child,
        matched_control_policy=parent,
    )
    assert pair.real_policy.structural_fingerprint == parent.structural_fingerprint
    assert pair.to_dict()["identical_policy_behavior"] is True
    assert pair.to_dict()["control_horizon_sessions"] == CONTROL_HORIZON_SESSIONS
    assert pair.to_dict()["diagnostic_horizon_sessions"] == DIAGNOSTIC_HORIZON_SESSIONS
    assert CENSORED_HORIZON_CLASS_ID.endswith("_V1")
