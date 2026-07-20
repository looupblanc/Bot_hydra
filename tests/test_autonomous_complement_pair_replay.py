from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from hydra.production.autonomous_complement_pair_replay import (
    DEFAULT_MANIFEST,
    AutonomousComplementPairError,
    _load_manifest,
    _policy,
    _require_causal_risk_charges,
    _verify_preflight,
    verify_result,
)


ROOT = Path(__file__).resolve().parents[1]
PREFLIGHT = Path(
    "reports/economic_evolution/autonomous_complement_pair_replay_v1/"
    "preflight_receipt.json"
)
RESULT = Path(
    "reports/economic_evolution/autonomous_complement_pair_replay_v1/"
    "economic_result.json"
)


def _read(path: Path) -> dict[str, object]:
    value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_manifest_self_hash_and_causal_risk_receipts_are_frozen() -> None:
    manifest = _load_manifest(ROOT / DEFAULT_MANIFEST)
    risk = manifest["risk_charge_contract"]
    assert risk["derivation"] == "MAX_CAUSAL_DECLARED_STOP_RISK_PER_MINI"
    assert risk["statistic"] == "MAX_NOT_P99"
    assert risk["future_outcomes_used"] is False
    assert risk["stop_inputs_available_at_decision_time"] is True
    sources = risk["sources"]
    policy = manifest["governor"]
    assert set(sources) == set(policy["component_priority"])
    assert all(row["event_record_count"] > 0 for row in sources.values())
    assert all(len(row["event_file_sha256"]) == 64 for row in sources.values())
    assert policy["nominal_risk_charge_per_mini"] == {
        candidate_id: row["derived_max_risk_charge_per_mini"]
        for candidate_id, row in sources.items()
    }


def test_epsilon_risk_charge_is_rejected_fail_closed() -> None:
    manifest = _load_manifest(ROOT / DEFAULT_MANIFEST)
    policy = _policy(manifest)
    candidate_id = policy.component_priority[1]
    charges = list(policy.nominal_risk_charge_per_mini)
    charges[1] = (candidate_id, 1e-6)
    epsilon = replace(policy, nominal_risk_charge_per_mini=tuple(charges))
    derived = dict(policy.nominal_risk_charge_map)
    with pytest.raises(AutonomousComplementPairError, match="epsilon"):
        _require_causal_risk_charges(epsilon, derived)


def test_preflight_has_zero_external_side_effects() -> None:
    preflight = _verify_preflight(_read(PREFLIGHT))
    for field in (
        "confirmation_partition_reads",
        "q4_access_count_delta",
        "broker_connections",
        "orders",
        "registry_writes",
        "database_writes",
    ):
        assert preflight[field] == 0


def test_exact_pair_result_smoke_reconciles_hashes_and_coverage() -> None:
    result = verify_result(_read(RESULT))
    assert result["candidate_id"] == (
        "autonomous_complement_pair_11d0fa_1f3ff3_prudent_v1"
    )
    assert result["component_ids"] == [
        "hazard_11d0fa817a785d264adb2e96",
        "hazard_1f3ff3e1f6d2b9d5e8eec1b3",
    ]
    assert {
        horizon: result["summaries"]["NORMAL"][horizon]["episode_count"]
        for horizon in ("5", "10", "20")
    } == {"5": 41, "10": 18, "20": 7}
    assert result["summaries"]["STRESSED_1_5X"]["20"]["mll_breach_count"] == 0
    assert result["unique_trajectory_concentration"]["cleared"] is True
