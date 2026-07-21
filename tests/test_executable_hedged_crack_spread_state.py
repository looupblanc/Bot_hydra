from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from hydra.economic_evolution.schema import stable_hash
from hydra.research.executable_hedged_crack_spread_state import (
    _cost,
    _delivery,
    _first_after,
    audit_inputs,
    frozen_specs,
)


MANIFEST = Path("config/research/executable_hedged_crack_spread_state_v1.json")


def _manifest() -> dict[str, object]:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def test_manifest_is_hashed_bounded_and_50k_fails_shared_contract_cap() -> None:
    manifest = _manifest()
    core = dict(manifest)
    claimed = core.pop("manifest_hash")
    assert stable_hash(core) == claimed
    specs = frozen_specs(manifest)
    assert len(specs) == 24
    assert len(set(specs)) == 24
    assert manifest["account_gate"]["shared_CL_QM_RB_HO_mini_caps"] == {
        "50K": 3,
        "100K": 6,
        "150K": 9,
    }
    assert all(
        int(row["gross_mini_contracts"]) > 3
        for row in manifest["physical_contract_ratios"].values()
    )


def test_fill_lookup_is_strictly_after_decision_and_never_forward_fills_contract() -> None:
    timestamps = pd.to_datetime(
        ["2024-01-02T14:00:00Z", "2024-01-02T14:02:00Z", "2024-01-02T14:03:00Z"]
    )
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "timestamp_ns": timestamps.astype("datetime64[ns, UTC]").array.asi8,
            "session_day": [20240102] * 3,
            "contract": ["RBF4", "RBF4", "RBG4"],
            "roll_unsafe": [False, False, False],
            "open": [1.0, 2.0, 3.0],
        }
    )
    fill = _first_after(
        frame,
        pd.Timestamp("2024-01-02T14:00:00Z"),
        contract="RBF4",
        session_day=20240102,
        deadline=pd.Timestamp("2024-01-02T14:02:00Z"),
    )
    assert fill is not None
    assert pd.Timestamp(fill["timestamp"]) == pd.Timestamp("2024-01-02T14:02:00Z")
    assert (
        _first_after(
            frame,
            pd.Timestamp("2024-01-02T14:02:00Z"),
            contract="RBF4",
            session_day=20240102,
            deadline=pd.Timestamp("2024-01-02T14:04:00Z"),
        )
        is None
    )


def test_contract_delivery_and_multi_leg_costs_are_explicit() -> None:
    assert _delivery(pd.Series(["RBF4", "RBG4"]), "RB").tolist() == ["F4", "G4"]
    assert _delivery(pd.Series(["BAD"]), "RB").isna().all()
    manifest = _manifest()
    balanced = _cost(manifest, "BALANCED_211", "normal")
    standard = _cost(manifest, "STANDARD_321", "normal")
    stressed = _cost(manifest, "STANDARD_321", "stressed")
    assert balanced["total_usd"] > 0.0
    assert standard["total_usd"] > balanced["total_usd"]
    assert stressed["total_usd"] > standard["total_usd"]


def test_authoritative_inputs_and_rule_caps_reconcile() -> None:
    audit = audit_inputs(".")
    assert audit["q4_access_count_delta"] == 0
    assert audit["broker_connections"] == 0
    assert audit["orders"] == 0
    assert audit["shared_contract_caps"] == {"50K": 3, "100K": 6, "150K": 9}
