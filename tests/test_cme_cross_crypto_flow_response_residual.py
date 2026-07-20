from __future__ import annotations

import json
from pathlib import Path

from hydra.economic_evolution.schema import stable_hash
from scripts.acquire_cme_cross_crypto_flow_response_residual import (
    _read_manifest,
    _requests,
)


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config/research/cme_cross_crypto_flow_response_residual_v1.json"


def test_manifest_is_self_hashed_pre_q4_and_sequential() -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    claimed = payload.pop("manifest_hash")
    assert stable_hash(payload) == claimed
    data = payload["data_contract"]
    assert data["q4_2024_access"] is False
    assert data["tranche_a"]["end_exclusive"] == "2024-01-01"
    assert data["tranche_b_conditional"]["end_exclusive"] == "2024-10-01"
    assert data["tranche_b_conditional"]["acquire_only_after_validation_gate"] is True


def test_tranche_a_requests_and_crypto_contract_cap_are_frozen() -> None:
    manifest = _read_manifest(ROOT)
    requests = _requests(manifest)
    assert set(requests) == {"tbbo", "definition"}
    assert all(row["symbols"] == ["MBT.c.0", "MET.c.0"] for row in requests.values())
    assert all(row["end"] == "2024-01-01" for row in requests.values())
    assert manifest["candidate_lattice"]["proposal_count"] == 24
    assert manifest["account_contract"]["maximum_contracts_by_account"] == {
        "50K": 5,
        "100K": 10,
        "150K": 15,
    }


def test_frozen_costs_reconcile_and_remain_under_authority() -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    data = payload["data_contract"]
    for key in ("tranche_a", "tranche_b_conditional"):
        estimated = sum(
            row["estimated_cost_usd"] for row in data[key]["official_estimates"].values()
        )
        assert abs(estimated - data[key]["official_total_cost_usd"]) < 1e-12
    budget = payload["budget"]
    assert (
        budget["cumulative_actual_before_usd"]
        + data["tranche_a"]["official_total_cost_usd"]
        + data["tranche_b_conditional"]["official_total_cost_usd"]
        <= budget["authoritative_cumulative_cap_usd"]
    )
