from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.research.economic_evolution_campaign import (
    EconomicEvolutionCampaignError,
    _account_population,
    _load_and_validate_preregistration,
    _policy_from_dict,
    _sleeve_from_dict,
)


def test_campaign_rehydrates_frozen_seed_specs() -> None:
    root = Path(__file__).resolve().parents[1]
    archive = json.loads(
        (root / "reports/economic_evolution/pilot_0001/seed_archive.json").read_text()
    )
    sleeve = _sleeve_from_dict(archive["sleeves"][0]["specification"])
    policy = _policy_from_dict(archive["policies"][0]["policy"])

    assert sleeve.inherited_status is None
    assert policy.inherited_status is None
    assert sleeve.structural_fingerprint == archive["sleeves"][0]["specification"]["structural_fingerprint"]
    assert policy.structural_fingerprint == archive["policies"][0]["policy"]["structural_fingerprint"]


def test_account_population_rejects_missing_runtimes() -> None:
    root = Path(__file__).resolve().parents[1]
    archive = json.loads(
        (root / "reports/economic_evolution/pilot_0001/seed_archive.json").read_text()
    )
    parent = _policy_from_dict(archive["policies"][0]["policy"])
    result = _account_population(
        (parent,),
        type("EmptyEvolution", (), {"children": ()})(),
        (),
        {},
        limit=10,
    )
    assert result == ()


def test_campaign_preregistration_fails_closed_on_external_actions(
    tmp_path: Path,
) -> None:
    value = {
        "schema": "hydra_economic_evolution_campaign_preregistration_v1",
        "q4_access_allowed": True,
        "new_data_purchase_allowed": False,
        "network_access_allowed": False,
        "broker_or_orders_allowed": False,
    }
    value["preregistration_hash"] = stable_hash(value)
    path = tmp_path / "campaign.json"
    path.write_text(json.dumps(value))

    with pytest.raises(EconomicEvolutionCampaignError, match="protected"):
        _load_and_validate_preregistration(path)
