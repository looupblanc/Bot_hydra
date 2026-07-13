from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.mission.economic_evolution_runtime import (
    CAMPAIGN_ID,
    EconomicEvolutionRuntimeError,
    classify_economic_evolution_action,
    load_and_verify_campaign_result,
    verify_economic_evolution_freeze,
)


def test_campaign_freeze_and_tag_are_verifiable() -> None:
    root = Path(__file__).resolve().parents[1]
    config = verify_economic_evolution_freeze(root)

    assert config["campaign_id"] == CAMPAIGN_ID
    assert config["q4_access_allowed"] is False
    assert config["new_data_purchase_allowed"] is False
    assert config["network_access_allowed"] is False
    assert config["broker_or_orders_allowed"] is False


def test_campaign_action_is_ready_without_reading_outcomes() -> None:
    root = Path(__file__).resolve().parents[1]
    action = classify_economic_evolution_action(
        root,
        {
            "action_type": "PREDECESSOR_TERMINAL",
            "phase": "4",
            "g12_tripwire_verdict": "ARTEFACT_GEOMETRY_ONLY",
        },
    )

    assert action["action_type"] == "ECONOMIC_EVOLUTION_CAMPAIGN_0002_PREREGISTERED"
    assert action["g12_tripwire_verdict"] == "ARTEFACT_GEOMETRY_ONLY"
    assert action["new_data_purchase_authorized"] is False
    assert action["protected_holdout_access_authorized"] is False


def test_campaign_result_rejects_unauthorized_promotion(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    config = verify_economic_evolution_freeze(root)
    result = {
        "schema": "hydra_economic_evolution_campaign_result_v1",
        "campaign_id": CAMPAIGN_ID,
        "preregistration_hash": config["preregistration_hash"],
        "funnel": {
            "raw_structural_proposals": 50000,
            "pre_holdout_ready": 1,
            "paper_shadow_ready": 0,
        },
        "governance": {
            "protected_holdout_accessed": False,
            "q4_accessed": False,
            "outbound_order_capability": False,
            "broker_connections": 0,
            "orders": 0,
            "status_inheritance": False,
        },
    }
    path = tmp_path / "result.json"
    path.write_text(json.dumps(result))

    with pytest.raises(EconomicEvolutionRuntimeError, match="promotion"):
        load_and_verify_campaign_result(path, config)
