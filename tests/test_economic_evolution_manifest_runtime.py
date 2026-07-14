from __future__ import annotations

import json
from pathlib import Path

from hydra.mission.economic_evolution_account_timeline_terminal_runtime import (
    NEXT_CAMPAIGN_ID,
    load_and_verify_account_timeline_terminal_verdict,
)
from hydra.mission.economic_evolution_account_timeline_runtime import (
    CAMPAIGN_OUTPUT_RELATIVE_PATH,
    CAMPAIGN_RESULT_NAME,
    verify_account_timeline_freeze,
)
from hydra.mission.economic_evolution_manifest_runtime import (
    EconomicEvolutionManifestRuntime,
    load_and_verify_manifest_queue,
)
from hydra.research.economic_evolution_account_timeline_campaign import (
    load_and_verify_account_timeline_result,
)


ROOT = Path(__file__).resolve().parents[1]


def test_0012_terminal_verdict_matches_completed_economics() -> None:
    config = verify_account_timeline_freeze(ROOT)
    result = load_and_verify_account_timeline_result(
        ROOT / CAMPAIGN_OUTPUT_RELATIVE_PATH / CAMPAIGN_RESULT_NAME,
        config,
    )
    verdict = load_and_verify_account_timeline_terminal_verdict(
        ROOT,
        result=result,
    )
    assert verdict["terminal_decision"]["verdict"] == (
        "CLASS_TOMBSTONE_EXACT_GRAMMAR"
    )
    assert verdict["result"]["policies_with_at_least_one_combine_pass"] == 0
    assert verdict["graveyard_append"]["parameter_level_feedback"] is False


def test_manifest_queue_is_reloadable_and_campaign_entry_is_frozen() -> None:
    queue = load_and_verify_manifest_queue(ROOT)
    assert queue["runtime_policy"]["reload_queue_each_controller_step"] is True
    assert queue["runtime_policy"]["controller_source_change_for_new_manifest"] is False
    assert queue["governance"]["q4_access_allowed"] is False
    runtime = EconomicEvolutionManifestRuntime(ROOT, ROOT / "mission/state")
    config = runtime._verify_entry(queue["entries"][0])
    assert config["campaign_id"] == NEXT_CAMPAIGN_ID
    assert config["compute"]["account_worker_count"] == 3
    assert config["governance"]["broker_or_orders_allowed"] is False


def test_manifest_runtime_requires_terminal_0012_predecessor(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    runtime = EconomicEvolutionManifestRuntime(ROOT, state)
    predecessor = {
        "action_type": "ECONOMIC_EVOLUTION_ACCOUNT_TIMELINE_0012_TOMBSTONED",
        "economic_account_timeline_terminal_state": "COMPLETE",
        "economic_account_timeline_parameter_rescue_allowed": False,
        "economic_account_timeline_same_class_relaunch_allowed": False,
        "next_experiment_id": NEXT_CAMPAIGN_ID,
    }
    runtime._verify_first_predecessor(predecessor, NEXT_CAMPAIGN_ID)


def test_production_queue_does_not_authorize_proof_data_or_orders() -> None:
    queue = json.loads(
        (ROOT / "config/v7/economic_evolution_production_queue.json").read_text()
    )
    assert queue["governance"] == {
        "q4_access_allowed": False,
        "new_data_purchase_allowed": False,
        "broker_or_orders_allowed": False,
        "proof_window_consumption_allowed": False,
    }
