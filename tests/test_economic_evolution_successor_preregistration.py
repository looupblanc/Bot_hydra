from __future__ import annotations

import hashlib
import json
from pathlib import Path

from hydra.economic_evolution.generator import generate_structural_population
from hydra.economic_evolution.schema import stable_hash
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config/v7/economic_evolution_persistent_0003.json"


def test_successor_preregistration_is_frozen_and_gate_equivalent() -> None:
    value = json.loads(CONFIG.read_text(encoding="utf-8"))
    payload = dict(value)
    frozen_hash = payload.pop("preregistration_hash")

    assert stable_hash(payload) == frozen_hash
    for relative, expected in value["implementation_files"].items():
        assert hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() == expected
    predecessor = json.loads(
        (ROOT / "config/v7/economic_evolution_persistent_0002.json").read_text(
            encoding="utf-8"
        )
    )
    for key in (
        "cheap_screen_policy",
        "exact_replay_period",
        "incremental_value_policy",
        "validator_calibration",
        "incremental_episode_policy",
        "assembly_episode_policy",
        "rolling_episode_policy",
        "account_research_gate",
        "combine_path_gate",
        "failure_policy",
        "archive_policy",
        "statuses",
    ):
        assert value[key] == predecessor[key]
    assert value["q4_access_allowed"] is False
    assert value["new_data_purchase_allowed"] is False
    assert value["network_access_allowed"] is False
    assert value["broker_or_orders_allowed"] is False
    assert value["successor_basis"]["status_inheritance"] is False


def test_successor_structural_manifest_and_seed_are_deterministic() -> None:
    value = json.loads(CONFIG.read_text(encoding="utf-8"))
    population = generate_structural_population(
        campaign_id=value["campaign_id"],
        raw_proposal_count=int(value["funnel"]["raw_proposals"]),
    )
    assert (
        population.candidate_manifest_hash
        == value["structural_population"]["candidate_manifest_hash"]
    )
    assert (
        population.unique_sleeve_count
        == value["structural_population"]["expected_unique_sleeves"]
    )
    seed_path = ROOT / value["seed_archive"]["path"]
    assert hashlib.sha256(seed_path.read_bytes()).hexdigest() == value["seed_archive"][
        "file_sha256"
    ]
    seed = load_and_verify_seed_archive(seed_path)
    assert seed["archive_hash"] == value["seed_archive"]["archive_hash"]
    seed_behaviors = {
        row["specification"]["behavioral_fingerprint"] for row in seed["sleeves"]
    }
    assert sum(
        row.behavioral_fingerprint in seed_behaviors for row in population.sleeves
    ) == value["structural_population"]["expected_seed_behavioral_overlap"]


def test_successor_reservation_is_conservative() -> None:
    value = json.loads(CONFIG.read_text(encoding="utf-8"))
    funnel = value["funnel"]
    enumerated_upper_bound = sum(
        int(funnel[key])
        for key in (
            "raw_proposals",
            "maximum_exact_component_replays",
            "incremental_value_evaluations",
            "structural_account_policies",
            "failure_directed_policy_children",
            "exact_account_policy_evaluations",
            "rolling_combine_elite_count",
            "xfa_elite_count",
        )
    )
    assert value["multiplicity"]["prospective_global_reservation"] >= enumerated_upper_bound
