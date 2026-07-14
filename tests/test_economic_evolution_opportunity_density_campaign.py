from __future__ import annotations

from pathlib import Path

from hydra.economic_evolution.account_opportunity_density import (
    generate_opportunity_density_population,
)
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive
from hydra.research.economic_evolution_opportunity_density_campaign import (
    load_and_verify_opportunity_density_preregistration,
)


ROOT = Path(__file__).resolve().parents[1]
CONFIG = (
    ROOT
    / "config/v7/economic_evolution_opportunity_density_0013_revision_01.json"
)
SEED = ROOT / "reports/economic_evolution/seeds/persistent_0003_successor_seed.json"


def test_opportunity_density_preregistration_and_population_are_frozen() -> None:
    config = load_and_verify_opportunity_density_preregistration(CONFIG)
    seed = load_and_verify_seed_archive(SEED)
    population = generate_opportunity_density_population(
        seed,
        campaign_id=str(config["campaign_id"]),
        policy_pair_count=int(config["structural_population"]["policy_pair_count"]),
        maximum_components=int(config["structural_population"]["component_count"]),
        minimum_component_events=int(
            config["structural_population"]["minimum_component_events"]
        ),
        minimum_markets=int(config["structural_population"]["minimum_markets"]),
        minimum_sessions=int(config["structural_population"]["minimum_sessions"]),
    )
    assert population.manifest_hash == config["structural_population"][
        "policy_manifest_hash"
    ]
    assert len(population.pairs) == 512
    assert config["runtime_manifest"]["controller_source_change_required"] is False
    assert config["governance"]["q4_access_allowed"] is False
    assert config["governance"]["broker_or_orders_allowed"] is False
