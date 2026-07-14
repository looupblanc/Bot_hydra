from __future__ import annotations

import json
from pathlib import Path

from hydra.economic_evolution.account_complementary_sleeve import (
    generate_complementary_sleeve_population,
)
from hydra.economic_evolution.account_static_parent_basket import (
    STATIC_PARENT_BASKET_CLASS_ID,
    generate_static_parent_basket_population,
)
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive


ROOT = Path(__file__).resolve().parents[1]


def _population():
    seed = load_and_verify_seed_archive(
        ROOT / "reports/economic_evolution/seeds/persistent_0003_successor_seed.json"
    )
    source = generate_complementary_sleeve_population(
        seed,
        campaign_id="hydra_economic_evolution_complementary_sleeve_0017",
        parent_campaign_id="hydra_economic_evolution_three_zone_sizing_0016",
        sizing_parent_campaign_id="hydra_economic_evolution_buffer_sizing_0015",
        coverage_parent_campaign_id="hydra_economic_evolution_coverage_union_0014",
        policy_pair_count=512,
        maximum_components=48,
        minimum_component_events=20,
    )
    elite = json.loads(
        (ROOT / "WORM/economic-evolution-0018-canonical-elites-2026-07-14.json")
        .read_text(encoding="utf-8")
    )
    bank = json.loads(
        (ROOT / "WORM/economic-evolution-static-parent-bank-0023-2026-07-14.json")
        .read_text(encoding="utf-8")
    )
    return generate_static_parent_basket_population(
        elite,
        [row.to_dict() for row in source.components],
        parent_bank=bank,
        campaign_id="hydra_economic_evolution_static_parent_basket_0023",
    )


def test_static_parent_population_is_bounded_deterministic_and_status_clean() -> None:
    first = _population()
    second = _population()
    assert first.manifest_hash == second.manifest_hash
    assert first.summary()["class_id"] == STATIC_PARENT_BASKET_CLASS_ID
    assert len(first.proposals) == 512
    assert len(first.pairs) == 128
    assert all(2 <= len(row.source_parent_ids) <= 4 for row in first.proposals)
    assert all(10 <= len(row.component_ids) <= 13 for row in first.proposals)
    assert all(row.inherited_status is None for row in first.proposals)
    assert all(row.real_policy.component_ids != row.matched_control_policy.component_ids for row in first.pairs)


def test_static_parent_population_does_not_pair_anchor_with_its_source_parent() -> None:
    population = _population()
    forbidden = {
        "elite_robustness_child_ca668fb2e9189ee6fd0805ce",
        "complementary_sleeve_real_2a948b1b1da0eb1d07fee597",
    }
    assert all(
        not forbidden.issubset(set(row.source_parent_ids))
        for row in population.proposals
    )
    assert all(row.to_dict()["underlying_signals_changed"] is False for row in population.pairs)
