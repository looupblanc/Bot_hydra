from __future__ import annotations

from collections import Counter

from hydra.research.v7_boosted_tournament_specs import (
    bounded_basket_structures,
    candidate_specs,
    mechanism_families,
)


def test_boosted_tournament_has_eight_distinct_families_and_256_specs() -> None:
    families = mechanism_families()
    specs = candidate_specs()

    assert len(families) == 8
    assert len({row.family_id for row in families}) == 8
    assert all(len(row.motifs) == 8 for row in families)
    assert len(specs) == 256
    assert Counter(row.family_id for row in specs) == {
        row.family_id: 32 for row in families
    }
    assert len({row.candidate_id for row in specs}) == 256
    assert len({row.specification_hash for row in specs}) == 256


def test_bounded_baskets_are_deterministic_and_require_new_components() -> None:
    component_ids = tuple(f"component_{index:02d}" for index in range(13))
    new_ids = ("component_11", "component_12")
    roles = {candidate_id: "DIVERSIFIER" for candidate_id in component_ids}
    roles["component_12"] = "TARGET_VELOCITY"

    first = bounded_basket_structures(
        component_ids,
        new_component_ids=new_ids,
        role_map=roles,
    )
    second = bounded_basket_structures(
        tuple(reversed(component_ids)),
        new_component_ids=tuple(reversed(new_ids)),
        role_map=roles,
    )

    assert first == second
    assert len(first) == 320
    assert len({row["structural_hash"] for row in first}) == 320
    assert all(set(row["component_ids"]).intersection(new_ids) for row in first)
    assert all(2 <= len(row["component_ids"]) <= 4 for row in first)

