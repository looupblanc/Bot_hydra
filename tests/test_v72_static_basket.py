from __future__ import annotations

from hydra.account_policy.v72_static_basket import (
    freeze_rotation_basket,
    generate_static_basket_structures,
)


def _components() -> list[dict[str, str]]:
    roles = ["TARGET_VELOCITY"] * 4 + ["MLL_PROTECTION"] * 7
    return [
        {"candidate_id": f"component_{position:02d}", "role": role}
        for position, role in enumerate(roles)
    ]


def test_v72_static_search_has_exact_preregistered_structure_count() -> None:
    structures = generate_static_basket_structures(_components())

    assert len(structures) == 1009
    assert sum(row.allocation_profile == "UNIT_EQUAL" for row in structures) == 550
    assert sum(
        row.allocation_profile == "TARGET_VELOCITY_TILT" for row in structures
    ) == 459
    assert len({row.structural_hash for row in structures}) == len(structures)


def test_rotation_freeze_uses_design_only_metrics_deterministically() -> None:
    components = _components()
    structure = next(
        row
        for row in generate_static_basket_structures(components)
        if row.allocation_profile == "TARGET_VELOCITY_TILT"
        and {"component_00", "component_01"}.issubset(row.component_ids)
    )
    metrics = {
        row["candidate_id"]: float(position)
        for position, row in enumerate(components)
    }
    roles = {row["candidate_id"]: row["role"] for row in components}
    frozen = freeze_rotation_basket(
        structure,
        individual_design_stress_net=metrics,
        component_roles=roles,
        design_block_ids=("A", "B", "C"),
        held_out_block_id="D",
    )

    target_members = [
        component
        for component in frozen.component_ids
        if roles[component] == "TARGET_VELOCITY"
    ]
    expected_tilt = max(target_members, key=metrics.__getitem__)
    assert frozen.risk_units[expected_tilt] == 2
    assert frozen.component_priority[0] == max(
        frozen.component_ids, key=metrics.__getitem__
    )
    assert frozen.design_block_ids == ("A", "B", "C")
    assert frozen.held_out_block_id == "D"
