from __future__ import annotations

from hydra.account_policy.archive import AccountPolicyArchive, PolicyArchiveEntry
from hydra.account_policy.evolution import (
    ComponentRuntime,
    generate_basket_population,
    select_cluster_primaries,
)
from hydra.account_policy.schema import ComponentDescriptor, ComponentRole


def _component(index: int, *, cluster: int | None = None) -> ComponentRuntime:
    market = ("ES", "NQ", "CL", "GC")[index % 4]
    descriptor = ComponentDescriptor(
        component_id=f"component-{index}",
        specification_hash=f"spec-{index}",
        market=market,
        execution_market={"ES": "MES", "NQ": "MNQ", "CL": "MCL", "GC": "MGC"}[market],
        family=f"family-{index % 5}",
        timeframe="1m|15m",
        role=(
            ComponentRole.DEFENSIVE_COMPONENT
            if index % 5 == 0
            else ComponentRole.TARGET_VELOCITY_COMPONENT
        ),
        behavioral_cluster=f"cluster-{cluster if cluster is not None else index}",
        source_experiment="source",
        source_result_hash=f"result-{index}",
        net_pnl_after_costs=1000.0 + index,
        cost_stress_net_pnl=800.0 + index,
        event_count=40,
        rolling_pass_rate=0.05 if index % 3 == 0 else 0.0,
        rolling_mll_breach_rate=0.05,
        median_target_progress=0.20 + index / 100.0,
    )
    return ComponentRuntime(descriptor, {"candidate_id": descriptor.component_id}, (), tuple(range(200)), "TEST")


def test_cluster_primaries_keep_one_best_component_per_behavior() -> None:
    weak = _component(1, cluster=7)
    strong_descriptor = ComponentDescriptor(
        **{
            **_component(2, cluster=7).descriptor.to_dict(),
            "component_id": "strong",
            "role": ComponentRole.TARGET_VELOCITY_COMPONENT,
            "rolling_pass_rate": 0.20,
            "behavioral_cluster": "cluster-7",
        }
    )
    strong = ComponentRuntime(strong_descriptor, {"candidate_id": "strong"}, (), tuple(range(200)), "TEST")

    selected = select_cluster_primaries([weak, strong, _component(3)])

    assert {row.descriptor.component_id for row in selected} == {"strong", "component-3"}


def test_basket_generation_produces_hundreds_without_clone_inflation() -> None:
    components = [_component(index) for index in range(24)]
    first = generate_basket_population(components, count=300, generation_index=0)
    second = generate_basket_population(components, count=300, generation_index=0)

    assert first == second
    assert len(first) == 300
    assert len({row.fingerprint for row in first}) == 300
    assert all(2 <= len(row.component_ids) <= 5 for row in first)
    assert all(len(set(row.component_ids)) == len(row.component_ids) for row in first)

    later = generate_basket_population(
        components,
        count=100,
        generation_index=1,
        excluded_fingerprints={row.structural_fingerprint for row in first},
    )
    assert len(later) == 100
    assert not (
        {row.structural_fingerprint for row in first}
        & {row.structural_fingerprint for row in later}
    )


def test_account_policy_archive_preserves_quality_diversity_niches() -> None:
    archive = AccountPolicyArchive(maximum_per_niche=1)
    weak = PolicyArchiveEntry(
        "weak", "STATIC", ("STATIC", "2", "BALANCED"), 0.4, 0.0, 0.1, 0.3, ("a", "b"), {}
    )
    strong = PolicyArchiveEntry(
        "strong", "STATIC", ("STATIC", "2", "BALANCED"), 0.6, 0.1, 0.05, 0.5, ("c", "d"), {}
    )
    different = PolicyArchiveEntry(
        "controller", "CONTROLLED", ("CONTROLLED", "3", "MLL"), 0.5, 0.05, 0.0, 0.4, ("a", "c", "e"), {}
    )

    assert archive.insert(weak)
    assert archive.insert(strong)
    assert archive.insert(different)
    assert {row.policy_id for row in archive.entries} == {"strong", "controller"}
    assert archive.summary()["niche_count"] == 2
