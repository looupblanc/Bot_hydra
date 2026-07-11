from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from hydra.factory.mutation_hypothesis import (
    AccountObjectivePool,
    StrategyRole,
    assign_objective_pool,
)
from hydra.factory.promising_lineage_mutator import (
    MutationIntegrityError,
    _apply_prior_equity_guard,
    run_promising_lineage_mutation,
)


def _manifest() -> Path:
    return Path("config/research/promising_lineage_sources_v1.json")


@pytest.fixture(scope="module")
def mutation_result(tmp_path_factory: pytest.TempPathFactory) -> dict:
    manifest = _manifest()
    return run_promising_lineage_mutation(
        tmp_path_factory.mktemp("lineage-mutation"),
        source_manifest_path=manifest,
        source_manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
        code_commit="test-commit",
    )


def test_mutation_covers_all_frozen_parents_without_status_inheritance(
    mutation_result: dict,
) -> None:
    assert mutation_result["parent_count"] == 16
    assert mutation_result["primary_child_count"] == 16
    assert mutation_result["mutation_hypothesis_count"] == 18
    assert mutation_result["ym_versioned_hypotheses"] == 3
    assert all(row["parent_unchanged"] for row in mutation_result["parent_audit"])
    primary = [
        row
        for row in mutation_result["candidates"]
        if row["candidate_id"].endswith("__prior_equity_guard_v3")
    ]
    assert len(primary) == 16
    assert len({row["candidate_id"] for row in primary}) == 16
    assert all(row["status"] == "RESEARCH_PROTOTYPE" for row in mutation_result["candidates"])
    assert all(row["status_inherited"] is False for row in mutation_result["candidates"])
    assert all(row["inherited_passes"] == [] for row in mutation_result["candidates"])
    assert all(row["paper_shadow_ready"] is False for row in mutation_result["candidates"])
    assert mutation_result["q4_access_count"] == 0
    assert mutation_result["order_capability"] is False


def test_prior_equity_guard_never_reads_current_or_future_outcome() -> None:
    timestamps = pd.date_range("2024-01-01", periods=30, freq="D", tz="UTC")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "net_pnl": [float(index - 10) for index in range(30)],
        }
    )
    retained, audit = _apply_prior_equity_guard(frame)
    baseline_mask = frame.index.isin(retained.index)
    mutated = frame.copy()
    mutated.loc[18, "net_pnl"] = 1_000_000.0
    mutated_retained, _ = _apply_prior_equity_guard(mutated)
    mutated_mask = frame.index.isin(mutated_retained.index)
    assert audit["current_event_outcome_used"] is False
    assert audit["activation_shift_periods"] == 1
    assert baseline_mask[:19].tolist() == mutated_mask[:19].tolist()


def test_ym_children_are_versioned_and_clone_is_not_inflated(
    mutation_result: dict,
) -> None:
    children = [
        row
        for row in mutation_result["candidates"]
        if row["parent_candidate_id"] == "strategy_open_gap_continuation_YM_v1"
    ]
    assert len(children) == 3
    assert all(row["candidate_id"] != row["parent_candidate_id"] for row in children)
    micro = next(row for row in children if "micro_first" in row["candidate_id"])
    assert micro["behaviorally_duplicate"] is True
    assert micro["counts_as_distinct_strategy"] is False
    assert micro["disposition"] == "BACKUP_RISK_CONFIGURATION_NOT_DISTINCT_STRATEGY"


def test_objective_pools_are_separate_not_universal(mutation_result: dict) -> None:
    counts = mutation_result["objective_pool_counts"]
    assert set(counts) == {pool.value for pool in AccountObjectivePool}
    assert sum(counts.values()) == mutation_result["mutation_hypothesis_count"]
    candidate = {
        "mechanism_family": "plain_alpha",
        "topstep": {
            "path_candidate": False,
            "ten_micro_xfa_standard": {"payout_cycles_survived": 2},
        },
    }
    assert (
        assign_objective_pool(candidate, StrategyRole.ALPHA)
        == AccountObjectivePool.XFA_PAYOUT_POOL
    )


def test_source_hash_and_development_boundary_fail_closed(tmp_path: Path) -> None:
    manifest = _manifest()
    with pytest.raises(MutationIntegrityError, match="manifest hash drift"):
        run_promising_lineage_mutation(
            tmp_path / "bad-hash",
            source_manifest_path=manifest,
            source_manifest_sha256="0" * 64,
            code_commit="test",
        )
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["development_end_exclusive"] = "2024-12-31"
    changed = tmp_path / "changed.json"
    changed.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(MutationIntegrityError, match="data boundary"):
        run_promising_lineage_mutation(
            tmp_path / "bad-boundary",
            source_manifest_path=changed,
            source_manifest_sha256=hashlib.sha256(changed.read_bytes()).hexdigest(),
            code_commit="test",
        )
