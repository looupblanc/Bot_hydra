from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.calibration.cost_hurdle_calibration import calibrated_atom_cost_policy
from hydra.calibration.validator_benchmark import ATTACK_CLASSIFICATION, benchmark_validator
from hydra.governance.invariants import q4_access_count, run_governance_checks
from hydra.governance.protected_manifest import build_protected_manifest
from hydra.mission.evidence_graph import EvidenceNode, edge_scope_violation
from hydra.mission.mission_state import connect_state, mission_lock, mission_paths, request_stop, stop_requested, clear_stop
from hydra.validation.evidence_scope import EvidenceScope


BASELINE = "b56c98b8179d67e87d0290690fd8b73f70040dbe"


def test_lower_scope_evidence_cannot_promote_to_higher_scope() -> None:
    component = EvidenceNode(
        node_id="component-1",
        scope=EvidenceScope.COMPONENT.value,
        status="COMPONENT_PASS",
        parent_ids=(),
        provenance_hash="abc",
    )
    atom = EvidenceNode(
        node_id="atom-1",
        scope=EvidenceScope.EDGE_ATOM.value,
        status="ATOM_VALIDATED",
        parent_ids=("component-1",),
        provenance_hash="abc",
    )
    assert edge_scope_violation(component.scope, atom.scope)


def test_q4_access_counter_respects_exclusive_development_boundary(tmp_path: Path) -> None:
    ledger = tmp_path / "data_access.jsonl"
    rows = [
        {"period_accessed": "2024-07-01:2024-10-01", "data_role": "CONTAMINATED_DEVELOPMENT"},
        {"period_accessed": "2024-10-01:2024-10-02", "data_role": "DEVELOPMENT"},
        {"period_accessed": "2024-09-30:2024-10-02", "data_role": "DEVELOPMENT"},
        {"period_accessed": "2024-08-01:2024-08-02", "data_role": "SEALED_BLIND_HOLDOUT"},
    ]
    ledger.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    assert q4_access_count(str(ledger)) == 3


def test_q4_access_counter_normalizes_only_exact_exclusive_boundary_marker(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "data_access.jsonl"
    rows = [
        {
            "period_accessed": "2023-01-01:2024-10-01_EXCLUSIVE",
            "data_role": "CONTAMINATED_DEVELOPMENT",
        },
        {
            "period_accessed": "2023-01-01:2024-10-02_EXCLUSIVE",
            "data_role": "DEVELOPMENT",
        },
        {
            "period_accessed": "2024-10-01_EXCLUSIVE:2024-10-02_EXCLUSIVE",
            "data_role": "DEVELOPMENT",
        },
        {
            "period_accessed": "2023-01-01:2024-10-01_EXCLUSIVE_EXCLUSIVE",
            "data_role": "DEVELOPMENT",
        },
        {
            "period_accessed": "2024-08-01:2024-08-02",
            "data_role": "SEALED_BLIND_HOLDOUT",
        },
        {
            "period_accessed": "2024-08-01:2024-08-02",
            "data_role": "FINAL_LOCKBOX",
        },
    ]
    ledger.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

    # Exact development boundary is excluded.  Later/marked starts,
    # ambiguous repeated markers, and protected roles remain counted.
    assert q4_access_count(str(ledger)) == 5


def test_governance_manifest_contains_protected_files() -> None:
    manifest = build_protected_manifest(baseline_commit=BASELINE)
    protected = {item.path for item in manifest.digests}
    assert "config/governance/hydra_governance_v1.yaml" in protected
    assert "hydra/governance/kernel.py" in protected
    assert all(item.exists for item in manifest.digests)


def test_mission_lock_blocks_second_writer(tmp_path: Path) -> None:
    paths = mission_paths(str(tmp_path / "state"))
    with mission_lock(paths):
        with pytest.raises(RuntimeError):
            with mission_lock(paths):
                pass


def test_stop_and_resume_state_file(tmp_path: Path) -> None:
    paths = mission_paths(str(tmp_path / "state"))
    request_stop(paths, "unit_test")
    assert stop_requested(paths)
    clear_stop(paths)
    assert not stop_requested(paths)


def test_mission_state_is_deterministic(tmp_path: Path) -> None:
    paths = mission_paths(str(tmp_path / "state"))
    conn = connect_state(paths)
    try:
        conn.execute("INSERT OR REPLACE INTO kv(key, value, updated_at) VALUES ('a', '1', 'now')")
        conn.commit()
        rows = conn.execute("SELECT key, value FROM kv ORDER BY key").fetchall()
    finally:
        conn.close()
    assert rows == [("a", "1")]


def test_validator_calibration_rejects_nulls_and_detects_injected_edges() -> None:
    result = benchmark_validator(seed=9050)
    assert result.false_positive_rate <= 0.20
    assert result.power_on_meaningful_effects >= 0.80
    assert result.passed


def test_attack_classification_separates_diagnostics_from_fatal_attacks() -> None:
    assert ATTACK_CLASSIFICATION["lookahead"] == "FATAL_MANDATORY"
    assert ATTACK_CLASSIFICATION["best_event_removed"] == "ROBUSTNESS_DIAGNOSTIC"
    assert ATTACK_CLASSIFICATION["placebo_market"] == "INFORMATIONAL_ONLY"


def test_cost_hurdle_policy_separates_atom_and_strategy_costs() -> None:
    policy = calibrated_atom_cost_policy()
    assert policy.atom_statistical_hurdle_multiplier < 1.0
    assert not policy.strategy_execution_cost_required


def test_systemd_units_reference_venv_and_single_writer() -> None:
    service = Path("deploy/systemd/hydra-autonomous-mission.service").read_text(encoding="utf-8")
    assert "/root/hydra-bot/.venv/bin/python" in service
    assert "--single-writer" in service
    assert "--no-live-trading" in service
    assert "Restart=on-failure" in service


def test_governance_checks_pass_without_q4_or_live_trading() -> None:
    result = run_governance_checks(baseline_commit=BASELINE, remaining_budget_usd=77.036754)
    assert result.checks["q4_not_accessed"]
    assert result.checks["no_live_trading"]
    assert result.checks["scope_promotion_blocked"]
