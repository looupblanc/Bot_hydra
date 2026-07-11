from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from hydra.factory.post_mutation_shadow_admission import (
    PREREGISTRATION_SHA256,
    PostMutationShadowAdmissionError,
    run_post_mutation_shadow_admission,
)
from hydra.factory.post_mutation_successive_halving import (
    PREREGISTRATION_SHA256 as HALVING_PREREGISTRATION_SHA256,
    _select_elites,
    run_post_mutation_successive_halving,
)
from hydra.shadow.prior_trade_guard import (
    PriorTradeGuard,
    PriorTradeGuardError,
    PriorTradeGuardSpecification,
)
from hydra.shadow.activation import ShadowActivationError, run_immutable_shadow_activation
from hydra.shadow.specification import ShadowSpecification


ADMISSION_TASK = Path("reports/engineering/hydra_post_mutation_shadow_admission_20260711.md")
HALVING_TASK = Path("reports/engineering/hydra_post_mutation_successive_halving_20260711.md")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _mutation_inputs(root: Path, *, threshold: float = -100.0) -> tuple[Path, Path]:
    child_id = "strategy_open_gap_continuation_YM_v1__prior_equity_guard_v3"
    parent_id = "strategy_open_gap_continuation_YM_v1"
    candidate = {
        "candidate_id": child_id,
        "parent_candidate_id": parent_id,
        "status": "RESEARCH_PROTOTYPE",
        "status_inherited": False,
        "inherited_passes": [],
        "role": "alpha",
        "objective_pool": "COMBINE_PASSER_POOL",
        "hypothesis": {
            "hypothesis_id": "hyp_prior_guard",
            "parent_candidate_id": parent_id,
            "child_candidate_id": child_id,
            "mutation_class": "PRIOR_EQUITY_TEMPORAL_TRANSFER_GUARD",
            "exact_change": "Use prior twelve completed trades against a frozen threshold.",
            "training_policy": "Frozen earliest development segment; prior trades only.",
            "status_inheritance_allowed": False,
            "q4_access_allowed": False,
            "live_or_broker_allowed": False,
        },
        "guard": {
            "training_count": 6,
            "trailing_window": 12,
            "minimum_prior_observations": 6,
            "frozen_threshold": threshold,
            "current_event_outcome_used": False,
            "activation_shift_periods": 1,
        },
        "structural_fingerprint": "structural-child-v1",
        "parent_behavior_fingerprint": "parent-path",
        "behavior_fingerprint": "child-path",
        "behaviorally_duplicate": False,
        "metrics": {
            "retained_fraction": 0.75,
            "full_2023_replay_available": True,
            "candidate_null_bh_adjusted_p": 0.05,
            "topstep": {
                "micro_one_contract_mll_safe": True,
                "micro_one_contract_min_mll_buffer": 3_100.0,
                "path_candidate": True,
                "ten_micro_combine": {
                    "passed": True,
                    "mll_breached": False,
                    "consistency_ok": True,
                },
            },
        },
        "shadow_activation_allowed": False,
        "paper_shadow_ready": False,
        "q4_access_count": 0,
        "order_capability": False,
    }
    timestamps = []
    for start in ("2023-06-01", "2024-02-01", "2024-05-01", "2024-08-01"):
        base = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        timestamps.extend(base + timedelta(days=index) for index in range(4))
    rows = [
        {
            "candidate_id": child_id,
            "parent_candidate_id": parent_id,
            "timestamp": timestamp.isoformat(),
            "event_session_id": timestamp.date().isoformat(),
            "symbol": "MYM",
            "active_contract": "MYMU4",
            "net_pnl": 800.0,
            "gross_pnl": 802.0,
            "cost": 2.0,
            "mae_dollars": -75.0,
        }
        for timestamp in timestamps
    ]
    root.mkdir(parents=True, exist_ok=True)
    ledger = root / "mutation_trades.jsonl"
    ledger.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    payload = {
        "schema": "hydra_promising_lineage_mutation_result_v1",
        "code_commit": "frozen-mutation-code",
        "development_end_exclusive": "2024-10-01",
        "q4_access_count": 0,
        "network_requests": 0,
        "paid_data_requests": 0,
        "order_capability": False,
        "parent_audit": [{"candidate_id": parent_id, "parent_unchanged": True}],
        "candidates": [candidate],
        "artifacts": {"trade_ledger_sha256": _sha(ledger)},
    }
    payload["result_hash"] = _canonical_hash(payload)
    result = root / "mutation_result.json"
    result.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return result, ledger


def _run_halving(root: Path, *, threshold: float = -100.0) -> dict:
    mutation_result, ledger = _mutation_inputs(root / "inputs", threshold=threshold)
    return run_post_mutation_successive_halving(
        root / "halving",
        engineering_task_path=HALVING_TASK,
        engineering_task_sha256=HALVING_PREREGISTRATION_SHA256,
        mutation_result_path=mutation_result,
        mutation_result_sha256=_sha(mutation_result),
        mutation_trade_ledger_path=ledger,
        mutation_trade_ledger_sha256=_sha(ledger),
        code_commit="halving-test-code",
    )


def _parent_configuration(path: Path) -> ShadowSpecification:
    specification = ShadowSpecification(
        strategy_id="strategy_open_gap_continuation_YM_v1",
        strategy_version="v1_immutable_parent",
        feature_versions=("gap_state_v1",),
        markets=("YM", "MYM"),
        timeframes=("1m", "session"),
        session_rules={"timezone": "America/Chicago", "flatten": "15:10"},
        entry_rules={"decision": "08:31", "closed_bars_only": True},
        exit_rules={"holding_minutes": 60},
        sizing={"instrument": "MYM", "contracts": 1},
        costs={"round_turn_usd": 3.0},
        stale_data_seconds=120,
        expected_update_seconds=60,
        duplicate_signal_window_seconds=86_400,
        maximum_exposure=1.0,
        simulated_mll_floor=-4_500.0,
        internal_daily_risk_limit=500.0,
        kill_conditions=("stale_data", "mll_floor", "missing_guard_state"),
        logging={"signals": True, "virtual_fills": True},
        reconciliation={"startup": "fail_closed"},
        source_manifest_hash="a" * 64,
        outbound_orders_enabled=False,
    )
    specification.write_immutable(path)
    return specification


def _parent_source_result(path: Path) -> Path:
    payload = {
        "schema": "equity_open_gap_continuation_pilot_v1",
        "data_access_record": {
            "data_role": "DEVELOPMENT",
            "period_accessed": "2023-01-01:2024-10-01",
        },
        "data_provenance": {
            "period_start": "2023-01-01",
            "period_end_exclusive": "2024-10-01",
            "data_fingerprint": "d" * 64,
            "contract_map_sha256": "c" * 64,
            "symbols": ["YM", "MYM"],
            "files": [{"path": "development.parquet", "sha256": "f" * 64}],
        },
        "integrity_proof": {
            "decision_after_source_close": True,
            "exact_future_horizon": True,
            "finite_primary_costs": True,
            "nonempty_event_source": True,
            "one_event_per_market_session": True,
            "past_only_threshold": True,
            "q4_excluded": True,
            "reference_strictly_past": True,
            "same_explicit_contract_reference": True,
        },
        "candidates": [
            {
                "candidate_id": "strategy_open_gap_continuation_YM_v1",
                "primary_market": "YM",
                "execution_market": "MYM",
                "fold_results": {"2023_h2": {"events": 8, "net_pnl": 100.0}},
                "shadow_evidence": {"hard_invalidations": []},
            }
        ],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _run_admission(root: Path, halving: dict, parent_path: Path, parent: ShadowSpecification) -> dict:
    result_path = Path(halving["artifacts"]["result"]["path"])
    manifest_path = Path(halving["artifacts"]["elite_manifest"]["path"])
    evidence_path = Path(halving["artifacts"]["candidate_evidence"]["path"])
    parent_source_path = root / "parent_source_result.json"
    if not parent_source_path.exists():
        _parent_source_result(parent_source_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return run_post_mutation_shadow_admission(
        root / "admission",
        engineering_task_path=ADMISSION_TASK,
        engineering_task_sha256=PREREGISTRATION_SHA256,
        halving_result_path=result_path,
        halving_result_sha256=_sha(result_path),
        halving_result_hash=halving["result_hash"],
        elite_manifest_path=manifest_path,
        elite_manifest_sha256=_sha(manifest_path),
        elite_manifest_hash=manifest["manifest_hash"],
        candidate_evidence_path=evidence_path,
        candidate_evidence_sha256=_sha(evidence_path),
        parent_source_result_path=parent_source_path,
        parent_source_result_sha256=_sha(parent_source_path),
        parent_shadow_configuration_path=parent_path,
        parent_shadow_configuration_sha256=_sha(parent_path),
        parent_shadow_configuration_hash=parent.configuration_hash,
        code_commit="admission-test-code",
    )


def test_prior_trade_guard_fails_closed_until_hashed_genesis_and_restarts() -> None:
    specification = PriorTradeGuardSpecification(
        trailing_window=3,
        minimum_prior_observations=2,
        warmup_completed_trades=2,
        frozen_threshold=10.0,
    )
    missing = PriorTradeGuard(specification)
    missing_decision = missing.evaluate(decision_at="2026-07-11T12:00:00+00:00")
    assert not missing_decision.allowed
    assert missing_decision.reason == "MISSING_RECONCILED_GUARD_STATE_FAIL_CLOSED"
    with pytest.raises(PriorTradeGuardError, match="before state reconciliation"):
        missing.record_completed_trade(
            trade_id="forbidden", completed_at="2026-07-11T11:00:00+00:00", net_pnl=1.0
        )

    guard = PriorTradeGuard.initialize_genesis(specification)
    before_current = guard.evaluate(decision_at="2026-07-11T12:00:00+00:00")
    assert before_current.allowed and before_current.prior_completed_trade_count == 0
    guard.record_completed_trade(
        trade_id="trade-1", completed_at="2026-07-11T12:01:00+00:00", net_pnl=-100.0
    )
    # The outcome completed at 12:01 cannot be consulted by the 12:00 decision.
    with pytest.raises(PriorTradeGuardError, match="completed after decision"):
        guard.evaluate(decision_at="2026-07-11T12:00:00+00:00")
    guard.record_completed_trade(
        trade_id="trade-2", completed_at="2026-07-11T12:02:00+00:00", net_pnl=-100.0
    )
    blocked = guard.evaluate(decision_at="2026-07-11T12:03:00+00:00")
    assert not blocked.allowed
    assert blocked.prior_window_net_pnl == -200.0

    state = guard.export_state()
    restored = PriorTradeGuard.restore(specification, state)
    assert restored.evaluate(decision_at="2026-07-11T12:03:00+00:00") == blocked
    state["completed_trades"][0]["net_pnl"] = 1_000.0
    with pytest.raises(PriorTradeGuardError, match="state hash drift"):
        PriorTradeGuard.restore(specification, state)


def test_evidence_propagation_cannot_change_halving_selected_ids(tmp_path: Path) -> None:
    halving = _run_halving(tmp_path / "run")
    propagated = copy.deepcopy(halving["candidates"])
    legacy = copy.deepcopy(propagated)
    for row in legacy:
        row.pop("mutation_evidence", None)
        row["selected_elite"] = False
    legacy_selected, _ = _select_elites(legacy)

    for row in propagated:
        row["mutation_evidence"]["guard"]["frozen_threshold"] = 999_999.0
        row["selected_elite"] = False
    propagated_selected, _ = _select_elites(propagated)

    expected = halving["selected_candidate_ids"]
    assert sorted(row["candidate_id"] for row in legacy_selected) == expected
    assert sorted(row["candidate_id"] for row in propagated_selected) == expected


def test_admission_exports_one_inactive_immutable_zero_order_candidate(tmp_path: Path) -> None:
    halving = _run_halving(tmp_path / "research")
    parent_path = tmp_path / "parent_shadow.json"
    parent = _parent_configuration(parent_path)
    parent_bytes = parent_path.read_bytes()

    result = _run_admission(tmp_path, halving, parent_path, parent)

    assert result["shadow_candidates"] == 1
    assert result["shadow_research_active"] == 0
    assert result["paper_shadow_ready"] == 0
    assert result["q4_access_count"] == 0
    assert result["network_requests"] == 0
    assert result["order_capability"] is False
    assert len(result["candidates"]) == len(result["shadow_configurations"]) == 1
    candidate = result["candidates"][0]
    assert candidate["status"] == "SHADOW_RESEARCH_CANDIDATE"
    assert candidate["operational_classification"] == "INACTIVE_SHADOW_RESEARCH_CANDIDATE"
    assert candidate["admission"]["activation_requires_generic_workflow"] is True
    assert candidate["shadow_evidence"]["account_mll_safe"] is True
    configuration_path = Path(result["shadow_configurations"][0]["path"])
    configuration = json.loads(configuration_path.read_text(encoding="utf-8"))
    assert configuration["outbound_orders_enabled"] is False
    assert configuration["entry_rules"]["prior_trade_guard"]["current_event_outcome_used"] is False
    provenance = configuration["entry_rules"]["mutation_provenance"]
    assert provenance["parent_candidate_id"] == "strategy_open_gap_continuation_YM_v1"
    assert provenance["child_candidate_id"] == candidate["candidate_id"]
    assert provenance["data_role"] == "DEVELOPMENT_AND_FALSIFICATION_ONLY"
    assert len(provenance["source_data_fingerprint"]) == 64
    assert configuration["costs"]["admission_evidence"]["stress_cost_multiplier"] == 1.5
    assert configuration["source_manifest_hash"] == candidate["admission"]["selection_decision_hash"]
    assert configuration["reconciliation"]["prior_trade_guard_restart"] == "verify_state_hash_or_fail_closed"
    assert parent_path.read_bytes() == parent_bytes

    repeated = _run_admission(tmp_path, halving, parent_path, parent)
    assert repeated["result_hash"] == result["result_hash"]
    assert repeated["shadow_configurations"][0]["configuration_hash"] == configuration["configuration_hash"]

    # Admission itself stays inactive; only the existing generic workflow can
    # produce an active, still-zero-order shadow record.
    source_path = Path(result["artifacts"]["result"]["path"])
    incomplete_surface = tmp_path / "virtual_only.py"
    incomplete_surface.write_text("def virtual_fill():\n    return 0.0\n", encoding="utf-8")
    with pytest.raises(ShadowActivationError, match="not wired fail-closed"):
        run_immutable_shadow_activation(
            tmp_path / "unsafe_activation",
            engineering_task_path=ADMISSION_TASK,
            engineering_task_sha256=PREREGISTRATION_SHA256,
            source_result_path=source_path,
            source_result_sha256=_sha(source_path),
            source_result_hash=result["result_hash"],
            candidate_id=candidate["candidate_id"],
            shadow_configuration_path=configuration_path,
            shadow_configuration_sha256=_sha(configuration_path),
            shadow_configuration_hash=configuration["configuration_hash"],
            code_commit="test",
            code_surface_paths=[incomplete_surface],
        )
    activated = run_immutable_shadow_activation(
        tmp_path / "generic_activation",
        engineering_task_path=ADMISSION_TASK,
        engineering_task_sha256=PREREGISTRATION_SHA256,
        source_result_path=source_path,
        source_result_sha256=_sha(source_path),
        source_result_hash=result["result_hash"],
        candidate_id=candidate["candidate_id"],
        shadow_configuration_path=configuration_path,
        shadow_configuration_sha256=_sha(configuration_path),
        shadow_configuration_hash=configuration["configuration_hash"],
        code_commit="test",
        code_surface_paths=[
            Path("hydra/shadow/runner.py"),
            Path("hydra/shadow/prior_trade_guard.py"),
        ],
    )
    assert activated["shadow_active"] == 1
    assert activated["paper_shadow_ready"] == 0
    assert activated["activation_manifest"]["outbound_orders_enabled"] is False
    assert activated["activation_manifest"]["prior_trade_guard_wiring_audit"]["passed"]


def test_missing_or_drifted_admission_evidence_fails_closed(tmp_path: Path) -> None:
    halving = _run_halving(tmp_path / "research")
    result_path = Path(halving["artifacts"]["result"]["path"])
    persisted = json.loads(result_path.read_text(encoding="utf-8"))
    persisted["candidates"][0]["mutation_evidence"]["retained_fraction"] = 0.99
    result_path.write_text(json.dumps(persisted, indent=2, sort_keys=True), encoding="utf-8")
    parent_path = tmp_path / "parent_shadow.json"
    parent = _parent_configuration(parent_path)

    with pytest.raises(PostMutationShadowAdmissionError, match="Candidate evidence drift"):
        _run_admission(tmp_path, halving, parent_path, parent)


def test_single_2023_event_cannot_substitute_for_registered_source_coverage(
    tmp_path: Path,
) -> None:
    halving = _run_halving(tmp_path / "research")
    parent_path = tmp_path / "parent_shadow.json"
    parent = _parent_configuration(parent_path)
    source_path = _parent_source_result(tmp_path / "parent_source_result.json")
    source = json.loads(source_path.read_text(encoding="utf-8"))
    source["data_access_record"]["period_accessed"] = "2023-06-01:2024-10-01"
    source_path.write_text(
        json.dumps(source, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    with pytest.raises(PostMutationShadowAdmissionError, match="complete registered"):
        _run_admission(tmp_path, halving, parent_path, parent)
