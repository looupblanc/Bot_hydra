from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_director_runtime as runtime


def _manifest() -> dict[str, Any]:
    return {
        "campaign_id": "hydra_autonomous_economic_discovery_director_0035",
        "manifest_hash": "a" * 64,
        "source_commit": "b" * 40,
        "post_breadth_branch_portfolio": {
            "scheduled_macro_release_decision_card_hash": "c" * 64,
        },
    }


def _result(*, passed: bool = False) -> dict[str, Any]:
    selected = [{"candidate_id": "macro_candidate"}]
    passing = ["macro_candidate"] if passed else []
    status = (
        "SCHEDULED_MACRO_RELEASE_TRIPWIRE_PASSED_TIER_E_DEVELOPMENT_ONLY"
        if passed
        else "SCHEDULED_MACRO_RELEASE_CAUSAL_REACTION_FALSIFIED"
    )
    core = {
        "schema": runtime.SCHEDULED_MACRO_RELEASE_SCHEMA,
        "branch_id": runtime.SCHEDULED_MACRO_RELEASE_BRANCH_ID,
        "status": status,
        "economic_verdict": status,
        "evidence_role": "BOUNDED_PRE_Q4_DEVELOPMENT_TRIPWIRE_ONLY",
        "evidence_ceiling": "TIER_E_DEVELOPMENT_ONLY",
        "source_bindings": {"decision_card_hash": "c" * 64},
        "official_release_count": 56,
        "official_release_counts": {
            "BLS_CPI": 21,
            "BLS_EMPLOYMENT_SITUATION": 21,
            "FOMC_STATEMENT": 14,
        },
        "role_release_counts": {
            "DISCOVERY": 28,
            "VALIDATION": 14,
            "FINAL_DEVELOPMENT": 14,
        },
        "candidate_lattice_count": 96,
        "candidate_screens": [
            {"candidate_id": f"macro_screen_{index:03d}"}
            for index in range(96)
        ],
        "selected_candidate_count": 1,
        "selected_candidates": selected,
        "discovery_account_frontier": [],
        "evaluations": [{"candidate_id": "macro_candidate"}],
        "best_evaluation": {"candidate_id": "macro_candidate"},
        "passing_candidate_ids": passing,
        "promotion_status": None,
        "tier_q_created": 0,
        "independent_confirmation_claimed": False,
        "q4_access_count_delta": 0,
        "incremental_data_spend_usd": 0.0,
        "broker_connections": 0,
        "orders": 0,
        "tombstoned_eia_inventory_grammar_resurrected": False,
        "next_action": (
            "PRESERVE_AS_TIER_E_AND_REQUIRE_ONE_GENUINELY_FRESH_CONFIRMATION_WITHOUT_RETUNING"
            if passed
            else "TOMBSTONE_THIS_EXACT_MACRO_REACTION_LATTICE_AND_REALLOCATE_TO_A_NON_EVENT_CLOCK_REPRESENTATION"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


class _ThreadExecutor:
    def __init__(self, *, max_workers: int, mp_context: object) -> None:
        assert max_workers == 1
        del mp_context
        self._pool = ThreadPoolExecutor(max_workers=1)

    def __enter__(self) -> "_ThreadExecutor":
        return self

    def __exit__(self, *args: object) -> None:
        self._pool.shutdown(wait=True)

    def submit(self, function: Any, *args: Any) -> Any:
        return self._pool.submit(function, *args)


def _run(
    tmp_path: Path, *, prior_state: dict[str, Any] | None = None
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    output = tmp_path / "output"
    output.mkdir(exist_ok=True)
    return runtime._run_scheduled_macro_release_tripwire_relay(
        root=tmp_path,
        manifest=_manifest(),
        output=output,
        live_writer=object(),
        branch_writer=AtomicResultWriter(output / "branch_results"),
        prior_state=prior_state or {"checkpoint_sequence": 0},
        started=time.monotonic(),
        heartbeat_seconds=0.01,
        runtime_results={},
    )


def test_relay_runs_read_only_worker_once_parent_persists_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    card = (
        tmp_path
        / "config/research/scheduled_macro_release_causal_reaction_tripwire_v1.json"
    )
    card.parent.mkdir(parents=True)
    card.write_text("{}", encoding="utf-8")
    calls = {"worker": 0, "decision": 0}
    monkeypatch.setattr(runtime, "ProcessPoolExecutor", _ThreadExecutor)
    monkeypatch.setattr(runtime, "_publish", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_write_mission_views", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_kpis", lambda *_args, **_kwargs: {})

    def worker(*_args: Any) -> dict[str, Any]:
        calls["worker"] += 1
        return _result()

    monkeypatch.setattr(runtime, "_scheduled_macro_release_tripwire_worker", worker)
    monkeypatch.setattr(
        runtime,
        "_append_decision_once",
        lambda *_args: calls.__setitem__("decision", calls["decision"] + 1),
    )

    first_state, first_results, first = _run(tmp_path)
    assert calls == {"worker": 1, "decision": 1}
    assert first_state["stage"] == "SCHEDULED_MACRO_RELEASE_CAUSAL_REACTION_FALSIFIED"
    assert first_state["scheduled_macro_release_worker_limit"] == 1
    assert first_state["scheduled_macro_release_parent_writer_only"] is True
    assert first_state["scheduled_macro_release_candidate_screen_count"] == 96
    assert first_state["scheduled_macro_release_tier_q_created_count"] == 0
    assert "POST_BREADTH_SCHEDULED_MACRO_RELEASE" in first_results
    artifact = (
        tmp_path
        / "output/branch_results/post_breadth_portfolio/scheduled_macro_release_tripwire.json"
    )
    assert artifact.is_file()

    second_state, _second_results, second = _run(
        tmp_path, prior_state=first_state
    )
    assert calls == {"worker": 1, "decision": 1}
    assert second["result_hash"] == first["result_hash"]
    assert second_state["scheduled_macro_release_official_event_count"] == 56


@pytest.mark.parametrize("passed", [False, True])
def test_verifier_accepts_only_the_two_frozen_gate_statuses(passed: bool) -> None:
    verified = runtime._verify_scheduled_macro_release_tripwire_result(
        _result(passed=passed)
    )
    assert bool(verified["passing_candidate_ids"]) is passed


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "SCHEDULED_MACRO_RELEASE_UNDECLARED_STATUS"),
        ("tier_q_created", 1),
        ("orders", 1),
        ("q4_access_count_delta", 1),
        ("tombstoned_eia_inventory_grammar_resurrected", True),
    ],
)
def test_verifier_rejects_status_promotion_or_safety_drift(
    field: str, value: object
) -> None:
    result = _result()
    result[field] = value
    result["result_hash"] = stable_hash(
        {key: item for key, item in result.items() if key != "result_hash"}
    )
    with pytest.raises(
        runtime.AutonomousDirectorRuntimeError,
        match="scheduled macro release tripwire identity",
    ):
        runtime._verify_scheduled_macro_release_tripwire_result(result)
