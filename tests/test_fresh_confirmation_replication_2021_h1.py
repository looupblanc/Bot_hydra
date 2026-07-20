from __future__ import annotations

import json
from pathlib import Path

from hydra.economic_evolution.schema import stable_hash


ROOT = Path(__file__).resolve().parents[1]
REPORT = (
    ROOT
    / "reports/economic_evolution/fresh_confirmation_replication_2021_h1_v1"
    / "decision_report.json"
)


def _load() -> dict:
    return json.loads(REPORT.read_text(encoding="utf-8"))


def test_replication_decision_report_is_hash_bound_and_fail_closed() -> None:
    report = _load()
    core = dict(report)
    claimed = core.pop("result_hash")
    assert stable_hash(core) == claimed
    assert report["status"] == "SEALED_OVERFIT_CONFIRMATION_FAILURE_BRANCH_CLOSED"
    assert report["economic_verdict"] == "NO_TIER_C_CANDIDATE_FROM_CLEAN_TIER_G_SET"
    assert report["tier_c_promoted"] is False
    assert report["third_confirmation_allowed"] is False
    assert report["retuning_performed"] is False
    assert report["recalibration_performed"] is False
    assert report["q4_access_count_delta"] == 0
    assert report["broker_connections"] == 0
    assert report["orders"] == 0


def test_replication_keeps_exact_candidate_and_matching_h20_gate() -> None:
    report = _load()
    assert report["selected_candidate_id"] == "hazard_19327ab34a21d623c654a6cc"
    replication = report["second_replication"]
    assert replication["period"] == "2021-01-04:2021-07-01"
    assert replication["batch_stream_equal"] is True
    assert replication["emitted_intent_count"] == replication["completed_event_count"] == 190
    h20 = replication["cells"]["20"]
    assert h20["full_coverage_start_count"] == 4
    assert h20["normal"]["pass_count"] == 0
    assert h20["stressed"]["pass_count"] == 0
    assert h20["normal"]["mll_breach_count"] == 0
    assert h20["stressed"]["mll_breach_count"] == 0
    assert h20["stressed"]["net_total_usd"] < 0.0
    assert replication["tier_c_gate"]["passed"] is False


def test_all_three_clean_tier_g_books_retain_prior_confirmation_failure() -> None:
    report = _load()
    rows = report["audited_clean_tier_g"]
    assert report["audited_clean_tier_g_count"] == len(rows) == 3
    assert {row["candidate_id"] for row in rows} == {
        "hazard_19327ab34a21d623c654a6cc",
        "hazard_1f49e74c20f7bad315dd5cee",
        "hazard_367100adab5fe2a69a4f3257",
    }
    assert all(not row["first_confirmation_2025_h1"]["tier_c_gate_passed"] for row in rows)
    assert sum(bool(row["second_confirmation_selected"]) for row in rows) == 1
    assert report["evidence_roles"]["B1_B4"] == "VIEWED_DEVELOPMENT_NOT_REUSED"
    assert report["evidence_roles"]["Q4"] == "NOT_ACCESSED"
