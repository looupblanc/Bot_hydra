import json
from pathlib import Path

from hydra.economic_evolution.schema import stable_hash


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports/economic_evolution/fresh_two_book_confirmation_2019h1_v1"
BOOK_IDS = {
    "autonomous_marginal_book_b09b8e7b30f90b34737eb724",
    "autonomous_marginal_book_2f3752128ff0fd44a71b2327",
}


def _read(name: str) -> dict:
    return json.loads((REPORT / name).read_text(encoding="utf-8"))


def _assert_hash(value: dict, key: str) -> None:
    core = dict(value)
    claimed = core.pop(key)
    assert stable_hash(core) == claimed


def test_spread_filter_keeps_only_complete_outright_signal_roots() -> None:
    acquisition = _read("acquisition_receipt.json")
    feature = _read("feature_receipt.json")
    _assert_hash(acquisition, "receipt_hash")
    _assert_hash(feature, "result_hash")

    assert acquisition["normalization"]["classification"] == (
        "NON_OUTRIGHT_OR_PRELAUNCH_ALIAS_IGNORED_BEFORE_ROLL_MAP"
    )
    assert {"7849", "7859"}.issubset(
        set(acquisition["normalization"]["rejected_instrument_ids"])
    )
    assert acquisition["normalization"]["rejected_record_count"] > 0
    assert set(feature["segment_coverage"]) == {"ES", "NQ", "YM", "RTY", "CL"}
    assert all(not row["gaps"] for row in feature["segment_coverage"].values())
    assert all(
        all(row["checks"].values()) for row in feature["outright_definition_checks"]
    )
    assert feature["micro_proxy_feature_reads"] == 0
    assert feature["micro_execution_aliases_are_accounting_only"] is True


def test_confirmation_is_one_shot_and_preserves_two_book_identity() -> None:
    contract = _read("contract.json")
    result = _read("economic_result.json")
    decision = _read("decision_report.json")
    _assert_hash(contract, "contract_hash")
    _assert_hash(result, "result_hash")
    _assert_hash(decision, "result_hash")

    assert {row["policy_id"] for row in contract["books"]} == BOOK_IDS
    assert {row["policy_id"] for row in result["book_results"]} == BOOK_IDS
    assert result["packet_reuse_allowed"] is False
    assert result["retuning_performed"] is False
    assert result["recalibration_performed"] is False
    assert result["q4_access_count_delta"] == 0
    assert result["broker_connections"] == 0
    assert result["orders"] == 0
    for row in result["book_results"]:
        frozen = next(
            book for book in contract["books"] if book["policy_id"] == row["policy_id"]
        )
        assert row["policy_spec_hash"] == frozen["policy_spec_hash"]
        assert row["component_ids"] == frozen["component_ids"]
        assert row["book_or_component_mutated"] is False

