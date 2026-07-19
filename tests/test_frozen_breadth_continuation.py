from __future__ import annotations

from dataclasses import dataclass

from hydra.economic_evolution.schema import stable_hash
from hydra.production.frozen_breadth_continuation import (
    ACCOUNT_SIZE_TIERS,
    DATA_ROLE,
    END,
    START,
    SYMBOLS,
    decide_tier_g_gate,
    freeze_breadth_continuation_contract,
    frozen_data_request,
    validate_acquisition_receipt,
)
from scripts.acquire_frozen_breadth_q3 import estimate_or_acquire
from hydra.data.budget import DatabentoBudgetConfig


def test_real_source_freeze_is_singleton_and_caps_evidence_at_tier_g() -> None:
    contract = freeze_breadth_continuation_contract(".")
    assert contract["status"] == "FROZEN_AWAITING_UNTOUCHED_Q3_ACQUISITION"
    assert contract["evidence_ceiling"] == "TIER_G_DEVELOPMENT"
    assert contract["tier_c_permitted"] is False
    assert contract["source_development"]["shared_normal_stressed_pass_blocks"] == [
        "B3",
        "B4",
    ]
    assert {
        row["account_label"]: row["integer_quantity_tier"]
        for row in contract["account_size_matrix"]
    } == ACCOUNT_SIZE_TIERS
    core = dict(contract)
    claimed = core.pop("contract_hash")
    assert stable_hash(core) == claimed


def test_q3_request_is_exact_and_excludes_q4() -> None:
    request = frozen_data_request()
    assert request["symbols"] == list(SYMBOLS)
    assert request["start"] == START == "2025-07-01"
    assert request["end"] == END == "2025-10-01"
    assert request["data_role"] == DATA_ROLE
    assert request["q4_access_allowed"] is False
    assert request["official_estimated_cost_usd"]["total"] == 1.586562249810


def _cell(*, normal_passes: int, stressed_passes: int, stressed_net: float, mll: float):
    return {
        "horizon_trading_days": 10,
        "full_coverage_start_count": 6,
        "normal": {"pass_count": normal_passes},
        "stressed": {
            "pass_count": stressed_passes,
            "net_total_usd": stressed_net,
            "mll_breach_rate": mll,
            "all_passing_paths_consistency_compliant": stressed_passes > 0,
        },
    }


def test_tier_g_gate_passes_but_never_permits_tier_c() -> None:
    gate = decide_tier_g_gate(
        [_cell(normal_passes=1, stressed_passes=1, stressed_net=10.0, mll=0.0)],
        source_pass_blocks=["B3", "B4"],
    )
    assert gate["passed"] is True
    assert gate["evidence_ceiling"] == "TIER_G_DEVELOPMENT"
    assert gate["tier_c_permitted"] is False


def test_tier_g_gate_fails_on_mll_or_single_source_block() -> None:
    unsafe = decide_tier_g_gate(
        [_cell(normal_passes=1, stressed_passes=1, stressed_net=10.0, mll=0.11)],
        source_pass_blocks=["B4"],
    )
    assert unsafe["passed"] is False
    assert unsafe["checks"]["controlled_stressed_mll"] is False
    assert unsafe["checks"]["source_block_diverse"] is False


def test_acquisition_receipt_must_match_frozen_request() -> None:
    contract = freeze_breadth_continuation_contract(".")
    receipt = {
        "request": {
            key: contract["data_request"][key]
            for key in ("dataset", "schema", "symbols", "stype_in", "start", "end")
        },
        "data_role": DATA_ROLE,
        "actual_cost_usd": 1.5,
        "files": [{"path": "/immutable", "sha256": "a" * 64}],
    }
    reconciled = validate_acquisition_receipt(contract, receipt)
    assert reconciled["status"] == "BREADTH_Q3_ACQUISITION_RECONCILED"


class _Metadata:
    def get_cost(self, **request):
        return 1.5 if request["schema"] == "ohlcv-1m" else 0.001

    def get_record_count(self, **request):
        return 100 if request["schema"] == "ohlcv-1m" else 10

    def get_billable_size(self, **request):
        return 1000 if request["schema"] == "ohlcv-1m" else 100


class _Symbology:
    def resolve(self, **request):
        if request["stype_out"] == "instrument_id":
            return {
                "result": {
                    symbol: [{"s": str(index + 1)}]
                    for index, symbol in enumerate(request["symbols"])
                }
            }
        return {
            "result": {
                symbol: [{"s": f"RAW{symbol}"}] for symbol in request["symbols"]
            }
        }


@dataclass
class _Client:
    metadata: _Metadata = _Metadata()
    symbology: _Symbology = _Symbology()


def test_dry_run_reestimates_exact_five_symbol_request_without_download(tmp_path) -> None:
    contract = freeze_breadth_continuation_contract(".")
    result = estimate_or_acquire(
        contract=contract,
        root=tmp_path,
        client=_Client(),
        execute=False,
        budget=DatabentoBudgetConfig(
            ledger_path=str(tmp_path / "spend.jsonl"),
            summary_path=str(tmp_path / "summary.md"),
        ),
        receipt_path=tmp_path / "receipt.json",
    )
    assert result["download_status"] == "DRY_RUN_ONLY"
    assert result["request"]["symbols"] == list(SYMBOLS)
    assert result["aggregate_live_estimate_usd"] == 1.501
    assert result["estimated_records"] == {"ohlcv": 100, "definition": 10}
    assert result["execute"] is False
