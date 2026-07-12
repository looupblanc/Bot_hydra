from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

from hydra.data.budget import (
    AUTO_UNDER_HARD_CAP,
    DatabentoBudgetConfig,
    DatabentoSpendRecord,
    append_spend_record,
    cumulative_spend,
    enforce_budget,
    read_ledger,
    request_id_for,
    sha256_file,
    utc_now,
)
from hydra.data.databento_loader import load_api_key
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


PLAN_PATH = "WORM/v7-d1-trades-pilot-2026-07-12.json"
PLAN_SHA256 = "37022f048337f173db9e01997087b998051a4512aa444e46f0f5d31cc45b29f2"
CONTRACT_SHA256 = "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
DATASET = "GLBX.MDP3"
SCHEMA = "trades"
API_SYMBOLS = ("ES.c.0", "MES.c.0")
LOGICAL_SYMBOLS = ("ES", "MES")
START = "2024-08-01T00:00:00Z"
END = "2024-10-01T00:00:00Z"
MAX_REQUEST_COST_USD = 40.41
MINIMUM_REMAINING_USD = 30.0
D1_PHASE_CAP_USD = 60.0
EXPECTED_RECORD_COUNT = 32_277_017
EXPECTED_BILLABLE_SIZE_BYTES = 1_549_296_816
RAW_OUTPUT = (
    "data/cache/databento_v7_d1/"
    "GLBX-MDP3_trades_ES_MES_2024-08-01_2024-10-01.dbn.zst"
)
REPORT_OUTPUT = "reports/v7/data/d1_trades_pilot_result.json"
ACCESS_LEDGER = "reports/data_access/data_access_ledger.jsonl"
PURPOSE = (
    "HYDRA V7 D1 development-only ES/MES aggressor-print pilot for four "
    "preregistered microstructure hypothesis classes; Q4 and forward gap excluded"
)
CANDIDATE_TIER = "V7_PREREGISTERED_HYPOTHESIS_CLASS_PILOT"


class D1TradesError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class OfficialEstimate:
    record_count: int
    estimated_cost_usd: float
    billable_size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AcquisitionAuthorization:
    request_id: str
    allowed: bool
    reason: str
    estimate: OfficialEstimate
    actual_spend_before_usd: float
    estimated_commitments_before_usd: float
    projected_actual_spend_usd: float
    projected_remaining_usd: float
    cache_hit: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def request_kwargs() -> dict[str, Any]:
    return {
        "dataset": DATASET,
        "schema": SCHEMA,
        "symbols": list(API_SYMBOLS),
        "stype_in": "continuous",
        "stype_out": "instrument_id",
        "start": START,
        "end": END,
    }


def verify_frozen_plan(project_root: str | Path = ".") -> dict[str, Any]:
    root = Path(project_root).resolve()
    plan_path = root / PLAN_PATH
    contract_path = root / "MISSION_CONTRACT.md"
    if _sha256(plan_path) != PLAN_SHA256:
        raise D1TradesError("D1 WORM acquisition plan hash mismatch")
    if _sha256(contract_path) != CONTRACT_SHA256:
        raise D1TradesError("mission contract hash mismatch")
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    request = plan.get("request") or {}
    if request.get("data_role") != "DEVELOPMENT_ONLY":
        raise D1TradesError("D1 request is not development-only")
    if request.get("q4_included") or request.get("forward_gap_included"):
        raise D1TradesError("D1 request crosses a protected window")
    return plan


def authorize_estimate(
    estimate: OfficialEstimate,
    *,
    ledger_path: str | Path,
    raw_output_path: str | Path,
    hard_cap_usd: float = 100.0,
) -> AcquisitionAuthorization:
    request_id = request_id_for(_request_identity())
    ledger = read_ledger(ledger_path)
    actual_commitments, actual_spend = cumulative_spend(ledger_path)
    completed = [
        row
        for row in ledger
        if row.get("request_id") == request_id
        and row.get("download_status") in {"DOWNLOADED", "CACHE_HIT"}
    ]
    raw_exists = Path(raw_output_path).is_file()
    if completed and raw_exists:
        return AcquisitionAuthorization(
            request_id=request_id,
            allowed=True,
            reason="VERIFIED_CACHE_HIT",
            estimate=estimate,
            actual_spend_before_usd=actual_spend,
            estimated_commitments_before_usd=actual_commitments,
            projected_actual_spend_usd=actual_spend,
            projected_remaining_usd=hard_cap_usd - actual_spend,
            cache_hit=True,
        )
    if completed != [] or raw_exists:
        raise D1TradesError("D1 cache and persistent spend ledger disagree")
    projected_actual = actual_spend + estimate.estimated_cost_usd
    remaining = hard_cap_usd - projected_actual
    reasons: list[str] = []
    if estimate.record_count <= 0 or estimate.billable_size_bytes <= 0:
        reasons.append("EMPTY_OFFICIAL_ESTIMATE")
    if estimate.estimated_cost_usd > MAX_REQUEST_COST_USD:
        reasons.append("REQUEST_COST_EXCEEDS_WORM_LIMIT")
    if estimate.estimated_cost_usd > D1_PHASE_CAP_USD:
        reasons.append("D1_PHASE_CAP_EXCEEDED")
    if remaining < MINIMUM_REMAINING_USD:
        reasons.append("FINAL_VALIDATION_RESERVE_BREACHED")
    if estimate.record_count != EXPECTED_RECORD_COUNT:
        reasons.append("HISTORICAL_RECORD_COUNT_DRIFT")
    if estimate.billable_size_bytes != EXPECTED_BILLABLE_SIZE_BYTES:
        reasons.append("HISTORICAL_BILLABLE_SIZE_DRIFT")
    return AcquisitionAuthorization(
        request_id=request_id,
        allowed=not reasons,
        reason="AUTHORIZED" if not reasons else reasons[0],
        estimate=estimate,
        actual_spend_before_usd=actual_spend,
        estimated_commitments_before_usd=actual_commitments,
        projected_actual_spend_usd=projected_actual,
        projected_remaining_usd=remaining,
        cache_hit=False,
    )


def acquire_d1_trades(
    *,
    project_root: str | Path = ".",
    budget: DatabentoBudgetConfig | None = None,
    client_factory: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    verify_frozen_plan(root)
    budget = budget or DatabentoBudgetConfig()
    raw_path = root / RAW_OUTPUT
    ledger_path = root / budget.ledger_path
    key = load_api_key()
    if not key:
        raise D1TradesError("DATABENTO_API_KEY is unavailable")
    if client_factory is None:
        import databento as db

        client_factory = db.Historical
    client = client_factory(key)
    metadata_kwargs = request_kwargs()
    metadata_kwargs.pop("stype_out")
    estimate = OfficialEstimate(
        record_count=int(client.metadata.get_record_count(**metadata_kwargs)),
        estimated_cost_usd=float(client.metadata.get_cost(**metadata_kwargs)),
        billable_size_bytes=int(client.metadata.get_billable_size(**metadata_kwargs)),
    )
    authorization = authorize_estimate(
        estimate,
        ledger_path=ledger_path,
        raw_output_path=raw_path,
        hard_cap_usd=budget.hard_cap_usd,
    )
    if not authorization.allowed:
        raise D1TradesError(f"D1 acquisition blocked: {authorization.reason}")
    if authorization.cache_hit:
        return _write_report(
            root,
            authorization=authorization,
            raw_path=raw_path,
            network_request_made=False,
            actual_spend_usd=0.0,
        )
    projected, actual_before = enforce_budget(
        budget, estimate.estimated_cost_usd
    )
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=authorization.request_id,
            timestamp_utc=utc_now(),
            dataset=DATASET,
            schema=SCHEMA,
            symbols=list(LOGICAL_SYMBOLS),
            stype_in="continuous",
            start=START,
            end=END,
            estimated_cost_usd=estimate.estimated_cost_usd,
            actual_cost_usd=None,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual_before,
            cache_hit=False,
            research_purpose=PURPOSE,
            candidate_tier=CANDIDATE_TIER,
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=None,
            checksum=None,
            download_status="ESTIMATED_ONLY",
        ),
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = raw_path.with_name(f".{raw_path.name}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        client.timeseries.get_range(**request_kwargs(), path=str(temporary))
        if not temporary.is_file() or temporary.stat().st_size <= 0:
            raise D1TradesError("Databento returned an empty D1 cache")
        os.replace(temporary, raw_path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    checksum = sha256_file(raw_path)
    _, current_actual = enforce_budget(budget, 0.0)
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=authorization.request_id,
            timestamp_utc=utc_now(),
            dataset=DATASET,
            schema=SCHEMA,
            symbols=list(LOGICAL_SYMBOLS),
            stype_in="continuous",
            start=START,
            end=END,
            estimated_cost_usd=0.0,
            actual_cost_usd=estimate.estimated_cost_usd,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=current_actual
            + estimate.estimated_cost_usd,
            cache_hit=False,
            research_purpose=PURPOSE,
            candidate_tier=CANDIDATE_TIER,
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=str(raw_path),
            checksum=checksum,
            download_status="DOWNLOADED",
        ),
    )
    enforce_data_access(
        period="2024-08-01:2024-10-01_EXCLUSIVE",
        role=DataRole.DEVELOPMENT,
        requesting_module="hydra.data.v7_d1_trades",
        candidate_ids=[
            "D1H1_AGGRESSOR_IMBALANCE_PERSISTENCE",
            "D1H2_PRINT_ABSORPTION_REVERSAL",
            "D1H3_SWEEP_QUALITY_CONTINUATION",
            "D1H4_SESSION_AGGRESSION_STATE_TRANSITION",
        ],
        reason=PURPOSE,
        freeze_manifest_hash=PLAN_SHA256,
        ledger_path=ACCESS_LEDGER,
    )
    return _write_report(
        root,
        authorization=authorization,
        raw_path=raw_path,
        network_request_made=True,
        actual_spend_usd=estimate.estimated_cost_usd,
    )


def _request_identity() -> dict[str, Any]:
    return {
        "dataset": DATASET,
        "schema": SCHEMA,
        "symbols": list(LOGICAL_SYMBOLS),
        "stype_in": "continuous",
        "start": START,
        "end": END,
        "purpose": PURPOSE,
        "candidate_tier": CANDIDATE_TIER,
    }


def _write_report(
    root: Path,
    *,
    authorization: AcquisitionAuthorization,
    raw_path: Path,
    network_request_made: bool,
    actual_spend_usd: float,
) -> dict[str, Any]:
    report = {
        "schema": "hydra_v7_d1_trades_acquisition_result_v1",
        "plan_path": PLAN_PATH,
        "plan_sha256": PLAN_SHA256,
        "request_id": authorization.request_id,
        "authorization": authorization.to_dict(),
        "network_request_made": network_request_made,
        "actual_spend_usd": actual_spend_usd,
        "data_role": "DEVELOPMENT_ONLY",
        "raw_output_path": str(raw_path.relative_to(root)),
        "raw_size_bytes": raw_path.stat().st_size,
        "raw_sha256": sha256_file(raw_path),
        "q4_access_count_delta": 0,
        "proof_window_burn_delta": 0,
        "forward_gap_access_count": 0,
        "outbound_order_count": 0,
        "api_key_logged": False,
        "next_action": "build_explicit_contract_event_bar_manifest_then_preregister_grammar",
        "CONTRE": (
            "The acquired period contains only two development months; high event "
            "count cannot substitute for independent day and regime coverage."
        ),
    }
    output = root / REPORT_OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "AcquisitionAuthorization",
    "D1TradesError",
    "OfficialEstimate",
    "acquire_d1_trades",
    "authorize_estimate",
    "request_kwargs",
    "verify_frozen_plan",
]
