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


PLAN_PATH = "WORM/v7-d1-trades-year-control-extension-2026-07-12.json"
PLAN_SHA256 = "e816f5802c3b7cc1520ce65861e8a6ce69356cd12efcc6ce83129fec2a081080"
CONTRACT_SHA256 = "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
DATASET = "GLBX.MDP3"
SCHEMA = "trades"
API_SYMBOLS = ("ES.c.0", "MES.c.0")
LOGICAL_SYMBOLS = ("ES", "MES")
START = "2023-08-02T00:00:00Z"
END = "2023-09-01T00:00:00Z"
MAX_REQUEST_COST_USD = 19.46
MINIMUM_REMAINING_USD = 30.0
D1_PHASE_CAP_USD = 60.0
EXPECTED_RECORD_COUNT = 15_545_497
EXPECTED_BILLABLE_SIZE_BYTES = 746_183_856
EXPECTED_COST_USD = 19.45826035738
RAW_OUTPUT = (
    "data/cache/databento_v7_d1/"
    "GLBX-MDP3_trades_ES_MES_2023-08-02_2023-09-01.dbn.zst"
)
REPORT_OUTPUT = "reports/v7/data/d1_year_control_extension_result.json"
ACCESS_LEDGER = "reports/data_access/data_access_ledger.jsonl"
PURPOSE = (
    "HYDRA V7 D1 development-only ES/MES second-year aggressor-print window "
    "for mandatory year-block null control; Q4 and forward gap excluded"
)
CANDIDATE_TIER = "V7_D1_YEAR_CONTROL_EXTENSION"
ORIGINAL_D1_TIER = "V7_PREREGISTERED_HYPOTHESIS_CLASS_PILOT"


class D1YearControlError(RuntimeError):
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
    d1_spend_before_usd: float
    projected_actual_spend_usd: float
    projected_d1_spend_usd: float
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
    if _sha256(root / PLAN_PATH) != PLAN_SHA256:
        raise D1YearControlError("D1 year-control WORM plan hash mismatch")
    if _sha256(root / "MISSION_CONTRACT.md") != CONTRACT_SHA256:
        raise D1YearControlError("mission contract hash mismatch")
    plan = json.loads((root / PLAN_PATH).read_text(encoding="utf-8"))
    request = plan.get("request") or {}
    if request.get("data_role") != "DEVELOPMENT_ONLY":
        raise D1YearControlError("D1 year-control request is not development-only")
    if request.get("q4_included") or request.get("forward_gap_included"):
        raise D1YearControlError("D1 year-control request crosses protected data")
    return plan


def authorize_estimate(
    estimate: OfficialEstimate,
    *,
    ledger_path: str | Path,
    raw_output_path: str | Path,
    hard_cap_usd: float = 125.0,
) -> AcquisitionAuthorization:
    request_id = request_id_for(_request_identity())
    ledger = read_ledger(ledger_path)
    _estimated, actual_spend = cumulative_spend(ledger_path)
    d1_spend = sum(
        float(row.get("actual_cost_usd") or 0.0)
        for row in ledger
        if row.get("candidate_tier") in {ORIGINAL_D1_TIER, CANDIDATE_TIER}
    )
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
            d1_spend_before_usd=d1_spend,
            projected_actual_spend_usd=actual_spend,
            projected_d1_spend_usd=d1_spend,
            projected_remaining_usd=hard_cap_usd - actual_spend,
            cache_hit=True,
        )
    if completed or raw_exists:
        raise D1YearControlError("D1 year-control cache and ledger disagree")
    projected_actual = actual_spend + estimate.estimated_cost_usd
    projected_d1 = d1_spend + estimate.estimated_cost_usd
    remaining = hard_cap_usd - projected_actual
    reasons: list[str] = []
    if estimate.record_count != EXPECTED_RECORD_COUNT:
        reasons.append("HISTORICAL_RECORD_COUNT_DRIFT")
    if estimate.billable_size_bytes != EXPECTED_BILLABLE_SIZE_BYTES:
        reasons.append("HISTORICAL_BILLABLE_SIZE_DRIFT")
    if abs(estimate.estimated_cost_usd - EXPECTED_COST_USD) > 1.0e-9:
        reasons.append("OFFICIAL_COST_DRIFT")
    if estimate.estimated_cost_usd > MAX_REQUEST_COST_USD:
        reasons.append("REQUEST_COST_EXCEEDS_WORM_LIMIT")
    if projected_d1 > D1_PHASE_CAP_USD:
        reasons.append("D1_PHASE_CAP_EXCEEDED")
    if remaining < MINIMUM_REMAINING_USD:
        reasons.append("FINAL_VALIDATION_RESERVE_BREACHED")
    return AcquisitionAuthorization(
        request_id=request_id,
        allowed=not reasons,
        reason="AUTHORIZED" if not reasons else reasons[0],
        estimate=estimate,
        actual_spend_before_usd=actual_spend,
        d1_spend_before_usd=d1_spend,
        projected_actual_spend_usd=projected_actual,
        projected_d1_spend_usd=projected_d1,
        projected_remaining_usd=remaining,
        cache_hit=False,
    )


def acquire_d1_year_control(
    *,
    project_root: str | Path = ".",
    budget: DatabentoBudgetConfig | None = None,
    client_factory: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    verify_frozen_plan(root)
    budget = budget or DatabentoBudgetConfig()
    if budget.hard_cap_usd < 125.0 or budget.safety_ceiling_usd < 123.0:
        raise D1YearControlError("constitution data budget is not active")
    raw_path = root / RAW_OUTPUT
    ledger_path = root / budget.ledger_path
    key = load_api_key()
    if not key:
        raise D1YearControlError("DATABENTO_API_KEY is unavailable")
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
        raise D1YearControlError(
            f"D1 year-control acquisition blocked: {authorization.reason}"
        )
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
            raise D1YearControlError("Databento returned an empty D1 cache")
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
        period="2023-08-02:2023-09-01_EXCLUSIVE",
        role=DataRole.DEVELOPMENT,
        requesting_module="hydra.data.v7_d1_year_control",
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
        "schema": "hydra_v7_d1_year_control_acquisition_result_v1",
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
        "next_action": "build_date_matched_D1_event_features_and_run_new_dataset_tripwire",
        "CONTRE": (
            "Only two August year blocks are available; a GREEN tripwire can "
            "reject gross geometry artefacts but cannot establish long-run "
            "microstructure stability."
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
    "D1YearControlError",
    "OfficialEstimate",
    "acquire_d1_year_control",
    "authorize_estimate",
    "request_kwargs",
    "verify_frozen_plan",
]
