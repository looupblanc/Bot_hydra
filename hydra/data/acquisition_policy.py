from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hydra.data.budget import (
    AUTO_UNDER_HARD_CAP,
    DatabentoBudgetConfig,
    DatabentoSpendRecord,
    append_spend_record,
    enforce_budget,
    request_id_for,
    read_ledger,
    sha256_file,
    utc_now,
)
from hydra.data.databento_loader import DatabentoRequest, estimate_request, load_cached_ohlcv, validate_ohlcv_frame
from hydra.utils.config import project_path


@dataclass(frozen=True)
class AcquisitionDecision:
    request_id: str
    cache_hit: bool
    may_download: bool
    estimated_cost_usd: float
    reason: str
    output_path: str
    checksum: str | None


def audit_cache_for_request(request: DatabentoRequest) -> tuple[bool, str | None, dict[str, Any] | None]:
    path = Path(request.output_path)
    if not path.exists():
        return False, None, None
    frame = load_cached_ohlcv(path, timeframe=request.timeframe)
    stats = validate_ohlcv_frame(frame, timeframe=request.timeframe)
    return True, sha256_file(path), stats


def decide_databento_acquisition(
    request: DatabentoRequest,
    budget: DatabentoBudgetConfig,
    research_purpose: str,
    candidate_tier: str,
    key: str | None,
    estimate: dict[str, Any] | None = None,
) -> AcquisitionDecision:
    payload = {
        "dataset": request.dataset,
        "schema": request.schema,
        "symbols": request.symbols,
        "stype_in": request.stype_in,
        "start": request.start,
        "end": request.end,
        "purpose": research_purpose,
        "candidate_tier": candidate_tier,
    }
    request_id = request_id_for(payload)
    cache_hit, checksum, _stats = audit_cache_for_request(request)
    prior = [
        row
        for row in read_ledger(project_path(budget.ledger_path))
        if row.get("request_id") == request_id and row.get("download_status") in {"DOWNLOADED", "CACHE_HIT"}
    ]
    if prior and not cache_hit:
        return AcquisitionDecision(request_id, True, False, 0.0, "duplicate_request_blocked_by_ledger", request.output_path, None)
    if cache_hit:
        projected, actual = enforce_budget(budget, 0.0)
        append_spend_record(
            budget,
            DatabentoSpendRecord(
                request_id=request_id,
                timestamp_utc=utc_now(),
                dataset=request.dataset,
                schema=request.schema,
                symbols=request.symbols,
                stype_in=request.stype_in,
                start=request.start,
                end=request.end,
                estimated_cost_usd=0.0,
                actual_cost_usd=0.0,
                cumulative_estimated_spend_usd=projected,
                cumulative_actual_spend_usd=actual,
                cache_hit=True,
                research_purpose=research_purpose,
                candidate_tier=candidate_tier,
                approval_mode=AUTO_UNDER_HARD_CAP,
                resulting_file=request.output_path,
                checksum=checksum,
                download_status="CACHE_HIT",
            ),
        )
        return AcquisitionDecision(request_id, True, False, 0.0, "complete_cache_hit", request.output_path, checksum)
    if estimate is None:
        if key is None:
            raise RuntimeError("Databento API key is required for cost estimation before paid acquisition.")
        estimate = estimate_request(request, key)
    estimated_cost = float(estimate["estimated_cost_usd"])
    projected, actual = enforce_budget(budget, estimated_cost)
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=request_id,
            timestamp_utc=utc_now(),
            dataset=request.dataset,
            schema=request.schema,
            symbols=request.symbols,
            stype_in=request.stype_in,
            start=request.start,
            end=request.end,
            estimated_cost_usd=estimated_cost,
            actual_cost_usd=None,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual,
            cache_hit=False,
            research_purpose=research_purpose,
            candidate_tier=candidate_tier,
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=None,
            checksum=None,
            download_status="ESTIMATED_ONLY",
        ),
    )
    return AcquisitionDecision(request_id, False, True, estimated_cost, "cache_miss_under_budget", request.output_path, None)


def record_download_complete(
    request: DatabentoRequest,
    budget: DatabentoBudgetConfig,
    request_id: str,
    estimated_cost_usd: float,
    actual_cost_usd: float | None,
    research_purpose: str,
    candidate_tier: str,
    resulting_file: str | None = None,
) -> DatabentoSpendRecord:
    final_file = resulting_file or request.output_path
    checksum = sha256_file(final_file) if final_file and Path(final_file).exists() else None
    projected, current_actual = enforce_budget(budget, 0.0)
    actual = float(actual_cost_usd if actual_cost_usd is not None else estimated_cost_usd)
    record = DatabentoSpendRecord(
        request_id=request_id,
        timestamp_utc=utc_now(),
        dataset=request.dataset,
        schema=request.schema,
        symbols=request.symbols,
        stype_in=request.stype_in,
        start=request.start,
        end=request.end,
        estimated_cost_usd=0.0,
        actual_cost_usd=actual,
        cumulative_estimated_spend_usd=projected,
        cumulative_actual_spend_usd=current_actual + actual,
        cache_hit=False,
        research_purpose=research_purpose,
        candidate_tier=candidate_tier,
        approval_mode=AUTO_UNDER_HARD_CAP,
        resulting_file=final_file,
        checksum=checksum,
        download_status="DOWNLOADED",
    )
    append_spend_record(budget, record)
    return record
