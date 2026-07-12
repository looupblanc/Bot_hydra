from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

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
from hydra.data.databento_loader import (
    DatabentoRequest,
    _import_databento,
    load_api_key,
    request_from_config,
)
from hydra.governance.q4_one_shot import AuthorizedQ4Capability
from hydra.utils.config import project_path


PLAN_SCHEMA = "hydra_q4_databento_plan_v1"
MINIMUM_RESERVED_BUDGET_USD = 30.0
ROOT_GROUPS = (("NQ", "MNQ"), ("CL", "MCL"), ("ES", "MES"), ("YM", "MYM"), ("RTY", "M2K"), ("GC", "MGC"))


class Q4DataPlanError(RuntimeError):
    pass


def build_q4_data_plan(
    manifest: Mapping[str, Any],
    *,
    cache_root: str | Path = "data/cache/databento",
    budget: DatabentoBudgetConfig | None = None,
    client: Any | None = None,
    record_request_plans: bool = False,
) -> dict[str, Any]:
    if int(manifest.get("q4_access_count_before") or 0) != 0:
        raise Q4DataPlanError("Q4 data plan requires zero prior access.")
    required_roots = {
        str(value)
        for candidate in manifest.get("candidates") or []
        for value in (
            candidate.get("primary_market"),
            candidate.get("execution_market"),
        )
        if value
    }
    groups = [group for group in ROOT_GROUPS if required_roots & set(group)]
    planned_roots = {root for group in groups for root in group}
    if required_roots - planned_roots:
        raise Q4DataPlanError(
            f"Unsupported Q4 root set: {sorted(required_roots - planned_roots)}"
        )
    if client is None:
        key = load_api_key()
        if not key:
            raise Q4DataPlanError("Databento API key is required for official Q4 estimates.")
        client = _import_databento().Historical(key)
    cfg = budget or DatabentoBudgetConfig()
    sources: list[dict[str, Any]] = []
    all_instrument_ids: set[str] = set()
    for group in groups:
        request = request_from_config(
            {
                "data": {
                    "databento": {
                        "dataset": "GLBX.MDP3",
                        "schema": "ohlcv-1m",
                        "symbols": list(group),
                        "start_date": "2024-10-01",
                        "end_date": "2025-01-01",
                        "cache_folder": str(cache_root),
                        "stype_in": "continuous",
                        "stype_out": "instrument_id",
                    }
                }
            }
        )
        raw_path = project_path(request.raw_output_path)
        if not raw_path.is_file():
            superset = _find_raw_superset_cache(Path(cache_root), group)
            if superset is not None:
                raw_path = superset
        cache_hit = raw_path.is_file()
        estimate = 0.0 if cache_hit else float(
            client.metadata.get_cost(
                dataset=request.dataset,
                start=request.start,
                end=request.end,
                symbols=request.api_symbols,
                schema=request.schema,
                stype_in=request.stype_in,
            )
        )
        mapping = client.symbology.resolve(
            dataset=request.dataset,
            symbols=request.api_symbols,
            stype_in="continuous",
            stype_out="instrument_id",
            start_date=request.start,
            end_date=request.end,
        )
        instrument_ids = sorted(
            {
                str(row["s"])
                for intervals in (mapping.get("result") or {}).values()
                for row in intervals
            },
            key=int,
        )
        if not instrument_ids:
            raise Q4DataPlanError(f"No Q4 instrument mapping for {group}.")
        all_instrument_ids.update(instrument_ids)
        request_id = request_id_for(
            {
                "dataset": request.dataset,
                "schema": request.schema,
                "symbols": request.symbols,
                "start": request.start,
                "end": request.end,
                "purpose": "atomic_q4_one_shot",
            }
        )
        source = {
            "request_id": request_id,
            "request": request.to_dict(),
            "roots": list(group),
            "instrument_ids": instrument_ids,
            "raw_path": str(raw_path),
            "cache_hit": cache_hit,
            "cache_sha256": sha256_file(raw_path) if cache_hit else None,
            "official_estimated_cost_usd": estimate,
        }
        sources.append(source)
        if record_request_plans and not cache_hit:
            _record_estimate_once(
                cfg,
                request_id=request_id,
                dataset=request.dataset,
                schema=request.schema,
                symbols=request.symbols,
                stype_in=request.stype_in,
                start=request.start,
                end=request.end,
                estimate=estimate,
                purpose="manifest-bound Q4 OHLCV one-shot decision",
            )
    definition_ids = sorted(all_instrument_ids, key=int)
    definition_name = hashlib.sha256(
        "|".join(definition_ids).encode()
    ).hexdigest()[:20]
    definition_path = project_path(cache_root) / (
        f"GLBX-MDP3_definition_q4_{definition_name}_2024-10-01_2025-01-01.dbn.zst"
    )
    definition_cache_hit = definition_path.is_file()
    definition_estimate = 0.0 if definition_cache_hit else float(
        client.metadata.get_cost(
            dataset="GLBX.MDP3",
            start="2024-10-01",
            end="2025-01-01",
            symbols=definition_ids,
            schema="definition",
            stype_in="instrument_id",
        )
    )
    definition_request_id = request_id_for(
        {
            "dataset": "GLBX.MDP3",
            "schema": "definition",
            "symbols": definition_ids,
            "start": "2024-10-01",
            "end": "2025-01-01",
            "purpose": "atomic_q4_explicit_contract_definitions",
        }
    )
    if record_request_plans and not definition_cache_hit:
        _record_estimate_once(
            cfg,
            request_id=definition_request_id,
            dataset="GLBX.MDP3",
            schema="definition",
            symbols=definition_ids,
            stype_in="instrument_id",
            start="2024-10-01",
            end="2025-01-01",
            estimate=definition_estimate,
            purpose="manifest-bound Q4 explicit contract definitions",
        )
    total_estimate = sum(float(row["official_estimated_cost_usd"]) for row in sources) + definition_estimate
    _enforce_reserved_budget(cfg, total_estimate if not record_request_plans else 0.0)
    plan: dict[str, Any] = {
        "schema": PLAN_SCHEMA,
        "cohort_id": manifest["cohort_id"],
        "cohort_manifest_hash": manifest["manifest_hash"],
        "period": ["2024-10-01", "2025-01-01"],
        "sources": sources,
        "definitions": {
            "request_id": definition_request_id,
            "instrument_ids": definition_ids,
            "raw_path": str(definition_path),
            "cache_hit": definition_cache_hit,
            "cache_sha256": sha256_file(definition_path) if definition_cache_hit else None,
            "official_estimated_cost_usd": definition_estimate,
        },
        "official_total_estimated_cost_usd": total_estimate,
        "minimum_reserved_budget_usd": MINIMUM_RESERVED_BUDGET_USD,
        "request_plans_recorded": record_request_plans,
        "data_decoded_or_inspected": False,
    }
    plan["plan_hash"] = _stable_hash(plan)
    return plan


def acquire_q4_data(
    plan: Mapping[str, Any],
    capability: AuthorizedQ4Capability,
    *,
    budget: DatabentoBudgetConfig | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    capability.validate_scope()
    semantic = dict(plan)
    expected_hash = str(semantic.pop("plan_hash", ""))
    if _stable_hash(semantic) != expected_hash:
        raise Q4DataPlanError("Q4 data plan semantic hash is invalid.")
    if str(plan.get("cohort_manifest_hash")) != capability.cohort_manifest_hash:
        raise Q4DataPlanError("Q4 data plan/capability mismatch.")
    if client is None:
        key = load_api_key()
        if not key:
            raise Q4DataPlanError("Databento API key is unavailable during Q4 acquisition.")
        client = _import_databento().Historical(key)
    cfg = budget or DatabentoBudgetConfig()
    receipts: list[dict[str, Any]] = []
    for source in plan.get("sources") or []:
        request = DatabentoRequest(**dict(source["request"]))
        raw_path = Path(str(source["raw_path"]))
        if raw_path.is_file():
            expected = str(source.get("cache_sha256") or "")
            if expected and sha256_file(raw_path) != expected:
                raise Q4DataPlanError("Q4 raw cache changed after the frozen plan.")
            receipts.append(
                {
                    "request_id": source["request_id"],
                    "path": str(raw_path),
                    "sha256": sha256_file(raw_path),
                    "cache_hit": True,
                    "actual_cost_usd": 0.0,
                }
            )
            continue
        current_estimate = float(
            client.metadata.get_cost(
                dataset=request.dataset,
                start=request.start,
                end=request.end,
                symbols=request.api_symbols,
                schema=request.schema,
                stype_in=request.stype_in,
            )
        )
        _assert_estimate_within_plan(
            current_estimate, float(source["official_estimated_cost_usd"])
        )
        _enforce_reserved_budget(cfg, 0.0)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = raw_path.with_name(f".{raw_path.stem}.{os.getpid()}.dbn.zst")
        client.timeseries.get_range(
            dataset=request.dataset,
            start=request.start,
            end=request.end,
            symbols=request.api_symbols,
            schema=request.schema,
            stype_in=request.stype_in,
            stype_out=request.stype_out,
            path=temporary,
        )
        os.replace(temporary, raw_path)
        _record_download_once(
            cfg,
            request_id=str(source["request_id"]),
            dataset=request.dataset,
            schema=request.schema,
            symbols=request.symbols,
            stype_in=request.stype_in,
            start=request.start,
            end=request.end,
            actual=current_estimate,
            purpose="manifest-bound Q4 OHLCV one-shot decision",
            resulting_file=raw_path,
        )
        receipts.append(
            {
                "request_id": source["request_id"],
                "path": str(raw_path),
                "sha256": sha256_file(raw_path),
                "cache_hit": False,
                "actual_cost_usd": current_estimate,
            }
        )
    definitions = dict(plan["definitions"])
    definition_path = Path(str(definitions["raw_path"]))
    definition_cost = 0.0
    if definition_path.is_file():
        expected = str(definitions.get("cache_sha256") or "")
        if expected and sha256_file(definition_path) != expected:
            raise Q4DataPlanError("Q4 definition cache changed after the frozen plan.")
    else:
        definition_cost = float(
            client.metadata.get_cost(
                dataset="GLBX.MDP3",
                start="2024-10-01",
                end="2025-01-01",
                symbols=list(definitions["instrument_ids"]),
                schema="definition",
                stype_in="instrument_id",
            )
        )
        _assert_estimate_within_plan(
            definition_cost, float(definitions["official_estimated_cost_usd"])
        )
        _enforce_reserved_budget(cfg, 0.0)
        definition_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = definition_path.with_name(
            f".{definition_path.stem}.{os.getpid()}.dbn.zst"
        )
        client.timeseries.get_range(
            dataset="GLBX.MDP3",
            start="2024-10-01",
            end="2025-01-01",
            symbols=list(definitions["instrument_ids"]),
            schema="definition",
            stype_in="instrument_id",
            stype_out="instrument_id",
            path=temporary,
        )
        os.replace(temporary, definition_path)
        _record_download_once(
            cfg,
            request_id=str(definitions["request_id"]),
            dataset="GLBX.MDP3",
            schema="definition",
            symbols=list(definitions["instrument_ids"]),
            stype_in="instrument_id",
            start="2024-10-01",
            end="2025-01-01",
            actual=definition_cost,
            purpose="manifest-bound Q4 explicit contract definitions",
            resulting_file=definition_path,
        )
    return {
        "schema": "hydra_q4_data_acquisition_receipt_v1",
        "cohort_id": plan["cohort_id"],
        "sources": receipts,
        "definitions": {
            "path": str(definition_path),
            "sha256": sha256_file(definition_path),
            "cache_hit": bool(definitions.get("cache_hit")),
            "actual_cost_usd": definition_cost,
        },
        "actual_incremental_cost_usd": sum(
            float(row["actual_cost_usd"]) for row in receipts
        )
        + definition_cost,
    }


def _record_estimate_once(
    config: DatabentoBudgetConfig,
    *,
    request_id: str,
    dataset: str,
    schema: str,
    symbols: list[str],
    stype_in: str,
    start: str,
    end: str,
    estimate: float,
    purpose: str,
) -> None:
    ledger = project_path(config.ledger_path)
    if any(
        row.get("request_id") == request_id
        and row.get("download_status") in {"ESTIMATED_ONLY", "DOWNLOADED", "CACHE_HIT"}
        for row in read_ledger(ledger)
    ):
        return
    projected, actual = enforce_budget(config, estimate)
    append_spend_record(
        config,
        DatabentoSpendRecord(
            request_id=request_id,
            timestamp_utc=utc_now(),
            dataset=dataset,
            schema=schema,
            symbols=list(symbols),
            stype_in=stype_in,
            start=start,
            end=end,
            estimated_cost_usd=estimate,
            actual_cost_usd=None,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual,
            cache_hit=False,
            research_purpose=purpose,
            candidate_tier="FINAL_PRE_HOLDOUT_COHORT",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=None,
            checksum=None,
            download_status="ESTIMATED_ONLY",
        ),
    )


def _record_download_once(
    config: DatabentoBudgetConfig,
    *,
    request_id: str,
    dataset: str,
    schema: str,
    symbols: list[str],
    stype_in: str,
    start: str,
    end: str,
    actual: float,
    purpose: str,
    resulting_file: Path,
) -> None:
    ledger = project_path(config.ledger_path)
    if any(
        row.get("request_id") == request_id
        and row.get("download_status") == "DOWNLOADED"
        for row in read_ledger(ledger)
    ):
        return
    estimated, current_actual = cumulative_spend(ledger)
    append_spend_record(
        config,
        DatabentoSpendRecord(
            request_id=request_id,
            timestamp_utc=utc_now(),
            dataset=dataset,
            schema=schema,
            symbols=list(symbols),
            stype_in=stype_in,
            start=start,
            end=end,
            estimated_cost_usd=0.0,
            actual_cost_usd=actual,
            cumulative_estimated_spend_usd=estimated,
            cumulative_actual_spend_usd=current_actual + actual,
            cache_hit=False,
            research_purpose=purpose,
            candidate_tier="FINAL_PRE_HOLDOUT_COHORT",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=str(resulting_file),
            checksum=sha256_file(resulting_file),
            download_status="DOWNLOADED",
        ),
    )


def _enforce_reserved_budget(
    config: DatabentoBudgetConfig, incremental_estimate: float
) -> None:
    estimated, actual = cumulative_spend(project_path(config.ledger_path))
    projected = max(estimated, actual) + float(incremental_estimate)
    if config.hard_cap_usd - projected < MINIMUM_RESERVED_BUDGET_USD:
        raise Q4DataPlanError(
            "Q4 request would violate the USD 30 final-lockbox/execution reserve."
        )
    enforce_budget(config, incremental_estimate)


def _assert_estimate_within_plan(current: float, planned: float) -> None:
    if current > planned * 1.10 + 0.01:
        raise Q4DataPlanError(
            f"Official Q4 cost drifted from ${planned:.6f} to ${current:.6f}."
        )


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _find_raw_superset_cache(
    cache_root: Path, required_roots: tuple[str, ...]
) -> Path | None:
    required = set(required_roots)
    prefix = "GLBX-MDP3_ohlcv-1m_"
    suffix = "_2024-10-01_2025-01-01.dbn.zst"
    matches: list[tuple[int, Path]] = []
    for path in Path(cache_root).glob(f"{prefix}*{suffix}"):
        name = path.name
        if not name.startswith(prefix) or not name.endswith(suffix):
            continue
        middle = name[len(prefix) : -len(suffix)]
        roots = set(middle.split("_"))
        if required <= roots:
            matches.append((len(roots), path))
    return min(matches, default=(0, None), key=lambda row: (row[0], str(row[1])))[1]
