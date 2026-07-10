#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.budget import (
    AUTO_UNDER_HARD_CAP,
    DatabentoBudgetConfig,
    DatabentoSpendRecord,
    append_spend_record,
    enforce_budget,
    read_ledger,
    request_id_for,
    sha256_file,
    utc_now,
    write_budget_summary,
)
from hydra.data.contract_mapping import build_explicit_roll_map, build_rule_based_roll_map, write_roll_map
from hydra.data.databento_loader import _import_databento, load_api_key
from hydra.data.databento_loader import load_cached_databento_range
from hydra.data.pair_contract_synchronization import audit_pair_synchronization
from hydra.data.roll_audit import audit_roll_discontinuities, compare_roll_maps
from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a canonical Databento explicit contract map for HYDRA.")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--symbols", nargs="+", default=["ES", "MES", "NQ", "MNQ"])
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--end", default="2024-07-01")
    parser.add_argument("--report-tag", default="explicit_contract_map_q1_q2_v1")
    parser.add_argument("--budget-usd", type=float, default=100.0)
    parser.add_argument("--budget-safety-ceiling-usd", type=float, default=98.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    budget = DatabentoBudgetConfig(hard_cap_usd=args.budget_usd, safety_ceiling_usd=args.budget_safety_ceiling_usd)
    key = load_api_key()
    if not key:
        raise RuntimeError("DATABENTO_API_KEY is required for explicit contract metadata.")
    db = _import_databento()
    client = db.Historical(key)
    continuous_symbols = [f"{symbol}.c.0" for symbol in args.symbols]
    continuous_mapping_raw = client.symbology.resolve(
        dataset=args.dataset,
        symbols=continuous_symbols,
        stype_in="continuous",
        stype_out="instrument_id",
        start_date=args.start,
        end_date=args.end,
    )
    continuous_mapping = {symbol: rows for symbol, rows in continuous_mapping_raw.get("result", {}).items()}
    instrument_ids = sorted({str(row["s"]) for rows in continuous_mapping.values() for row in rows}, key=lambda value: int(value))
    raw_mapping_raw = client.symbology.resolve(
        dataset=args.dataset,
        symbols=instrument_ids,
        stype_in="instrument_id",
        stype_out="raw_symbol",
        start_date=args.start,
        end_date=args.end,
    )
    raw_symbol_mapping = _flatten_raw_symbol_mapping(raw_mapping_raw.get("result", {}))
    definition_cache = _definition_cache_paths(args.dataset, args.start, args.end, instrument_ids)
    definition_records, definition_status = load_or_download_definitions(
        client=client,
        budget=budget,
        dataset=args.dataset,
        start=args.start,
        end=args.end,
        instrument_ids=instrument_ids,
        cache_paths=definition_cache,
    )
    explicit_map = build_explicit_roll_map(
        args.symbols,
        start=args.start,
        end=args.end,
        continuous_mapping=continuous_mapping,
        raw_symbol_mapping=raw_symbol_mapping,
        definition_records=definition_records,
        dataset=args.dataset,
        schema=args.schema,
    )
    explicit_path, explicit_hash = write_roll_map(explicit_map)
    rule_map = build_rule_based_roll_map(args.symbols, start=args.start, end=args.end, dataset=args.dataset, schema=args.schema)
    q1 = load_cached_databento_range(args.dataset, args.schema, args.symbols, "2024-01-01", "2024-03-31")
    q2 = load_cached_databento_range(args.dataset, args.schema, args.symbols, "2024-04-01", "2024-07-01")
    combined = pd.concat([q1, q2], ignore_index=True)
    sample = _sample_timestamps_by_symbol(combined, args.symbols)
    comparison = compare_roll_maps(rule_map, explicit_map, sample)
    pair_audits = {
        "NQ_ES": audit_pair_synchronization(explicit_map, _sample_pair_timestamps(combined, ["NQ", "ES"]), left_symbol="NQ", right_symbol="ES"),
        "MNQ_MES": audit_pair_synchronization(explicit_map, _sample_pair_timestamps(combined, ["MNQ", "MES"]), left_symbol="MNQ", right_symbol="MES"),
    }
    roll_audit = audit_roll_discontinuities(combined, explicit_map)
    budget_summary = write_budget_summary(budget)
    summary = {
        "created_at": utc_now_iso(),
        "dataset": args.dataset,
        "schema": args.schema,
        "symbols": args.symbols,
        "period": {"start": args.start, "end": args.end},
        "databento_requests": {
            "symbology_continuous_to_instrument_id": True,
            "symbology_instrument_id_to_raw_symbol": True,
            "definition_timeseries_request": definition_status,
        },
        "new_databento_purchase": definition_status["download_status"] == "DOWNLOADED",
        "spend_this_phase_usd": definition_status["actual_cost_usd"],
        "contract_map_path": str(explicit_path),
        "contract_map_hash": explicit_hash,
        "definition_cache": definition_status,
        "continuous_mapping": continuous_mapping,
        "raw_symbol_mapping": raw_symbol_mapping,
        "roll_map_status": explicit_map.map_type,
        "contracts": [contract.__dict__ for contract in explicit_map.contracts],
        "rule_proxy_comparison": comparison,
        "roll_audit": roll_audit,
        "pair_synchronization": pair_audits,
        "budget_summary_path": str(budget_summary),
    }
    report_path = write_report(summary, args.report_tag)
    print(json.dumps({**summary, "report_path": str(report_path)}, indent=2, sort_keys=True, default=str))
    return 0


def load_or_download_definitions(
    *,
    client: Any,
    budget: DatabentoBudgetConfig,
    dataset: str,
    start: str,
    end: str,
    instrument_ids: list[str],
    cache_paths: dict[str, Path],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    if cache_paths["parsed"].exists():
        records = json.loads(cache_paths["parsed"].read_text(encoding="utf-8"))
        status = {
            "download_status": "CACHE_HIT",
            "actual_cost_usd": 0.0,
            "estimated_cost_usd": 0.0,
            "parsed_path": str(cache_paths["parsed"]),
            "raw_path": str(cache_paths["raw"]),
            "checksum": sha256_file(cache_paths["parsed"]),
        }
        _append_cache_hit(budget, dataset, start, end, instrument_ids, status)
        return records, status
    payload = {
        "dataset": dataset,
        "schema": "definition",
        "symbols": instrument_ids,
        "stype_in": "instrument_id",
        "start": start,
        "end": end,
        "purpose": "explicit contract definitions for Q1-Q2 roll map",
    }
    request_id = request_id_for(payload)
    estimate = float(
        client.metadata.get_cost(
            dataset=dataset,
            start=start,
            end=end,
            symbols=instrument_ids,
            schema="definition",
            stype_in="instrument_id",
        )
    )
    projected, actual = enforce_budget(budget, estimate)
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=request_id,
            timestamp_utc=utc_now(),
            dataset=dataset,
            schema="definition",
            symbols=instrument_ids,
            stype_in="instrument_id",
            start=start,
            end=end,
            estimated_cost_usd=estimate,
            actual_cost_usd=None,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual,
            cache_hit=False,
            research_purpose="explicit contract definitions for Q1-Q2 roll map",
            candidate_tier="data_integrity",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=None,
            checksum=None,
            download_status="ESTIMATED_ONLY",
        ),
    )
    cache_paths["raw"].parent.mkdir(parents=True, exist_ok=True)
    store = client.timeseries.get_range(
        dataset=dataset,
        start=start,
        end=end,
        symbols=instrument_ids,
        schema="definition",
        stype_in="instrument_id",
        stype_out="instrument_id",
        path=cache_paths["raw"],
    )
    frame = store.to_df(pretty_ts=True, map_symbols=False)
    records = parse_definition_frame(frame, instrument_ids)
    cache_paths["parsed"].write_text(json.dumps(records, indent=2, sort_keys=True, default=str), encoding="utf-8")
    checksum = sha256_file(cache_paths["parsed"])
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=request_id,
            timestamp_utc=utc_now(),
            dataset=dataset,
            schema="definition",
            symbols=instrument_ids,
            stype_in="instrument_id",
            start=start,
            end=end,
            estimated_cost_usd=0.0,
            actual_cost_usd=estimate,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual + estimate,
            cache_hit=False,
            research_purpose="explicit contract definitions for Q1-Q2 roll map",
            candidate_tier="data_integrity",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=str(cache_paths["parsed"]),
            checksum=checksum,
            download_status="DOWNLOADED",
        ),
    )
    return records, {
        "download_status": "DOWNLOADED",
        "actual_cost_usd": estimate,
        "estimated_cost_usd": estimate,
        "parsed_path": str(cache_paths["parsed"]),
        "raw_path": str(cache_paths["raw"]),
        "checksum": checksum,
        "record_count": len(frame),
    }


def parse_definition_frame(frame: pd.DataFrame, instrument_ids: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    df = frame.reset_index()
    id_column = "instrument_id" if "instrument_id" in df.columns else "symbol"
    if id_column not in df.columns:
        raise RuntimeError(f"Definition frame has no instrument_id/symbol column. Columns={list(df.columns)}")
    for instrument_id in instrument_ids:
        rows = df[df[id_column].astype(str) == str(instrument_id)]
        if rows.empty:
            continue
        row = rows.sort_values("ts_event" if "ts_event" in rows.columns else rows.columns[0]).iloc[-1].to_dict()
        clean = {}
        for key, value in row.items():
            if pd.isna(value):
                continue
            if hasattr(value, "isoformat"):
                clean[key] = value.isoformat()
            else:
                clean[key] = value.item() if hasattr(value, "item") else value
        out[str(instrument_id)] = clean
    return out


def _flatten_raw_symbol_mapping(raw: dict[str, list[dict[str, Any]]]) -> dict[str, str]:
    out = {}
    for instrument_id, rows in raw.items():
        if rows:
            out[str(instrument_id)] = str(rows[0]["s"])
    return out


def _definition_cache_paths(dataset: str, start: str, end: str, instrument_ids: list[str]) -> dict[str, Path]:
    digest = hashlib.sha256(json.dumps({"dataset": dataset, "start": start, "end": end, "ids": instrument_ids}, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    folder = project_path("data", "cache", "contract_maps")
    return {
        "raw": folder / f"definitions_{dataset.replace('.', '-')}_{start}_{end}_{digest}.dbn.zst",
        "parsed": folder / f"definitions_{dataset.replace('.', '-')}_{start}_{end}_{digest}.json",
    }


def _append_cache_hit(
    budget: DatabentoBudgetConfig,
    dataset: str,
    start: str,
    end: str,
    instrument_ids: list[str],
    status: dict[str, Any],
) -> None:
    payload = {
        "dataset": dataset,
        "schema": "definition",
        "symbols": instrument_ids,
        "stype_in": "instrument_id",
        "start": start,
        "end": end,
        "purpose": "explicit contract definitions for Q1-Q2 roll map",
    }
    projected, actual = enforce_budget(budget, 0.0)
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=request_id_for(payload),
            timestamp_utc=utc_now(),
            dataset=dataset,
            schema="definition",
            symbols=instrument_ids,
            stype_in="instrument_id",
            start=start,
            end=end,
            estimated_cost_usd=0.0,
            actual_cost_usd=0.0,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual,
            cache_hit=True,
            research_purpose="explicit contract definitions for Q1-Q2 roll map",
            candidate_tier="data_integrity",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=status["parsed_path"],
            checksum=status["checksum"],
            download_status="CACHE_HIT",
        ),
    )


def _sample_timestamps_by_symbol(df: pd.DataFrame, symbols: list[str]) -> dict[str, list[Any]]:
    out = {}
    for symbol in symbols:
        subset = df[df["symbol"] == symbol]
        out[symbol] = list(pd.to_datetime(subset["timestamp"], utc=True).iloc[:: max(len(subset) // 500, 1)][:500])
    return out


def _sample_pair_timestamps(df: pd.DataFrame, symbols: list[str]) -> list[Any]:
    subset = df[df["symbol"].isin(symbols)]
    return list(pd.to_datetime(subset["timestamp"], utc=True).iloc[:: max(len(subset) // 500, 1)][:500])


def write_report(summary: dict[str, Any], tag: str) -> Path:
    stamp = utc_now_iso().replace("-", "").replace(":", "").replace("+00:00", "Z")
    path = project_path("reports", "roll_audit", f"explicit_contract_map_{stamp}_{tag}.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Explicit Contract Map {tag}",
        "",
        "Historical research only. No live trading approval.",
        "",
        "```json",
        json.dumps(summary, indent=2, sort_keys=True, default=str)[:120000],
        "```",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


if __name__ == "__main__":
    raise SystemExit(main())
