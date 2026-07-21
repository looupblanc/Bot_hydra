#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[1]
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))


MANIFEST_PATH = Path("config/research/treasury_auction_demand_shock_tripwire_v1.json")
NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def role_for(day: str, roles: dict[str, list[str]]) -> str | None:
    for role, (start, end) in roles.items():
        if start <= day < end:
            return role
    return None


def acquire_json(cache: Path, label: str, url: str) -> tuple[list[dict], str]:
    cache.mkdir(parents=True, exist_ok=True)
    path = cache / f"{label}.json"
    if not path.exists():
        request = Request(url, headers={"User-Agent": "HYDRA-research/1.0"})
        with urlopen(request, timeout=30) as response:
            payload = response.read()
        parsed = json.loads(payload)
        path.write_bytes(canonical(parsed) + b"\n")
    records = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise RuntimeError(f"official {label} response is not a list")
    return records, digest_file(path)


def parse_available(record: dict) -> pd.Timestamp:
    updated = str(record.get("updatedTimestamp", ""))
    if not updated:
        raise RuntimeError("auction result lacks updatedTimestamp")
    value = pd.Timestamp(updated)
    if value.tzinfo is None:
        value = value.tz_localize(NY)
    return value.tz_convert(UTC)


def load_manifest(root: Path) -> dict:
    path = root / MANIFEST_PATH
    manifest = json.loads(path.read_text(encoding="utf-8"))
    core = dict(manifest)
    claimed = core.pop("manifest_hash")
    actual = hashlib.sha256(canonical(core)).hexdigest()
    if actual != claimed:
        raise RuntimeError("manifest hash drift")
    for item in manifest["frozen_price_inputs"].values():
        source = root / item["path"]
        if digest_file(source) != item["sha256"]:
            raise RuntimeError(f"price input hash drift: {source}")
    rule = manifest["official_rule_snapshot"]
    if digest_file(root / rule["path"]) != rule["sha256"]:
        raise RuntimeError("rule snapshot hash drift")
    return manifest


def event_rows(manifest: dict, records: list[dict]) -> list[dict]:
    allowed = set(manifest["official_event_sources"]["allowed_terms"])
    mapping = manifest["term_to_market"]
    roles = manifest["chronological_roles"]
    selected: dict[tuple[str, str], dict] = {}
    for record in records:
        term = str(record.get("securityTerm", ""))
        day = str(record.get("auctionDate", ""))[:10]
        role = role_for(day, roles)
        if term not in allowed or role is None:
            continue
        key = (str(record.get("cusip", "")), day)
        row = {
            "event_id": hashlib.sha256(f"{key[0]}|{day}|{term}".encode()).hexdigest()[:24],
            "cusip": key[0],
            "auction_date": day,
            "role": role,
            "term": term,
            "market": mapping[term],
            "available_at": parse_available(record),
        }
        prior = selected.get(key)
        if prior is None or row["available_at"] > prior["available_at"]:
            selected[key] = row
    return sorted(selected.values(), key=lambda row: (row["available_at"], row["term"], row["cusip"]))


def replay_market(root: Path, manifest: dict, market: str, events: list[dict]) -> list[dict]:
    source = root / manifest["frozen_price_inputs"][market]["path"]
    bars = pd.read_parquet(source, columns=["timestamp", "open", "high", "low", "close", "contract"])
    bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
    bars = bars.sort_values("timestamp", kind="mergesort").drop_duplicates("timestamp", keep="last").reset_index(drop=True)
    spec = manifest["market_specs"][market]
    preflight = manifest["preflight"]
    quantity = int(preflight["contracts"])
    per_contract_cost = float(spec["round_turn_fee_usd"]) + 2.0 * float(spec["tick"]) * float(spec["point_value"])
    output: list[dict] = []
    # Compare integer UTC nanoseconds so pandas/NumPy cannot silently mix
    # timezone-aware Timestamp objects with timezone-naive datetime64 values.
    timestamps = bars["timestamp"].astype("int64").to_numpy()
    for event in events:
        start = int(np.searchsorted(timestamps, int(event["available_at"].value), side="right"))
        if start >= len(bars):
            continue
        entry_row = bars.iloc[start]
        if entry_row["timestamp"].tz_convert(NY).date().isoformat() != event["auction_date"]:
            continue
        entry = float(entry_row["open"])
        for horizon in preflight["horizons_minutes"]:
            deadline = entry_row["timestamp"] + pd.Timedelta(minutes=int(horizon))
            stop = int(np.searchsorted(timestamps, int(deadline.value), side="right"))
            path = bars.iloc[start:stop]
            if path.empty or path.iloc[-1]["timestamp"] < deadline - pd.Timedelta(minutes=2):
                continue
            exit_price = float(path.iloc[-1]["close"])
            change = exit_price - entry
            direction = 0 if change == 0 else (1 if change > 0 else -1)
            gross = abs(change) * float(spec["point_value"]) * quantity
            stressed = gross - per_contract_cost * quantity
            if direction > 0:
                adverse_points = float(path["low"].min()) - entry
            elif direction < 0:
                adverse_points = entry - float(path["high"].max())
            else:
                adverse_points = 0.0
            minimum_equity = adverse_points * float(spec["point_value"]) * quantity - per_contract_cost * quantity
            output.append({
                **{key: (value.isoformat() if isinstance(value, pd.Timestamp) else value) for key, value in event.items()},
                "horizon_minutes": int(horizon),
                "entry_time": entry_row["timestamp"].isoformat(),
                "entry_contract": str(entry_row["contract"]),
                "entry_price": entry,
                "exit_time": path.iloc[-1]["timestamp"].isoformat(),
                "exit_price": exit_price,
                "oracle_direction": direction,
                "gross_pnl_usd": gross,
                "stressed_net_usd": stressed,
                "minimum_event_equity_usd": minimum_equity,
                "event_mll_breach": minimum_equity <= -float(preflight["maximum_loss_limit_usd"]),
            })
    return output


def account_windows(rows: list[dict], horizon: int, manifest: dict, session_calendar: list[str]) -> dict:
    preflight = manifest["preflight"]
    target = float(preflight["profit_target_usd"])
    mll = float(preflight["maximum_loss_limit_usd"])
    width = int(preflight["non_overlapping_account_window_sessions"])
    selected = [row for row in rows if row["horizon_minutes"] == horizon]
    by_day: dict[str, list[dict]] = {}
    for row in selected:
        by_day.setdefault(row["auction_date"], []).append(row)
    summaries = []
    for start in range(0, len(session_calendar) - width + 1, width):
        days = session_calendar[start : start + width]
        equity = 0.0
        high_water = 0.0
        best_day = 0.0
        breached = False
        for day in days:
            daily = 0.0
            for event in sorted(by_day.get(day, []), key=lambda row: row["available_at"]):
                if high_water - (equity + float(event["minimum_event_equity_usd"])) >= mll:
                    breached = True
                    break
                daily += float(event["stressed_net_usd"])
                equity += float(event["stressed_net_usd"])
                high_water = max(high_water, equity)
            best_day = max(best_day, daily)
            if breached:
                break
        consistency_target = max(target, 2.0 * best_day)
        summaries.append({
            "start": days[0], "end": days[-1], "net_usd": equity,
            "best_day_usd": best_day, "consistency_target_usd": consistency_target,
            "mll_breached": breached, "passed": (not breached and equity >= consistency_target),
        })
    return {
        "window_count": len(summaries),
        "pass_count": sum(row["passed"] for row in summaries),
        "mll_breach_count": sum(row["mll_breached"] for row in summaries),
        "median_net_usd": float(np.median([row["net_usd"] for row in summaries])) if summaries else None,
        "maximum_net_usd": max((row["net_usd"] for row in summaries), default=None),
        "windows": summaries,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(PROJECT))
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    manifest = load_manifest(root)
    cache = root / "data/cache/official/treasury_auction_demand_shock_v1"
    all_records: list[dict] = []
    source_hashes = {}
    for label, url in (("note", manifest["official_event_sources"]["note"]), ("bond", manifest["official_event_sources"]["bond"])):
        records, digest = acquire_json(cache, label, url)
        all_records.extend(records)
        source_hashes[label] = digest
    events = event_rows(manifest, all_records)
    rows: list[dict] = []
    for market in manifest["frozen_price_inputs"]:
        rows.extend(replay_market(root, manifest, market, [event for event in events if event["market"] == market]))
    rows.sort(key=lambda row: (row["available_at"], row["horizon_minutes"], row["event_id"]))
    role_summary = {}
    for role in manifest["chronological_roles"]:
        role_summary[role] = {}
        for horizon in manifest["preflight"]["horizons_minutes"]:
            part = [row for row in rows if row["role"] == role and row["horizon_minutes"] == horizon]
            role_summary[role][str(horizon)] = {
                "event_count": len(part),
                "stressed_net_total_usd": sum(row["stressed_net_usd"] for row in part),
                "stressed_net_median_usd": float(np.median([row["stressed_net_usd"] for row in part])) if part else None,
                "positive_stressed_count": sum(row["stressed_net_usd"] > 0 for row in part),
                "event_mll_breach_count": sum(row["event_mll_breach"] for row in part),
            }
    # ZN has the most complete session calendar and is used only for fixed
    # evaluation-window boundaries, never for signal eligibility.
    zn = pd.read_parquet(root / manifest["frozen_price_inputs"]["ZN"]["path"], columns=["session_id"])
    sessions = sorted(day for day in zn["session_id"].astype(str).unique() if "2023-01-01" <= day < "2024-10-01")
    windows = {str(h): account_windows(rows, int(h), manifest, sessions) for h in manifest["preflight"]["horizons_minutes"]}
    event_count = len({row["event_id"] for row in rows})
    validation_count = len({row["event_id"] for row in rows if row["role"] == "VALIDATION"})
    final_count = len({row["event_id"] for row in rows if row["role"] == "FINAL_DEVELOPMENT"})
    total_paths = len(rows)
    breach_rate = sum(row["event_mll_breach"] for row in rows) / total_paths if total_paths else 1.0
    gate = (
        event_count >= manifest["preflight"]["minimum_total_events"]
        and validation_count >= manifest["preflight"]["minimum_validation_events"]
        and final_count >= manifest["preflight"]["minimum_final_events"]
        and max(value["pass_count"] for value in windows.values()) >= manifest["preflight"]["minimum_oracle_20_session_passes"]
        and breach_rate <= manifest["preflight"]["maximum_oracle_mll_breach_rate"]
    )
    result = {
        "schema": "hydra_treasury_auction_upper_bound_result_v1",
        "branch_id": manifest["branch_id"],
        "manifest_hash": manifest["manifest_hash"],
        "status": "TREASURY_AUCTION_UPPER_BOUND_SUPPORTS_CAUSAL_TEST" if gate else "TREASURY_AUCTION_INFORMATION_BOUND_FALSIFIED",
        "non_deployable": True,
        "event_count": event_count,
        "event_count_by_role": {role: len({row["event_id"] for row in rows if row["role"] == role}) for role in manifest["chronological_roles"]},
        "path_count": total_paths,
        "role_horizon_summary": role_summary,
        "account_windows": windows,
        "event_mll_breach_rate": breach_rate,
        "upper_bound_gate_pass": gate,
        "official_source_hashes": source_hashes,
        "incremental_spend_usd": 0.0,
        "q4_access_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "next_action": "FREEZE_AND_RUN_CAUSAL_DEMAND_SCORE" if gate else "TOMBSTONE_AUCTION_INFORMATION_REPRESENTATION_AND_PIVOT",
    }
    result["result_hash"] = hashlib.sha256(canonical(result)).hexdigest()
    output = root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(canonical(result) + b"\n")
    print(json.dumps({key: result[key] for key in ("status", "event_count", "path_count", "event_mll_breach_rate", "upper_bound_gate_pass", "result_hash")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ.setdefault(name, "1")
    raise SystemExit(main())
