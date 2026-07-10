from __future__ import annotations

import gzip
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso


SKETCH_SCHEMA = {
    "candidate_id": "str",
    "parent_candidate_id": "str|null",
    "validation_hash": "sha256",
    "daily_pnl_hash": "sha256",
    "trade_timestamp_signature": "sha256",
    "direction_signature": "sha256",
    "holding_time_histogram": "dict[str,int]",
    "session_histogram": "dict[str,int]",
    "symbol_exposure": "dict[str,int]",
    "regime_exposure": "dict[str,int]",
    "tail_event_signature": "sha256",
    "entry_overlap_signature": "sha256",
}

TRADE_LEDGER_SCHEMA = {
    "candidate_id": "str",
    "parent_candidate_id": "str|null",
    "entry_timestamp": "iso8601",
    "exit_timestamp": "iso8601",
    "symbol": "str",
    "contract": "str|null",
    "direction": "int",
    "quantity": "float",
    "entry_price": "float",
    "exit_price": "float",
    "commissions": "float",
    "slippage": "float",
    "gross_pnl": "float",
    "net_pnl": "float",
    "mae": "float",
    "mfe": "float",
    "session": "str",
    "regime": "str",
    "reason_for_entry": "str",
    "reason_for_exit": "str",
    "validation_period": "str",
}


def build_behavioral_sketch(
    *,
    candidate_id: str,
    parent_candidate_id: str | None,
    trades: list[dict[str, Any]],
    daily: pd.DataFrame,
    validation_period: str,
) -> dict[str, Any]:
    entry_times = [str(t.get("entry_timestamp") or t.get("entry_i")) for t in trades]
    directions = [int(t.get("side") or t.get("direction") or 0) for t in trades]
    holdings = [int((t.get("exit_i") or 0) - (t.get("entry_i") or 0)) for t in trades]
    sessions = [str(t.get("session") or t.get("session_id") or "unknown") for t in trades]
    symbols = [str(t.get("symbol") or "unknown") for t in trades]
    regimes = [str(t.get("regime") or "unknown") for t in trades]
    pnls = [float(t.get("pnl") or t.get("net_pnl") or 0.0) for t in trades]
    tail_cutoff = _tail_cutoff(pnls)
    tail_events = [entry_times[i] for i, pnl in enumerate(pnls) if abs(pnl) >= tail_cutoff] if tail_cutoff else []
    daily_payload = []
    if not daily.empty:
        daily_payload = [
            {"date": str(row.date), "pnl": round(float(row.pnl), 2)}
            for row in daily.itertuples(index=False)
            if hasattr(row, "date") and hasattr(row, "pnl")
        ]
    payload = {
        "candidate_id": candidate_id,
        "parent_candidate_id": parent_candidate_id,
        "validation_period": validation_period,
        "daily_pnl_hash": _hash_json(daily_payload),
        "trade_timestamp_signature": _hash_json(entry_times),
        "direction_signature": _hash_json(directions),
        "holding_time_histogram": _histogram(holdings, bucket=5),
        "session_histogram": dict(Counter(sessions)),
        "symbol_exposure": dict(Counter(symbols)),
        "regime_exposure": dict(Counter(regimes)),
        "tail_event_signature": _hash_json(tail_events),
        "entry_overlap_signature": _minhash_signature(entry_times),
    }
    payload["validation_hash"] = _hash_json(payload)
    return payload


def build_trade_ledger_rows(
    *,
    candidate_id: str,
    parent_candidate_id: str | None,
    trades: list[dict[str, Any]],
    validation_period: str,
) -> list[dict[str, Any]]:
    rows = []
    for trade in trades:
        rows.append(
            {
                "candidate_id": candidate_id,
                "parent_candidate_id": parent_candidate_id,
                "entry_timestamp": str(trade.get("entry_timestamp") or ""),
                "exit_timestamp": str(trade.get("exit_timestamp") or ""),
                "symbol": str(trade.get("symbol") or ""),
                "contract": trade.get("contract"),
                "direction": int(trade.get("side") or trade.get("direction") or 0),
                "quantity": float(trade.get("quantity") or trade.get("max_position") or 1.0),
                "entry_price": float(trade.get("entry_price") or 0.0),
                "exit_price": float(trade.get("exit_price") or 0.0),
                "commissions": float(trade.get("commissions") or 0.0),
                "slippage": float(trade.get("slippage") or 0.0),
                "gross_pnl": float(trade.get("gross_pnl") or trade.get("pnl") or 0.0),
                "net_pnl": float(trade.get("net_pnl") or trade.get("pnl") or 0.0),
                "mae": float(trade.get("mae") or 0.0),
                "mfe": float(trade.get("mfe") or 0.0),
                "session": str(trade.get("session") or trade.get("session_id") or ""),
                "regime": str(trade.get("regime") or ""),
                "reason_for_entry": str(trade.get("entry_reason") or "signal"),
                "reason_for_exit": str(trade.get("exit_reason") or ""),
                "validation_period": validation_period,
            }
        )
    return rows


def write_behavioral_artifacts(
    *,
    tag: str,
    sketches: list[dict[str, Any]],
    ledgers: list[dict[str, Any]],
    base_dir: str = "data/cache/behavioral_evidence",
) -> dict[str, Any]:
    folder = project_path(base_dir, tag)
    folder.mkdir(parents=True, exist_ok=True)
    sketch_path = folder / "behavioral_sketches.jsonl.gz"
    ledger_path = folder / "trade_ledgers.jsonl.gz"
    _write_jsonl_gz(sketch_path, sketches)
    _write_jsonl_gz(ledger_path, ledgers)
    manifest = {
        "created_at": utc_now_iso(),
        "tag": tag,
        "storage_note": "Full behavioral evidence is under ignored data/cache and must not be committed.",
        "sketch_schema": SKETCH_SCHEMA,
        "trade_ledger_schema": TRADE_LEDGER_SCHEMA,
        "sketch_count": len(sketches),
        "ledger_row_count": len(ledgers),
        "sketch_path": str(sketch_path),
        "ledger_path": str(ledger_path),
        "sketch_sha256": _file_sha256(sketch_path),
        "ledger_sha256": _file_sha256(ledger_path),
    }
    return manifest


def _write_jsonl_gz(path: Path, rows: list[dict[str, Any]]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def _hash_json(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _histogram(values: list[int], bucket: int) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for value in values:
        lo = (int(value) // bucket) * bucket
        counts[f"{lo}-{lo + bucket - 1}"] += 1
    return dict(counts)


def _tail_cutoff(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(abs(v) for v in values)
    return ordered[max(0, int(0.9 * (len(ordered) - 1)))]


def _minhash_signature(values: list[str], size: int = 64) -> str:
    hashed = sorted(hashlib.sha1(v.encode("utf-8")).hexdigest() for v in set(values))
    return _hash_json(hashed[:size])
