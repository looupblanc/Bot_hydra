from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd

from hydra.data.contract_mapping import RollMap, active_contract, annotate_contracts, is_unsafe_roll_window


def audit_roll_discontinuities(df: pd.DataFrame, roll_map: RollMap) -> dict[str, Any]:
    annotated = annotate_contracts(df, roll_map)
    result: dict[str, Any] = {
        "roll_map_type": roll_map.map_type,
        "explicit_contract_metadata_available": roll_map.map_type.startswith("EXPLICIT"),
        "symbols": {},
        "roll_artifact_suspected": False,
    }
    for symbol, frame in annotated.groupby("symbol"):
        frame = frame.sort_values("timestamp")
        returns = frame["close"].pct_change().abs()
        unsafe = frame[frame["unsafe_roll_window"]]
        max_unsafe_return = float(returns.loc[unsafe.index].max() or 0.0) if len(unsafe) else 0.0
        volume_ratio = _volume_discontinuity(frame)
        gap_suspected = max_unsafe_return > 0.03
        if gap_suspected:
            result["roll_artifact_suspected"] = True
        result["symbols"][str(symbol)] = {
            "bars": int(len(frame)),
            "unsafe_roll_bars": int(frame["unsafe_roll_window"].sum()),
            "active_contracts": dict(Counter(frame["active_contract"])),
            "max_abs_return_in_unsafe_window": round(max_unsafe_return, 6),
            "volume_discontinuity_ratio": round(volume_ratio, 6),
            "gap_suspected": bool(gap_suspected),
        }
    return result


def audit_trade_roll_exposure(trades: list[dict[str, Any]], roll_map: RollMap) -> dict[str, Any]:
    total = len(trades)
    unsafe_count = 0
    cross_roll_count = 0
    unsafe_pnl = 0.0
    cross_roll_pnl = 0.0
    by_contract_pair: Counter[str] = Counter()
    for trade in trades:
        symbol = str(trade.get("symbol") or "")
        entry_ts = trade.get("entry_timestamp")
        exit_ts = trade.get("exit_timestamp")
        if not symbol or not entry_ts or not exit_ts:
            continue
        entry_contract = active_contract(roll_map, symbol, entry_ts).contract
        exit_contract = active_contract(roll_map, symbol, exit_ts).contract
        pnl = float(trade.get("net_pnl") or trade.get("pnl") or 0.0)
        pair = f"{entry_contract}->{exit_contract}"
        by_contract_pair[pair] += 1
        unsafe = is_unsafe_roll_window(roll_map, symbol, entry_ts) or is_unsafe_roll_window(roll_map, symbol, exit_ts)
        if unsafe:
            unsafe_count += 1
            unsafe_pnl += pnl
        if entry_contract != exit_contract:
            cross_roll_count += 1
            cross_roll_pnl += pnl
    return {
        "trade_count": total,
        "unsafe_roll_trade_count": unsafe_count,
        "cross_roll_trade_count": cross_roll_count,
        "unsafe_roll_net_pnl": round(unsafe_pnl, 2),
        "cross_roll_net_pnl": round(cross_roll_pnl, 2),
        "unsafe_roll_trade_share": round(unsafe_count / max(total, 1), 6),
        "cross_roll_trade_share": round(cross_roll_count / max(total, 1), 6),
        "contract_pairs": dict(by_contract_pair),
        "roll_sensitive": bool(cross_roll_count > 0 or unsafe_count / max(total, 1) > 0.10),
    }


def synchronized_pair_audit(roll_map: RollMap, timestamps: list[Any], pair: tuple[str, str] = ("NQ", "ES")) -> dict[str, Any]:
    mismatches = 0
    samples = []
    for ts in timestamps:
        contracts = {symbol: active_contract(roll_map, symbol, ts).contract for symbol in pair}
        month_codes = {contract[-2] for contract in contracts.values()}
        if len(month_codes) > 1:
            mismatches += 1
            if len(samples) < 10:
                samples.append({"timestamp": str(ts), "contracts": contracts})
    return {
        "pair": list(pair),
        "timestamps_checked": len(timestamps),
        "mismatches": mismatches,
        "synchronized": mismatches == 0,
        "samples": samples,
    }


def _volume_discontinuity(frame: pd.DataFrame) -> float:
    if "volume" not in frame.columns or frame.empty:
        return 0.0
    vol = frame["volume"].astype(float)
    rolling = vol.rolling(390, min_periods=20).median().replace(0, pd.NA)
    ratios = (vol / rolling).replace([float("inf"), -float("inf")], pd.NA).dropna()
    return float(ratios.max() or 0.0) if len(ratios) else 0.0
