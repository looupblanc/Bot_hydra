from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd

from hydra.data.contract_mapping import RollMap, active_contract, is_unsafe_roll_window, maturity_key


@dataclass(frozen=True)
class PairValidity:
    timestamp: str
    left_symbol: str
    right_symbol: str
    left_contract: str
    right_contract: str
    synchronized_quarterly_maturity: bool
    residual_time_to_expiry_days: int
    pair_valid: bool
    roll_transition_exclusion: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def pair_validity_at(
    roll_map: RollMap,
    timestamp: Any,
    *,
    left_symbol: str = "NQ",
    right_symbol: str = "ES",
) -> PairValidity:
    ts = _as_utc(timestamp)
    left = active_contract(roll_map, left_symbol, ts)
    right = active_contract(roll_map, right_symbol, ts)
    same_maturity = maturity_key(left) == maturity_key(right)
    residual_days = abs((pd.Timestamp(left.expiry_date) - pd.Timestamp(right.expiry_date)).days)
    roll_exclusion = is_unsafe_roll_window(roll_map, left_symbol, ts) or is_unsafe_roll_window(roll_map, right_symbol, ts)
    valid = bool(same_maturity and residual_days == 0 and not roll_exclusion)
    if not same_maturity:
        reason = "mismatched_quarterly_maturity"
    elif residual_days:
        reason = "mismatched_expiry_date"
    elif roll_exclusion:
        reason = "roll_transition_exclusion"
    else:
        reason = "synchronized_pair"
    return PairValidity(
        timestamp=ts.isoformat(),
        left_symbol=left_symbol,
        right_symbol=right_symbol,
        left_contract=left.contract,
        right_contract=right.contract,
        synchronized_quarterly_maturity=same_maturity,
        residual_time_to_expiry_days=int(residual_days),
        pair_valid=valid,
        roll_transition_exclusion=roll_exclusion,
        reason=reason,
    )


def audit_pair_synchronization(
    roll_map: RollMap,
    timestamps: list[Any],
    *,
    left_symbol: str = "NQ",
    right_symbol: str = "ES",
) -> dict[str, Any]:
    checks = [pair_validity_at(roll_map, ts, left_symbol=left_symbol, right_symbol=right_symbol) for ts in timestamps]
    invalid = [check for check in checks if not check.pair_valid]
    maturity_mismatches = [check for check in checks if not check.synchronized_quarterly_maturity]
    roll_exclusions = [check for check in checks if check.roll_transition_exclusion]
    return {
        "pair": [left_symbol, right_symbol],
        "timestamps_checked": len(checks),
        "valid_count": len(checks) - len(invalid),
        "invalid_count": len(invalid),
        "maturity_mismatch_count": len(maturity_mismatches),
        "roll_transition_exclusion_count": len(roll_exclusions),
        "pair_validity_rate": round((len(checks) - len(invalid)) / max(len(checks), 1), 6),
        "samples": [check.to_dict() for check in invalid[:10]],
    }


def _as_utc(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")
