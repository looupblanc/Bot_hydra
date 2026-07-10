from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import RollMap
from hydra.data.pair_contract_synchronization import pair_validity_at
from hydra.markets.instruments import instrument_spec
from hydra.representations.dynamic_hedge_ratio import hedge_ratio
from hydra.representations.residual_state import residual_zscore


@dataclass(frozen=True)
class PairedRelativeValueConfig:
    left_symbol: str = "NQ"
    right_symbol: str = "ES"
    hedge_ratio_method: str = "rolling_ols"
    hedge_window: int = 120
    z_window: int = 120
    entry_z: float = 1.25
    exit_z: float = 0.25
    pair_validity_required: bool = True
    beta_neutral: bool = True


def build_paired_residual_frame(df: pd.DataFrame, roll_map: RollMap, config: PairedRelativeValueConfig) -> pd.DataFrame:
    left = _symbol_frame(df, config.left_symbol)
    right = _symbol_frame(df, config.right_symbol)
    paired = left.join(right, how="inner", lsuffix=f"_{config.left_symbol}", rsuffix=f"_{config.right_symbol}")
    paired = paired.sort_index()
    left_close = paired[f"close_{config.left_symbol}"].astype(float)
    right_close = paired[f"close_{config.right_symbol}"].astype(float)
    left_ret = left_close.pct_change()
    right_ret = right_close.pct_change()
    left_spec = instrument_spec(config.left_symbol)
    right_spec = instrument_spec(config.right_symbol)
    beta = hedge_ratio(
        config.hedge_ratio_method,
        left_ret,
        right_ret,
        window=config.hedge_window,
        left_point_value=left_spec.point_value,
        right_point_value=right_spec.point_value,
    )
    residual = left_ret - beta * right_ret
    z = residual_zscore(residual, config.z_window)
    pair_checks = [
        pair_validity_at(roll_map, ts, left_symbol=config.left_symbol, right_symbol=config.right_symbol)
        for ts in paired.index
    ]
    pair_valid = pd.Series([item.pair_valid for item in pair_checks], index=paired.index, dtype=bool)
    roll_exclusion = pd.Series([item.roll_transition_exclusion for item in pair_checks], index=paired.index, dtype=bool)
    left_contract = [item.left_contract for item in pair_checks]
    right_contract = [item.right_contract for item in pair_checks]
    signal = pd.Series(0, index=paired.index, dtype=int)
    signal[z >= config.entry_z] = -1
    signal[z <= -config.entry_z] = 1
    signal[z.abs() <= config.exit_z] = 0
    signal = signal.where(pair_valid & ~roll_exclusion & beta.notna(), 0)
    out = pd.DataFrame(
        {
            "timestamp": paired.index,
            "symbol": f"{config.left_symbol}/{config.right_symbol}",
            "left_symbol": config.left_symbol,
            "right_symbol": config.right_symbol,
            "left_contract": left_contract,
            "right_contract": right_contract,
            "left_close": left_close,
            "right_close": right_close,
            "left_return": left_ret,
            "right_return": right_ret,
            "hedge_ratio": beta,
            "residual_return": residual,
            "residual_z": z,
            "feature": z,
            "forward_return": residual.shift(-30).rolling(30, min_periods=1).sum(),
            "pair_valid": pair_valid,
            "roll_transition_exclusion": roll_exclusion,
            "signal": signal.shift(1).fillna(0).astype(int),
        },
        index=paired.index,
    )
    return out.reset_index(drop=True).replace([np.inf, -np.inf], np.nan)


def _symbol_frame(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    subset = df[df["symbol"] == symbol].copy()
    if subset.empty:
        raise ValueError(f"No rows for symbol {symbol}")
    subset["timestamp"] = pd.to_datetime(subset["timestamp"], utc=True)
    return subset.set_index("timestamp")[["open", "high", "low", "close", "volume"]].sort_index()
