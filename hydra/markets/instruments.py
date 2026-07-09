from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class InstrumentSpec:
    symbol: str
    tick_size: float
    tick_value: float
    point_value: float
    is_micro: bool


INSTRUMENTS: dict[str, InstrumentSpec] = {
    "ES": InstrumentSpec("ES", tick_size=0.25, tick_value=12.5, point_value=50.0, is_micro=False),
    "MES": InstrumentSpec("MES", tick_size=0.25, tick_value=1.25, point_value=5.0, is_micro=True),
    "NQ": InstrumentSpec("NQ", tick_size=0.25, tick_value=5.0, point_value=20.0, is_micro=False),
    "MNQ": InstrumentSpec("MNQ", tick_size=0.25, tick_value=0.5, point_value=2.0, is_micro=True),
}


def instrument_spec(symbol: str) -> InstrumentSpec:
    root = symbol.upper()
    if root not in INSTRUMENTS:
        raise KeyError(f"Unsupported instrument metadata for symbol: {symbol}")
    return INSTRUMENTS[root]
