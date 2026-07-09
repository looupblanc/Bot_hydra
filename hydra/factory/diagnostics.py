from __future__ import annotations

from copy import deepcopy
from typing import Any


DIAGNOSTIC_WARNING = "Synthetic results are pipeline diagnostics only and must not be interpreted as real trading edge."


def run_mode_label(synthetic: bool, diagnostic_relaxed: bool) -> str:
    if synthetic and diagnostic_relaxed:
        return "synthetic diagnostic"
    if synthetic:
        return "synthetic strict"
    return "real data"


def apply_diagnostic_relaxed_config(config: dict[str, Any]) -> dict[str, Any]:
    cfg = deepcopy(config)
    diagnostic = cfg.get("diagnostics", {}).get("synthetic_relaxed", {})
    validation = cfg.setdefault("validation", {})
    propfirm = cfg.setdefault("propfirm", {})
    validation["min_trades"] = diagnostic.get("min_trades", 25)
    validation["min_profit_factor"] = diagnostic.get("min_profit_factor", 1.01)
    validation["min_robustness_score"] = diagnostic.get("min_robustness_score", 0.15)
    propfirm["reject_if_mll_buffer_below"] = diagnostic.get("reject_if_mll_buffer_below", propfirm.get("reject_if_mll_buffer_below", 250))
    return cfg


def diagnostic_bars(config: dict[str, Any]) -> int:
    return int(config.get("diagnostics", {}).get("synthetic_relaxed", {}).get("bars", 3000))
