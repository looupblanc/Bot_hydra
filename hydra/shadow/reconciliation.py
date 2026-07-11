from __future__ import annotations

from typing import Any


def reconcile_expected_observed(
    expected_signal_ids: set[str], observed_events: list[dict[str, Any]]
) -> dict[str, Any]:
    observed = {
        str(item.get("signal_id") or (item.get("fill") or {}).get("signal_id") or "")
        for item in observed_events
    }
    observed.discard("")
    return {
        "expected": len(expected_signal_ids),
        "observed": len(observed),
        "missing": sorted(expected_signal_ids - observed),
        "unexpected": sorted(observed - expected_signal_ids),
        "passed": expected_signal_ids == observed,
    }
