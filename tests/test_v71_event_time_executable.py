from __future__ import annotations

from hydra.strategy.v71_event_time_executable import (
    assert_no_order_capability,
    load_executable_strategies,
)


def test_event_time_executable_configs_are_frozen_and_orderless() -> None:
    rows = load_executable_strategies(".")
    assert len(rows) == 2
    assert {row.alias for row in rows} == {
        "fast_volume_completion_60m",
        "high_rate_low_progress_60m",
    }
    assert all(row.holding_minutes == 60 for row in rows)
    assert all(row.position_quantity_primary == 1 for row in rows)
    assert_no_order_capability(rows)
