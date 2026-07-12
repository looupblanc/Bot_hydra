from __future__ import annotations

from hydra.calibration.v71_power_sample_extension import _summarize_world


def test_sample_extension_selects_first_powered_count_without_threshold_change() -> None:
    plan = {
        "acceptance": {
            "null_false_positive_rate_max": 0.10,
            "power_min": 0.60,
            "target_effect_usd_per_trade": 50.0,
        }
    }
    rows = [
        {"event_count": 240, "effect_usd_per_trade": 0.0, "false_positive_rate": 0.01, "power": 0.0},
        {"event_count": 320, "effect_usd_per_trade": 0.0, "false_positive_rate": 0.02, "power": 0.0},
        {"event_count": 240, "effect_usd_per_trade": 50.0, "false_positive_rate": 0.0, "power": 0.55},
        {"event_count": 320, "effect_usd_per_trade": 50.0, "false_positive_rate": 0.0, "power": 0.70},
    ]

    result = _summarize_world(rows, plan)

    assert result["passed"] is True
    assert result["minimum_required_event_count"] == 320
    assert result["power_at_required_count"] == 0.70
