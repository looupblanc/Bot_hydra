from __future__ import annotations

from hydra.calibration.injected_edges import InjectedEdgeSpec


def positive_control_specs() -> list[InjectedEdgeSpec]:
    return [
        InjectedEdgeSpec("mean_shift_strong", "mean_shift", 1, 0.0012, 0.12, 15, 5),
        InjectedEdgeSpec("path_asymmetry_medium", "path_asymmetry", 1, 0.0009, 0.10, 30, 4),
        InjectedEdgeSpec("tail_risk_defensive", "tail_risk", -1, 0.0010, 0.10, 20, 4),
        InjectedEdgeSpec("volatility_prediction", "volatility_prediction", 1, 0.0014, 0.12, 25, 4),
        InjectedEdgeSpec("regime_conditional", "mean_shift", 1, 0.0015, 0.18, 20, 3, regime_specific=True),
    ]

