from __future__ import annotations

import numpy as np

from hydra.research.turbo_promotion import (
    _daily_frame,
    _session_block_sign_flip_probability,
)


def test_candidate_null_rejects_null_and_detects_large_session_effect():
    days = np.repeat(np.arange(80), 2)
    null = np.tile(np.asarray([-10.0, 10.0]), 80)
    effect = np.full(160, 30.0)
    assert _session_block_sign_flip_probability(null, days, cost=0.0, seed=7) > 0.20
    assert _session_block_sign_flip_probability(effect, days, cost=0.0, seed=7) < 0.05


def test_daily_frame_has_topstep_intraday_contract():
    frame = _daily_frame(
        np.asarray([100.0, -40.0, 200.0]),
        np.asarray([10, 10, 11]),
    )
    assert frame["pnl"].tolist() == [60.0, 200.0]
    assert frame["worst_intraday_pnl"].tolist() == [-40.0, 0.0]
    assert frame["trades"].tolist() == [2, 1]
