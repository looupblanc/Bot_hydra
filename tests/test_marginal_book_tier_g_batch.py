from hydra.production.marginal_book_tier_g_batch import (
    _exposure_distance,
    _graduation_horizon,
    _pearson,
    _profit_share,
)


def _summary(*, p5=0, p10=0, p20=0, blocks5=None, blocks10=None, blocks20=None):
    values = {}
    for horizon, passes, blocks in (
        (5, p5, blocks5 or {}),
        (10, p10, blocks10 or {}),
        (20, p20, blocks20 or {}),
    ):
        values[str(horizon)] = {
            "overall": {"pass_count": passes, "block_pass_counts": blocks}
        }
    return {"NORMAL": values}


def test_graduation_horizon_is_shortest_multi_pass_multi_block_horizon():
    value = _summary(
        p5=1,
        blocks5={"B1": 1},
        p10=2,
        blocks10={"B1": 1, "B3": 1},
        p20=3,
        blocks20={"B1": 1, "B2": 1, "B4": 1},
    )
    assert _graduation_horizon(value) == 10


def test_graduation_horizon_rejects_repeated_passes_in_one_block():
    assert _graduation_horizon(
        _summary(p5=2, blocks5={"B4": 2}, p10=1, blocks10={"B4": 1})
    ) is None


def test_profit_share_uses_positive_profit_denominator():
    assert _profit_share([10.0, 5.0, -100.0]) == 2.0 / 3.0


def test_exposure_distance_is_zero_for_identity():
    assert _exposure_distance((1.0, 2.0, 10), (1.0, 2.0, 10)) == 0.0


def test_pearson_reports_exact_opposites_and_degenerate_none():
    assert _pearson([1.0, 2.0, 3.0], [3.0, 2.0, 1.0]) == -1.0
    assert _pearson([1.0, 1.0], [2.0, 3.0]) is None
