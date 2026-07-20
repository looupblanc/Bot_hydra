from hydra.research.cross_era_marginal_pair_builder import (
    CONSISTENCY_DEGRADATION_TOLERANCE,
    MLL_BUFFER_DEGRADATION_FRACTION,
    P25_DEGRADATION_TOLERANCE,
    _marginal_cell,
)


def _summary(*, passes=0, p25=0.1, median=0.1, net=100.0, buffer=1500.0, consistency=1.0):
    return {
        "full_coverage_start_count": 4,
        "pass_count": passes,
        "mll_breach_count": 0,
        "target_progress_p25": p25,
        "target_progress_median": median,
        "net_total_usd": net,
        "minimum_mll_buffer_usd": buffer,
        "consistency_compliance_rate": consistency,
    }


def _result(policy_id: str, normal: dict, stressed: dict):
    return {
        "policy_id": policy_id,
        "account_label": "50K",
        "governor_profile_id": "test_governor",
        "evaluation": {
            "summaries": {
                "NORMAL": {"5": normal},
                "STRESSED_1_5X": {"5": stressed},
            }
        },
    }


def test_pair_requires_strict_marginal_improvement() -> None:
    parent = _result("parent", _summary(), _summary())
    same = _result("pair", _summary(), _summary())
    assert (
        _marginal_cell(
            same,
            [parent],
            horizon=5,
            rule={"maximum_loss_limit_usd": 2000},
            old_progress_floor=0.1,
        )
        is None
    )
    improved = _result(
        "pair",
        _summary(p25=0.12),
        _summary(p25=0.12),
    )
    assert _marginal_cell(
        improved,
        [parent],
        horizon=5,
        rule={"maximum_loss_limit_usd": 2000},
        old_progress_floor=0.1,
    )


def test_pair_rejects_material_parent_degradation() -> None:
    parent = _result("parent", _summary(), _summary())
    degraded = _result(
        "pair",
        _summary(passes=1, p25=0.1 - P25_DEGRADATION_TOLERANCE - 0.001),
        _summary(
            passes=1,
            p25=0.1,
            consistency=1.0 - CONSISTENCY_DEGRADATION_TOLERANCE - 0.001,
            buffer=1500.0 - 2000.0 * MLL_BUFFER_DEGRADATION_FRACTION - 1.0,
        ),
    )
    assert (
        _marginal_cell(
            degraded,
            [parent],
            horizon=5,
            rule={"maximum_loss_limit_usd": 2000},
            old_progress_floor=0.1,
        )
        is None
    )
