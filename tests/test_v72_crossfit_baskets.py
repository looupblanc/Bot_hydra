from __future__ import annotations

from hydra.validation.v72_crossfit_baskets import select_rotation_frontier


def _record(
    basket_hash: str,
    *,
    net: float,
    progress: float,
    mll: float,
    consistency: float,
    conflict: float,
    hard: bool = True,
) -> dict:
    metrics = {
        "account_net_sum": net,
        "maximum_target_progress_median": progress,
        "mll_breach_rate": mll,
        "consistency_pass_rate": consistency,
        "conflict_rate": conflict,
    }
    return {
        "frozen_basket": {"basket_hash": basket_hash},
        "design_metrics": {"BASE": metrics, "STRESS_1_5X": metrics},
        "hard_filter_passed": hard,
    }


def test_pareto_selection_uses_design_metrics_only_and_is_deterministic() -> None:
    dominated = _record(
        "dominated", net=10.0, progress=0.1, mll=0.1, consistency=0.5, conflict=0.5
    )
    strong_net = _record(
        "strong-net", net=100.0, progress=0.4, mll=0.0, consistency=1.0, conflict=0.2
    )
    strong_progress = _record(
        "strong-progress", net=80.0, progress=0.8, mll=0.0, consistency=1.0, conflict=0.1
    )
    hard_fail = _record(
        "hard-fail", net=1000.0, progress=1.0, mll=0.0, consistency=1.0, conflict=0.0,
        hard=False,
    )

    selected, frontier_count, hard_count = select_rotation_frontier(
        [dominated, strong_progress, hard_fail, strong_net]
    )

    assert hard_count == 3
    assert frontier_count == 2
    assert [row["frozen_basket"]["basket_hash"] for row in selected] == [
        "strong-net",
        "strong-progress",
    ]
