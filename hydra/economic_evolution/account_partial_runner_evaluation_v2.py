from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Mapping

from hydra.economic_evolution.account_evaluation import (
    UnsupportedExactExecution,
    _scale_routed_trade,
    build_exact_sleeve_runtime,
)
from hydra.economic_evolution.account_partial_runner import (
    MATCHED_CONTROL_EXIT,
    TOTAL_QUANTITY,
)
from hydra.economic_evolution.account_partial_runner_evaluation import (
    PartialRunnerExactRuntime,
    build_partial_runner_exact_runtime,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.economic_evolution.screen import BoundSleeve
from hydra.features.feature_matrix import FeatureMatrix


MUTATED_PARTIAL_RUNNER_SLEEVE_IDS = frozenset(
    {
        "sleeve_65bad2088913fc9fca0a145d",
        "sleeve_9f99649247c698bf206d0507",
        "sleeve_b8e98b3a73cacb8a105a3116",
        "sleeve_c5da4b5a67abadeb7d68eabe",
        "sleeve_fe3e0de298753596c2459cf4",
    }
)


def build_partial_runner_exact_runtime_v2(
    bound: BoundSleeve,
    matrix: FeatureMatrix,
    *,
    start_inclusive: str,
    end_exclusive: str,
) -> PartialRunnerExactRuntime:
    if bound.sleeve.sleeve_id in MUTATED_PARTIAL_RUNNER_SLEEVE_IDS:
        return build_partial_runner_exact_runtime(
            bound,
            matrix,
            start_inclusive=start_inclusive,
            end_exclusive=end_exclusive,
        )
    # The campaign mutates only the five preregistered complementary sleeves.
    # Every other component must retain its one-lot exact parent ledger and
    # must not acquire a new volatility dependency merely because it shares a
    # population with the exit experiment.
    base = build_exact_sleeve_runtime(
        bound,
        matrix,
        start_inclusive=start_inclusive,
        end_exclusive=end_exclusive,
    )
    control = tuple(
        _scale_routed_trade(row, units=TOTAL_QUANTITY, cost_stress=1.0)
        for row in base.events
    )
    control_net = sum(row.event.net_pnl for row in control)
    control_gross = sum(row.event.gross_pnl for row in control)
    return PartialRunnerExactRuntime(
        sleeve_id=base.sleeve_id,
        signal_market=base.signal_market,
        execution_market=base.execution_market,
        role=base.role,
        source_campaign=base.source_campaign,
        specification_hash=stable_hash(
            {
                "parent": base.specification_hash,
                "exit": MATCHED_CONTROL_EXIT,
                "runner_not_applicable": True,
            }
        ),
        eligible_session_days=base.eligible_session_days,
        events=base.events,
        control_events=control,
        partial_runner_events=control,
        event_count=base.event_count,
        net_pnl=base.net_pnl,
        cost_stress_1_5x_net=base.cost_stress_1_5x_net,
        maximum_drawdown=base.maximum_drawdown,
        best_positive_event_share=base.best_positive_event_share,
        partial_runner_net_pnl=float(control_net),
        partial_runner_cost_stress_1_5x_net=float(
            control_gross - 1.5 * (control_gross - control_net)
        ),
        target_hit_count=0,
        exit_implementation="EXACT_TIME_EXIT_UNMUTATED_COMPONENT",
    )


def build_partial_runner_exact_runtimes_v2(
    bound: Mapping[str, BoundSleeve],
    matrices: Mapping[str, FeatureMatrix],
    *,
    start_inclusive: str,
    end_exclusive: str,
    worker_count: int,
) -> tuple[dict[str, PartialRunnerExactRuntime], list[dict[str, str]]]:
    output: dict[str, PartialRunnerExactRuntime] = {}
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = {
            pool.submit(
                build_partial_runner_exact_runtime_v2,
                row,
                matrices[row.sleeve.market],
                start_inclusive=start_inclusive,
                end_exclusive=end_exclusive,
            ): sleeve_id
            for sleeve_id, row in bound.items()
        }
        for future in as_completed(futures):
            sleeve_id = futures[future]
            try:
                output[sleeve_id] = future.result()
            except (ValueError, UnsupportedExactExecution) as exc:
                failures.append(
                    {
                        "sleeve_id": sleeve_id,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                )
    return dict(sorted(output.items())), sorted(
        failures, key=lambda row: row["sleeve_id"]
    )


__all__ = [
    "MUTATED_PARTIAL_RUNNER_SLEEVE_IDS",
    "build_partial_runner_exact_runtime_v2",
    "build_partial_runner_exact_runtimes_v2",
]
