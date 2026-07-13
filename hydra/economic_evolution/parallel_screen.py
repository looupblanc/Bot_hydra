from __future__ import annotations

import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import get_context
from pathlib import Path
from typing import Mapping, Sequence

from hydra.economic_evolution.schema import SleeveSpec
from hydra.economic_evolution.screen import (
    CheapScreenPolicy,
    CheapScreenResult,
    run_ultra_cheap_screen,
)
from hydra.features.feature_matrix import FeatureMatrix


def run_ultra_cheap_screen_parallel(
    sleeves: Sequence[SleeveSpec],
    matrices: Mapping[str, FeatureMatrix],
    *,
    policy: CheapScreenPolicy,
    worker_count: int,
) -> CheapScreenResult:
    """Evaluate independent markets concurrently and merge deterministically.

    Every worker receives one read-only feature matrix and a disjoint sleeve
    subset.  The frozen v1 screen remains the authoritative computation; this
    adapter only changes scheduling.  It owns no DB or registry writer.
    """

    if worker_count < 1:
        raise ValueError("worker_count must be positive")
    started = time.perf_counter()
    markets = tuple(
        market
        for market in sorted({row.market for row in sleeves})
        if market in matrices
    )
    by_market = {
        market: tuple(row for row in sleeves if row.market == market)
        for market in markets
    }

    def evaluate(market: str) -> CheapScreenResult:
        return run_ultra_cheap_screen(
            by_market[market],
            {market: matrices[market]},
            policy=policy,
        )

    if len(markets) <= 1 or worker_count == 1:
        results = [evaluate(market) for market in markets]
    else:
        with ThreadPoolExecutor(
            max_workers=min(worker_count, len(markets)),
            thread_name_prefix="hydra-cheap-screen",
        ) as pool:
            futures = {market: pool.submit(evaluate, market) for market in markets}
            # Deliberately collect by market rather than completion order.
            results = [futures[market].result() for market in markets]

    return _merge_results(
        sleeves,
        results,
        policy=policy,
        elapsed_seconds=time.perf_counter() - started,
    )


def run_ultra_cheap_screen_processes(
    sleeves: Sequence[SleeveSpec],
    matrix_roots: Mapping[str, str | Path],
    *,
    policy: CheapScreenPolicy,
    worker_count: int,
) -> CheapScreenResult:
    """Evaluate markets in isolated processes and merge in the coordinator.

    Workers open canonical arrays read-only from their hash-checked manifests.
    They never receive a DB path and return results to the single coordinator.
    """

    if worker_count < 1:
        raise ValueError("worker_count must be positive")
    started = time.perf_counter()
    markets = tuple(
        market
        for market in sorted({row.market for row in sleeves})
        if market in matrix_roots
    )
    tasks = tuple(
        (
            market,
            tuple(row for row in sleeves if row.market == market),
            str(Path(matrix_roots[market]).resolve()),
            policy,
        )
        for market in markets
    )
    if len(tasks) <= 1 or worker_count == 1:
        results = [_evaluate_market_from_root(task) for task in tasks]
    else:
        with ProcessPoolExecutor(
            max_workers=min(worker_count, len(tasks)),
            mp_context=get_context("spawn"),
        ) as pool:
            futures = [pool.submit(_evaluate_market_from_root, task) for task in tasks]
            # Submission order is canonical market order.
            results = [future.result() for future in futures]
    return _merge_results(
        sleeves,
        results,
        policy=policy,
        elapsed_seconds=time.perf_counter() - started,
    )


def _evaluate_market_from_root(
    task: tuple[str, tuple[SleeveSpec, ...], str, CheapScreenPolicy],
) -> CheapScreenResult:
    market, sleeves, root, policy = task
    matrix = FeatureMatrix.open(root, mmap=True)
    return run_ultra_cheap_screen(sleeves, {market: matrix}, policy=policy)


def _merge_results(
    sleeves: Sequence[SleeveSpec],
    results: Sequence[CheapScreenResult],
    *,
    policy: CheapScreenPolicy,
    elapsed_seconds: float,
) -> CheapScreenResult:
    rows = sorted(
        (row for result in results for row in result.rows),
        key=lambda row: str(row["sleeve_id"]),
    )
    return CheapScreenResult(
        policy=policy,
        proposal_count=len(sleeves),
        bound_count=sum(result.bound_count for result in results),
        unique_execution_path_count=sum(
            result.unique_execution_path_count for result in results
        ),
        execution_cache_hit_count=sum(
            result.execution_cache_hit_count for result in results
        ),
        rows=tuple(rows),
        elapsed_seconds=elapsed_seconds,
    )


__all__ = [
    "run_ultra_cheap_screen_parallel",
    "run_ultra_cheap_screen_processes",
]
