from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np

from hydra.economic_evolution.incremental_value import (
    IncrementalValuePolicy,
    MatchedAccountObservation,
    evaluate_incremental_value,
)
from hydra.economic_evolution.schema import EconomicRole


@dataclass(frozen=True, slots=True)
class ValidatorCalibrationResult:
    seed: int
    repetitions: int
    starts_per_trial: int
    independent_blocks: int
    null_false_positive_rate: float
    meaningful_effect_power: float
    injected_stressed_net_effect: float
    injected_target_progress_effect: float
    null_positive_count: int
    injected_positive_count: int
    thresholds_changed: bool = False

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def calibrate_incremental_validator(
    policy: IncrementalValuePolicy,
    *,
    seed: int,
    repetitions: int = 256,
    starts_per_block: int = 4,
    noise_scale: float = 75.0,
) -> ValidatorCalibrationResult:
    """Calibrate fixed gates on synthetic null and positive controls only."""

    if repetitions < 32 or starts_per_block < 2 or noise_scale <= 0.0:
        raise ValueError("calibration requires substantive bounded controls")
    blocks = policy.minimum_independent_blocks
    starts = blocks * starts_per_block
    rng = np.random.default_rng(seed)
    meaningful_net = max(policy.minimum_stressed_net_uplift * 2.0, noise_scale)
    meaningful_progress = max(
        policy.minimum_target_progress_uplift * 2.0,
        meaningful_net / 9_000.0,
    )
    null_positive = 0
    injected_positive = 0
    for repetition in range(repetitions):
        baseline = _baseline(starts=starts, blocks=blocks)
        block_noise = rng.normal(0.0, noise_scale, size=blocks)
        idiosyncratic = rng.normal(0.0, noise_scale * 0.5, size=starts)
        null = _included(
            baseline,
            block_noise=block_noise,
            idiosyncratic=idiosyncratic,
            net_effect=0.0,
            progress_effect=0.0,
        )
        injected = _included(
            baseline,
            block_noise=block_noise,
            idiosyncratic=idiosyncratic,
            net_effect=meaningful_net,
            progress_effect=meaningful_progress,
        )
        null_positive += int(
            evaluate_incremental_value(
                f"null-{repetition}",
                EconomicRole.PRIMARY_ALPHA,
                baseline,
                null,
                policy=policy,
            ).status
            == "MICRO_EDGE_USEFUL"
        )
        injected_positive += int(
            evaluate_incremental_value(
                f"injected-{repetition}",
                EconomicRole.PRIMARY_ALPHA,
                baseline,
                injected,
                policy=policy,
            ).status
            == "MICRO_EDGE_USEFUL"
        )
    return ValidatorCalibrationResult(
        seed=seed,
        repetitions=repetitions,
        starts_per_trial=starts,
        independent_blocks=blocks,
        null_false_positive_rate=null_positive / repetitions,
        meaningful_effect_power=injected_positive / repetitions,
        injected_stressed_net_effect=meaningful_net,
        injected_target_progress_effect=meaningful_progress,
        null_positive_count=null_positive,
        injected_positive_count=injected_positive,
    )


def _baseline(
    *, starts: int, blocks: int
) -> tuple[MatchedAccountObservation, ...]:
    return tuple(
        MatchedAccountObservation(
            start_id=f"start-{index:03d}",
            block_id=f"block-{index % blocks:02d}",
            net_after_costs=0.0,
            stressed_net_after_costs=0.0,
            target_progress=0.0,
            mll_breached=False,
            consistency_ok=True,
            shared_loss_days=0,
            conflict_count=0,
            total_cost=0.0,
        )
        for index in range(starts)
    )


def _included(
    baseline: tuple[MatchedAccountObservation, ...],
    *,
    block_noise: np.ndarray,
    idiosyncratic: np.ndarray,
    net_effect: float,
    progress_effect: float,
) -> tuple[MatchedAccountObservation, ...]:
    block_ids = sorted({row.block_id for row in baseline})
    block_index = {value: index for index, value in enumerate(block_ids)}
    output: list[MatchedAccountObservation] = []
    for index, row in enumerate(baseline):
        noise = float(block_noise[block_index[row.block_id]] + idiosyncratic[index])
        output.append(
            MatchedAccountObservation(
                start_id=row.start_id,
                block_id=row.block_id,
                net_after_costs=net_effect + noise,
                stressed_net_after_costs=net_effect + noise,
                target_progress=progress_effect + noise / 9_000.0,
                mll_breached=False,
                consistency_ok=True,
                shared_loss_days=0,
                conflict_count=0,
                total_cost=0.0,
            )
        )
    return tuple(output)


__all__ = ["ValidatorCalibrationResult", "calibrate_incremental_validator"]
