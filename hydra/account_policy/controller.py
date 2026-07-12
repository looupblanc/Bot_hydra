from __future__ import annotations

from typing import Iterable

from hydra.account_policy.schema import BasketPolicy, ControllerPolicy, stable_hash


def generate_controller_population(
    basket: BasketPolicy,
    *,
    generation_index: int,
) -> tuple[ControllerPolicy, ...]:
    """Create bounded deterministic controller variants for one basket."""

    templates = (
        (1_500.0, 2_500.0, 2, 2_250.0, 900.0, ()),
        (2_000.0, 3_000.0, 2, 1_800.0, 750.0, ()),
        (2_500.0, 4_000.0, 3, 1_500.0, 600.0, ()),
        (1_750.0, 3_500.0, 2, 2_000.0, 800.0, ("VOLATILITY_NORMAL", "VOLATILITY_CONTRACTION")),
    )
    output: list[ControllerPolicy] = []
    priority = basket.component_priority or basket.component_ids
    for index, (loss, lock, streak, low, critical, regimes) in enumerate(templates):
        raw = {
            "basket": basket.policy_id,
            "generation": generation_index,
            "template": index,
            "priority": priority,
        }
        output.append(
            ControllerPolicy(
                controller_id="controller_" + stable_hash(raw)[:18],
                basket_policy_id=basket.policy_id,
                component_priority=tuple(priority),
                daily_loss_limit=loss,
                daily_profit_lock=lock,
                loss_streak_derisk_after=streak,
                low_buffer_threshold=low,
                critical_buffer_threshold=critical,
                maximum_simultaneous_positions=min(
                    basket.maximum_simultaneous_positions, 3
                ),
                maximum_mini_equivalent=basket.maximum_mini_equivalent,
                allow_regimes=tuple(regimes),
            )
        )
    output.append(
        ControllerPolicy(
            controller_id="random_router_"
            + stable_hash({"basket": basket.policy_id, "generation": generation_index})[:16],
            basket_policy_id=basket.policy_id,
            component_priority=tuple(priority),
            daily_loss_limit=100_000.0,
            daily_profit_lock=100_000.0,
            loss_streak_derisk_after=999,
            low_buffer_threshold=1.0,
            critical_buffer_threshold=0.5,
            maximum_simultaneous_positions=basket.maximum_simultaneous_positions,
            maximum_mini_equivalent=basket.maximum_mini_equivalent,
            random_control_seed=71 + generation_index,
            routing_policy="MATCHED_RANDOM_HASH_CONTROL",
        )
    )
    return tuple(output)


def controller_is_immutable(policy: ControllerPolicy) -> bool:
    return bool(policy.fingerprint and policy.policy_version)


def controller_population_fingerprints(
    policies: Iterable[ControllerPolicy],
) -> tuple[str, ...]:
    return tuple(sorted(policy.fingerprint for policy in policies))


__all__ = [
    "controller_is_immutable",
    "controller_population_fingerprints",
    "generate_controller_population",
]
