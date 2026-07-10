from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable


@dataclass
class PolicyArm:
    name: str
    trials: int = 0
    reward: float = 0.0

    @property
    def mean(self) -> float:
        return self.reward / max(self.trials, 1)


class UCBBandit:
    def __init__(self, names: Iterable[str], exploration: float = 0.35) -> None:
        self.arms = {name: PolicyArm(name) for name in names}
        self.exploration = exploration

    def choose(self) -> str:
        total = sum(arm.trials for arm in self.arms.values()) + 1
        for arm in self.arms.values():
            if arm.trials == 0:
                return arm.name
        return max(
            self.arms.values(),
            key=lambda arm: arm.mean + self.exploration * math.sqrt(math.log(total) / arm.trials),
        ).name

    def update(self, name: str, reward: float) -> None:
        arm = self.arms[name]
        arm.trials += 1
        arm.reward += float(reward)

    def snapshot(self) -> dict[str, dict[str, float]]:
        return {name: {"trials": arm.trials, "mean_reward": arm.mean} for name, arm in self.arms.items()}

