from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import hydra.research.economic_evolution_coverage_three_zone_campaign as base
import hydra.research.economic_evolution_partial_runner_campaign as v1
from hydra.economic_evolution.account_partial_runner_evaluation_v2 import (
    build_partial_runner_exact_runtimes_v2,
)


PARTIAL_RUNNER_ENGINE_VERSION_V2 = "hydra_partial_runner_campaign_v2"


def run_partial_runner_campaign_v2(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    with _bound_partial_runner_campaign_v2():
        return base.run_coverage_three_zone_campaign(
            output_dir,
            preregistration_path=preregistration_path,
            contract_map_path=contract_map_path,
            cache_root=cache_root,
        )


@contextmanager
def _bound_partial_runner_campaign_v2() -> Iterator[None]:
    with v1._bound_partial_runner_campaign():
        prior_builder = base._build_exact_runtimes
        prior_engine = base.THREE_ZONE_ENGINE_VERSION
        base._build_exact_runtimes = build_partial_runner_exact_runtimes_v2
        base.THREE_ZONE_ENGINE_VERSION = PARTIAL_RUNNER_ENGINE_VERSION_V2
        try:
            yield
        finally:
            base._build_exact_runtimes = prior_builder
            base.THREE_ZONE_ENGINE_VERSION = prior_engine


__all__ = [
    "PARTIAL_RUNNER_ENGINE_VERSION_V2",
    "run_partial_runner_campaign_v2",
]
