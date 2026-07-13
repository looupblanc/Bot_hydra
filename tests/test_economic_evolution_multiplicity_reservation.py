from __future__ import annotations

import shutil
from pathlib import Path

from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from scripts.reserve_economic_evolution_pilot_multiplicity import (
    EXPECTED_AFTER,
    EVENT_ID,
    reserve,
)


def test_economic_evolution_reservation_is_hash_bound_and_idempotent(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    worm_target = tmp_path / "config/v7/economic_evolution_pilot_0001.json"
    worm_target.parent.mkdir(parents=True)
    shutil.copy2(root / "config/v7/economic_evolution_pilot_0001.json", worm_target)
    registry_target = tmp_path / "mission/state/proof_registry.json"
    registry_target.parent.mkdir(parents=True)
    shutil.copy2(root / "mission/state/proof_registry.json", registry_target)

    first = reserve(
        project_root=tmp_path,
        proof_registry_path=registry_target,
    )
    second = reserve(
        project_root=tmp_path,
        proof_registry_path=registry_target,
    )
    registry = load_and_verify(registry_target)

    assert first == second
    assert first["event_id"] == EVENT_ID
    assert multiplicity_trial_count(registry) == EXPECTED_AFTER
    assert burned_window_ids(registry) == ("Q4_2024",)
    assert sum(row["event_id"] == EVENT_ID for row in registry["entries"]) == 1
