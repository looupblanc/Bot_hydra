from __future__ import annotations

import json
from pathlib import Path

from hydra.research.economic_evolution_pilot import (
    _load_preregistration,
    _validate_preregistration,
)


def test_pre_outcome_revision_preserves_population_and_thresholds() -> None:
    root = Path(__file__).resolve().parents[1]
    base_path = root / "config/v7/economic_evolution_pilot_0001.json"
    revision_path = (
        root / "config/v7/economic_evolution_pilot_0001_revision_01.json"
    )
    base = json.loads(base_path.read_text(encoding="utf-8"))
    effective, source = _load_preregistration(revision_path)
    _validate_preregistration(effective, revision_path)

    assert effective["structural_population"] == base["structural_population"]
    assert effective["funnel"] == base["funnel"]
    assert effective["cheap_screen_policy"] == base["cheap_screen_policy"]
    assert effective["incremental_value_policy"] == base["incremental_value_policy"]
    assert effective["account_research_gate"] == base["account_research_gate"]
    assert effective["combine_path_gate"] == base["combine_path_gate"]
    assert effective["revision_provenance"]["threshold_change"] is False
    assert effective["revision_provenance"]["population_change"] is False
    assert source["revision"]["economic_outcome_seen_before_revision"] is False
