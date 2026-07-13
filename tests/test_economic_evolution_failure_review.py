from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.research.economic_evolution_failure_review import (
    EconomicEvolutionFailureReviewError,
    load_failure_review_preregistration,
    run_economic_evolution_failure_review,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )


def _fixture(root: Path) -> Path:
    policy = {
        "policy_id": "policy_frozen",
        "sleeve_ids": ["sleeve_alpha", "sleeve_diversifier"],
    }
    policy_hash = stable_hash(policy)
    sources = root / "reports/source"
    _write_json(
        sources / "validation.json",
        {
            "candidate_id": "policy_frozen",
            "candidate_specification_hash": policy_hash,
            "scientific_status": "EXPENSIVE_VALIDATION_UNDERPOWERED",
            "validated": False,
            "independent_confirmation_queue_eligible": False,
            "gates": {"normal_net_positive": True, "DSR": False},
        },
    )

    def profile(net: float, progress: float, consistency: float) -> dict[str, object]:
        return {
            "daily_observation_count": 154,
            "pooled_net_pnl": net,
            "positive_block_count": 3,
            "block_count": 4,
            "target_progress_median": progress,
            "target_progress_maximum": progress * 2.0,
            "mll_breach_rate": 0.0,
            "minimum_mll_buffer": 2500.0,
            "consistency_pass_rate": consistency,
            "block_results": [
                {
                    "block_id": f"B{index + 1}",
                    "net_pnl": value,
                    "target_progress": value / 9000.0,
                    "consistency_ok": index == 0,
                    "mll_breached": False,
                }
                for index, value in enumerate((1000.0, -500.0, 750.0, 1250.0))
            ],
        }

    _write_json(
        sources / "profiles.json",
        {
            "CONTROLLED_BASE": profile(4600.0, 0.18, 0.25),
            "CONTROLLED_STRESS_1_5X": profile(3800.0, 0.15, 0.25),
            "CONTROLLED_STRESS_2X": profile(3000.0, 0.13, 0.25),
        },
    )
    _write_json(
        sources / "statistics.json",
        {
            "effective_sample": {"effective_independent_observations": 154.0},
            "block_bootstrap": {
                "confidence_interval_95": [-10.0, 60.0],
                "probability_mean_net_positive": 0.92,
            },
            "block_sign_randomization": {"one_sided_p_value": 0.076},
            "power_calibration": {"power_on_minimum_useful_effect": 0.0},
            "DSR": {"deflated_z": -2.4},
            "BH": {"rejected": False},
        },
    )
    _write_json(
        sources / "controls.json",
        {
            "static_control_dominates": False,
            "dominating_leave_one_out_sleeves": [],
        },
    )
    _write_jsonl(
        sources / "policies.jsonl",
        [{"policy": policy, "development_only": True, "validated": False}],
    )
    _write_jsonl(
        sources / "components.jsonl",
        [
            {
                "sleeve_id": "sleeve_alpha",
                "signal_market": "NQ",
                "execution_market": "MNQ",
                "role": "PRIMARY_ALPHA",
                "net_pnl": 1000.0,
                "cost_stress_1_5x_net": 800.0,
            },
            {
                "sleeve_id": "sleeve_diversifier",
                "signal_market": "CL",
                "execution_market": "MCL",
                "role": "MARKET_DIVERSIFIER",
                "net_pnl": -100.0,
                "cost_stress_1_5x_net": -200.0,
            },
        ],
    )
    implementation = root / "hydra/review_impl.py"
    implementation.parent.mkdir(parents=True)
    implementation.write_text("FROZEN = True\n", encoding="utf-8")
    script = root / "scripts/run_review.py"
    script.parent.mkdir(parents=True)
    script.write_text("FROZEN = True\n", encoding="utf-8")
    paths = {
        "validation_result": "reports/source/validation.json",
        "profile_results": "reports/source/profiles.json",
        "statistical_validation": "reports/source/statistics.json",
        "matched_controls": "reports/source/controls.json",
        "source_account_policies": "reports/source/policies.jsonl",
        "exact_components": "reports/source/components.jsonl",
    }
    config: dict[str, object] = {
        "schema": (
            "hydra_economic_evolution_failure_directed_review_"
            "preregistration_v1"
        ),
        "review_id": "review_0006",
        "implementation_commit": "frozen_commit",
        "implementation_files": {
            "hydra/review_impl.py": _sha256(implementation),
            "scripts/run_review.py": _sha256(script),
        },
        "retrospective_only": True,
        "new_statistical_comparisons_allowed": False,
        "multiplicity_delta": 0,
        "candidate": {
            "policy_id": "policy_frozen",
            "policy_specification_hash": policy_hash,
            "sleeve_ids": ["sleeve_alpha", "sleeve_diversifier"],
        },
        "source_artifacts": {
            "paths": paths,
            "sha256": {
                key: _sha256(root / relative) for key, relative in paths.items()
            },
        },
        "diagnostic_reference": {"useful_target_progress": 0.75},
        "next_research_class": {
            "class_id": "INDEPENDENT_DENSITY",
            "next_experiment_id": "campaign_0007",
            "new_ids_required": True,
        },
        "q4_access_allowed": False,
        "new_data_purchase_allowed": False,
        "network_access_allowed": False,
        "broker_or_orders_allowed": False,
        "shadow_admission_allowed": False,
        "proof_window_consumption_allowed": False,
        "pre_holdout_promotion_allowed": False,
        "paper_shadow_promotion_allowed": False,
        "parameter_rescue_allowed": False,
        "status_inheritance_allowed": False,
        "CONTRE": "The retrospective decision is selected after development outcomes.",
    }
    config["preregistration_hash"] = stable_hash(config)
    path = root / "config/v7/failure_review.json"
    _write_json(path, config)
    return path


@pytest.fixture(autouse=True)
def _git_ancestor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "hydra.research.economic_evolution_failure_review.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0),
    )


def test_failure_review_is_retrospective_and_never_promotes(tmp_path: Path) -> None:
    config = _fixture(tmp_path)
    result = run_economic_evolution_failure_review(
        tmp_path / "reports/output",
        preregistration_path=config,
    )

    assert result["dominant_failure"] == "INSUFFICIENT_STATISTICAL_POWER"
    assert result["candidate_exact_status"] == (
        "FROZEN_DEVELOPMENT_UNDERPOWERED_NO_PROOF"
    )
    assert result["class_status"] == "CLASS_REFORMULATION_ALLOWED_NEW_IDS_ONLY"
    assert result["multiplicity_delta"] == 0
    assert result["decision"]["mutate_exact_policy"] is False
    assert result["decision"]["consume_independent_proof"] is False
    assert result["decision"]["class_level_reformulation"] is True
    assert result["pre_holdout_ready_count"] == 0
    assert result["paper_shadow_ready_count"] == 0
    assert result["orders"] == 0
    assert result["observed_evidence"]["negative_blocks"] == ["B2"]
    assert [
        row["sleeve_id"]
        for row in result["observed_evidence"][
            "negative_stressed_exact_components"
        ]
    ] == ["sleeve_diversifier"]


def test_failure_review_rejects_any_protected_action(tmp_path: Path) -> None:
    config_path = _fixture(tmp_path)
    config = json.loads(config_path.read_text())
    config["q4_access_allowed"] = True
    config["preregistration_hash"] = stable_hash(
        {key: value for key, value in config.items() if key != "preregistration_hash"}
    )
    _write_json(config_path, config)

    with pytest.raises(EconomicEvolutionFailureReviewError, match="q4_access"):
        load_failure_review_preregistration(config_path)


def test_failure_review_rejects_source_drift(tmp_path: Path) -> None:
    config_path = _fixture(tmp_path)
    (tmp_path / "reports/source/statistics.json").write_text(
        '{"tampered": true}', encoding="utf-8"
    )

    with pytest.raises(EconomicEvolutionFailureReviewError, match="source drift"):
        run_economic_evolution_failure_review(
            tmp_path / "reports/output",
            preregistration_path=config_path,
        )
