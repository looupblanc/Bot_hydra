from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from hydra.promotion.decision_bridge_prepare import run_decision_bridge_v4_preparation
from hydra.promotion.final_cohort import FinalCohortError, build_final_cohort_manifest
from hydra.shadow.package_factory import ShadowPackageError, build_shadow_package


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _spec(candidate_id: str, market: str, role: int, lineage: str) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "family": f"family_{candidate_id}",
        "lineage_id": lineage,
        "market": market,
        "feature": "past_return_60",
        "operator": 2,
        "threshold": 0.001,
        "context_feature": "ctx_30m_return",
        "context_operator": -1,
        "context_threshold": 0.0,
        "holding_events": 5,
        "side": -1,
        "session_code": 2,
        "quantity": 1,
        "point_value": 20.0 if market == "NQ" else 1000.0,
        "round_turn_cost": 14.5 if market == "NQ" else 24.5,
        "timeframe": "1m|30m",
        "role": role,
        "version": 1,
    }


def _validation(spec: dict[str, object], role: str, execution_market: str) -> dict[str, object]:
    spec_hash = hashlib.sha256(
        json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "candidate_id": spec["candidate_id"],
        "immutable_specification_hash": spec_hash,
        "decision": "PRE_HOLDOUT_READY",
        "decision_reasons": [],
        "role": role,
        "primary_market": spec["market"],
        "execution_market": execution_market,
        "timeframe": spec["timeframe"],
        "stage_completion": {
            "full_economic_replay": True,
            "full_risk_replay": True,
            "full_promotion_validation": True,
        },
        "economic": {"complete": True, "events": 30, "net_pnl": 2500.0},
        "risk": {
            "complete": True,
            "mll_breached": False,
            "minimum_mll_buffer": 3500.0,
            "topstep": {
                "complete": True,
                "selected_micro_contracts": 2,
                "rule_version": "test_topstep_v1",
            },
        },
        "role_evidence": {"role": role},
    }


def _inputs(tmp_path: Path) -> dict[str, object]:
    specs = {
        "alpha": _spec("alpha", "NQ", 2, "lineage_alpha"),
        "payout": _spec("payout", "CL", 3, "lineage_payout"),
        "defensive": _spec("defensive", "NQ", 4, "lineage_defensive"),
    }
    validations = [
        _validation(specs["alpha"], "COMBINE_PASSER", "MNQ"),
        _validation(specs["payout"], "XFA_PAYOUT", "MCL"),
        _validation(specs["defensive"], "DEFENSIVE", "MNQ"),
    ]
    manifest = {
        "candidate_ids": ["alpha", "payout", "defensive"],
        "specifications": specs,
        "manifest_hash": "source-manifest",
    }
    clusters = [
        {"cluster_id": f"cluster_{candidate}", "member_ids": [candidate]}
        for candidate in specs
    ]
    paths = {}
    for name, payload in {
        "manifest": manifest,
        "validation": validations,
        "clusters": clusters,
    }.items():
        path = tmp_path / f"{name}.json"
        path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
        paths[name] = path
    policy = tmp_path / "policy.md"
    policy.write_text("frozen q4 policy\n", encoding="utf-8")
    task = tmp_path / "task.md"
    task.write_text("authorized engineering task\n", encoding="utf-8")
    return {
        "specs": specs,
        "validations": validations,
        "clusters": clusters,
        "manifest": manifest,
        "paths": paths,
        "policy": policy,
        "task": task,
    }


def test_shadow_package_is_complete_immutable_and_zero_order(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    spec = inputs["specs"]["alpha"]
    validation = inputs["validations"][0]
    package = build_shadow_package(
        spec,
        validation,
        source_commit="a" * 40,
        freeze_timestamp_utc="2026-07-12T00:00:00+00:00",
        evidence_sha256="b" * 64,
    )
    payload = package.to_dict()
    assert payload["broker_connectivity"] is False
    assert payload["outbound_order_capability"] is False
    assert payload["feature_contract"]["closed_bars_only"] is True
    assert payload["data_policy"]["post_freeze_only"] is True
    assert len(payload["package_hash"]) == 64

    unsafe = package.__class__(**{**package.__dict__, "outbound_order_capability": True})
    with pytest.raises(ShadowPackageError, match="broker or order"):
        unsafe.validate()


def test_final_cohort_enforces_distinct_cluster_and_caps(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    packages = {
        str(spec["candidate_id"]): build_shadow_package(
            spec,
            inputs["validations"][index],
            source_commit="a" * 40,
            freeze_timestamp_utc="2026-07-12T00:00:00+00:00",
            evidence_sha256="b" * 64,
        ).to_dict()
        for index, spec in enumerate(inputs["specs"].values())
    }
    manifest = build_final_cohort_manifest(
        pre_holdout_manifest=inputs["manifest"],
        validations=inputs["validations"],
        behavioral_clusters=inputs["clusters"],
        package_records=packages,
        source_commit="a" * 40,
        freeze_timestamp_utc="2026-07-12T00:00:00+00:00",
        policy_path=inputs["policy"],
        policy_sha256=_sha(inputs["policy"]),
        source_artifact_hashes={"source": "c" * 64},
        q4_access_count_before=0,
    )
    assert manifest["candidate_count"] == 3
    assert len({row["behavioral_cluster_id"] for row in manifest["candidates"]}) == 3
    assert manifest["q4_access_authorized"] is False

    duplicate_clusters = [
        {"cluster_id": "same", "member_ids": ["alpha", "payout", "defensive"]}
    ]
    with pytest.raises(FinalCohortError, match="Duplicate Level-2"):
        build_final_cohort_manifest(
            pre_holdout_manifest=inputs["manifest"],
            validations=inputs["validations"],
            behavioral_clusters=duplicate_clusters,
            package_records=packages,
            source_commit="a" * 40,
            freeze_timestamp_utc="2026-07-12T00:00:00+00:00",
            policy_path=inputs["policy"],
            policy_sha256=_sha(inputs["policy"]),
            source_artifact_hashes={"source": "c" * 64},
            q4_access_count_before=0,
        )


def test_preparation_freezes_packages_and_manifest_without_q4(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs = _inputs(tmp_path)
    monkeypatch.setattr(
        "subprocess.check_output", lambda *args, **kwargs: "a" * 40 + "\n"
    )
    result = run_decision_bridge_v4_preparation(
        tmp_path / "output",
        pre_holdout_manifest_path=inputs["paths"]["manifest"],
        pre_holdout_manifest_sha256=_sha(inputs["paths"]["manifest"]),
        complete_validation_path=inputs["paths"]["validation"],
        complete_validation_sha256=_sha(inputs["paths"]["validation"]),
        behavioral_clusters_path=inputs["paths"]["clusters"],
        behavioral_clusters_sha256=_sha(inputs["paths"]["clusters"]),
        policy_path=inputs["policy"],
        policy_sha256=_sha(inputs["policy"]),
        engineering_task_path=inputs["task"],
        engineering_task_sha256=_sha(inputs["task"]),
        code_commit="a" * 40,
        freeze_timestamp_utc="2026-07-12T00:00:00+00:00",
        q4_access_count=0,
    )
    assert result["candidate_count"] == 3
    assert result["q4_access_count"] == 0
    assert result["q4_access_authorized"] is False
    manifest = json.loads(Path(result["cohort_manifest_path"]).read_text())
    assert manifest["status"] == "FINAL_Q4_COHORT_FROZEN_UNAUTHORIZED"
    assert all(Path(path).is_file() for path in result["shadow_package_paths"].values())
