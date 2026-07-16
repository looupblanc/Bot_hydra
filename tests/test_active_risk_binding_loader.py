from __future__ import annotations

import hashlib
import inspect
import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest
import hydra.shadow.active_risk_binding_loader as binding_loader_module

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.economic_evolution.schema import EconomicRole, SleeveSpec
from hydra.production.portfolio_books import SleeveRecord, stable_hash
from hydra.research.turbo_feature_builder import (
    FEATURE_BUNDLE_VERSION,
    FEATURE_DAG_HASH,
)
from hydra.shadow.active_risk_binding_loader import (
    ActiveRiskBindingLoaderError,
    build_active_risk_package_from_sealed_selection,
    load_frozen_active_risk_sources,
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _array(root: Path, name: str, value: np.ndarray) -> dict[str, object]:
    path = root / f"{name}.npy"
    np.save(path, value, allow_pickle=False)
    return {
        "path": path.name,
        "sha256": _sha(path),
        "shape": list(value.shape),
        "dtype": str(value.dtype),
    }


def _report_evidence_contract(
    *, verification: str = "TWO_PREREGISTERED_DEEP_GUARDS_REUSED"
) -> dict[str, object]:
    value: dict[str, object] = {
        "path": "/repo/data/cache/evidence_bundles/campaign.evidence-v1",
        "manifest_sha256": "1" * 64,
        "bundle_content_sha256": "2" * 64,
        "dataset_row_counts": {"episodes": 152_064, "provenance": 1},
        "verification": verification,
    }
    if verification == "TWO_PREREGISTERED_DEEP_GUARDS_REUSED":
        value.update(
            {
                "preregistered_deep_guard_count": 2,
                "additional_deep_guard_performed_by_report": False,
            }
        )
    return value


def _matches_report_evidence_contract(value: dict[str, object]) -> bool:
    return binding_loader_module._report_evidence_contract_matches(
        value,
        path="/repo/data/cache/evidence_bundles/campaign.evidence-v1",
        manifest_sha256="1" * 64,
        bundle_content_sha256="2" * 64,
        dataset_row_counts={"episodes": 152_064, "provenance": 1},
    )


def test_report_evidence_contract_accepts_exact_deep_guard_forms() -> None:
    assert _matches_report_evidence_contract(_report_evidence_contract())
    assert _matches_report_evidence_contract(
        _report_evidence_contract(verification="DEEP_VERIFIED")
    )


def test_report_evidence_contract_rejects_any_nonexact_guard_contract() -> None:
    current = _report_evidence_contract()
    mutations = []
    for key, value in (
        ("path", "/repo/data/cache/evidence_bundles/other.evidence-v1"),
        ("manifest_sha256", "3" * 64),
        ("bundle_content_sha256", "4" * 64),
        ("dataset_row_counts", {"episodes": 152_063, "provenance": 1}),
        ("verification", "DEEP_VERIFIED"),
        ("preregistered_deep_guard_count", 1),
        ("additional_deep_guard_performed_by_report", True),
    ):
        mutations.append({**current, key: value})
    mutations.append({**current, "unrecognized_provenance_claim": True})
    mutations.append(
        {
            key: value
            for key, value in current.items()
            if key != "preregistered_deep_guard_count"
        }
    )

    assert all(not _matches_report_evidence_contract(row) for row in mutations)


def _fixture(tmp_path: Path) -> tuple[Path, dict[str, object], Path]:
    campaign_id = "hydra_active_risk_pool_target_velocity_0026"
    source_campaign = "hydra_economic_evolution_source_test_0001"
    cheap_policy = {
        "calibration_start": "2023-01-01",
        "calibration_end_exclusive": "2023-07-01",
        "screen_start": "2023-07-01",
        "screen_end_exclusive": "2024-01-01",
    }
    source_manifest = {
        "campaign_id": source_campaign,
        "cheap_screen_policy": cheap_policy,
    }
    source_manifest["preregistration_hash"] = stable_hash(source_manifest)
    _write_json(tmp_path / "config/v7/source_test.json", source_manifest)

    matrix_root = tmp_path / "data/cache/economic_evolution/features/matrix"
    matrix_root.mkdir(parents=True)
    start = date(2023, 1, 2)
    session_day = np.array(
        [(start + timedelta(days=index // 2) - date(1970, 1, 1)).days for index in range(240)],
        dtype=np.int32,
    )
    session_code = np.zeros(240, dtype=np.int16)
    trigger = np.linspace(-1.0, 2.0, 240, dtype=np.float64)
    context = np.linspace(3.0, -2.0, 240, dtype=np.float64)
    arrays = {
        "session_day": _array(matrix_root, "session_day", session_day),
        "session_code": _array(matrix_root, "session_code", session_code),
        "feature__past_volatility": _array(
            matrix_root, "feature__past_volatility", trigger
        ),
        "feature__ctx_15m_return": _array(
            matrix_root, "feature__ctx_15m_return", context
        ),
    }
    source_data = "1" * 64
    roll_map = "2" * 64
    feature_manifest = {
        "arrays": arrays,
        "availability_contract": "completed_source_bar_at_or_before_decision",
        "key": {
            "start_inclusive": "2023-01-01",
            "end_exclusive": "2024-10-01",
            "market": "ES",
            "source_data_sha256": source_data,
            "roll_map_hash": roll_map,
            "transformation_version": FEATURE_BUNDLE_VERSION,
            "feature_dag_hash": FEATURE_DAG_HASH,
        },
        "mutable": False,
        "provenance": {
            "market": "ES",
            "execution_market": "MES",
            "features": ["past_volatility", "ctx_15m_return"],
            "outbound_order_capability": False,
            "q4_access_count_delta": 0,
            "data_fingerprint": source_data,
            "contract_map_sha256": roll_map,
        },
        "row_count": 240,
        "schema": "hydra_canonical_feature_store_v2",
        "writer_count": 1,
    }
    feature_manifest["bundle_hash"] = stable_hash(feature_manifest)
    _write_json(matrix_root / "manifest.json", feature_manifest)

    specs: dict[str, SleeveSpec] = {}
    records: dict[str, SleeveRecord] = {}
    screen_rows: list[dict[str, object]] = []
    selected = (
        (session_day >= (date(2023, 1, 1) - date(1970, 1, 1)).days)
        & (session_day < (date(2023, 7, 1) - date(1970, 1, 1)).days)
        & (session_code >= 0)
    )
    trigger_threshold = float(np.quantile(trigger[selected], 0.75))
    context_threshold = float(np.quantile(context[selected], 0.25))
    ids = tuple(f"sleeve_{index:02d}" for index in range(18))
    for sleeve_id in ids:
        spec = SleeveSpec(
            sleeve_id=sleeve_id,
            component_ids=(f"component_{sleeve_id}",),
            market="ES",
            execution_market="MES",
            timeframe="15m",
            session_code=0,
            trigger_feature="past_volatility",
            trigger_operator="GT",
            trigger_quantile=0.75,
            context_feature="ctx_15m_return",
            context_operator="LT",
            context_quantile=0.25,
            side=1,
            holding_bars=15,
            exit_style="TIME_ONLY",
            role=EconomicRole.PRIMARY_ALPHA,
            source_campaign=source_campaign,
            lineage_id=f"lineage_{sleeve_id}",
        )
        immutable = stable_hash({"execution": sleeve_id})
        record = SleeveRecord(
            sleeve_id=sleeve_id,
            immutable_fingerprint=immutable,
            behavioral_fingerprint=spec.behavioral_fingerprint,
            signal_ledger_sha256=stable_hash({"signals": sleeve_id}),
            trade_ledger_sha256=stable_hash({"trades": sleeve_id}),
            market="ES",
            contract="MES",
            timeframe="15m",
            session="OPEN",
            economic_role="PRIMARY_ALPHA",
            source_campaign=source_campaign,
            family_id=spec.lineage_id,
        )
        specs[sleeve_id] = spec
        records[sleeve_id] = record
        screen_rows.append(
            {
                "sleeve_id": sleeve_id,
                "structural_fingerprint": spec.structural_fingerprint,
                "behavioral_fingerprint": spec.behavioral_fingerprint,
                "execution_fingerprint": immutable,
                "market": "ES",
                "execution_market": "MES",
                "finite": True,
                "trigger_threshold": trigger_threshold,
                "context_threshold": context_threshold,
            }
        )
    screen_dir = tmp_path / "reports/economic_evolution/source_test_0001"
    _write_json(screen_dir / "preregistration_copy.json", source_manifest)
    screen_path = screen_dir / "cheap_screen_results.jsonl"
    screen_path.parent.mkdir(parents=True, exist_ok=True)
    screen_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in screen_rows),
        encoding="utf-8",
    )

    policy = ActiveRiskPoolPolicy(
        policy_id="active_pool_finalist",
        component_priority=ids,
        nominal_risk_charge_per_mini=tuple((sleeve_id, 250.0) for sleeve_id in ids),
        maximum_concurrent_sleeves=4,
        aggregate_open_risk_ceiling=2_000.0,
        maximum_mll_buffer_fraction=0.5,
        protected_mll_buffer=500.0,
        maximum_mini_equivalent=15.0,
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=2_000.0,
        daily_consistency_profit_guard=4_000.0,
        target_protection_distance=1_000.0,
        target_protection_mode=TargetProtectionMode.SCALE_50,
        static_risk_tier=1.0,
    )
    membership = []
    members = []
    for sleeve_id in ids:
        spec = specs[sleeve_id]
        record = records[sleeve_id]
        membership.append(
            {
                "component_id": sleeve_id,
                "component_role": record.economic_role,
                "risk_allocation": 1.0,
                "immutable_fingerprint": record.immutable_fingerprint,
                "behavioral_fingerprint": record.behavioral_fingerprint,
                "signal_ledger_sha256": record.signal_ledger_sha256,
                "trade_ledger_sha256": record.trade_ledger_sha256,
                "market": record.market,
                "contract": record.contract,
                "timeframe": record.timeframe,
                "session": record.session,
                "source_campaign": record.source_campaign,
                "sleeve_specification": spec.to_dict(),
            }
        )
        members.append(
            {
                "sleeve_id": sleeve_id,
                "sleeve_specification": spec.to_dict(),
                "record": record.to_dict(),
            }
        )
    declaration = {
        "policy_id": policy.policy_id,
        "active_risk_policy": policy.to_dict(),
        "active_risk_policy_sha256": stable_hash(policy.to_dict()),
        "membership": membership,
        "membership_sha256": stable_hash(membership),
    }
    campaign = {
        "campaign_id": campaign_id,
        "source_commit": "a" * 40,
        "data": {
            "role": "DEVELOPMENT_ONLY_Q4_EXCLUDED",
            "period": ["2023-01-01", "2024-10-01"],
            "feature_source_fingerprint": source_data,
            "contract_map_sha256": roll_map,
            "cached_features_only": True,
            "feature_recalculation_allowed": False,
            "q4_access_allowed": False,
            "new_purchase_allowed": False,
        },
        "sleeve_bank": {"member_count": 18, "members": members},
    }
    campaign["manifest_hash"] = stable_hash(campaign)
    campaign_path = tmp_path / "config/v7/active.json"
    _write_json(campaign_path, campaign)
    return campaign_path, declaration, screen_path


def test_loader_reconstructs_exact_eighteen_frozen_bindings(tmp_path: Path) -> None:
    campaign_path, declaration, _screen_path = _fixture(tmp_path)
    decoy = tmp_path / "reports/economic_evolution/000_lexicographic_decoy"
    decoy.mkdir(parents=True)
    (decoy / "cheap_screen_results.jsonl").write_text(
        '{"sleeve_id":"sleeve_00","trigger_threshold":999}\n',
        encoding="utf-8",
    )
    sources = load_frozen_active_risk_sources(
        repository_root=tmp_path,
        campaign_manifest_path=campaign_path,
        frozen_finalist_declaration=declaration,
    )

    assert len(sources.signal_bindings) == 18
    assert sources.audit["binding_count"] == 18
    assert sources.audit["thresholds_recalibrated_for_forward_use"] is False
    assert sources.audit["all_calibrations_exactly_reproduced"] is True
    first = sources.signal_bindings["sleeve_00"]
    assert first.calibration_start == "2023-01-01"
    assert first.calibration_end_exclusive == "2023-07-01"
    assert first.trigger_finite_observation_count == 240
    assert first.context_finite_observation_count == 240
    assert first.feature_matrix_market == "ES"
    assert first.feature_matrix_execution_market == "MES"
    assert sources.audit["screen_source_resolution"]["global_directory_search_used"] is False
    assert "freeze_timestamp_utc" not in inspect.signature(
        build_active_risk_package_from_sealed_selection
    ).parameters


def test_loader_fails_closed_when_persisted_threshold_does_not_reproduce(
    tmp_path: Path,
) -> None:
    campaign_path, declaration, screen_path = _fixture(tmp_path)
    rows = [json.loads(line) for line in screen_path.read_text().splitlines()]
    rows[0]["trigger_threshold"] += 0.01
    screen_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )

    with pytest.raises(
        ActiveRiskBindingLoaderError,
        match="persisted cheap-screen calibrations disagree|cannot be reproduced",
    ):
        load_frozen_active_risk_sources(
            repository_root=tmp_path,
            campaign_manifest_path=campaign_path,
            frozen_finalist_declaration=declaration,
        )


def test_loader_rejects_q4_or_noncausal_feature_manifest(tmp_path: Path) -> None:
    campaign_path, declaration, _ = _fixture(tmp_path)
    manifest_path = next(
        (tmp_path / "data/cache/economic_evolution/features").glob("*/manifest.json")
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["provenance"]["q4_access_count_delta"] = 1
    manifest.pop("bundle_hash")
    manifest["bundle_hash"] = stable_hash(manifest)
    _write_json(manifest_path, manifest)
    with pytest.raises(ActiveRiskBindingLoaderError, match="provenance drift"):
        load_frozen_active_risk_sources(
            repository_root=tmp_path,
            campaign_manifest_path=campaign_path,
            frozen_finalist_declaration=declaration,
        )

    manifest["provenance"]["q4_access_count_delta"] = 0
    manifest["availability_contract"] = "future_bar_allowed"
    manifest.pop("bundle_hash")
    manifest["bundle_hash"] = stable_hash(manifest)
    _write_json(manifest_path, manifest)
    with pytest.raises(ActiveRiskBindingLoaderError, match="provenance drift"):
        load_frozen_active_risk_sources(
            repository_root=tmp_path,
            campaign_manifest_path=campaign_path,
            frozen_finalist_declaration=declaration,
        )


def test_pilot_fallback_is_explicit_hash_bound_and_cardinality_frozen(
    tmp_path: Path,
) -> None:
    ids = [f"pilot_sleeve_{index}" for index in range(7)]
    specs = {
        sleeve_id: SleeveSpec(
            sleeve_id=sleeve_id,
            component_ids=(f"component_{index}",),
            market="ES",
            execution_market="MES",
            timeframe="15m",
            session_code=0,
            trigger_feature="past_volatility",
            trigger_operator="GT",
            trigger_quantile=0.75,
            context_feature=None,
            context_operator=None,
            context_quantile=None,
            side=1,
            holding_bars=15,
            exit_style="TIME_ONLY",
            role=EconomicRole.PRIMARY_ALPHA,
            source_campaign="hydra_economic_evolution_pilot_0001",
            lineage_id=f"lineage_{index}",
        )
        for index, sleeve_id in enumerate(ids)
    }
    ledger = (
        tmp_path
        / "reports/economic_evolution/account_state_router_0011/cheap_screen_results.jsonl"
    )
    ledger.parent.mkdir(parents=True)
    rows = [{"sleeve_id": sleeve_id, "row": index} for index, sleeve_id in enumerate(ids)]
    ledger.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    _write_json(
        ledger.parent / "preregistration_copy.json",
        {
            "cheap_screen_policy": {
                "calibration_start": "2023-01-01",
                "calibration_end_exclusive": "2023-07-01",
            }
        },
    )
    fallback = {
        "schema": "hydra_pilot_0001_calibration_fallback_v1",
        "source_campaign": "hydra_economic_evolution_pilot_0001",
        "fallback_reason": "ORIGINAL_CHEAP_SCREEN_LEDGER_NOT_PERSISTED",
        "exact_replay_path": ledger.relative_to(tmp_path).as_posix(),
        "exact_replay_file_sha256": _sha(ledger),
        "allowed_sleeve_row_sha256": {
            row["sleeve_id"]: stable_hash(row) for row in rows
        },
        "outcome_fields_used": False,
        "threshold_identity_required": True,
        "future_directory_discovery_prohibited": True,
    }
    fallback["manifest_hash"] = stable_hash(fallback)
    fallback_path = (
        tmp_path / "config/v7/active_risk_pilot_0001_calibration_fallback.json"
    )
    _write_json(fallback_path, fallback)
    indexed, audit = binding_loader_module._screen_index(tmp_path, specs)
    assert all(len(indexed[sleeve_id]) == 1 for sleeve_id in ids)
    assert audit["pilot_fallback_sleeve_count"] == 7

    changed_rows = list(rows)
    changed_rows[0] = {**changed_rows[0], "row": 999}
    ledger.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in changed_rows),
        encoding="utf-8",
    )
    fallback["exact_replay_file_sha256"] = _sha(ledger)
    fallback.pop("manifest_hash")
    fallback["manifest_hash"] = stable_hash(fallback)
    _write_json(fallback_path, fallback)
    with pytest.raises(ActiveRiskBindingLoaderError, match="row hash drift"):
        binding_loader_module._screen_index(tmp_path, specs)

    fallback["allowed_sleeve_row_sha256"].pop(ids[-1])
    fallback.pop("manifest_hash")
    fallback["manifest_hash"] = stable_hash(fallback)
    _write_json(fallback_path, fallback)
    with pytest.raises(ActiveRiskBindingLoaderError, match="allowlist differs"):
        binding_loader_module._screen_index(tmp_path, specs)
