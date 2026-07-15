from __future__ import annotations

import hashlib
import json

import pytest

from hydra.economic_evolution.schema import EconomicRole, SleeveSpec
from hydra.production.portfolio_books import BookPair, SleeveRecord
from hydra.shadow.package_factory import PACKAGE_SCHEMA, write_shadow_package
from hydra.shadow.portfolio_package import (
    PortfolioShadowPackageError,
    build_portfolio_shadow_package,
    reconstruct_portfolio_shadow_package,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _spec(
    sleeve_id: str,
    *,
    market: str,
    execution_market: str,
    timeframe: str,
    session_code: int,
    role: EconomicRole,
) -> SleeveSpec:
    return SleeveSpec(
        sleeve_id=sleeve_id,
        component_ids=(f"component-{sleeve_id}",),
        market=market,
        execution_market=execution_market,
        timeframe=timeframe,
        session_code=session_code,
        trigger_feature="past_volatility",
        trigger_operator="GT",
        trigger_quantile=0.75,
        context_feature="ctx_30m_return",
        context_operator="LT",
        context_quantile=0.35,
        side=1 if market != "NQ" else -1,
        holding_bars=30,
        exit_style="TIME_ONLY",
        role=role,
        source_campaign="hydra_economic_production_0024",
        lineage_id=f"lineage-{sleeve_id}",
    )


def _record(spec: SleeveSpec) -> SleeveRecord:
    return SleeveRecord(
        sleeve_id=spec.sleeve_id,
        immutable_fingerprint=spec.structural_fingerprint,
        behavioral_fingerprint=spec.behavioral_fingerprint,
        signal_ledger_sha256=_sha(f"signal:{spec.sleeve_id}"),
        trade_ledger_sha256=_sha(f"trade:{spec.sleeve_id}"),
        market=spec.market,
        contract=spec.execution_market,
        timeframe=spec.timeframe,
        session={-1: "ALL", 0: "OPEN", 1: "MIDDLE", 2: "LATE"}[
            spec.session_code
        ],
        economic_role=spec.role.value,
        source_campaign=spec.source_campaign,
        family_id=f"family-{spec.sleeve_id}",
    )


def _book_and_specs() -> tuple[BookPair, dict[str, SleeveSpec]]:
    specs = {
        row.sleeve_id: row
        for row in (
            _spec(
                "sleeve-es",
                market="ES",
                execution_market="MES",
                timeframe="15m",
                session_code=0,
                role=EconomicRole.CONSISTENCY_SMOOTHER,
            ),
            _spec(
                "sleeve-nq",
                market="NQ",
                execution_market="MNQ",
                timeframe="30m",
                session_code=2,
                role=EconomicRole.PRIMARY_ALPHA,
            ),
            _spec(
                "sleeve-cl",
                market="CL",
                execution_market="MCL",
                timeframe="60m",
                session_code=1,
                role=EconomicRole.PAYOUT_STABILIZER,
            ),
        )
    }
    records = {key: _record(value) for key, value in specs.items()}
    pair = BookPair.create(
        combine_sleeves=(records["sleeve-nq"], records["sleeve-es"]),
        combine_allocation_units=(2, 1),
        combine_risk_tier=1.15,
        xfa_sleeves=(records["sleeve-cl"], records["sleeve-es"]),
        xfa_allocation_units=(1, 2),
        xfa_risk_tier=0.75,
        conflict_policy="PRIORITY",
        behaviorally_novel=True,
        generator_seed=25,
        proposal_index=7,
    )
    return pair, specs


def _build():
    pair, specs = _book_and_specs()
    return pair, specs, build_portfolio_shadow_package(
        pair,
        specs,
        source_commit="a" * 40,
        selection_completed_at_utc="2026-07-15T02:00:00Z",
        freeze_timestamp_utc="2026-07-15T02:00:01Z",
        evidence_bundle_identity_sha256="b" * 64,
        evidence_bundle_configuration_sha256="c" * 64,
    )


def test_portfolio_package_is_full_executable_and_reconstructible(tmp_path) -> None:
    pair, specs, package = _build()
    payload = package.to_dict()

    assert payload["schema"] == PACKAGE_SCHEMA
    assert payload["broker_connectivity"] is False
    assert payload["outbound_order_capability"] is False
    assert payload["virtual_fill_policy"]["real_order_submission"] is False
    assert payload["sizing_policy"]["combine_book"] == {
        "sleeve_ids": list(pair.combine_sleeve_ids),
        "allocation_units_by_sleeve": {
            "sleeve-nq": 2,
            "sleeve-es": 1,
        },
        "static_risk_tier": 1.15,
    }
    assert payload["sizing_policy"]["xfa_book"] == {
        "sleeve_ids": list(pair.xfa_sleeve_ids),
        "allocation_units_by_sleeve": {
            "sleeve-cl": 1,
            "sleeve-es": 2,
        },
        "static_risk_tier": 0.75,
    }
    immutable = payload["feature_contract"]["immutable_sleeves"]
    assert set(immutable) == set(specs)
    assert immutable["sleeve-nq"]["specification"]["trigger_feature"] == "past_volatility"
    assert payload["entry_policy"]["sleeves"]["sleeve-nq"]["side"] == -1
    assert payload["exit_policy"]["sleeves"]["sleeve-cl"]["holding_bars"] == 30
    assert payload["session_policy"]["sleeves"]["sleeve-es"]["session_label"] == "OPEN"

    machine, _ = write_shadow_package(package, tmp_path / "forward")
    disk_payload = json.loads(machine.read_text(encoding="utf-8"))
    reconstructed = reconstruct_portfolio_shadow_package(disk_payload)

    assert reconstructed.book_pair == pair
    assert dict(reconstructed.sleeve_specs) == specs
    assert reconstructed.package.package_hash == package.package_hash


def test_freeze_is_explicit_caller_supplied_post_selection() -> None:
    _, _, package = _build()
    assert package.freeze_timestamp_utc == "2026-07-15T02:00:01Z"
    assert package.data_policy["freeze_timestamp_utc"] == "2026-07-15T02:00:01Z"
    assert package.data_policy["selection_completed_at_utc"] == "2026-07-15T02:00:00Z"
    assert package.data_policy["freeze_basis"] == "CALLER_SUPPLIED_POST_SELECTION"
    assert (
        package.data_policy["first_eligible_bar_close_operator"]
        == "STRICTLY_GREATER_THAN_FREEZE"
    )

    pair, specs = _book_and_specs()
    with pytest.raises(PortfolioShadowPackageError, match="cannot precede"):
        build_portfolio_shadow_package(
            pair,
            specs,
            source_commit="a" * 40,
            selection_completed_at_utc="2026-07-15T02:00:01Z",
            freeze_timestamp_utc="2026-07-15T02:00:00Z",
            evidence_bundle_identity_sha256="b" * 64,
            evidence_bundle_configuration_sha256="c" * 64,
        )


def test_package_rejects_incomplete_sleeves_and_tampering() -> None:
    pair, specs = _book_and_specs()
    incomplete = dict(specs)
    incomplete.pop("sleeve-cl")
    with pytest.raises(PortfolioShadowPackageError, match="coverage drift"):
        build_portfolio_shadow_package(
            pair,
            incomplete,
            source_commit="a" * 40,
            selection_completed_at_utc="2026-07-15T02:00:00Z",
            freeze_timestamp_utc="2026-07-15T02:00:01Z",
            evidence_bundle_identity_sha256="b" * 64,
            evidence_bundle_configuration_sha256="c" * 64,
        )

    _, _, package = _build()
    tampered = package.to_dict()
    tampered["sizing_policy"]["combine_book"]["static_risk_tier"] = 1.30
    with pytest.raises(PortfolioShadowPackageError, match="book view drift"):
        reconstruct_portfolio_shadow_package(tampered)


def test_forward_guards_require_fresh_post_freeze_bars() -> None:
    _, _, package = _build()
    policy = package.data_policy
    assert policy["post_freeze_only"] is True
    assert policy["fresh_completed_bars_only"] is True
    assert policy["duplicate_bars_rejected"] is True
    assert policy["duplicate_source_sequences_rejected"] is True
    assert policy["out_of_order_bars_rejected"] is True
    assert policy["q4_access_authorized"] is False
    assert policy["new_data_purchase_authorized"] is False
    assert "bar_close_not_strictly_post_freeze" in package.kill_conditions
