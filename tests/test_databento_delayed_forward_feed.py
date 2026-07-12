from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from hydra.data.budget import DatabentoBudgetConfig
from hydra.data.contract_mapping import ContractInfo, RollMap
from hydra.data.current_contract_map import build_current_roll_map
from hydra.shadow.databento_forward_feed import run_databento_forward_update
from hydra.shadow.activation import audit_zero_order_surface
from hydra.shadow.forward_feed_manifest import (
    ForwardFeedManifestError,
    build_forward_boundary_manifest,
    build_read_only_source_manifest,
    validate_forward_boundary_manifest,
    validate_read_only_source_manifest,
    write_manifest,
)


UTC = timezone.utc


def _current_map(path: Path) -> None:
    contract = ContractInfo(
        root="MYM",
        contract="MYMU6",
        month_code="U",
        year=2026,
        expiry_date="2026-09-18",
        last_trade_date="2026-09-18",
        active_start="2026-07-10T00:00:00+00:00",
        active_end="2026-07-12T00:00:00+00:00",
        roll_date="2026-07-12T00:00:00+00:00",
        tick_size=1.0,
        tick_value=0.5,
        point_value=0.5,
        contract_multiplier=0.5,
        is_micro=True,
        instrument_id="42004247",
        parent_symbol="MYM",
        continuous_symbol="MYM.c.0",
        activation_time="2025-09-19T00:00:00+00:00",
        deactivation_time="2026-07-12T00:00:00+00:00",
        roll_reason="explicit_test",
        transition_uncertainty="none",
    )
    roll_map = RollMap(
        dataset="GLBX.MDP3",
        schema="ohlcv-1m",
        map_type="EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2",
        symbols=["MYM"],
        contracts=[contract],
        unsafe_window_days=0,
        notes=["test"],
    )
    path.write_text(json.dumps(roll_map.to_dict()) + "\n", encoding="utf-8")


class _Metadata:
    def __init__(self) -> None:
        self.record_requests: list[dict[str, object]] = []

    def get_dataset_range(self, *, dataset: str) -> dict[str, object]:
        assert dataset == "GLBX.MDP3"
        return {
            "start": "2010-01-01T00:00:00Z",
            "end": "2026-07-11T23:35:00Z",
            "schema": {
                "ohlcv-1m": {
                    "start": "2010-01-01T00:00:00Z",
                    "end": "2026-07-11T23:35:00Z",
                }
            },
        }

    def get_record_count(self, **kwargs: object) -> int:
        self.record_requests.append(dict(kwargs))
        return 0


def _boundary(tmp_path: Path) -> tuple[Path, str]:
    config = tmp_path / "config.json"
    config.write_text("{}\n", encoding="utf-8")
    payload = build_forward_boundary_manifest(
        [
            {
                "candidate_id": "strategy_forward_test_v1",
                "configuration_path": str(config),
                "configuration_sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
                "configuration_hash": "a" * 64,
                "freeze_timestamp_utc": "2026-07-11T10:42:10+00:00",
                "required_roots": ["MYM"],
                "stale_data_seconds": 75,
            }
        ],
        created_at=datetime(2026, 7, 12, 7, 0, tzinfo=UTC),
    )
    path = write_manifest(tmp_path / "boundary.json", payload)
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def test_source_manifest_proves_read_only_surface_without_credentials() -> None:
    now = datetime(2026, 7, 12, 7, 0, tzinfo=UTC)
    manifest = build_read_only_source_manifest(
        dataset="GLBX.MDP3",
        checked_at=now,
        valid_through=date(2026, 7, 13),
        dataset_range={"end": "2026-07-11T23:35:00Z"},
    )
    validate_read_only_source_manifest(manifest, now=now)
    assert manifest["broker_connections"] == 0
    assert manifest["outbound_orders"] == 0
    assert manifest["credential_material_persisted"] is False
    with pytest.raises(ForwardFeedManifestError):
        validate_read_only_source_manifest(
            {**manifest, "outbound_orders": 1}, now=now
        )


def test_boundary_manifest_rejects_pre_freeze_policy_drift(tmp_path: Path) -> None:
    path, _digest = _boundary(tmp_path)
    payload = json.loads(path.read_text())
    payload["pre_freeze_backfill_prohibited"] = False
    payload["manifest_hash"] = "0" * 64
    with pytest.raises(ForwardFeedManifestError):
        validate_forward_boundary_manifest(payload)


def test_weekend_update_waits_without_buying_or_backfilling(tmp_path: Path) -> None:
    boundary, boundary_sha = _boundary(tmp_path)
    maps = tmp_path / "maps"
    maps.mkdir()
    _current_map(maps / "roll_map_current.json")
    metadata = _Metadata()
    fake = SimpleNamespace(metadata=metadata)
    budget = DatabentoBudgetConfig(
        hard_cap_usd=100.0,
        safety_ceiling_usd=98.0,
        ledger_path=str(tmp_path / "budget.jsonl"),
        summary_path=str(tmp_path / "budget.md"),
    )

    result = run_databento_forward_update(
        tmp_path / "output",
        boundary_manifest_path=boundary,
        boundary_manifest_sha256=boundary_sha,
        state_dir=tmp_path / "shadow_state",
        contract_map_dir=maps,
        budget=budget,
        code_commit="b" * 40,
        now=datetime(2026, 7, 12, 7, 0, tzinfo=UTC),
        client=fake,
    )

    assert result["scientific_conclusion"] == "WAITING_FOR_FIRST_POST_FREEZE_FORWARD_BAR"
    assert result["fresh_forward_bars_processed"] == 0
    assert result["candidate_heartbeats_published"] == 0
    assert result["incremental_databento_spend_usd"] == 0.0
    assert result["outbound_orders"] == 0
    assert result["broker_connections"] == 0
    assert not (tmp_path / "budget.jsonl").exists()
    assert metadata.record_requests
    assert pd.Timestamp(metadata.record_requests[0]["start"]) >= pd.Timestamp(
        "2026-07-11T10:42:10Z"
    )
    assert pd.Timestamp(result["next_check_at_utc"]) == pd.Timestamp(
        "2026-07-12T22:02:00Z"
    )


def test_current_roll_map_uses_aware_bounds_and_definition_history() -> None:
    history = pd.DataFrame(
        [
            {
                "ts_event": "2026-07-10T00:00:00Z",
                "instrument_id": 42004247,
                "raw_symbol": "MYMU6",
                "instrument_class": "F",
                "security_type": "FUT",
                "asset": "MYM",
                "min_price_increment": 1.0,
                "expiration": "2026-09-18T00:00:00Z",
                "activation": "2025-09-19T00:00:00Z",
            }
        ]
    )
    roll_map = build_current_roll_map(
        roots=["MYM"],
        start="2026-07-10T00:00:00Z",
        end="2026-07-11T23:30:00Z",
        continuous_mapping={
            "MYM.c.0": [
                {"d0": "2026-07-10", "d1": "2026-07-12", "s": "42004247"}
            ]
        },
        raw_symbol_mapping={"42004247": "MYMU6"},
        definition_history=history,
    )
    assert roll_map.contracts[0].contract == "MYMU6"
    assert roll_map.contracts[0].instrument_id == "42004247"


def test_forward_feed_code_surface_has_no_order_or_broker_adapter() -> None:
    audit = audit_zero_order_surface(
        [
            "hydra/shadow/databento_forward_feed.py",
            "hydra/shadow/forward_feed_manifest.py",
            "scripts/run_shadow_forward_update.py",
        ]
    )
    assert audit["passed"] is True
    assert audit["outbound_order_capability"] is False
