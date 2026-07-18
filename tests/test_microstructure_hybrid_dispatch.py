from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import manifest as production_manifest
from hydra.production import runtime as production_runtime
from hydra.production.microstructure_hybrid_manifest import HybridManifestError


CAMPAIGN_MODE = "HYBRID_STRUCTURAL_ALPHA_ORDER_FLOW"


def _manifest_file(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    path = tmp_path / "config/v7/microstructure_hybrid_0033.json"
    payload: dict[str, Any] = {
        "schema": production_manifest.PRODUCTION_MANIFEST_SCHEMA,
        "campaign_mode": CAMPAIGN_MODE,
        "campaign_id": "hydra_hybrid_structural_alpha_order_flow_0033",
    }
    payload["manifest_hash"] = stable_hash(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path, payload


def test_production_manifest_dispatches_0033_to_specialized_validator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, expected = _manifest_file(tmp_path)
    observed: dict[str, Any] = {}

    def validate(value: dict[str, Any], *, manifest_path: Path) -> None:
        observed["manifest"] = value
        observed["path"] = manifest_path

    monkeypatch.setattr(
        "hydra.production.microstructure_hybrid_manifest."
        "validate_microstructure_hybrid_manifest",
        validate,
    )

    actual = production_manifest.load_and_validate_production_manifest(path)

    assert actual == expected
    assert observed == {"manifest": expected, "path": path.resolve()}


def test_production_manifest_wraps_0033_contract_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, _ = _manifest_file(tmp_path)

    def reject(value: dict[str, Any], *, manifest_path: Path) -> None:
        del value, manifest_path
        raise HybridManifestError("0033 frozen action drift")

    monkeypatch.setattr(
        "hydra.production.microstructure_hybrid_manifest."
        "validate_microstructure_hybrid_manifest",
        reject,
    )

    with pytest.raises(
        production_manifest.ProductionManifestError,
        match="0033 frozen action drift",
    ):
        production_manifest.load_and_validate_production_manifest(path)


def _install_fake_hybrid_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[types.ModuleType, dict[str, Any]]:
    calls: dict[str, Any] = {}
    module = types.ModuleType("hydra.production.microstructure_hybrid_runtime")

    def read_status(manifest_path: str | Path) -> dict[str, Any]:
        calls["status"] = manifest_path
        return {"state": "HYBRID_STATUS"}

    def run_manifest(
        manifest_path: str | Path,
        *,
        contract_map_path: str | Path,
        cache_root: str | Path,
        stop_after: str | None,
    ) -> dict[str, Any]:
        calls["run"] = {
            "manifest_path": manifest_path,
            "contract_map_path": contract_map_path,
            "cache_root": cache_root,
            "stop_after": stop_after,
        }
        return {"state": "HYBRID_RUN"}

    module.read_microstructure_hybrid_status = read_status  # type: ignore[attr-defined]
    module.run_microstructure_hybrid_manifest = run_manifest  # type: ignore[attr-defined]
    monkeypatch.setitem(
        sys.modules, "hydra.production.microstructure_hybrid_runtime", module
    )
    return module, calls


def test_read_live_status_dispatches_0033_to_specialized_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, calls = _install_fake_hybrid_runtime(monkeypatch)
    monkeypatch.setattr(
        production_runtime,
        "load_and_validate_production_manifest",
        lambda path: {"campaign_mode": CAMPAIGN_MODE},
    )

    result = production_runtime.read_live_status("manifest.json")

    assert result == {"state": "HYBRID_STATUS"}
    assert calls["status"] == "manifest.json"


def test_run_production_manifest_dispatches_all_0033_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, calls = _install_fake_hybrid_runtime(monkeypatch)
    monkeypatch.setattr(
        production_runtime,
        "load_and_validate_production_manifest",
        lambda path: {"campaign_mode": CAMPAIGN_MODE},
    )

    result = production_runtime.run_production_manifest(
        "manifest.json",
        contract_map_path="contract-map.json",
        cache_root="feature-cache",
        stop_after="FIRST_HALVING",
    )

    assert result == {"state": "HYBRID_RUN"}
    assert calls["run"] == {
        "manifest_path": "manifest.json",
        "contract_map_path": "contract-map.json",
        "cache_root": "feature-cache",
        "stop_after": "FIRST_HALVING",
    }
