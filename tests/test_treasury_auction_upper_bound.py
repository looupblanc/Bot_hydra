from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/run_treasury_auction_upper_bound.py"
MANIFEST = ROOT / "config/research/treasury_auction_demand_shock_tripwire_v1.json"
RESULT = ROOT / "reports/research_tripwires/treasury_auction_demand_shock_v1/upper_bound_result.json"


def _module():
    spec = importlib.util.spec_from_file_location("treasury_auction_upper_bound", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_manifest_and_result_hashes_are_canonical() -> None:
    module = _module()
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    claimed_manifest = manifest.pop("manifest_hash")
    assert hashlib.sha256(module.canonical(manifest)).hexdigest() == claimed_manifest

    result = json.loads(RESULT.read_text(encoding="utf-8"))
    claimed_result = result.pop("result_hash")
    assert hashlib.sha256(module.canonical(result)).hexdigest() == claimed_result


def test_availability_is_timezone_safe_and_role_is_frozen() -> None:
    module = _module()
    available = module.parse_available({"updatedTimestamp": "2024-06-12T13:03:04"})
    assert available == pd.Timestamp("2024-06-12T17:03:04Z")
    roles = {
        "DISCOVERY": ["2023-01-01", "2024-01-01"],
        "VALIDATION": ["2024-01-01", "2024-05-01"],
        "FINAL_DEVELOPMENT": ["2024-05-01", "2024-10-01"],
    }
    assert module.role_for("2023-12-31", roles) == "DISCOVERY"
    assert module.role_for("2024-01-01", roles) == "VALIDATION"
    assert module.role_for("2024-05-01", roles) == "FINAL_DEVELOPMENT"
    assert module.role_for("2024-10-01", roles) is None


def test_sealed_upper_bound_is_explicitly_non_deployable() -> None:
    result = json.loads(RESULT.read_text(encoding="utf-8"))
    assert result["status"] == "TREASURY_AUCTION_UPPER_BOUND_SUPPORTS_CAUSAL_TEST"
    assert result["non_deployable"] is True
    assert result["upper_bound_gate_pass"] is True
    assert result["event_count"] == 81
    assert result["path_count"] == 242
    assert result["event_count_by_role"] == {
        "DISCOVERY": 45,
        "VALIDATION": 16,
        "FINAL_DEVELOPMENT": 20,
    }
    assert result["account_windows"]["60"]["pass_count"] == 1
    assert result["broker_connections"] == 0
    assert result["orders"] == 0
    assert result["q4_access_count"] == 0
