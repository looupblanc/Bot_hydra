from __future__ import annotations

import json
from pathlib import Path

from hydra.data.budget import sha256_file
from hydra.economic_evolution.schema import stable_hash
from scripts.acquire_cftc_grain_positioning_crowding import _query, _read_manifest, acquire


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config/research/cftc_grain_positioning_crowding_tripwire_v1.json"


def test_manifest_is_self_hashed_and_pre_q4() -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    claimed = payload.pop("manifest_hash")
    assert stable_hash(payload) == claimed
    assert payload["candidate_lattice"]["proposal_count"] == 48
    assert payload["cftc_data_contract"]["q4_2024_access"] is False
    assert payload["frozen_price_input"]["q4_2024_access"] is False
    assert payload["governance"]["new_databento_spend_usd"] == 0.0


def test_query_is_futures_only_and_exactly_three_grain_markets() -> None:
    manifest = _read_manifest(ROOT)
    _url, params = _query(manifest)
    where = params["$where"]
    assert "futonly_or_combined='FutOnly'" in where
    assert all(code in where for code in ("001602", "002602", "005602"))
    assert "2024-09-24T23:59:59.999" in where
    assert params["$limit"] == "5000"


def test_frozen_price_inputs_reconcile() -> None:
    manifest = _read_manifest(ROOT)
    price = manifest["frozen_price_input"]
    assert sha256_file(ROOT / price["ohlcv_path"]) == price["ohlcv_sha256"]
    assert sha256_file(ROOT / price["definition_path"]) == price["definition_sha256"]


def test_publication_delay_is_conservative_and_shutdown_is_excluded() -> None:
    manifest = _read_manifest(ROOT)
    causal = manifest["publication_causality"]
    assert causal["conservative_actionable_time"].startswith("REPORT_DATE_PLUS_8")
    assert causal["available_at_must_be_lte_decision_time"] is True
    assert causal["future_label_eligibility"] is False
    assert causal["excluded_disrupted_report_dates"] == {
        "start": "2018-12-18",
        "end_inclusive": "2019-03-05",
        "reason": (
            "2018-2019 federal shutdown publication backlog; exact historical "
            "release timestamps are unavailable."
        ),
    }


def test_dry_run_has_zero_spend_and_no_execution_side_effects() -> None:
    result = acquire(ROOT, execute=False)
    assert result["download_status"] == "DRY_RUN_ONLY"
    assert result["estimated_cost_usd"] == 0.0
    assert result["q4_access_count_delta"] == 0
    assert result["broker_connections"] == 0
    assert result["orders"] == 0
