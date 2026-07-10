from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hydra.utils.config import project_path


@dataclass(frozen=True)
class FreezeManifest:
    manifest_type: str
    created_at_utc: str
    candidate_ids: list[str]
    strategy_specs: list[dict[str, Any]]
    execution_assumptions: dict[str, Any]
    commission_assumptions: dict[str, Any]
    slippage_assumptions: dict[str, Any]
    topstep_rule_version: str
    source_code_commit: str
    data_fingerprints: dict[str, str]
    validation_thresholds: dict[str, Any]
    expected_decision_policy: dict[str, Any]

    def canonical_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"), default=str)

    def manifest_hash(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


def build_manifest(
    manifest_type: str,
    candidate_rows: list[dict[str, Any]],
    source_code_commit: str,
    data_fingerprints: dict[str, str],
    topstep_rule_version: str,
    validation_thresholds: dict[str, Any],
    expected_decision_policy: dict[str, Any],
) -> FreezeManifest:
    specs = []
    candidate_ids = []
    for row in candidate_rows:
        candidate_ids.append(str(row["candidate_id"]))
        specs.append(
            {
                "candidate_id": row["candidate_id"],
                "family": row.get("family"),
                "symbol": row.get("symbol"),
                "timeframe": row.get("timeframe"),
                "parameters_json": row.get("parameters_json"),
                "risk_json": row.get("risk_json"),
                "strategy_fingerprint": row.get("strategy_fingerprint"),
                "entry_logic": f"{row.get('family')}_regime_path_entry",
                "exit_policy": _safe_json(row.get("risk_json", "{}")).get("exit_policy", "unknown"),
                "sizing_policy": _safe_json(row.get("risk_json", "{}")).get("sizing_mode", "unknown"),
            }
        )
    return FreezeManifest(
        manifest_type=manifest_type,
        created_at_utc=datetime.now(timezone.utc).isoformat(),
        candidate_ids=sorted(candidate_ids),
        strategy_specs=sorted(specs, key=lambda item: item["candidate_id"]),
        execution_assumptions={
            "fill_model": "ohlcv_1m_conservative_intrabar_until_tick_validation",
            "positions_flattened_by": "15:10 America/Chicago",
            "live_trading": False,
        },
        commission_assumptions={"round_turn_cost_source": "hydra.backtest.costs.round_turn_cost"},
        slippage_assumptions={"default_bps": 0.5, "forced_liquidation_slippage_bps": 1.0},
        topstep_rule_version=topstep_rule_version,
        source_code_commit=source_code_commit,
        data_fingerprints=data_fingerprints,
        validation_thresholds=validation_thresholds,
        expected_decision_policy=expected_decision_policy,
    )


def write_manifest(manifest: FreezeManifest, folder: str = "reports/lockbox") -> tuple[Path, str]:
    target_dir = project_path(folder)
    target_dir.mkdir(parents=True, exist_ok=True)
    digest = manifest.manifest_hash()
    path = target_dir / f"{manifest.manifest_type}_{digest[:16]}.json"
    payload = asdict(manifest)
    payload["manifest_hash"] = digest
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path, digest


def _safe_json(value: str) -> dict[str, Any]:
    try:
        return json.loads(value or "{}")
    except json.JSONDecodeError:
        return {}

