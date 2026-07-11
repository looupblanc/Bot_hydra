from __future__ import annotations

import json
import math
import subprocess
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.mission.calibration_retest_execution import (
    DEFAULT_HISTORICAL_REPORT,
    _build_past_only_feature_frame,
    _file_sha256,
    _future_target,
    _load_governed_development_frame,
    _load_markdown_json,
    _stable_hash,
    _strict_json_value,
    _verify_development_manifest,
)

MAP_TYPE = "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2"
MARKETS = ("GC", "MGC", "CL", "MCL")
FOLDS = {"2023_h2": ("2023-07-01", "2024-01-01"), "2024_q1": ("2024-01-01", "2024-04-01"), "2024_q2": ("2024-04-01", "2024-07-01"), "2024_q3": ("2024-07-01", "2024-10-01")}
COSTS = {"GC": 12.0, "MGC": 4.5, "CL": 12.0, "MCL": 4.5}
POINTS = {"GC": 100.0, "MGC": 10.0, "CL": 1000.0, "MCL": 100.0}


class MetalEnergyPilotError(RuntimeError):
    pass


def run_metal_energy_session_transition_pilot(output_dir: str | Path, *, engineering_task_path: str | Path, engineering_task_sha256: str, repaired_map_path: str | Path, repaired_map_sha256: str, repaired_roll_map_hash: str, code_commit: str, record_data_access: bool = True) -> dict[str, Any]:
    task, roll_path = Path(engineering_task_path), Path(repaired_map_path)
    _verify(task, engineering_task_sha256, "task")
    _verify(roll_path, repaired_map_sha256, "map")
    roll = load_roll_map(roll_path)
    if roll.map_type != MAP_TYPE or roll.roll_map_hash() != repaired_roll_map_hash:
        raise MetalEnergyPilotError("Map semantic hash/type mismatch")
    if len(code_commit) == 40 and subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip() != code_commit:
        raise MetalEnergyPilotError("Frozen code commit mismatch")
    historical = _load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT))
    source_prereg = Path("/root/hydra-bot/reports/mission_experiments/calibration_affected_atom_retest_v3_design_v1/calibration_affected_atom_retest_v3_preregistration.json")
    _verify(source_prereg, "d3e6ab3fe77ccb759902bb2241fef8e6203e583259eb25648d739fa751b15e26", "development preregistration")
    _verify_development_manifest((json.loads(source_prereg.read_text()).get("source") or {}).get("development_data_manifest") or {})
    prereg = {"schema": "metal_energy_session_transition_preregistration_v1", "atom_id": "atom_metal_energy_session_transition_20260711_v1", "markets": list(MARKETS), "horizon": 30, "costs": COSTS, "q4": False, "task_sha256": engineering_task_sha256, "map_sha256": repaired_map_sha256, "roll_map_hash": repaired_roll_map_hash, "code_commit": code_commit}
    prereg["preregistration_hash"] = _stable_hash(prereg)
    out = Path(output_dir); out.mkdir(parents=True, exist_ok=True)
    pre_path = out / "metal_energy_session_transition_preregistration.json"; _write(pre_path, json.dumps(prereg, indent=2, sort_keys=True) + "\n")
    access = None
    if record_data_access:
        from hydra.mission.calibration_retest_execution import _record_data_access_once
        access = _record_data_access_once("2023-01-01:2024-10-01", [prereg["atom_id"]], "metal energy session transition development pilot; Q4 excluded")
    raw, provenance = _load_governed_development_frame(historical, [{"target_markets": list(MARKETS)}], contract_map_path=roll_path, required_contract_map_type=MAP_TYPE)
    frame = _build_past_only_feature_frame(raw)
    frame["target"] = _future_target(frame, 30, defensive=False)
    rows = _evaluate(frame)
    gates = {"temporal_transfer": all(rows[f]["net"] > 0 for f in ("2024_q1", "2024_q2", "2024_q3")), "market_transfer": all(rows[f]["markets"].get(m, {}).get("events", 0) >= 30 for f in ("2024_q1", "2024_q2", "2024_q3") for m in MARKETS), "finite": all(math.isfinite(rows[f]["net"]) for f in rows)}
    payload = {"schema": "metal_energy_session_transition_pilot_v1", "scientific_conclusion": "METAL_ENERGY_SESSION_TRANSITION_SURVIVES_DEVELOPMENT" if all(gates.values()) else "METAL_ENERGY_SESSION_TRANSITION_FAILS_OR_INSUFFICIENT", "mechanism_status": "DEVELOPMENT_SURVIVOR" if all(gates.values()) else "NOT_VALIDATED", "gates": gates, "fold_results": rows, "preregistration_hash": prereg["preregistration_hash"], "preregistration_path": str(pre_path), "data_provenance": provenance, "data_access_record": access, "validated_mechanisms": 0, "validated_strategies": 0, "governance": {"q4": False, "paid": False, "live": False}}
    payload = _strict_json_value(payload); payload["result_hash"] = _stable_hash(payload)
    result_path = out / "metal_energy_session_transition_result.json"; report_path = out / "metal_energy_session_transition_report.md"
    _write(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n"); _write(report_path, f"# Metal/Energy Session Transition\n\n- Conclusion: `{payload['scientific_conclusion']}`\n- Gates: `{payload['gates']}`\n")
    return {**payload, "artifacts": {"result_json_path": str(result_path), "report_path": str(report_path)}, "report_path": str(report_path)}


def _evaluate(frame: pd.DataFrame) -> dict[str, Any]:
    out = {}
    for fold, (start, end) in FOLDS.items():
        x = frame[(frame["trading_session_id"] >= start) & (frame["trading_session_id"] < end)].copy()
        x = x.dropna(subset=["target", "close"])
        first = x.groupby(["symbol", "active_contract", "trading_session_id"], sort=False).head(1).copy()
        prior = x.sort_values("timestamp").groupby(["symbol", "active_contract"], sort=False)["close"].shift(1)
        x["prior_close"] = prior
        first["prior_close"] = x.loc[first.index, "prior_close"]
        first["displacement"] = first["open"].astype(float) / first["prior_close"].astype(float) - 1.0
        first = first.dropna(subset=["displacement"])
        first["signal"] = -np.sign(first["displacement"])
        first["net"] = first["target"].astype(float) * first["signal"] * first["close"].astype(float) * first["symbol"].map(POINTS) - first["symbol"].map(COSTS)
        out[fold] = {"net": float(first["net"].sum()), "events": int(len(first)), "markets": {str(m): {"events": int(len(g)), "net": float(g["net"].sum())} for m, g in first.groupby("symbol")}}
    return out


def _verify(path: Path, sha: str, label: str) -> None:
    if not path.is_file() or _file_sha256(path) != sha: raise MetalEnergyPilotError(f"Frozen {label} missing or changed")


def _write(path: Path, content: str) -> None:
    if path.exists() and path.read_text() != content: raise MetalEnergyPilotError(f"Immutable artifact conflict: {path}")
    if not path.exists(): path.write_text(content)
