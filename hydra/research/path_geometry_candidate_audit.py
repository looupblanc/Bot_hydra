from __future__ import annotations

import hashlib
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
    FOLDS,
    _build_past_only_feature_frame,
    _file_sha256,
    _future_target,
    _load_governed_development_frame,
    _load_markdown_json,
    _non_overlapping_events,
    _stable_hash,
    _strict_json_value,
    _verify_development_manifest,
    _verify_prefix_invariance,
    _verify_target_boundaries,
)
from hydra.utils.config import project_path


VERSION = "path_geometry_candidate_audit_v1"
MAP_TYPE = "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2"
MARKETS = ("NQ", "ES", "MNQ", "YM")
PRIMARY_FOLDS = ("2024_q1", "2024_q2", "2024_q3")
POINT_VALUE = {"NQ": 20.0, "ES": 50.0, "MNQ": 2.0, "YM": 5.0}
ROUND_TURN_COST = {"NQ": 9.0, "ES": 9.0, "MNQ": 4.5, "YM": 9.0}


class PathGeometryAuditError(RuntimeError):
    pass


def run_path_geometry_candidate_audit(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    repaired_map_path: str | Path,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
) -> dict[str, Any]:
    task = Path(engineering_task_path)
    roll_path = Path(repaired_map_path)
    _verify_file(task, engineering_task_sha256, "engineering task")
    _verify_file(roll_path, repaired_map_sha256, "repaired contract map")
    roll_map = load_roll_map(roll_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != repaired_roll_map_hash:
        raise PathGeometryAuditError("Repaired map semantic hash/type mismatch.")
    _verify_runtime_commit(code_commit)
    historical = _load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT))
    frozen_preregistration = project_path(
        "reports", "mission_experiments", "calibration_affected_atom_retest_v3_design_v1",
        "calibration_affected_atom_retest_v3_preregistration.json",
    )
    if not frozen_preregistration.is_file():
        frozen_preregistration = Path(
            "/root/hydra-bot/reports/mission_experiments/calibration_affected_atom_retest_v3_design_v1/"
            "calibration_affected_atom_retest_v3_preregistration.json"
        )
    _verify_file(
        frozen_preregistration,
        "d3e6ab3fe77ccb759902bb2241fef8e6203e583259eb25648d739fa751b15e26",
        "frozen development preregistration",
    )
    frozen_source = json.loads(frozen_preregistration.read_text(encoding="utf-8"))
    _verify_development_manifest(
        (frozen_source.get("source") or {}).get("development_data_manifest") or {}
    )
    prereg = {
        "schema": f"{VERSION}_preregistration",
        "atom_id": "atom_path_geometry_nq_range_relocation_20240711_v1",
        "candidate": "intraday_range_migration_path_asymmetry_09_01",
        "components": ["path_asymmetry", "range_relocation"],
        "horizon_bars": 30,
        "markets": list(MARKETS),
        "primary_folds": list(PRIMARY_FOLDS),
        "minimum_useful_effect": 0.0002,
        "costs": ROUND_TURN_COST,
        "source": {
            "task_path": str(task.resolve()),
            "task_sha256": engineering_task_sha256,
            "map_path": str(roll_path.resolve()),
            "map_sha256": repaired_map_sha256,
            "roll_map_hash": repaired_roll_map_hash,
            "development_end_exclusive": "2024-10-01",
        },
        "governance": {"q4": False, "paid_data": False, "network": False, "live": False},
        "code_commit": code_commit,
    }
    prereg["preregistration_hash"] = _stable_hash(prereg)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    prereg_path = out / "path_geometry_candidate_preregistration.json"
    _write_immutable(prereg_path, json.dumps(prereg, indent=2, sort_keys=True) + "\n")
    access = None
    if record_data_access:
        from hydra.mission.calibration_retest_execution import _record_data_access_once

        access = _record_data_access_once(
            "2023-01-01:2024-10-01", [prereg["atom_id"]], "candidate-level path geometry audit; Q4 excluded"
        )
    raw, provenance = _load_governed_development_frame(
        historical,
        [{"target_markets": list(MARKETS)}],
        contract_map_path=roll_path,
        required_contract_map_type=MAP_TYPE,
    )
    features = _build_features(raw)
    prefix = _verify_geometry_prefix_invariance(raw, features)
    features["target"] = _future_target(features, 30, defensive=False)
    target_proof = _verify_target_boundaries(features, horizon=30)
    if not prefix.get("passed") or not target_proof.get("passed"):
        raise PathGeometryAuditError("Causal feature/target integrity proof failed.")
    rows = _evaluate(features)
    gates = _gates(rows)
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": "CANDIDATE_SURVIVES_DEVELOPMENT_GATES" if all(gates.values()) else "CANDIDATE_FAILS_OR_INSUFFICIENT",
        "candidate_status": "DEVELOPMENT_SURVIVOR" if all(gates.values()) else "NOT_VALIDATED",
        "interpretation_boundary": "No strategy, Q4, final lockbox, Topstep approval, or live execution is authorized.",
        "atom_id": prereg["atom_id"],
        "preregistration_hash": prereg["preregistration_hash"],
        "preregistration_path": str(prereg_path),
        "gates": gates,
        "fold_results": rows,
        "data_provenance": {**provenance, "prefix_proof": prefix, "target_proof": target_proof},
        "data_access_record": access,
        "governance": {"q4_access_count_delta": 0, "incremental_databento_spend_usd": 0.0, "live": False},
        "validated_mechanisms": 0,
        "validated_strategies": 0,
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = out / "path_geometry_candidate_result.json"
    report_path = out / "path_geometry_candidate_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _report(payload))
    return {**payload, "artifacts": {"result_json_path": str(result_path), "report_path": str(report_path)}, "report_path": str(report_path)}


def _build_features(raw: pd.DataFrame) -> pd.DataFrame:
    base = _build_past_only_feature_frame(raw)
    pieces = []
    for _, group in base.groupby(["symbol", "active_contract"], sort=True):
        g = group.sort_values("timestamp").copy()
        key = g["contiguous_segment_id"]
        close = g["close"].astype(float)
        high = g["high"].astype(float).groupby(key, sort=False).rolling(60, min_periods=20).max().reset_index(level=0, drop=True).sort_index().groupby(key, sort=False).shift(1)
        low = g["low"].astype(float).groupby(key, sort=False).rolling(60, min_periods=20).min().reset_index(level=0, drop=True).sort_index().groupby(key, sort=False).shift(1)
        width = (high - low).replace(0, np.nan)
        location = (close - low) / width
        path = close.diff().abs().groupby(key, sort=False).rolling(30, min_periods=10).sum().reset_index(level=0, drop=True).sort_index().groupby(key, sort=False).shift(1)
        displacement = close.diff(30).groupby(key, sort=False).shift(1).abs()
        g["path_asymmetry"] = ((g["close"] - g["open"]) / (g["high"] - g["low"]).replace(0, np.nan)).clip(-3, 3)
        g["range_relocation"] = (location - 0.5).groupby(key, sort=False).rolling(30, min_periods=10).mean().reset_index(level=0, drop=True).sort_index().groupby(key, sort=False).shift(1)
        g["effort_vs_progress"] = displacement / path.replace(0, np.nan)
        pieces.append(g)
    return pd.concat(pieces, ignore_index=True).sort_values(["symbol", "timestamp"]).reset_index(drop=True).replace([np.inf, -np.inf], np.nan)


def _verify_geometry_prefix_invariance(raw: pd.DataFrame, full: pd.DataFrame) -> dict[str, Any]:
    checked = 0
    for (_symbol, _contract), raw_group in raw.groupby(["symbol", "active_contract"], sort=True):
        raw_group = raw_group.sort_values("timestamp").reset_index(drop=True)
        full_group = full[(full["symbol"] == _symbol) & (full["active_contract"] == _contract)].sort_values("timestamp")
        segments = [g.sort_values("timestamp").reset_index(drop=True) for _, g in full_group.groupby("contiguous_segment_id") if len(g) >= 240]
        for segment in segments[:4]:
            timestamps = set(pd.to_datetime(segment["timestamp"], utc=True))
            source = raw_group[pd.to_datetime(raw_group["timestamp"], utc=True).isin(timestamps)].reset_index(drop=True)
            for fraction in (0.35, 0.55, 0.75, 0.90):
                cutoff = max(200, int(len(source) * fraction))
                if cutoff >= len(source):
                    continue
                prefix = _build_features(source.iloc[:cutoff].copy())
                expected = segment.iloc[:cutoff]
                for key in ("path_asymmetry", "range_relocation"):
                    left = pd.to_numeric(prefix[key], errors="coerce").to_numpy(dtype=float)
                    right = pd.to_numeric(expected[key], errors="coerce").to_numpy(dtype=float)
                    if not np.array_equal(np.isnan(left), np.isnan(right)):
                        return {"passed": False, "reason": f"nan_mask_mismatch:{key}", "comparisons": checked}
                    finite = np.isfinite(left) & np.isfinite(right)
                    if not np.array_equal(np.isfinite(left), np.isfinite(right)) or (finite.any() and not np.array_equal(left[finite], right[finite])):
                        return {"passed": False, "reason": f"value_mismatch:{key}", "comparisons": checked}
                    checked += int(finite.sum())
    return {"passed": checked > 0, "comparisons": checked, "method": "geometry_prefix_recomputation"}


def _evaluate(features: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for fold, train_start, train_end, test_start, test_end in FOLDS:
        test = features[(features["trading_session_id"] >= test_start) & (features["trading_session_id"] < test_end)].copy()
        test = test.dropna(subset=["path_asymmetry", "range_relocation", "target"])
        score = np.nanmean(np.vstack([np.sign(test["path_asymmetry"]), np.sign(test["range_relocation"])]), axis=0)
        test["signal"] = np.where(score > 0.15, 1, np.where(score < -0.15, -1, 0))
        # The frozen screen enters only on a flat-to-active transition; taking
        # every active bar would manufacture turnover and is not the candidate.
        prior_signal = test.groupby(
            ["symbol", "active_contract", "trading_session_id", "contiguous_segment_id"],
            sort=False,
        )["signal"].shift(1).fillna(0)
        entry_frame = test[(test["signal"] != 0) & (prior_signal == 0)].copy()
        events = _non_overlapping_events(entry_frame, 30)
        trades = []
        for _, row in events.iterrows():
            target = float(row["target"])
            gross = target * float(row["close"]) * POINT_VALUE.get(str(row["symbol"]), 0.0) / max(float(row["close"]), 1e-9) * float(row["signal"])
            trades.append(gross - ROUND_TURN_COST.get(str(row["symbol"]), 9.0))
        by_market = {}
        for symbol, group in test.groupby("symbol", sort=True):
            active = group[group["signal"] != 0]
            by_market[str(symbol)] = {"events": int(len(active)), "mean_target": float(active["target"].mean()) if len(active) else None, "days": int(active["trading_session_id"].nunique())}
        result[fold] = {"events": int(len(events)), "net_pnl_proxy": float(sum(trades)), "positive": bool(sum(trades) > 0), "markets": by_market}
    return result


def _gates(rows: dict[str, Any]) -> dict[str, bool]:
    primary = [rows.get(fold, {}) for fold in PRIMARY_FOLDS]
    return {"temporal_transfer": len(primary) == 3 and all(row.get("positive") for row in primary), "minimum_events": all(row.get("events", 0) >= 30 for row in primary), "cross_market_presence": all(any(v.get("events", 0) >= 10 for v in row.get("markets", {}).values()) for row in primary), "cost_proxy": all(math.isfinite(float(row.get("net_pnl_proxy", 0.0))) for row in primary)}


def _verify_file(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or _file_sha256(path) != expected:
        raise PathGeometryAuditError(f"Frozen {label} missing or changed: {path}")


def _verify_runtime_commit(commit: str) -> None:
    if not len(commit) == 40:
        return
    if subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip() != commit:
        raise PathGeometryAuditError("Runtime commit differs from frozen commit.")


def _write_immutable(path: Path, content: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") == content:
            return
        raise PathGeometryAuditError(f"Refusing divergent immutable artifact: {path}")
    path.write_text(content, encoding="utf-8")


def _report(payload: dict[str, Any]) -> str:
    return "# HYDRA Path Geometry Candidate Audit\n\n" + f"- Conclusion: `{payload['scientific_conclusion']}`\n" + f"- Candidate status: `{payload['candidate_status']}`\n" + f"- Result hash: `{payload['result_hash']}`\n" + f"- Gates: `{payload['gates']}`\n\n" + payload["interpretation_boundary"] + "\n"
