"""Pooled scheduled-information-shock meta-policy.

The bounded experiment reuses three independently acquired causal ecologies:
EIA natural-gas storage releases, USDA WASDE releases, and Treasury auctions.
It first materializes paired continuation/fade outcomes into one immutable
event schema.  CFTC is deliberately fail-closed because its prior result did
not persist a compatible event ledger.
"""

from __future__ import annotations

import gc
import hashlib
import json
import math
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.economic_evolution.schema import stable_hash
from hydra.research import natural_gas_storage_shock_tripwire as ng
from hydra.research import usda_grain_information_shock_tripwire as usda


ROOT = Path(__file__).resolve().parents[2]
OUTPUT = Path("reports/research_tripwires/scheduled_information_shock_meta_policy_v1")
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")


class MetaPolicyError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PolicySpec:
    ridge: float
    minimum_predicted_edge_to_cost: float
    high_risk_threshold: float

    @property
    def policy_id(self) -> str:
        return "shockmeta_" + stable_hash(asdict(self))[:20]


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(dict(payload), indent=2, sort_keys=True, default=str) + "\n")
    os.replace(temporary, path)


def _write_once(path: Path, payload: Mapping[str, Any]) -> None:
    text = json.dumps(dict(payload), indent=2, sort_keys=True, default=str) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise MetaPolicyError(f"immutable artifact drift: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def frozen_specs() -> tuple[PolicySpec, ...]:
    specs = tuple(
        PolicySpec(ridge, edge, high)
        for ridge in (1.0, 4.0, 16.0)
        for edge in (0.0, 0.25, 0.50, 1.0)
        for high in (0.75, 1.25)
    )
    if len(specs) != 24 or len({row.policy_id for row in specs}) != 24:
        raise MetaPolicyError("24-policy lattice drift")
    return specs


def freeze(root: Path, output: Path) -> dict[str, Any]:
    source_paths = {
        "ng_result": root / "reports/research_tripwires/natural_gas_storage_shock_v1/economic_result.json",
        "usda_result": root / "reports/research_tripwires/usda_grain_information_shock_v1/economic_result.json",
        "treasury_ledger": root / "reports/research_tripwires/treasury_auction_demand_shock_causal_v1/causal_event_ledger.jsonl",
        "cftc_result": root / "reports/research_tripwires/cftc_grain_positioning_crowding_v1/economic_result.json",
    }
    if any(not path.is_file() for path in source_paths.values()):
        raise MetaPolicyError("scheduled-shock source evidence missing")
    decision_core = {
        "schema": "hydra_scheduled_information_shock_meta_policy_decision_v1",
        "status": "FROZEN_BEFORE_POOLED_OUTCOMES",
        "hypothesis": "A regularized pooled TRADE_CONTINUATION/TRADE_FADE/ABSTAIN router can share information across scheduled shocks after causal within-ecology normalization.",
        "smallest_decisive_test": "24 frozen ridge/edge/risk specs; leave-one-ecology-out discovery; chronological validation and final-development.",
        "strongest_argument_against": "Release responses are economically heterogeneous and source-specific costs may overwhelm any common state.",
        "expected_data_cost_usd": 0.0,
        "next_distinct_alternative": "CROSS_ASSET_VOLATILITY_RISK_PREMIUM_CARRY_SHOCK",
    }
    decision = {**decision_core, "decision_hash": stable_hash(decision_core)}
    _write_once(output / "decision_card.json", decision)
    core = {
        "schema": "hydra_scheduled_information_shock_meta_policy_manifest_v1",
        "status": "FROZEN_BEFORE_POOLED_OUTCOMES",
        "decision_hash": decision["decision_hash"],
        "inputs": {key: {"path": str(path.relative_to(root)), "sha256": _sha(path)} for key, path in source_paths.items()},
        "included_ecologies": ["EIA_NG_STORAGE", "USDA_WASDE", "TREASURY_AUCTION"],
        "excluded_ecologies": {"CFTC_POSITIONING": "NO_COMPATIBLE_PER_EVENT_LEDGER_FAIL_CLOSED"},
        "roles": list(ROLES),
        "feature_contract": ["surprise", "liquidity", "volatility", "response"],
        "actions": ["CONTINUATION", "FADE", "ABSTAIN"],
        "policy_specs": [asdict(row) for row in frozen_specs()],
        "policy_count": 24,
        "cheap_gate": {"minimum_total_events": 100, "positive_stressed_each_held_role": True, "maximum_positive_ecology_share": 0.50},
        "account_replay_gate": "HELD_GATE_ONLY",
        "new_data_purchase": False,
        "q4_access": 0,
        "broker_connections": 0,
        "orders": 0,
        "worker_processes": 1,
        "numeric_threads": 1,
    }
    manifest = {**core, "manifest_hash": stable_hash(core)}
    _write_once(output / "manifest.json", manifest)
    return manifest


def _paired(primary: Sequence[Mapping[str, Any]], fade: Sequence[Mapping[str, Any]], *, ecology: str, market_key: str) -> list[dict[str, Any]]:
    fade_by_key = {(str(row["release_timestamp"]), str(row["execution_symbol"])): row for row in fade}
    output: list[dict[str, Any]] = []
    for row in primary:
        key = (str(row["release_timestamp"]), str(row["execution_symbol"]))
        other = fade_by_key.get(key)
        if other is None:
            continue
        cost = float(row["stressed_cost_usd"])
        output.append({
            "event_id": stable_hash({"ecology": ecology, "key": key})[:24],
            "ecology": ecology,
            "market": str(row[market_key]),
            "timestamp": str(row["decision_time"]),
            "role": str(row["role"]),
            "surprise_raw": float(row["response_score"]),
            "liquidity_raw": float(row["prior_range"]) / max(cost, 1e-9),
            "volatility_raw": float(row["prior_range"]),
            "response_raw": float(row["response_score"]),
            "cost_usd": cost,
            "continuation_normal": float(row["normal_net_usd"]),
            "continuation_stressed": float(row["stressed_net_usd"]),
            "continuation_worst": float(row["minimum_open_pnl_stressed_usd"]),
            "fade_normal": float(other["normal_net_usd"]),
            "fade_stressed": float(other["stressed_net_usd"]),
            "fade_worst": float(other["minimum_open_pnl_stressed_usd"]),
        })
    return output


def _load_ng(root: Path) -> list[dict[str, Any]]:
    audit = ng.audit_inputs(root)
    bars, _ = ng._load_bars(audit)
    manifest = audit["manifest"]
    releases = ng._release_timestamps(bars["NG"], manifest)
    guard = ng._roll_guard_days(bars["NG"])
    cell = ng.Cell("RELEASE_RESPONSE_CONTINUATION", 3, 0.5, 60, 1.0, 3.0, "NG")
    primary, _ = ng._evaluate_cell(bars, releases, cell, manifest, roll_guard=guard)
    fade, _ = ng._evaluate_cell(bars, releases, cell, manifest, roll_guard=guard, direction_flip=True)
    output = _paired(primary, fade, ecology="EIA_NG_STORAGE", market_key="execution_symbol")
    del bars
    gc.collect()
    return output


def _load_usda(root: Path) -> list[dict[str, Any]]:
    audit = usda.audit_inputs(root)
    bars, _ = usda._load_bars(audit)
    manifest = audit["manifest"]
    releases = usda._release_timestamps(manifest)
    output: list[dict[str, Any]] = []
    for market in usda.SYMBOLS:
        guard = usda._roll_guard_days(bars[market])
        cell = usda.Cell("RESPONSE_CONTINUATION", "OWN_MARKET", 3, 0.5, 60, 1.0, 1.5, market)
        primary, _ = usda._evaluate_cell(bars, releases, cell, manifest, roll_guard=guard)
        fade, _ = usda._evaluate_cell(bars, releases, cell, manifest, roll_guard=guard, direction_flip=True)
        output.extend(_paired(primary, fade, ecology="USDA_WASDE", market_key="execution_symbol"))
    del bars
    gc.collect()
    return output


def _load_treasury(root: Path) -> list[dict[str, Any]]:
    path = root / "reports/research_tripwires/treasury_auction_demand_shock_causal_v1/causal_event_ledger.jsonl"
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    index = {(row["event_id"], row["policy"], row["scenario"]): row for row in rows}
    output: list[dict[str, Any]] = []
    for event_id in sorted({row["event_id"] for row in rows}):
        normal = index.get((event_id, "CAUSAL_DEMAND", "NORMAL"))
        stressed = index.get((event_id, "CAUSAL_DEMAND", "STRESSED"))
        fade_n = index.get((event_id, "DIRECTION_FLIP", "NORMAL"))
        fade_s = index.get((event_id, "DIRECTION_FLIP", "STRESSED"))
        if not all(row and row.get("status") == "TRADE_COMPLETED" for row in (normal, stressed, fade_n, fade_s)):
            continue
        assert normal is not None and stressed is not None and fade_n is not None and fade_s is not None
        cost = float(stressed["fees_usd"]) + float(stressed["slippage_usd"])
        output.append({
            "event_id": stable_hash({"ecology": "TREASURY_AUCTION", "event": event_id})[:24],
            "ecology": "TREASURY_AUCTION",
            "market": str(normal["market"]),
            "timestamp": str(normal["available_at"]),
            "role": str(normal["role"]),
            "surprise_raw": abs(float(normal["demand_score"])),
            "liquidity_raw": float(normal["quantity"]) / max(cost, 1e-9),
            "volatility_raw": abs(float(normal["stop_price"]) - float(normal["entry_price"])),
            "response_raw": abs(float(normal["demand_score"])),
            "cost_usd": cost,
            "continuation_normal": float(normal["net_pnl_usd"]),
            "continuation_stressed": float(stressed["net_pnl_usd"]),
            "continuation_worst": float(stressed["minimum_trade_pnl_usd"]),
            "fade_normal": float(fade_n["net_pnl_usd"]),
            "fade_stressed": float(fade_s["net_pnl_usd"]),
            "fade_worst": float(fade_s["minimum_trade_pnl_usd"]),
        })
    return output


def _trailing_normalize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (row["ecology"], row["timestamp"], row["event_id"]))
    histories: dict[tuple[str, str], list[float]] = {}
    for row in ordered:
        for name in ("surprise", "liquidity", "volatility", "response"):
            value = float(row[f"{name}_raw"])
            history = histories.setdefault((str(row["ecology"]), name), [])
            trailing = np.asarray(history[-24:], dtype=float)
            if len(trailing) >= 6:
                center = float(np.median(trailing))
                mad = float(np.median(np.abs(trailing - center)))
                scale = max(1.4826 * mad, float(np.std(trailing)), 1e-9)
                row[f"{name}_z"] = float(np.clip((value - center) / scale, -4.0, 4.0))
                row["feature_ready"] = bool(row.get("feature_ready", True))
            else:
                row[f"{name}_z"] = 0.0
                row["feature_ready"] = False
            history.append(value)
    return sorted(ordered, key=lambda row: (row["timestamp"], row["ecology"], row["event_id"]))


def materialize(root: Path, output: Path, manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    del manifest
    events = _trailing_normalize([*_load_ng(root), *_load_usda(root), *_load_treasury(root)])
    event_path = output / "pooled_event_ledger.jsonl"
    data = "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in events)
    event_path.write_text(data, encoding="utf-8")
    _atomic(output / "materialization_state.json", {
        "status": "POOLED_EVENTS_READY",
        "event_count": len(events),
        "event_count_by_ecology": {ecology: sum(row["ecology"] == ecology for row in events) for ecology in sorted({row["ecology"] for row in events})},
        "feature_ready_count": sum(bool(row["feature_ready"]) for row in events),
        "ledger_sha256": _sha(event_path),
    })
    return events


def _design(rows: Sequence[Mapping[str, Any]]) -> np.ndarray:
    return np.asarray(
        [
            [
                1.0,
                float(row["surprise_z"]),
                float(row["liquidity_z"]),
                float(row["volatility_z"]),
                float(row["response_z"]),
                float(row["surprise_z"]) * float(row["response_z"]),
                float(row["liquidity_z"]) * float(row["volatility_z"]),
            ]
            for row in rows
        ],
        dtype=float,
    )


def _fit(rows: Sequence[Mapping[str, Any]], ridge: float) -> tuple[np.ndarray, np.ndarray]:
    if len(rows) < 12:
        raise MetaPolicyError("insufficient pooled training events")
    design = _design(rows)
    cost = np.asarray([max(float(row["cost_usd"]), 1.0) for row in rows])
    continuation = np.asarray([float(row["continuation_stressed"]) for row in rows]) / cost
    fade = np.asarray([float(row["fade_stressed"]) for row in rows]) / cost
    penalty = np.eye(design.shape[1], dtype=float) * float(ridge)
    penalty[0, 0] = 0.0
    gram = design.T @ design + penalty
    return (
        np.linalg.solve(gram, design.T @ continuation),
        np.linalg.solve(gram, design.T @ fade),
    )


def _decide(
    rows: Sequence[Mapping[str, Any]],
    coefficients: tuple[np.ndarray, np.ndarray],
    spec: PolicySpec,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    design = _design(rows)
    continuation = design @ coefficients[0]
    fade = design @ coefficients[1]
    output: list[dict[str, Any]] = []
    for index, source in enumerate(rows):
        best = max(float(continuation[index]), float(fade[index]))
        if best < spec.minimum_predicted_edge_to_cost:
            action, risk = "ABSTAIN", 0.0
        else:
            action = "CONTINUATION" if continuation[index] >= fade[index] else "FADE"
            risk = 1.5 if best >= spec.high_risk_threshold else 1.0
        if action == "CONTINUATION":
            normal = float(source["continuation_normal"]) * risk
            stressed = float(source["continuation_stressed"]) * risk
            worst = float(source["continuation_worst"]) * risk
        elif action == "FADE":
            normal = float(source["fade_normal"]) * risk
            stressed = float(source["fade_stressed"]) * risk
            worst = float(source["fade_worst"]) * risk
        else:
            normal = stressed = worst = 0.0
        output.append({
            **dict(source),
            "policy_id": spec.policy_id,
            "action": action,
            "risk_tier": risk,
            "predicted_continuation_edge_to_cost": float(continuation[index]),
            "predicted_fade_edge_to_cost": float(fade[index]),
            "normal_net_usd": normal,
            "stressed_net_usd": stressed,
            "minimum_open_pnl_stressed_usd": worst,
        })
    return output


def _summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    traded = [row for row in rows if row["action"] != "ABSTAIN"]
    positive = [float(row["stressed_net_usd"]) for row in traded if float(row["stressed_net_usd"]) > 0.0]
    ecology_net = {
        ecology: float(sum(float(row["stressed_net_usd"]) for row in traded if row["ecology"] == ecology))
        for ecology in sorted({str(row["ecology"]) for row in rows})
    }
    positive_ecologies = [max(0.0, value) for value in ecology_net.values()]
    return {
        "event_count": len(rows),
        "trade_count": len(traded),
        "trade_coverage": len(traded) / len(rows) if rows else 0.0,
        "action_counts": {action: sum(row["action"] == action for row in rows) for action in ("CONTINUATION", "FADE", "ABSTAIN")},
        "normal_net_usd": float(sum(float(row["normal_net_usd"]) for row in traded)),
        "stressed_net_usd": float(sum(float(row["stressed_net_usd"]) for row in traded)),
        "minimum_open_pnl_stressed_usd": min((float(row["minimum_open_pnl_stressed_usd"]) for row in traded), default=None),
        "positive_trade_rate": sum(float(row["stressed_net_usd"]) > 0.0 for row in traded) / len(traded) if traded else 0.0,
        "maximum_single_trade_positive_profit_share": max(positive) / sum(positive) if positive and sum(positive) > 0.0 else None,
        "stressed_net_by_ecology": ecology_net,
        "maximum_positive_ecology_share": max(positive_ecologies) / sum(positive_ecologies) if sum(positive_ecologies) > 0.0 else 1.0,
        "decision_hash": stable_hash([{key: row[key] for key in ("event_id", "action", "risk_tier", "normal_net_usd", "stressed_net_usd")} for row in rows]),
    }


def _loeo(events: Sequence[Mapping[str, Any]], spec: PolicySpec) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    discovery = [row for row in events if row["feature_ready"] and row["role"] == "DISCOVERY"]
    predictions: list[dict[str, Any]] = []
    by_ecology: dict[str, Any] = {}
    for ecology in sorted({str(row["ecology"]) for row in discovery}):
        train = [row for row in discovery if row["ecology"] != ecology]
        test = [row for row in discovery if row["ecology"] == ecology]
        decided = _decide(test, _fit(train, spec.ridge), spec)
        predictions.extend(decided)
        by_ecology[ecology] = _summary(decided)
    predictions.sort(key=lambda row: (row["timestamp"], row["event_id"]))
    return predictions, {"aggregate": _summary(predictions), "by_ecology": by_ecology}


def _baseline(rows: Sequence[Mapping[str, Any]], action: str) -> dict[str, Any]:
    synthetic = []
    for row in rows:
        chosen = dict(row)
        chosen.update({"action": action, "risk_tier": 1.0})
        prefix = "continuation" if action == "CONTINUATION" else "fade"
        chosen["normal_net_usd"] = float(row[f"{prefix}_normal"])
        chosen["stressed_net_usd"] = float(row[f"{prefix}_stressed"])
        chosen["minimum_open_pnl_stressed_usd"] = float(row[f"{prefix}_worst"])
        synthetic.append(chosen)
    return _summary(synthetic)


def _upper_bound(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    synthetic = []
    for row in rows:
        selected = dict(row)
        if max(float(row["continuation_stressed"]), float(row["fade_stressed"])) <= 0.0:
            selected.update({"action": "ABSTAIN", "risk_tier": 0.0, "normal_net_usd": 0.0, "stressed_net_usd": 0.0, "minimum_open_pnl_stressed_usd": 0.0})
        else:
            prefix = "continuation" if float(row["continuation_stressed"]) >= float(row["fade_stressed"]) else "fade"
            selected.update({"action": prefix.upper(), "risk_tier": 1.0, "normal_net_usd": float(row[f"{prefix}_normal"]), "stressed_net_usd": float(row[f"{prefix}_stressed"]), "minimum_open_pnl_stressed_usd": float(row[f"{prefix}_worst"])})
        synthetic.append(selected)
    return {"classification": "NON_DEPLOYABLE_FULL_OUTCOME_UPPER_BOUND", **_summary(synthetic)}


def evaluate(root: Path, output: Path, manifest: Mapping[str, Any], events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    usable = [row for row in events if row["feature_ready"]]
    races = []
    for spec in frozen_specs():
        decisions, summary = _loeo(usable, spec)
        races.append({"policy": asdict(spec), "policy_id": spec.policy_id, "summary": summary, "decision_hash": stable_hash(decisions)})
    races.sort(key=lambda row: (float(row["summary"]["aggregate"]["stressed_net_usd"]), -float(row["summary"]["aggregate"]["maximum_positive_ecology_share"]), int(row["summary"]["aggregate"]["trade_count"]), row["policy_id"]), reverse=True)
    winner = PolicySpec(**races[0]["policy"])
    selection_core = {
        "schema": "hydra_scheduled_information_shock_meta_selection_v1",
        "status": "FROZEN_AFTER_LOEO_DISCOVERY_BEFORE_CHRONOLOGICAL_HELD_EVALUATION",
        "manifest_hash": manifest["manifest_hash"],
        "selected_policy": asdict(winner),
        "selected_policy_id": winner.policy_id,
        "loeo_discovery": races[0]["summary"],
    }
    selection = {**selection_core, "selection_hash": stable_hash(selection_core)}
    _write_once(output / "selection_freeze.json", selection)
    discovery = [row for row in usable if row["role"] == "DISCOVERY"]
    coefficients = _fit(discovery, winner.ridge)
    role_rows: dict[str, list[dict[str, Any]]] = {}
    role_summary: dict[str, Any] = {}
    for role in ROLES:
        selected = [row for row in usable if row["role"] == role]
        role_rows[role] = _decide(selected, coefficients, winner)
        role_summary[role] = _summary(role_rows[role])
    held_rows = [*role_rows["VALIDATION"], *role_rows["FINAL_DEVELOPMENT"]]
    held_summary = _summary(held_rows)
    held_ecologies = held_summary["stressed_net_by_ecology"]
    loeo_ecologies = races[0]["summary"]["by_ecology"]
    checks = {
        "minimum_100_total_events": len(usable) >= 100,
        "loeo_positive_each_ecology": all(float(row["stressed_net_usd"]) > 0.0 for row in loeo_ecologies.values()),
        "positive_stressed_validation": float(role_summary["VALIDATION"]["stressed_net_usd"]) > 0.0,
        "positive_stressed_final": float(role_summary["FINAL_DEVELOPMENT"]["stressed_net_usd"]) > 0.0,
        "positive_each_held_ecology": bool(held_ecologies) and all(float(value) > 0.0 for value in held_ecologies.values()),
        "no_single_ecology_domination": float(held_summary["maximum_positive_ecology_share"]) <= 0.50,
        "nontrivial_trade_coverage": 0.10 <= float(held_summary["trade_coverage"]) <= 0.90,
    }
    gate = all(checks.values())
    held_source = [row for row in usable if row["role"] in ("VALIDATION", "FINAL_DEVELOPMENT")]
    upper = _upper_bound(held_source)
    verdict = "SCHEDULED_INFORMATION_SHOCK_META_POLICY_HELD_GATE_GREEN" if gate else "SCHEDULED_INFORMATION_SHOCK_META_POLICY_FALSIFIED"
    core = {
        "schema": "hydra_scheduled_information_shock_meta_policy_result_v1",
        "status": "COMPLETE",
        "verdict": verdict,
        "manifest_hash": manifest["manifest_hash"],
        "selection_hash": selection["selection_hash"],
        "counts": {"pooled_events": len(events), "feature_ready_events": len(usable), "policy_specs": 24, "loeo_ecologies": len(loeo_ecologies), "account_replay_executed": False},
        "selected_policy": asdict(winner),
        "selected_policy_id": winner.policy_id,
        "loeo_discovery": races[0]["summary"],
        "chronological_roles": role_summary,
        "held_summary": held_summary,
        "controls": {"always_continuation": _baseline(held_source, "CONTINUATION"), "always_fade": _baseline(held_source, "FADE")},
        "non_deployable_upper_bound": upper,
        "gate_checks": checks,
        "held_gate_passed": gate,
        "account_replay_block_reason": None if gate else "POOLED_HELD_ECOLOGY_GATE_NOT_MET",
        "new_data_purchase": False,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "xfa_paths": 0,
        "next_action": "RUN_DIRECT_EXACT_ACCOUNT_REPLAY" if gate else "TOMBSTONE_POOLED_SCHEDULED_SHOCK_ROUTER_AND_START_CROSS_ASSET_VOLATILITY_RISK_PREMIUM",
    }
    result = {**core, "result_hash": stable_hash(core)}
    _write_once(output / "economic_result.json", result)
    _atomic(output / "production_state.json", {key: result[key] for key in ("status", "verdict", "counts", "result_hash", "next_action")})
    return result


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(argv or ())
    os.environ.update({"OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "NUMEXPR_NUM_THREADS": "1"})
    output = ROOT / OUTPUT
    manifest = freeze(ROOT, output)
    _atomic(output / "production_state.json", {"status": "EVENT_MATERIALIZATION_ACTIVE", "pid": os.getpid()})
    started = time.perf_counter()
    ledger = output / "pooled_event_ledger.jsonl"
    if "--evaluate-existing" in arguments:
        if not ledger.is_file():
            raise MetaPolicyError("pooled event ledger unavailable")
        events = [json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines() if line.strip()]
    else:
        events = materialize(ROOT, output, manifest)
    result = evaluate(ROOT, output, manifest, events)
    print(json.dumps({"verdict": result["verdict"], "counts": result["counts"], "runtime_seconds": time.perf_counter() - started, "result_hash": result["result_hash"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
