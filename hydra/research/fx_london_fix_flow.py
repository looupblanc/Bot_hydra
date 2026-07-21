"""Causal London 16:00 benchmark-fix flow and inventory-unwind tripwire.

The older FX ecology never conditioned on the benchmark-fix clock.  This
bounded branch freezes 128 complete policies before opening outcomes, selects
only on 2018--2021 discovery, and evaluates 2022, 2023, and 2024Q1--Q3 as
separate viewed-development roles.  It never accesses Q4 or a network source.
"""

from __future__ import annotations

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
from hydra.research import fx_causal_ecology as fx


SCHEMA = "hydra_fx_london_fix_flow_v1"
ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = Path("reports/research_tripwires/fx_london_fix_flow_v1")
RAW = Path("data/cache/databento/fx_causal_ecology/9a2ae71aabd3a11c8b7f/raw_ohlcv.dbn.zst")
DEFINITIONS = Path("data/cache/databento/fx_causal_ecology/9a2ae71aabd3a11c8b7f/raw_definition.dbn.zst")
EXPECTED_RAW_SHA = "ea055647b648d7c49e8efb85ba9bd6a30b17ec1aaa5ba0a0ab663dd15fd7cbe8"
EXPECTED_DEFINITION_SHA = "4accf4d291799914091ffec1d4e6d2c808a3e98ad11bd4aacddceea14470c2dd"
ROLES = {
    "DISCOVERY": ("2018-01-02", "2022-01-01"),
    "VALIDATION": ("2022-01-01", "2023-01-01"),
    "FINAL_DEVELOPMENT": ("2023-01-01", "2024-01-01"),
    "REPLICATION_DEVELOPMENT": ("2024-01-01", "2024-10-01"),
}
ACCOUNT_LABELS = ("50K", "100K", "150K")
RISK_FRACTIONS = (0.10, 0.20, 0.30)


class LondonFixError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class FixPolicy:
    mechanism: str
    root: str
    session_scope: str
    amplitude_atr: float
    signal_mode: str
    exit_parameter: str
    stop_atr: float = 1.0
    target_atr: float = 2.0

    @property
    def policy_id(self) -> str:
        return "fxfix_" + stable_hash(asdict(self))[:24]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_once(path: Path, payload: Mapping[str, Any]) -> None:
    text = json.dumps(dict(payload), indent=2, sort_keys=True, default=str) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise LondonFixError(f"immutable artifact drift: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def _write_state(path: Path, payload: Mapping[str, Any]) -> None:
    """Atomically replace the mutable operational state marker.

    Economic and selection artifacts remain write-once.  The production state
    is deliberately mutable because it advances from loading to a terminal
    status without changing any evidence.
    """
    text = json.dumps(dict(payload), indent=2, sort_keys=True, default=str) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def frozen_policies() -> tuple[FixPolicy, ...]:
    policies: list[FixPolicy] = []
    # Complete pre-fix lattice: 96 policies.
    for root in fx.ROOTS:
        for scope in ("DAILY", "MONTH_END"):
            for amplitude in (0.5, 1.0):
                for mode in ("OWN", "CONSENSUS_3_OF_4"):
                    for exit_clock in ("16:01", "16:05", "16:15"):
                        policies.append(FixPolicy(
                            "PRE_FIX_CONTINUATION", root, scope, amplitude,
                            mode, exit_clock,
                        ))
    # Balanced post-fix lattice: 32 policies; every 15/30/60 horizon is present.
    for root in fx.ROOTS:
        for amplitude in (0.5, 1.0):
            for mode, holds in (
                ("OWN", (15, 30)),
                ("CONSENSUS_3_OF_4", (30, 60)),
            ):
                for holding in holds:
                    policies.append(FixPolicy(
                        "POST_FIX_INVENTORY_UNWIND", root, "DAILY", amplitude,
                        mode, f"HOLD_{holding}M",
                    ))
    if len(policies) != 128 or len({row.policy_id for row in policies}) != 128:
        raise LondonFixError("frozen 128-policy lattice drift")
    return tuple(policies)


def freeze(output: Path) -> dict[str, Any]:
    if _sha256(ROOT / RAW) != EXPECTED_RAW_SHA:
        raise LondonFixError("FX OHLCV hash drift")
    if _sha256(ROOT / DEFINITIONS) != EXPECTED_DEFINITION_SHA:
        raise LondonFixError("FX definition hash drift")
    prior = ROOT / "reports/research_tripwires/fx_causal_ecology_pilot_v1_revision_02/economic_result.json"
    if not prior.is_file():
        raise LondonFixError("prior FX cemetery evidence absent")
    decision_core = {
        "schema": f"{SCHEMA}_decision_card",
        "status": "FROZEN_BEFORE_OUTCOMES",
        "hypothesis": "Benchmark-fix hedging creates continuation into 16:00 London and causal inventory unwind after the fix, distinct from generic FX residual ecology.",
        "strongest_argument_against": "One-minute aggressive execution may consume the clock-conditioned edge and month-end flow may be too sparse.",
        "smallest_decisive_experiment": "128 frozen policies, discovery-only selection, three held chronological development roles, controls and exact 50/100/150K P5/P10/P20.",
        "expected_cost_usd": 0.0,
        "next_materially_distinct_alternative": "CROSS_ASSET_VOLATILITY_RISK_PREMIUM_CARRY_SHOCK",
    }
    decision = {**decision_core, "decision_hash": stable_hash(decision_core)}
    _write_once(output / "decision_card.json", decision)
    policies = [asdict(row) for row in frozen_policies()]
    core = {
        "schema": f"{SCHEMA}_manifest",
        "status": "FROZEN_BEFORE_OUTCOMES",
        "decision_hash": decision["decision_hash"],
        "cemetery_audit": {
            "prior_fx_result_sha256": _sha256(prior),
            "exact_fix_clock_hit_count": 0,
            "classification": "MATERIALLY_DISTINCT_BENCHMARK_CLOCK_CONDITIONING",
        },
        "inputs": {
            "raw_path": str(RAW), "raw_sha256": EXPECTED_RAW_SHA,
            "definition_path": str(DEFINITIONS),
            "definition_sha256": EXPECTED_DEFINITION_SHA,
        },
        "roles": ROLES,
        "selection_uses_only": "DISCOVERY",
        "policies": policies,
        "policy_count": len(policies),
        "causal_contract": {
            "pre_fix": "15:30_TO_COMPLETED_15:55_EUROPE_LONDON; DECIDE_15:56; NEXT_OBSERVED_OPEN; EXIT_16:01/05/15",
            "post_fix": "15:50_TO_COMPLETED_16:00; DECIDE_16:01; FILL_16:02; OPPOSITE_DIRECTION; HOLD_15/30/60",
            "roll_exclusion": "PLUS_MINUS_ONE_SESSION",
            "costs": "FX_FROZEN_NORMAL_AND_1_5X_STRESSED",
        },
        "worker_processes": 1, "numeric_threads_per_worker": 1,
        "new_data_purchase": False, "q4_access": 0,
        "broker_connections": 0, "orders": 0,
    }
    manifest = {**core, "manifest_hash": stable_hash(core)}
    _write_once(output / "manifest.json", manifest)
    return manifest


def _clock_maps(panel: Mapping[str, Any]) -> dict[str, Any]:
    timestamps = panel["timestamps"]
    london = timestamps.tz_convert("Europe/London")
    date = london.strftime("%Y-%m-%d").to_numpy()
    minute = (london.hour * 60 + london.minute).to_numpy()
    lookup = {(str(date[i]), int(minute[i])): i for i in range(len(timestamps))}
    ordered_dates = tuple(dict.fromkeys(str(value) for value in date))
    month_last: set[str] = set()
    grouped: dict[str, list[str]] = {}
    for value in ordered_dates:
        grouped.setdefault(value[:7], []).append(value)
    for values in grouped.values():
        month_last.add(values[-1])
    return {"date": date, "minute": minute, "lookup": lookup, "dates": ordered_dates, "month_last": month_last}


def _roll_exclusions(panel: Mapping[str, Any], clocks: Mapping[str, Any]) -> dict[str, set[str]]:
    output = {root: set() for root in fx.ROOTS}
    dates = list(clocks["dates"])
    for root in fx.ROOTS:
        prior: int | None = None
        for offset, date in enumerate(dates):
            index = clocks["lookup"].get((date, 15 * 60 + 55))
            if index is None:
                continue
            value = panel["contract_state"][root].iat[index]
            if pd.isna(value):
                continue
            current = int(value)
            if prior is not None and current != prior:
                for neighbor in (offset - 1, offset, offset + 1):
                    if 0 <= neighbor < len(dates):
                        output[root].add(dates[neighbor])
            prior = current
    return output


def _finite(panel: Mapping[str, Any], field: str, root: str, index: int) -> float | None:
    value = float(panel[field][root].iat[index])
    return value if math.isfinite(value) else None


def _displacements(panel: Mapping[str, Any], clocks: Mapping[str, Any], date: str, mechanism: str) -> tuple[dict[str, float], int, int, int] | None:
    lookup = clocks["lookup"]
    if mechanism == "PRE_FIX_CONTINUATION":
        start_minute, end_minute, decision_minute = 15 * 60 + 30, 15 * 60 + 55, 15 * 60 + 56
    else:
        start_minute, end_minute, decision_minute = 15 * 60 + 50, 16 * 60, 16 * 60 + 1
    start = lookup.get((date, start_minute))
    end = lookup.get((date, end_minute))
    decision = lookup.get((date, decision_minute))
    if start is None or end is None or decision is None or not start < end < decision:
        return None
    values: dict[str, float] = {}
    for root in fx.ROOTS:
        opening = _finite(panel, "open", root, start)
        closing = _finite(panel, "close", root, end)
        if opening is not None and closing is not None:
            values[root] = closing - opening
    if len(values) < 3:
        return None
    return values, start, end, decision


def materialize(policy: FixPolicy, panel: Mapping[str, Any], clocks: Mapping[str, Any], roll_exclusions: Mapping[str, set[str]], role: tuple[str, str], *, direction_flip: bool = False, clock_shift_minutes: int = 0) -> tuple[fx.RawTrade, ...]:
    start_role = pd.Timestamp(role[0], tz="UTC")
    end_role = pd.Timestamp(role[1], tz="UTC")
    output: list[fx.RawTrade] = []
    for date in clocks["dates"]:
        utc_noon = pd.Timestamp(date, tz="Europe/London").tz_convert("UTC") + pd.Timedelta(hours=12)
        if not (start_role <= utc_noon < end_role):
            continue
        if policy.session_scope == "MONTH_END" and date not in clocks["month_last"]:
            continue
        if date in roll_exclusions[policy.root]:
            continue
        displaced = _displacements(panel, clocks, date, policy.mechanism)
        if displaced is None:
            continue
        moves, _start, _end, decision = displaced
        decision += int(clock_shift_minutes)
        if decision < 0 or decision >= len(panel["timestamps"]):
            continue
        atr = _finite(panel, "atr", policy.root, decision)
        own = moves.get(policy.root)
        if atr is None or atr <= 0.0 or own is None or abs(own) < policy.amplitude_atr * atr:
            continue
        signs = [1 if value > 0 else -1 for value in moves.values() if value != 0.0]
        common = 1 if signs.count(1) >= 3 else (-1 if signs.count(-1) >= 3 else 0)
        if policy.signal_mode == "CONSENSUS_3_OF_4":
            if common == 0 or (1 if own > 0 else -1) != common:
                continue
            direction = common
        else:
            direction = 1 if own > 0 else -1
        if policy.mechanism == "POST_FIX_INVENTORY_UNWIND":
            direction = -direction
        if direction_flip:
            direction = -direction
        fill_minute = 15 * 60 + 57 if policy.mechanism == "PRE_FIX_CONTINUATION" else 16 * 60 + 2
        fill_minute += int(clock_shift_minutes)
        entry = clocks["lookup"].get((date, fill_minute))
        if entry is None:
            continue
        if policy.mechanism == "PRE_FIX_CONTINUATION":
            hour, minute = (int(value) for value in policy.exit_parameter.split(":"))
            exit_index = clocks["lookup"].get((date, hour * 60 + minute))
        else:
            holding = int(policy.exit_parameter.split("_")[1][:-1])
            exit_index = clocks["lookup"].get((date, fill_minute + holding))
        if exit_index is None or exit_index <= entry:
            continue
        contract = panel["contract_state"][policy.root]
        entry_contract = contract.iat[entry]
        if pd.isna(entry_contract):
            continue
        entry_contract = int(entry_contract)
        entry_price = _finite(panel, "open", policy.root, entry)
        if entry_price is None:
            continue
        stop_distance = atr * policy.stop_atr
        stop = entry_price - direction * stop_distance
        target = entry_price + direction * stop_distance * policy.target_atr
        terminal_price: float | None = None
        terminal_index: int | None = None
        worst = 0.0
        best = 0.0
        same_bar = False
        for index in range(entry, exit_index + 1):
            current = contract.iat[index]
            if pd.isna(current) or int(current) != entry_contract:
                break
            high = _finite(panel, "high", policy.root, index)
            low = _finite(panel, "low", policy.root, index)
            close = _finite(panel, "close", policy.root, index)
            if high is None or low is None or close is None:
                continue
            favorable = high >= target if direction > 0 else low <= target
            adverse = low <= stop if direction > 0 else high >= stop
            best = max(best, (high - entry_price) if direction > 0 else (entry_price - low))
            worst = min(worst, (low - entry_price) if direction > 0 else (entry_price - high))
            terminal_index, terminal_price = index, close
            if adverse:
                terminal_price = stop
                same_bar = favorable
                break
            if favorable:
                terminal_price = target
                break
        if terminal_index is None or terminal_price is None:
            continue
        point = fx.POINT_VALUES[policy.root]
        gross = (terminal_price - entry_price) * direction * point
        normal_cost = fx._cost_per_contract(policy.root, stressed=False)
        stressed_cost = fx._cost_per_contract(policy.root, stressed=True)
        session_day = int(panel["session_day"][entry])
        output.append(fx.RawTrade(
            trade_id=f"{policy.policy_id}:{date}", root=policy.root,
            direction=direction, decision_ns=int(panel["timestamps"][decision].value),
            entry_ns=int(panel["timestamps"][entry].value),
            exit_ns=int(panel["timestamps"][terminal_index].value),
            session_day=session_day, entry_price=entry_price, exit_price=terminal_price,
            stop_distance=stop_distance, gross_one_contract=gross,
            normal_net_one_contract=gross - normal_cost,
            stressed_net_one_contract=gross - stressed_cost,
            normal_worst_one_contract=min(worst * point - normal_cost, gross - normal_cost),
            stressed_worst_one_contract=min(worst * point - stressed_cost, gross - stressed_cost),
            normal_best_one_contract=max(best * point - normal_cost, gross - normal_cost),
            stressed_best_one_contract=max(best * point - stressed_cost, gross - stressed_cost),
            same_bar_ambiguous=same_bar,
        ))
    return tuple(output)


def _trade_summary(trades: Sequence[fx.RawTrade]) -> dict[str, Any]:
    gross = float(sum(row.gross_one_contract for row in trades))
    normal = float(sum(row.normal_net_one_contract for row in trades))
    stressed = float(sum(row.stressed_net_one_contract for row in trades))
    cost = float(sum(row.gross_one_contract - row.stressed_net_one_contract for row in trades))
    positive = sorted((max(0.0, row.stressed_net_one_contract) for row in trades), reverse=True)
    return {
        "trade_count": len(trades), "gross_usd": gross,
        "normal_net_usd": normal, "stressed_net_usd": stressed,
        "edge_to_stressed_cost_ratio": gross / cost if cost > 0 else 0.0,
        "top_trade_positive_profit_share": (
            positive[0] / sum(positive) if positive and sum(positive) > 0 else 0.0
        ),
        "trade_hash": stable_hash([asdict(row) for row in trades]),
    }


def _discovery_score(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        float(row["stressed_net_usd"]) > 0.0,
        float(row["stressed_net_usd"]) / max(math.sqrt(int(row["trade_count"])), 1.0),
        float(row["edge_to_stressed_cost_ratio"]),
        int(row["trade_count"]),
    )


def run(root: Path = ROOT, output: Path = OUTPUT_DIR) -> dict[str, Any]:
    os.environ.update({"OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "NUMEXPR_NUM_THREADS": "1"})
    started = time.perf_counter()
    destination = root / output
    manifest = freeze(destination)
    _write_state(destination / "production_state.json", {"status": "RAW_LOAD_ACTIVE", "pid": os.getpid(), "policy_count": 128})
    source_manifest, _receipt, bars = fx.load_inputs(root)
    panel = fx.build_panel(bars)
    clocks = _clock_maps(panel)
    exclusions = _roll_exclusions(panel, clocks)
    rules = json.loads((root / source_manifest["official_rule_snapshot"]["path"]).read_text(encoding="utf-8"))
    configs = fx._rule_configs(rules)
    discovery = []
    for payload in manifest["policies"]:
        policy = FixPolicy(**dict(payload))
        trades = materialize(policy, panel, clocks, exclusions, ROLES["DISCOVERY"])
        discovery.append({"policy": payload, "policy_id": policy.policy_id, **_trade_summary(trades)})
    discovery.sort(key=_discovery_score, reverse=True)
    selected = discovery[:8]
    freeze_core = {
        "schema": f"{SCHEMA}_selection_freeze", "status": "FROZEN_AFTER_DISCOVERY_BEFORE_HELD_ROLES",
        "manifest_hash": manifest["manifest_hash"], "selection_role": "DISCOVERY",
        "selected": [{"policy": row["policy"], "policy_id": row["policy_id"]} for row in selected],
    }
    selection = {**freeze_core, "freeze_hash": stable_hash(freeze_core)}
    _write_once(destination / "selection_freeze.json", selection)
    held: dict[str, list[dict[str, Any]]] = {}
    trade_cache: dict[tuple[str, str], tuple[fx.RawTrade, ...]] = {}
    for role in ("VALIDATION", "FINAL_DEVELOPMENT", "REPLICATION_DEVELOPMENT"):
        values = []
        for row in selected:
            policy = FixPolicy(**dict(row["policy"]))
            trades = materialize(policy, panel, clocks, exclusions, ROLES[role])
            trade_cache[(policy.policy_id, role)] = trades
            values.append({"policy": row["policy"], "policy_id": policy.policy_id, **_trade_summary(trades)})
        held[role] = values
    serious = []
    for selected_row in selected:
        policy_id = str(selected_row["policy_id"])
        roles = {role: next(row for row in held[role] if row["policy_id"] == policy_id) for role in held}
        gate = {
            "positive_stressed_all_held_roles": all(row["stressed_net_usd"] > 0.0 for row in roles.values()),
            "edge_to_cost_1_25_all_held_roles": all(row["edge_to_stressed_cost_ratio"] > 1.25 for row in roles.values()),
            "minimum_10_events_each_held_role": all(row["trade_count"] >= 10 for row in roles.values()),
            "no_single_trade_above_25pct": all(row["top_trade_positive_profit_share"] <= 0.25 for row in roles.values()),
        }
        serious.append({
            "policy": selected_row["policy"], "policy_id": policy_id,
            "discovery": selected_row, "held_roles": roles, "gate": gate,
            "held_gate_passed": all(gate.values()),
        })
    account_inputs = serious[:4]
    account_rows = []
    oracle_rows = []
    for row in account_inputs:
        for role in ("VALIDATION", "FINAL_DEVELOPMENT", "REPLICATION_DEVELOPMENT"):
            trades = trade_cache[(row["policy_id"], role)]
            days = fx._eligible_days(panel, ROLES[role])
            for label in ACCOUNT_LABELS:
                config, maximum = configs[label]
                for risk in RISK_FRACTIONS:
                    account = fx.account_frontier(trades, days, config=config, maximum_contracts=maximum, risk_fraction=risk)
                    account_rows.append({"policy_id": row["policy_id"], "role": role, "account_label": label, "risk_fraction": risk, "account": account})
                    oracle = tuple(trade for trade in trades if trade.stressed_net_one_contract > 0.0)
                    oracle_rows.append({
                        "policy_id": row["policy_id"], "role": role,
                        "account_label": label, "risk_fraction": risk,
                        "classification": "NON_DEPLOYABLE_LEGAL_UPPER_BOUND",
                        "account": fx.account_frontier(oracle, days, config=config, maximum_contracts=maximum, risk_fraction=risk),
                    })
    controls = {}
    for row in serious:
        policy = FixPolicy(**dict(row["policy"]))
        role = ROLES["FINAL_DEVELOPMENT"]
        controls[policy.policy_id] = {
            "direction_flip": _trade_summary(materialize(policy, panel, clocks, exclusions, role, direction_flip=True)),
            "clock_shift_minus_30m": _trade_summary(materialize(policy, panel, clocks, exclusions, role, clock_shift_minutes=-30)),
            "clock_shift_plus_30m": _trade_summary(materialize(policy, panel, clocks, exclusions, role, clock_shift_minutes=30)),
        }
    deployable_p20 = sum(
        int(row["account"][scenario]["horizons"]["20"]["passes"])
        for row in account_rows for scenario in ("NORMAL", "STRESSED_1_5X")
    )
    oracle_p20 = sum(
        int(row["account"][scenario]["horizons"]["20"]["passes"])
        for row in oracle_rows for scenario in ("NORMAL", "STRESSED_1_5X")
    )
    gate_passers = [row for row in serious if row["held_gate_passed"]]
    verdict = (
        "FX_LONDON_FIX_FLOW_GREEN" if gate_passers and deployable_p20 > 0
        else ("FX_LONDON_FIX_FLOW_SELECTION_BOTTLENECK" if oracle_p20 > 0 else "FX_LONDON_FIX_FLOW_FALSIFIED")
    )
    core = {
        "schema": f"{SCHEMA}_result", "status": "COMPLETE", "verdict": verdict,
        "manifest_hash": manifest["manifest_hash"], "selection_freeze_hash": selection["freeze_hash"],
        "data": {"raw_sha256": EXPECTED_RAW_SHA, "bar_count": len(bars), "first": panel["timestamps"][0].isoformat(), "last": panel["timestamps"][-1].isoformat(), "q4_rows_read": 0},
        "counts": {"policies_frozen": 128, "discovery_evaluated": len(discovery), "held_policies": len(serious), "held_gate_passers": len(gate_passers), "account_cells": len(account_rows), "deployable_p20_passes": deployable_p20, "oracle_p20_passes": oracle_p20},
        "discovery_top": discovery[:16], "serious": serious,
        "account_results": account_rows, "non_deployable_upper_bound": oracle_rows,
        "controls": controls,
        "runtime_seconds": time.perf_counter() - started,
        "new_data_purchase": False, "q4_access_count_delta": 0,
        "broker_connections": 0, "orders": 0, "xfa_paths": 0,
        "next_action": (
            "FREEZE_PASSERS_FOR_FRESH_CONFIRMATION" if verdict == "FX_LONDON_FIX_FLOW_GREEN"
            else ("PRESERVE_FIX_FEATURE_AND_TEST_CAUSAL_QUALITY_ROUTER" if oracle_p20 > 0 else "TOMBSTONE_FIX_FLOW_AND_START_CROSS_ASSET_VOLATILITY_RISK_PREMIUM")
        ),
    }
    hash_core = {key: value for key, value in core.items() if key != "runtime_seconds"}
    result = {**core, "result_hash": stable_hash(hash_core)}
    _write_once(destination / "economic_result.json", result)
    _write_state(destination / "production_state.json", {key: result[key] for key in ("status", "verdict", "counts", "runtime_seconds", "result_hash", "next_action")})
    return result


def main(argv: Sequence[str] | None = None) -> int:
    del argv
    result = run()
    print(json.dumps({key: result[key] for key in ("verdict", "counts", "runtime_seconds", "result_hash", "next_action")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
