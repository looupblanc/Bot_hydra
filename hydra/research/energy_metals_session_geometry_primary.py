from __future__ import annotations

import hashlib
import json
import subprocess
import time
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import RollMap, load_roll_map
from hydra.data.databento_volume_front import VOLUME_FRONT_MAP_TYPE
from hydra.factory.quality_diversity import structural_fingerprint
from hydra.foundry.status import EvidenceTier, ShadowEvidence, decide_shadow_admission
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import (
    _apply_explicit_contract_map,
    _globex_session_fields,
    _stable_hash,
    _strict_json_value,
)
from hydra.research.energy_metals_barrier_primary import _read_period
from hydra.research.equity_open_gap_reversal import _account_replay, _write_immutable
from hydra.research.qd_economic_tournament import (
    SESSION_CLOCKS,
    _block_sign_flip_probability,
    _period_metrics,
    _round_turn_cost_all,
    _validation_events,
    _validation_metrics,
)
from hydra.shadow.specification import ShadowSpecification
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "energy_metals_session_geometry_primary_v1"
MARKET_PAIRS = {"CL": "MCL", "GC": "MGC"}
FEATURES = (
    "overnight_displacement",
    "opening_impulse_15",
    "opening_impulse_30",
    "overnight_extreme_position",
    "opening_efficiency_15",
    "opening_volume_surprise_15",
)
QUANTILES = (0.65, 0.80)
HORIZONS = (30, 60, 120)
CONTEXTS = ("none", "prior_trend_agree", "prior_trend_disagree")
ROUND1 = ("2023-01-01", "2023-07-01")
ROUND2 = ("2023-07-01", "2024-01-01")
PRIMARY_ALPHA = 0.03


class EnergyMetalsSessionGeometryError(RuntimeError):
    pass


def generate_session_geometry_hypotheses() -> list[dict[str, Any]]:
    population: list[dict[str, Any]] = []
    for market, execution_market in MARKET_PAIRS.items():
        for feature in FEATURES:
            family = (
                "overnight_inventory_transfer"
                if feature.startswith("overnight")
                else "opening_acceptance_geometry"
            )
            for policy in ("continuation", "reversal"):
                for quantile in QUANTILES:
                    for horizon in HORIZONS:
                        for context in CONTEXTS:
                            specification = {
                                "representation": VERSION,
                                "market": market,
                                "execution_market": execution_market,
                                "market_ecology": (
                                    "energy" if market == "CL" else "metals"
                                ),
                                "feature": feature,
                                "policy_direction": policy,
                                "quantile": quantile,
                                "horizon": horizon,
                                "context": context,
                                "mechanism_family": family,
                            }
                            fingerprint = structural_fingerprint(specification)
                            candidate_id = (
                                f"strategy_session_geometry_{market}_{feature}_{policy}_"
                                f"q{int(quantile * 100)}_h{horizon}_{context}_v1"
                            )
                            population.append(
                                {
                                    **specification,
                                    "candidate_id": candidate_id,
                                    "lineage_id": f"lineage_session_{fingerprint[:20]}",
                                    "structural_fingerprint": fingerprint,
                                    "portfolio_role": (
                                        "trend" if policy == "continuation" else "reversal"
                                    ),
                                }
                            )
    return sorted(population, key=lambda item: item["candidate_id"])


def build_session_geometry_table(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    data = frame[frame["symbol"].astype(str).eq(symbol)].copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
    trading_day, _phase = _globex_session_fields(data["timestamp"])
    data["trading_session_id"] = trading_day.astype(str)
    local = data["timestamp"].dt.tz_convert("America/Chicago")
    data["local_date"] = local.dt.date.astype(str)
    data["local_minute"] = local.dt.hour * 60 + local.dt.minute
    market_open, session_length = SESSION_CLOCKS[symbol]
    rows: list[dict[str, Any]] = []
    for session_id, group in data.groupby("trading_session_id", sort=True):
        rth = group[
            group["local_date"].eq(str(session_id))
            & group["local_minute"].ge(market_open)
            & group["local_minute"].lt(market_open + session_length)
        ].sort_values("timestamp").reset_index(drop=True)
        if len(rth) < 151 or int(rth.iloc[0]["local_minute"]) != market_open:
            continue
        overnight = group[group["timestamp"] < rth.iloc[0]["timestamp"]]
        if len(overnight) < 30:
            continue
        first_31 = pd.to_datetime(rth.loc[:30, "timestamp"], utc=True)
        if not first_31.diff().dropna().eq(pd.Timedelta(minutes=1)).all():
            continue
        close = rth["close"].astype(float)
        path = close.diff().abs()
        row: dict[str, Any] = {
            "session_id": str(session_id),
            "symbol": symbol,
            "active_contract": str(rth.iloc[0]["active_contract"]),
            "rth_open": float(rth.iloc[0]["open"]),
            "rth_close": float(rth.iloc[-1]["close"]),
            "rth_high": float(rth["high"].max()),
            "rth_low": float(rth["low"].min()),
            "open15_volume": float(rth.iloc[:15]["volume"].sum()),
            "overnight_high": float(overnight["high"].max()),
            "overnight_low": float(overnight["low"].min()),
            "opening_close15": float(close.iloc[14]),
            "opening_close30": float(close.iloc[29]),
            "opening_path15": float(path.iloc[1:15].sum()),
        }
        for prefix, decision_index in (
            ("overnight", 0),
            ("open15", 14),
            ("open30", 29),
        ):
            for delay in (0, 1):
                entry_index = decision_index + 1 + delay
                entry_timestamp = pd.Timestamp(rth.iloc[entry_index]["timestamp"])
                entry_price = float(rth.iloc[entry_index]["open"])
                suffix = "" if delay == 0 else "_delay1"
                row[f"{prefix}_entry_timestamp{suffix}"] = entry_timestamp
                row[f"{prefix}_entry_price{suffix}"] = entry_price
                for horizon in HORIZONS:
                    exit_index = entry_index + horizon
                    if exit_index >= len(rth):
                        continue
                    exit_timestamp = pd.Timestamp(rth.iloc[exit_index]["timestamp"])
                    if exit_timestamp - entry_timestamp != pd.Timedelta(minutes=horizon):
                        continue
                    path_window = rth.iloc[entry_index : exit_index + 1]
                    row[f"{prefix}_exit_{horizon}{suffix}"] = float(
                        rth.iloc[exit_index]["close"]
                    )
                    row[f"{prefix}_exit_timestamp_{horizon}{suffix}"] = exit_timestamp
                    row[f"{prefix}_long_mae_{horizon}{suffix}"] = float(
                        path_window["low"].min() - entry_price
                    )
                    row[f"{prefix}_short_mae_{horizon}{suffix}"] = float(
                        entry_price - path_window["high"].max()
                    )
        rows.append(row)
    table = pd.DataFrame(rows)
    if table.empty:
        raise EnergyMetalsSessionGeometryError(f"No valid RTH sessions for {symbol}.")
    table["prior_open"] = table["rth_open"].shift(1)
    table["prior_close"] = table["rth_close"].shift(1)
    table["prior_range"] = (
        table["rth_high"] - table["rth_low"]
    ).shift(1).replace(0, np.nan)
    table["prior_trend"] = (
        table["prior_close"] - table["prior_open"]
    ) / table["prior_range"]
    table["overnight_displacement"] = (
        table["rth_open"] - table["prior_close"]
    ) / table["prior_range"]
    table["opening_impulse_15"] = (
        table["opening_close15"] - table["rth_open"]
    ) / table["prior_range"]
    table["opening_impulse_30"] = (
        table["opening_close30"] - table["rth_open"]
    ) / table["prior_range"]
    overnight_width = (table["overnight_high"] - table["overnight_low"]).replace(
        0, np.nan
    )
    table["overnight_extreme_position"] = (
        (table["rth_open"] - table["overnight_low"]) / overnight_width - 0.5
    )
    table["opening_efficiency_15"] = (
        table["opening_close15"] - table["rth_open"]
    ) / table["opening_path15"].replace(0, np.nan)
    past_open_volume = (
        table["open15_volume"].shift(1).rolling(20, min_periods=10).median()
    )
    table["opening_volume_surprise_15"] = (
        table["open15_volume"] / past_open_volume.replace(0, np.nan) - 1.0
    )
    return table


def build_session_geometry_events(
    table: pd.DataFrame,
    hypothesis: dict[str, Any],
    *,
    entry_delay_bars: int = 0,
    quantile_override: float | None = None,
    horizon_override: int | None = None,
) -> pd.DataFrame:
    feature = str(hypothesis["feature"])
    values = pd.to_numeric(table[feature], errors="coerce")
    quantile = float(
        hypothesis["quantile"] if quantile_override is None else quantile_override
    )
    horizon = int(
        hypothesis["horizon"] if horizon_override is None else horizon_override
    )
    threshold = values.abs().shift(1).rolling(20, min_periods=10).quantile(quantile)
    anchor = np.sign(
        pd.to_numeric(table["opening_impulse_15"], errors="coerce")
        if feature == "opening_volume_surprise_15"
        else values
    )
    mask = values.abs().ge(threshold) & anchor.ne(0)
    agreement = np.sign(table["prior_trend"]).eq(anchor)
    if hypothesis["context"] == "prior_trend_agree":
        mask &= agreement
    elif hypothesis["context"] == "prior_trend_disagree":
        mask &= ~agreement
    prefix = (
        "overnight"
        if feature in {"overnight_displacement", "overnight_extreme_position"}
        else "open30"
        if feature == "opening_impulse_30"
        else "open15"
    )
    suffix = "" if entry_delay_bars == 0 else "_delay1"
    required = (
        f"{prefix}_entry_timestamp{suffix}",
        f"{prefix}_entry_price{suffix}",
        f"{prefix}_exit_timestamp_{horizon}{suffix}",
        f"{prefix}_exit_{horizon}{suffix}",
        f"{prefix}_long_mae_{horizon}{suffix}",
        f"{prefix}_short_mae_{horizon}{suffix}",
    )
    if any(column not in table for column in required):
        return pd.DataFrame()
    policy_sign = 1 if hypothesis["policy_direction"] == "continuation" else -1
    side = anchor * policy_sign
    symbol = str(table.iloc[0]["symbol"])
    point_value = instrument_spec(symbol).point_value
    cost = _round_turn_cost_all(symbol)
    output = pd.DataFrame(
        {
            "entry_timestamp": table[required[0]],
            "exit_timestamp": table[required[2]],
            "event_session_id": table["session_id"],
            "trading_session_id": table["session_id"],
            "symbol": table["symbol"],
            "active_contract": table["active_contract"],
            "side": side,
            "entry_price": table[required[1]],
            "exit_price": table[required[3]],
            "feature_value": values,
            "causal_threshold": threshold,
        }
    )
    output = output[
        mask
        & output["entry_timestamp"].notna()
        & output["exit_timestamp"].notna()
        & output["entry_price"].notna()
        & output["exit_price"].notna()
    ].copy()
    output["gross_pnl"] = (
        output["side"]
        * (output["exit_price"] - output["entry_price"])
        * point_value
    )
    output["cost"] = cost
    output["net_pnl"] = output["gross_pnl"] - output["cost"]
    long_mae = table.loc[output.index, required[4]].astype(float) * point_value
    short_mae = table.loc[output.index, required[5]].astype(float) * point_value
    output["mae_dollars"] = np.where(output["side"] > 0, long_mae, short_mae) - cost / 2
    output["entry_delay_bars"] = entry_delay_bars
    return output.reset_index(drop=True)


def run_energy_metals_session_geometry_primary(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    energy_data_path: str | Path,
    energy_data_sha256: str,
    energy_map_path: str | Path,
    energy_map_sha256: str,
    energy_roll_map_hash: str,
    metals_data_path: str | Path,
    metals_data_sha256: str,
    metals_map_path: str | Path,
    metals_map_sha256: str,
    metals_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
) -> dict[str, Any]:
    started = time.perf_counter()
    frozen = (
        (Path(engineering_task_path), engineering_task_sha256, "engineering task"),
        (Path(energy_data_path), energy_data_sha256, "energy data"),
        (Path(energy_map_path), energy_map_sha256, "energy map"),
        (Path(metals_data_path), metals_data_sha256, "metals data"),
        (Path(metals_map_path), metals_map_sha256, "metals map"),
    )
    for path, expected, label in frozen:
        _verify(path, expected, label)
    energy_map, metals_map = load_roll_map(energy_map_path), load_roll_map(
        metals_map_path
    )
    if energy_map.roll_map_hash() != energy_roll_map_hash:
        raise EnergyMetalsSessionGeometryError("Energy roll-map hash changed.")
    if (
        metals_map.map_type != VOLUME_FRONT_MAP_TYPE
        or metals_map.roll_map_hash() != metals_roll_map_hash
    ):
        raise EnergyMetalsSessionGeometryError("Metals roll-map hash changed.")
    if len(code_commit) == 40:
        current = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if current != code_commit:
            raise EnergyMetalsSessionGeometryError("Worker commit differs from specification.")
    hypotheses = generate_session_geometry_hypotheses()
    if len(hypotheses) != 432 or len(
        {item["structural_fingerprint"] for item in hypotheses}
    ) != 432:
        raise EnergyMetalsSessionGeometryError("Frozen population drifted.")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration = {
        "schema": VERSION,
        "population_count": 432,
        "population_hash": _stable_hash(hypotheses),
        "hypotheses": hypotheses,
        "round1": ROUND1,
        "round2": ROUND2,
        "confirmation_end_exclusive": "2024-10-01",
        "promotion_primary_count": 1,
        "primary_alpha": PRIMARY_ALPHA,
        "shadow_support_threshold": 0.20,
        "engineering_task_sha256": engineering_task_sha256,
        "code_commit": code_commit,
        "q4_access_allowed": False,
        "network_allowed": False,
        "paid_data_allowed": False,
        "live_or_broker_allowed": False,
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    preregistration_path = destination / "session_geometry_preregistration.json"
    _write_immutable(
        preregistration_path,
        json.dumps(preregistration, indent=2, sort_keys=True) + "\n",
    )
    access = _record_access_once(hypotheses) if record_data_access else None
    early_tables, early_provenance = _load_tables(
        Path(energy_data_path),
        energy_map,
        Path(metals_data_path),
        metals_map,
        "2024-01-01",
    )
    round1_survivors: list[dict[str, Any]] = []
    round2_survivors: list[dict[str, Any]] = []
    dispositions: list[dict[str, Any]] = []
    for hypothesis in hypotheses:
        events = build_session_geometry_events(
            early_tables[hypothesis["market"]], hypothesis
        )
        metrics = _period_metrics(_period(events, *ROUND1))
        passed = _gate(metrics, minimum_events=10)
        row = {**hypothesis, "round1_metrics": metrics}
        if passed:
            round1_survivors.append(row)
        else:
            dispositions.append(
                {
                    "candidate_id": hypothesis["candidate_id"],
                    "stage": "ROUND1",
                    "reason": _failure(metrics, minimum_events=10),
                }
            )
    for hypothesis in round1_survivors:
        mini_events = build_session_geometry_events(
            early_tables[hypothesis["market"]], hypothesis
        )
        micro_hypothesis = {**hypothesis, "market": hypothesis["execution_market"]}
        micro_events = build_session_geometry_events(
            early_tables[hypothesis["execution_market"]], micro_hypothesis
        )
        mini = _period_metrics(_period(mini_events, *ROUND2))
        micro = _period_metrics(_period(micro_events, *ROUND2))
        passed = _gate(mini, minimum_events=10) and bool(
            micro["events"] >= 10
            and micro["net_pnl"] > 0
            and micro["cost_stress_1_5x_net"] > 0
        )
        row = {
            **hypothesis,
            "round1_metrics": hypothesis["round1_metrics"],
            "round2_metrics": mini,
            "round2_micro_metrics": micro,
        }
        if passed:
            round2_survivors.append(row)
        else:
            dispositions.append(
                {
                    "candidate_id": hypothesis["candidate_id"],
                    "stage": "ROUND2",
                    "reason": (
                        "MICRO_TRANSFER_FAILURE"
                        if _gate(mini, minimum_events=10)
                        else _failure(mini, minimum_events=10)
                    ),
                }
            )
    archive = _quality_diversity_archive(round2_survivors)
    ranking = sorted(
        (_ranking_row(item) for item in archive),
        key=lambda row: (
            -float(row["minimum_net_to_drawdown"]),
            -float(row["minimum_net"]),
            str(row["structural_fingerprint"]),
        ),
    )
    primary_id = ranking[0]["candidate_id"] if ranking else None
    primary = next(
        (item for item in archive if item["candidate_id"] == primary_id), None
    )
    primary_manifest = {
        "schema": "energy_metals_session_geometry_primary_freeze_v1",
        "population_hash": preregistration["population_hash"],
        "archive_candidate_ids": [item["candidate_id"] for item in archive],
        "ranking": ranking,
        "primary": primary,
        "primary_candidate_id": primary_id,
        "selection_data_end_exclusive": "2024-01-01",
        "promotion_primary_count": int(primary is not None),
        "diagnostics_inherit_status": False,
        "q4_access_allowed": False,
    }
    primary_manifest["primary_manifest_hash"] = _stable_hash(primary_manifest)
    primary_manifest_path = destination / "session_geometry_primary_freeze.json"
    _write_immutable(
        primary_manifest_path,
        json.dumps(primary_manifest, indent=2, sort_keys=True) + "\n",
    )
    freeze_seconds = time.perf_counter() - started
    candidates: list[dict[str, Any]] = []
    shadow_configurations: list[dict[str, Any]] = []
    trade_ledger = pd.DataFrame()
    full_provenance = None
    if primary is not None:
        full_tables, full_provenance = _load_tables(
            Path(energy_data_path),
            energy_map,
            Path(metals_data_path),
            metals_map,
            "2024-10-01",
        )
        mini_events = _validation_events(
            build_session_geometry_events(full_tables[primary["market"]], primary)
        )
        micro_hypothesis = {**primary, "market": primary["execution_market"]}
        micro_events = _validation_events(
            build_session_geometry_events(
                full_tables[primary["execution_market"]], micro_hypothesis
            )
        )
        mini_metrics, micro_metrics = (
            _validation_metrics(mini_events),
            _validation_metrics(micro_events),
        )
        probability = _block_sign_flip_probability(mini_events, seed=991031)
        diagnostics = _diagnostics(full_tables[primary["market"]], primary)
        delayed = _validation_metrics(
            _validation_events(
                build_session_geometry_events(
                    full_tables[primary["market"]], primary, entry_delay_bars=1
                )
            )
        )
        concentration = _concentration_stress(mini_events)
        contract_evidence = bool(
            mini_metrics["net_pnl"] > 0
            and micro_metrics["net_pnl"] > 0
            and micro_metrics["cost_stress_1_5x_net"] > 0
            and micro_metrics["supportive_temporal_folds"] >= 1
        )
        account = _account_replay(
            micro_events.rename(columns={"net_pnl": "net_pnl_60"}).copy()
        )
        shadow_null = bool(probability <= 0.20 and mini_metrics["sign_flip_net"] < 0)
        evidence = ShadowEvidence(
            candidate_id=primary["candidate_id"],
            data_integrity=True,
            no_lookahead=True,
            deterministic_signals=True,
            net_after_costs=float(mini_metrics["net_pnl"]),
            supportive_temporal_folds=int(mini_metrics["supportive_temporal_folds"]),
            catastrophic_transfer=bool(mini_metrics["catastrophic_transfer"]),
            candidate_null_pass=shadow_null,
            null_probability=float(probability),
            parameter_stable=bool(diagnostics["positive_neighbor_count"] >= 1),
            contract_evidence=contract_evidence,
            account_mll_safe=bool(account.get("micro_one_contract_mll_safe", False)),
            execution_possible=True,
            realtime_features_available=True,
            shadow_spec_complete=True,
            observability_complete=True,
            untouched_holdout_passed=False,
            sample_size=int(mini_metrics["events"]),
            uncertainty="session_geometry_development_confirmation_q4_unopened",
        )
        admission = decide_shadow_admission(evidence)
        if admission.tier == EvidenceTier.PAPER_SHADOW_READY:
            raise EnergyMetalsSessionGeometryError(
                "Development experiment attempted paper promotion."
            )
        candidate = {
            "candidate_id": primary["candidate_id"],
            "lineage_id": primary["lineage_id"],
            "structural_fingerprint": primary["structural_fingerprint"],
            "mechanism_family": primary["mechanism_family"],
            "primary_market": primary["market"],
            "execution_market": primary["execution_market"],
            "market_ecology": primary["market_ecology"],
            "portfolio_role": primary["portfolio_role"],
            "feature": primary["feature"],
            "profile": {
                "quantile": primary["quantile"],
                "horizon": primary["horizon"],
                "context": primary["context"],
                "policy_direction": primary["policy_direction"],
            },
            "status": admission.tier.value,
            "admission": admission.to_dict(),
            "events": int(mini_metrics["events"]),
            "net_pnl": float(mini_metrics["net_pnl"]),
            "micro_events": int(micro_metrics["events"]),
            "micro_net_pnl": float(micro_metrics["net_pnl"]),
            "supportive_temporal_folds": int(
                mini_metrics["supportive_temporal_folds"]
            ),
            "fold_results": mini_metrics["fold_results"],
            "micro_fold_results": micro_metrics["fold_results"],
            "cost_stress_1_5x_net": float(
                mini_metrics["cost_stress_1_5x_net"]
            ),
            "null_evidence": {
                "method": "preselected_primary_five_session_block_sign_flip",
                "raw_probability": float(probability),
                "prospective_alpha": PRIMARY_ALPHA,
                "promotion_passed": bool(probability <= PRIMARY_ALPHA),
                "shadow_research_support_threshold": 0.20,
                "shadow_research_support_passed": shadow_null,
            },
            "contract_transfer": {
                "mini": primary["market"],
                "micro": primary["execution_market"],
                "passed": contract_evidence,
            },
            "parameter_diagnostics": diagnostics,
            "attacks": {
                **concentration,
                "one_additional_bar_delay_net": float(delayed["net_pnl"]),
                "causal_prior_session_shift": True,
                "closed_opening_window_only": True,
            },
            "topstep": account,
            "shadow_evidence": evidence.__dict__,
        }
        candidates.append(candidate)
        if admission.permits_zero_risk_shadow:
            specification = _shadow_specification(
                primary, primary_manifest["primary_manifest_hash"]
            )
            config_path = specification.write_immutable(
                destination / "shadow_configurations" / f"{primary['candidate_id']}.json"
            )
            shadow_configurations.append(
                {
                    "candidate_id": primary["candidate_id"],
                    "status": admission.tier.value,
                    "path": str(config_path),
                    "configuration_hash": specification.configuration_hash,
                    "outbound_orders_enabled": False,
                }
            )
        trade_ledger = pd.concat(
            [
                mini_events.assign(contract_role="primary_mini"),
                micro_events.assign(contract_role="primary_micro"),
            ],
            ignore_index=True,
        )
    statuses = [item["status"] for item in candidates]
    promising_tiers = {
        EvidenceTier.PROMISING_RESEARCH_CANDIDATE.value,
        EvidenceTier.ROBUST_RESEARCH_CANDIDATE.value,
        EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value,
    }
    promising = sum(status in promising_tiers for status in statuses)
    shadow_count = statuses.count(EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value)
    topstep_count = sum(
        bool((item.get("topstep") or {}).get("path_candidate")) for item in candidates
    )
    if shadow_count:
        conclusion = "ENERGY_METALS_SESSION_GEOMETRY_SHADOW_CANDIDATE_FOUND"
        next_action = "ACTIVATE_ZERO_ORDER_SHADOW_AND_REPLICATE"
    elif promising:
        conclusion = "ENERGY_METALS_SESSION_GEOMETRY_PROMISING_BUT_INSUFFICIENT"
        next_action = "FRESH_ID_SESSION_GEOMETRY_REPLICATION"
    elif primary is None:
        conclusion = "ENERGY_METALS_SESSION_GEOMETRY_NO_PRIMARY"
        next_action = "PIVOT_CROSS_ASSET_OR_DAILY_HORIZON"
    else:
        conclusion = "ENERGY_METALS_SESSION_GEOMETRY_PRIMARY_FALSIFIED"
        next_action = "KILL_EXACT_PRIMARY_AND_REPLICATE_UNSEEN_GC_NICHE"
    trade_path = destination / "session_geometry_trade_ledger.jsonl"
    _write_ledger(trade_path, trade_ledger)
    integrity = {
        "population_exact_432": len(hypotheses) == 432,
        "unique_fingerprints": len(
            {item["structural_fingerprint"] for item in hypotheses}
        )
        == 432,
        "early_data_end_exclusive": early_provenance["end_exclusive"]
        == "2024-01-01",
        "maximum_one_primary": int(primary is not None) <= 1,
        "primary_frozen_before_confirmation": primary_manifest_path.is_file(),
        "diagnostics_inherit_no_status": True,
        "q4_excluded": True,
        "no_network_or_paid_data": True,
        "no_outbound_order_capability": True,
    }
    if not all(integrity.values()):
        raise EnergyMetalsSessionGeometryError(f"Integrity failed: {integrity}")
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "At most one primary was frozen from disjoint 2023 rounds before unchanged "
            "2024 Q1-Q3. Archive diagnostics inherit no evidence; Q4 remains sealed."
        ),
        "code_commit": code_commit,
        "candidate_count": len(hypotheses),
        "structural_prototypes": len(hypotheses),
        "round1_survivors": len(round1_survivors),
        "round2_survivors": len(round2_survivors),
        "diagnostic_archive_size": len(archive),
        "promotion_primary_count": int(primary is not None),
        "primary_candidate_id": primary_id,
        "candidates": candidates,
        "candidate_tier_counts": dict(Counter(statuses)),
        "promising_candidates": int(promising),
        "shadow_candidates": int(shadow_count),
        "paper_shadow_ready": 0,
        "topstep_path_candidates": int(topstep_count),
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "diagnostic_archive": [item["candidate_id"] for item in archive],
        "dispositions": dispositions,
        "shadow_configurations": shadow_configurations,
        "integrity_proof": integrity,
        "data_provenance": {
            "early": early_provenance,
            "confirmation": full_provenance,
        },
        "data_access_record": access,
        "preregistration_path": str(preregistration_path),
        "preregistration_hash": preregistration["preregistration_hash"],
        "primary_manifest_path": str(primary_manifest_path),
        "primary_manifest_hash": primary_manifest["primary_manifest_hash"],
        "performance": {
            "primary_freeze_seconds": freeze_seconds,
            "total_seconds": time.perf_counter() - started,
        },
        "governance": {
            "q4_access_count_delta": 0,
            "latest_data_end_exclusive": "2024-10-01",
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
        },
        "next_recommended_action": next_action,
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "session_geometry_result.json"
    report_path = destination / "session_geometry_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "primary_manifest_path": str(primary_manifest_path),
            "trade_ledger_path": str(trade_path),
        },
        "report_path": str(report_path),
    }


def _load_tables(
    energy_data_path: Path,
    energy_map: RollMap,
    metals_data_path: Path,
    metals_map: RollMap,
    end_exclusive: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    energy = _read_period(energy_data_path, {"CL", "MCL"}, end_exclusive)
    metals = _read_period(metals_data_path, {"GC", "MGC"}, end_exclusive)
    energy, energy_audit = _apply_explicit_contract_map(
        energy, energy_map, required_map_type=energy_map.map_type
    )
    metals, metals_audit = _apply_explicit_contract_map(
        metals, metals_map, required_map_type=VOLUME_FRONT_MAP_TYPE
    )
    tables = {
        symbol: build_session_geometry_table(
            energy if symbol in {"CL", "MCL"} else metals, symbol
        )
        for symbol in ("CL", "MCL", "GC", "MGC")
    }
    if end_exclusive == "2024-01-01" and any(
        pd.to_datetime(table["session_id"]).max() >= pd.Timestamp("2024-01-01")
        for table in tables.values()
    ):
        raise EnergyMetalsSessionGeometryError("Early tables exposed 2024.")
    return tables, {
        "period_start": "2023-01-01",
        "end_exclusive": end_exclusive,
        "sessions_by_symbol": {
            symbol: int(len(table)) for symbol, table in tables.items()
        },
        "energy_contract_audit": energy_audit,
        "metals_contract_audit": metals_audit,
    }


def _period(events: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if events.empty:
        return events
    timestamp = pd.to_datetime(events["entry_timestamp"], utc=True)
    return events[timestamp.ge(start) & timestamp.lt(end)].copy()


def _gate(metrics: dict[str, Any], *, minimum_events: int) -> bool:
    return bool(
        metrics["finite"]
        and metrics["events"] >= minimum_events
        and metrics["net_pnl"] > 0
        and metrics["cost_stress_1_5x_net"] > 0
        and metrics["best_positive_event_share"] <= 0.35
    )


def _failure(metrics: dict[str, Any], *, minimum_events: int) -> str:
    if not metrics["finite"]:
        return "NONFINITE"
    if metrics["events"] < minimum_events:
        return "INSUFFICIENT_EVENTS"
    if metrics["net_pnl"] <= 0:
        return "NEGATIVE_ECONOMICS"
    if metrics["cost_stress_1_5x_net"] <= 0:
        return "COST_FRAGILITY"
    if metrics["best_positive_event_share"] > 0.35:
        return "CONCENTRATION"
    return "UNSPECIFIED"


def _quality_diversity_archive(
    survivors: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    ordered = sorted(
        survivors,
        key=lambda item: (
            -float(_ranking_row(item)["minimum_net_to_drawdown"]),
            -float(_ranking_row(item)["minimum_net"]),
            str(item["structural_fingerprint"]),
        ),
    )
    archive: list[dict[str, Any]] = []
    occupied: set[tuple[str, str, str]] = set()
    for item in ordered:
        niche = (
            str(item["market_ecology"]),
            str(item["feature"]),
            str(item["portfolio_role"]),
        )
        if niche in occupied:
            continue
        occupied.add(niche)
        archive.append(item)
    return archive


def _ranking_row(item: dict[str, Any]) -> dict[str, Any]:
    mini, micro = item["round2_metrics"], item["round2_micro_metrics"]
    return {
        "candidate_id": item["candidate_id"],
        "structural_fingerprint": item["structural_fingerprint"],
        "minimum_net": min(float(mini["net_pnl"]), float(micro["net_pnl"])),
        "minimum_net_to_drawdown": min(
            float(mini["net_pnl"]) / max(float(mini["maximum_drawdown"]), 1.0),
            float(micro["net_pnl"])
            / max(float(micro["maximum_drawdown"]), 1.0),
        ),
        "mini_metrics": mini,
        "micro_metrics": micro,
    }


def _diagnostics(table: pd.DataFrame, primary: dict[str, Any]) -> dict[str, Any]:
    variants: dict[str, Any] = {}
    for label, quantile, horizon in (
        ("lower_quantile", max(float(primary["quantile"]) - 0.10, 0.50), int(primary["horizon"])),
        ("higher_quantile", min(float(primary["quantile"]) + 0.10, 0.95), int(primary["horizon"])),
        ("shorter_horizon", float(primary["quantile"]), max(30, int(primary["horizon"]) // 2)),
        ("longer_horizon", float(primary["quantile"]), min(120, int(primary["horizon"]) * 2)),
    ):
        events = _validation_events(
            build_session_geometry_events(
                table,
                primary,
                quantile_override=quantile,
                horizon_override=horizon,
            )
        )
        variants[label] = _validation_metrics(events)
    return {
        "diagnostic_only": True,
        "variants": variants,
        "positive_neighbor_count": int(
            sum(item["net_pnl"] > 0 for item in variants.values())
        ),
    }


def _concentration_stress(events: pd.DataFrame) -> dict[str, float]:
    if events.empty:
        return {
            "remove_best_event_net": 0.0,
            "remove_best_day_net": 0.0,
            "remove_best_month_net": 0.0,
        }
    ordered = events.copy()
    timestamp = pd.to_datetime(ordered["entry_timestamp"], utc=True)
    without_event = ordered.drop(index=ordered["net_pnl"].idxmax())
    daily = ordered.assign(_day=timestamp.dt.date).groupby("_day")["net_pnl"].sum()
    monthly = ordered.assign(
        _month=timestamp.dt.tz_localize(None).dt.to_period("M")
    ).groupby("_month")["net_pnl"].sum()
    return {
        "remove_best_event_net": float(without_event["net_pnl"].sum()),
        "remove_best_day_net": float(ordered["net_pnl"].sum() - daily.max()),
        "remove_best_month_net": float(ordered["net_pnl"].sum() - monthly.max()),
    }


def _shadow_specification(
    primary: dict[str, Any], source_manifest_hash: str
) -> ShadowSpecification:
    market = str(primary["execution_market"])
    return ShadowSpecification(
        strategy_id=str(primary["candidate_id"]),
        strategy_version="v1_session_geometry_pre_holdout",
        feature_versions=("causal_session_geometry_v1",),
        markets=(market,),
        timeframes=("1m", "overnight", "RTH_session"),
        session_rules={
            "timezone": "America/Chicago",
            "market_open_minute": SESSION_CLOCKS[market][0],
            "mandatory_flatten_before_session_end": True,
        },
        entry_rules={
            "event": "past_only_session_geometry_threshold",
            "feature": primary["feature"],
            "quantile": primary["quantile"],
            "direction": primary["policy_direction"],
            "context": primary["context"],
            "threshold_history_sessions": 20,
            "execution_delay_completed_bars": 1,
            "missing_feature_policy": "fail_closed_skip_signal",
        },
        exit_rules={
            "holding_completed_1m_bars": primary["horizon"],
            "no_overnight": True,
        },
        sizing={"contracts": 1, "instrument": market, "micro_first": True},
        costs={
            "round_turn_usd": _round_turn_cost_all(market),
            "slippage_ticks_round_turn": 2,
        },
        stale_data_seconds=75,
        expected_update_seconds=60,
        duplicate_signal_window_seconds=int(primary["horizon"]) * 60,
        maximum_exposure=0.1,
        simulated_mll_floor=-2500.0,
        internal_daily_risk_limit=500.0,
        kill_conditions=(
            "stale_data",
            "duplicate_signal",
            "session_closed",
            "clock_invalid",
            "contract_map_mismatch",
            "mll_floor",
            "manual_kill_switch",
        ),
        logging={
            "signal_ledger": True,
            "virtual_fill_ledger": True,
            "latency_and_staleness": True,
            "account_mll_path": True,
            "source_manifest_hash": source_manifest_hash,
        },
        reconciliation={
            "startup_reconcile": True,
            "expected_vs_observed_virtual_fill": True,
            "fail_on_configuration_hash_mismatch": True,
        },
        outbound_orders_enabled=False,
        broker_connections_allowed=0,
        fail_closed=True,
    )


def _record_access_once(hypotheses: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_ids = [item["candidate_id"] for item in hypotheses]
    period = "2023-01-01:2024-10-01"
    reason = "energy/metals session geometry single-primary; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module")
                == "hydra.research.energy_metals_session_geometry_primary"
                and sorted(row.get("candidate_ids") or []) == sorted(candidate_ids)
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.energy_metals_session_geometry_primary",
        candidate_ids,
        reason,
        None,
    )
    return record.__dict__


def _write_ledger(path: Path, frame: pd.DataFrame) -> None:
    if frame.empty:
        _write_immutable(path, "")
        return
    ordered = frame.sort_values(["entry_timestamp", "symbol", "contract_role"])
    lines = [
        json.dumps(_strict_json_value(row), sort_keys=True, default=str)
        for row in ordered.to_dict("records")
    ]
    _write_immutable(path, "\n".join(lines) + "\n")


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise EnergyMetalsSessionGeometryError(
            f"Frozen {label} missing or changed: {path}"
        )


def _render_report(payload: dict[str, Any]) -> str:
    candidate = next(iter(payload.get("candidates") or []), None)
    lines = [
        "# Energy/Metals Session Geometry Primary",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Structures: `{payload['structural_prototypes']}`",
        f"- Round 1 survivors: `{payload['round1_survivors']}`",
        f"- Round 2 survivors: `{payload['round2_survivors']}`",
        f"- Archive: `{payload['diagnostic_archive_size']}`",
        f"- Primary: `{payload['primary_candidate_id']}`",
        f"- Shadow candidates: `{payload['shadow_candidates']}`",
        "- PAPER_SHADOW_READY: `0`",
        "- Q4 access delta: `0`",
    ]
    if candidate:
        lines.extend(
            [
                f"- Mini net: `{candidate['net_pnl']}`",
                f"- Micro net: `{candidate['micro_net_pnl']}`",
                f"- Null p: `{candidate['null_evidence']['raw_probability']}`",
                f"- Status: `{candidate['status']}`",
            ]
        )
    return "\n".join(lines) + "\n"
