from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.execution.v7_cost_model import CostStress, V7CostModel, load_cost_model
from hydra.features.feature_matrix import FeatureMatrix
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.markets.instruments import instrument_spec
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.mll_variants import MllMode
from hydra.propfirm.rolling_combine import (
    EpisodeStartPolicy,
    RollingCombineSummary,
    evaluate_rolling_combine,
)
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.research.v7_hypothesis_grammar import (
    MARKETS,
    V7CandidateSpec,
    V7MarketBars,
    V7Signal,
    candidate_specs,
    generate_signal_population,
    load_v7_market_bars,
)
from hydra.validation.v7_null_tripwire import (
    NullControl,
    SyntheticMarketPath,
    build_synthetic_market_path,
)
from hydra.validation.v7_phase2_multiplicity import (
    benjamini_hochberg,
    deflated_sharpe_statistics,
)


PREREGISTRATION_SHA256 = (
    "1adeab25abb0f75f067caa523c499536c85d66b1811a500d32a3b6caf30a74cc"
)
SIGNAL_MANIFEST_FILE_SHA256 = (
    "b0babdf95de791b24c65dc9fe9def54ecde87d95b6fbfc9038cb24d2c61a66a2"
)
CONTRACT_SHA256 = (
    "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
)
N_TRIALS = 246_946
MINUTE_NS = 60_000_000_000
FDR_Q = 0.10
NULL_THRESHOLD = 0.8
CONTROL_SEEDS = {
    NullControl.DAILY_BLOCK_SHUFFLE: {
        "ES": 74101,
        "NQ": 74102,
        "RTY": 74103,
        "YM": 74104,
        "GC": 74105,
        "CL": 74106,
    },
    NullControl.VOLATILITY_MATCHED_RANDOM_WALK: {
        "ES": 74201,
        "NQ": 74202,
        "RTY": 74203,
        "YM": 74204,
        "GC": 74205,
        "CL": 74206,
    },
    NullControl.YEAR_BLOCK_PERMUTATION: {
        "ES": 74301,
        "NQ": 74302,
        "RTY": 74303,
        "YM": 74304,
        "GC": 74305,
        "CL": 74306,
    },
}


class GrammarValidationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CandidateEvents:
    spec: V7CandidateSpec
    base: tuple[TradePathEvent, ...]
    stress_1_5x: tuple[TradePathEvent, ...]
    stress_2x: tuple[TradePathEvent, ...]
    eligible_days: tuple[int, ...]


def horizon_cost_bucket(holding_minutes: int) -> str:
    if holding_minutes <= 1:
        return "1m"
    if holding_minutes <= 5:
        return "5m"
    if holding_minutes <= 15:
        return "15m"
    if holding_minutes <= 30:
        return "30m"
    if holding_minutes <= 60:
        return "60m"
    return "session"


def signal_to_event(
    signal: V7Signal,
    spec: V7CandidateSpec,
    bars: V7MarketBars,
    cost_model: V7CostModel,
    *,
    stress: CostStress,
) -> TradePathEvent:
    if signal.candidate_id != spec.candidate_id or signal.market != spec.market:
        raise GrammarValidationError("signal/spec identity mismatch")
    if signal.entry_index < 0 or signal.exit_index >= bars.row_count:
        raise GrammarValidationError("signal index is outside the market matrix")
    if signal.contract_code != int(bars.contract_code[signal.entry_index]):
        raise GrammarValidationError("signal explicit contract changed")
    if signal.segment_code != int(bars.segment_code[signal.entry_index]):
        raise GrammarValidationError("signal segment changed")
    positions = slice(signal.entry_index, signal.exit_index + 1)
    if not (
        np.all(
            bars.contract_code[positions]
            == bars.contract_code[signal.entry_index]
        )
        and np.all(
            bars.segment_code[positions] == bars.segment_code[signal.entry_index]
        )
        and np.all(np.diff(bars.timestamp_ns[positions]) == MINUTE_NS)
    ):
        raise GrammarValidationError("event crosses contract, segment, or gap")
    entry = float(bars.open[signal.entry_index])
    exit_price = float(bars.close[signal.exit_index])
    if not math.isfinite(entry) or not math.isfinite(exit_price):
        raise GrammarValidationError("event has nonfinite execution price")
    point_value = instrument_spec(spec.market).point_value
    scale = float(signal.side * point_value)
    gross = (exit_price - entry) * scale
    cost = cost_model.round_turn_cost(
        spec.market,
        horizon_cost_bucket(spec.holding_minutes),
        stress=stress,
        contracts=1.0,
    )
    path_high = float(np.max(bars.high[positions]))
    path_low = float(np.min(bars.low[positions]))
    adverse_price = path_low if signal.side > 0 else path_high
    favorable_price = path_high if signal.side > 0 else path_low
    adverse = (adverse_price - entry) * scale - cost
    favorable = (favorable_price - entry) * scale - cost
    net = gross - cost
    return TradePathEvent(
        event_id=(
            f"{spec.candidate_id}:{signal.session_day}:{signal.decision_ns}:"
            f"{stress.value}"
        ),
        decision_ns=signal.decision_ns,
        exit_ns=signal.exit_ns,
        session_day=signal.session_day,
        net_pnl=float(net),
        gross_pnl=float(gross),
        worst_unrealized_pnl=float(min(adverse, net, 0.0)),
        best_unrealized_pnl=float(max(favorable, net, 0.0)),
        quantity=1,
        mini_equivalent=1.0,
        regime=spec.hypothesis_id,
        session_compliant=bool(
            int(bars.local_minute[signal.exit_index]) <= 15 * 60 + 9
        ),
        contract_limit_compliant=True,
        same_bar_ambiguous=bool(adverse < 0.0 < favorable),
    )


def null_ratio_verdict(
    real_pass_rate: float, pooled_null_pass_rate: float
) -> tuple[str, float | None]:
    if real_pass_rate <= 0.0:
        return "BLOCKED_UNDERPOWERED_GRAMMAR_TRIPWIRE", None
    ratio = pooled_null_pass_rate / real_pass_rate
    if ratio >= NULL_THRESHOLD:
        return "ARTEFACT_GEOMETRY_ONLY_KILL_GRAMMAR", ratio
    return "GREEN_NULL_ADJUSTED_BASELINE", ratio


def run_grammar_validation(
    *,
    project_root: str | Path,
    preregistration_path: str | Path,
    signal_manifest_path: str | Path,
    proof_registry_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    preregistration = Path(preregistration_path).resolve()
    signal_manifest = Path(signal_manifest_path).resolve()
    _verify_inputs(root, preregistration, signal_manifest, proof_registry_path)
    bars = load_v7_market_bars(root)
    signals = generate_signal_population(
        bars, graveyard_path=root / "mission/state/graveyard.db"
    )
    _verify_signal_manifest(signal_manifest, signals)
    specs = {row.candidate_id: row for row in candidate_specs()}
    cost_model = load_cost_model()
    real_events = _build_candidate_events(specs, signals, bars, cost_model)

    candidate_rows: list[dict[str, Any]] = []
    p_values: dict[str, float] = {}
    real_combine: dict[str, RollingCombineSummary] = {}
    for candidate_id in sorted(specs):
        bundle = real_events[candidate_id]
        base_metrics = _event_metrics(bundle.base, bundle.eligible_days)
        stress_1_5 = _event_metrics(bundle.stress_1_5x, bundle.eligible_days)
        stress_2 = _event_metrics(bundle.stress_2x, bundle.eligible_days)
        stage1_pass = (
            int(base_metrics["event_count"]) >= 30
            and float(base_metrics["expectancy_per_trade"]) > 0.0
        )
        trajectory_compliance = _event_compliance(bundle.base)
        stage2_pass = (
            stage1_pass
            and float(stress_1_5["expectancy_per_trade"]) > 0.0
            and float(stress_2["expectancy_per_trade"]) > 0.0
            and trajectory_compliance
        )
        if stage2_pass:
            walk_forward = _walk_forward(bundle.stress_1_5x, bundle.eligible_days)
            daily = _daily_vector(bundle.stress_1_5x, bundle.eligible_days)
            dsr = deflated_sharpe_statistics(daily, n_trials=N_TRIALS)
            p_values[candidate_id] = float(dsr["one_sided_p_value"])
        else:
            walk_forward = _empty_walk_forward()
            dsr = {
                "observations": len(bundle.eligible_days),
                "sample_sharpe_daily": 0.0,
                "sample_sharpe_annualized": 0.0,
                "expected_max_sharpe_daily": None,
                "expected_max_sharpe_annualized": None,
                "skewness": None,
                "pearson_kurtosis": None,
                "deflated_z": -1.0e12,
                "DSR_probability": 0.0,
                "one_sided_p_value": 1.0,
                "not_run_reason": "killed_before_walk_forward",
            }
            p_values[candidate_id] = 1.0
        default_combine = evaluate_rolling_combine(
            bundle.base,
            bundle.eligible_days,
            policy=_episode_policy(),
            config=Topstep150KConfig(mll_mode=MllMode.EOD_LEVEL_RT_BREACH),
        )
        intraday_combine = evaluate_rolling_combine(
            bundle.base,
            bundle.eligible_days,
            policy=_episode_policy(),
            config=Topstep150KConfig(mll_mode=MllMode.INTRADAY_HWM),
        )
        dll_combine = evaluate_rolling_combine(
            bundle.base,
            bundle.eligible_days,
            policy=_episode_policy(),
            config=Topstep150KConfig(
                mll_mode=MllMode.EOD_LEVEL_RT_BREACH,
                use_optional_daily_loss_limit=True,
            ),
        )
        real_combine[candidate_id] = default_combine
        candidate_rows.append(
            {
                "candidate_id": candidate_id,
                "specification": specs[candidate_id].to_dict(),
                "signal_count": len(signals[candidate_id]),
                "stage0_valid": True,
                "stage1_pass": stage1_pass,
                "stage2_pass": stage2_pass,
                "base": base_metrics,
                "stress_1_5x": stress_1_5,
                "stress_2x": stress_2,
                "SIM_EXPLOIT": float(stress_2["expectancy_per_trade"]) <= 0.0,
                "trajectory_compliance": trajectory_compliance,
                "walk_forward": walk_forward,
                "DSR": dsr,
                "combine_diagnostic_not_fitness": {
                    "eod_level_rt_breach": _compact_combine(default_combine),
                    "intraday_hwm": _compact_combine(intraday_combine),
                    "optional_DLL_3000": _compact_combine(dll_combine),
                },
            }
        )

    bh = benjamini_hochberg(p_values, q=FDR_Q)
    tripwire = _run_full_signal_tripwire(
        root=root,
        specs=specs,
        real_bars=bars,
        real_combine=real_combine,
        cost_model=cost_model,
    )
    for row in candidate_rows:
        candidate_id = str(row["candidate_id"])
        row["BH"] = bh[candidate_id]
        gates = {
            "stage1_minimum_events_and_base_expectancy": bool(row["stage1_pass"]),
            "cost_1_5x_positive": float(
                row["stress_1_5x"]["expectancy_per_trade"]
            )
            > 0.0,
            "SIM_EXPLOIT_2x_survived": not bool(row["SIM_EXPLOIT"]),
            "ruleset_trajectory_compliance": bool(row["trajectory_compliance"]),
            "walk_forward_1_5x_positive": float(
                row["walk_forward"]["pooled_expectancy_per_trade"]
            )
            > 0.0,
            "DSR_deflated_z_gt_0": float(row["DSR"]["deflated_z"]) > 0.0,
            "BH_FDR_10pct_rejected": bool(row["BH"]["rejected"]),
            "grammar_tripwire_GREEN": tripwire["verdict"]
            == "GREEN_NULL_ADJUSTED_BASELINE",
        }
        row["promotion_gates"] = gates
        row["shadow_queue_eligible"] = all(gates.values())

    selected = _select_shadow_queue(candidate_rows, maximum=5)
    early_killed = sum(not bool(row["stage2_pass"]) for row in candidate_rows)
    final_verdict = (
        "ARTEFACT"
        if tripwire["verdict"] == "ARTEFACT_GEOMETRY_ONLY_KILL_GRAMMAR"
        else "BLOCKED"
        if tripwire["verdict"] == "BLOCKED_UNDERPOWERED_GRAMMAR_TRIPWIRE"
        else "GREEN"
        if selected
        else "NULL"
    )
    result: dict[str, Any] = {
        "schema": "hydra_v7_grammar_0001_validation_result_v1",
        "grammar_id": "hydra_v7_grammar_0001_scheduled_inventory_and_hazard",
        "verdict": final_verdict,
        "preregistration_sha256": PREREGISTRATION_SHA256,
        "signal_manifest_file_sha256": SIGNAL_MANIFEST_FILE_SHA256,
        "N_trials": N_TRIALS,
        "candidate_count": len(candidate_rows),
        "signal_count": sum(int(row["signal_count"]) for row in candidate_rows),
        "stage1_survivor_count": sum(bool(row["stage1_pass"]) for row in candidate_rows),
        "stage2_survivor_count": sum(bool(row["stage2_pass"]) for row in candidate_rows),
        "killed_before_walk_forward_count": early_killed,
        "kill_before_walk_forward_rate": early_killed / len(candidate_rows),
        "DSR_positive_count": sum(
            float(row["DSR"]["deflated_z"]) > 0.0 for row in candidate_rows
        ),
        "BH_rejection_count": sum(bool(row["BH"]["rejected"]) for row in candidate_rows),
        "SIM_EXPLOIT_count": sum(bool(row["SIM_EXPLOIT"]) for row in candidate_rows),
        "tripwire": tripwire,
        "selected_shadow_queue_candidate_ids": selected,
        "candidate_results": candidate_rows,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "phase_data_spend_usd": 0.0,
        "outbound_order_count": 0,
        "combine_pass_rate_used_as_fitness": False,
        "diff_validation": [
            "hydra/validation/v7_grammar_0001_validation.py",
            "scripts/run_v7_grammar_0001_validation.py",
            "tests/test_v7_grammar_0001_validation.py",
        ],
        "CONTRE": (
            "The grammar encodes only 24 fixed structures and most have low event "
            "counts; a blocked or null verdict may reflect insufficient opportunity "
            "density as much as absence of the stated mechanisms. Thresholds remain "
            "frozen and the next response must be a new economic hypothesis."
        ),
        "prochaine_action": (
            "freeze_candidate_fiches_WORM_before_gap_ingestion"
            if selected
            else "tombstone_grammar_classes_and_preregister_grammar_0002"
        ),
    }
    return _write_result(result, output_dir)


def _verify_inputs(
    root: Path,
    preregistration: Path,
    signal_manifest: Path,
    proof_registry_path: str | Path,
) -> None:
    if _sha256(preregistration) != PREREGISTRATION_SHA256:
        raise GrammarValidationError("grammar WORM hash mismatch")
    if _sha256(root / "MISSION_CONTRACT.md") != CONTRACT_SHA256:
        raise GrammarValidationError("mission contract hash mismatch")
    if _sha256(signal_manifest) != SIGNAL_MANIFEST_FILE_SHA256:
        raise GrammarValidationError("outcome-free signal manifest hash mismatch")
    proof = load_and_verify(proof_registry_path)
    if multiplicity_trial_count(proof) != N_TRIALS:
        raise GrammarValidationError("multiplicity reservation mismatch")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise GrammarValidationError("unexpected proof window state")
    if json.loads((root / "reports/v7/phase1/g1_result.json").read_text())["verdict"] != "GREEN":
        raise GrammarValidationError("G1 is not GREEN")
    if json.loads((root / "reports/v7/phase2/phase2_result.json").read_text())["verdict"] != "NULL":
        raise GrammarValidationError("inherited Phase 2 track is not closed")


def _verify_signal_manifest(
    path: Path, signals: Mapping[str, Sequence[V7Signal]]
) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = str(payload.get("manifest_hash") or "")
    unhashed = dict(payload)
    unhashed.pop("manifest_hash", None)
    if expected != _stable_hash(unhashed):
        raise GrammarValidationError("signal manifest logical hash mismatch")
    regenerated = {
        candidate_id: [row.to_dict() for row in rows]
        for candidate_id, rows in signals.items()
    }
    if regenerated != payload["signals"]:
        raise GrammarValidationError("signal regeneration differs from frozen manifest")
    if payload.get("contains_outcomes_or_pnl") is not False:
        raise GrammarValidationError("signal manifest contains forbidden outcomes")


def _build_candidate_events(
    specs: Mapping[str, V7CandidateSpec],
    signals: Mapping[str, Sequence[V7Signal]],
    bars: Mapping[str, V7MarketBars],
    cost_model: V7CostModel,
) -> dict[str, CandidateEvents]:
    output: dict[str, CandidateEvents] = {}
    for candidate_id, spec in sorted(specs.items()):
        market_bars = bars[spec.market]
        eligible_days = tuple(sorted({int(value) for value in market_bars.session_day}))
        output[candidate_id] = CandidateEvents(
            spec=spec,
            base=tuple(
                signal_to_event(row, spec, market_bars, cost_model, stress=CostStress.BASE)
                for row in signals[candidate_id]
            ),
            stress_1_5x=tuple(
                signal_to_event(
                    row, spec, market_bars, cost_model, stress=CostStress.STRESS_1_5X
                )
                for row in signals[candidate_id]
            ),
            stress_2x=tuple(
                signal_to_event(
                    row, spec, market_bars, cost_model, stress=CostStress.STRESS_2X
                )
                for row in signals[candidate_id]
            ),
            eligible_days=eligible_days,
        )
    return output


def _run_full_signal_tripwire(
    *,
    root: Path,
    specs: Mapping[str, V7CandidateSpec],
    real_bars: Mapping[str, V7MarketBars],
    real_combine: Mapping[str, RollingCombineSummary],
    cost_model: V7CostModel,
) -> dict[str, Any]:
    matrices = _load_feature_matrices(root)
    real_episodes = sum(row.episode_start_count for row in real_combine.values())
    real_passes = sum(row.pass_count for row in real_combine.values())
    controls: dict[str, Any] = {}
    pooled_episodes = 0
    pooled_passes = 0
    for control in NullControl:
        synthetic_paths = {
            market: build_synthetic_market_path(
                market,
                matrices[market],
                control=control,
                seed=CONTROL_SEEDS[control][market],
                block_size=5,
            )
            for market in MARKETS
        }
        synthetic_bars = {
            market: _synthetic_market_bars(real_bars[market], path)
            for market, path in synthetic_paths.items()
        }
        synthetic_signals = generate_signal_population(
            synthetic_bars, graveyard_path=root / "mission/state/graveyard.db"
        )
        synthetic_events = _build_candidate_events(
            specs, synthetic_signals, synthetic_bars, cost_model
        )
        episode_count = pass_count = breach_count = 0
        per_candidate: dict[str, Any] = {}
        for candidate_id in sorted(specs):
            bundle = synthetic_events[candidate_id]
            summary = evaluate_rolling_combine(
                bundle.base,
                bundle.eligible_days,
                policy=_episode_policy(),
                config=Topstep150KConfig(
                    mll_mode=MllMode.EOD_LEVEL_RT_BREACH
                ),
            )
            episode_count += summary.episode_start_count
            pass_count += summary.pass_count
            breach_count += summary.mll_breach_count
            per_candidate[candidate_id] = {
                "signal_count": len(synthetic_signals[candidate_id]),
                **_compact_combine(summary),
            }
        pooled_episodes += episode_count
        pooled_passes += pass_count
        controls[control.value] = {
            "episode_count": episode_count,
            "pass_count": pass_count,
            "pass_rate": pass_count / max(episode_count, 1),
            "mll_breach_count": breach_count,
            "mll_breach_rate": breach_count / max(episode_count, 1),
            "signal_count": sum(
                len(rows) for rows in synthetic_signals.values()
            ),
            "path_hashes": {
                market: path.path_hash
                for market, path in sorted(synthetic_paths.items())
            },
            "candidate_results": per_candidate,
        }
    real_rate = real_passes / max(real_episodes, 1)
    null_rate = pooled_passes / max(pooled_episodes, 1)
    verdict, ratio = null_ratio_verdict(real_rate, null_rate)
    return {
        "verdict": verdict,
        "threshold": NULL_THRESHOLD,
        "NULL_RATIO": ratio,
        "feature_and_signal_recomputation": True,
        "real": {
            "episode_count": real_episodes,
            "pass_count": real_passes,
            "pass_rate": real_rate,
            "mll_breach_count": sum(
                row.mll_breach_count for row in real_combine.values()
            ),
            "signal_count": sum(row.event_count for row in real_combine.values()),
        },
        "pooled_null": {
            "episode_count": pooled_episodes,
            "pass_count": pooled_passes,
            "pass_rate": null_rate,
        },
        "controls": controls,
        "combine_pass_rate_is_diagnostic_not_fitness": True,
    }


def _load_feature_matrices(root: Path) -> dict[str, FeatureMatrix]:
    manifest = json.loads((root / "data/manifest.json").read_text(encoding="utf-8"))
    paths: dict[str, Path] = {}
    for artifact in manifest["artifacts"]:
        if artifact.get("kind") != "CANONICAL_FEATURE_MANIFEST":
            continue
        path = root / str(artifact["path"])
        payload = json.loads(path.read_text(encoding="utf-8"))
        key = payload.get("key", {})
        market = str(key.get("market") or "")
        if (
            market in MARKETS
            and key.get("transformation_version")
            == "hydra_turbo_feature_bundle_v3_risk_path"
            and key.get("end_exclusive") == "2024-10-01"
        ):
            paths[market] = path.parent
    if set(paths) != set(MARKETS):
        raise GrammarValidationError("canonical V3 matrix scope is incomplete")
    return {
        market: FeatureMatrix.open(path, mmap=True)
        for market, path in sorted(paths.items())
    }


def _synthetic_market_bars(
    real: V7MarketBars, path: SyntheticMarketPath
) -> V7MarketBars:
    raw_close = np.asarray(path.close, dtype=np.float64)
    raw_high = np.asarray(path.high, dtype=np.float64)
    raw_low = np.asarray(path.low, dtype=np.float64)
    close = np.empty_like(raw_close)
    high = np.empty_like(raw_high)
    low = np.empty_like(raw_low)
    open_ = np.empty_like(raw_close)
    tick = instrument_spec(real.market).tick_size
    days, starts, counts = np.unique(
        real.session_day, return_index=True, return_counts=True
    )
    for _day, raw_start, raw_count in zip(days, starts, counts, strict=True):
        start = int(raw_start)
        end = int(raw_start + raw_count)
        positions = slice(start, end)
        # Phase-1 paths are return-like and may be centred around zero.  V7
        # hypotheses use ratios, so anchor every trading session to its real
        # pre-null opening level without changing any within-session increment.
        offset = float(real.open[start] - raw_close[start])
        minimum = float(np.min(raw_low[positions] + offset))
        if minimum <= tick:
            offset += (2.0 * tick) - minimum
        close[positions] = raw_close[positions] + offset
        high[positions] = raw_high[positions] + offset
        low[positions] = raw_low[positions] + offset
        open_[start] = float(real.open[start])
        for index in range(start + 1, end):
            contiguous = bool(
                real.segment_code[index] == real.segment_code[index - 1]
                and real.contract_code[index] == real.contract_code[index - 1]
                and real.timestamp_ns[index] - real.timestamp_ns[index - 1]
                == MINUTE_NS
            )
            open_[index] = close[index - 1] if contiguous else close[index]
    high = np.maximum(high, np.maximum(open_, close))
    low = np.minimum(low, np.minimum(open_, close))
    if not (
        np.all(np.isfinite(open_))
        and np.all(np.isfinite(high))
        and np.all(np.isfinite(low))
        and np.all(np.isfinite(close))
        and np.all(open_ > 0.0)
        and np.all(close > 0.0)
    ):
        raise GrammarValidationError("synthetic price rebasing failed")
    return V7MarketBars(
        market=real.market,
        timestamp_ns=real.timestamp_ns,
        decision_ns=real.decision_ns,
        availability_ns=real.availability_ns,
        session_day=real.session_day,
        contract_code=real.contract_code,
        segment_code=real.segment_code,
        open=open_,
        high=high,
        low=low,
        close=close,
        local_minute=real.local_minute,
        local_weekday=real.local_weekday,
        bundle_hash=path.path_hash,
    )


def _event_metrics(
    events: Sequence[TradePathEvent], eligible_days: Sequence[int]
) -> dict[str, Any]:
    gross = float(sum(row.gross_pnl for row in events))
    net = float(sum(row.net_pnl for row in events))
    count = len(events)
    daily = _daily_vector(events, eligible_days)
    cumulative = np.cumsum(daily)
    peak = np.maximum.accumulate(np.concatenate(([0.0], cumulative)))[:-1]
    drawdown = float(np.max(peak - cumulative)) if len(cumulative) else 0.0
    return {
        "event_count": count,
        "gross_pnl": gross,
        "net_pnl": net,
        "expectancy_per_trade": net / max(count, 1),
        "maximum_drawdown": drawdown,
        "positive_day_count": int(np.sum(daily > 0.0)),
        "negative_day_count": int(np.sum(daily < 0.0)),
        "same_bar_ambiguous_count": sum(row.same_bar_ambiguous for row in events),
    }


def _daily_vector(
    events: Sequence[TradePathEvent], eligible_days: Sequence[int]
) -> np.ndarray:
    index = {int(day): position for position, day in enumerate(eligible_days)}
    values = np.zeros(len(eligible_days), dtype=np.float64)
    for event in events:
        if event.session_day not in index:
            raise GrammarValidationError("event session day is not eligible")
        values[index[event.session_day]] += float(event.net_pnl)
    return values


def _event_compliance(events: Sequence[TradePathEvent]) -> bool:
    ordered = sorted(events, key=lambda row: (row.decision_ns, row.event_id))
    overlap = any(
        right.decision_ns < left.exit_ns
        for left, right in zip(ordered, ordered[1:], strict=False)
    )
    return bool(
        not overlap
        and all(row.session_compliant for row in events)
        and all(row.contract_limit_compliant for row in events)
        and all(row.quantity == 1 and row.mini_equivalent == 1.0 for row in events)
    )


def _walk_forward(
    events: Sequence[TradePathEvent], eligible_days: Sequence[int]
) -> dict[str, Any]:
    folds = np.array_split(np.asarray(eligible_days, dtype=np.int64), 4)
    output: list[dict[str, Any]] = []
    total_net = 0.0
    total_events = 0
    for index, raw in enumerate(folds):
        retained = raw if index == 0 else raw[5:]
        day_set = set(int(value) for value in retained)
        selected = [row for row in events if row.session_day in day_set]
        net = float(sum(row.net_pnl for row in selected))
        total_net += net
        total_events += len(selected)
        output.append(
            {
                "fold": index + 1,
                "raw_session_days": len(raw),
                "embargo_days": 0 if index == 0 else 5,
                "retained_session_days": len(retained),
                "event_count": len(selected),
                "net_pnl": net,
                "expectancy_per_trade": net / max(len(selected), 1),
            }
        )
    return {
        "folds": output,
        "retained_event_count": total_events,
        "pooled_net_pnl": total_net,
        "pooled_expectancy_per_trade": total_net / max(total_events, 1),
        "purge": "exact_holding_horizon_no_cross_boundary_event_observed",
        "embargo_days": 5,
    }


def _empty_walk_forward() -> dict[str, Any]:
    return {
        "folds": [],
        "retained_event_count": 0,
        "pooled_net_pnl": 0.0,
        "pooled_expectancy_per_trade": 0.0,
        "not_run_reason": "killed_before_walk_forward",
        "purge": "not_run",
        "embargo_days": 5,
    }


def _episode_policy() -> EpisodeStartPolicy:
    return EpisodeStartPolicy(
        maximum_starts=24,
        minimum_spacing_sessions=10,
        minimum_observation_sessions=30,
        maximum_duration_sessions=60,
        regime_balanced=False,
    )


def _compact_combine(summary: RollingCombineSummary) -> dict[str, Any]:
    return {
        "episode_start_count": summary.episode_start_count,
        "effective_block_count": summary.effective_block_count,
        "pass_count": summary.pass_count,
        "pass_rate": summary.pass_rate,
        "mll_breach_count": summary.mll_breach_count,
        "mll_breach_rate": summary.mll_breach_rate,
        "timeout_count": summary.timeout_count,
        "timeout_rate": summary.timeout_rate,
        "compliance_failure_count": summary.compliance_failure_count,
        "median_days_to_target": summary.median_days_to_target,
        "median_target_progress_when_not_passed": summary.median_target_progress_when_not_passed,
        "minimum_mll_buffer": summary.minimum_mll_buffer,
        "consistency_pass_rate": summary.consistency_pass_rate,
        "event_count": summary.event_count,
    }


def _select_shadow_queue(
    rows: Sequence[Mapping[str, Any]], *, maximum: int
) -> list[str]:
    eligible = [row for row in rows if bool(row["shadow_queue_eligible"])]
    eligible.sort(
        key=lambda row: (
            -float(row["DSR"]["deflated_z"]),
            -float(row["walk_forward"]["pooled_expectancy_per_trade"]),
            float(row["stress_2x"]["maximum_drawdown"]),
            str(row["candidate_id"]),
        )
    )
    selected: list[str] = []
    mechanisms: set[str] = set()
    for row in eligible:
        mechanism = str(row["specification"]["mechanism_class"])
        if mechanism in mechanisms:
            continue
        selected.append(str(row["candidate_id"]))
        mechanisms.add(mechanism)
        if len(selected) >= maximum:
            break
    return selected


def _write_result(result: dict[str, Any], output_dir: str | Path) -> dict[str, Any]:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "grammar0001_validation_result.json"
    report_path = destination / "grammar0001_validation_report.md"
    temporary = result_path.with_name(result_path.name + ".tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    report_path.write_text(_render_report(result), encoding="utf-8")
    result["result_path"] = str(result_path)
    result["result_sha256"] = _sha256(result_path)
    result["report_path"] = str(report_path)
    return result


def _render_report(result: Mapping[str, Any]) -> str:
    tripwire = result["tripwire"]
    lines = [
        "# HYDRA V7 — Validation grammaire 0001",
        "",
        f"[HYDRA-V7] phase=4 step=3 verdict={result['verdict']}",
        "gate=GRAMMAR_0001 preuve=reports/v7/phase4/grammar0001_validation_result.json#pending tests=pending",
        f"budget_llm=0.000000/solde budget_data=0.000000/60.00 N_trials={result['N_trials']} burned=1",
        "diff_validation=hydra/validation/v7_grammar_0001_validation.py,scripts/run_v7_grammar_0001_validation.py,tests/test_v7_grammar_0001_validation.py CONTRE=la_grammaire_ne_contient_que_24_structures_et_peut_etre_sous_puissante",
        f"prochaine_action={result['prochaine_action']}",
        "",
        "## Tripwire en tête",
        "",
        f"- Verdict : `{tripwire['verdict']}`",
        f"- Pass-rate réel : `{tripwire['real']['pass_rate']:.6f}`",
        f"- Pass-rate null poolé : `{tripwire['pooled_null']['pass_rate']:.6f}`",
        f"- NULL_RATIO : `{tripwire['NULL_RATIO']}`",
        "- Le pass-rate est diagnostique uniquement, jamais une fitness.",
        "",
        "## Funnel",
        "",
        f"- Structures : `{result['candidate_count']}`",
        f"- Signaux : `{result['signal_count']}`",
        f"- Survivants Stage 1 : `{result['stage1_survivor_count']}`",
        f"- Survivants Stage 2 : `{result['stage2_survivor_count']}`",
        f"- Tués avant walk-forward : `{result['killed_before_walk_forward_count']}` (`{result['kill_before_walk_forward_rate']:.2%}`)",
        f"- DSR positifs : `{result['DSR_positive_count']}`",
        f"- Rejets BH : `{result['BH_rejection_count']}`",
        f"- SIM_EXPLOIT : `{result['SIM_EXPLOIT_count']}`",
        f"- File shadow : `{len(result['selected_shadow_queue_candidate_ids'])}`",
        "",
        "## CONTRE",
        "",
        str(result["CONTRE"]),
        "",
    ]
    return "\n".join(lines)


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


__all__ = [
    "GrammarValidationError",
    "horizon_cost_bucket",
    "null_ratio_verdict",
    "run_grammar_validation",
    "signal_to_event",
]
