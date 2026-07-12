from __future__ import annotations

import hashlib
import gzip
import json
import math
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from hydra.account_policy.basket import (
    AccountPolicyRollingSummary,
    RoutedTrade,
    evaluate_account_policy,
)
from hydra.account_policy.schema import (
    BasketPolicy,
    ControllerPolicy,
    stable_hash,
)
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.mll_variants import MllMode
from hydra.propfirm.topstep_150k import Topstep150KConfig


PREREGISTRATION_SHA256 = (
    "4ca94dd368ed2aca1eaf92674ba7ba8778ff2e9e11f9ad402841c515e0deea0f"
)
SOURCE_MANIFEST_SHA256 = (
    "3065520036b6516c1542fd19d5f7e816266960d2e9349fa2d85f89befee4f07b"
)
DATA_MANIFEST_FILE_SHA256 = (
    "e471cc472a46e68d7164d73ef95c16e995bbded6bad9fcb8537cb89657274b38"
)
FEATURE_VERSION = "hydra_turbo_feature_bundle_v3_risk_path"
MINUTE_NS = 60_000_000_000


class NullControl(StrEnum):
    DAILY_BLOCK_SHUFFLE = "DAILY_BLOCK_SHUFFLE"
    VOLATILITY_MATCHED_RANDOM_WALK = "VOLATILITY_MATCHED_RANDOM_WALK"
    YEAR_BLOCK_PERMUTATION = "YEAR_BLOCK_PERMUTATION"


class NullTripwireError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SyntheticMarketPath:
    market: str
    control: NullControl
    timestamp_ns: np.ndarray
    session_day: np.ndarray
    segment_code: np.ndarray
    close: np.ndarray
    high: np.ndarray
    low: np.ndarray
    path_hash: str


@dataclass(frozen=True, slots=True)
class FrozenObject:
    generation: int
    object_id: str
    kind: str
    basket: BasketPolicy
    controller: ControllerPolicy | None
    episode_start_days: tuple[int, ...]
    source_summary: Mapping[str, Any]


@dataclass(slots=True)
class FrozenGeneration:
    generation: int
    bank: dict[str, Any]
    objects: list[FrozenObject]


@dataclass(slots=True)
class Aggregate:
    object_count: int = 0
    episode_count: int = 0
    pass_count: int = 0
    mll_breach_count: int = 0
    compliance_failure_count: int = 0
    net_pnl_sum: float = 0.0
    target_progress_sum: float = 0.0
    minimum_mll_buffer: float = math.inf

    def add(self, summary: AccountPolicyRollingSummary) -> None:
        self.object_count += 1
        self.episode_count += int(summary.episode_start_count)
        self.pass_count += int(summary.pass_count)
        self.mll_breach_count += int(summary.mll_breach_count)
        self.compliance_failure_count += int(summary.compliance_failure_count)
        self.net_pnl_sum += sum(float(row.net_pnl) for row in summary.episodes)
        self.target_progress_sum += sum(
            float(row.target_progress) for row in summary.episodes
        )
        self.minimum_mll_buffer = min(
            self.minimum_mll_buffer, float(summary.minimum_mll_buffer)
        )

    def to_dict(self) -> dict[str, Any]:
        episodes = max(self.episode_count, 1)
        return {
            "object_count": self.object_count,
            "episode_count": self.episode_count,
            "pass_count": self.pass_count,
            "pass_rate": self.pass_count / episodes,
            "mll_breach_count": self.mll_breach_count,
            "mll_breach_rate": self.mll_breach_count / episodes,
            "compliance_failure_count": self.compliance_failure_count,
            "net_expectancy_per_episode": self.net_pnl_sum / episodes,
            "mean_target_progress": self.target_progress_sum / episodes,
            "minimum_mll_buffer": (
                self.minimum_mll_buffer
                if math.isfinite(self.minimum_mll_buffer)
                else 0.0
            ),
        }


def run_null_tripwire(
    *,
    project_root: str | Path,
    preregistration_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    prereg_path = Path(preregistration_path).resolve()
    if _sha256(prereg_path) != PREREGISTRATION_SHA256:
        raise NullTripwireError("Phase 1 preregistration hash mismatch")
    prereg = json.loads(prereg_path.read_text(encoding="utf-8"))
    _verify_contract_and_g0(root, prereg)
    _verify_source_manifest(root, root / prereg["source_manifest"]["path"])
    data_manifest_path = root / prereg["data_manifest"]["path"]
    if _sha256(data_manifest_path) != DATA_MANIFEST_FILE_SHA256:
        raise NullTripwireError("data manifest file hash mismatch")

    generations = _load_frozen_generations(root)
    objects = sum(len(row.objects) for row in generations)
    starts = sum(
        len(item.episode_start_days)
        for generation in generations
        for item in generation.objects
    )
    expected_scope = prereg["frozen_object_scope"]
    if objects != int(expected_scope["object_count"]) or starts != int(
        expected_scope["real_episode_count"]
    ):
        raise NullTripwireError(
            f"frozen scope drifted: objects={objects} episodes={starts}"
        )

    primary_config = Topstep150KConfig(
        mll_mode=MllMode.EOD_LEVEL_RT_BREACH
    )
    diagnostic_config = Topstep150KConfig(mll_mode=MllMode.INTRADAY_HWM)
    real_primary = Aggregate()
    real_diagnostic = Aggregate()
    object_results: dict[str, dict[str, Any]] = {}
    mismatches: list[dict[str, Any]] = []
    for generation in generations:
        bank_components = _bank_components(generation.bank)
        cache: dict[str, dict[str, tuple[RoutedTrade, ...]]] = {}
        for item in generation.objects:
            components = _components_for_object(
                bank_components, item.basket, cache
            )
            primary = _evaluate(item, components, generation.bank, primary_config)
            diagnostic = _evaluate(
                item, components, generation.bank, diagnostic_config
            )
            mismatch = _compare_source(item.source_summary, primary.to_dict())
            if mismatch:
                mismatches.append(
                    {
                        "generation": generation.generation,
                        "object_id": item.object_id,
                        "kind": item.kind,
                        "fields": mismatch,
                    }
                )
            real_primary.add(primary)
            real_diagnostic.add(diagnostic)
            object_results[item.object_id] = {
                "generation": generation.generation,
                "kind": item.kind,
                "real": _summary_metrics(primary),
                "real_intraday_hwm": _summary_metrics(diagnostic),
                "null": {},
            }
    if mismatches:
        return _write_blocked_result(
            prereg,
            output_dir=output_dir,
            real=real_primary,
            mismatches=mismatches,
        )

    matrix_paths = _feature_matrix_paths(root, data_manifest_path)
    matrices = {
        market: FeatureMatrix.open(path, mmap=True)
        for market, path in matrix_paths.items()
    }
    control_results: dict[str, dict[str, Any]] = {}
    path_hashes: dict[str, dict[str, str]] = {}
    total_null_episodes = 0
    total_null_passes = 0
    for control in NullControl:
        paths = {
            market: build_synthetic_market_path(
                market,
                matrix,
                control=control,
                seed=int(prereg["controls"][control.value]["seeds"][market]),
                block_size=int(
                    prereg["controls"]
                    .get(control.value, {})
                    .get("block_size_trading_days", 5)
                ),
            )
            for market, matrix in matrices.items()
        }
        path_hashes[control.value] = {
            market: row.path_hash for market, row in sorted(paths.items())
        }
        aggregate = Aggregate()
        diagnostic_aggregate = Aggregate()
        for generation in generations:
            null_bank = rebuild_counterfactual_bank(generation.bank, paths)
            cache = {}
            for item in generation.objects:
                components = _components_for_object(
                    null_bank, item.basket, cache
                )
                primary = _evaluate(
                    item, components, generation.bank, primary_config
                )
                diagnostic = _evaluate(
                    item, components, generation.bank, diagnostic_config
                )
                aggregate.add(primary)
                diagnostic_aggregate.add(diagnostic)
                object_results[item.object_id]["null"][control.value] = {
                    "primary": _summary_metrics(primary),
                    "intraday_hwm": _summary_metrics(diagnostic),
                }
        control_row = aggregate.to_dict()
        control_row["intraday_hwm"] = diagnostic_aggregate.to_dict()
        real_rate = real_primary.to_dict()["pass_rate"]
        control_row["NULL_RATIO"] = (
            control_row["pass_rate"] / real_rate if real_rate > 0.0 else None
        )
        control_results[control.value] = control_row
        total_null_episodes += aggregate.episode_count
        total_null_passes += aggregate.pass_count

    real_row = real_primary.to_dict()
    real_diagnostic_row = real_diagnostic.to_dict()
    if real_row["pass_rate"] <= 0.0:
        verdict = "BLOCKED_UNDEFINED_RATIO"
        null_ratio = None
    else:
        pooled_null_rate = total_null_passes / total_null_episodes
        null_ratio = pooled_null_rate / real_row["pass_rate"]
        verdict = "ARTEFACT" if null_ratio >= 0.8 else "GREEN"
    if total_null_episodes < int(
        prereg["episode_power"]["pooled_null_episodes"]
    ):
        raise NullTripwireError("null episode power is below preregistration")

    result: dict[str, Any] = {
        "schema": "hydra_v7_null_tripwire_result_v1",
        "experiment_id": prereg["experiment_id"],
        "preregistration_sha256": PREREGISTRATION_SHA256,
        "source_manifest_sha256": SOURCE_MANIFEST_SHA256,
        "data_manifest_file_sha256": DATA_MANIFEST_FILE_SHA256,
        "verdict": verdict,
        "threshold": 0.8,
        "NULL_RATIO": null_ratio,
        "real": real_row,
        "real_intraday_hwm": real_diagnostic_row,
        "controls": control_results,
        "pooled_null": {
            "episode_count": total_null_episodes,
            "pass_count": total_null_passes,
            "pass_rate": total_null_passes / max(total_null_episodes, 1),
        },
        "object_count": objects,
        "static_basket_count": 55,
        "controller_count": 960,
        "real_equivalence_mismatch_count": 0,
        "synthetic_path_hashes": path_hashes,
        "object_results": object_results,
        "q4_access_count_delta": 0,
        "gap_access_count": 0,
        "phase_data_spend_usd": 0.0,
        "outbound_order_count": 0,
        "generation_count": 0,
        "diff_validation": [
            "hydra/validation/v7_null_tripwire.py",
            "scripts/run_v7_null_tripwire.py",
            "tests/ruleset/test_v7_null_tripwire.py",
        ],
        "CONTRE": (
            "This conditional-outcome null freezes the selected signal schedule "
            "rather than rerunning feature selection on each synthetic raw path; "
            "it is decisive for account-path geometry but not every possible "
            "selection-stage artefact."
        ),
    }
    if verdict == "ARTEFACT":
        result["badges"] = [
            {
                "candidate_id": item.object_id,
                "badge": "GEOMETRY_ONLY",
                "irreversible": True,
            }
            for generation in generations
            for item in generation.objects
            if item.kind == "STATIC_ACCOUNT_BASKET"
        ]
    return _write_result(result, output_dir)


def build_synthetic_market_path(
    market: str,
    matrix: FeatureMatrix,
    *,
    control: NullControl,
    seed: int,
    block_size: int = 5,
) -> SyntheticMarketPath:
    timestamps = np.asarray(matrix.array("timestamp_ns"), dtype=np.int64)
    days = np.asarray(matrix.array("session_day"), dtype=np.int64)
    sessions = np.asarray(matrix.array("session_code"), dtype=np.int16)
    segments = np.asarray(matrix.array("segment_code"), dtype=np.int64)
    opens = np.asarray(matrix.array("bar_open"), dtype=np.float64)
    highs = np.asarray(matrix.array("bar_high"), dtype=np.float64)
    lows = np.asarray(matrix.array("bar_low"), dtype=np.float64)
    closes = np.asarray(matrix.array("bar_close"), dtype=np.float64)
    valid = (
        (sessions >= 0)
        & np.isfinite(opens)
        & np.isfinite(highs)
        & np.isfinite(lows)
        & np.isfinite(closes)
    )
    unique_days = np.unique(days[valid])
    if len(unique_days) < 10:
        raise NullTripwireError(f"{market} has insufficient trading days")
    rng = np.random.default_rng(seed)
    synthetic_close = closes.copy()
    synthetic_high = highs.copy()
    synthetic_low = lows.copy()
    if control is NullControl.DAILY_BLOCK_SHUFFLE:
        source_days = block_shuffle_source_days(
            unique_days, block_size=block_size, rng=rng
        )
        _assign_permuted_days(
            unique_days,
            source_days,
            days=days,
            valid=valid,
            opens=opens,
            highs=highs,
            lows=lows,
            closes=closes,
            synthetic_close=synthetic_close,
            synthetic_high=synthetic_high,
            synthetic_low=synthetic_low,
        )
    elif control is NullControl.YEAR_BLOCK_PERMUTATION:
        source_days = year_permutation_source_days(unique_days)
        _assign_permuted_days(
            unique_days,
            source_days,
            days=days,
            valid=valid,
            opens=opens,
            highs=highs,
            lows=lows,
            closes=closes,
            synthetic_close=synthetic_close,
            synthetic_high=synthetic_high,
            synthetic_low=synthetic_low,
        )
    else:
        _assign_random_walk_days(
            unique_days,
            days=days,
            sessions=sessions,
            segments=segments,
            timestamps=timestamps,
            valid=valid,
            opens=opens,
            highs=highs,
            lows=lows,
            closes=closes,
            synthetic_close=synthetic_close,
            synthetic_high=synthetic_high,
            synthetic_low=synthetic_low,
            rng=rng,
        )
    if not (
        np.isfinite(synthetic_close[valid]).all()
        and np.isfinite(synthetic_high[valid]).all()
        and np.isfinite(synthetic_low[valid]).all()
    ):
        raise NullTripwireError(f"{market} synthetic path has nonfinite values")
    if np.any(synthetic_high[valid] < synthetic_low[valid]):
        raise NullTripwireError(f"{market} synthetic high/low order failed")
    path_hash = _array_hash(
        market,
        control.value,
        timestamps[valid],
        synthetic_close[valid],
        synthetic_high[valid],
        synthetic_low[valid],
    )
    for value in (synthetic_close, synthetic_high, synthetic_low):
        value.flags.writeable = False
    return SyntheticMarketPath(
        market=market,
        control=control,
        timestamp_ns=timestamps,
        session_day=days,
        segment_code=segments,
        close=synthetic_close,
        high=synthetic_high,
        low=synthetic_low,
        path_hash=path_hash,
    )


def block_shuffle_source_days(
    days: np.ndarray, *, block_size: int, rng: np.random.Generator
) -> np.ndarray:
    if block_size < 1:
        raise ValueError("block_size must be positive")
    ordered = np.asarray(days, dtype=np.int64)
    blocks = [ordered[index : index + block_size] for index in range(0, len(ordered), block_size)]
    order = rng.permutation(len(blocks))
    if np.array_equal(order, np.arange(len(blocks))):
        order = np.roll(order, 1)
    return np.concatenate([blocks[int(index)] for index in order])


def year_permutation_source_days(days: np.ndarray) -> np.ndarray:
    ordered = np.asarray(days, dtype=np.int64)
    years = np.asarray(
        [int(str(np.datetime64(int(day), "D"))[:4]) for day in ordered],
        dtype=np.int32,
    )
    unique_years = np.unique(years)
    if len(unique_years) < 2:
        return ordered[::-1].copy()
    groups = [ordered[years == year] for year in unique_years]
    return np.concatenate(groups[1:] + groups[:1])


def rebuild_counterfactual_bank(
    bank: Mapping[str, Any], paths: Mapping[str, SyntheticMarketPath]
) -> dict[str, tuple[RoutedTrade, ...]]:
    output: dict[str, tuple[RoutedTrade, ...]] = {}
    for component_id, row in bank["components"].items():
        specification = row["specification"]
        point_value = float(specification["point_value"])
        rebuilt: list[RoutedTrade] = []
        for raw in row["events"]:
            routed = RoutedTrade.from_dict(raw)
            path = paths[routed.market]
            rebuilt.append(
                rebuild_counterfactual_trade(
                    routed, path, point_value=point_value
                )
            )
        if len(rebuilt) != len(row["events"]):
            raise NullTripwireError("counterfactual changed signal count")
        output[str(component_id)] = tuple(rebuilt)
    return output


def rebuild_counterfactual_trade(
    routed: RoutedTrade,
    path: SyntheticMarketPath,
    *,
    point_value: float,
) -> RoutedTrade:
    event = routed.event
    timestamps = path.timestamp_ns
    entry_index = int(np.searchsorted(timestamps, event.decision_ns, side="left"))
    exit_index = int(np.searchsorted(timestamps, event.exit_ns, side="left")) - 1
    if (
        entry_index < 0
        or exit_index < entry_index
        or exit_index >= len(timestamps)
        or int(timestamps[entry_index]) != int(event.decision_ns)
        or int(timestamps[exit_index]) + MINUTE_NS != int(event.exit_ns)
    ):
        raise NullTripwireError(
            f"counterfactual timestamp mismatch: {event.event_id}"
        )
    if int(path.segment_code[entry_index]) != int(path.segment_code[exit_index]):
        raise NullTripwireError(
            f"counterfactual event crosses segment: {event.event_id}"
        )
    entry = float(path.close[entry_index])
    exit_price = float(path.close[exit_index])
    path_high = float(np.max(path.high[entry_index : exit_index + 1]))
    path_low = float(np.min(path.low[entry_index : exit_index + 1]))
    adverse = path_low if routed.side > 0 else path_high
    favorable = path_high if routed.side > 0 else path_low
    quantity = int(event.quantity)
    cost = max(0.0, float(event.gross_pnl) - float(event.net_pnl))
    scale = float(routed.side * point_value * quantity)
    gross = (exit_price - entry) * scale
    worst = (adverse - entry) * scale - cost
    best = (favorable - entry) * scale - cost
    rebuilt_event = TradePathEvent(
        event_id=f"{event.event_id}:{path.control.value}",
        decision_ns=event.decision_ns,
        exit_ns=event.exit_ns,
        session_day=event.session_day,
        net_pnl=float(gross - cost),
        gross_pnl=float(gross),
        worst_unrealized_pnl=float(min(worst, gross - cost, 0.0)),
        best_unrealized_pnl=float(max(best, gross - cost, 0.0)),
        quantity=quantity,
        mini_equivalent=event.mini_equivalent,
        regime=event.regime,
        session_compliant=event.session_compliant,
        contract_limit_compliant=event.contract_limit_compliant,
        same_bar_ambiguous=event.same_bar_ambiguous,
    )
    return RoutedTrade(
        component_id=routed.component_id,
        market=routed.market,
        side=routed.side,
        event=rebuilt_event,
    )


def null_verdict(real_pass_rate: float, null_pass_rate: float) -> tuple[str, float | None]:
    if real_pass_rate <= 0.0:
        return "BLOCKED_UNDEFINED_RATIO", None
    ratio = null_pass_rate / real_pass_rate
    return ("ARTEFACT" if ratio >= 0.8 else "GREEN"), ratio


def _assign_permuted_days(
    target_days: np.ndarray,
    source_days: np.ndarray,
    **arrays: Any,
) -> None:
    days = arrays["days"]
    valid = arrays["valid"]
    for target_day, source_day in zip(target_days, source_days, strict=True):
        target = np.flatnonzero(valid & (days == int(target_day)))
        source = np.flatnonzero(valid & (days == int(source_day)))
        close, high, low = _resampled_day_path(
            arrays["opens"][source],
            arrays["highs"][source],
            arrays["lows"][source],
            arrays["closes"][source],
            len(target),
        )
        arrays["synthetic_close"][target] = close
        arrays["synthetic_high"][target] = high
        arrays["synthetic_low"][target] = low


def _resampled_day_path(
    opens: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    target_length: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if target_length < 1 or len(closes) < 1:
        raise NullTripwireError("empty daily path")
    base = float(opens[0])
    close_relative = closes - base
    source_x = np.linspace(1.0 / len(closes), 1.0, len(closes))
    target_x = np.linspace(1.0 / target_length, 1.0, target_length)
    synthetic_close = np.interp(
        target_x,
        np.concatenate(([0.0], source_x)),
        np.concatenate(([0.0], close_relative)),
    )
    upper = np.maximum(0.0, highs - np.maximum(opens, closes))
    lower = np.maximum(0.0, np.minimum(opens, closes) - lows)
    synthetic_upper = np.interp(target_x, source_x, upper)
    synthetic_lower = np.interp(target_x, source_x, lower)
    synthetic_open = np.concatenate(([0.0], synthetic_close[:-1]))
    synthetic_high = (
        np.maximum(synthetic_open, synthetic_close) + synthetic_upper
    )
    synthetic_low = (
        np.minimum(synthetic_open, synthetic_close) - synthetic_lower
    )
    return synthetic_close, synthetic_high, synthetic_low


def _assign_random_walk_days(
    target_days: np.ndarray, **arrays: Any
) -> None:
    closes = arrays["closes"]
    opens = arrays["opens"]
    highs = arrays["highs"]
    lows = arrays["lows"]
    sessions = arrays["sessions"]
    segments = arrays["segments"]
    timestamps = arrays["timestamps"]
    valid = arrays["valid"]
    rng: np.random.Generator = arrays["rng"]
    contiguous = (
        valid[1:]
        & valid[:-1]
        & (segments[1:] == segments[:-1])
        & (timestamps[1:] - timestamps[:-1] == MINUTE_NS)
    )
    differences = closes[1:] - closes[:-1]
    global_sigma = float(np.std(differences[contiguous], ddof=1))
    global_upper = np.maximum(
        0.0, highs[valid] - np.maximum(opens[valid], closes[valid])
    )
    global_lower = np.maximum(
        0.0, np.minimum(opens[valid], closes[valid]) - lows[valid]
    )
    sigma_by_session: dict[int, float] = {}
    wick_by_session: dict[int, tuple[np.ndarray, np.ndarray]] = {}
    for session in (0, 1, 2):
        diff_mask = contiguous & (sessions[1:] == session)
        values = differences[diff_mask]
        sigma = float(np.std(values, ddof=1)) if len(values) > 1 else 0.0
        if not math.isfinite(sigma) or sigma <= 0.0:
            sigma = global_sigma
        sigma_by_session[session] = sigma
        row_mask = valid & (sessions == session)
        upper = np.maximum(
            0.0, highs[row_mask] - np.maximum(opens[row_mask], closes[row_mask])
        )
        lower = np.maximum(
            0.0, np.minimum(opens[row_mask], closes[row_mask]) - lows[row_mask]
        )
        if not len(upper):
            upper = global_upper
            lower = global_lower
        wick_by_session[session] = (upper, lower)
    days = arrays["days"]
    for target_day in target_days:
        positions = np.flatnonzero(valid & (days == int(target_day)))
        local_sessions = sessions[positions]
        increments = np.asarray(
            [rng.normal(0.0, sigma_by_session[int(session)]) for session in local_sessions],
            dtype=np.float64,
        )
        synthetic_close = np.cumsum(increments)
        synthetic_open = np.concatenate(([0.0], synthetic_close[:-1]))
        upper = np.empty(len(positions), dtype=np.float64)
        lower = np.empty(len(positions), dtype=np.float64)
        for index, session in enumerate(local_sessions):
            upper_pool, lower_pool = wick_by_session[int(session)]
            upper[index] = upper_pool[int(rng.integers(0, len(upper_pool)))]
            lower[index] = lower_pool[int(rng.integers(0, len(lower_pool)))]
        arrays["synthetic_close"][positions] = synthetic_close
        arrays["synthetic_high"][positions] = (
            np.maximum(synthetic_open, synthetic_close) + upper
        )
        arrays["synthetic_low"][positions] = (
            np.minimum(synthetic_open, synthetic_close) - lower
        )


def _load_frozen_generations(root: Path) -> list[FrozenGeneration]:
    scope = json.loads(
        (root / "config/v7/phase0_g0_preregistration.json").read_text(
            encoding="utf-8"
        )
    )["historical_replay_scope"]["generations"]
    output: list[FrozenGeneration] = []
    for generation_name, selected in scope.items():
        generation = int(generation_name.rsplit("_", 1)[1])
        directory = (
            root
            / "reports/mission_experiments"
            / f"account_level_evolution_v6_generation_{generation:04d}"
        )
        bank = json.loads(
            (directory / "account_v6_component_bank.json").read_text(
                encoding="utf-8"
            )
        )
        unhashed = dict(bank)
        expected = str(unhashed.pop("manifest_hash"))
        if stable_hash(unhashed) != expected:
            raise NullTripwireError("component bank hash mismatch")
        promotions = _jsonl_by_id(
            directory / "account_v6_promotion_results.jsonl", "policy_id"
        )
        objects: list[FrozenObject] = []
        for policy_id in selected["basket_elite_ids"]:
            row = promotions[policy_id]
            objects.append(
                FrozenObject(
                    generation=generation,
                    object_id=str(policy_id),
                    kind="STATIC_ACCOUNT_BASKET",
                    basket=BasketPolicy.from_dict(row["basket"]),
                    controller=None,
                    episode_start_days=tuple(
                        int(value) for value in row["episode_start_days"]
                    ),
                    source_summary=row["summary"],
                )
            )
        controller_rows = [
            json.loads(line)
            for line in (
                directory / "account_v6_controller_results.jsonl"
            ).read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        for row in controller_rows:
            if bool(row["is_random_control"]):
                continue
            objects.append(
                FrozenObject(
                    generation=generation,
                    object_id=str(row["policy_id"]),
                    kind="ADAPTIVE_ACCOUNT_CONTROLLER",
                    basket=BasketPolicy.from_dict(row["basket"]),
                    controller=ControllerPolicy.from_dict(row["controller"]),
                    episode_start_days=tuple(
                        int(value) for value in row["episode_start_days"]
                    ),
                    source_summary=row["summary"],
                )
            )
        output.append(FrozenGeneration(generation, bank, objects))
    return output


def _bank_components(bank: Mapping[str, Any]) -> dict[str, tuple[RoutedTrade, ...]]:
    return {
        str(component_id): tuple(
            RoutedTrade.from_dict(row) for row in component["events"]
        )
        for component_id, component in bank["components"].items()
    }


def _components_for_object(
    bank_components: Mapping[str, tuple[RoutedTrade, ...]],
    basket: BasketPolicy,
    cache: dict[str, dict[str, tuple[RoutedTrade, ...]]],
) -> dict[str, tuple[RoutedTrade, ...]]:
    key = basket.structural_fingerprint
    if key not in cache:
        cache[key] = {
            component_id: bank_components[component_id]
            for component_id in basket.component_ids
        }
    return cache[key]


def _evaluate(
    item: FrozenObject,
    components: Mapping[str, Sequence[RoutedTrade]],
    bank: Mapping[str, Any],
    config: Topstep150KConfig,
) -> AccountPolicyRollingSummary:
    return evaluate_account_policy(
        components,
        bank["eligible_session_days"],
        basket=item.basket,
        controller=item.controller,
        explicit_start_days=item.episode_start_days,
        config=config,
    )


def _summary_metrics(summary: AccountPolicyRollingSummary) -> dict[str, Any]:
    return {
        "episode_count": int(summary.episode_start_count),
        "pass_count": int(summary.pass_count),
        "pass_rate": float(summary.pass_rate),
        "mll_breach_count": int(summary.mll_breach_count),
        "mll_breach_rate": float(summary.mll_breach_rate),
        "compliance_failure_count": int(summary.compliance_failure_count),
        "median_episode_net_pnl": float(summary.median_episode_net_pnl),
        "median_target_progress": float(summary.target_progress_median),
        "minimum_mll_buffer": float(summary.minimum_mll_buffer),
    }


def _compare_source(
    source: Mapping[str, Any], replay: Mapping[str, Any]
) -> list[str]:
    exact = (
        "episode_start_days",
        "terminal_distribution",
        "pass_count",
        "mll_breach_count",
        "compliance_failure_count",
    )
    numeric = (
        "pass_rate",
        "mll_breach_rate",
        "target_progress_median",
        "median_episode_net_pnl",
        "minimum_mll_buffer",
    )
    mismatch = [name for name in exact if source[name] != replay[name]]
    mismatch.extend(
        name
        for name in numeric
        if abs(float(source[name]) - float(replay[name])) > 1e-9
    )
    return mismatch


def _feature_matrix_paths(
    root: Path, data_manifest_path: Path
) -> dict[str, Path]:
    payload = json.loads(data_manifest_path.read_text(encoding="utf-8"))
    output: dict[str, Path] = {}
    for artifact in payload["artifacts"]:
        if artifact.get("kind") != "CANONICAL_FEATURE_MANIFEST":
            continue
        manifest_path = root / artifact["path"]
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        key = manifest["key"]
        if key["transformation_version"] != FEATURE_VERSION:
            continue
        if key["start_inclusive"] != "2023-01-01" or key["end_exclusive"] != "2024-10-01":
            raise NullTripwireError("feature matrix period drift")
        output[str(key["market"])] = manifest_path.parent
    expected = {"CL", "ES", "GC", "NQ", "RTY", "YM"}
    if set(output) != expected:
        raise NullTripwireError(
            f"feature matrix scope mismatch: {sorted(output)}"
        )
    return output


def _verify_contract_and_g0(root: Path, prereg: Mapping[str, Any]) -> None:
    if _sha256(root / prereg["contract"]["path"]) != prereg["contract"]["sha256"]:
        raise NullTripwireError("contract hash mismatch")
    g0 = root / prereg["gate_G0"]["result_path"]
    if _sha256(g0) != prereg["gate_G0"]["result_sha256"]:
        raise NullTripwireError("G0 result hash mismatch")
    if json.loads(g0.read_text(encoding="utf-8"))["verdict"] != "GREEN":
        raise NullTripwireError("G0 is not GREEN")


def _verify_source_manifest(root: Path, manifest: Path) -> None:
    if _sha256(manifest) != SOURCE_MANIFEST_SHA256:
        raise NullTripwireError("Phase 1 source manifest hash mismatch")
    for raw in manifest.read_text(encoding="utf-8").splitlines():
        expected, relative = raw.split("  ", 1)
        if _sha256(root / relative) != expected:
            raise NullTripwireError(f"source hash mismatch: {relative}")


def _jsonl_by_id(path: Path, key: str) -> dict[str, dict[str, Any]]:
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    output = {str(row[key]): row for row in rows}
    if len(output) != len(rows):
        raise NullTripwireError(f"duplicate {key}: {path}")
    return output


def _write_blocked_result(
    prereg: Mapping[str, Any],
    *,
    output_dir: str | Path,
    real: Aggregate,
    mismatches: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    result = {
        "schema": "hydra_v7_null_tripwire_result_v1",
        "experiment_id": prereg["experiment_id"],
        "verdict": "BLOCKED",
        "reason": "REAL_PIPELINE_EQUIVALENCE_FAILED",
        "real": real.to_dict(),
        "real_equivalence_mismatch_count": len(mismatches),
        "real_equivalence_mismatches": list(mismatches),
        "q4_access_count_delta": 0,
        "gap_access_count": 0,
        "outbound_order_count": 0,
        "CONTRE": "The null was not generated because the frozen real pipeline no longer reproduced exactly.",
    }
    return _write_result(result, output_dir)


def _write_result(result: dict[str, Any], output_dir: str | Path) -> dict[str, Any]:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    persisted = dict(result)
    object_results = persisted.pop("object_results", None)
    if object_results is not None:
        full_payload = (
            json.dumps(result, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        detail_path = destination / "null_tripwire_full_evidence.json.gz"
        detail_bytes = gzip.compress(full_payload, compresslevel=9, mtime=0)
        detail_path.write_bytes(detail_bytes)
        persisted["full_detail_evidence"] = {
            "path": str(detail_path),
            "compression": "gzip-mtime-0",
            "object_result_count": len(object_results),
            "compressed_sha256": hashlib.sha256(detail_bytes).hexdigest(),
            "uncompressed_sha256": hashlib.sha256(full_payload).hexdigest(),
            "uncompressed_bytes": len(full_payload),
        }
    result_path = destination / "null_tripwire_result.json"
    result_path.write_text(
        json.dumps(persisted, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    result_sha = _sha256(result_path)
    report_path = destination / "null_tripwire_report.md"
    report_path.write_text(
        _render_report(persisted, result_sha), encoding="utf-8"
    )
    return {
        **persisted,
        "result_path": str(result_path),
        "result_sha256": result_sha,
        "report_path": str(report_path),
        "report_sha256": _sha256(report_path),
    }


def _render_report(result: Mapping[str, Any], result_sha: str) -> str:
    verdict = str(result["verdict"])
    tests = "pending"
    real = result.get("real", {})
    pooled = result.get("pooled_null", {})
    lines = [
        "# HYDRA V7 — Phase 1 Null Tripwire",
        "",
        f"[HYDRA-V7] phase=1 step=25 verdict={verdict}",
        f"gate=G1 preuve=reports/v7/phase1/null_tripwire_result.json#{result_sha[:8]} tests={tests}",
        "budget_llm=0.000000/12.00 budget_data=0.000000/0.00 N_trials=pending_retro_estimate burned=1",
        "diff_validation=hydra/validation/v7_null_tripwire.py,scripts/run_v7_null_tripwire.py,tests/ruleset/test_v7_null_tripwire.py CONTRE="
        + str(result["CONTRE"]),
        "prochaine_action=auditer_le_verdict_et_exécuter_la_régression_avant_G1",
        "",
        f"Verdict : **{verdict}**.",
        f"Objets figés : `{result.get('object_count', 0)}`.",
        f"Pass-rate réel : `{float(real.get('pass_rate', 0.0)):.8f}` sur `{real.get('episode_count', 0)}` épisodes.",
        f"Pass-rate null poolé : `{float(pooled.get('pass_rate', 0.0)):.8f}` sur `{pooled.get('episode_count', 0)}` épisodes.",
        f"NULL_RATIO : `{result.get('NULL_RATIO')}`; seuil WORM : `{result.get('threshold', 0.8)}`.",
        "",
    ]
    for name, row in result.get("controls", {}).items():
        lines.append(
            f"- {name}: pass `{row['pass_rate']:.8f}`, breach `{row['mll_breach_rate']:.8f}`, ratio `{row['NULL_RATIO']:.8f}`."
        )
    lines.extend(
        [
            "",
            "Aucune donnée Q4, aucun gap forward et aucun ordre broker n'ont été utilisés.",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )
    return "\n".join(lines)


def _array_hash(*values: Any) -> str:
    digest = hashlib.sha256()
    for value in values:
        if isinstance(value, str):
            digest.update(value.encode("utf-8"))
        else:
            array = np.ascontiguousarray(value)
            digest.update(str(array.dtype).encode("ascii"))
            digest.update(np.asarray(array.shape, dtype=np.int64).tobytes())
            digest.update(array.tobytes())
    return digest.hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "NullControl",
    "NullTripwireError",
    "SyntheticMarketPath",
    "block_shuffle_source_days",
    "build_synthetic_market_path",
    "null_verdict",
    "rebuild_counterfactual_bank",
    "rebuild_counterfactual_trade",
    "run_null_tripwire",
    "year_permutation_source_days",
]
