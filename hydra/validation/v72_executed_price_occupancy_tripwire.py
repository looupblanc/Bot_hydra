from __future__ import annotations

import hashlib
import json
import os
from decimal import Decimal, ROUND_HALF_DOWN
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from hydra.data.v72_executed_price_occupancy_store import (
    MINUTE_NS,
    SOURCES,
    TICK_RAW,
    TICK_SIZE,
    OccupancyStreamAccumulator,
    SourceSpec,
    _compute_mode_migration,
    _validate_frame,
)
from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.research.v72_executed_price_occupancy import (
    FEATURE_PATH,
    build_executed_price_occupancy_states,
    candidate_specs,
    generate_signal_population,
    load_executed_price_occupancy_sources,
    signal_path_hash,
)
from hydra.validation.v71_opportunity_density_tripwire import (
    build_candidate_events,
    classify_tripwire,
)
from hydra.validation.v7_d1_new_dataset_tripwire import (
    D1NullControl,
    _eligible_days_by_year,
    _evaluate_world,
    _within_year_price_null,
    _year_permuted_prices,
)
from hydra.validation.v7_report_schema import validate_v7_report_text
from hydra.validation.v7_tripwire_evidence import exact_tripwire_evidence


TRIPWIRE_POLICY_PATH = (
    "WORM/v7.2-executed-price-occupancy-tripwire-0012-2026-07-13.json"
)
TRIPWIRE_POLICY_SHA256 = "0a40db491ae3cc28fedfe00107b3e3ba417896f4533460e3fabc1c1a1c664786"
GRAMMAR_PATH = "WORM/v7.2-executed-price-occupancy-grammar-0012-2026-07-13.json"
GRAMMAR_SHA256 = "d0fa4eb200f47e1df9d3323c09f9e0c3729802a001b9c946bdf43824846a4c0c"
ADDENDUM_PATH = (
    "WORM/v7.2-executed-price-occupancy-validation-addendum-0012-2026-07-13.json"
)
ADDENDUM_SHA256 = "badd801bf80dbeba77424e5d73de9c3c1187a9a8d52e38da96127a2de88f2dbd"
SIGNAL_MANIFEST_PATH = (
    "reports/v7_2/discovery_0012/v72_executed_price_occupancy_signal_manifest.json"
)
SIGNAL_MANIFEST_SHA256 = "8c6796df76cea34ca87c83e01d5291bc8b5f401d86acf4ce7090891b9a1955d0"
RESERVATION_EVENT_ID = (
    "v7_2_executed_price_occupancy_structural_tripwire_reservation_0012"
)
EXPECTED_GLOBAL_N_TRIALS = 265_347
NULL_THRESHOLD = 0.80
SHUFFLE_SEEDS = {(2023, "ES"): 7_212_001, (2024, "ES"): 7_212_001}
RANDOM_SEEDS = {(2023, "ES"): 7_212_002, (2024, "ES"): 7_212_002}
INVARIANT_FEATURE_COLUMNS = (
    "calendar_year",
    "session_date",
    "contract",
    "instrument_id",
    "minute_start_ns",
    "source_close_ns",
    "availability_ns",
    "trade_count",
    "total_volume",
    "buy_volume",
    "sell_volume",
    "neutral_volume",
    "signed_volume",
    "signed_flow_fraction",
)


class V72ExecutedPriceOccupancyTripwireError(RuntimeError):
    pass


def run_executed_price_occupancy_tripwire(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_2/discovery_0012",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    _verify_inputs(root, proof_registry_path)
    minute, real_states, source_audit = load_executed_price_occupancy_sources(root)
    feature = pd.read_parquet(root / FEATURE_PATH)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    real_signals = generate_signal_population(
        real_states, project_root=root, graveyard_path=None
    )
    _verify_signal_manifest(root, real_signals, specs)
    costs = load_cost_model()
    real_events = build_candidate_events(
        minute,
        real_signals,
        specs,
        costs,
        stress=CostStress.STRESS_1_5X,
    )
    eligible = _eligible_days_by_year(minute)
    real = _evaluate_world(real_events, eligible)
    controls: dict[str, Any] = {}
    pooled_passes = 0
    pooled_episodes = 0
    for control in D1NullControl:
        null_minute, null_feature, reconstruction_audit = (
            build_executed_price_occupancy_null_world(
                root, minute, control=control
            )
        )
        _verify_preserved_observables(minute, feature, null_minute, null_feature)
        null_states, null_audit = build_executed_price_occupancy_states(
            null_feature, null_minute
        )
        _verify_invariant_source_counts(source_audit, null_audit)
        null_signals = generate_signal_population(
            null_states, project_root=root, graveyard_path=None
        )
        null_events = build_candidate_events(
            null_minute,
            null_signals,
            specs,
            costs,
            stress=CostStress.STRESS_1_5X,
        )
        summary = _evaluate_world(null_events, eligible)
        summary["signal_count"] = sum(len(rows) for rows in null_signals.values())
        summary["source_audit"] = null_audit
        summary["reconstruction_audit"] = reconstruction_audit
        controls[control.value] = summary
        pooled_passes += int(summary["pass_count"])
        pooled_episodes += int(summary["episode_count"])
    if int(real["episode_count"]) != 480 or pooled_episodes != 1440:
        raise V72ExecutedPriceOccupancyTripwireError(
            "executed-price occupancy tripwire denominator drift"
        )
    verdict, null_ratio = classify_tripwire(
        real_passes=int(real["pass_count"]),
        real_episodes=int(real["episode_count"]),
        null_passes=pooled_passes,
        null_episodes=pooled_episodes,
    )
    exact = exact_tripwire_evidence(
        real_passes=int(real["pass_count"]),
        real_episodes=int(real["episode_count"]),
        null_passes=pooled_passes,
        null_episodes=pooled_episodes,
        tripwire_verdict=verdict,
    )
    result = {
        "schema": "hydra_v7_2_executed_price_occupancy_tripwire_result_v1",
        "tripwire_id": "hydra_v7_2_executed_price_occupancy_tripwire_0012",
        "grammar_id": "hydra_v7_2_executed_price_occupancy_grammar_0012",
        "verdict": verdict,
        "threshold": NULL_THRESHOLD,
        "NULL_RATIO": null_ratio,
        "raw_pass_counts": {
            "real": f"{int(real['pass_count'])}/{int(real['episode_count'])}",
            "null": f"{pooled_passes}/{pooled_episodes}",
        },
        "exact_binomial_test": exact.to_dict(),
        "evidence_strength": exact.evidence_strength,
        "real": {
            **real,
            "signal_count": sum(len(rows) for rows in real_signals.values()),
        },
        "pooled_null": {
            "episode_count": pooled_episodes,
            "pass_count": pooled_passes,
            "pass_rate": pooled_passes / pooled_episodes,
        },
        "controls": controls,
        "candidate_count": len(specs),
        "diagnostic_quantities": [1, 2, 4, 8],
        "source_audit": source_audit,
        "event_timestamps_sizes_sides_preserved_in_price_nulls": True,
        "occupancy_features_signals_and_account_paths_recomputed": True,
        "signal_paths_allowed_to_change_in_price_nulls": True,
        "cost_stress": CostStress.STRESS_1_5X.value,
        "combine_pass_rate_is_diagnostic_not_fitness": True,
        "candidate_promotion_authorized": False,
        "N_trials": multiplicity_trial_count(
            load_and_verify(_proof_path(root, proof_registry_path))
        ),
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "new_data_purchase_count": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "Rank-residual null reconstruction intentionally preserves within-minute "
            "event ordering geometry, so it can be conservative; raw counts and exact-"
            "binomial evidence must be read separately from the already negative "
            "walk-forward economics."
        ),
        "prochaine_action": (
            "freeze_power_audit_for_surviving_walk_forward_candidates"
            if verdict == "GREEN_NULL_ADJUSTED_BASELINE"
            else "tombstone_executed_price_occupancy_class_as_geometry_only"
            if verdict == "ARTEFACT_GEOMETRY_ONLY"
            else "record_underpowered_tripwire_and_no_candidate_promotion"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def build_executed_price_occupancy_null_world(
    root: str | Path,
    minute: pd.DataFrame,
    *,
    control: D1NullControl,
    chunk_size: int = 1_000_000,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    project_root = Path(root).resolve()
    bridge_minute = _null_bridge_minute(minute, control)
    feature_frames: list[pd.DataFrame] = []
    minute_metrics: dict[int, dict[str, float | int]] = {}
    source_audits: list[dict[str, Any]] = []
    for source in SOURCES:
        source_bridge = bridge_minute[
            (bridge_minute["calendar_year"] == source.calendar_year)
            & (bridge_minute["contract"].astype(str) == source.contract)
        ]
        bridge = {
            int(row.minute_start_ns): (
                _price_to_tick(float(row.open)),
                _price_to_tick(float(row.close)),
            )
            for row in source_bridge.itertuples(index=False)
        }
        frame, metrics, audit = _reconstruct_source(
            project_root / source.path,
            source,
            bridge,
            chunk_size=chunk_size,
        )
        feature_frames.append(frame)
        minute_metrics.update(metrics)
        source_audits.append(audit)
    null_feature = pd.concat(feature_frames, ignore_index=True).sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    null_feature["mode_migration_ticks"] = _compute_mode_migration(null_feature)
    _validate_frame(null_feature)

    null_minute = bridge_minute.copy().sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    if set(minute_metrics) != set(
        null_minute["minute_start_ns"].astype(int).tolist()
    ):
        raise V72ExecutedPriceOccupancyTripwireError(
            "null reconstructed minute coverage drift"
        )
    metric_frame = pd.DataFrame.from_dict(minute_metrics, orient="index")
    metric_frame.index.name = "minute_start_ns"
    metric_frame = metric_frame.reindex(
        null_minute["minute_start_ns"].to_numpy(np.int64)
    )
    for column in (
        "open",
        "high",
        "low",
        "close",
        "vwap",
        "price_change_points",
        "path_length_points",
        "signed_path_efficiency",
    ):
        null_minute[column] = metric_frame[column].to_numpy(float)
    return null_minute, null_feature, {
        "control": control.value,
        "bridge_coordinate": "stable_trade_rank",
        "rounding": "nearest_quarter_tick_half_ties_lower",
        "source_audits": source_audits,
        "minute_count": len(null_minute),
        "retained_trade_count": sum(
            int(row["retained_es_rth_record_count"]) for row in source_audits
        ),
    }


def reconstruct_minute_tick_path(
    real_ticks: np.ndarray,
    *,
    null_open_tick: int,
    null_close_tick: int,
) -> np.ndarray:
    values = np.asarray(real_ticks, dtype=np.int64)
    if len(values) == 0:
        raise V72ExecutedPriceOccupancyTripwireError(
            "cannot reconstruct an empty trade path"
        )
    if len(values) == 1:
        return np.asarray([null_open_tick], dtype=np.int64)
    denominator = len(values) - 1
    positions = np.arange(len(values), dtype=np.int64)
    inverse = denominator - positions
    real_bridge_numerator = values[0] * inverse + values[-1] * positions
    residual_numerator = values * denominator - real_bridge_numerator
    null_bridge_numerator = (
        int(null_open_tick) * inverse + int(null_close_tick) * positions
    )
    return _round_rational_ties_lower(
        null_bridge_numerator + residual_numerator, denominator
    )


def _round_rational_ties_lower(
    numerator: np.ndarray, denominator: int
) -> np.ndarray:
    if denominator <= 0:
        raise V72ExecutedPriceOccupancyTripwireError(
            "rational tick denominator must be positive"
        )
    values = np.asarray(numerator, dtype=np.int64)
    quotient = np.floor_divide(values, denominator)
    remainder = values - quotient * denominator
    return (quotient + (2 * remainder > denominator)).astype(np.int64)


def _reconstruct_source(
    path: Path,
    source: SourceSpec,
    bridge: Mapping[int, tuple[int, int]],
    *,
    chunk_size: int,
) -> tuple[pd.DataFrame, dict[int, dict[str, float | int]], dict[str, Any]]:
    import databento as db

    store = db.DBNStore.from_file(path)
    if str(store.metadata.dataset) != "GLBX.MDP3" or str(store.metadata.schema) != "trades":
        raise V72ExecutedPriceOccupancyTripwireError(
            "unexpected null reconstruction DBN metadata"
        )
    stream = OccupancyStreamAccumulator(source=source)
    metrics: dict[int, dict[str, float | int]] = {}
    pending_minute: int | None = None
    pending_parts: list[np.ndarray] = []
    raw_count = bounded_count = retained_count = 0

    def flush_pending() -> None:
        nonlocal pending_minute, pending_parts
        if pending_minute is None:
            return
        records = (
            pending_parts[0]
            if len(pending_parts) == 1
            else np.concatenate(pending_parts)
        )
        if pending_minute not in bridge:
            raise V72ExecutedPriceOccupancyTripwireError(
                "null bridge missing a retained minute"
            )
        real_raw = np.asarray(records["price"], dtype=np.int64)
        real_ticks = ((real_raw + TICK_RAW // 2) // TICK_RAW).astype(np.int64)
        null_open, null_close = bridge[pending_minute]
        null_ticks = reconstruct_minute_tick_path(
            real_ticks,
            null_open_tick=null_open,
            null_close_tick=null_close,
        )
        synthetic = records.copy()
        synthetic["price"] = null_ticks * TICK_RAW
        stream.ingest(synthetic)
        sizes = np.asarray(records["size"], dtype=np.int64)
        path_length = float(np.abs(np.diff(null_ticks)).sum() * TICK_SIZE)
        price_change = float((null_ticks[-1] - null_ticks[0]) * TICK_SIZE)
        metrics[pending_minute] = {
            "open": float(null_ticks[0] * TICK_SIZE),
            "high": float(null_ticks.max() * TICK_SIZE),
            "low": float(null_ticks.min() * TICK_SIZE),
            "close": float(null_ticks[-1] * TICK_SIZE),
            "vwap": float(
                np.dot(null_ticks.astype(np.float64), sizes.astype(np.float64))
                / sizes.sum()
                * TICK_SIZE
            ),
            "price_change_points": price_change,
            "path_length_points": path_length,
            "signed_path_efficiency": (
                price_change / path_length if path_length > 0.0 else 0.0
            ),
        }
        pending_minute = None
        pending_parts = []

    for chunk in store.to_ndarray(count=chunk_size):
        if len(chunk) == 0:
            continue
        raw_count += len(chunk)
        ts = np.asarray(chunk["ts_event"], dtype=np.int64)
        bounded = (ts >= source.start_ns) & (ts < source.end_ns)
        bounded_count += int(np.count_nonzero(bounded))
        actions = np.asarray(chunk["action"])
        instruments = np.asarray(chunk["instrument_id"], dtype=np.int64)
        timestamps = pd.to_datetime(ts, unit="ns", utc=True).tz_convert(
            "America/Chicago"
        )
        minutes_ct = np.asarray(timestamps.hour * 60 + timestamps.minute)
        weekdays = np.asarray(timestamps.weekday)
        keep = (
            bounded
            & (actions == b"T")
            & (instruments == source.instrument_id)
            & (weekdays < 5)
            & (minutes_ct >= 8 * 60 + 30)
            & (minutes_ct < 15 * 60 + 10)
        )
        if not np.any(keep):
            continue
        retained = chunk[keep]
        retained_count += len(retained)
        minute_ids = (
            np.asarray(retained["ts_event"], dtype=np.int64) // MINUTE_NS
        ) * MINUTE_NS
        starts = np.flatnonzero(np.r_[True, minute_ids[1:] != minute_ids[:-1]])
        ends = np.r_[starts[1:], len(retained)]
        for left, right in zip(starts, ends, strict=True):
            minute_id = int(minute_ids[left])
            if pending_minute is not None and minute_id != pending_minute:
                flush_pending()
            if pending_minute is None:
                pending_minute = minute_id
            pending_parts.append(retained[left:right])
    flush_pending()
    frame = stream.finish()
    if retained_count <= 0 or frame.empty or len(frame) != len(bridge):
        raise V72ExecutedPriceOccupancyTripwireError(
            "null reconstruction retained-minute drift"
        )
    return frame, metrics, {
        "calendar_year": source.calendar_year,
        "contract": source.contract,
        "raw_record_count": raw_count,
        "bounded_record_count": bounded_count,
        "retained_es_rth_record_count": retained_count,
        "minute_group_count": len(frame),
        "bridge_minute_count": len(bridge),
        "metric_minute_count": len(metrics),
    }


def _null_bridge_minute(
    minute: pd.DataFrame, control: D1NullControl
) -> pd.DataFrame:
    if control == D1NullControl.YEAR_BLOCK_PERMUTATION:
        return _year_permuted_prices(minute, "minute_start_ns")
    seeds = (
        SHUFFLE_SEEDS
        if control == D1NullControl.DAILY_BLOCK_SHUFFLE
        else RANDOM_SEEDS
    )
    return _within_year_price_null(
        minute,
        "minute_start_ns",
        control=control,
        seeds=seeds,
    )


def _price_to_tick(price: float) -> int:
    if not np.isfinite(price) or price <= 0.0:
        raise V72ExecutedPriceOccupancyTripwireError(
            "null bridge contains an invalid ES price"
        )
    value = Decimal(str(price)) / Decimal(str(TICK_SIZE))
    return int(value.quantize(Decimal("1"), rounding=ROUND_HALF_DOWN))


def _verify_preserved_observables(
    real_minute: pd.DataFrame,
    real_feature: pd.DataFrame,
    null_minute: pd.DataFrame,
    null_feature: pd.DataFrame,
) -> None:
    for column in INVARIANT_FEATURE_COLUMNS:
        if not real_feature[column].equals(null_feature[column]):
            raise V72ExecutedPriceOccupancyTripwireError(
                f"executed-price occupancy null changed invariant feature: {column}"
            )
    for column in (
        "minute_start_ns",
        "availability_ns",
        "calendar_year",
        "contract",
        "trade_count",
        "total_volume",
        "buy_aggressor_volume",
        "sell_aggressor_volume",
        "unknown_side_volume",
    ):
        if not real_minute[column].equals(null_minute[column]):
            raise V72ExecutedPriceOccupancyTripwireError(
                f"executed-price occupancy null changed minute invariant: {column}"
            )


def _verify_invariant_source_counts(
    real: Mapping[str, Any], null: Mapping[str, Any]
) -> None:
    for field in ("minute_count", "session_count", "exact_source_match_count"):
        if int(real[field]) != int(null[field]):
            raise V72ExecutedPriceOccupancyTripwireError(
                f"executed-price occupancy null source-count drift: {field}"
            )


def _verify_inputs(root: Path, proof_registry_path: str | Path) -> None:
    expected = {
        TRIPWIRE_POLICY_PATH: TRIPWIRE_POLICY_SHA256,
        GRAMMAR_PATH: GRAMMAR_SHA256,
        ADDENDUM_PATH: ADDENDUM_SHA256,
        SIGNAL_MANIFEST_PATH: SIGNAL_MANIFEST_SHA256,
    }
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V72ExecutedPriceOccupancyTripwireError(
            "executed-price occupancy tripwire frozen input drift: "
            + ",".join(drift)
        )
    proof = load_and_verify(_proof_path(root, proof_registry_path))
    if multiplicity_trial_count(proof) < EXPECTED_GLOBAL_N_TRIALS:
        raise V72ExecutedPriceOccupancyTripwireError(
            "executed-price occupancy tripwire multiplicity reservation is absent"
        )
    by_id = {str(row["event_id"]): row for row in proof["entries"]}
    reservation = by_id.get(RESERVATION_EVENT_ID)
    if (
        reservation is None
        or int(reservation["multiplicity"]["delta_trials"]) != 120
        or int(reservation["multiplicity"]["cumulative_N_trials"])
        != EXPECTED_GLOBAL_N_TRIALS
    ):
        raise V72ExecutedPriceOccupancyTripwireError(
            "executed-price occupancy tripwire reservation provenance is incomplete"
        )
    if "Q4_2024" not in burned_window_ids(proof):
        raise V72ExecutedPriceOccupancyTripwireError(
            "Q4 is not irreversibly BURNED"
        )


def _verify_signal_manifest(
    root: Path, signals: Mapping[str, Any], specs: Mapping[str, Any]
) -> None:
    manifest = json.loads((root / SIGNAL_MANIFEST_PATH).read_text(encoding="utf-8"))
    rows = {str(row["candidate_id"]): row for row in manifest["candidate_paths"]}
    if (
        set(rows) != set(specs)
        or set(signals) != set(specs)
        or manifest.get("contains_outcomes_or_pnl") is not False
    ):
        raise V72ExecutedPriceOccupancyTripwireError(
            "executed-price occupancy tripwire signal manifest drift"
        )
    for candidate_id, spec in specs.items():
        if rows[candidate_id]["specification_hash"] != spec.specification_hash:
            raise V72ExecutedPriceOccupancyTripwireError(
                "executed-price occupancy tripwire specification drift"
            )
        if rows[candidate_id]["signal_path_hash"] != signal_path_hash(
            signals[candidate_id]
        ):
            raise V72ExecutedPriceOccupancyTripwireError(
                "executed-price occupancy tripwire signal path drift"
            )


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v72_executed_price_occupancy_tripwire_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    displayed = (
        result_path.relative_to(root) if result_path.is_relative_to(root) else result_path
    )
    report_path = destination / "v72_executed_price_occupancy_tripwire_report.md"
    ratio = result["NULL_RATIO"]
    ratio_text = "n/a" if ratio is None else f"{float(ratio):.6f}"
    report = "\n".join(
        [
            "# HYDRA V7.2 — Executed-price occupancy permanent tripwire",
            "",
            f"[HYDRA-V7] phase=4 step=208 verdict={result['verdict']}",
            f"gate=V72_G12_TRIPWIRE preuve={displayed}#{result_hash[:8]} tests=real_vs_3_reconstructed_print_null_worlds",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={result['N_trials']} burned=1",
            "diff_validation=hydra/validation/v72_executed_price_occupancy_tripwire.py,tests/test_v72_executed_price_occupancy_tripwire.py CONTRE=la_geometrie_residuelle_intra_minute_est_preserve",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Réel: `{result['raw_pass_counts']['real']}`",
            f"- Null: `{result['raw_pass_counts']['null']}`",
            f"- NULL_RATIO: `{ratio_text}`",
            f"- Force: `{result['evidence_strength']}`",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )
    validate_v7_report_text(report)
    report_path.write_text(report, encoding="utf-8")
    result["result_path"] = str(result_path)
    result["result_sha256"] = result_hash
    result["report_path"] = str(report_path)
    return result


def _proof_path(root: Path, path: str | Path) -> Path:
    result = Path(path)
    return result if result.is_absolute() else root / result


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "EXPECTED_GLOBAL_N_TRIALS",
    "INVARIANT_FEATURE_COLUMNS",
    "V72ExecutedPriceOccupancyTripwireError",
    "build_executed_price_occupancy_null_world",
    "reconstruct_minute_tick_path",
    "run_executed_price_occupancy_tripwire",
]
