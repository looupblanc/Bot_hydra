"""Sparse, read-only economic forensics over the sealed 0031 ledgers.

The 0031 pilot persisted executable trades and their causal signal provenance,
but its headline report intentionally contains only bounded campaign summaries.
This module reconstructs a transparent gross-to-net bridge and opportunity
deduplication audit directly from those immutable Parquet ledgers.  It does not
generate signals, rerun the event engine, mutate evidence, or promote a policy.

Two accounting levels are deliberately kept separate:

* ``realized_gross_usd`` is the executable gross PnL already containing the
  pilot's frozen adverse marketable-price adjustment.
* ``gross_reference_usd`` removes that fixed adjustment and, when exact BBO
  snapshots exist, also reconciles spread and depth into a residual mid-price
  move.  The bridge back to normal/stressed net is algebraically exact.

For passive trades the frozen ledger's ``base_slippage_cost_usd`` is a
two-sided diagnostic amount even though the implementation applies the fixed
adverse adjustment only on exit.  Treating it as an amount to subtract from
``gross_pnl_usd`` would double count costs.  The bridge below records both the
stored diagnostic amount and the amount actually embedded in executable PnL.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from statistics import fmean, median
from typing import Any, Iterable, Mapping, Sequence


FORENSICS_SCHEMA = "hydra_microstructure_sparse_forensics_v1"
OPPORTUNITY_CLUSTER_SCHEMA = "hydra_microstructure_opportunity_cluster_v1"


class SparseForensicsError(RuntimeError):
    """The immutable 0031 ledgers cannot support a deterministic audit."""


@dataclass(frozen=True, slots=True)
class SparseForensicsConfig:
    """Frozen interpretation contract for the 0031 sparse audit."""

    time_gap_ns: int = 30_000_000_000
    price_zone_ticks: int = 4
    account_mll_usd: float = 4_500.0
    account_target_usd: float = 9_000.0
    tick_size: Mapping[str, float] = field(
        default_factory=lambda: {"NQ": 0.25, "YM": 1.0}
    )
    point_value: Mapping[str, float] = field(
        default_factory=lambda: {"NQ": 20.0, "YM": 5.0}
    )
    duplicate_flag_fraction: float = 0.25
    bridge_tolerance_usd: float = 1e-6
    include_cluster_rows: bool = True

    def validate(self) -> None:
        if self.time_gap_ns <= 0 or self.price_zone_ticks <= 0:
            raise SparseForensicsError("opportunity clustering bounds must be positive")
        if self.account_mll_usd <= 0 or self.account_target_usd <= 0:
            raise SparseForensicsError("account target and MLL must be positive")
        if not 0.0 <= self.duplicate_flag_fraction <= 1.0:
            raise SparseForensicsError("duplicate flag fraction is invalid")
        for market in set(self.tick_size) | set(self.point_value):
            if (
                market not in self.tick_size
                or market not in self.point_value
                or float(self.tick_size[market]) <= 0
                or float(self.point_value[market]) <= 0
            ):
                raise SparseForensicsError(f"market economics incomplete: {market}")


def load_authoritative_0031_ledgers(
    pilot_dir: str | Path,
) -> dict[str, Any]:
    """Load only the bounded Parquet layers needed by the sparse audit.

    The function is read-only.  Dataset parts are read in lexical order and no
    artifact is created.  ``decision_report.json`` supplies the frozen session
    ordering; all economic rows come from the Parquet ledgers.
    """

    root = Path(pilot_dir).resolve()
    datasets = root / "datasets"
    if not datasets.is_dir():
        raise SparseForensicsError(f"0031 datasets directory is absent: {datasets}")

    columns: dict[str, Sequence[str] | None] = {
        "sleeve_manifests": None,
        "signals": None,
        "trades": None,
        "episodes": None,
        "account_daily_paths": None,
        "feature_matrices": (
            "feature_hash",
            "market",
            "session_id",
            "decision_ns",
            "available_ns",
            "spread_ticks",
        ),
        "book_snapshots": (
            "market",
            "session_id",
            "available_ns",
            "bid_price",
            "ask_price",
        ),
    }
    loaded = {
        name: _read_parquet_parts(datasets / name, requested)
        for name, requested in columns.items()
    }
    report_path = root / "decision_report.json"
    if not report_path.is_file():
        raise SparseForensicsError("0031 decision report is absent")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    sessions = report.get("selected_sessions")
    if not isinstance(sessions, list) or not sessions:
        raise SparseForensicsError("0031 selected session order is absent")
    loaded["selected_sessions"] = [str(value) for value in sessions]
    loaded["decision_report"] = report
    loaded["source_paths"] = {
        name: [str(path) for path in sorted((datasets / name).glob("*.parquet"))]
        for name in columns
    } | {"decision_report": [str(report_path)]}
    return loaded


def audit_authoritative_0031(
    pilot_dir: str | Path,
    *,
    config: SparseForensicsConfig | None = None,
) -> dict[str, Any]:
    """Load and audit the sealed pilot without replaying market events."""

    material = load_authoritative_0031_ledgers(pilot_dir)
    return analyze_sparse_forensics(
        sleeve_manifests=material["sleeve_manifests"],
        signals=material["signals"],
        trades=material["trades"],
        episodes=material["episodes"],
        account_daily_paths=material["account_daily_paths"],
        feature_matrices=material["feature_matrices"],
        book_snapshots=material["book_snapshots"],
        selected_sessions=material["selected_sessions"],
        source_paths=material["source_paths"],
        config=config,
    )


def analyze_sparse_forensics(
    *,
    sleeve_manifests: Sequence[Mapping[str, Any]],
    signals: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    episodes: Sequence[Mapping[str, Any]] = (),
    account_daily_paths: Sequence[Mapping[str, Any]] = (),
    feature_matrices: Sequence[Mapping[str, Any]] = (),
    book_snapshots: Sequence[Mapping[str, Any]] = (),
    selected_sessions: Sequence[str] = (),
    source_paths: Mapping[str, Sequence[str]] | None = None,
    config: SparseForensicsConfig | None = None,
) -> dict[str, Any]:
    """Return an exact, machine-readable economic and sparsity audit."""

    cfg = config or SparseForensicsConfig()
    cfg.validate()
    manifests = _unique_by(sleeve_manifests, "sleeve_id", "sleeve manifest")
    signal_by_id = _unique_by(signals, "signal_id", "signal")
    feature_by_hash = _unique_by(
        feature_matrices, "feature_hash", "feature matrix", allow_empty=True
    )
    snapshots = _snapshot_index(book_snapshots)

    unknown_signal_sleeves = {
        str(row.get("sleeve_id")) for row in signals
    } - set(manifests)
    unknown_trade_sleeves = {
        str(row.get("sleeve_id")) for row in trades
    } - set(manifests)
    if unknown_signal_sleeves or unknown_trade_sleeves:
        raise SparseForensicsError("signal/trade references an unknown sleeve")

    bridge_rows: list[dict[str, Any]] = []
    for trade in trades:
        signal_id = str(_required(trade, "signal_id"))
        signal = signal_by_id.get(signal_id)
        if signal is None:
            raise SparseForensicsError(f"trade has no source signal: {signal_id}")
        sleeve_id = str(_required(trade, "sleeve_id"))
        bridge_rows.append(
            _trade_bridge(
                trade,
                signal=signal,
                manifest=manifests[sleeve_id],
                feature=feature_by_hash.get(str(signal.get("feature_hash"))),
                snapshots=snapshots,
                cfg=cfg,
            )
        )

    clusters, trade_cluster = _opportunity_clusters(
        signals=signals,
        signal_by_id=signal_by_id,
        trades=trades,
        bridge_rows=bridge_rows,
        manifests=manifests,
        feature_by_hash=feature_by_hash,
        snapshots=snapshots,
        cfg=cfg,
    )
    for row in bridge_rows:
        cluster = trade_cluster.get(str(row["trade_id"]))
        row["opportunity_cluster_id"] = None if cluster is None else cluster["cluster_id"]
        row["duplicate_in_cluster"] = False if cluster is None else bool(
            cluster["primary_trade_id"] != row["trade_id"]
        )

    by_sleeve = _bridge_aggregates(
        bridge_rows,
        manifests=manifests,
        group_field="sleeve_id",
        cfg=cfg,
    )
    by_family = _bridge_aggregates(
        bridge_rows,
        manifests=manifests,
        group_field="family",
        cfg=cfg,
    )
    sleeve_activity, session_activity = _activity_tables(
        signals=signals,
        trades=trades,
        signal_by_id=signal_by_id,
        manifests=manifests,
    )
    episode_rows, episode_summary = _episode_trade_audit(
        episodes=episodes,
        trades=trades,
        selected_sessions=selected_sessions,
        cfg=cfg,
    )
    conflict = _potential_conflicts(
        signals=signals,
        manifests=manifests,
        feature_by_hash=feature_by_hash,
        snapshots=snapshots,
        cfg=cfg,
    )

    gross_to_net = _aggregate_bridge(bridge_rows, cfg=cfg)
    counts = {
        "sleeves": len(manifests),
        "signals": len(signals),
        "trades": len(trades),
        "episodes": len(episodes),
        "account_daily_paths": len(account_daily_paths),
        "feature_rows": len(feature_matrices),
        "book_snapshots": len(book_snapshots),
        "opportunity_clusters": len(clusters),
        "executed_opportunity_clusters": sum(
            int(row["trade_count"] > 0) for row in clusters
        ),
    }
    result: dict[str, Any] = {
        "schema": FORENSICS_SCHEMA,
        "config": _jsonable(asdict(cfg)),
        "source_paths": _jsonable(source_paths or {}),
        "counts": counts,
        "gross_to_net": gross_to_net,
        "bridge_by_sleeve": by_sleeve,
        "bridge_by_family": by_family,
        "sleeve_activity": sleeve_activity,
        "session_activity": session_activity,
        "episode_trade_rows": episode_rows,
        "episode_trade_summary": episode_summary,
        "opportunity_cluster_summary": _cluster_summary(clusters),
        "governor_and_conflicts": conflict,
        "limitations": {
            "governor_decision_ledger_available": False,
            "actual_suppression_attribution_available": False,
            "passive_entry_exact_bbo_is_partial": True,
            "intratrade_mll_crossing_timestamp_available": False,
            "statistically_independent_unit": "SESSION_NOT_SIGNAL_OR_TRADE",
            "opportunity_clusters_are_operational_dedup_units_not_iid_samples": True,
            "normal_and_stressed_episode_paths_may_end_on_different_terminal_trades": True,
        },
    }
    if cfg.include_cluster_rows:
        result["opportunity_clusters"] = clusters
    result["audit_hash"] = _stable_hash(result)
    return result


def _trade_bridge(
    trade: Mapping[str, Any],
    *,
    signal: Mapping[str, Any],
    manifest: Mapping[str, Any],
    feature: Mapping[str, Any] | None,
    snapshots: Mapping[tuple[str, int], Mapping[str, Any]],
    cfg: SparseForensicsConfig,
) -> dict[str, Any]:
    market = str(_required(trade, "market"))
    if market not in cfg.tick_size or market not in cfg.point_value:
        raise SparseForensicsError(f"market economics unavailable: {market}")
    point_value = float(cfg.point_value[market])
    tick = float(cfg.tick_size[market])
    direction = int(_required(trade, "direction"))
    quantity = float(_required(trade, "filled_quantity"))
    if direction not in {-1, 1} or quantity <= 0:
        raise SparseForensicsError("trade direction/quantity is invalid")

    entry_price = float(_required(trade, "entry_price"))
    exit_price = float(_required(trade, "exit_price"))
    realized = float(_required(trade, "gross_pnl_usd"))
    expected_gross = direction * (exit_price - entry_price) * point_value * quantity
    _require_close(realized, expected_gross, cfg, "gross PnL disagrees with fills")
    commission = float(_required(trade, "normal_costs_usd"))
    normal_net = float(_required(trade, "normal_net_pnl_usd"))
    stressed_cost = float(_required(trade, "stressed_costs_usd"))
    stressed_net = float(_required(trade, "stressed_net_pnl_usd"))
    stored_base = float(_required(trade, "base_slippage_cost_usd"))
    normal_total = float(_required(trade, "normal_total_cost_usd"))
    stressed_total = float(_required(trade, "stressed_total_cost_usd"))
    _require_close(normal_net, realized - commission, cfg, "normal bridge mismatch")
    _require_close(stressed_net, realized - stressed_cost, cfg, "stressed bridge mismatch")
    _require_close(normal_total, stored_base + commission, cfg, "normal total-cost mismatch")
    _require_close(stressed_total, stored_base + stressed_cost, cfg, "stressed total-cost mismatch")

    execution_path = str(_required(trade, "execution_path"))
    if execution_path == "AGGRESSIVE":
        marketable = stored_base
        entry_fixed = stored_base / 2.0
    elif execution_path == "PASSIVE":
        # The passive entry is the queue limit; only the aggressive exit has
        # the pilot's frozen adverse adjustment.
        marketable = stored_base / 2.0
        entry_fixed = 0.0
    else:
        raise SparseForensicsError(f"unsupported execution path: {execution_path}")
    exit_fixed = stored_base / 2.0

    signal_time = int(_required(signal, "decision_time_ns"))
    entry_time = int(_required(trade, "entry_time_ns"))
    exit_time = int(_required(trade, "exit_time_ns"))
    decision_snapshot = snapshots.get((market, signal_time))
    entry_snapshot_exact = snapshots.get((market, entry_time))
    exit_snapshot = snapshots.get((market, exit_time))
    entry_snapshot = entry_snapshot_exact or decision_snapshot
    entry_snapshot_source = (
        "EXACT_ENTRY"
        if entry_snapshot_exact is not None
        else "DECISION_PROXY"
        if decision_snapshot is not None
        else "UNAVAILABLE"
    )

    entry_half_spread = _half_spread_usd(entry_snapshot, point_value, quantity)
    exit_half_spread = _half_spread_usd(exit_snapshot, point_value, quantity)
    spread_complete = entry_half_spread is not None and exit_half_spread is not None
    if spread_complete:
        spread_component = (
            float(entry_half_spread) + float(exit_half_spread)
            if execution_path == "AGGRESSIVE"
            else float(exit_half_spread) - float(entry_half_spread)
        )
    else:
        # An entry-only feature spread is useful as a reported proxy but is not
        # inserted into the exact decomposition.
        spread_component = 0.0
    feature_spread_ticks = None if feature is None else _optional_float(feature.get("spread_ticks"))

    entry_depth, entry_depth_exact = _entry_depth_slippage(
        trade,
        snapshot=entry_snapshot,
        snapshot_exact=entry_snapshot_exact is not None,
        entry_fixed_usd=entry_fixed,
        point_value=point_value,
        quantity=quantity,
    )
    exit_depth, exit_depth_exact = _exit_depth_slippage(
        trade,
        snapshot=exit_snapshot,
        exit_fixed_usd=exit_fixed,
        point_value=point_value,
        quantity=quantity,
    )
    depth_complete = entry_depth_exact and exit_depth_exact
    depth = float(entry_depth + exit_depth) if depth_complete else 0.0

    # Algebraic residual after observable access frictions are stripped.  With
    # exact BBO coverage this is the mid-price/adverse-selection outcome; with
    # incomplete passive-entry coverage it deliberately absorbs the unknown.
    adverse_residual = realized + marketable + depth + spread_component
    reference = adverse_residual
    normal_rebuilt = reference - spread_component - depth - marketable - commission
    stressed_incremental = stressed_cost - commission
    stressed_rebuilt = (
        reference
        - spread_component
        - depth
        - marketable
        - commission
        - stressed_incremental
    )
    _require_close(normal_rebuilt, normal_net, cfg, "normal exact decomposition mismatch")
    _require_close(stressed_rebuilt, stressed_net, cfg, "stressed exact decomposition mismatch")

    planned_target = direction * (
        float(_required(trade, "target_price")) - entry_price
    ) * point_value * quantity
    return {
        "trade_id": str(_required(trade, "trade_id")),
        "signal_id": str(_required(trade, "signal_id")),
        "sleeve_id": str(_required(trade, "sleeve_id")),
        "family": str(_required(manifest, "family")),
        "deployability_tier": str(manifest.get("deployability_tier") or "UNKNOWN"),
        "market": market,
        "session_id": str(_required(trade, "session_id")),
        "side": "LONG" if direction > 0 else "SHORT",
        "execution_path": execution_path,
        "filled_quantity": quantity,
        "planned_target_gross_usd": planned_target,
        "gross_reference_usd": reference,
        "realized_gross_usd": realized,
        "spread_component_usd": spread_component,
        "entry_feature_spread_ticks": feature_spread_ticks,
        "commission_usd": commission,
        "marketable_slippage_usd": marketable,
        "stored_two_sided_base_slippage_usd": stored_base,
        "depth_slippage_usd": depth,
        "adverse_selection_residual_usd": adverse_residual,
        "stressed_incremental_slippage_usd": stressed_incremental,
        "normal_net_usd": normal_net,
        "stressed_net_usd": stressed_net,
        "normal_bridge_residual_usd": normal_net - normal_rebuilt,
        "stressed_bridge_residual_usd": stressed_net - stressed_rebuilt,
        "entry_snapshot_source": entry_snapshot_source,
        "spread_decomposition_complete": spread_complete,
        "depth_decomposition_complete": depth_complete,
        "entry_time_ns": entry_time,
        "exit_time_ns": exit_time,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "round_trip_price_notional_usd": (
            abs(entry_price) + abs(exit_price)
        )
        * point_value
        * quantity,
    }


def _opportunity_clusters(
    *,
    signals: Sequence[Mapping[str, Any]],
    signal_by_id: Mapping[str, Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    bridge_rows: Sequence[Mapping[str, Any]],
    manifests: Mapping[str, Mapping[str, Any]],
    feature_by_hash: Mapping[str, Mapping[str, Any]],
    snapshots: Mapping[tuple[str, int], Mapping[str, Any]],
    cfg: SparseForensicsConfig,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    del signal_by_id
    trades_by_signal: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    bridge_by_trade = {str(row["trade_id"]): row for row in bridge_rows}
    for trade in trades:
        trades_by_signal[str(_required(trade, "signal_id"))].append(trade)

    grouped: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        sleeve_id = str(_required(signal, "sleeve_id"))
        manifest = manifests[sleeve_id]
        market = str(_required(signal, "market"))
        direction = int(_required(signal, "direction"))
        decision_ns = int(_required(signal, "decision_time_ns"))
        price, price_source = _signal_reference_price(
            signal,
            feature=feature_by_hash.get(str(signal.get("feature_hash"))),
            snapshots=snapshots,
            matched_trades=trades_by_signal.get(str(_required(signal, "signal_id")), ()),
        )
        zone = _price_zone(price, market=market, cfg=cfg)
        grouped[
            (
                market,
                str(_required(manifest, "family")),
                "LONG" if direction > 0 else "SHORT",
                str(_required(signal, "session_id")),
                zone,
            )
        ].append(
            {
                "signal": signal,
                "decision_ns": decision_ns,
                "price_source": price_source,
                "trades": list(trades_by_signal.get(str(signal["signal_id"]), ())),
            }
        )

    clusters: list[dict[str, Any]] = []
    trade_cluster: dict[str, dict[str, Any]] = {}
    for group_key in sorted(grouped):
        atoms = sorted(
            grouped[group_key],
            key=lambda row: (
                int(row["decision_ns"]),
                str(row["signal"]["sleeve_id"]),
                str(row["signal"]["signal_id"]),
            ),
        )
        batches: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        previous_time: int | None = None
        for atom in atoms:
            timestamp = int(atom["decision_ns"])
            if previous_time is None or timestamp - previous_time <= cfg.time_gap_ns:
                current.append(atom)
            else:
                batches.append(current)
                current = [atom]
            previous_time = timestamp
        if current:
            batches.append(current)

        for ordinal, batch in enumerate(batches):
            market, family, side, session_id, zone = group_key
            cluster_id = _stable_hash(
                {
                    "schema": OPPORTUNITY_CLUSTER_SCHEMA,
                    "group": list(group_key),
                    "ordinal": ordinal,
                    "first_decision_ns": int(batch[0]["decision_ns"]),
                }
            )
            cluster_trades = sorted(
                [trade for atom in batch for trade in atom["trades"]],
                key=lambda row: (
                    int(row["entry_time_ns"]),
                    str(row["sleeve_id"]),
                    str(row["trade_id"]),
                ),
            )
            primary_trade = (
                None if not cluster_trades else str(cluster_trades[0]["trade_id"])
            )
            duplicates = cluster_trades[1:] if cluster_trades else []
            bridges = [bridge_by_trade[str(row["trade_id"])] for row in cluster_trades]
            signal_to_trade_ids = {
                str(atom["signal"]["signal_id"]): [
                    str(trade["trade_id"]) for trade in atom["trades"]
                ]
                for atom in batch
            }
            row = {
                "schema": OPPORTUNITY_CLUSTER_SCHEMA,
                "cluster_id": cluster_id,
                "market": market,
                "family": family,
                "side": side,
                "session_id": session_id,
                "price_zone": zone,
                "first_decision_ns": int(batch[0]["decision_ns"]),
                "last_decision_ns": int(batch[-1]["decision_ns"]),
                "signal_count": len(batch),
                "signal_ids": sorted(signal_to_trade_ids),
                "trade_ids": [str(trade["trade_id"]) for trade in cluster_trades],
                "signal_to_trade_ids": signal_to_trade_ids,
                "unique_feature_count": len(
                    {
                        str(atom["signal"].get("feature_hash")) for atom in batch
                    }
                ),
                "sleeve_count": len(
                    {str(atom["signal"]["sleeve_id"]) for atom in batch}
                ),
                "trade_count": len(cluster_trades),
                "primary_trade_id": primary_trade,
                "duplicate_trade_count": len(duplicates),
                "duplicate_round_trip_price_notional_usd": math.fsum(
                    float(bridge_by_trade[str(item["trade_id"])]["round_trip_price_notional_usd"])
                    for item in duplicates
                ),
                "duplicate_normal_cost_usd": math.fsum(
                    float(bridge_by_trade[str(item["trade_id"])]["commission_usd"])
                    for item in duplicates
                ),
                "gross_reference_usd": math.fsum(
                    float(item["gross_reference_usd"]) for item in bridges
                ),
                "realized_gross_usd": math.fsum(
                    float(item["realized_gross_usd"]) for item in bridges
                ),
                "normal_net_usd": math.fsum(
                    float(item["normal_net_usd"]) for item in bridges
                ),
                "stressed_net_usd": math.fsum(
                    float(item["stressed_net_usd"]) for item in bridges
                ),
                "price_source_counts": dict(
                    sorted(Counter(str(atom["price_source"]) for atom in batch).items())
                ),
            }
            clusters.append(row)
            for trade in cluster_trades:
                trade_id = str(trade["trade_id"])
                if trade_id in trade_cluster:
                    raise SparseForensicsError("trade belongs to multiple opportunity clusters")
                trade_cluster[trade_id] = row
    return sorted(clusters, key=lambda row: str(row["cluster_id"])), trade_cluster


def _bridge_aggregates(
    rows: Sequence[Mapping[str, Any]],
    *,
    manifests: Mapping[str, Mapping[str, Any]],
    group_field: str,
    cfg: SparseForensicsConfig,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row[group_field])].append(row)
    result = []
    for group_id in sorted(grouped):
        selected = grouped[group_id]
        aggregate = _aggregate_bridge(selected, cfg=cfg)
        duplicate_count = sum(bool(row.get("duplicate_in_cluster")) for row in selected)
        duplicate_turnover = math.fsum(
            float(row["round_trip_price_notional_usd"])
            for row in selected
            if row.get("duplicate_in_cluster")
        )
        aggregate.update(
            {
                "group_id": group_id,
                "group_type": group_field,
                "duplicate_trade_count": duplicate_count,
                "duplicate_trade_fraction": duplicate_count / max(1, len(selected)),
                "duplicate_round_trip_price_notional_usd": duplicate_turnover,
                "classification": _classification(aggregate),
                "diagnostic_classification": _diagnostic_classification(
                    aggregate
                ),
            }
        )
        if group_field == "sleeve_id":
            manifest = manifests[group_id]
            aggregate.update(
                {
                    "family": str(manifest["family"]),
                    "market": str(manifest["market"]),
                    "deployability_tier": str(manifest.get("deployability_tier") or "UNKNOWN"),
                    "duplicate_turnover_flag": (
                        aggregate["duplicate_trade_fraction"]
                        >= cfg.duplicate_flag_fraction
                    ),
                }
            )
        result.append(aggregate)
    return result


def _aggregate_bridge(
    rows: Sequence[Mapping[str, Any]], *, cfg: SparseForensicsConfig
) -> dict[str, Any]:
    sum_fields = (
        "planned_target_gross_usd",
        "gross_reference_usd",
        "realized_gross_usd",
        "spread_component_usd",
        "commission_usd",
        "marketable_slippage_usd",
        "stored_two_sided_base_slippage_usd",
        "depth_slippage_usd",
        "adverse_selection_residual_usd",
        "stressed_incremental_slippage_usd",
        "normal_net_usd",
        "stressed_net_usd",
        "round_trip_price_notional_usd",
    )
    result: dict[str, Any] = {
        "trade_count": len(rows),
        **{
            field: math.fsum(float(row[field]) for row in rows)
            for field in sum_fields
        },
    }
    result.update(
        {
            "normal_bridge_residual_usd": math.fsum(
                float(row["normal_bridge_residual_usd"]) for row in rows
            ),
            "stressed_bridge_residual_usd": math.fsum(
                float(row["stressed_bridge_residual_usd"]) for row in rows
            ),
            "spread_complete_trade_count": sum(
                bool(row["spread_decomposition_complete"]) for row in rows
            ),
            "depth_complete_trade_count": sum(
                bool(row["depth_decomposition_complete"]) for row in rows
            ),
            "gross_positive_trade_count": sum(
                float(row["realized_gross_usd"]) > 0.0 for row in rows
            ),
            "normal_positive_trade_count": sum(
                float(row["normal_net_usd"]) > 0.0 for row in rows
            ),
            "stressed_positive_trade_count": sum(
                float(row["stressed_net_usd"]) > 0.0 for row in rows
            ),
        }
    )
    _require_close(
        float(result["normal_bridge_residual_usd"]),
        0.0,
        cfg,
        "aggregate normal bridge is not exact",
    )
    _require_close(
        float(result["stressed_bridge_residual_usd"]),
        0.0,
        cfg,
        "aggregate stressed bridge is not exact",
    )
    return result


def _classification(row: Mapping[str, Any]) -> str:
    """Return exactly one preregistered 0032 economic classification."""

    if float(row["gross_reference_usd"]) <= 0.0:
        return "NO_GROSS_ALPHA"
    if float(row["stressed_net_usd"]) <= 0.0:
        return "COST_AND_TURNOVER_DOMINATED"
    return "SPARSE_ALPHA_CANDIDATE"


def _diagnostic_classification(row: Mapping[str, Any]) -> str:
    """Retain the finer bridge diagnosis without changing the gate taxonomy."""

    if float(row["gross_reference_usd"]) <= 0.0:
        return "NEGATIVE_REFERENCE_EDGE"
    if float(row["realized_gross_usd"]) <= 0.0:
        return "EXECUTION_FRICTION_DOMINATED"
    if float(row["normal_net_usd"]) <= 0.0:
        return "COMMISSION_DOMINATED"
    if float(row["stressed_net_usd"]) <= 0.0:
        return "STRESS_COST_FRAGILE"
    return "STRESSED_POSITIVE"


def _activity_tables(
    *,
    signals: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    signal_by_id: Mapping[str, Mapping[str, Any]],
    manifests: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    signals_by_sleeve: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    trades_by_sleeve: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in signals:
        signals_by_sleeve[str(row["sleeve_id"])].append(row)
    for row in trades:
        trades_by_sleeve[str(row["sleeve_id"])].append(row)
    sleeve_rows = []
    for sleeve_id in sorted(manifests):
        sleeve_signals = signals_by_sleeve.get(sleeve_id, [])
        sleeve_trades = trades_by_sleeve.get(sleeve_id, [])
        session_counts = {
            session: {
                "signal_count": sum(
                    str(row["session_id"]) == session for row in sleeve_signals
                ),
                "trade_count": sum(
                    str(row["session_id"]) == session for row in sleeve_trades
                ),
            }
            for session in sorted(
                {
                    str(row["session_id"])
                    for row in (*sleeve_signals, *sleeve_trades)
                }
            )
        }
        sleeve_rows.append(
            {
                "sleeve_id": sleeve_id,
                "family": str(manifests[sleeve_id]["family"]),
                "market": str(manifests[sleeve_id]["market"]),
                "signal_count": len(sleeve_signals),
                "trade_count": len(sleeve_trades),
                "fill_fraction": len(sleeve_trades) / max(1, len(sleeve_signals)),
                "session_counts": session_counts,
            }
        )

    sessions = sorted(
        {str(row["session_id"]) for row in signals}
        | {str(row["session_id"]) for row in trades}
    )
    session_rows = []
    for session in sessions:
        session_signals = [row for row in signals if str(row["session_id"]) == session]
        session_trades = [row for row in trades if str(row["session_id"]) == session]
        session_rows.append(
            {
                "session_id": session,
                "signal_count": len(session_signals),
                "unique_signal_feature_count": len(
                    {str(row.get("feature_hash")) for row in session_signals}
                ),
                "trade_count": len(session_trades),
                "unique_trade_feature_count": len(
                    {
                        str(signal_by_id[str(row["signal_id"])].get("feature_hash"))
                        for row in session_trades
                    }
                ),
                "realized_gross_usd": math.fsum(
                    float(row["gross_pnl_usd"]) for row in session_trades
                ),
                "normal_net_usd": math.fsum(
                    float(row["normal_net_pnl_usd"]) for row in session_trades
                ),
                "stressed_net_usd": math.fsum(
                    float(row["stressed_net_pnl_usd"]) for row in session_trades
                ),
            }
        )
    return sleeve_rows, session_rows


def _episode_trade_audit(
    *,
    episodes: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    selected_sessions: Sequence[str],
    cfg: SparseForensicsConfig,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not episodes:
        return [], []
    if not selected_sessions:
        raise SparseForensicsError("episode audit requires frozen session ordering")
    session_index = {str(value): index for index, value in enumerate(selected_sessions)}
    by_sleeve_session: dict[str, dict[str, list[Mapping[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for trade in trades:
        by_sleeve_session[str(trade["sleeve_id"])][str(trade["session_id"])].append(trade)
    for by_session in by_sleeve_session.values():
        for rows in by_session.values():
            rows.sort(key=lambda row: (int(row["exit_time_ns"]), str(row["trade_id"])))

    detail: list[dict[str, Any]] = []
    for episode in episodes:
        sleeve_id = str(_required(episode, "sleeve_id"))
        start_session = str(_required(episode, "start_session"))
        if start_session not in session_index:
            raise SparseForensicsError("episode start is absent from session ordering")
        scenario = str(_required(episode, "scenario"))
        net_field = (
            "normal_net_pnl_usd"
            if scenario == "NORMAL"
            else "stressed_net_pnl_usd"
            if scenario == "STRESSED_1_5X"
            else None
        )
        cost_field = (
            "normal_costs_usd"
            if scenario == "NORMAL"
            else "stressed_costs_usd"
            if scenario == "STRESSED_1_5X"
            else None
        )
        if net_field is None or cost_field is None:
            raise SparseForensicsError(f"unsupported episode cost scenario: {scenario}")
        horizon = int(_required(episode, "horizon_days"))
        offset = session_index[start_session]
        included = selected_sessions[offset : min(len(selected_sessions), offset + horizon)]
        cumulative = 0.0
        trailing_high = 0.0
        used = 0
        used_costs = 0.0
        used_sessions: Counter[str] = Counter()
        breached = False
        for session in included:
            day_pnl = 0.0
            for trade in by_sleeve_session[sleeve_id].get(str(session), ()):
                minimum_equity = cumulative + day_pnl + min(
                    0.0, float(trade["minimum_unrealized_pnl_usd"])
                )
                loss_limit = max(-cfg.account_mll_usd, trailing_high - cfg.account_mll_usd)
                day_pnl += float(trade[net_field])
                used_costs += float(trade[cost_field])
                used += 1
                used_sessions[str(session)] += 1
                if minimum_equity < loss_limit:
                    breached = True
                    break
            cumulative += day_pnl
            trailing_high = max(trailing_high, cumulative)
            if breached:
                break
        _require_close(
            cumulative,
            float(_required(episode, "net_pnl_usd")),
            cfg,
            "episode trade count does not reconcile net PnL",
        )
        _require_close(
            used_costs,
            float(_required(episode, "costs_usd")),
            cfg,
            "episode trade count does not reconcile costs",
        )
        detail.append(
            {
                "episode_id": str(episode["episode_id"]),
                "sleeve_id": sleeve_id,
                "scenario": scenario,
                "horizon_days": horizon,
                "full_coverage": bool(episode["full_coverage"]),
                "coverage_status": str(episode["coverage_status"]),
                "trade_count": used,
                "session_trade_counts": dict(sorted(used_sessions.items())),
            }
        )

    grouped: dict[tuple[int, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in detail:
        grouped[(int(row["horizon_days"]), str(row["scenario"]))].append(row)
    summary = []
    for (horizon, scenario), rows in sorted(grouped.items()):
        values = [int(row["trade_count"]) for row in rows]
        summary.append(
            {
                "horizon_days": horizon,
                "scenario": scenario,
                "episode_count": len(rows),
                "full_coverage_count": sum(bool(row["full_coverage"]) for row in rows),
                "trade_occurrence_count": sum(values),
                "mean_trades_per_episode": fmean(values),
                "median_trades_per_episode": median(values),
                "minimum_trades_per_episode": min(values),
                "maximum_trades_per_episode": max(values),
                "terminal_counts": dict(
                    sorted(Counter(str(row["coverage_status"]) for row in rows).items())
                ),
            }
        )
    return detail, summary


def _potential_conflicts(
    *,
    signals: Sequence[Mapping[str, Any]],
    manifests: Mapping[str, Mapping[str, Any]],
    feature_by_hash: Mapping[str, Mapping[str, Any]],
    snapshots: Mapping[tuple[str, int], Mapping[str, Any]],
    cfg: SparseForensicsConfig,
) -> dict[str, Any]:
    del feature_by_hash
    grouped: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for signal in signals:
        market = str(signal["market"])
        timestamp = int(signal["decision_time_ns"])
        snapshot = snapshots.get((market, timestamp))
        price = (
            None
            if snapshot is None
            else (float(snapshot["bid_price"]) + float(snapshot["ask_price"])) / 2.0
        )
        grouped[
            (
                market,
                str(signal["session_id"]),
                _price_zone(price, market=market, cfg=cfg),
            )
        ].append(signal)
    potential_clusters = 0
    potential_signals = 0
    multi_sleeve_clusters = 0
    for key in grouped:
        rows = sorted(grouped[key], key=lambda row: int(row["decision_time_ns"]))
        current: list[Mapping[str, Any]] = []
        previous: int | None = None
        for row in rows + [None]:  # type: ignore[list-item]
            timestamp = None if row is None else int(row["decision_time_ns"])
            if (
                row is not None
                and (previous is None or timestamp - previous <= cfg.time_gap_ns)
            ):
                current.append(row)
                previous = timestamp
                continue
            if current:
                sides = {int(value["direction"]) for value in current}
                sleeves = {str(value["sleeve_id"]) for value in current}
                if len(sides) > 1:
                    potential_clusters += 1
                    potential_signals += len(current)
                if len(sleeves) > 1:
                    multi_sleeve_clusters += 1
            current = [] if row is None else [row]
            previous = timestamp
    return {
        "governor_evidence_status": "UNAVAILABLE_STANDALONE_SLEEVE_PILOT",
        "observed_governor_decision_count": 0,
        "observed_conflict_rejection_count": 0,
        "actual_suppressed_signal_count": None,
        "potential_opposite_side_cluster_count": potential_clusters,
        "signals_in_potential_opposite_side_clusters": potential_signals,
        "multi_sleeve_cluster_count": multi_sleeve_clusters,
        "potential_is_not_observed_suppression": True,
        "manifest_family_count": len(
            {str(value["family"]) for value in manifests.values()}
        ),
    }


def _cluster_summary(clusters: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    executed = [row for row in clusters if int(row["trade_count"]) > 0]
    signal_count = sum(int(row["signal_count"]) for row in clusters)
    trade_count = sum(int(row["trade_count"]) for row in clusters)
    duplicate_count = sum(int(row["duplicate_trade_count"]) for row in clusters)
    return {
        "schema": OPPORTUNITY_CLUSTER_SCHEMA,
        "cluster_count": len(clusters),
        "executed_cluster_count": len(executed),
        "signal_count": signal_count,
        "trade_count": trade_count,
        "duplicate_trade_count": duplicate_count,
        "duplicate_trade_fraction": duplicate_count / max(1, trade_count),
        "duplicate_round_trip_price_notional_usd": math.fsum(
            float(row["duplicate_round_trip_price_notional_usd"])
            for row in clusters
        ),
        "duplicate_normal_cost_usd": math.fsum(
            float(row["duplicate_normal_cost_usd"]) for row in clusters
        ),
        "clusters_by_market": dict(
            sorted(Counter(str(row["market"]) for row in clusters).items())
        ),
        "clusters_by_family": dict(
            sorted(Counter(str(row["family"]) for row in clusters).items())
        ),
        "clusters_by_side": dict(
            sorted(Counter(str(row["side"]) for row in clusters).items())
        ),
        "clusters_by_session": dict(
            sorted(Counter(str(row["session_id"]) for row in clusters).items())
        ),
    }


def _snapshot_index(
    rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, int], Mapping[str, Any]]:
    result: dict[tuple[str, int], Mapping[str, Any]] = {}
    for row in rows:
        key = (str(_required(row, "market")), int(_required(row, "available_ns")))
        previous = result.get(key)
        if previous is not None and previous != row:
            raise SparseForensicsError(f"ambiguous BBO snapshot: {key}")
        result[key] = row
    return result


def _entry_depth_slippage(
    trade: Mapping[str, Any],
    *,
    snapshot: Mapping[str, Any] | None,
    snapshot_exact: bool,
    entry_fixed_usd: float,
    point_value: float,
    quantity: float,
) -> tuple[float, bool]:
    if snapshot is None or not snapshot_exact:
        return 0.0, False
    direction = int(trade["direction"])
    fixed_price = entry_fixed_usd / (point_value * quantity)
    raw_entry = float(trade["entry_price"]) - direction * fixed_price
    touch = float(snapshot["ask_price"] if direction > 0 else snapshot["bid_price"])
    return max(0.0, direction * (raw_entry - touch) * point_value * quantity), True


def _exit_depth_slippage(
    trade: Mapping[str, Any],
    *,
    snapshot: Mapping[str, Any] | None,
    exit_fixed_usd: float,
    point_value: float,
    quantity: float,
) -> tuple[float, bool]:
    if snapshot is None:
        return 0.0, False
    direction = int(trade["direction"])
    fixed_price = exit_fixed_usd / (point_value * quantity)
    raw_exit = float(trade["exit_price"]) + direction * fixed_price
    touch = float(snapshot["bid_price"] if direction > 0 else snapshot["ask_price"])
    return max(0.0, direction * (touch - raw_exit) * point_value * quantity), True


def _half_spread_usd(
    snapshot: Mapping[str, Any] | None, point_value: float, quantity: float
) -> float | None:
    if snapshot is None:
        return None
    spread = float(snapshot["ask_price"]) - float(snapshot["bid_price"])
    if not math.isfinite(spread) or spread < 0:
        raise SparseForensicsError("BBO spread is invalid")
    return 0.5 * spread * point_value * quantity


def _signal_reference_price(
    signal: Mapping[str, Any],
    *,
    feature: Mapping[str, Any] | None,
    snapshots: Mapping[tuple[str, int], Mapping[str, Any]],
    matched_trades: Sequence[Mapping[str, Any]],
) -> tuple[float | None, str]:
    market = str(signal["market"])
    decision_ns = int(signal["decision_time_ns"])
    snapshot = snapshots.get((market, decision_ns))
    if snapshot is not None:
        return (
            float(snapshot["bid_price"]) + float(snapshot["ask_price"])
        ) / 2.0, "DECISION_BBO_MID"
    if matched_trades:
        return float(matched_trades[0]["entry_price"]), "EXECUTED_ENTRY_FALLBACK"
    if feature is not None and _optional_float(feature.get("spread_ticks")) is not None:
        return None, "FEATURE_WITHOUT_ABSOLUTE_PRICE"
    return None, "UNAVAILABLE"


def _price_zone(
    price: float | None, *, market: str, cfg: SparseForensicsConfig
) -> str:
    if price is None or not math.isfinite(price):
        return "UNPRICED"
    width = float(cfg.tick_size[market]) * cfg.price_zone_ticks
    return str(math.floor(price / width))


def _read_parquet_parts(
    directory: Path, columns: Sequence[str] | None
) -> list[dict[str, Any]]:
    paths = sorted(directory.glob("*.parquet"))
    if not paths:
        raise SparseForensicsError(f"Parquet dataset is absent: {directory}")
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - production dependency guard
        raise SparseForensicsError("pyarrow is required for 0031 Parquet audit") from exc
    rows: list[dict[str, Any]] = []
    for path in paths:
        selected = None if columns is None else list(columns)
        rows.extend(pq.read_table(path, columns=selected).to_pylist())
    return rows


def _unique_by(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    label: str,
    *,
    allow_empty: bool = False,
) -> dict[str, Mapping[str, Any]]:
    if not rows and not allow_empty:
        raise SparseForensicsError(f"{label} ledger is empty")
    result: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        key = str(_required(row, field))
        if key in result:
            raise SparseForensicsError(f"duplicate {label}: {key}")
        result[key] = row
    return result


def _required(row: Mapping[str, Any], field: str) -> Any:
    value = row.get(field)
    if value is None:
        raise SparseForensicsError(f"required field is absent: {field}")
    return value


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _require_close(
    observed: float,
    expected: float,
    cfg: SparseForensicsConfig,
    message: str,
) -> None:
    if not math.isclose(
        float(observed),
        float(expected),
        rel_tol=1e-12,
        abs_tol=cfg.bridge_tolerance_usd,
    ):
        raise SparseForensicsError(
            f"{message}: observed={observed!r} expected={expected!r}"
        )


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _stable_hash(value: Any) -> str:
    payload = json.dumps(
        _jsonable(value), sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


__all__ = [
    "FORENSICS_SCHEMA",
    "OPPORTUNITY_CLUSTER_SCHEMA",
    "SparseForensicsConfig",
    "SparseForensicsError",
    "analyze_sparse_forensics",
    "audit_authoritative_0031",
    "load_authoritative_0031_ledgers",
]
