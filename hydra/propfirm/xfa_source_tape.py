"""Immutable XFA source-event tape reconstructed without signal evaluation.

Campaign 0026 persisted authoritative signal, entry, exit and trade ledgers, but
the compact trade ledger intentionally omitted the intra-trade adverse and
favourable excursion fields required by the XFA MLL state machine.  This module
rehydrates only those two path fields from the already frozen OHLC arrays.  It
never evaluates a feature threshold, calls ``_signal_positions`` or replays a
Combine account.

The resulting tape is a reusable input for payout-policy counterfactuals.  Its
events retain the exact entry/exit identities and economics of the sealed
EvidenceBundle and can be hashed or written once to ignored cache storage.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from hydra.account_policy.basket import RoutedTrade
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import (
    iter_evidence_records,
    require_complete_evidence_bundle,
)
from hydra.markets.instruments import instrument_spec
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.scaling_plan import mini_equivalent


XFA_SOURCE_TAPE_SCHEMA = "hydra_xfa_source_event_tape_v1"
XFA_SOURCE_EVENT_SCHEMA = "hydra_xfa_source_event_v1"
MINUTE_NS = 60_000_000_000


class XfaSourceTapeError(RuntimeError):
    """The immutable ledgers cannot be reconciled into an exact XFA tape."""


@dataclass(frozen=True, slots=True)
class XfaSourceTape:
    schema: str
    campaign_id: str
    events: Mapping[str, tuple[RoutedTrade, ...]]
    eligible_session_days: tuple[int, ...]
    event_count: int
    component_count: int
    normal_net_pnl: float
    normal_gross_pnl: float
    source_manifest_sha256: str
    feature_bundle_hashes: Mapping[str, str]
    tape_hash: str

    def __post_init__(self) -> None:
        if self.schema != XFA_SOURCE_TAPE_SCHEMA:
            raise XfaSourceTapeError("XFA source-tape schema drift")
        if self.event_count != sum(len(rows) for rows in self.events.values()):
            raise XfaSourceTapeError("XFA source-tape event count drift")
        if self.component_count != len(self.events):
            raise XfaSourceTapeError("XFA source-tape component count drift")
        if not self.eligible_session_days:
            raise XfaSourceTapeError("XFA source-tape calendar is empty")
        if self.tape_hash != stable_hash(self._hash_payload()):
            raise XfaSourceTapeError("XFA source-tape hash drift")

    def _hash_payload(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "campaign_id": self.campaign_id,
            "events": {
                component_id: [row.to_dict() for row in self.events[component_id]]
                for component_id in sorted(self.events)
            },
            "eligible_session_days": list(self.eligible_session_days),
            "event_count": self.event_count,
            "component_count": self.component_count,
            "normal_net_pnl": self.normal_net_pnl,
            "normal_gross_pnl": self.normal_gross_pnl,
            "source_manifest_sha256": self.source_manifest_sha256,
            "feature_bundle_hashes": dict(sorted(self.feature_bundle_hashes.items())),
        }

    def manifest(self) -> dict[str, Any]:
        """Return the compact, Git-safe tape manifest (events stay in cache)."""

        all_events = [row.event for rows in self.events.values() for row in rows]
        event_ids = [event.event_id for event in all_events]
        return {
            **{
                key: value
                for key, value in self._hash_payload().items()
                if key != "events"
            },
            "event_ids_sha256": stable_hash(
                {
                    component_id: [row.event.event_id for row in rows]
                    for component_id, rows in sorted(self.events.items())
                }
            ),
            "unique_event_id_count": len(set(event_ids)),
            "normal_costs": self.normal_gross_pnl - self.normal_net_pnl,
            "minimum_worst_unrealized_pnl": min(
                event.worst_unrealized_pnl for event in all_events
            ),
            "maximum_best_unrealized_pnl": max(
                event.best_unrealized_pnl for event in all_events
            ),
            "tape_hash": self.tape_hash,
            "signal_recalculation_performed": False,
            "combine_replay_performed": False,
            "market_feature_recalculation_performed": False,
            "ohlc_arrays_used_only_for_frozen_trade_excursions": True,
        }


@dataclass(frozen=True, slots=True)
class _FeatureArrays:
    market: str
    bundle_hash: str
    decision_ns: np.ndarray
    timestamp_ns: np.ndarray
    entry_price: np.ndarray
    bar_high: np.ndarray
    bar_low: np.ndarray
    segment_code: np.ndarray
    session_day: np.ndarray
    session_code: np.ndarray


def build_xfa_source_tape(
    *,
    evidence_bundle_path: str | Path,
    feature_cache_root: str | Path,
    runtime_summaries_path: str | Path,
    campaign_id: str,
    expected_event_count: int | None = 2_052,
    expected_component_count: int | None = 18,
    verify_bundle: bool = True,
) -> XfaSourceTape:
    """Rehydrate the exact XFA event tape from frozen ledgers and OHLC arrays.

    ``component_entries``, ``component_exits`` and ``component_trades`` are the
    sole source of trade identity and realized economics.  Cached bar high/low
    arrays are consulted only between each immutable entry and exit to restore
    the MLL excursion bounds that were present in the original ``RoutedTrade``.
    """

    bundle = Path(evidence_bundle_path).resolve()
    if verify_bundle:
        manifest = require_complete_evidence_bundle(
            bundle, campaign_id=campaign_id, deep=False
        )
    else:
        manifest = _load_json(bundle / "evidence_bundle_manifest.json")
        if str(manifest.get("campaign_id")) != campaign_id:
            raise XfaSourceTapeError("source EvidenceBundle campaign drift")

    entries = _keyed_rows(
        iter_evidence_records(bundle, "component_entries"), "component entries"
    )
    exits = _keyed_rows(
        iter_evidence_records(bundle, "component_exits"), "component exits"
    )
    trades = _keyed_rows(
        iter_evidence_records(bundle, "component_trades"), "component trades"
    )
    if not (set(entries) == set(exits) == set(trades)):
        raise XfaSourceTapeError("entry/exit/trade identities do not reconcile")
    if expected_event_count is not None and len(trades) != expected_event_count:
        raise XfaSourceTapeError(
            f"XFA source event count drift: {len(trades)} != {expected_event_count}"
        )

    trade_ids = [key[1] for key in trades]
    if len(set(trade_ids)) != len(trade_ids):
        raise XfaSourceTapeError("component trade IDs are not globally unique")
    required_markets = {str(row.get("market") or "") for row in trades.values()}
    if "" in required_markets:
        raise XfaSourceTapeError("component trade market is missing")
    arrays = _discover_feature_arrays(
        Path(feature_cache_root).resolve(), required_markets=required_markets
    )
    events: dict[str, list[RoutedTrade]] = {}
    normal_net = 0.0
    normal_gross = 0.0
    for key in sorted(trades):
        trade = trades[key]
        entry = entries[key]
        exit_row = exits[key]
        routed = _rehydrate_trade(trade, entry, exit_row, arrays)
        events.setdefault(routed.component_id, []).append(routed)
        normal_net += routed.event.net_pnl
        normal_gross += routed.event.gross_pnl

    frozen = {
        component_id: tuple(
            sorted(
                rows,
                key=lambda row: (
                    row.event.session_day,
                    row.event.decision_ns,
                    row.event.event_id,
                ),
            )
        )
        for component_id, rows in sorted(events.items())
    }
    if expected_component_count is not None and len(frozen) != expected_component_count:
        raise XfaSourceTapeError(
            "XFA source component count drift: "
            f"{len(frozen)} != {expected_component_count}"
        )
    runtime_summaries = _runtime_summaries(runtime_summaries_path, set(frozen))
    _reconcile_runtime_summaries(frozen, runtime_summaries)
    eligible_days = _common_days_from_summaries(runtime_summaries)
    source_manifest_path = bundle / "evidence_bundle_manifest.json"
    payload = {
        "schema": XFA_SOURCE_TAPE_SCHEMA,
        "campaign_id": campaign_id,
        "events": {
            component_id: [row.to_dict() for row in frozen[component_id]]
            for component_id in sorted(frozen)
        },
        "eligible_session_days": list(eligible_days),
        "event_count": len(trades),
        "component_count": len(frozen),
        "normal_net_pnl": normal_net,
        "normal_gross_pnl": normal_gross,
        "source_manifest_sha256": _sha256(source_manifest_path),
        "feature_bundle_hashes": {
            market: value.bundle_hash for market, value in sorted(arrays.items())
        },
    }
    return XfaSourceTape(
        schema=XFA_SOURCE_TAPE_SCHEMA,
        campaign_id=campaign_id,
        events=frozen,
        eligible_session_days=eligible_days,
        event_count=len(trades),
        component_count=len(frozen),
        normal_net_pnl=float(normal_net),
        normal_gross_pnl=float(normal_gross),
        source_manifest_sha256=str(payload["source_manifest_sha256"]),
        feature_bundle_hashes=dict(payload["feature_bundle_hashes"]),
        tape_hash=stable_hash(payload),
    )


def write_xfa_source_tape(tape: XfaSourceTape, output_dir: str | Path) -> dict[str, Any]:
    """Write the large event tape compressed and a compact deterministic manifest."""

    target = Path(output_dir).resolve()
    target.mkdir(parents=True, exist_ok=True)
    event_path = target / "source_events.jsonl.gz"
    manifest_path = target / "manifest.json"
    if event_path.exists() or manifest_path.exists():
        if not event_path.is_file() or not manifest_path.is_file():
            raise XfaSourceTapeError("partial immutable XFA source tape exists")
        existing = _load_json(manifest_path)
        if (
            existing.get("tape_hash") != tape.tape_hash
            or (existing.get("event_file") or {}).get("sha256")
            != _sha256(event_path)
        ):
            raise XfaSourceTapeError("immutable XFA source tape drift")
        return existing
    temporary = target / ".source_events.jsonl.gz.tmp"
    try:
        with temporary.open("wb") as raw:
            with gzip.GzipFile(
                filename="source_events.jsonl",
                mode="wb",
                fileobj=raw,
                mtime=0,
            ) as compressed:
                with io.TextIOWrapper(
                    compressed, encoding="utf-8", newline="\n"
                ) as handle:
                    for component_id in sorted(tape.events):
                        for row in tape.events[component_id]:
                            payload = {
                                "schema": XFA_SOURCE_EVENT_SCHEMA,
                                **row.to_dict(),
                            }
                            handle.write(
                                json.dumps(
                                    payload,
                                    sort_keys=True,
                                    separators=(",", ":"),
                                )
                                + "\n"
                            )
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(temporary, event_path)
    finally:
        temporary.unlink(missing_ok=True)
    manifest = {
        **tape.manifest(),
        "event_file": {
            "path": event_path.name,
            "sha256": _sha256(event_path),
            "size_bytes": event_path.stat().st_size,
        },
    }
    manifest["manifest_hash"] = stable_hash(manifest)
    content = json.dumps(manifest, sort_keys=True, indent=2) + "\n"
    manifest_tmp = target / ".manifest.json.tmp"
    try:
        with manifest_tmp.open("w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(manifest_tmp, manifest_path)
    finally:
        manifest_tmp.unlink(missing_ok=True)
    return manifest


def _keyed_rows(
    rows: Iterable[Mapping[str, Any]], label: str
) -> dict[tuple[str, str], dict[str, Any]]:
    output: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        key = (str(row.get("component_id") or ""), str(row.get("trade_id") or ""))
        if not all(key) or key in output:
            raise XfaSourceTapeError(f"{label} contain an invalid or duplicate key")
        output[key] = row
    return output


def _discover_feature_arrays(
    root: Path, *, required_markets: set[str] | None = None
) -> dict[str, _FeatureArrays]:
    output: dict[str, _FeatureArrays] = {}
    required = {
        "decision_ns": "decision_ns",
        "timestamp_ns": "timestamp_ns",
        "entry_price": "entry_price",
        "bar_high": "bar_high",
        "bar_low": "bar_low",
        "segment_code": "segment_code",
        "session_day": "session_day",
        "session_code": "session_code",
    }
    for manifest_path in sorted(root.glob("*/manifest.json")):
        manifest = _load_json(manifest_path)
        key = manifest.get("key") or {}
        market = str(key.get("market") or "")
        if not market:
            continue
        if required_markets is not None and market not in required_markets:
            continue
        if market in output:
            raise XfaSourceTapeError(f"duplicate feature bundle for market {market}")
        declared = manifest.get("arrays") or {}
        loaded: dict[str, np.ndarray] = {}
        for field, array_name in required.items():
            spec = declared.get(array_name)
            if not isinstance(spec, Mapping):
                raise XfaSourceTapeError(
                    f"feature bundle {market} lacks {array_name}"
                )
            array_path = manifest_path.parent / str(spec.get("path") or "")
            if not array_path.is_file():
                raise XfaSourceTapeError(
                    f"feature bundle array is missing: {market}:{array_name}"
                )
            loaded[field] = np.load(array_path, mmap_mode="r", allow_pickle=False)
        output[market] = _FeatureArrays(
            market=market,
            bundle_hash=str(manifest.get("bundle_hash") or ""),
            **loaded,
        )
    if not output:
        raise XfaSourceTapeError("no immutable feature bundles were discovered")
    if required_markets is not None and set(output) != required_markets:
        missing = sorted(required_markets - set(output))
        raise XfaSourceTapeError(
            f"immutable feature bundles are missing for markets: {missing}"
        )
    return output


def _rehydrate_trade(
    trade: Mapping[str, Any],
    entry: Mapping[str, Any],
    exit_row: Mapping[str, Any],
    arrays: Mapping[str, _FeatureArrays],
) -> RoutedTrade:
    component_id = str(trade["component_id"])
    trade_id = str(trade["trade_id"])
    exact_fields = (
        (entry.get("entry_time"), trade.get("entry_time"), "entry_time"),
        (exit_row.get("exit_time"), trade.get("exit_time"), "exit_time"),
        (entry.get("market"), trade.get("market"), "market"),
        (entry.get("contract"), trade.get("contract"), "contract"),
        (entry.get("side"), trade.get("side"), "side"),
    )
    for left, right, field in exact_fields:
        if left != right:
            raise XfaSourceTapeError(f"trade {trade_id} {field} drift")
    for left, right, field in (
        (entry.get("quantity"), trade.get("quantity"), "quantity"),
        (entry.get("entry_price"), trade.get("entry_price"), "entry_price"),
        (exit_row.get("exit_price"), trade.get("exit_price"), "exit_price"),
    ):
        _assert_close(left, right, f"trade {trade_id} {field}")

    market = str(trade["market"])
    if market not in arrays:
        raise XfaSourceTapeError(f"trade {trade_id} has no feature bundle: {market}")
    matrix = arrays[market]
    decision_ns = _timestamp_ns(str(entry["entry_time"]))
    exit_ns = _timestamp_ns(str(exit_row["exit_time"]))
    position = _locate(matrix.decision_ns, decision_ns, "decision", trade_id)
    exit_index = _locate(
        matrix.timestamp_ns, exit_ns - MINUTE_NS, "exit", trade_id
    )
    entry_index = position + 1
    if not entry_index <= exit_index < len(matrix.bar_high):
        raise XfaSourceTapeError(f"trade {trade_id} has an invalid exact window")
    if int(matrix.segment_code[entry_index]) != int(matrix.segment_code[exit_index]):
        raise XfaSourceTapeError(f"trade {trade_id} crosses a frozen segment")
    _assert_close(matrix.entry_price[position], entry["entry_price"], "cached entry")

    side = 1 if str(trade["side"]) == "LONG" else -1
    if str(trade["side"]) not in {"LONG", "SHORT"}:
        raise XfaSourceTapeError(f"trade {trade_id} has an invalid side")
    quantity = int(trade["quantity"])
    contract = str(trade["contract"])
    point_value = float(instrument_spec(contract).point_value)
    entry_price = float(trade["entry_price"])
    cost = float(trade["costs"])
    high = float(np.max(matrix.bar_high[entry_index : exit_index + 1]))
    low = float(np.min(matrix.bar_low[entry_index : exit_index + 1]))
    adverse = low if side > 0 else high
    favorable = high if side > 0 else low
    worst_gross = (adverse - entry_price) * side * point_value * quantity
    best_gross = (favorable - entry_price) * side * point_value * quantity
    expected_gross = (
        (float(trade["exit_price"]) - entry_price)
        * side
        * point_value
        * quantity
    )
    _assert_close(expected_gross, trade["gross_pnl"], f"trade {trade_id} gross")
    _assert_close(
        float(trade["gross_pnl"]) - cost,
        trade["net_pnl"],
        f"trade {trade_id} net",
    )

    event = TradePathEvent(
        event_id=trade_id,
        decision_ns=decision_ns,
        exit_ns=exit_ns,
        session_day=int(matrix.session_day[position]),
        net_pnl=float(trade["net_pnl"]),
        gross_pnl=float(trade["gross_pnl"]),
        worst_unrealized_pnl=float(worst_gross - cost),
        best_unrealized_pnl=float(best_gross - cost),
        quantity=quantity,
        mini_equivalent=mini_equivalent(contract, quantity),
        regime="REHYDRATED_IMMUTABLE_TRADE_PATH",
        session_compliant=bool(int(matrix.session_code[position]) >= 0),
        contract_limit_compliant=bool(mini_equivalent(contract, quantity) <= 15.0),
        same_bar_ambiguous=bool(
            entry_index == exit_index and worst_gross < 0.0 < best_gross
        ),
    )
    return RoutedTrade(
        component_id=component_id,
        market=contract,
        side=side,
        event=event,
    )


def _common_eligible_days(
    path: str | Path, component_ids: set[str]
) -> tuple[int, ...]:
    return _common_days_from_summaries(_runtime_summaries(path, component_ids))


def _runtime_summaries(
    path: str | Path, component_ids: set[str]
) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = json.loads(line)
            component_id = str(raw.get("sleeve_id") or "")
            if component_id in component_ids:
                if component_id in rows:
                    raise XfaSourceTapeError(
                        f"duplicate runtime summary: {component_id}"
                    )
                rows[component_id] = dict(raw)
    if set(rows) != component_ids or any(
        not row.get("eligible_session_days") for row in rows.values()
    ):
        raise XfaSourceTapeError("runtime summaries do not cover every component")
    return rows


def _common_days_from_summaries(
    rows: Mapping[str, Mapping[str, Any]],
) -> tuple[int, ...]:
    calendars = [
        {int(value) for value in row.get("eligible_session_days") or ()}
        for row in rows.values()
    ]
    iterator = iter(calendars)
    common = set(next(iterator))
    for value in iterator:
        common.intersection_update(value)
    if not common:
        raise XfaSourceTapeError("component calendars have no common XFA day")
    return tuple(sorted(common))


def _reconcile_runtime_summaries(
    events: Mapping[str, Sequence[RoutedTrade]],
    summaries: Mapping[str, Mapping[str, Any]],
) -> None:
    for component_id, rows in events.items():
        summary = summaries[component_id]
        if int(summary.get("event_count") or -1) != len(rows):
            raise XfaSourceTapeError(
                f"runtime event count drift for component {component_id}"
            )
        net = sum(row.event.net_pnl for row in rows)
        stressed = sum(
            row.event.net_pnl
            - 0.5 * max(0.0, row.event.gross_pnl - row.event.net_pnl)
            for row in rows
        )
        _assert_close(
            net,
            summary.get("net_pnl"),
            f"runtime normal net for component {component_id}",
        )
        _assert_close(
            stressed,
            summary.get("cost_stress_1_5x_net"),
            f"runtime stressed net for component {component_id}",
        )


def _locate(array: np.ndarray, value: int, label: str, trade_id: str) -> int:
    position = int(np.searchsorted(array, value, side="left"))
    if position >= len(array) or int(array[position]) != value:
        raise XfaSourceTapeError(f"trade {trade_id} {label} timestamp is absent")
    if position + 1 < len(array) and int(array[position + 1]) == value:
        raise XfaSourceTapeError(f"trade {trade_id} {label} timestamp is ambiguous")
    return position


def _timestamp_ns(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1_000_000_000)


def _assert_close(left: Any, right: Any, label: str) -> None:
    if not math.isclose(float(left), float(right), rel_tol=1e-10, abs_tol=1e-8):
        raise XfaSourceTapeError(f"{label} drift: {left} != {right}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise XfaSourceTapeError(f"JSON object expected: {path}")
    return value


__all__ = [
    "XFA_SOURCE_EVENT_SCHEMA",
    "XFA_SOURCE_TAPE_SCHEMA",
    "XfaSourceTape",
    "XfaSourceTapeError",
    "build_xfa_source_tape",
    "write_xfa_source_tape",
]
