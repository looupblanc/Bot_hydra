from __future__ import annotations

"""Causal, streaming option-settlement surface reconstruction.

This module deliberately stops at market-state reconstruction.  It does not
create labels, signals, trades, or economic results.  Definitions and
settlements are joined with the latest definition whose ``ts_recv`` is no
later than the settlement ``ts_recv``; this is important because Databento
instrument IDs can be reused.
"""

import hashlib
import json
import math
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping


SCHEMA = "hydra_causal_option_settlement_surface_v1"
SETTLEMENT_STAT_TYPE = 3
PRICE_SCALE = 1_000_000_000.0
INVALID_I64 = 9_223_372_036_854_775_807
INVALID_U64 = 18_446_744_073_709_551_615
DAY_NS = 86_400_000_000_000
MINIMUM_DAYS_TO_EXPIRY = 5.0
MAXIMUM_FRONT_DAYS_TO_EXPIRY = 140.0
MAXIMUM_SECOND_DAYS_TO_EXPIRY = 260.0
FORWARD_LOG_MONEYNESS_MAXIMUM = 0.10
ATM_LOG_MONEYNESS_MAXIMUM = 0.03
WING_MONEYNESS = 0.02


class SettlementSurfaceError(RuntimeError):
    """The source streams cannot be reconciled without weakening causality."""


def _value(value: Any) -> Any:
    return getattr(value, "value", value)


def _hash_payload(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _iso_ns(value: int) -> str:
    seconds, nanos = divmod(int(value), 1_000_000_000)
    base = datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{nanos:09d}Z"


def _reference_date(value: int) -> str:
    return datetime.fromtimestamp(int(value) // 1_000_000_000, tz=timezone.utc).date().isoformat()


@dataclass(frozen=True, slots=True)
class OptionDefinition:
    instrument_id: int
    definition_ts_recv_ns: int
    market: str
    underlying: str
    option_type: str
    expiration_ns: int
    strike: float
    raw_symbol: str
    fingerprint: str


@dataclass(frozen=True, slots=True)
class SettlementObservation:
    instrument_id: int
    reference_ts_ns: int
    ts_event_ns: int
    ts_recv_ns: int
    price: float
    definition: OptionDefinition
    fingerprint: str


@dataclass(frozen=True, slots=True)
class TermSurface:
    underlying: str
    expiration_ns: int
    expiration: str
    available_at_ns: int
    available_at: str
    days_to_expiry: float
    paired_strike_count: int
    robust_forward: float
    parity_forward_mad: float
    atm_strike: float
    atm_straddle: float
    atm_straddle_fraction: float
    atm_straddle_vol_proxy: float
    downside_upside_wing_premium_skew: float | None
    wing_moneyness: float
    lower_wing_strike: float | None
    upper_wing_strike: float | None
    constituent_hash: str


@dataclass(frozen=True, slots=True)
class SettlementSurfaceSnapshot:
    schema: str
    market: str
    settlement_reference_date: str
    settlement_reference_ts_ns: int
    available_at_ns: int
    available_at: str
    status: str
    front_term: TermSurface | None
    next_term: TermSurface | None
    forward_term_slope: float | None
    front_next_term_slope: float | None
    wing_skew_term_slope: float | None
    source_hashes: Mapping[str, str]
    snapshot_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SurfaceBuildDiagnostics:
    statistics_seen: int = 0
    settlement_records_seen: int = 0
    settlement_records_accepted: int = 0
    missing_definition: int = 0
    ineligible_definition: int = 0
    invalid_settlement: int = 0
    snapshots_emitted: int = 0


def _definition_from_record(record: Any, markets: frozenset[str]) -> OptionDefinition | None:
    action = str(_value(getattr(record, "security_update_action", "")))
    option_type = str(_value(getattr(record, "instrument_class", "")))
    user_defined = str(_value(getattr(record, "user_defined_instrument", "")))
    market = str(getattr(record, "asset", ""))
    underlying = str(getattr(record, "underlying", ""))
    strike_raw = int(getattr(record, "strike_price", INVALID_I64))
    expiration = int(getattr(record, "expiration", INVALID_U64))
    if (
        action.upper() in {"D", "DELETE"}
        or option_type not in {"C", "P"}
        or user_defined != "N"
        or market not in markets
        or not underlying.startswith(market)
        or strike_raw == INVALID_I64
        or expiration in {INVALID_I64, INVALID_U64}
    ):
        return None
    payload = {
        "instrument_id": int(record.instrument_id),
        "definition_ts_recv_ns": int(record.ts_recv),
        "market": market,
        "underlying": underlying,
        "option_type": option_type,
        "expiration_ns": expiration,
        "strike": strike_raw / PRICE_SCALE,
        "raw_symbol": str(getattr(record, "raw_symbol", "")),
    }
    return OptionDefinition(**payload, fingerprint=_hash_payload(payload))


def _observation(record: Any, definition: OptionDefinition) -> SettlementObservation | None:
    price_raw = int(getattr(record, "price", INVALID_I64))
    reference = int(getattr(record, "ts_ref", INVALID_U64))
    if price_raw == INVALID_I64 or price_raw < 0 or reference == INVALID_U64:
        return None
    payload = {
        "instrument_id": int(record.instrument_id),
        "definition_fingerprint": definition.fingerprint,
        "reference_ts_ns": reference,
        "ts_event_ns": int(record.ts_event),
        "ts_recv_ns": int(record.ts_recv),
        "price": price_raw / PRICE_SCALE,
    }
    return SettlementObservation(
        instrument_id=payload["instrument_id"],
        reference_ts_ns=payload["reference_ts_ns"],
        ts_event_ns=payload["ts_event_ns"],
        ts_recv_ns=payload["ts_recv_ns"],
        price=payload["price"],
        definition=definition,
        fingerprint=_hash_payload(payload),
    )


def _median(values: Iterable[float]) -> float:
    return float(statistics.median(values))


def _term_surface(
    observations: Iterable[SettlementObservation],
    minimum_pairs: int,
    reference_ts_ns: int,
) -> TermSurface | None:
    nodes: dict[float, dict[str, SettlementObservation]] = {}
    underlying = ""
    expiration = 0
    for observation in observations:
        definition = observation.definition
        underlying = definition.underlying
        expiration = definition.expiration_ns
        side = nodes.setdefault(definition.strike, {})
        prior = side.get(definition.option_type)
        if prior is None or observation.ts_recv_ns > prior.ts_recv_ns:
            side[definition.option_type] = observation
    pairs = [(strike, side["C"], side["P"]) for strike, side in nodes.items() if "C" in side and "P" in side]
    if len(pairs) < minimum_pairs:
        return None
    parity_all = [strike + call.price - put.price for strike, call, put in pairs]
    initial = _median(parity_all)
    if initial <= 0:
        return None
    # The contract freezes a two-step robust forward: obtain an initial parity
    # median from all complete pairs, then retain only strikes inside +/-10%
    # absolute log-moneyness and recompute the parity median.
    pairs = [row for row in pairs if row[0] > 0 and abs(math.log(row[0] / initial)) <= FORWARD_LOG_MONEYNESS_MAXIMUM]
    if len(pairs) < minimum_pairs:
        return None
    parity = [strike + call.price - put.price for strike, call, put in pairs]
    recomputed = _median(parity)
    deviations = [abs(value - recomputed) for value in parity]
    mad = _median(deviations)
    # Median itself is the frozen robust estimator; do not add an unregistered
    # MAD cutoff after the manifest's moneyness filter.
    forward = recomputed
    atm_candidates = [row for row in pairs if abs(math.log(row[0] / forward)) <= ATM_LOG_MONEYNESS_MAXIMUM]
    if not atm_candidates:
        return None
    atm = min(atm_candidates, key=lambda row: (abs(math.log(row[0] / forward)), row[0]))
    atm_straddle = atm[1].price + atm[2].price
    lower = [row for row in pairs if row[0] < forward]
    upper = [row for row in pairs if row[0] > forward]
    lower_node = min(lower, key=lambda row: abs(math.log(row[0] / forward) + WING_MONEYNESS)) if lower else None
    upper_node = min(upper, key=lambda row: abs(math.log(row[0] / forward) - WING_MONEYNESS)) if upper else None
    skew = None
    if lower_node is not None and upper_node is not None and atm_straddle > 0:
        skew = (lower_node[2].price - upper_node[1].price) / atm_straddle
    availability = max(max(call.ts_recv_ns, put.ts_recv_ns) for _, call, put in pairs)
    days_to_expiry = (expiration - reference_ts_ns) / DAY_NS
    if days_to_expiry <= 0:
        return None
    years_to_expiry = days_to_expiry / 365.25
    vol_proxy = math.sqrt(math.pi / 2.0) * (atm_straddle / forward) / math.sqrt(years_to_expiry)
    constituents = sorted(call.fingerprint for _, call, _ in pairs) + sorted(put.fingerprint for _, _, put in pairs)
    payload = {
        "underlying": underlying,
        "expiration_ns": expiration,
        "expiration": _iso_ns(expiration),
        "available_at_ns": availability,
        "available_at": _iso_ns(availability),
        "days_to_expiry": days_to_expiry,
        "paired_strike_count": len(pairs),
        "robust_forward": forward,
        "parity_forward_mad": mad,
        "atm_strike": atm[0],
        "atm_straddle": atm_straddle,
        "atm_straddle_fraction": atm_straddle / forward,
        "atm_straddle_vol_proxy": vol_proxy,
        "downside_upside_wing_premium_skew": skew,
        "wing_moneyness": WING_MONEYNESS,
        "lower_wing_strike": lower_node[0] if lower_node else None,
        "upper_wing_strike": upper_node[0] if upper_node else None,
        "constituent_hash": _hash_payload({"observations": constituents}),
    }
    return TermSurface(**payload)


def _snapshots_for_reference(
    reference_ts_ns: int,
    observations: Iterable[SettlementObservation],
    markets: tuple[str, ...],
    source_hashes: Mapping[str, str],
    minimum_pairs_per_term: int,
) -> Iterator[SettlementSurfaceSnapshot]:
    by_market_term: dict[tuple[str, str, int], list[SettlementObservation]] = {}
    max_recv_by_market: dict[str, int] = {}
    for observation in observations:
        definition = observation.definition
        key = (definition.market, definition.underlying, definition.expiration_ns)
        by_market_term.setdefault(key, []).append(observation)
        max_recv_by_market[definition.market] = max(max_recv_by_market.get(definition.market, 0), observation.ts_recv_ns)
    for market in markets:
        terms = [
            term
            for (term_market, _, _), rows in by_market_term.items()
            if term_market == market
            for term in [_term_surface(rows, minimum_pairs_per_term, reference_ts_ns)]
            if term is not None
        ]
        # If multiple underlyings share an expiry, retain the most complete one.
        best_by_expiry: dict[int, TermSurface] = {}
        for term in terms:
            prior = best_by_expiry.get(term.expiration_ns)
            if prior is None or term.paired_strike_count > prior.paired_strike_count:
                best_by_expiry[term.expiration_ns] = term
        terms = sorted(best_by_expiry.values(), key=lambda term: term.expiration_ns)
        front_candidates = [
            term for term in terms
            if MINIMUM_DAYS_TO_EXPIRY <= term.days_to_expiry <= MAXIMUM_FRONT_DAYS_TO_EXPIRY
        ]
        front = front_candidates[0] if front_candidates else None
        second_candidates = [
            term for term in terms
            if front is not None
            and term.expiration_ns > front.expiration_ns
            and MINIMUM_DAYS_TO_EXPIRY <= term.days_to_expiry <= MAXIMUM_SECOND_DAYS_TO_EXPIRY
        ]
        nxt = second_candidates[0] if second_candidates else None
        selected = [term for term in (front, nxt) if term is not None]
        available_at_ns = max((term.available_at_ns for term in selected), default=max_recv_by_market.get(market, reference_ts_ns))
        status = "COMPLETE_FRONT_NEXT" if len(selected) == 2 else "INSUFFICIENT_FRONT_NEXT_TERMS"
        forward_slope = (nxt.robust_forward / front.robust_forward - 1.0) if front and nxt else None
        term_slope = (nxt.atm_straddle_vol_proxy - front.atm_straddle_vol_proxy) if front and nxt else None
        skew_slope = None
        if (
            front
            and nxt
            and front.downside_upside_wing_premium_skew is not None
            and nxt.downside_upside_wing_premium_skew is not None
        ):
            skew_slope = nxt.downside_upside_wing_premium_skew - front.downside_upside_wing_premium_skew
        core = {
            "schema": SCHEMA,
            "market": market,
            "settlement_reference_date": _reference_date(reference_ts_ns),
            "settlement_reference_ts_ns": reference_ts_ns,
            "available_at_ns": available_at_ns,
            "available_at": _iso_ns(available_at_ns),
            "status": status,
            "front_term": asdict(front) if front else None,
            "next_term": asdict(nxt) if nxt else None,
            "forward_term_slope": forward_slope,
            "front_next_term_slope": term_slope,
            "wing_skew_term_slope": skew_slope,
            "source_hashes": dict(source_hashes),
        }
        yield SettlementSurfaceSnapshot(
            schema=SCHEMA,
            market=market,
            settlement_reference_date=core["settlement_reference_date"],
            settlement_reference_ts_ns=reference_ts_ns,
            available_at_ns=available_at_ns,
            available_at=core["available_at"],
            status=status,
            front_term=front,
            next_term=nxt,
            forward_term_slope=forward_slope,
            front_next_term_slope=term_slope,
            wing_skew_term_slope=skew_slope,
            source_hashes=dict(source_hashes),
            snapshot_hash=_hash_payload(core),
        )


def build_surface_snapshots(
    definitions: Iterable[Any],
    statistics_records: Iterable[Any],
    *,
    source_hashes: Mapping[str, str],
    markets: tuple[str, ...] = ("ES", "NQ"),
    minimum_pairs_per_term: int = 5,
    diagnostics: SurfaceBuildDiagnostics | None = None,
) -> Iterator[SettlementSurfaceSnapshot]:
    """Merge ordered record streams and emit at most one snapshot/date/market.

    Memory is bounded by the current definition map plus a single settlement
    reference date.  A decreasing settlement reference or receive timestamp is
    rejected because emitting then revising a causal snapshot would be unsafe.
    """

    diag = diagnostics if diagnostics is not None else SurfaceBuildDiagnostics()
    market_set = frozenset(markets)
    definition_iterator = iter(definitions)
    next_definition = next(definition_iterator, None)
    definition_state: dict[int, OptionDefinition | None] = {}
    last_definition_recv = -1
    last_settlement_recv = -1
    current_reference: int | None = None
    current_observations: dict[tuple[int, str], SettlementObservation] = {}

    for record in statistics_records:
        diag.statistics_seen += 1
        if int(_value(getattr(record, "stat_type", -1))) != SETTLEMENT_STAT_TYPE:
            continue
        diag.settlement_records_seen += 1
        receive = int(record.ts_recv)
        if receive < last_settlement_recv:
            raise SettlementSurfaceError("settlement stream ts_recv is not monotonic")
        last_settlement_recv = receive
        while next_definition is not None and int(next_definition.ts_recv) <= receive:
            definition_receive = int(next_definition.ts_recv)
            if definition_receive < last_definition_recv:
                raise SettlementSurfaceError("definition stream ts_recv is not monotonic")
            last_definition_recv = definition_receive
            instrument_id = int(next_definition.instrument_id)
            # Store None too: an ineligible update must invalidate an earlier
            # eligible definition for a reused instrument ID.
            definition_state[instrument_id] = _definition_from_record(next_definition, market_set)
            next_definition = next(definition_iterator, None)
        instrument_id = int(record.instrument_id)
        if instrument_id not in definition_state:
            diag.missing_definition += 1
            continue
        definition = definition_state[instrument_id]
        if definition is None:
            diag.ineligible_definition += 1
            continue
        observation = _observation(record, definition)
        if observation is None:
            diag.invalid_settlement += 1
            continue
        reference = observation.reference_ts_ns
        if current_reference is not None and reference < current_reference:
            raise SettlementSurfaceError("settlement reference timestamp regressed")
        if current_reference is not None and reference != current_reference:
            for snapshot in _snapshots_for_reference(
                current_reference,
                current_observations.values(),
                markets,
                source_hashes,
                minimum_pairs_per_term,
            ):
                diag.snapshots_emitted += 1
                yield snapshot
            current_observations.clear()
        current_reference = reference
        identity = (observation.instrument_id, observation.definition.fingerprint)
        prior = current_observations.get(identity)
        if prior is None or observation.ts_recv_ns >= prior.ts_recv_ns:
            current_observations[identity] = observation
        diag.settlement_records_accepted += 1

    if current_reference is not None:
        for snapshot in _snapshots_for_reference(
            current_reference,
            current_observations.values(),
            markets,
            source_hashes,
            minimum_pairs_per_term,
        ):
            diag.snapshots_emitted += 1
            yield snapshot


def iter_dbn_surface_snapshots(
    statistics_path: str | Path,
    definitions_path: str | Path,
    *,
    markets: tuple[str, ...] = ("ES", "NQ"),
    minimum_pairs_per_term: int = 5,
    diagnostics: SurfaceBuildDiagnostics | None = None,
) -> Iterator[SettlementSurfaceSnapshot]:
    """Stream a DBN bundle without materialising either source as a DataFrame."""

    import databento as db  # Optional dependency until an actual DBN is read.

    statistics_path = Path(statistics_path)
    definitions_path = Path(definitions_path)
    hashes = {
        "statistics_sha256": _sha256_file(statistics_path),
        "definitions_sha256": _sha256_file(definitions_path),
    }
    yield from build_surface_snapshots(
        db.DBNStore.from_file(definitions_path),
        db.DBNStore.from_file(statistics_path),
        source_hashes=hashes,
        markets=markets,
        minimum_pairs_per_term=minimum_pairs_per_term,
        diagnostics=diagnostics,
    )


def write_snapshots_jsonl(snapshots: Iterable[SettlementSurfaceSnapshot], path: str | Path) -> str:
    """Write deterministic JSONL and return its SHA-256 (no economic status)."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    with target.open("wb") as handle:
        for snapshot in snapshots:
            line = (json.dumps(snapshot.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
            handle.write(line)
            digest.update(line)
    return digest.hexdigest()
