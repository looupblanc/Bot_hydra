"""Scientific backend for HYDRA selective order-flow veto campaign 0034.

The implementation is intentionally narrow.  It first audits the two frozen
0033 seeds without opening a market-data file, then reconstructs a long
structural-opportunity universe from the immutable 0028 causal ledgers.  Only
after that gate does it ask Databento's official metadata endpoint to price
short, merged windows around those opportunities.  A purchase can therefore
never precede the no-purchase robustness decision.

The production action lattice contains only ``ABSTAIN``, ``TRADE_1X`` and
``TRADE_1_5X``.  Direction, entry timestamp, stop, target and exit are copied
from the causal structural anchor.  If no deployable event-window sample fits
the frozen USD 8 / USD 20-reserve envelope, the backend returns an honest weak
decision and performs no acquisition or pseudo-evaluation.
"""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
import fcntl
import gzip
import hashlib
import json
import math
from multiprocessing import get_context
import os
from pathlib import Path
import resource
import time
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

import numpy as np

from hydra.data.budget import (
    AUTO_UNDER_HARD_CAP,
    DatabentoBudgetConfig,
    DatabentoSpendRecord,
    append_spend_record,
    cumulative_spend,
    enforce_budget,
    read_ledger,
    request_id_for,
    sha256_file,
    utc_now,
)
from hydra.data.databento_loader import load_api_key
from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import REQUIRED_DATASETS
from hydra.propfirm.combine_episode import CombineTerminal, TradePathEvent
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.production.selective_veto_seed_audit import (
    PRIMARY_SEED_ID,
    SECONDARY_SEED_ID,
    run_seed_robustness_audit,
    write_seed_audit_checkpoint,
)
from hydra.production.selective_veto_manifest import (
    ACCOUNT_RULE_SNAPSHOTS,
    MATERIAL_STRESSED_TARGET_PROGRESS_UPLIFT_MINIMUM,
    STRUCTURAL_FAMILIES,
)
from hydra.production.selective_veto_metadata import (
    MetadataRetryPolicy,
    ResilientMetadataEstimator,
)
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access
from hydra.research.causal_sleeve_replay import (
    CausalTradeMark,
    CausalTradeTrajectory,
)


CAMPAIGN_ID = "hydra_selective_order_flow_veto_expansion_0034"
PILOT_VERSION = "hydra_selective_veto_long_sample_pilot_v1"
DATASET = "GLBX.MDP3"
SCHEMAS = ("trades", "tbbo", "mbp-1")
WINDOW_COUNTS = (100, 250, 500, 1_000)
PRIMARY_ACTIONS = ("ABSTAIN", "TRADE_1X", "TRADE_1_5X")
Q4_START_NS = int(datetime(2024, 10, 1, tzinfo=UTC).timestamp() * 1e9)
FEATURE_NAMES = (
    "flow_2s",
    "flow_30s",
    "bbo_imbalance",
    "microprice_deviation_ticks",
    "spread_ticks",
)
# The purchased event windows were frozen at sixty seconds after each
# structural decision.  A legacy structural fill can legitimately have the
# same timestamp as the decision (the 0028 ledger records the completed-bar
# boundary), so it cannot serve as the upper bound for a strictly
# post-decision TBBO quote.  The selective overlay instead uses the first
# executable quote after the decision, bounded by this already-preregistered
# event window.
MAX_POST_DECISION_ENTRY_DELAY_NS = 60 * 1_000_000_000
ALLOWED_STRUCTURAL_FAMILIES = frozenset(STRUCTURAL_FAMILIES)
PINNED_ROLL_MAP = Path(
    "data/cache/contract_maps/roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json"
)
PINNED_ROLL_MAP_FILE_SHA256 = (
    "401ca56ebab606c3eb2cbcf6ed244204f264ed2894c2ee0eb2310998f9244fda"
)
PINNED_ROLL_MAP_HASH = (
    "705ce6fe27bac7dea9cb9d492413a5112bb60765c66aa75d03f9711bef348208"
)
ACCOUNT_SNAPSHOTS = {
    label: {
        "account_size": float(row["account_size_usd"]),
        "target": float(row["profit_target_usd"]),
        "mll": float(row["maximum_loss_limit_usd"]),
        "max_mini": float(row["maximum_mini_contracts"]),
        "no_daily_loss_limit": bool(row["no_daily_loss_limit"]),
        "use_optional_daily_loss_limit": bool(
            row["use_optional_daily_loss_limit"]
        ),
    }
    for label, row in ACCOUNT_RULE_SNAPSHOTS.items()
}

SOURCE_0033 = Path(
    "reports/economic_evolution/hybrid_structural_alpha_order_flow_0033"
)
SOURCE_0028 = Path(
    "data/cache/economic_production/hydra_causal_target_velocity_0028"
)
ANCHOR_IDS_0033 = (
    "hazard_00efa9bbb8ddd4eebb3a1483",
    "hazard_01b84c042c95b8a6a206a8da",
    "hazard_026cc350e74a5098105325c2",
    "hazard_04561035cc31c31b0dd3f85a",
    "hazard_04742cf1b919fc9771ee291d",
    "hazard_0542f3388c119e46904ba18b",
    "hazard_063cc5711d0e40f9e375e2f8",
    "hazard_072556d2ea8da045d1072812",
    "hazard_109e715f60d31444d7ad42f6",
    "hazard_178325b10e8b10efa9966c91",
    "hazard_1931ef31a7ddd6c349445ddd",
    "hazard_19772a45d22932906b543e59",
    "hazard_1d191acd46c786f415e413eb",
    "hazard_1fe0960034e66a60281b7304",
    "hazard_20281c39a8744fce6d7b452e",
    "hazard_232fc968a6a1b55c0d3adb29",
    "hazard_25e4f411aafb0840d86843d5",
    "hazard_2ce4b3e6934c5b55ff8829bb",
    "hazard_319170b7485b4802091b05dd",
    "hazard_33532edae13835fab0408952",
    "hazard_3431aecefc87c345371ca745",
    "hazard_39113048dd9e10ded1a41a74",
)


class SelectiveVetoPilotError(RuntimeError):
    """The bounded 0034 scientific backend cannot continue safely."""


class MetadataAPI(Protocol):
    def get_record_count(self, **kwargs: Any) -> int: ...
    def get_billable_size(self, **kwargs: Any) -> int: ...
    def get_cost(self, **kwargs: Any) -> float: ...


@dataclass(frozen=True, slots=True)
class StructuralAnchor:
    anchor_event_id: str
    source_candidate_id: str
    market: str
    execution_market: str
    structural_family: str
    source_mechanism: str
    contract: str
    decision_time_ns: int
    event_time_ns: int
    direction: int
    quantity: int
    timeframe: str
    normal_net_pnl_usd: float | None
    stressed_net_pnl_usd: float | None
    normal_worst_unrealized_pnl_usd: float | None
    stressed_worst_unrealized_pnl_usd: float | None
    normal_best_unrealized_pnl_usd: float
    stressed_best_unrealized_pnl_usd: float
    normal_initial_unrealized_pnl_usd: float
    stressed_initial_unrealized_pnl_usd: float
    normal_marks: tuple[Mapping[str, Any], ...]
    stressed_marks: tuple[Mapping[str, Any], ...]
    normal_fill_price: float
    stressed_fill_price: float
    raw_exit_price: float | None
    stop_price: float
    target_price: float
    fill_time_ns: int
    outcome_time_ns: int | None
    session_day: int
    outcome: str
    same_bar_ambiguous: bool
    session_compliant: bool
    contract_limit_compliant: bool
    source_event_hash: str

    @property
    def session_id(self) -> str:
        # 0028 persisted the authoritative Topstep trading-session ordinal.
        # UTC calendar dates can split the overnight trading day incorrectly.
        return datetime.fromtimestamp(self.session_day * 86_400, tz=UTC).date().isoformat()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EventWindow:
    market: str
    contract: str
    start_ns: int
    end_ns: int
    anchor_ids: tuple[str, ...]

    @property
    def duration_seconds(self) -> float:
        return (self.end_ns - self.start_ns) / 1e9

    def request(self, schema: str) -> dict[str, Any]:
        return {
            "dataset": DATASET,
            "symbols": [self.contract],
            "schema": schema,
            "stype_in": "raw_symbol",
            "start": _iso_ns(self.start_ns),
            "end": _iso_ns(self.end_ns),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "start": _iso_ns(self.start_ns),
            "end": _iso_ns(self.end_ns),
            "duration_seconds": self.duration_seconds,
        }


@dataclass(frozen=True, slots=True)
class CausalEntryQuote:
    """First executable top-of-book state observed after an anchor decision."""

    schema: str
    event_time_ns: int
    available_at_ns: int
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float
    first_mark_available_at_ns: int | None = None
    post_fill_worst_liquidation_price: float | None = None
    post_fill_best_liquidation_price: float | None = None
    post_fill_last_liquidation_price: float | None = None

    def to_dict(self) -> dict[str, Any]:
        core = asdict(self)
        return {**core, "quote_fingerprint": stable_hash(core)}


@dataclass(frozen=True, slots=True)
class TargetedCostConfig:
    pre_decision_seconds: int = 120
    post_decision_seconds: int = 60
    maximum_incremental_spend_usd: float = 8.0
    minimum_budget_reserve_usd: float = 20.0
    current_remaining_budget_usd: float = 28.498462508622012
    window_counts: tuple[int, ...] = WINDOW_COUNTS
    schemas: tuple[str, ...] = SCHEMAS

    def validate(self) -> None:
        if (
            self.pre_decision_seconds != 120
            or self.post_decision_seconds != 60
            or self.window_counts != WINDOW_COUNTS
            or self.schemas != SCHEMAS
            or not math.isclose(self.maximum_incremental_spend_usd, 8.0)
            or not math.isclose(self.minimum_budget_reserve_usd, 20.0)
            or self.current_remaining_budget_usd
            - self.maximum_incremental_spend_usd
            < self.minimum_budget_reserve_usd - 1e-9
        ):
            raise SelectiveVetoPilotError("0034 targeted cost envelope drift")


def _from_ns(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1e9, tz=UTC)


def _iso_ns(value: int) -> str:
    return _from_ns(value).isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SelectiveVetoPilotError(f"immutable JSON source unavailable: {path}") from exc
    if not isinstance(value, dict):
        raise SelectiveVetoPilotError(f"immutable JSON source is not an object: {path}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise SelectiveVetoPilotError(f"immutable JSONL source unavailable: {path}")
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise SelectiveVetoPilotError(f"non-object JSONL row: {path}")
        rows.append(value)
    return rows


def _normalize_family(mechanism: str) -> str:
    normalized = {
        "CROSS_ASSET_STATE": "CROSS_MARKET_DIVERGENCE",
        "DISPLACEMENT_ACCELERATION": "SESSION_TRANSITION",
        "EXHAUSTION_REVERSAL": "FAILED_BREAKOUT",
        "MULTI_TIMEFRAME_ALIGNMENT": "MULTI_TIMEFRAME_CONTINUATION",
        "RANGE_BREAKOUT_WITH_ROOM": "OPENING_RANGE",
        "DIRECTIONAL_PRESSURE_RELEASE": "COMPRESSION_TO_EXPANSION",
        "FAILED_CONTINUATION_REVERSAL": "FAILED_BREAKOUT",
        "PARTICIPATION_DENSITY": "SESSION_TRANSITION",
        "COMPRESSION_TO_EXPANSION": "COMPRESSION_TO_EXPANSION",
    }.get(mechanism)
    if normalized not in ALLOWED_STRUCTURAL_FAMILIES:
        raise SelectiveVetoPilotError(
            f"0034 structural mechanism falls outside frozen family denominator: {mechanism}"
        )
    return normalized


def _load_roll_contracts(root: Path) -> list[dict[str, Any]]:
    path = root / PINNED_ROLL_MAP
    if not path.is_file() or _sha256(path) != PINNED_ROLL_MAP_FILE_SHA256:
        raise SelectiveVetoPilotError("pinned 0034 NQ/YM roll-map file drift")
    value = _read_json(path)
    if str(value.get("roll_map_hash") or "") != PINNED_ROLL_MAP_HASH:
        raise SelectiveVetoPilotError("pinned 0034 NQ/YM roll-map identity drift")
    contracts = value.get("contracts")
    symbols = set(str(v) for v in value.get("symbols") or ())
    if not isinstance(contracts, list) or not {"NQ", "YM"} <= symbols:
        raise SelectiveVetoPilotError("pinned date-aware NQ/YM roll map is incomplete")
    return contracts


def _resolve_contract(
    contracts: Sequence[Mapping[str, Any]], market: str, decision_time_ns: int
) -> str:
    day = _from_ns(decision_time_ns).date().isoformat()
    matches = []
    for row in contracts:
        if str(row.get("root") or row.get("parent_symbol") or "") != market:
            continue
        if bool(row.get("is_micro")):
            continue
        start = str(row.get("active_start") or "")[:10]
        end = str(row.get("active_end") or "")[:10]
        if start and end and start <= day <= end:
            matches.append(str(row.get("contract") or ""))
    matches = sorted({value for value in matches if value})
    if len(matches) != 1:
        raise SelectiveVetoPilotError(
            f"explicit {market} contract resolution is ambiguous for {day}: {matches}"
        )
    return matches[0]


def _load_eligible_session_calendar(root: Path) -> tuple[list[int], str]:
    """Load the immutable 0028 full-coverage trading-day calendar."""

    source = (
        root
        / SOURCE_0028
        / "stage2_episode_evidence"
        / f"{ANCHOR_IDS_0033[0]}.jsonl"
    )
    rows = _read_jsonl(source)
    matches = [
        row
        for row in rows
        if row.get("scenario") == "NORMAL"
        and row.get("horizon") == "FULL_CHRONOLOGICAL_HORIZON"
    ]
    if len(matches) != 1:
        raise SelectiveVetoPilotError("0028 immutable full-coverage calendar is ambiguous")
    days = [
        int(row["session_day"])
        for row in dict(matches[0]["episode"]).get("daily_path") or ()
    ]
    if not days or days != sorted(set(days)):
        raise SelectiveVetoPilotError("0028 immutable full-coverage calendar is invalid")
    return days, _sha256(source)


def build_long_anchor_universe(root: str | Path) -> tuple[list[StructuralAnchor], dict[str, Any]]:
    """Reuse the exact 22 causal anchor structures across their full 0028 history.

    Neighboring updates are collapsed by market, direction, normalized family,
    and a two-minute causal bucket.  The earliest decision wins; outcomes never
    participate in de-duplication.
    """

    base = Path(root).resolve()
    population_path = SOURCE_0028 / "stage1_candidate_population.jsonl"
    population_rows = _read_jsonl(base / population_path)
    population = {
        str(row.get("candidate_id") or ""): row
        for row in population_rows
        if str(row.get("candidate_id") or "") in set(ANCHOR_IDS_0033)
    }
    if set(population) != set(ANCHOR_IDS_0033):
        missing = sorted(set(ANCHOR_IDS_0033) - set(population))
        raise SelectiveVetoPilotError(f"0034 structural anchor sources absent: {missing}")
    contracts = _load_roll_contracts(base)
    eligible_session_days, calendar_source_sha256 = _load_eligible_session_calendar(base)
    event_root = base / SOURCE_0028 / "stage2_event_evidence"
    raw: list[StructuralAnchor] = []
    source_hashes: dict[str, str] = {}
    censored_or_incomplete = 0
    for candidate_id in ANCHOR_IDS_0033:
        source = population[candidate_id]
        candidate = source.get("candidate") or {}
        market = str(candidate.get("market") or "")
        mechanism = str(candidate.get("mechanism") or "UNKNOWN")
        if market not in {"NQ", "YM"}:
            raise SelectiveVetoPilotError("0034 seed anchor market drift")
        event_path = event_root / f"{candidate_id}.jsonl"
        source_hashes[candidate_id] = _sha256(event_path)
        for row in _read_jsonl(event_path):
            decision = int(row["decision_time_ns"])
            if decision >= Q4_START_NS:
                continue
            required_execution = (
                "normal_net_pnl",
                "stressed_net_pnl",
                "normal_fill_price",
                "stressed_fill_price",
                "raw_exit_price",
                "fill_time_ns",
                "outcome_time_ns",
            )
            if any(row.get(name) is None for name in required_execution):
                censored_or_incomplete += 1
                continue
            if str(row.get("market") or "") != market:
                raise SelectiveVetoPilotError("0034 anchor event market drift")
            raw.append(
                StructuralAnchor(
                    anchor_event_id=str(row["event_id"]),
                    source_candidate_id=candidate_id,
                    market=market,
                    execution_market=str(row.get("execution_market") or ""),
                    structural_family=_normalize_family(mechanism),
                    source_mechanism=mechanism,
                    contract=_resolve_contract(contracts, market, decision),
                    decision_time_ns=decision,
                    event_time_ns=int(row["event_time_ns"]),
                    direction=int(row["direction"]),
                    quantity=int(row["quantity"]),
                    timeframe=str(row.get("timeframe") or candidate.get("timeframe") or "1m"),
                    normal_net_pnl_usd=(
                        None if row.get("normal_net_pnl") is None else float(row["normal_net_pnl"])
                    ),
                    stressed_net_pnl_usd=(
                        None if row.get("stressed_net_pnl") is None else float(row["stressed_net_pnl"])
                    ),
                    normal_worst_unrealized_pnl_usd=(
                        None
                        if row.get("normal_worst_unrealized_pnl") is None
                        else float(row["normal_worst_unrealized_pnl"])
                    ),
                    stressed_worst_unrealized_pnl_usd=(
                        None
                        if row.get("stressed_worst_unrealized_pnl") is None
                        else float(row["stressed_worst_unrealized_pnl"])
                    ),
                    normal_best_unrealized_pnl_usd=float(row["normal_best_unrealized_pnl"]),
                    stressed_best_unrealized_pnl_usd=float(row["stressed_best_unrealized_pnl"]),
                    normal_initial_unrealized_pnl_usd=float(row["normal_initial_unrealized_pnl"]),
                    stressed_initial_unrealized_pnl_usd=float(row["stressed_initial_unrealized_pnl"]),
                    normal_marks=tuple(dict(value) for value in row["normal_marks"]),
                    stressed_marks=tuple(dict(value) for value in row["stressed_marks"]),
                    normal_fill_price=float(row["normal_fill_price"]),
                    stressed_fill_price=float(row["stressed_fill_price"]),
                    raw_exit_price=(
                        None if row.get("raw_exit_price") is None else float(row["raw_exit_price"])
                    ),
                    stop_price=float(row["adverse_price"]),
                    target_price=float(row["favorable_price"]),
                    fill_time_ns=int(row["fill_time_ns"]),
                    outcome_time_ns=(
                        None if row.get("outcome_time_ns") is None else int(row["outcome_time_ns"])
                    ),
                    session_day=int(row["session_day"]),
                    outcome=str(row["outcome"]),
                    same_bar_ambiguous=bool(row.get("same_bar_ambiguous", False)),
                    session_compliant=bool(row.get("session_compliant", True)),
                    contract_limit_compliant=bool(row.get("contract_limit_compliant", True)),
                    source_event_hash=stable_hash(row),
                )
            )
    raw_before_calendar_filter = len(raw)
    eligible_day_set = set(eligible_session_days)
    raw = [row for row in raw if row.session_day in eligible_day_set]
    calendar_excluded = raw_before_calendar_filter - len(raw)
    raw.sort(key=lambda row: (row.decision_time_ns, row.source_candidate_id, row.anchor_event_id))
    dedup: dict[tuple[str, int, str, int], StructuralAnchor] = {}
    for row in raw:
        key = (
            row.market,
            row.direction,
            row.structural_family,
            row.decision_time_ns // (120 * 1_000_000_000),
        )
        dedup.setdefault(key, row)
    anchors = sorted(
        dedup.values(),
        key=lambda row: (row.decision_time_ns, row.market, row.anchor_event_id),
    )
    observed_families = {row.structural_family for row in anchors}
    if observed_families != ALLOWED_STRUCTURAL_FAMILIES:
        raise SelectiveVetoPilotError(
            "0034 long anchor universe does not contain exactly the six frozen "
            f"structural families: {sorted(observed_families)}"
        )
    provenance = {
        "source_candidate_ids": list(ANCHOR_IDS_0033),
        "source_candidate_count": len(ANCHOR_IDS_0033),
        "raw_event_count_before_calendar_filter": raw_before_calendar_filter,
        "raw_event_count": len(raw),
        "calibration_events_excluded": calendar_excluded,
        "anchors_generated": len(anchors),
        "duplicates_rejected": len(raw) - len(anchors),
        "structural_family_denominator": sorted(ALLOWED_STRUCTURAL_FAMILIES),
        "observed_structural_families": sorted(observed_families),
        "structural_family_denominator_exact": True,
        "censored_or_incomplete_events_excluded": censored_or_incomplete,
        "deduplication_rule": "MARKET_DIRECTION_NORMALIZED_FAMILY_TWO_MINUTE_BUCKET_EARLIEST",
        "microstructure_outcomes_used": False,
        "q4_event_count": 0,
        "event_file_sha256": source_hashes,
        "population_sha256": _sha256(base / population_path),
        "roll_map_path": str(PINNED_ROLL_MAP),
        "roll_map_file_sha256": PINNED_ROLL_MAP_FILE_SHA256,
        "roll_map_hash": PINNED_ROLL_MAP_HASH,
        "anchor_universe_hash": stable_hash([row.to_dict() for row in anchors]),
        "eligible_session_days": eligible_session_days,
        "eligible_session_day_count": len(eligible_session_days),
        "calendar_source_sha256": calendar_source_sha256,
    }
    return anchors, provenance


def make_event_windows(
    anchors: Sequence[StructuralAnchor], *, pre_seconds: int = 120, post_seconds: int = 60
) -> list[EventWindow]:
    windows = [
        EventWindow(
            market=row.market,
            contract=row.contract,
            start_ns=row.decision_time_ns - pre_seconds * 1_000_000_000,
            end_ns=row.decision_time_ns + post_seconds * 1_000_000_000,
            anchor_ids=(row.anchor_event_id,),
        )
        for row in anchors
    ]
    windows.sort(key=lambda row: (row.market, row.contract, row.start_ns, row.end_ns))
    merged: list[EventWindow] = []
    for row in windows:
        prior = merged[-1] if merged else None
        if (
            prior is not None
            and prior.market == row.market
            and prior.contract == row.contract
            and row.start_ns <= prior.end_ns
        ):
            merged[-1] = EventWindow(
                market=prior.market,
                contract=prior.contract,
                start_ns=prior.start_ns,
                end_ns=max(prior.end_ns, row.end_ns),
                anchor_ids=tuple(sorted(set(prior.anchor_ids + row.anchor_ids))),
            )
        else:
            merged.append(row)
    if any(row.start_ns >= row.end_ns or row.end_ns >= Q4_START_NS for row in merged):
        raise SelectiveVetoPilotError("0034 event window is invalid or touches Q4")
    return merged


def _strongest_markets(seed_audit: Mapping[str, Any]) -> tuple[str, str]:
    seeds = {str(row["policy_id"]): row for row in seed_audit.get("seeds") or ()}
    primary = seeds.get(PRIMARY_SEED_ID)
    if primary is None:
        raise SelectiveVetoPilotError("primary seed attribution is absent")
    totals = {
        str(row["market"]): float(row["stressed_net_usd"])
        for row in primary.get("market_attribution") or ()
    }
    preference = {"NQ": 0, "YM": 1}
    ordered = sorted(
        ("NQ", "YM"),
        key=lambda market: (-totals.get(market, -math.inf), preference[market]),
    )
    return ordered[0], ordered[1]


def _feature_coverage(schema: str) -> str:
    return {
        "trades": "TRADES_ONLY_FLOW_FEATURES;SEED_BBO_FEATURES_UNAVAILABLE",
        "tbbo": "L1_TRADES_BBO_FLOW_IMBALANCE_MICROPRICE_SPREAD_COMPLETE",
        "mbp-1": "L2_TOP_DEPTH_PLUS_L1_FEATURES_COMPLETE",
    }[schema]


def _whole_session_prefix(
    anchors: Sequence[StructuralAnchor], requested_count: int
) -> list[StructuralAnchor]:
    """Return the largest chronological whole-session prefix within N anchors."""

    by_day: dict[str, list[StructuralAnchor]] = defaultdict(list)
    for anchor in anchors:
        by_day[anchor.session_id].append(anchor)
    selected: list[StructuralAnchor] = []
    for day in sorted(by_day):
        session_rows = sorted(
            by_day[day], key=lambda row: (row.decision_time_ns, row.anchor_event_id)
        )
        if len(selected) + len(session_rows) > requested_count:
            break
        selected.extend(session_rows)
    if not selected:
        raise SelectiveVetoPilotError(
            f"0034 requested prefix {requested_count} cannot include one whole session"
        )
    return selected


def _estimate_window(
    metadata: MetadataAPI,
    window: EventWindow,
    schema: str,
    *,
    estimator: ResilientMetadataEstimator | None = None,
) -> tuple[int, int, float, bool]:
    request = window.request(schema)
    if estimator is not None:
        estimate = estimator.estimate(request)
        return (
            estimate.estimated_records,
            estimate.estimated_bytes,
            estimate.estimated_cost_usd,
            estimate.zero_records,
        )
    records = int(metadata.get_record_count(**request))
    size = int(metadata.get_billable_size(**request))
    cost = float(metadata.get_cost(**request))
    if records < 0 or size < 0 or not math.isfinite(cost) or cost < 0.0:
        raise SelectiveVetoPilotError("official Databento metadata returned invalid values")
    return records, size, cost, records == 0


def generate_targeted_cost_matrix(
    metadata: MetadataAPI,
    anchors: Sequence[StructuralAnchor],
    seed_audit: Mapping[str, Any],
    *,
    config: TargetedCostConfig | None = None,
    metadata_cache_path: str | Path | None = None,
) -> dict[str, Any]:
    """Price the full one/two-market × schema × anchor-count grid officially."""

    cfg = config or TargetedCostConfig()
    cfg.validate()
    estimator = (
        ResilientMetadataEstimator(
            metadata,
            cache_path=metadata_cache_path,
            # Test doubles make no endpoint calls.  The production Databento
            # client is always constrained to ten starts per second.
            enforce_rate_limit=type(metadata).__module__.startswith("databento."),
        )
        if metadata_cache_path is not None
        else None
    )
    primary_market, control_market = _strongest_markets(seed_audit)
    by_market = {
        market: [row for row in anchors if row.market == market]
        for market in (primary_market, control_market)
    }
    if any(len(by_market[market]) < max(cfg.window_counts) for market in by_market):
        # A two-market cell is defined over N total anchors, not N per market;
        # only the primary one-market lane must itself reach 1,000.
        if len(by_market[primary_market]) < max(cfg.window_counts):
            raise SelectiveVetoPilotError("strongest market has fewer than 1,000 frozen anchors")
    rows: list[dict[str, Any]] = []
    role_costs = {role: 0.0 for role in ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")}
    cell_plans: list[dict[str, Any]] = []
    unique_windows: dict[
        tuple[str, int, int, str], tuple[EventWindow, str]
    ] = {}
    cache: dict[tuple[str, int, int, str], tuple[int, int, float, bool]] = {}
    for market_count in (1, 2):
        markets = (primary_market,) if market_count == 1 else (primary_market, control_market)
        eligible = sorted(
            (row for row in anchors if row.market in markets),
            key=lambda row: (row.decision_time_ns, row.market, row.anchor_event_id),
        )
        for count in cfg.window_counts:
            if len(eligible) < count:
                raise SelectiveVetoPilotError("0034 cost-grid anchor denominator is incomplete")
            selected = _whole_session_prefix(eligible, count)
            windows = make_event_windows(
                selected,
                pre_seconds=cfg.pre_decision_seconds,
                post_seconds=cfg.post_decision_seconds,
            )
            for schema in cfg.schemas:
                cell_plans.append(
                    {
                        "market_count": market_count,
                        "markets": markets,
                        "count": count,
                        "selected": selected,
                        "windows": windows,
                        "schema": schema,
                    }
                )
                for window in windows:
                    key = (window.contract, window.start_ns, window.end_ns, schema)
                    unique_windows.setdefault(key, (window, schema))

    # Resolve every unique window/schema triple before aggregation.  The
    # estimator performs only metadata I/O in its worker threads and appends
    # completed triples to the persistent cache on this calling thread.
    prefetch = list(unique_windows.items())
    if estimator is not None:
        estimates = estimator.estimate_many(
            window.request(schema) for _, (window, schema) in prefetch
        )
        for ((key, _), estimate) in zip(prefetch, estimates, strict=True):
            cache[key] = (
                estimate.estimated_records,
                estimate.estimated_bytes,
                estimate.estimated_cost_usd,
                estimate.zero_records,
            )
    else:
        for key, (window, schema) in prefetch:
            cache[key] = _estimate_window(metadata, window, schema)

    for plan in cell_plans:
        market_count = int(plan["market_count"])
        markets = tuple(plan["markets"])
        count = int(plan["count"])
        selected = list(plan["selected"])
        windows = list(plan["windows"])
        schema = str(plan["schema"])
        records = 0
        size = 0
        cost = 0.0
        zero_record_windows = 0
        requests: list[dict[str, Any]] = []
        for window in windows:
            key = (window.contract, window.start_ns, window.end_ns, schema)
            estimate = cache[key]
            records += estimate[0]
            size += estimate[1]
            cost += estimate[2]
            zero_record_windows += int(estimate[3])
            requests.append(
                {
                    **window.to_dict(),
                    "request": window.request(schema),
                    "estimated_records": estimate[0],
                    "estimated_bytes": estimate[1],
                    "estimated_cost_usd": estimate[2],
                    "zero_records": estimate[3],
                }
            )
        core = {
            "dataset": DATASET,
            "schema": schema,
            "anchor_window_count": count,
            "effective_anchor_count": len(selected),
            "whole_session_prefix": True,
            "market_count": market_count,
            "markets": list(markets),
            "strongest_market": primary_market,
            "control_market": control_market if market_count == 2 else None,
            "merged_window_count": len(windows),
            "merged_window_duration_seconds": float(
                math.fsum(row.duration_seconds for row in windows)
            ),
            "estimated_records": records,
            "estimated_bytes": size,
            "estimated_cost_usd": cost,
            "zero_record_window_count": zero_record_windows,
            "contains_zero_record_windows": zero_record_windows > 0,
            "feature_coverage": _feature_coverage(schema),
            "requests": requests,
            "anchor_ids": [row.anchor_event_id for row in selected],
        }
        rows.append({**core, "estimate_fingerprint": stable_hash(core)})

    affordable_limit = min(
        cfg.maximum_incremental_spend_usd,
        cfg.current_remaining_budget_usd - cfg.minimum_budget_reserve_usd,
    )
    deployable = [
        row
        for row in rows
        if row["schema"] in {"tbbo", "mbp-1"}
        and int(row["estimated_records"]) > 0
        and int(row["zero_record_window_count"]) == 0
        and float(row["estimated_cost_usd"]) <= affordable_limit + 1e-9
    ]
    schema_rank = {"tbbo": 2, "mbp-1": 1}
    selected_offer = (
        max(
            deployable,
            key=lambda row: (
                int(row["anchor_window_count"]),
                int(row["market_count"]),
                schema_rank[str(row["schema"])],
                -float(row["estimated_cost_usd"]),
            ),
        )
        if deployable
        else None
    )
    if selected_offer:
        selected_anchor_ids = list(selected_offer["anchor_ids"])
        thirds = _chronological_roles(
            [row for row in anchors if row.anchor_event_id in set(selected_anchor_ids)]
        )
        request_cost_by_anchor: dict[str, float] = {}
        for request in selected_offer["requests"]:
            ids = list(request["anchor_ids"])
            share = float(request["estimated_cost_usd"]) / max(len(ids), 1)
            for anchor_id in ids:
                request_cost_by_anchor[anchor_id] = share
        for role, role_ids in thirds.items():
            role_costs[role] = float(
                math.fsum(request_cost_by_anchor.get(value, 0.0) for value in role_ids)
            )
    result = {
        "status": "OFFICIAL_COST_MATRIX_COMPLETE" if selected_offer else "NO_AFFORDABLE_OFFER",
        "official_metadata_get_cost_used": True,
        "official_record_count_used": True,
        "official_billable_size_used": True,
        "full_session_matrix_reused_as_final": False,
        "dataset": DATASET,
        "strongest_market": primary_market,
        "control_market": control_market,
        "rows": rows,
        "selected_offer": selected_offer,
        "chronological_role_costs": role_costs,
        "effective_affordable_limit_usd": affordable_limit,
        "new_mbo_purchase_allowed": False,
        "q4_accessed": False,
        "metadata_resilience": {
            "append_only_cache_enabled": metadata_cache_path is not None,
            "complete_triples_only": True,
            "maximum_endpoint_calls_per_second": 10.0,
            "bounded_retry_count": 3,
            "zero_record_windows_excluded_from_selection": True,
        },
    }
    return {**result, "cost_matrix_hash": stable_hash(result)}


def _chronological_roles(anchors: Sequence[StructuralAnchor]) -> dict[str, list[str]]:
    """Split on session boundaries so final-development contains whole dates."""

    dates = sorted({row.session_id for row in anchors})
    if len(dates) < 3:
        raise SelectiveVetoPilotError("long sample lacks three chronological role blocks")
    discovery_end = max(1, int(math.floor(len(dates) * 0.60)))
    validation_end = max(discovery_end + 1, int(math.floor(len(dates) * 0.80)))
    validation_end = min(validation_end, len(dates) - 1)
    role_dates = {
        "DISCOVERY": set(dates[:discovery_end]),
        "VALIDATION": set(dates[discovery_end:validation_end]),
        "FINAL_DEVELOPMENT": set(dates[validation_end:]),
    }
    return {
        role: [row.anchor_event_id for row in anchors if row.session_id in values]
        for role, values in role_dates.items()
    }


def _seed_runtime_mapping(audit: Mapping[str, Any]) -> dict[str, Any]:
    policies = {}
    for seed in audit.get("seeds") or ():
        policies[str(seed["policy_id"])] = {
            "policy_id": seed["policy_id"],
            "robustness_status": seed["robustness_status"],
            "leave_one_opportunity_out": seed["leave_one_opportunity_out"],
            "top_trade_removal": seed["top_trade_removal"],
            "leave_one_anchor_family_out": seed["leave_one_anchor_family_out"],
            "cost_stress": {
                "contract": audit["cost_stress_contract"],
                "heldout": seed["heldout_summary"],
            },
            "feature_dependencies": seed["feature_dependency"],
            "account_size_matrix": seed["account_size_matrix"],
            "market_attribution": seed["market_attribution"],
            "anchor_family_attribution": seed["anchor_family_attribution"],
            "session_attribution": seed["session_attribution"],
            "opportunity_attribution": seed["opportunity_attribution"],
            "robustness_criteria": seed["robustness_criteria"],
            "frozen_policy": seed["frozen_policy"],
        }
    return {
        "decision": audit["result"],
        "completed_before_cost_estimation": True,
        "completed_before_purchase": True,
        "actual_spend_usd": 0.0,
        "policies": policies,
        "audit_fingerprint": audit["audit_fingerprint"],
    }


def _no_purchase_acquisition(prior_budget: float) -> dict[str, Any]:
    return {
        "purchase_performed": False,
        "actual_spend_usd": 0.0,
        "prior_budget_usd": prior_budget,
        "remaining_budget_usd": prior_budget,
        "q4_accessed": False,
        "broker_connections": 0,
        "orders": 0,
        "independent_anchors_acquired": 0,
        "raw_data_immutable": True,
        "temporal_roles_frozen_before_download": True,
        "data_access_ledger_appended": False,
        "budget_ledger_appended": False,
        "acquisition_receipt_fingerprint": None,
        "official_estimate_fingerprint": None,
        "manifest_bound_data_purchase_count": 0,
        "unmanifested_data_purchase_count": 0,
        "budget_ledger_before_sha256": None,
        "budget_ledger_after_sha256": None,
        "data_access_ledger_before_sha256": None,
        "data_access_ledger_after_sha256": None,
    }


def _empty_cost(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "official_metadata_get_cost_used": False,
        "full_session_matrix_reused_as_final": False,
        "rows": [],
        "selected_offer": None,
        "chronological_role_costs": {
            "DISCOVERY": 0.0,
            "VALIDATION": 0.0,
            "FINAL_DEVELOPMENT": 0.0,
        },
    }


def _empty_long(status: str, decision: str) -> dict[str, Any]:
    return {
        "status": status,
        "decision": decision,
        "policy_frozen_before_final_development": False,
        "policy": None,
        "paired_results": [],
        "account_size_matrix": [],
        "fastest_viable_account_size": None,
    }


def _diagnostic_forward(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "authorized_research_feed": False,
        "policy_ids": [PRIMARY_SEED_ID, SECONDARY_SEED_ID],
        "append_only": True,
        "zero_order": True,
        "parameter_changes": 0,
        "economic_promotion_allowed": False,
        "paper_shadow_ready": False,
        "broker_connections": 0,
        "orders": 0,
        "fresh_events_processed": 0,
    }


def _official_client() -> Any:
    key = load_api_key()
    if not key:
        raise SelectiveVetoPilotError("DATABENTO_API_KEY is unavailable for official cost estimation")
    import databento as db

    return db.Historical(key)


def _write_json_once(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(dict(value), sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    if path.is_file():
        if path.read_text(encoding="utf-8") != encoded:
            raise SelectiveVetoPilotError(f"immutable 0034 artifact differs: {path}")
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(encoded, encoding="utf-8")
    os.replace(temporary, path)


@dataclass(frozen=True, slots=True)
class _DownloadRetryPolicy:
    """Bounded vendor-download contract for the one 0034 tranche.

    Downloads remain serial: the two permitted CPU workers are reserved for
    economic replay, while vendor I/O is paced independently here.  Retrying
    is limited to explicit throttling and transient server failures.
    """

    maximum_calls_per_second: float = 2.0
    maximum_retries: int = 3
    base_retry_seconds: float = 0.5
    maximum_retry_seconds: float = 30.0


class _DownloadCallGate:
    def __init__(
        self,
        *,
        policy: _DownloadRetryPolicy | None = None,
        enforce_rate_limit: bool,
    ) -> None:
        self.policy = policy or _DownloadRetryPolicy()
        self.enforce_rate_limit = bool(enforce_rate_limit)
        self.last_call: float | None = None
        self.call_count = 0
        self.retry_count = 0

    def wait_for_slot(self) -> None:
        now = time.monotonic()
        if self.enforce_rate_limit and self.last_call is not None:
            interval = 1.0 / self.policy.maximum_calls_per_second
            remaining = interval - (now - self.last_call)
            if remaining > 0.0:
                time.sleep(remaining)
                now = time.monotonic()
        self.last_call = now
        self.call_count += 1


def _vendor_http_status(exc: Exception) -> int | None:
    for name in ("http_status", "status_code", "status"):
        value = getattr(exc, name, None)
        if isinstance(value, int):
            return value
    response = getattr(exc, "response", None)
    value = getattr(response, "status_code", None)
    return value if isinstance(value, int) else None


def _official_databento_object(value: Any) -> bool:
    return type(value).__module__.startswith("databento.")


def _download_window_bounded(
    client: Any,
    request: Mapping[str, Any],
    path: Path,
    *,
    gate: _DownloadCallGate,
) -> dict[str, int]:
    """Download one immutable window with bounded transient retries.

    A process-specific temporary file is never reused after an interrupted
    process.  A stale temporary file therefore causes a fail-closed recovery
    instead of an ambiguous vendor re-request and possible double charge.
    """

    stale = sorted(path.parent.glob(f".{path.name}.*.partial"))
    if stale:
        raise SelectiveVetoPilotError(
            "0034 has an unresolved partial window; refusing automatic redownload"
        )
    temporary = path.with_name(f".{path.name}.{os.getpid()}.partial")
    attempts = 0
    for attempt in range(gate.policy.maximum_retries + 1):
        attempts += 1
        gate.wait_for_slot()
        temporary.unlink(missing_ok=True)
        try:
            client.timeseries.get_range(
                **dict(request),
                stype_out="instrument_id",
                path=str(temporary),
            )
            if not temporary.is_file() or temporary.stat().st_size <= 0:
                raise SelectiveVetoPilotError(
                    "Databento returned an empty event-window file"
                )
            os.replace(temporary, path)
            return {"attempts": attempts, "retries": attempts - 1}
        except Exception as exc:
            status = _vendor_http_status(exc)
            retryable = status == 429 or (status is not None and 500 <= status <= 599)
            if (
                not retryable
                or attempt >= gate.policy.maximum_retries
                or isinstance(exc, SelectiveVetoPilotError)
            ):
                # A failed in-process request has a known exception outcome;
                # its incomplete process-local temporary can be discarded.
                temporary.unlink(missing_ok=True)
                if isinstance(exc, SelectiveVetoPilotError):
                    raise
                raise SelectiveVetoPilotError(
                    "Databento event-window download failed"
                ) from exc
            gate.retry_count += 1
            temporary.unlink(missing_ok=True)
            time.sleep(
                min(
                    gate.policy.base_retry_seconds * (2**attempt),
                    gate.policy.maximum_retry_seconds,
                )
            )
    raise AssertionError("bounded 0034 download retry loop exhausted")


def _revalidate_offer_metadata(
    metadata: MetadataAPI,
    offer: Mapping[str, Any],
    *,
    cache_path: Path,
) -> dict[str, Any]:
    """Reprice every selected window immediately before authorization.

    This is deliberately a fresh, acquisition-specific append-only cache,
    distinct from the grid-estimation cache.  It is resumable if metadata I/O
    is interrupted, concurrent because it is network-bound, and globally
    capped at ten endpoint starts per second for the official client.
    """

    offer_contract = _validated_offer_contract(offer)
    request_rows = list(offer_contract["windows"])
    estimator = ResilientMetadataEstimator(
        metadata,
        cache_path=cache_path,
        retry_policy=MetadataRetryPolicy(
            maximum_endpoint_calls_per_second=10.0,
            maximum_retries=3,
        ),
        enforce_rate_limit=_official_databento_object(metadata),
    )
    estimates = estimator.estimate_many(
        (_mapping(row, "selected offer request")["request"] for row in request_rows),
        max_workers=32,
    )
    windows: list[dict[str, Any]] = []
    for index, (row, estimate) in enumerate(zip(request_rows, estimates, strict=True)):
        source = _mapping(row, f"selected offer request {index}")
        if estimate.zero_records:
            raise SelectiveVetoPilotError(
                "selected 0034 window has zero records at acquisition revalidation"
            )
        core = {
            "window_index": index,
            "request": dict(estimate.request),
            "request_fingerprint": estimate.request_fingerprint,
            "metadata_estimate_hash": estimate.estimate_hash,
            "estimated_records": estimate.estimated_records,
            "estimated_bytes": estimate.estimated_bytes,
            "authorized_cost_usd": estimate.estimated_cost_usd,
            "anchor_ids": list(source.get("anchor_ids") or ()),
            "market": str(source.get("market") or ""),
            "contract": str(source.get("contract") or ""),
        }
        if (
            dict(estimate.request) != dict(source["request"])
            or estimate.request_fingerprint != source["request_fingerprint"]
        ):
            raise SelectiveVetoPilotError(
                "0034 metadata endpoint changed the frozen request identity"
            )
        windows.append({**core, "window_metadata_hash": stable_hash(core)})
    total_cost = float(math.fsum(float(row["authorized_cost_usd"]) for row in windows))
    core = {
        "schema": "hydra_selective_veto_acquisition_metadata_revalidation_v1",
        "data_schema": str(offer_contract["schema"]),
        "offer_contract_hash": str(offer_contract["offer_contract_hash"]),
        "estimate_fingerprint": str(offer["estimate_fingerprint"]),
        "window_count": len(windows),
        "windows": windows,
        "authorized_cost_usd": total_cost,
        "maximum_endpoint_calls_per_second": 10.0,
        "maximum_retries": 3,
        "concurrent_io_worker_limit": 32,
        "endpoint_call_count": estimator.endpoint_call_count,
        "retry_count": estimator.retry_count,
        "cache_hit_count": estimator.cache_hit_count,
        "cache_miss_count": estimator.cache_miss_count,
    }
    return {**core, "revalidation_hash": stable_hash(core)}


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SelectiveVetoPilotError(f"0034 {label} is not a mapping")
    return value


def _validate_self_hash(
    value: Mapping[str, Any], fingerprint_field: str, label: str
) -> str:
    fingerprint = str(value.get(fingerprint_field) or "")
    core = {key: item for key, item in value.items() if key != fingerprint_field}
    if not fingerprint or fingerprint != stable_hash(core):
        raise SelectiveVetoPilotError(f"0034 {label} self-hash drift")
    return fingerprint


def _parse_request_time(value: Any, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise SelectiveVetoPilotError(
            f"0034 acquisition {label} is not an ISO timestamp"
        ) from exc
    if parsed.tzinfo is None:
        raise SelectiveVetoPilotError(
            f"0034 acquisition {label} lacks an explicit timezone"
        )
    return parsed.astimezone(UTC)


def _validated_offer_contract(offer: Mapping[str, Any]) -> dict[str, Any]:
    """Authenticate the immutable offer and its exact event-window identity."""

    estimate_fingerprint = _validate_self_hash(
        offer, "estimate_fingerprint", "selected offer"
    )
    schema = str(offer.get("schema") or "")
    if schema not in SCHEMAS or str(offer.get("dataset") or "") != DATASET:
        raise SelectiveVetoPilotError("0034 selected offer dataset/schema drift")
    request_rows = list(offer.get("requests") or ())
    if not request_rows:
        raise SelectiveVetoPilotError("0034 selected offer contains no requests")
    windows: list[dict[str, Any]] = []
    flattened_anchor_ids: list[str] = []
    q4_start = datetime(2024, 10, 1, tzinfo=UTC)
    for index, value in enumerate(request_rows):
        source = _mapping(value, f"offer request {index}")
        request = dict(_mapping(source.get("request"), f"offer request {index} body"))
        contract = str(source.get("contract") or "")
        market = str(source.get("market") or "")
        anchor_ids = [str(item) for item in source.get("anchor_ids") or ()]
        symbols = [str(item) for item in request.get("symbols") or ()]
        if (
            str(request.get("dataset") or "") != DATASET
            or str(request.get("schema") or "") != schema
            or str(request.get("stype_in") or "") != "raw_symbol"
            or symbols != [contract]
            or not contract
            or not market
            or not anchor_ids
            or len(anchor_ids) != len(set(anchor_ids))
            or str(source.get("start") or "") != str(request.get("start") or "")
            or str(source.get("end") or "") != str(request.get("end") or "")
        ):
            raise SelectiveVetoPilotError(
                "0034 selected offer request/schema/symbol/anchor identity drift"
            )
        start = _parse_request_time(request["start"], "start")
        end = _parse_request_time(request["end"], "end")
        if not start < end or start >= q4_start or end > q4_start:
            raise SelectiveVetoPilotError(
                "0034 selected offer crosses forbidden Q4 bounds"
            )
        flattened_anchor_ids.extend(anchor_ids)
        windows.append(
            {
                "window_index": index,
                "request": request,
                "request_fingerprint": stable_hash(request),
                "anchor_ids": anchor_ids,
                "market": market,
                "contract": contract,
            }
        )
    offer_anchor_ids = [str(item) for item in offer.get("anchor_ids") or ()]
    if (
        len(flattened_anchor_ids) != len(set(flattened_anchor_ids))
        or sorted(flattened_anchor_ids) != sorted(offer_anchor_ids)
        or int(offer.get("effective_anchor_count", -1)) != len(offer_anchor_ids)
        or int(offer.get("merged_window_count", -1)) != len(windows)
    ):
        raise SelectiveVetoPilotError(
            "0034 selected offer anchor/window denominator drift"
        )
    core = {
        "estimate_fingerprint": estimate_fingerprint,
        "dataset": DATASET,
        "schema": schema,
        "anchor_ids": offer_anchor_ids,
        "windows": windows,
    }
    return {**core, "offer_contract_hash": stable_hash(core)}


def _validate_metadata_revalidation(
    revalidation: Mapping[str, Any], offer_contract: Mapping[str, Any]
) -> list[dict[str, Any]]:
    _validate_self_hash(
        revalidation, "revalidation_hash", "metadata revalidation"
    )
    expected_windows = list(offer_contract["windows"])
    actual_windows = list(revalidation.get("windows") or ())
    if (
        str(revalidation.get("estimate_fingerprint") or "")
        != str(offer_contract["estimate_fingerprint"])
        or str(revalidation.get("offer_contract_hash") or "")
        != str(offer_contract["offer_contract_hash"])
        or str(revalidation.get("data_schema") or "")
        != str(offer_contract["schema"])
        or int(revalidation.get("window_count", -1)) != len(expected_windows)
        or len(actual_windows) != len(expected_windows)
    ):
        raise SelectiveVetoPilotError("0034 metadata revalidation identity drift")
    validated: list[dict[str, Any]] = []
    for expected, value in zip(expected_windows, actual_windows, strict=True):
        actual = dict(_mapping(value, "metadata-revalidated window"))
        _validate_self_hash(
            actual, "window_metadata_hash", "metadata-revalidated window"
        )
        if (
            int(actual.get("window_index", -1)) != int(expected["window_index"])
            or dict(_mapping(actual.get("request"), "revalidated request"))
            != dict(expected["request"])
            or str(actual.get("request_fingerprint") or "")
            != str(expected["request_fingerprint"])
            or [str(item) for item in actual.get("anchor_ids") or ()]
            != list(expected["anchor_ids"])
            or str(actual.get("market") or "") != str(expected["market"])
            or str(actual.get("contract") or "") != str(expected["contract"])
        ):
            raise SelectiveVetoPilotError(
                "0034 offer/revalidation request or anchor drift"
            )
        validated.append(actual)
    return validated


def _reuse_prior_acquisition_after_bounded_repair(
    offer_contract: Mapping[str, Any],
    offer: Mapping[str, Any],
    *,
    root: Path,
    manifest: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Reuse one exact immutable purchase after the bounded adapter repair.

    This path performs no metadata or timeseries API call and no ledger write.
    It validates the old manifest-bound acquisition down to every raw DBN and
    per-window receipt, then emits a new self-hashed provenance wrapper bound
    to the repaired manifest.  The original receipt remains unchanged.
    """

    repair_value = manifest.get("post_purchase_execution_bound_repair")
    if repair_value is None:
        return None
    repair = _mapping(repair_value, "post-purchase execution-bound repair")
    if (
        repair.get("classification")
        != "POST_PURCHASE_PRE_OUTCOME_EMPTY_EXECUTION_INTERVAL_DEFECT"
        or repair.get("repair_scope")
        != "FIRST_POST_DECISION_QUOTE_WITHIN_FROZEN_EVENT_WINDOW"
        or repair.get("prior_raw_bundle_reuse_allowed") is not True
        or repair.get("new_purchase_after_repair_allowed") is not False
        or repair.get("raw_records_changed") is not False
        or repair.get("anchor_set_changed") is not False
        or repair.get("temporal_roles_changed") is not False
        or repair.get("actions_or_thresholds_changed") is not False
        or int(repair.get("post_decision_entry_bound_seconds", -1)) != 60
    ):
        raise SelectiveVetoPilotError(
            "0034 post-purchase repair reuse authorization drift"
        )

    receipt_path = (root / str(repair.get("prior_receipt_path") or "")).resolve()
    allowed_root = (root / "data/cache/databento/selective_veto_0034").resolve()
    try:
        receipt_path.relative_to(allowed_root)
    except ValueError as exc:
        raise SelectiveVetoPilotError(
            "0034 prior acquisition receipt escapes immutable cache"
        ) from exc
    if (
        not receipt_path.is_file()
        or _sha256(receipt_path) != str(repair.get("prior_receipt_sha256") or "")
    ):
        raise SelectiveVetoPilotError("0034 prior acquisition receipt checksum drift")

    receipt = _read_json(receipt_path)
    _validate_self_hash(
        receipt, "acquisition_receipt_fingerprint", "prior acquisition receipt"
    )
    prior_manifest_hash = str(repair.get("prior_manifest_hash") or "")
    prior_request_id = str(repair.get("prior_request_id") or "")
    intent_path = (root / str(repair.get("prior_intent_path") or "")).resolve()
    authorization_path = (
        root / str(repair.get("prior_authorization_path") or "")
    ).resolve()
    try:
        intent_path.relative_to(allowed_root)
        authorization_path.relative_to(allowed_root)
    except ValueError as exc:
        raise SelectiveVetoPilotError(
            "0034 prior authorization chain escapes immutable cache"
        ) from exc
    if (
        not intent_path.is_file()
        or _sha256(intent_path) != str(repair.get("prior_intent_sha256") or "")
        or not authorization_path.is_file()
        or _sha256(authorization_path)
        != str(repair.get("prior_authorization_sha256") or "")
    ):
        raise SelectiveVetoPilotError(
            "0034 prior intent/authorization checksum drift"
        )
    intent = _read_json(intent_path)
    authorization = _read_json(authorization_path)
    _validate_self_hash(intent, "intent_fingerprint", "prior acquisition intent")
    _validate_self_hash(
        authorization,
        "authorization_fingerprint",
        "prior download authorization",
    )
    revalidation = dict(
        _mapping(intent.get("metadata_revalidation"), "prior metadata revalidation")
    )
    revalidated_windows = _validate_metadata_revalidation(
        revalidation, offer_contract
    )
    prior_windows: list[dict[str, Any]] = []
    for window in revalidated_windows:
        window_core = {
            "campaign_request_id": prior_request_id,
            "window_index": int(window["window_index"]),
            "request_fingerprint": str(window["request_fingerprint"]),
            "metadata_estimate_hash": str(window["metadata_estimate_hash"]),
        }
        prior_windows.append(
            {**window, "window_request_id": request_id_for(window_core)}
        )
    if (
        intent.get("request_id") != prior_request_id
        or intent.get("manifest_hash") != prior_manifest_hash
        or intent.get("estimate_fingerprint")
        != offer_contract["estimate_fingerprint"]
        or intent.get("offer_contract_hash") != offer_contract["offer_contract_hash"]
        or intent.get("data_schema") != offer_contract["schema"]
        or intent.get("intent_fingerprint")
        != str(repair.get("prior_intent_fingerprint") or "")
        or intent.get("metadata_revalidation_hash")
        != revalidation.get("revalidation_hash")
        or intent.get("metadata_revalidation_hash")
        != str(repair.get("prior_metadata_revalidation_hash") or "")
        or list(intent.get("windows") or ()) != prior_windows
    ):
        raise SelectiveVetoPilotError("0034 prior acquisition intent drift")
    if (
        authorization.get("request_id") != prior_request_id
        or authorization.get("manifest_hash") != prior_manifest_hash
        or authorization.get("intent_fingerprint")
        != intent.get("intent_fingerprint")
        or authorization.get("metadata_revalidation_hash")
        != intent.get("metadata_revalidation_hash")
        or authorization.get("offer_contract_hash")
        != offer_contract["offer_contract_hash"]
        or authorization.get("data_schema") != offer_contract["schema"]
        or authorization.get("window_contract_hash") != stable_hash(prior_windows)
        or int(authorization.get("window_count", -1)) != len(prior_windows)
        or authorization.get("authorization_fingerprint")
        != str(repair.get("prior_authorization_fingerprint") or "")
    ):
        raise SelectiveVetoPilotError("0034 prior download authorization drift")
    files = receipt.get("files")
    expected_windows = list(offer_contract["windows"])
    if (
        receipt.get("schema") != "hydra_selective_veto_acquisition_receipt_v2"
        or receipt.get("campaign_id") != CAMPAIGN_ID
        or receipt.get("manifest_hash") != prior_manifest_hash
        or receipt.get("request_id") != prior_request_id
        or receipt.get("acquisition_receipt_fingerprint")
        != str(repair.get("prior_acquisition_receipt_fingerprint") or "")
        or receipt.get("estimate_fingerprint")
        != offer_contract["estimate_fingerprint"]
        or receipt.get("offer_contract_hash")
        != offer_contract["offer_contract_hash"]
        or receipt.get("intent_fingerprint") != intent.get("intent_fingerprint")
        or receipt.get("authorization_fingerprint")
        != authorization.get("authorization_fingerprint")
        or receipt.get("metadata_revalidation_hash")
        != intent.get("metadata_revalidation_hash")
        or receipt.get("authorization_receipt_path") != str(authorization_path)
        or receipt.get("authorization_receipt_sha256")
        != _sha256(authorization_path)
        or not isinstance(files, list)
        or len(files) != len(expected_windows)
        or int(receipt.get("window_count", -1)) != len(expected_windows)
        or int(receipt.get("completed_window_count", -1))
        != len(expected_windows)
        or int(receipt.get("independent_anchors_acquired", -1))
        != int(offer.get("effective_anchor_count", -1))
        or bool(receipt.get("q4_accessed"))
        or int(receipt.get("broker_connections", -1)) != 0
        or int(receipt.get("orders", -1)) != 0
    ):
        raise SelectiveVetoPilotError("0034 prior acquisition identity drift")

    validated_files: list[Mapping[str, Any]] = []
    for item_value, expected, prior_window in zip(
        files, expected_windows, prior_windows, strict=True
    ):
        item = _mapping(item_value, "prior acquisition file")
        raw_path = Path(str(item.get("raw_path") or "")).resolve()
        window_receipt_path = Path(
            str(item.get("window_receipt_path") or "")
        ).resolve()
        try:
            raw_path.relative_to(allowed_root)
            window_receipt_path.relative_to(allowed_root)
        except ValueError as exc:
            raise SelectiveVetoPilotError(
                "0034 prior acquisition file escapes immutable cache"
            ) from exc
        expected_request = _mapping(expected.get("request"), "offer request")
        if (
            item.get("request_fingerprint")
            != expected.get("request_fingerprint")
            or item.get("window_request_id")
            != prior_window.get("window_request_id")
            or not math.isclose(
                float(item.get("authorized_cost_usd", math.nan)),
                float(prior_window.get("authorized_cost_usd", math.nan)),
                rel_tol=0.0,
                abs_tol=1e-9,
            )
            or item.get("schema") != offer_contract["schema"]
            or list(item.get("symbols") or ())
            != list(expected_request.get("symbols") or ())
            or list(item.get("anchor_ids") or ())
            != list(expected.get("anchor_ids") or ())
            or item.get("market") != expected.get("market")
            or item.get("contract") != expected.get("contract")
            or item.get("start") != expected_request.get("start")
            or item.get("end") != expected_request.get("end")
            or not raw_path.is_file()
            or raw_path.stat().st_size != int(item.get("raw_size_bytes", -1))
            or sha256_file(raw_path) != str(item.get("raw_sha256") or "")
            or not window_receipt_path.is_file()
            or _sha256(window_receipt_path)
            != str(item.get("window_receipt_sha256") or "")
        ):
            raise SelectiveVetoPilotError(
                "0034 prior acquisition raw/window contract drift"
            )
        window_receipt = _read_json(window_receipt_path)
        _validate_self_hash(
            window_receipt,
            "window_receipt_fingerprint",
            "prior window receipt",
        )
        if (
            window_receipt.get("campaign_request_id") != prior_request_id
            or window_receipt.get("window_request_id")
            != item.get("window_request_id")
            or int(window_receipt.get("window_index", -1))
            != int(expected.get("window_index", -1))
            or window_receipt.get("request_fingerprint")
            != expected.get("request_fingerprint")
            or window_receipt.get("metadata_estimate_hash")
            != prior_window.get("metadata_estimate_hash")
            or window_receipt.get("raw_path") != str(raw_path)
            or window_receipt.get("raw_sha256") != item.get("raw_sha256")
        ):
            raise SelectiveVetoPilotError(
                "0034 prior window receipt identity drift"
            )
        validated_files.append(item)

    actual_cost = float(receipt.get("actual_spend_usd", math.nan))
    expected_cost = float(
        math.fsum(float(item["authorized_cost_usd"]) for item in validated_files)
    )
    flattened_ids = [
        str(anchor_id)
        for item in validated_files
        for anchor_id in item.get("anchor_ids") or ()
    ]
    if (
        receipt.get("bundle_hash") != stable_hash(files)
        or receipt.get("bundle_hash") != str(repair.get("prior_bundle_hash") or "")
        or not math.isclose(actual_cost, expected_cost, rel_tol=0.0, abs_tol=1e-9)
        or sorted(flattened_ids)
        != sorted(str(value) for value in offer.get("anchor_ids") or ())
        or len(flattened_ids) != len(set(flattened_ids))
    ):
        raise SelectiveVetoPilotError(
            "0034 prior acquisition aggregate reconciliation drift"
        )

    budget_path = root / "reports/data_budget/databento_spend_ledger.jsonl"
    budget_rows = [
        row
        for row in read_ledger(budget_path)
        if str(row.get("request_id") or "")
        in {str(window["window_request_id"]) for window in prior_windows}
    ]
    reserved = [
        row for row in budget_rows if row.get("download_status") == "ESTIMATED_ONLY"
    ]
    downloaded = [
        row for row in budget_rows if row.get("download_status") == "DOWNLOADED"
    ]
    expected_window_ids = {
        str(window["window_request_id"]) for window in prior_windows
    }
    if (
        len(reserved) != len(prior_windows)
        or len(downloaded) != len(prior_windows)
        or {str(row.get("request_id")) for row in reserved} != expected_window_ids
        or {str(row.get("request_id")) for row in downloaded} != expected_window_ids
        or not math.isclose(
            math.fsum(float(row.get("actual_cost_usd") or 0.0) for row in downloaded),
            actual_cost,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
    ):
        raise SelectiveVetoPilotError(
            "0034 prior acquisition budget-ledger reconciliation drift"
        )
    access_path = root / "reports/data_access/data_access_ledger.jsonl"
    matching_access = [
        row
        for row in read_ledger(access_path)
        if prior_request_id
        in {str(value) for value in row.get("candidate_ids") or ()}
        and CAMPAIGN_ID
        in {str(value) for value in row.get("candidate_ids") or ()}
    ]
    if (
        len(matching_access) != 1
        or matching_access[0].get("data_role") != "CONTAMINATED_DEVELOPMENT"
        or matching_access[0].get("freeze_manifest_hash") != prior_manifest_hash
    ):
        raise SelectiveVetoPilotError(
            "0034 prior acquisition data-access reconciliation drift"
        )

    reuse = {
        **{
            key: value
            for key, value in receipt.items()
            if key not in {"manifest_hash", "acquisition_receipt_fingerprint"}
        },
        "manifest_hash": str(manifest["manifest_hash"]),
        "original_manifest_hash": prior_manifest_hash,
        "original_request_id": prior_request_id,
        "original_acquisition_receipt_path": str(receipt_path),
        "original_acquisition_receipt_sha256": _sha256(receipt_path),
        "original_acquisition_receipt_fingerprint": str(
            receipt["acquisition_receipt_fingerprint"]
        ),
        "original_intent_path": str(intent_path),
        "original_intent_sha256": _sha256(intent_path),
        "original_authorization_path": str(authorization_path),
        "original_authorization_sha256": _sha256(authorization_path),
        "budget_ledger_at_reuse_sha256": _sha256(budget_path),
        "data_access_ledger_at_reuse_sha256": _sha256(access_path),
        "post_purchase_execution_bound_repair": True,
        "prior_raw_bundle_reused": True,
        "new_purchase_performed_after_repair": False,
        "additional_spend_after_repair_usd": 0.0,
    }
    return {
        **reuse,
        "acquisition_receipt_fingerprint": stable_hash(reuse),
    }


def _acquire_selected_offer(
    client: Any,
    offer: Mapping[str, Any],
    *,
    root: Path,
    manifest: Mapping[str, Any],
    config: TargetedCostConfig,
) -> dict[str, Any]:
    """Acquire one manifest-bound bundle with per-window crash consistency."""

    offer_contract = _validated_offer_contract(offer)
    estimate = float(offer["estimated_cost_usd"])
    if estimate > config.maximum_incremental_spend_usd + 1e-9:
        raise SelectiveVetoPilotError("selected 0034 offer exceeds USD 8")
    if (
        config.current_remaining_budget_usd - estimate
        < config.minimum_budget_reserve_usd - 1e-9
    ):
        raise SelectiveVetoPilotError("selected 0034 offer consumes USD 20 reserve")
    reused = _reuse_prior_acquisition_after_bounded_repair(
        offer_contract,
        offer,
        root=root,
        manifest=manifest,
    )
    if reused is not None:
        return reused
    request_rows = list(offer_contract["windows"])
    request_core = {
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "estimate_fingerprint": offer_contract["estimate_fingerprint"],
        "offer_contract_hash": offer_contract["offer_contract_hash"],
        "schema": offer_contract["schema"],
        "anchor_window_count": offer["anchor_window_count"],
        "window_contracts": request_rows,
    }
    request_id = request_id_for(request_core)
    receipt_root = root / "data/cache/databento/selective_veto_0034"
    receipt_path = receipt_root / f"{request_id}_receipt.json"
    intent_path = receipt_root / f"{request_id}_intent.json"
    authorization_path = receipt_root / f"{request_id}_authorization.json"
    metadata_cache_path = receipt_root / f"{request_id}_metadata_revalidation.jsonl"
    window_receipt_root = receipt_root / f"{request_id}_windows"
    raw_root = receipt_root / "raw_dbn"

    def validate_intent(intent: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        _validate_self_hash(intent, "intent_fingerprint", "acquisition intent")
        if (
            intent.get("request_id") != request_id
            or intent.get("estimate_fingerprint")
            != offer_contract["estimate_fingerprint"]
            or intent.get("manifest_hash") != str(manifest["manifest_hash"])
            or intent.get("offer_contract_hash")
            != offer_contract["offer_contract_hash"]
            or intent.get("data_schema") != offer_contract["schema"]
        ):
            raise SelectiveVetoPilotError("0034 acquisition intent identity drift")
        revalidation = dict(
            _mapping(intent.get("metadata_revalidation"), "intent revalidation")
        )
        if str(intent.get("metadata_revalidation_hash") or "") != str(
            revalidation.get("revalidation_hash") or ""
        ):
            raise SelectiveVetoPilotError(
                "0034 intent/revalidation fingerprint drift"
            )
        revalidated = _validate_metadata_revalidation(
            revalidation, offer_contract
        )
        expected_windows: list[dict[str, Any]] = []
        for window in revalidated:
            window_core = {
                "campaign_request_id": request_id,
                "window_index": int(window["window_index"]),
                "request_fingerprint": str(window["request_fingerprint"]),
                "metadata_estimate_hash": str(window["metadata_estimate_hash"]),
            }
            expected_windows.append(
                {**window, "window_request_id": request_id_for(window_core)}
            )
        actual_windows = [
            dict(_mapping(row, "acquisition intent window"))
            for row in intent.get("windows") or ()
        ]
        if actual_windows != expected_windows:
            raise SelectiveVetoPilotError(
                "0034 intent/offer/revalidation window contract drift"
            )
        live_cost = float(
            math.fsum(float(row["authorized_cost_usd"]) for row in actual_windows)
        )
        if not math.isclose(
            live_cost, float(intent["authorized_cost_usd"]), abs_tol=1e-9
        ):
            raise SelectiveVetoPilotError(
                "0034 acquisition cost reconciliation drift"
            )
        return dict(intent), actual_windows

    def validate_authorization(
        authorization: Mapping[str, Any], intent: Mapping[str, Any], windows: Sequence[Mapping[str, Any]]
    ) -> dict[str, Any]:
        _validate_self_hash(
            authorization,
            "authorization_fingerprint",
            "download authorization",
        )
        if (
            authorization.get("request_id") != request_id
            or authorization.get("manifest_hash") != str(manifest["manifest_hash"])
            or authorization.get("intent_fingerprint")
            != str(intent["intent_fingerprint"])
            or authorization.get("metadata_revalidation_hash")
            != str(intent["metadata_revalidation_hash"])
            or authorization.get("offer_contract_hash")
            != str(offer_contract["offer_contract_hash"])
            or authorization.get("data_schema") != str(offer_contract["schema"])
            or authorization.get("window_contract_hash") != stable_hash(list(windows))
            or int(authorization.get("window_count", -1)) != len(windows)
        ):
            raise SelectiveVetoPilotError("0034 download authorization drift")
        return dict(authorization)

    def validate_final_receipt(receipt: Mapping[str, Any]) -> dict[str, Any]:
        _validate_self_hash(
            receipt, "acquisition_receipt_fingerprint", "acquisition receipt"
        )
        if not intent_path.is_file() or not authorization_path.is_file():
            raise SelectiveVetoPilotError(
                "0034 final receipt lacks immutable intent/authorization chain"
            )
        intent, intent_windows = validate_intent(_read_json(intent_path))
        authorization = validate_authorization(
            _read_json(authorization_path), intent, intent_windows
        )
        if (
            receipt.get("request_id") != request_id
            or receipt.get("estimate_fingerprint")
            != offer_contract["estimate_fingerprint"]
            or receipt.get("manifest_hash") != str(manifest["manifest_hash"])
            or receipt.get("offer_contract_hash")
            != str(offer_contract["offer_contract_hash"])
            or receipt.get("intent_fingerprint")
            != str(intent["intent_fingerprint"])
            or receipt.get("authorization_fingerprint")
            != str(authorization["authorization_fingerprint"])
            or receipt.get("metadata_revalidation_hash")
            != str(intent["metadata_revalidation_hash"])
            or receipt.get("authorization_receipt_path")
            != str(authorization_path)
            or receipt.get("authorization_receipt_sha256")
            != _sha256(authorization_path)
        ):
            raise SelectiveVetoPilotError("0034 acquisition receipt drift")
        files = receipt.get("files")
        if not isinstance(files, list) or len(files) != len(intent_windows):
            raise SelectiveVetoPilotError("0034 acquisition file denominator drift")
        for row, window in zip(files, intent_windows, strict=True):
            item = _mapping(row, "acquisition receipt file")
            path = Path(str(item["raw_path"]))
            if (
                item.get("window_request_id") != window["window_request_id"]
                or item.get("request_fingerprint") != window["request_fingerprint"]
                or item.get("schema") != offer_contract["schema"]
                or list(item.get("symbols") or ())
                != list(window["request"]["symbols"])
                or list(item.get("anchor_ids") or ()) != list(window["anchor_ids"])
                or item.get("market") != window["market"]
                or item.get("contract") != window["contract"]
                or item.get("start") != window["request"]["start"]
                or item.get("end") != window["request"]["end"]
                or not path.is_file()
                or path.stat().st_size != int(item["raw_size_bytes"])
                or sha256_file(path) != str(item["raw_sha256"])
            ):
                raise SelectiveVetoPilotError("0034 immutable raw window checksum drift")
            window_receipt_path = Path(str(item["window_receipt_path"]))
            if (
                not window_receipt_path.is_file()
                or _sha256(window_receipt_path)
                != str(item["window_receipt_sha256"])
            ):
                raise SelectiveVetoPilotError(
                    "0034 immutable window receipt checksum drift"
                )
            window_receipt = _read_json(window_receipt_path)
            _validate_self_hash(
                window_receipt,
                "window_receipt_fingerprint",
                "window receipt",
            )
            if (
                window_receipt.get("campaign_request_id") != request_id
                or window_receipt.get("window_request_id")
                != window["window_request_id"]
                or int(window_receipt.get("window_index", -1))
                != int(window["window_index"])
                or window_receipt.get("request_fingerprint")
                != window["request_fingerprint"]
                or window_receipt.get("metadata_estimate_hash")
                != window["metadata_estimate_hash"]
                or window_receipt.get("raw_path") != str(path)
                or window_receipt.get("raw_sha256") != item["raw_sha256"]
            ):
                raise SelectiveVetoPilotError(
                    "0034 window receipt contract drift"
                )
        if (
            receipt.get("bundle_hash") != stable_hash(files)
            or int(receipt.get("window_count", -1)) != len(files)
            or int(receipt.get("completed_window_count", -1)) != len(files)
            or bool(receipt.get("q4_accessed"))
            or not math.isclose(
                float(receipt.get("actual_spend_usd", math.nan)),
                math.fsum(
                    float(window["authorized_cost_usd"])
                    for window in intent_windows
                ),
                abs_tol=1e-9,
            )
        ):
            raise SelectiveVetoPilotError(
                "0034 acquisition final bundle fingerprint drift"
            )
        return dict(receipt)

    if receipt_path.is_file():
        return validate_final_receipt(_read_json(receipt_path))

    lock_path = root / "reports/data_access/selective_veto_0034_acquisition.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if receipt_path.is_file():
            return validate_final_receipt(_read_json(receipt_path))
        budget = DatabentoBudgetConfig(
            ledger_path=str(root / "reports/data_budget/databento_spend_ledger.jsonl"),
            summary_path=str(root / "reports/data_budget/databento_budget_summary.md"),
        )
        budget_path = Path(budget.ledger_path)
        access_path = root / "reports/data_access/data_access_ledger.jsonl"

        def ledger_hash(path: Path, absent: str) -> str:
            return _sha256(path) if path.is_file() else stable_hash(absent)

        if intent_path.is_file():
            intent, windows = validate_intent(_read_json(intent_path))
        else:
            revalidation = _revalidate_offer_metadata(
                client.metadata,
                offer,
                cache_path=metadata_cache_path,
            )
            live_cost = float(revalidation["authorized_cost_usd"])
            _estimated, current_actual = cumulative_spend(budget_path)
            live_remaining = float(budget.hard_cap_usd - current_actual)
            if live_cost > config.maximum_incremental_spend_usd + 1e-9:
                raise SelectiveVetoPilotError("live 0034 cost exceeds USD 8")
            if (
                config.current_remaining_budget_usd - live_cost
                < config.minimum_budget_reserve_usd - 1e-9
                or live_remaining - live_cost
                < config.minimum_budget_reserve_usd - 1e-9
            ):
                raise SelectiveVetoPilotError("live 0034 cost consumes live USD 20 reserve")
            windows: list[dict[str, Any]] = []
            for row in revalidation["windows"]:
                window = dict(_mapping(row, "metadata-revalidated window"))
                window_core = {
                    "campaign_request_id": request_id,
                    "window_index": int(window["window_index"]),
                    "request_fingerprint": str(window["request_fingerprint"]),
                    "metadata_estimate_hash": str(window["metadata_estimate_hash"]),
                }
                windows.append(
                    {
                        **window,
                        "window_request_id": request_id_for(window_core),
                    }
                )
            intent_core = {
                "schema": "hydra_selective_veto_purchase_intent_v2",
                "data_schema": str(offer_contract["schema"]),
                "request_id": request_id,
                "campaign_id": CAMPAIGN_ID,
                "manifest_hash": str(manifest["manifest_hash"]),
                "estimate_fingerprint": str(
                    offer_contract["estimate_fingerprint"]
                ),
                "offer_contract_hash": str(
                    offer_contract["offer_contract_hash"]
                ),
                "authorized_cost_usd": live_cost,
                "metadata_revalidation_hash": str(revalidation["revalidation_hash"]),
                "metadata_revalidation": revalidation,
                "windows": windows,
                "budget_ledger_before_sha256": ledger_hash(
                    budget_path, "ABSENT_BUDGET_LEDGER"
                ),
                "data_access_ledger_before_sha256": ledger_hash(
                    access_path, "ABSENT_DATA_ACCESS_LEDGER"
                ),
                "temporal_roles_frozen": True,
                "unmanifested_data_purchase_count": 0,
            }
            _write_json_once(
                intent_path,
                {**intent_core, "intent_fingerprint": stable_hash(intent_core)},
            )
            intent, windows = validate_intent(_read_json(intent_path))

        live_cost = float(
            math.fsum(float(row["authorized_cost_usd"]) for row in windows)
        )
        if live_cost > config.maximum_incremental_spend_usd + 1e-9:
            raise SelectiveVetoPilotError("authorized 0034 cost exceeds USD 8")
        _estimated, current_actual = cumulative_spend(budget_path)
        downloaded_cost = float(
            math.fsum(
                float(row.get("actual_cost_usd") or 0.0)
                for row in read_ledger(budget_path)
                if str(row.get("request_id"))
                in {str(window["window_request_id"]) for window in windows}
                and row.get("download_status") == "DOWNLOADED"
            )
        )
        outstanding_cost = live_cost - downloaded_cost
        if (
            budget.hard_cap_usd - current_actual - outstanding_cost
            < config.minimum_budget_reserve_usd - 1e-9
        ):
            raise SelectiveVetoPilotError(
                "0034 acquisition no longer preserves live USD 20 reserve"
            )

        access_rows = read_ledger(access_path)
        matching_access = [
            row
            for row in access_rows
            if request_id in set(str(value) for value in row.get("candidate_ids") or ())
        ]
        if not matching_access:
            enforce_data_access(
                period=(
                    f"DISJOINT_EVENT_WINDOWS:{min(row['request']['start'] for row in windows)}:"
                    f"{max(row['request']['end'] for row in windows)}"
                ),
                role=DataRole.CONTAMINATED_DEVELOPMENT,
                requesting_module="hydra.production.selective_veto_pilot",
                candidate_ids=[CAMPAIGN_ID, request_id],
                reason="frozen anchor-conditioned 0034 selective-veto development sample",
                freeze_manifest_hash=str(manifest["manifest_hash"]),
                ledger_path=str(access_path),
            )
        elif len(matching_access) != 1:
            raise SelectiveVetoPilotError("0034 data-access authorization duplicated")

        # Reserve every immutable window separately before the first download.
        # This makes partial completion and remaining liability explicit.
        for window in windows:
            window_id = str(window["window_request_id"])
            ledger_rows = [
                row for row in read_ledger(budget_path) if row.get("request_id") == window_id
            ]
            reserved_rows = [row for row in ledger_rows if row.get("download_status") == "ESTIMATED_ONLY"]
            downloaded_rows = [row for row in ledger_rows if row.get("download_status") == "DOWNLOADED"]
            if len(reserved_rows) > 1 or len(downloaded_rows) > 1:
                raise SelectiveVetoPilotError("0034 window budget record duplicated")
            if not reserved_rows and not downloaded_rows:
                cost = float(window["authorized_cost_usd"])
                projected, current_actual = enforce_budget(budget, cost)
                append_spend_record(
                    budget,
                    DatabentoSpendRecord(
                        request_id=window_id,
                        timestamp_utc=utc_now(),
                        dataset=DATASET,
                        schema=str(offer["schema"]),
                        symbols=[str(window["contract"])],
                        stype_in="raw_symbol",
                        start=str(window["request"]["start"]),
                        end=str(window["request"]["end"]),
                        estimated_cost_usd=cost,
                        actual_cost_usd=None,
                        cumulative_estimated_spend_usd=projected,
                        cumulative_actual_spend_usd=current_actual,
                        cache_hit=False,
                        research_purpose="0034 frozen anchor-conditioned selective-veto window",
                        candidate_tier="SELECTIVE_VETO_LONG_SAMPLE_0034",
                        approval_mode=AUTO_UNDER_HARD_CAP,
                        resulting_file=None,
                        checksum=str(window["window_metadata_hash"]),
                        download_status="ESTIMATED_ONLY",
                    ),
                )

        if authorization_path.is_file():
            authorization = validate_authorization(
                _read_json(authorization_path), intent, windows
            )
        else:
            authorization_core = {
                "schema": "hydra_selective_veto_download_authorization_v2",
                "data_schema": str(offer_contract["schema"]),
                "request_id": request_id,
                "manifest_hash": str(manifest["manifest_hash"]),
                "offer_contract_hash": str(
                    offer_contract["offer_contract_hash"]
                ),
                "intent_fingerprint": str(intent["intent_fingerprint"]),
                "metadata_revalidation_hash": str(intent["metadata_revalidation_hash"]),
                "window_count": len(windows),
                "window_contract_hash": stable_hash(windows),
                "data_access_recorded_before_download": True,
                "budget_reserved_before_download": True,
                "per_window_budget_reservation": True,
                "data_access_ledger_after_authorization_sha256": ledger_hash(
                    access_path, "ABSENT_DATA_ACCESS_LEDGER"
                ),
                "budget_ledger_after_reservation_sha256": ledger_hash(
                    budget_path, "ABSENT_BUDGET_LEDGER"
                ),
            }
            _write_json_once(
                authorization_path,
                {
                    **authorization_core,
                    "authorization_fingerprint": stable_hash(authorization_core),
                },
            )
            authorization = validate_authorization(
                _read_json(authorization_path), intent, windows
            )

        raw_root.mkdir(parents=True, exist_ok=True)
        window_receipt_root.mkdir(parents=True, exist_ok=True)
        gate = _DownloadCallGate(
            enforce_rate_limit=_official_databento_object(client.timeseries)
        )
        files: list[dict[str, Any]] = []
        for window in windows:
            index = int(window["window_index"])
            window_id = str(window["window_request_id"])
            raw_path = raw_root / f"{request_id}_{index:04d}_{offer['schema']}.dbn.zst"
            window_receipt_path = window_receipt_root / f"{index:04d}_{window_id}.json"
            download_metrics = {"attempts": 0, "retries": 0}
            if window_receipt_path.is_file():
                window_receipt = _read_json(window_receipt_path)
                _validate_self_hash(
                    window_receipt,
                    "window_receipt_fingerprint",
                    "window receipt",
                )
                if (
                    window_receipt.get("campaign_request_id") != request_id
                    or window_receipt.get("window_request_id") != window_id
                    or int(window_receipt.get("window_index", -1)) != index
                    or window_receipt.get("request_fingerprint")
                    != window["request_fingerprint"]
                    or window_receipt.get("metadata_estimate_hash")
                    != window["metadata_estimate_hash"]
                    or window_receipt.get("raw_path") != str(raw_path)
                ):
                    raise SelectiveVetoPilotError("0034 window receipt identity drift")
            else:
                if not raw_path.is_file():
                    download_metrics = _download_window_bounded(
                        client,
                        _mapping(window["request"], "window request"),
                        raw_path,
                        gate=gate,
                    )
                if raw_path.stat().st_size <= 0:
                    raise SelectiveVetoPilotError("0034 immutable raw window is empty")
                window_core = {
                    "schema": "hydra_selective_veto_window_receipt_v1",
                    "campaign_request_id": request_id,
                    "window_request_id": window_id,
                    "window_index": index,
                    "request_fingerprint": str(window["request_fingerprint"]),
                    "metadata_estimate_hash": str(window["metadata_estimate_hash"]),
                    "authorized_cost_usd": float(window["authorized_cost_usd"]),
                    "raw_path": str(raw_path),
                    "raw_sha256": sha256_file(raw_path),
                    "raw_size_bytes": raw_path.stat().st_size,
                    "download_attempts_this_process": download_metrics["attempts"],
                    "download_retries_this_process": download_metrics["retries"],
                    "append_only": True,
                }
                _write_json_once(
                    window_receipt_path,
                    {
                        **window_core,
                        "window_receipt_fingerprint": stable_hash(window_core),
                    },
                )
                window_receipt = _read_json(window_receipt_path)
            if (
                not raw_path.is_file()
                or raw_path.stat().st_size != int(window_receipt["raw_size_bytes"])
                or sha256_file(raw_path) != str(window_receipt["raw_sha256"])
            ):
                raise SelectiveVetoPilotError("0034 immutable window checksum drift")

            ledger_rows = [
                row for row in read_ledger(budget_path) if row.get("request_id") == window_id
            ]
            downloaded_rows = [row for row in ledger_rows if row.get("download_status") == "DOWNLOADED"]
            if len(downloaded_rows) > 1:
                raise SelectiveVetoPilotError("0034 window was charged more than once")
            if not downloaded_rows:
                estimated_total, cumulative_actual = cumulative_spend(budget_path)
                cost = float(window["authorized_cost_usd"])
                append_spend_record(
                    budget,
                    DatabentoSpendRecord(
                        request_id=window_id,
                        timestamp_utc=utc_now(),
                        dataset=DATASET,
                        schema=str(offer["schema"]),
                        symbols=[str(window["contract"])],
                        stype_in="raw_symbol",
                        start=str(window["request"]["start"]),
                        end=str(window["request"]["end"]),
                        estimated_cost_usd=0.0,
                        actual_cost_usd=cost,
                        cumulative_estimated_spend_usd=estimated_total,
                        cumulative_actual_spend_usd=cumulative_actual + cost,
                        cache_hit=False,
                        research_purpose="0034 frozen anchor-conditioned selective-veto window",
                        candidate_tier="SELECTIVE_VETO_LONG_SAMPLE_0034",
                        approval_mode=AUTO_UNDER_HARD_CAP,
                        resulting_file=str(raw_path),
                        checksum=str(window_receipt["raw_sha256"]),
                        download_status="DOWNLOADED",
                    ),
                )
            files.append(
                {
                    "raw_path": str(raw_path),
                    "raw_sha256": str(window_receipt["raw_sha256"]),
                    "raw_size_bytes": int(window_receipt["raw_size_bytes"]),
                    "window_request_id": window_id,
                    "window_receipt_path": str(window_receipt_path),
                    "window_receipt_sha256": _sha256(window_receipt_path),
                    "authorized_cost_usd": float(window["authorized_cost_usd"]),
                    "request_fingerprint": str(window["request_fingerprint"]),
                    "schema": str(offer_contract["schema"]),
                    "symbols": list(window["request"]["symbols"]),
                    "anchor_ids": list(window["anchor_ids"]),
                    "market": window["market"],
                    "contract": window["contract"],
                    "start": window["request"]["start"],
                    "end": window["request"]["end"],
                }
            )

        bundle_hash = stable_hash(files)
        core = {
            "schema": "hydra_selective_veto_acquisition_receipt_v2",
            "campaign_id": CAMPAIGN_ID,
            "request_id": request_id,
            "manifest_hash": str(manifest["manifest_hash"]),
            "estimate_fingerprint": offer_contract["estimate_fingerprint"],
            "offer_contract_hash": offer_contract["offer_contract_hash"],
            "intent_fingerprint": str(intent["intent_fingerprint"]),
            "authorization_fingerprint": str(
                authorization["authorization_fingerprint"]
            ),
            "metadata_revalidation_hash": str(intent["metadata_revalidation_hash"]),
            "actual_spend_usd": live_cost,
            "files": files,
            "window_count": len(windows),
            "completed_window_count": len(files),
            "bundle_hash": bundle_hash,
            "download_endpoint_call_count_this_process": gate.call_count,
            "download_retry_count_this_process": gate.retry_count,
            "download_maximum_calls_per_second": gate.policy.maximum_calls_per_second,
            "download_maximum_retries": gate.policy.maximum_retries,
            "per_window_incremental_cost_accounting": True,
            "independent_anchors_acquired": int(
                offer.get("effective_anchor_count", len(offer.get("anchor_ids") or ()))
            ),
            "raw_data_immutable": True,
            "temporal_roles_frozen_before_download": True,
            "data_access_ledger_appended": True,
            "budget_ledger_appended": True,
            "q4_accessed": False,
            "broker_connections": 0,
            "orders": 0,
            "manifest_bound_data_purchase_count": 1,
            "unmanifested_data_purchase_count": 0,
            "budget_ledger_before_sha256": str(intent["budget_ledger_before_sha256"]),
            "budget_ledger_after_sha256": ledger_hash(
                budget_path, "ABSENT_BUDGET_LEDGER"
            ),
            "data_access_ledger_before_sha256": str(intent["data_access_ledger_before_sha256"]),
            "data_access_ledger_after_sha256": ledger_hash(
                access_path, "ABSENT_DATA_ACCESS_LEDGER"
            ),
            "authorization_receipt_path": str(authorization_path),
            "authorization_receipt_sha256": _sha256(authorization_path),
        }
        _estimated_final, actual_final = cumulative_spend(budget_path)
        core["live_prior_budget_usd"] = float(
            budget.hard_cap_usd - (actual_final - live_cost)
        )
        core["live_remaining_budget_usd"] = float(budget.hard_cap_usd - actual_final)
        receipt = {**core, "acquisition_receipt_fingerprint": stable_hash(core)}
        _write_json_once(receipt_path, receipt)
        return validate_final_receipt(_read_json(receipt_path))


def _dataframe_from_dbn(path: Path) -> Any:
    import databento as db

    return db.DBNStore.from_file(path).to_df()


def _initialize_feature_worker() -> None:
    """Keep each economic worker single-threaded on the three-core VPS."""

    for name in (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        os.environ[name] = "1"


def _timestamp_ns(index: Any) -> np.ndarray:
    if hasattr(index, "asi8"):
        return np.asarray(index.asi8, dtype=np.int64)
    array = getattr(index, "array", None)
    if array is not None and hasattr(array, "asi8"):
        return np.asarray(array.asi8, dtype=np.int64)
    return np.asarray(index, dtype="datetime64[ns]").astype(np.int64)


def _number_column(frame: Any, names: Sequence[str], default: float = 0.0) -> np.ndarray:
    for name in names:
        if name in frame.columns:
            return np.asarray(frame[name], dtype=float)
    return np.full(len(frame), default, dtype=float)


def _event_codes(values: Any) -> np.ndarray:
    return np.asarray(
        [
            (value.decode("ascii") if isinstance(value, bytes) else str(value)).upper()
            for value in values
        ],
        dtype=str,
    )


def _feature_for_anchor(
    frame: Any,
    anchor: StructuralAnchor,
    *,
    schema: str = "tbbo",
) -> tuple[np.ndarray, str, CausalEntryQuote] | None:
    if frame is None or len(frame) == 0:
        return None
    if schema not in SCHEMAS:
        raise SelectiveVetoPilotError(f"unsupported 0034 acquired schema: {schema}")
    event_ns = (
        _timestamp_ns(frame["ts_event"])
        if "ts_event" in frame.columns
        else _timestamp_ns(frame.index)
    )
    available_ns = (
        _timestamp_ns(frame["ts_recv"])
        if "ts_recv" in frame.columns
        else _timestamp_ns(frame.index)
    )
    price = _number_column(frame, ("price",))
    size = _number_column(frame, ("size",))
    sides = _event_codes(frame["side"]) if "side" in frame.columns else np.full(len(frame), "N")
    sign = np.where(sides == "A", 1.0, np.where(sides == "B", -1.0, 0.0))
    if "action" in frame.columns:
        actions = _event_codes(frame["action"])
        is_trade_action = np.isin(actions, ("T", "TRADE"))
    elif schema == "mbp-1":
        # MBP-1 carries adds/cancels/modifies as price/size records.  Without
        # the action field they cannot safely be distinguished from trades.
        raise SelectiveVetoPilotError("MBP-1 feature frame lacks action field")
    else:
        is_trade_action = np.ones(len(frame), dtype=bool)
    trade = (
        np.isfinite(price)
        & (price > 0.0)
        & (size > 0.0)
        & is_trade_action
    )
    causal_at_decision = (
        (available_ns <= anchor.decision_time_ns)
        & (event_ns <= anchor.decision_time_ns)
    )
    two = (
        (available_ns >= anchor.decision_time_ns - 2_000_000_000)
        & causal_at_decision
        & trade
    )
    thirty = (
        (available_ns >= anchor.decision_time_ns - 30_000_000_000)
        & causal_at_decision
        & trade
    )
    flow2 = float(np.sum(sign[two] * size[two]))
    flow30 = float(np.sum(sign[thirty] * size[thirty]))
    bid = _number_column(frame, ("bid_px_00", "bid_price"), math.nan)
    ask = _number_column(frame, ("ask_px_00", "ask_price"), math.nan)
    bid_size = _number_column(frame, ("bid_sz_00", "bid_size"), 0.0)
    ask_size = _number_column(frame, ("ask_sz_00", "ask_size"), 0.0)
    valid_quote = (
        np.isfinite(bid)
        & np.isfinite(ask)
        & (ask >= bid)
        & (bid_size > 0.0)
        & (ask_size > 0.0)
    )
    causal_quotes = np.flatnonzero(
        causal_at_decision & valid_quote
    )
    if not len(causal_quotes):
        return None
    last = int(causal_quotes[-1])
    executable_quotes = np.flatnonzero(
        (available_ns > anchor.decision_time_ns)
        & (event_ns > anchor.decision_time_ns)
        & (
            available_ns
            < anchor.decision_time_ns + MAX_POST_DECISION_ENTRY_DELAY_NS
        )
        & valid_quote
    )
    if not len(executable_quotes):
        return None
    executable = int(executable_quotes[0])
    normal_mark_time = int(anchor.normal_marks[0]["availability_time_ns"])
    stressed_mark_time = int(anchor.stressed_marks[0]["availability_time_ns"])
    if normal_mark_time != stressed_mark_time:
        raise SelectiveVetoPilotError(
            "0034 normal/stressed first-mark availability drift"
        )
    post_fill_quotes = np.flatnonzero(
        (available_ns >= available_ns[executable])
        & (available_ns <= normal_mark_time)
        & (event_ns > anchor.decision_time_ns)
        & valid_quote
    )
    if not len(post_fill_quotes):
        return None
    tick = 0.25 if anchor.market == "NQ" else 1.0
    total_depth = float(bid_size[last] + ask_size[last])
    imbalance = float((bid_size[last] - ask_size[last]) / total_depth) if total_depth > 0.0 else 0.0
    mid = 0.5 * float(bid[last] + ask[last])
    micro = (
        float((ask[last] * bid_size[last] + bid[last] * ask_size[last]) / total_depth)
        if total_depth > 0.0
        else mid
    )
    values = np.array(
        [flow2, flow30, imbalance, (micro - mid) / tick, (float(ask[last]) - float(bid[last])) / tick],
        dtype=float,
    )
    if not np.all(np.isfinite(values)):
        return None
    liquidation = bid if anchor.direction > 0 else ask
    post_fill_liquidation = liquidation[post_fill_quotes]
    if anchor.direction > 0:
        worst_liquidation = float(np.min(post_fill_liquidation))
        best_liquidation = float(np.max(post_fill_liquidation))
    else:
        worst_liquidation = float(np.max(post_fill_liquidation))
        best_liquidation = float(np.min(post_fill_liquidation))
    quote = CausalEntryQuote(
        schema=schema,
        event_time_ns=int(event_ns[executable]),
        available_at_ns=int(available_ns[executable]),
        bid_price=float(bid[executable]),
        ask_price=float(ask[executable]),
        bid_size=float(bid_size[executable]),
        ask_size=float(ask_size[executable]),
        first_mark_available_at_ns=normal_mark_time,
        post_fill_worst_liquidation_price=worst_liquidation,
        post_fill_best_liquidation_price=best_liquidation,
        post_fill_last_liquidation_price=float(liquidation[post_fill_quotes[-1]]),
    )
    feature_hash = stable_hash(
        {
            "anchor_event_id": anchor.anchor_event_id,
            "available_at_ns": int(available_ns[last]),
            "decision_time_ns": anchor.decision_time_ns,
            "feature_names": FEATURE_NAMES,
            "values": values.tolist(),
        }
    )
    return values, feature_hash, quote


def _extract_features_from_frame_task(
    task: tuple[Any, tuple[StructuralAnchor, ...]]
    | tuple[Any, tuple[StructuralAnchor, ...], str],
) -> list[tuple[str, list[float], str, dict[str, Any]]]:
    """Pure deterministic extraction helper shared by sequential and workers."""

    if len(task) == 2:
        frame, anchors = task
        schema = "tbbo"
    else:
        frame, anchors, schema = task
    output: list[tuple[str, list[float], str, dict[str, Any]]] = []
    for anchor in anchors:
        value = _feature_for_anchor(frame, anchor, schema=schema)
        if value is None:
            continue
        features, feature_hash, quote = value
        output.append(
            (
                anchor.anchor_event_id,
                features.tolist(),
                feature_hash,
                quote.to_dict(),
            )
        )
    return output


def _extract_features_from_dbn_task(
    task: tuple[str, tuple[StructuralAnchor, ...], str],
) -> list[tuple[str, list[float], str, dict[str, Any]]]:
    """Load one immutable DBN file and extract its anchors inside a worker."""

    raw_path, anchors, schema = task
    frame = _dataframe_from_dbn(Path(raw_path))
    return _extract_features_from_frame_task((frame, anchors, schema))


def _load_acquired_features(
    receipt: Mapping[str, Any],
    anchors: Sequence[StructuralAnchor],
    *,
    schema: str,
) -> tuple[dict[str, tuple[np.ndarray, str, CausalEntryQuote]], bool]:
    by_id = {row.anchor_event_id: row for row in anchors}
    tasks: list[tuple[str, tuple[StructuralAnchor, ...], str]] = []
    for item in receipt.get("files") or ():
        task_anchors: list[StructuralAnchor] = []
        for anchor_id in item.get("anchor_ids") or ():
            anchor = by_id.get(str(anchor_id))
            if anchor is None:
                raise SelectiveVetoPilotError(
                    "acquired window references unknown anchor"
                )
            task_anchors.append(anchor)
        tasks.append((str(item["raw_path"]), tuple(task_anchors), schema))
    if not tasks:
        return {}, False

    _initialize_feature_worker()
    features: dict[str, tuple[np.ndarray, str, CausalEntryQuote]] = {}
    with ProcessPoolExecutor(
        max_workers=2,
        mp_context=get_context("spawn"),
        initializer=_initialize_feature_worker,
    ) as executor:
        extracted_batches = list(
            executor.map(_extract_features_from_dbn_task, tasks, chunksize=1)
        )
    # ``executor.map`` preserves task order; the authoritative parent remains
    # the sole aggregator and therefore preserves the old overwrite semantics
    # should an anchor be repeated in two immutable request files.
    for extracted in extracted_batches:
        for anchor_id, values, feature_hash, quote in extracted:
            if anchor_id in features:
                raise SelectiveVetoPilotError(
                    f"duplicate acquired feature row for anchor {anchor_id}"
                )
            features[anchor_id] = (
                np.asarray(values, dtype=float),
                feature_hash,
                CausalEntryQuote(
                    schema=str(quote["schema"]),
                    event_time_ns=int(quote["event_time_ns"]),
                    available_at_ns=int(quote["available_at_ns"]),
                    bid_price=float(quote["bid_price"]),
                    ask_price=float(quote["ask_price"]),
                    bid_size=float(quote["bid_size"]),
                    ask_size=float(quote["ask_size"]),
                    first_mark_available_at_ns=(
                        None
                        if quote.get("first_mark_available_at_ns") is None
                        else int(quote["first_mark_available_at_ns"])
                    ),
                    post_fill_worst_liquidation_price=(
                        None
                        if quote.get("post_fill_worst_liquidation_price") is None
                        else float(quote["post_fill_worst_liquidation_price"])
                    ),
                    post_fill_best_liquidation_price=(
                        None
                        if quote.get("post_fill_best_liquidation_price") is None
                        else float(quote["post_fill_best_liquidation_price"])
                    ),
                    post_fill_last_liquidation_price=(
                        None
                        if quote.get("post_fill_last_liquidation_price") is None
                        else float(quote["post_fill_last_liquidation_price"])
                    ),
                ),
            )
    return features, True


def _ridge_logistic_fit(x: np.ndarray, y: np.ndarray, weight: np.ndarray) -> dict[str, Any]:
    mean = np.mean(x, axis=0)
    scale = np.std(x, axis=0)
    scale[scale < 1e-9] = 1.0
    z = (x - mean) / scale
    design = np.column_stack([np.ones(len(z)), z])
    beta = np.zeros(design.shape[1], dtype=float)
    ridge = np.diag([0.0] + [1.0] * z.shape[1])
    for _ in range(60):
        linear = np.clip(design @ beta, -30.0, 30.0)
        probability = 1.0 / (1.0 + np.exp(-linear))
        curvature = np.maximum(probability * (1.0 - probability) * weight, 1e-8)
        gradient = design.T @ ((probability - y) * weight) + ridge @ beta
        hessian = design.T @ (curvature[:, None] * design) + ridge
        step = np.linalg.solve(hessian + np.eye(len(beta)) * 1e-9, gradient)
        beta -= step
        if float(np.max(np.abs(step))) < 1e-8:
            break
    return {"mean": mean, "scale": scale, "beta": beta}


def _ridge_logistic_predict(model: Mapping[str, Any], x: np.ndarray) -> np.ndarray:
    z = (x - model["mean"]) / model["scale"]
    design = np.column_stack([np.ones(len(z)), z])
    return 1.0 / (1.0 + np.exp(-np.clip(design @ model["beta"], -30.0, 30.0)))


def _ridge_linear_fit(x: np.ndarray, y: np.ndarray) -> dict[str, Any]:
    mean = np.mean(x, axis=0)
    scale = np.std(x, axis=0)
    scale[scale < 1e-9] = 1.0
    design = np.column_stack([np.ones(len(x)), (x - mean) / scale])
    penalty = np.diag([0.0] + [2.0] * x.shape[1])
    beta = np.linalg.solve(design.T @ design + penalty, design.T @ y)
    residual = y - design @ beta
    return {
        "mean": mean,
        "scale": scale,
        "beta": beta,
        "residual_scale": float(np.sqrt(np.mean(np.square(residual)))),
    }


def _ridge_linear_predict(model: Mapping[str, Any], x: np.ndarray) -> np.ndarray:
    design = np.column_stack(
        [np.ones(len(x)), (x - model["mean"]) / model["scale"]]
    )
    return np.asarray(design @ model["beta"], dtype=float)


def _point_value(anchor: StructuralAnchor) -> float:
    try:
        return {"MNQ": 2.0, "MYM": 0.5}[anchor.execution_market]
    except KeyError as exc:
        raise SelectiveVetoPilotError(
            f"unsupported 0034 execution market: {anchor.execution_market}"
        ) from exc


def _exact_action_quantity(anchor: StructuralAnchor, tier: float) -> int:
    """Resolve a tier without ever exceeding its frozen nominal multiplier."""

    if tier not in {1.0, 1.5}:
        raise SelectiveVetoPilotError("0034 executable risk tier drift")
    quantity = int(math.floor(anchor.quantity * tier + 1e-12))
    if quantity < 1 or quantity > anchor.quantity * tier + 1e-12:
        raise SelectiveVetoPilotError("0034 integer risk tier exceeds nominal ceiling")
    return quantity


def _entry_fill_from_quote(
    anchor: StructuralAnchor,
    quote: CausalEntryQuote,
    *,
    quantity: int,
    scenario: str,
) -> float:
    """Return a deterministic aggressive fill from the acquired causal BBO.

    The structural sleeves trade micros while the acquired quote is for the
    corresponding mini.  Quantity is therefore converted to mini-equivalent
    depth.  Any amount beyond displayed contra-side depth pays one additional
    tick per further displayed-depth unit.  The frozen normal-to-stressed
    adverse fill increment is retained on top of that observable BBO fill.
    """

    if quote.available_at_ns <= anchor.decision_time_ns:
        raise SelectiveVetoPilotError("0034 entry quote is not post-decision causal")
    if quote.event_time_ns <= anchor.decision_time_ns:
        raise SelectiveVetoPilotError("0034 entry event is not post-decision causal")
    if (
        quote.available_at_ns
        >= anchor.decision_time_ns + MAX_POST_DECISION_ENTRY_DELAY_NS
    ):
        raise SelectiveVetoPilotError(
            "0034 entry quote occurs after frozen event-window bound"
        )
    if (
        anchor.outcome_time_ns is not None
        and quote.available_at_ns >= anchor.outcome_time_ns
    ):
        raise SelectiveVetoPilotError("0034 entry quote occurs after anchor outcome")
    contra_depth = quote.ask_size if anchor.direction > 0 else quote.bid_size
    if contra_depth <= 0.0:
        raise SelectiveVetoPilotError("0034 executable contra-side depth is empty")
    requested_mini_equivalent = quantity / 10.0
    excess = max(0.0, requested_mini_equivalent - contra_depth)
    extra_levels = int(math.ceil(excess / contra_depth - 1e-12)) if excess else 0
    tick = 0.25 if anchor.market == "NQ" else 1.0
    stressed_increment = 0.0
    if scenario == "STRESSED_1_5X":
        stressed_increment = max(
            0.0,
            anchor.direction
            * (anchor.stressed_fill_price - anchor.normal_fill_price),
        )
    elif scenario != "NORMAL":
        raise SelectiveVetoPilotError("unsupported 0034 cost scenario")
    touch = quote.ask_price if anchor.direction > 0 else quote.bid_price
    return float(
        touch + anchor.direction * (extra_levels * tick + stressed_increment)
    )


def _causal_action_trajectory(
    anchor: StructuralAnchor,
    scenario: str,
    tier: float,
    execution_quote: CausalEntryQuote | None = None,
) -> CausalTradeTrajectory:
    """Reconstruct an integer-sized causal trajectory from immutable 0028 marks."""

    if anchor.outcome_time_ns is None or anchor.raw_exit_price is None:
        raise SelectiveVetoPilotError("future-censored anchor cannot become an account trade")
    quantity = _exact_action_quantity(anchor, tier)
    ratio = quantity / anchor.quantity
    if scenario == "NORMAL":
        legacy_fill = anchor.normal_fill_price
        legacy_net = float(anchor.normal_net_pnl_usd or 0.0)
        legacy_initial = anchor.normal_initial_unrealized_pnl_usd
        source_marks = anchor.normal_marks
    elif scenario == "STRESSED_1_5X":
        legacy_fill = anchor.stressed_fill_price
        legacy_net = float(anchor.stressed_net_pnl_usd or 0.0)
        legacy_initial = anchor.stressed_initial_unrealized_pnl_usd
        source_marks = anchor.stressed_marks
    else:
        raise SelectiveVetoPilotError("unsupported 0034 cost scenario")
    fill = (
        legacy_fill
        if execution_quote is None
        else _entry_fill_from_quote(
            anchor,
            execution_quote,
            quantity=quantity,
            scenario=scenario,
        )
    )
    legacy_gross = (
        anchor.direction
        * (float(anchor.raw_exit_price) - float(legacy_fill))
        * _point_value(anchor)
        * anchor.quantity
    )
    legacy_non_fill_cost = legacy_gross - legacy_net
    if legacy_non_fill_cost < -1e-6:
        raise SelectiveVetoPilotError("0034 immutable ledger implies negative all-in cost")
    non_fill_cost = max(0.0, legacy_non_fill_cost) * ratio
    gross = (
        anchor.direction
        * (float(anchor.raw_exit_price) - float(fill))
        * _point_value(anchor)
        * quantity
    )
    net = gross - non_fill_cost
    entry_adjustment = (
        anchor.direction
        * (legacy_fill - fill)
        * _point_value(anchor)
        * quantity
    )
    adjusted_marks = [
        CausalTradeMark(
            availability_time_ns=int(row["availability_time_ns"]),
            worst_unrealized_pnl=(
                float(row["worst_unrealized_pnl"]) * ratio + entry_adjustment
            ),
            best_unrealized_pnl=(
                float(row["best_unrealized_pnl"]) * ratio + entry_adjustment
            ),
            current_unrealized_pnl=(
                None
                if row.get("current_unrealized_pnl") is None
                else float(row["current_unrealized_pnl"]) * ratio
                + entry_adjustment
            ),
        )
        for row in source_marks
    ]
    initial_unrealized = legacy_initial * ratio + entry_adjustment
    microstructure_mark_fields = (
        None
        if execution_quote is None
        else (
            execution_quote.first_mark_available_at_ns,
            execution_quote.post_fill_worst_liquidation_price,
            execution_quote.post_fill_best_liquidation_price,
            execution_quote.post_fill_last_liquidation_price,
        )
    )
    if microstructure_mark_fields is not None and any(
        value is not None for value in microstructure_mark_fields
    ):
        if any(value is None for value in microstructure_mark_fields):
            raise SelectiveVetoPilotError(
                "0034 post-fill BBO mark is only partially specified"
            )
        first_mark_time = int(microstructure_mark_fields[0])
        if (
            first_mark_time != adjusted_marks[0].availability_time_ns
            or execution_quote is None
            or not execution_quote.available_at_ns < first_mark_time
            or first_mark_time > anchor.outcome_time_ns
        ):
            raise SelectiveVetoPilotError(
                "0034 post-fill BBO/structural mark chronology drift"
            )
        point_value = _point_value(anchor)

        def liquidation_pnl(price: float) -> float:
            return float(
                anchor.direction * (float(price) - fill) * point_value * quantity
                - non_fill_cost
            )

        liquidation_at_fill = (
            execution_quote.bid_price
            if anchor.direction > 0
            else execution_quote.ask_price
        )
        initial_unrealized = liquidation_pnl(liquidation_at_fill)
        observed_worst = liquidation_pnl(float(microstructure_mark_fields[1]))
        observed_best = liquidation_pnl(float(microstructure_mark_fields[2]))
        observed_current = liquidation_pnl(float(microstructure_mark_fields[3]))
        # The acquired TBBO stream provides the exact post-fill favorable and
        # current BBO path.  For MLL, retain the worse of that path and the
        # legacy full-minute low/high, which is deliberately conservative if
        # the few pre-fill milliseconds contained a more adverse print.
        adjusted_marks[0] = CausalTradeMark(
            availability_time_ns=first_mark_time,
            worst_unrealized_pnl=min(
                observed_worst, adjusted_marks[0].worst_unrealized_pnl
            ),
            best_unrealized_pnl=observed_best,
            current_unrealized_pnl=observed_current,
        )
    marks = tuple(adjusted_marks)
    path_worst = min(
        initial_unrealized,
        *(float(row.worst_unrealized_pnl) for row in marks),
    )
    path_best = max(
        initial_unrealized,
        *(float(row.best_unrealized_pnl) for row in marks),
    )
    return CausalTradeTrajectory(
        component_id=anchor.source_candidate_id,
        market=anchor.market,
        side=anchor.direction,
        event=TradePathEvent(
            event_id=(
                f"{anchor.anchor_event_id}:{scenario}:{quantity}:"
                f"{execution_quote.available_at_ns if execution_quote else 'STRUCTURAL'}"
            ),
            # The account is flat until the causal next-tradable-event fill.
            decision_ns=(
                anchor.fill_time_ns
                if execution_quote is None
                else execution_quote.available_at_ns
            ),
            exit_ns=anchor.outcome_time_ns,
            session_day=anchor.session_day,
            net_pnl=net,
            gross_pnl=float(gross),
            worst_unrealized_pnl=path_worst,
            best_unrealized_pnl=path_best,
            quantity=quantity,
            mini_equivalent=quantity / 10.0,
            regime=anchor.structural_family,
            session_compliant=anchor.session_compliant,
            contract_limit_compliant=anchor.contract_limit_compliant,
            same_bar_ambiguous=anchor.same_bar_ambiguous,
        ),
        marks=marks,
        initial_unrealized_pnl=initial_unrealized,
    )


def _scaled_value(value: float | None, quantity: int, tier: float) -> float | None:
    if value is None:
        return None
    scaled_quantity = max(1, int(math.floor(quantity * tier + 1e-12)))
    return float(value) * scaled_quantity / quantity


def _outcome_row(
    anchor: StructuralAnchor,
    role: str,
    economic_score: float,
    action: str,
    feature_hash: str,
    execution_quote: CausalEntryQuote | None = None,
) -> dict[str, Any]:
    tier = 0.0 if action == "ABSTAIN" else 1.0 if action == "TRADE_1X" else 1.5
    baseline_normal_path = _causal_action_trajectory(
        anchor, "NORMAL", 1.0, execution_quote
    )
    baseline_stressed_path = _causal_action_trajectory(
        anchor, "STRESSED_1_5X", 1.0, execution_quote
    )
    normal_path = (
        None
        if tier == 0.0
        else _causal_action_trajectory(anchor, "NORMAL", tier, execution_quote)
    )
    stressed_path = (
        None
        if tier == 0.0
        else _causal_action_trajectory(
            anchor, "STRESSED_1_5X", tier, execution_quote
        )
    )
    normal = 0.0 if normal_path is None else normal_path.event.net_pnl
    stressed = 0.0 if stressed_path is None else stressed_path.event.net_pnl
    baseline_normal = baseline_normal_path.event.net_pnl
    baseline_stressed = baseline_stressed_path.event.net_pnl
    normal_gross = 0.0 if normal_path is None else normal_path.event.gross_pnl
    stressed_gross = 0.0 if stressed_path is None else stressed_path.event.gross_pnl
    baseline_stressed_cost = (
        baseline_stressed_path.event.gross_pnl - baseline_stressed_path.event.net_pnl
    )
    stressed_cost = 0.0 if stressed_path is None else (
        stressed_path.event.gross_pnl - stressed_path.event.net_pnl
    )
    baseline_entry_time = (
        anchor.fill_time_ns
        if execution_quote is None
        else execution_quote.available_at_ns
    )
    baseline_duration = float(
        (anchor.outcome_time_ns - baseline_entry_time) / 1e9
    ) if anchor.outcome_time_ns is not None else None
    result = {
        "anchor_event_id": anchor.anchor_event_id,
        "source_candidate_id": anchor.source_candidate_id,
        "market": anchor.market,
        "contract": anchor.contract,
        "execution_market": anchor.execution_market,
        "structural_family": anchor.structural_family,
        "session_id": anchor.session_id,
        "session_day": anchor.session_day,
        "temporal_role": role,
        "decision_time_ns": anchor.decision_time_ns,
        "direction": anchor.direction,
        "action": action,
        "risk_tier": tier,
        "economic_action_score": economic_score,
        "feature_hash": feature_hash,
        "normal_net_pnl_usd": normal,
        "stressed_net_pnl_usd": stressed,
        "baseline_normal_net_pnl_usd": baseline_normal,
        "baseline_stressed_net_pnl_usd": baseline_stressed,
        "paired_normal_uplift_usd": float(normal or 0.0) - baseline_normal,
        "paired_stressed_uplift_usd": float(stressed or 0.0) - baseline_stressed,
        "normal_gross_pnl_usd": normal_gross,
        "stressed_gross_pnl_usd": stressed_gross,
        "normal_all_in_cost_usd": normal_gross - normal,
        "stressed_all_in_cost_usd": stressed_cost,
        "paired_entry_cost_delta_usd": stressed_cost - baseline_stressed_cost,
        "normal_worst_unrealized_pnl_usd": 0.0 if normal_path is None else normal_path.event.worst_unrealized_pnl,
        "stressed_worst_unrealized_pnl_usd": 0.0 if stressed_path is None else stressed_path.event.worst_unrealized_pnl,
        "normal_best_unrealized_pnl_usd": 0.0 if normal_path is None else normal_path.event.best_unrealized_pnl,
        "stressed_best_unrealized_pnl_usd": 0.0 if stressed_path is None else stressed_path.event.best_unrealized_pnl,
        "paired_mae_delta_usd": (0.0 if stressed_path is None else stressed_path.event.worst_unrealized_pnl) - baseline_stressed_path.event.worst_unrealized_pnl,
        "paired_mfe_delta_usd": (0.0 if stressed_path is None else stressed_path.event.best_unrealized_pnl) - baseline_stressed_path.event.best_unrealized_pnl,
        "stop_rate": float(stressed_path is not None and anchor.outcome == "ADVERSE_FIRST"),
        "target_rate": float(stressed_path is not None and anchor.outcome == "FAVORABLE_FIRST"),
        "paired_stop_rate_delta": -float(stressed_path is None and anchor.outcome == "ADVERSE_FIRST"),
        "paired_target_rate_delta": -float(stressed_path is None and anchor.outcome == "FAVORABLE_FIRST"),
        "holding_duration_seconds": None if stressed_path is None else baseline_duration,
        "paired_holding_duration_delta_seconds": -float(baseline_duration or 0.0) if stressed_path is None else 0.0,
        "paired_target_contribution_delta_usd": float(stressed or 0.0) - baseline_stressed,
        "paired_mll_contribution_delta_usd": (0.0 if stressed_path is None else stressed_path.event.worst_unrealized_pnl) - baseline_stressed_path.event.worst_unrealized_pnl,
        "quantity": 0 if tier == 0.0 else _exact_action_quantity(anchor, tier),
        "source_quantity": anchor.quantity,
        "integer_contract_sizing": True,
        "entry_time_ns": (
            None
            if tier == 0.0
            else anchor.fill_time_ns
            if execution_quote is None
            else execution_quote.available_at_ns
        ),
        "exit_time_ns": None if tier == 0.0 else anchor.outcome_time_ns,
        "normal_entry_price": (
            None
            if normal_path is None
            else anchor.normal_fill_price
            if execution_quote is None
            else _entry_fill_from_quote(
                anchor,
                execution_quote,
                quantity=normal_path.event.quantity,
                scenario="NORMAL",
            )
        ),
        "stressed_entry_price": (
            None
            if stressed_path is None
            else anchor.stressed_fill_price
            if execution_quote is None
            else _entry_fill_from_quote(
                anchor,
                execution_quote,
                quantity=stressed_path.event.quantity,
                scenario="STRESSED_1_5X",
            )
        ),
        "exit_price": None if tier == 0.0 else anchor.raw_exit_price,
        "stop_price": anchor.stop_price,
        "target_price": anchor.target_price,
        "same_bar_ambiguous": anchor.same_bar_ambiguous,
        "session_compliant": anchor.session_compliant,
        "contract_limit_compliant": anchor.contract_limit_compliant,
        "entry_fill_model": (
            "IMMUTABLE_STRUCTURAL_CAUSAL_FILL"
            if execution_quote is None
            else "ACQUIRED_BBO_AGGRESSIVE_DEPTH_SLIPPAGE_V1"
        ),
        "entry_execution_quote": (
            None if execution_quote is None else execution_quote.to_dict()
        ),
    }
    result["paired_outcome_hash"] = stable_hash(result)
    return result


def _summarize_paired(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    executed = [row for row in rows if float(row["risk_tier"]) > 0.0]
    positive = [max(0.0, float(row["stressed_net_pnl_usd"] or 0.0)) for row in executed]
    total_positive = math.fsum(positive)
    return {
        "opportunity_count": len(rows),
        "trade_count": len(executed),
        "trade_coverage": len(executed) / len(rows) if rows else 0.0,
        "abstention_rate": 1.0 - len(executed) / len(rows) if rows else 0.0,
        "normal_net_usd": float(math.fsum(float(row["normal_net_pnl_usd"] or 0.0) for row in rows)),
        "stressed_net_usd": float(math.fsum(float(row["stressed_net_pnl_usd"] or 0.0) for row in rows)),
        "baseline_normal_net_usd": float(math.fsum(float(row["baseline_normal_net_pnl_usd"]) for row in rows)),
        "baseline_stressed_net_usd": float(math.fsum(float(row["baseline_stressed_net_pnl_usd"]) for row in rows)),
        "paired_normal_uplift_usd": float(math.fsum(float(row["paired_normal_uplift_usd"]) for row in rows)),
        "paired_stressed_uplift_usd": float(math.fsum(float(row["paired_stressed_uplift_usd"]) for row in rows)),
        "improved_fraction": sum(float(row["paired_stressed_uplift_usd"]) > 0.0 for row in rows) / len(rows) if rows else 0.0,
        "harmed_fraction": sum(float(row["paired_stressed_uplift_usd"]) < 0.0 for row in rows) / len(rows) if rows else 0.0,
        "maximum_positive_trade_fraction": max(positive, default=0.0) / total_positive if total_positive > 0.0 else 0.0,
    }


def _sequential_evidence_checkpoints(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Evaluate preregistered checkpoints from their causal prefix only.

    In particular, a checkpoint may not be conditioned on the decision reached
    after the complete sample has been inspected.  This makes an early success
    (or continuation) reproducible from exactly the rows available at that
    checkpoint.
    """
    post_freeze = sorted(
        (
            row
            for row in rows
            if row["temporal_role"] == "FINAL_DEVELOPMENT"
        ),
        key=lambda row: (str(row["session_id"]), int(row["decision_time_ns"])),
    )
    sessions = sorted({str(row["session_id"]) for row in post_freeze})
    checkpoints = sorted({value for value in (5, 10, 15, len(sessions)) if 0 < value <= len(sessions)})
    output: list[dict[str, Any]] = []
    for count in checkpoints:
        included = set(sessions[:count])
        prefix = [row for row in post_freeze if str(row["session_id"]) in included]
        summary = _summarize_paired(prefix)
        positive_families = sum(
            _summarize_paired(
                [row for row in prefix if row["structural_family"] == family]
            )["stressed_net_usd"]
            > 0.0
            for family in {str(row["structural_family"]) for row in prefix}
        )
        success = bool(
            summary["stressed_net_usd"] > 0.0
            and summary["paired_stressed_uplift_usd"] > 0.0
            and positive_families >= 2
            and summary["maximum_positive_trade_fraction"] <= 0.25 + 1e-9
            and 0.20 - 1e-9 <= summary["trade_coverage"] <= 0.80 + 1e-9
        )
        futility = bool(
            count == len(sessions)
            and summary["stressed_net_usd"] <= 0.0
            and summary["paired_stressed_uplift_usd"] <= 0.0
        )
        decision = (
            "SUCCESS_EVIDENCE_SUFFICIENT"
            if success
            else "FUTILITY_STOP"
            if futility
            else "CONTINUE_ACQUISITION"
        )
        core = {
            "checkpoint_complete_session_count": count,
            "cumulative_anchor_count": len(prefix),
            "first_session": sessions[0],
            "last_session": sessions[count - 1],
            "summary": summary,
            "positive_anchor_family_count": positive_families,
            "decision": decision,
            "policy_refit_since_prior_checkpoint": False,
            "evidence_role": "FINAL_DEVELOPMENT_POST_FREEZE_PREFIX_ONLY",
            "validation_rows_used_for_checkpoint_decision": 0,
            "data_acquisition_mode": "ONE_FROZEN_TRANCHE_NOT_SEQUENTIAL_DOWNLOAD",
        }
        output.append({**core, "checkpoint_hash": stable_hash(core)})
    return output


def _action_for_probability(probability: float, low: float, high: float) -> str:
    if probability < low:
        return "ABSTAIN"
    if probability >= high:
        return "TRADE_1_5X"
    return "TRADE_1X"


def _account_policy(component_ids: Sequence[str], account_label: str) -> ActiveRiskPoolPolicy:
    snapshot = ACCOUNT_SNAPSHOTS[account_label]
    components = tuple(sorted(set(component_ids)))
    if not components:
        raise SelectiveVetoPilotError("0034 account replay has no executable component")
    return ActiveRiskPoolPolicy(
        policy_id=f"selective_veto_0034_distilled_v1:{account_label}",
        component_priority=components,
        nominal_risk_charge_per_mini=tuple((value, 2_250.0) for value in components),
        maximum_concurrent_sleeves=len(components),
        aggregate_open_risk_ceiling=float(snapshot["mll"]),
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=float(snapshot["max_mini"]),
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=float(snapshot["mll"]),
        daily_consistency_profit_guard=float(snapshot["target"]) * 0.50,
        target_protection_distance=0.0,
        target_protection_mode=TargetProtectionMode.NONE,
        static_risk_tier=1.0,
    )


def _account_rules(account_label: str) -> Topstep150KConfig:
    snapshot = ACCOUNT_SNAPSHOTS[account_label]
    return Topstep150KConfig(
        account_size=float(snapshot["account_size"]),
        combine_profit_target=float(snapshot["target"]),
        combine_max_loss_limit=float(snapshot["mll"]),
        combine_starting_balance=float(snapshot["account_size"]),
        no_daily_loss_limit=bool(snapshot["no_daily_loss_limit"]),
        optional_daily_loss_limit=min(3_000.0, float(snapshot["mll"])),
        use_optional_daily_loss_limit=bool(snapshot["use_optional_daily_loss_limit"]),
    )


def _episode_summary(episodes: Sequence[Any], mll: float) -> dict[str, Any]:
    if not episodes:
        return {
            "full_coverage_windows": 0,
            "pass_count": 0,
            "pass_rate": 0.0,
            "mll_breach_count": 0,
            "mll_breach_rate": 0.0,
            "consistency_compliance_rate": 0.0,
            "median_target_progress": 0.0,
            "lower_quartile_target_progress": 0.0,
            "minimum_mll_buffer_usd": float(mll),
            "median_days_to_target": None,
            "net_total_usd": 0.0,
            "episodes": [],
        }
    progress = np.asarray([row.target_progress for row in episodes], dtype=float)
    passing_days = [row.days_to_target for row in episodes if row.days_to_target is not None]
    return {
        "full_coverage_windows": len(episodes),
        "pass_count": int(sum(row.passed for row in episodes)),
        "pass_rate": float(sum(row.passed for row in episodes) / len(episodes)),
        "mll_breach_count": int(sum(row.mll_breached for row in episodes)),
        "mll_breach_rate": float(sum(row.mll_breached for row in episodes) / len(episodes)),
        "consistency_compliance_rate": float(sum(row.consistency_ok for row in episodes) / len(episodes)),
        "median_target_progress": float(np.median(progress)),
        "lower_quartile_target_progress": float(np.quantile(progress, 0.25)),
        "minimum_mll_buffer_usd": float(min(row.minimum_mll_buffer for row in episodes)),
        "median_days_to_target": float(np.median(passing_days)) if passing_days else None,
        "net_total_usd": float(math.fsum(row.net_pnl for row in episodes)),
        "episodes": [row.to_dict(include_paths=True) for row in episodes],
    }


def _account_matrix(
    rows: Sequence[Mapping[str, Any]],
    anchors: Mapping[str, StructuralAnchor],
    eligible_session_days: Sequence[int],
) -> list[dict[str, Any]]:
    """Run exact intraday P5/P10 replays on frozen, non-overlapping role starts."""

    executable = [row for row in rows if float(row["risk_tier"]) > 0.0]
    output: list[dict[str, Any]] = []
    for account_label, snapshot in ACCOUNT_SNAPSHOTS.items():
        policy = (
            _account_policy(
                [str(row["source_candidate_id"]) for row in executable], account_label
            )
            if executable
            else None
        )
        heldout: dict[tuple[str, int], list[Any]] = defaultdict(list)
        by_role: dict[str, Any] = {}
        for role in ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT"):
            role_all = [row for row in rows if row["temporal_role"] == role]
            role_executed = [row for row in executable if row["temporal_role"] == role]
            anchor_days = sorted({int(row["session_day"]) for row in role_all})
            days = tuple(
                int(day)
                for day in eligible_session_days
                if anchor_days and anchor_days[0] <= int(day) <= anchor_days[-1]
            )
            if anchor_days and not set(anchor_days).issubset(days):
                raise SelectiveVetoPilotError(
                    "0034 role anchor fell outside immutable full-coverage calendar"
                )
            role_result: dict[str, Any] = {}
            for scenario in ("NORMAL", "STRESSED_1_5X"):
                trajectories: dict[str, list[CausalTradeTrajectory]] = defaultdict(list)
                for row in role_executed:
                    anchor = anchors[str(row["anchor_event_id"])]
                    quote_value = row.get("entry_execution_quote")
                    execution_quote = (
                        None
                        if quote_value is None
                        else CausalEntryQuote(
                            schema=str(quote_value["schema"]),
                            event_time_ns=int(quote_value["event_time_ns"]),
                            available_at_ns=int(quote_value["available_at_ns"]),
                            bid_price=float(quote_value["bid_price"]),
                            ask_price=float(quote_value["ask_price"]),
                            bid_size=float(quote_value["bid_size"]),
                            ask_size=float(quote_value["ask_size"]),
                            first_mark_available_at_ns=(
                                None
                                if quote_value.get("first_mark_available_at_ns") is None
                                else int(quote_value["first_mark_available_at_ns"])
                            ),
                            post_fill_worst_liquidation_price=(
                                None
                                if quote_value.get("post_fill_worst_liquidation_price") is None
                                else float(quote_value["post_fill_worst_liquidation_price"])
                            ),
                            post_fill_best_liquidation_price=(
                                None
                                if quote_value.get("post_fill_best_liquidation_price") is None
                                else float(quote_value["post_fill_best_liquidation_price"])
                            ),
                            post_fill_last_liquidation_price=(
                                None
                                if quote_value.get("post_fill_last_liquidation_price") is None
                                else float(quote_value["post_fill_last_liquidation_price"])
                            ),
                        )
                    )
                    trajectories[str(row["source_candidate_id"])].append(
                        _causal_action_trajectory(
                            anchor,
                            scenario,
                            float(row["risk_tier"]),
                            execution_quote,
                        )
                    )
                scenario_result: dict[str, Any] = {}
                for horizon in (5, 10):
                    starts = [
                        days[index]
                        for index in range(0, len(days), horizon)
                        if index + horizon <= len(days)
                    ]
                    episodes = []
                    if policy is not None:
                        for start_day in starts:
                            episode = run_causal_shared_account_episode(
                                trajectories,
                                days,
                                policy=policy,
                                start_day=start_day,
                                maximum_duration_days=horizon,
                                config=_account_rules(account_label),
                            )
                            episodes.append(episode)
                            if role in {"VALIDATION", "FINAL_DEVELOPMENT"}:
                                heldout[(scenario, horizon)].append(episode)
                    scenario_result[f"p{horizon}"] = _episode_summary(
                        episodes, float(snapshot["mll"])
                    )
                role_result[scenario] = scenario_result
            by_role[role] = role_result
        cell: dict[str, Any] = {
            "account_label": account_label,
            "rule_snapshot": {
                **dict(snapshot),
                "status": (
                    "OFFICIAL_VERSIONED_RULE_SNAPSHOT"
                    if bool(
                        ACCOUNT_RULE_SNAPSHOTS[account_label].get(
                            "official_source_verified"
                        )
                    )
                    else "UNVERIFIED_RESEARCH_ACCOUNT_SIZE_SNAPSHOT"
                ),
            },
            "by_role": by_role,
        }
        for scenario, prefix in (("NORMAL", "normal_"), ("STRESSED_1_5X", "")):
            for horizon in (5, 10):
                cell[f"{prefix}p{horizon}"] = _episode_summary(
                    heldout[(scenario, horizon)], float(snapshot["mll"])
                )
        frozen_snapshot = ACCOUNT_RULE_SNAPSHOTS[account_label]
        cell["rule_snapshot_id"] = str(frozen_snapshot["snapshot_id"])
        cell["rule_snapshot_sha256"] = str(frozen_snapshot["snapshot_sha256"])
        cell["provenance_class"] = str(frozen_snapshot["provenance_class"])
        cell["scenarios"] = {
            "NORMAL": {"p5": cell["normal_p5"], "p10": cell["normal_p10"]},
            "STRESSED_1_5X": {"p5": cell["p5"], "p10": cell["p10"]},
        }
        cell["role_results_by_scenario"] = by_role
        output.append(cell)
    return output


def _entry_quote_from_outcome_row(
    row: Mapping[str, Any],
) -> CausalEntryQuote | None:
    value = row.get("entry_execution_quote")
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise SelectiveVetoPilotError("0034 paired row has malformed execution quote")
    return CausalEntryQuote(
        schema=str(value["schema"]),
        event_time_ns=int(value["event_time_ns"]),
        available_at_ns=int(value["available_at_ns"]),
        bid_price=float(value["bid_price"]),
        ask_price=float(value["ask_price"]),
        bid_size=float(value["bid_size"]),
        ask_size=float(value["ask_size"]),
        first_mark_available_at_ns=(
            None
            if value.get("first_mark_available_at_ns") is None
            else int(value["first_mark_available_at_ns"])
        ),
        post_fill_worst_liquidation_price=(
            None
            if value.get("post_fill_worst_liquidation_price") is None
            else float(value["post_fill_worst_liquidation_price"])
        ),
        post_fill_best_liquidation_price=(
            None
            if value.get("post_fill_best_liquidation_price") is None
            else float(value["post_fill_best_liquidation_price"])
        ),
        post_fill_last_liquidation_price=(
            None
            if value.get("post_fill_last_liquidation_price") is None
            else float(value["post_fill_last_liquidation_price"])
        ),
    )


def _same_opportunity_baseline_rows(
    selected_rows: Sequence[Mapping[str, Any]],
    anchors: Mapping[str, StructuralAnchor],
) -> list[dict[str, Any]]:
    """Rebuild A0 at 1x on exactly the opportunities seen by the rejector."""

    output: list[dict[str, Any]] = []
    for selected in selected_rows:
        anchor_id = str(selected["anchor_event_id"])
        anchor = anchors.get(anchor_id)
        if anchor is None:
            raise SelectiveVetoPilotError(
                f"0034 same-opportunity baseline lacks anchor {anchor_id}"
            )
        baseline = _outcome_row(
            anchor,
            str(selected["temporal_role"]),
            float(selected.get("economic_action_score") or 0.0),
            "TRADE_1X",
            str(selected["feature_hash"]),
            _entry_quote_from_outcome_row(selected),
        )
        if not math.isclose(
            float(baseline["stressed_net_pnl_usd"]),
            float(selected["baseline_stressed_net_pnl_usd"]),
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise SelectiveVetoPilotError(
                "0034 same-opportunity baseline economic reconciliation failed"
            )
        output.append(baseline)
    return output


def _target_progress_uplift_matrix(
    selected_matrix: Sequence[Mapping[str, Any]],
    baseline_matrix: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Compare selected and A0 account paths on identical starts.

    The preregistered no-pass alternative is deliberately strict: the same
    account size and horizon must improve stressed median target progress by at
    least five percentage points in both validation and final-development.
    """

    baseline_by_label = {
        str(cell["account_label"]): cell for cell in baseline_matrix
    }
    selected_labels = {str(cell["account_label"]) for cell in selected_matrix}
    if selected_labels != set(baseline_by_label):
        raise SelectiveVetoPilotError(
            "0034 selected/baseline account-size denominator mismatch"
        )
    output: list[dict[str, Any]] = []
    for selected in selected_matrix:
        account_label = str(selected["account_label"])
        baseline = baseline_by_label[account_label]
        for horizon in ("p5", "p10"):
            role_deltas: dict[str, float] = {}
            role_counts: dict[str, int] = {}
            role_values: dict[str, dict[str, float]] = {}
            for role in ("VALIDATION", "FINAL_DEVELOPMENT"):
                selected_summary = selected["by_role"][role]["STRESSED_1_5X"][
                    horizon
                ]
                baseline_summary = baseline["by_role"][role]["STRESSED_1_5X"][
                    horizon
                ]
                selected_count = int(selected_summary["full_coverage_windows"])
                baseline_count = int(baseline_summary["full_coverage_windows"])
                if selected_count != baseline_count:
                    raise SelectiveVetoPilotError(
                        "0034 same-opportunity account-start denominator mismatch"
                    )
                selected_progress = float(
                    selected_summary["median_target_progress"]
                )
                baseline_progress = float(
                    baseline_summary["median_target_progress"]
                )
                role_counts[role] = selected_count
                role_deltas[role] = selected_progress - baseline_progress
                role_values[role] = {
                    "selected_median_stressed_target_progress": selected_progress,
                    "baseline_median_stressed_target_progress": baseline_progress,
                }
            minimum_delta = min(role_deltas.values())
            material_stable = bool(
                all(value > 0 for value in role_counts.values())
                and minimum_delta
                >= MATERIAL_STRESSED_TARGET_PROGRESS_UPLIFT_MINIMUM - 1e-12
            )
            core = {
                "account_label": account_label,
                "horizon": horizon.upper(),
                "same_opportunity_account_starts": role_counts,
                "role_values": role_values,
                "stressed_target_progress_uplift": role_deltas,
                "minimum_validation_final_uplift": minimum_delta,
                "material_stable_uplift": material_stable,
                "minimum_required_uplift": (
                    MATERIAL_STRESSED_TARGET_PROGRESS_UPLIFT_MINIMUM
                ),
            }
            output.append({**core, "comparison_hash": stable_hash(core)})
    return output


def _select_fastest_viable_account(
    selected_matrix: Sequence[Mapping[str, Any]],
    target_progress_uplift: Sequence[Mapping[str, Any]],
    *,
    global_decision: str,
) -> Mapping[str, Any] | None:
    """Select only after GREEN and full account-specific qualification."""

    if global_decision != "LONG_SAMPLE_SELECTIVE_OVERLAY_GREEN":
        return None

    uplift_by_label: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in target_progress_uplift:
        uplift_by_label[str(row["account_label"])].append(row)
    viable: list[tuple[tuple[float, ...], Mapping[str, Any]]] = []
    for cell in selected_matrix:
        account_label = str(cell["account_label"])
        snapshot = ACCOUNT_RULE_SNAPSHOTS[account_label]
        if not bool(snapshot.get("official_source_verified")):
            continue
        uplift_rows = {
            str(row["horizon"]).lower(): row
            for row in uplift_by_label.get(account_label, ())
        }
        qualifying_horizons: list[tuple[float, ...]] = []
        for horizon in ("p5", "p10"):
            role_summaries = [
                cell["by_role"][role]["STRESSED_1_5X"][horizon]
                for role in ("VALIDATION", "FINAL_DEVELOPMENT")
            ]
            uplift = uplift_rows.get(horizon)
            speed_branch = bool(
                int(cell[horizon]["pass_count"]) > 0
                or (uplift is not None and bool(uplift["material_stable_uplift"]))
            )
            fully_qualified = bool(
                speed_branch
                and all(
                    int(summary["full_coverage_windows"]) > 0
                    and float(summary["net_total_usd"]) > 0.0
                    and float(summary["mll_breach_rate"]) <= 0.10 + 1e-12
                    and float(summary["consistency_compliance_rate"])
                    >= 0.50 - 1e-12
                    for summary in role_summaries
                )
            )
            if not fully_qualified:
                continue
            qualifying_horizons.append(
                (
                    1.0 if horizon == "p5" else 0.0,
                    float(cell[horizon]["pass_rate"]),
                    float(
                        uplift["minimum_validation_final_uplift"]
                        if uplift is not None
                        else float("-inf")
                    ),
                    float(cell[horizon]["median_target_progress"]),
                    float(cell[horizon]["minimum_mll_buffer_usd"]),
                )
            )
        if qualifying_horizons:
            best_horizon = max(qualifying_horizons)
            viable.append((best_horizon, cell))
    return max(viable, key=lambda item: item[0])[1] if viable else None


def _account_speed_gate(
    *,
    any_stressed_pass: bool,
    target_progress_uplift: Sequence[Mapping[str, Any]],
) -> bool:
    """Frozen disjunctive speed gate: an actual pass OR stable material uplift."""

    return bool(
        any_stressed_pass
        or any(bool(row["material_stable_uplift"]) for row in target_progress_uplift)
    )


def evaluate_long_sample(
    anchors: Sequence[StructuralAnchor],
    receipt: Mapping[str, Any],
    *,
    schema: str,
    frame_loader: Callable[[Path], Any] | None = None,
    eligible_session_days: Sequence[int] | None = None,
) -> dict[str, Any]:
    receipt_ids = [
        str(value)
        for item in receipt.get("files") or ()
        for value in item.get("anchor_ids") or ()
    ]
    if len(receipt_ids) != len(set(receipt_ids)):
        raise SelectiveVetoPilotError("acquired sample repeats a structural anchor")
    selected_ids = set(receipt_ids)
    anchor_by_id = {row.anchor_event_id: row for row in anchors}
    unknown = sorted(selected_ids - set(anchor_by_id))
    if unknown:
        raise SelectiveVetoPilotError(
            f"acquired sample references unknown structural anchors: {unknown[:5]}"
        )
    selected = [row for row in anchors if row.anchor_event_id in selected_ids]
    incomplete = [
        row.anchor_event_id
        for row in selected
        if row.normal_net_pnl_usd is None or row.stressed_net_pnl_usd is None
    ]
    if incomplete:
        raise SelectiveVetoPilotError(
            f"selected structural anchors lack paired economic outcomes: {incomplete[:5]}"
        )
    if len(selected) != len(selected_ids):
        raise SelectiveVetoPilotError("selected structural-anchor denominator drift")
    roles = _chronological_roles(selected)
    role_by_id = {anchor_id: role for role, ids in roles.items() for anchor_id in ids}
    worker_path_executed = False
    if frame_loader is None:
        features, worker_path_executed = _load_acquired_features(
            receipt, selected, schema=schema
        )
    else:
        by_id = {row.anchor_event_id: row for row in selected}
        features = {}
        for item in receipt.get("files") or ():
            frame = frame_loader(Path(str(item["raw_path"])))
            for anchor_id in item.get("anchor_ids") or ():
                anchor = by_id.get(str(anchor_id))
                if anchor is None:
                    raise SelectiveVetoPilotError(
                        "acquired window references unknown anchor"
                    )
                value = _feature_for_anchor(frame, anchor, schema=schema)
                if value is not None:
                    if anchor.anchor_event_id in features:
                        raise SelectiveVetoPilotError(
                            f"duplicate acquired feature row for anchor {anchor.anchor_event_id}"
                        )
                    features[anchor.anchor_event_id] = value
    missing_features = sorted(selected_ids - set(features))
    unexpected_features = sorted(set(features) - selected_ids)
    if missing_features or unexpected_features:
        raise SelectiveVetoPilotError(
            "0034 acquired feature coverage is not exact; "
            f"missing={missing_features[:10]}, unexpected={unexpected_features[:10]}"
        )
    usable = list(selected)
    discovery = [row for row in usable if role_by_id[row.anchor_event_id] == "DISCOVERY"]
    validation = [row for row in usable if role_by_id[row.anchor_event_id] == "VALIDATION"]
    final = [row for row in usable if role_by_id[row.anchor_event_id] == "FINAL_DEVELOPMENT"]
    expected_role_counts = {role: len(ids) for role, ids in roles.items()}
    usable_role_counts = {
        "DISCOVERY": len(discovery),
        "VALIDATION": len(validation),
        "FINAL_DEVELOPMENT": len(final),
    }
    if usable_role_counts != expected_role_counts or min(usable_role_counts.values()) == 0:
        raise SelectiveVetoPilotError(
            "0034 temporal-role feature coverage is incomplete: "
            f"expected={expected_role_counts}, usable={usable_role_counts}"
        )
    x_discovery = np.vstack([features[row.anchor_event_id][0] for row in discovery])
    discovery_paths = [
        _causal_action_trajectory(
            row,
            "STRESSED_1_5X",
            1.0,
            features[row.anchor_event_id][2],
        )
        for row in discovery
    ]
    discovery_net = np.asarray(
        [path.event.net_pnl for path in discovery_paths],
        dtype=float,
    )
    discovery_loss = np.asarray(discovery_net < 0.0, dtype=float)
    discovery_mae = np.asarray(
        [-min(float(path.event.worst_unrealized_pnl), 0.0) for path in discovery_paths],
        dtype=float,
    )
    discovery_cost = np.asarray(
        [
            path.event.gross_pnl - path.event.net_pnl
            for path in discovery_paths
        ],
        dtype=float,
    )
    net_model = _ridge_linear_fit(x_discovery, discovery_net)
    loss_model = _ridge_logistic_fit(
        x_discovery, discovery_loss, np.ones(len(discovery), dtype=float)
    )
    mae_model = _ridge_linear_fit(x_discovery, discovery_mae)
    cost_model = _ridge_linear_fit(x_discovery, discovery_cost)
    all_x = np.vstack([features[row.anchor_event_id][0] for row in usable])
    predicted_net = _ridge_linear_predict(net_model, all_x)
    predicted_loss = _ridge_logistic_predict(loss_model, all_x)
    predicted_mae = np.maximum(_ridge_linear_predict(mae_model, all_x), 0.0)
    predicted_cost = np.maximum(_ridge_linear_predict(cost_model, all_x), 0.0)
    # Net is already cost-adjusted.  Penalize model error, loss probability and
    # adverse excursion without subtracting the estimated cost twice.
    predicted_score = (
        predicted_net
        - float(net_model["residual_scale"]) * (0.50 + predicted_loss)
        - 0.10 * predicted_mae
    )
    estimates = {
        row.anchor_event_id: {
            "score": float(predicted_score[index]),
            "negative_probability": float(predicted_loss[index]),
            "mae_usd": float(predicted_mae[index]),
            "cost_usd": float(predicted_cost[index]),
        }
        for index, row in enumerate(usable)
    }
    discovery_score = np.asarray(
        [estimates[row.anchor_event_id]["score"] for row in discovery]
    )
    threshold_pairs = []
    for coverage, high_quantile in ((0.20, 0.90), (0.40, 0.85), (0.60, 0.80)):
        low = float(np.quantile(discovery_score, 1.0 - coverage))
        high = max(low, float(np.quantile(discovery_score, high_quantile)))
        threshold_pairs.append((coverage, low, high))
    candidates = []
    for coverage, low, high in threshold_pairs:
        rows = [
            _outcome_row(
                anchor,
                "VALIDATION",
                estimates[anchor.anchor_event_id]["score"],
                _action_for_probability(estimates[anchor.anchor_event_id]["score"], low, high),
                features[anchor.anchor_event_id][1],
                features[anchor.anchor_event_id][2],
            )
            for anchor in validation
        ]
        uplifts = np.asarray([float(row["paired_stressed_uplift_usd"]) for row in rows])
        lcb = float(np.mean(uplifts) - np.std(uplifts, ddof=1) / math.sqrt(len(uplifts))) if len(uplifts) > 1 else float(np.mean(uplifts))
        summary = _summarize_paired(rows)
        candidates.append({"coverage_target": coverage, "low_threshold": low, "high_threshold": high, "validation_lcb_uplift_usd_per_opportunity": lcb, "validation": summary})
    eligible = [row for row in candidates if 0.20 - 1e-9 <= float(row["validation"]["trade_coverage"]) <= 0.80 + 1e-9]
    frozen = max(eligible or candidates, key=lambda row: (row["validation_lcb_uplift_usd_per_opportunity"], row["validation"]["paired_stressed_uplift_usd"], -row["coverage_target"]))
    all_rows = [
        _outcome_row(
            anchor,
            role_by_id[anchor.anchor_event_id],
            estimates[anchor.anchor_event_id]["score"],
            _action_for_probability(estimates[anchor.anchor_event_id]["score"], float(frozen["low_threshold"]), float(frozen["high_threshold"])),
            features[anchor.anchor_event_id][1],
            features[anchor.anchor_event_id][2],
        )
        for anchor in usable
    ]
    for row in all_rows:
        estimate = estimates[str(row["anchor_event_id"])]
        row.update(
            {
                "predicted_negative_stressed_probability": estimate[
                    "negative_probability"
                ],
                "predicted_stressed_mae_usd": estimate["mae_usd"],
                "predicted_all_in_cost_usd": estimate["cost_usd"],
            }
        )
        row["paired_outcome_hash"] = stable_hash(
            {key: value for key, value in row.items() if key != "paired_outcome_hash"}
        )
    summaries = {
        role: _summarize_paired([row for row in all_rows if row["temporal_role"] == role])
        for role in ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
    }
    calendar = tuple(int(value) for value in (eligible_session_days or ()))
    if not calendar:
        raise SelectiveVetoPilotError(
            "0034 long replay requires the immutable 0028 trading-day calendar"
        )
    usable_by_id = {row.anchor_event_id: row for row in usable}
    matrix = _account_matrix(all_rows, usable_by_id, calendar)
    baseline_rows = _same_opportunity_baseline_rows(all_rows, usable_by_id)
    baseline_matrix = _account_matrix(baseline_rows, usable_by_id, calendar)
    target_progress_uplift = _target_progress_uplift_matrix(
        matrix, baseline_matrix
    )
    heldout = [row for row in all_rows if row["temporal_role"] in {"VALIDATION", "FINAL_DEVELOPMENT"}]
    positive_contexts = max(
        sum(_summarize_paired([row for row in heldout if row["structural_family"] == family])["stressed_net_usd"] > 0.0 for family in {row["structural_family"] for row in heldout}),
        sum(_summarize_paired([row for row in heldout if row["session_id"] == session])["stressed_net_usd"] > 0.0 for session in {row["session_id"] for row in heldout}),
    )
    no_mll = all(float(cell[horizon]["mll_breach_rate"]) <= 0.10 for cell in matrix for horizon in ("p5", "p10"))
    any_pass = any(int(cell[horizon]["pass_count"]) > 0 for cell in matrix for horizon in ("p5", "p10"))
    material_stable_target_progress_uplift = any(
        bool(row["material_stable_uplift"]) for row in target_progress_uplift
    )
    account_speed_gate = _account_speed_gate(
        any_stressed_pass=any_pass,
        target_progress_uplift=target_progress_uplift,
    )
    consistency = all(
        float(cell[horizon]["consistency_compliance_rate"]) >= 0.50
        for cell in matrix
        for horizon in ("p5", "p10")
        if int(cell[horizon]["full_coverage_windows"]) > 0
    )
    heldout_coverage = float(_summarize_paired(heldout)["trade_coverage"])
    green = (
        float(summaries["VALIDATION"]["stressed_net_usd"]) > 0.0
        and float(summaries["FINAL_DEVELOPMENT"]["stressed_net_usd"]) > 0.0
        and float(summaries["VALIDATION"]["paired_stressed_uplift_usd"]) > 0.0
        and float(summaries["FINAL_DEVELOPMENT"]["paired_stressed_uplift_usd"]) > 0.0
        and positive_contexts >= 2
        and float(_summarize_paired(heldout)["maximum_positive_trade_fraction"]) <= 0.25 + 1e-9
        and no_mll
        and consistency
        and 0.20 - 1e-9 <= heldout_coverage <= 0.80 + 1e-9
        and account_speed_gate
    )
    weak = (
        float(summaries["VALIDATION"]["paired_stressed_uplift_usd"]) > 0.0
        or float(summaries["FINAL_DEVELOPMENT"]["paired_stressed_uplift_usd"]) > 0.0
    )
    decision = "LONG_SAMPLE_SELECTIVE_OVERLAY_GREEN" if green else "LONG_SAMPLE_SELECTIVE_OVERLAY_WEAK" if weak else "LONG_SAMPLE_SELECTIVE_OVERLAY_FALSIFIED"
    fastest = _select_fastest_viable_account(
        matrix,
        target_progress_uplift,
        global_decision=decision,
    )
    policy_core = {
        "policy_id": "selective_veto_0034_distilled_v1",
        "model_class": "REGULARIZED_PESSIMISTIC_ECONOMIC_RESPONSE_V1",
        "feature_names": list(FEATURE_NAMES),
        "feature_count": len(FEATURE_NAMES),
        "actions": list(PRIMARY_ACTIONS),
        "low_trade_threshold": frozen["low_threshold"],
        "high_risk_threshold": frozen["high_threshold"],
        "expected_stressed_net_model": {
            "coefficient": np.asarray(net_model["beta"]).tolist(),
            "feature_mean": np.asarray(net_model["mean"]).tolist(),
            "feature_scale": np.asarray(net_model["scale"]).tolist(),
            "residual_scale": float(net_model["residual_scale"]),
        },
        "negative_outcome_model": {
            "coefficient": np.asarray(loss_model["beta"]).tolist()
        },
        "expected_mae_model": {
            "coefficient": np.asarray(mae_model["beta"]).tolist()
        },
        "expected_cost_model": {
            "coefficient": np.asarray(cost_model["beta"]).tolist()
        },
        "objective": "LCB_PAIRED_STRESSED_UPLIFT_WITH_LOSS_AND_MAE_PENALTY",
        "schema": schema,
        "deployability": "L1_DEPLOYABLE" if schema == "tbbo" else "L2_DEPLOYABLE",
        "direction_generation_allowed": False,
        "frozen_before_final_development": True,
    }
    policy = {**policy_core, "policy_fingerprint": stable_hash(policy_core)}
    return {
        "status": "COMPLETE",
        "decision": decision,
        "policy_frozen_before_final_development": True,
        "policy": policy,
        "candidate_thresholds": candidates,
        "role_results": summaries,
        "paired_results": all_rows,
        "attribution": {
            dimension: [
                {dimension: key, **_summarize_paired([row for row in heldout if row[dimension] == key])}
                for key in sorted({str(row[dimension]) for row in heldout})
            ]
            for dimension in ("market", "structural_family", "session_id")
        },
        "account_size_matrix": matrix,
        "same_opportunity_baseline_account_matrix": baseline_matrix,
        "same_opportunity_target_progress_uplift": target_progress_uplift,
        "fastest_viable_account_size": None if fastest is None else fastest["account_label"],
        "causal_feature_row_count": len(usable),
        "feature_coverage_invariant": {
            "selected_anchor_count": len(selected),
            "causal_feature_row_count": len(usable),
            "missing_anchor_count": 0,
            "unexpected_anchor_count": 0,
            "expected_role_counts": expected_role_counts,
            "usable_role_counts": usable_role_counts,
            "final_development_all_eligible_anchors_included": True,
        },
        "role_counts": {role: sum(row["temporal_role"] == role for row in all_rows) for role in ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")},
        "positive_context_count": positive_contexts,
        "single_trade_domination_fraction": _summarize_paired(heldout)["maximum_positive_trade_fraction"],
        "mll_within_tolerance": no_mll,
        "consistency_within_tolerance": consistency,
        "heldout_trade_coverage": heldout_coverage,
        "green_account_speed_gate": {
            "operator": "OR",
            "actual_stressed_p5_or_p10_pass": any_pass,
            "material_stable_stressed_target_progress_uplift": (
                material_stable_target_progress_uplift
            ),
        },
        "green_requires_actual_stressed_p5_or_p10_pass": False,
        "material_stressed_target_progress_uplift_minimum": (
            MATERIAL_STRESSED_TARGET_PROGRESS_UPLIFT_MINIMUM
        ),
        "sequential_checkpoints": _sequential_evidence_checkpoints(all_rows),
        "feature_extraction_runtime": {
            "process_pool_executed": worker_path_executed,
            "cpu_worker_count": 2 if worker_path_executed else 0,
        },
    }


def _read_evidence_part(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise SelectiveVetoPilotError(f"source EvidenceBundle part unavailable: {path}")
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]
    return [dict(row) for row in rows if "_evidence_part" not in row]


def _source_seed_material(root: Path) -> dict[str, list[dict[str, Any]]]:
    """Reuse both real 0033 seed bundles for the no-purchase audit path."""

    bundle = (
        root
        / "data/cache/evidence_bundles"
        / "hydra_hybrid_structural_alpha_order_flow_0033.evidence-v1"
        / "datasets"
    )
    output: dict[str, list[dict[str, Any]]] = {
        name: [] for name in REQUIRED_DATASETS
    }
    seed_ids = {PRIMARY_SEED_ID, SECONDARY_SEED_ID}
    component_prefixes = tuple(f"{seed_id}." for seed_id in sorted(seed_ids))
    for dataset in REQUIRED_DATASETS:
        if dataset == "provenance":
            continue
        rows = _read_evidence_part(bundle / dataset / "part-000000.jsonl.gz")
        for row in rows:
            include = False
            if dataset.startswith("component_"):
                include = str(row.get("component_id") or "").startswith(
                    component_prefixes
                )
            elif dataset == "account_policy_membership":
                include = str(row.get("policy_id") or "") in seed_ids
            elif dataset in {"account_daily_paths", "episodes"}:
                include = str(row.get("policy_id") or "") in seed_ids
            if include:
                row["campaign_id"] = CAMPAIGN_ID
                output[dataset].append(row)
    if any(not output[name] for name in REQUIRED_DATASETS if name != "provenance"):
        raise SelectiveVetoPilotError("real 0033 seed EvidenceBundle extraction is incomplete")
    return output


def _terminal_state(value: str) -> str:
    return {
        CombineTerminal.PASSED.value: "TARGET_REACHED",
        CombineTerminal.MLL_BREACH.value: "MLL_BREACHED",
        CombineTerminal.COMPLIANCE_FAILURE.value: "HARD_RULE_FAILURE",
        CombineTerminal.TIMEOUT.value: "OPERATIONAL_HORIZON_NOT_REACHED",
    }[value]


def _long_sample_material(
    long_sample: Mapping[str, Any], policy_id: str
) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    """Materialize real decisions, trades, and all three exact account sizes."""

    datasets: dict[str, list[dict[str, Any]]] = {
        name: [] for name in REQUIRED_DATASETS
    }
    rows = [dict(row) for row in long_sample.get("paired_results") or ()]
    components = sorted(
        {
            str(row["source_candidate_id"])
            for row in rows
            if float(row.get("risk_tier") or 0.0) > 0.0
        }
    )
    if not rows or not components:
        raise SelectiveVetoPilotError("long-sample evidence has no real policy decisions")
    for row in rows:
        component = str(row["source_candidate_id"])
        # The standard EvidenceBundle contract is executable: every declared
        # component must own at least one real trade.  Veto-only source sleeves
        # remain fully preserved in paired_results, but cannot masquerade as an
        # executable account member in the relational trade ledger.
        if component not in components:
            continue
        signal_id = f"{policy_id}.{row['anchor_event_id']}"
        side = "BUY" if int(row["direction"]) > 0 else "SELL"
        executed = float(row["risk_tier"]) > 0.0
        datasets["component_signals"].append(
            {
                "campaign_id": CAMPAIGN_ID,
                "component_id": component,
                "signal_id": signal_id,
                "event_time": _iso_ns(int(row["decision_time_ns"])),
                "market": str(row["market"]),
                "contract": str(row["contract"]),
                "timeframe": "EVENT",
                "signal": side if executed else "ABSTAIN",
                "sizing": float(row["quantity"]),
                "stop": float(row["stop_price"]),
                "target": float(row["target_price"]),
                "veto": not executed,
                "component_role": "SELECTIVE_VETO_STRUCTURAL_ANCHOR",
                "action": str(row["action"]),
                "feature_hash": str(row["feature_hash"]),
            }
        )
        if not executed:
            continue
        trade_id = signal_id
        entry_time = _iso_ns(int(row["entry_time_ns"]))
        exit_time = _iso_ns(int(row["exit_time_ns"]))
        datasets["component_entries"].append(
            {
                "campaign_id": CAMPAIGN_ID,
                "component_id": component,
                "trade_id": trade_id,
                "entry_time": entry_time,
                "market": str(row["market"]),
                "contract": str(row["contract"]),
                "side": side,
                "quantity": float(row["quantity"]),
                "entry_price": float(row["normal_entry_price"]),
                "sizing": float(row["quantity"]),
                "stop_price": float(row["stop_price"]),
                "target_price": float(row["target_price"]),
            }
        )
        datasets["component_exits"].append(
            {
                "campaign_id": CAMPAIGN_ID,
                "component_id": component,
                "trade_id": trade_id,
                "exit_time": exit_time,
                "exit_price": float(row["exit_price"]),
                "exit_reason": "FROZEN_CAUSAL_STRUCTURAL_EXIT",
            }
        )
        datasets["component_trades"].append(
            {
                "campaign_id": CAMPAIGN_ID,
                "component_id": component,
                "trade_id": trade_id,
                "entry_time": entry_time,
                "exit_time": exit_time,
                "market": str(row["market"]),
                "contract": str(row["contract"]),
                "side": side,
                "quantity": float(row["quantity"]),
                "entry_price": float(row["normal_entry_price"]),
                "exit_price": float(row["exit_price"]),
                "gross_pnl": float(row["normal_gross_pnl_usd"]),
                "costs": float(row["normal_all_in_cost_usd"]),
                "net_pnl": float(row["normal_net_pnl_usd"]),
            }
        )
    matrices = list(long_sample.get("account_size_matrix") or ())
    if {str(row.get("account_label")) for row in matrices} != set(
        ACCOUNT_RULE_SNAPSHOTS
    ):
        raise SelectiveVetoPilotError("exact three-size long-sample account matrix absent")
    account_policy_ids = {
        str(matrix["account_label"]): f"{policy_id}:{matrix['account_label']}"
        for matrix in matrices
    }
    for account_policy_id in account_policy_ids.values():
        for component in components:
            datasets["account_policy_membership"].append(
                {
                    "campaign_id": CAMPAIGN_ID,
                    "policy_id": account_policy_id,
                    "component_id": component,
                    "risk_allocation": 1.0,
                    "component_role": "SELECTIVE_VETO_STRUCTURAL_ANCHOR",
                }
            )
    for matrix in matrices:
        account_label = str(matrix["account_label"])
        account_policy_id = account_policy_ids[account_label]
        for role, role_value in dict(matrix["by_role"]).items():
            for scenario, scenario_value in dict(role_value).items():
                for horizon_key, summary in dict(scenario_value).items():
                    horizon = horizon_key.upper()
                    for episode in summary.get("episodes") or ():
                        episode_id = (
                            f"{account_policy_id}.{role}.{horizon}."
                            f"{int(episode['start_day'])}"
                        )
                        terminal = str(episode["terminal"])
                        terminal_state = _terminal_state(terminal)
                        for day in episode.get("daily_path") or ():
                            session_day = int(day["session_day"])
                            datasets["account_daily_paths"].append(
                                {
                                    "campaign_id": CAMPAIGN_ID,
                                    "policy_id": account_policy_id,
                                    "episode_id": episode_id,
                                    "trading_day": datetime.fromtimestamp(
                                        session_day * 86_400, tz=UTC
                                    ).date().isoformat(),
                                    "cost_scenario": scenario,
                                    "horizon": horizon,
                                    "realized_pnl": float(day["realized_pnl"]),
                                    "unrealized_pnl": float(day["unrealized_pnl"]),
                                    "daily_pnl": float(day["day_pnl"]),
                                    "equity": float(day["balance"])
                                    + float(day["unrealized_pnl"]),
                                    "mll": float(day["mll_floor"]),
                                    "mll_buffer": float(day["mll_buffer"]),
                                    "minimum_mll_buffer": float(
                                        day["minimum_mll_buffer"]
                                    ),
                                    "consistency": float(day["consistency"]),
                                    "target_progress": float(day["target_progress"]),
                                    "costs": float(day["costs"]),
                                    "conflicts": dict(day["conflicts"]),
                                    "consistency_ok": bool(day["consistency_ok"]),
                                    "exposure": dict(day["exposure"]),
                                    "component_attribution": dict(day["component_attribution"]),
                                    "account_size_label": account_label,
                                }
                            )
                        datasets["episodes"].append(
                            {
                                "campaign_id": CAMPAIGN_ID,
                                "policy_id": account_policy_id,
                                "episode_id": episode_id,
                                "episode_start": datetime.fromtimestamp(
                                    int(episode["start_day"]) * 86_400, tz=UTC
                                ).isoformat(),
                                "horizon": horizon,
                                "temporal_block": f"{account_label}_{role}",
                                "duration_trading_days": int(
                                    episode["eligible_days"]
                                ),
                                "target_reached": terminal_state
                                == "TARGET_REACHED",
                                "mll_breached": terminal_state == "MLL_BREACHED",
                                "censored_state": terminal_state
                                == "OPERATIONAL_HORIZON_NOT_REACHED",
                                "cost_scenario": scenario,
                                "costs": float(episode["total_cost"]),
                                "net_pnl": float(episode["net_pnl"]),
                                "target_progress": float(
                                    episode["target_progress"]
                                ),
                                "minimum_mll_buffer": float(
                                    episode["minimum_mll_buffer"]
                                ),
                                "consistency_ok": bool(
                                    episode["consistency_ok"]
                                ),
                                "days_to_target": episode["days_to_target"],
                                "failure_vector": [
                                    str(episode["terminal_reason"]),
                                    f"ACCOUNT_SIZE_{account_label}",
                                ],
                                "terminal_state": terminal_state,
                                "account_size_label": account_label,
                            }
                        )
    if any(not datasets[name] for name in REQUIRED_DATASETS if name != "provenance"):
        raise SelectiveVetoPilotError("complete real long-sample EvidenceBundle material absent")
    return datasets, components


def _canonical_material(
    manifest: Mapping[str, Any],
    root: Path,
    seed: Mapping[str, Any],
    anchor: Mapping[str, Any],
    cost: Mapping[str, Any],
    acquisition: Mapping[str, Any],
    long_sample: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]], dict[str, Any]]:
    source_commit = str(manifest["source_commit"])
    manifest_hash = str(manifest["manifest_hash"])
    policy = long_sample.get("policy") if isinstance(long_sample.get("policy"), Mapping) else None
    policy_id = str((policy or {}).get("policy_id") or PRIMARY_SEED_ID)
    policy_hash = str((policy or {}).get("policy_fingerprint") or seed["policies"][PRIMARY_SEED_ID]["frozen_policy"]["policy_fingerprint"])
    paired_rows = list(long_sample.get("paired_results") or ())
    if paired_rows:
        datasets, component_ids = _long_sample_material(long_sample, policy_id)
        evidence_basis = "0034_REAL_LONG_SAMPLE_CAUSAL_REPLAY"
    else:
        datasets = _source_seed_material(root)
        component_ids = sorted(
            {
                str(row["component_id"])
                for row in datasets["account_policy_membership"]
            }
        )
        evidence_basis = "0033_REAL_SEED_EVIDENCE_REUSED_FOR_0034_AUDIT"
    component_fingerprints = {value: stable_hash({"component_id": value, "anchor_universe_hash": anchor.get("anchor_universe_hash")}) for value in component_ids}
    access_ledger = root / "reports/data_access/data_access_ledger.jsonl"
    data_fingerprints = {
        "anchor_universe": str(anchor.get("anchor_universe_hash") or stable_hash(anchor)),
        "cost_matrix": str(cost.get("cost_matrix_hash") or stable_hash(cost)),
        "acquisition": str(acquisition.get("acquisition_receipt_fingerprint") or stable_hash(acquisition)),
        "data_access_ledger": _sha256(access_ledger) if access_ledger.is_file() else stable_hash("ABSENT_ACCESS_LEDGER"),
    }
    episode_keys = [
        {"policy_id": policy_value, "episode_id": episode_value, "horizon": horizon}
        for policy_value, episode_value, horizon in sorted(
            {
                (
                    str(row["policy_id"]),
                    str(row["episode_id"]),
                    str(row["horizon"]),
                )
                for row in datasets["episodes"]
            }
        )
    ]
    policy_ids = sorted({str(row["policy_id"]) for row in datasets["episodes"]})
    if paired_rows:
        policy_fingerprints = {}
        for account_label, snapshot in ACCOUNT_RULE_SNAPSHOTS.items():
            account_policy_id = f"{policy_id}:{account_label}"
            if account_policy_id not in policy_ids:
                raise SelectiveVetoPilotError(
                    f"long-sample EvidenceBundle lacks {account_label} policy identity"
                )
            policy_fingerprints[account_policy_id] = stable_hash(
                {
                    "base_policy_id": policy_id,
                    "base_policy_fingerprint": policy_hash,
                    "account_size_label": account_label,
                    "account_rule_snapshot_id": snapshot["snapshot_id"],
                    "account_rule_snapshot_sha256": snapshot["snapshot_sha256"],
                }
            )
    else:
        policy_fingerprints = {}
        for seed_policy_id in policy_ids:
            try:
                fingerprint = seed["policies"][seed_policy_id]["frozen_policy"][
                    "policy_fingerprint"
                ]
            except (KeyError, TypeError) as exc:
                raise SelectiveVetoPilotError(
                    f"frozen 0033 seed fingerprint unavailable: {seed_policy_id}"
                ) from exc
            policy_fingerprints[seed_policy_id] = str(fingerprint)
    identity = {
        "campaign_id": CAMPAIGN_ID,
        "grammar_id": "SELECTIVE_ORDER_FLOW_VETO_EXPANSION_V1",
        "policy_fingerprints": policy_fingerprints,
        "component_fingerprints": component_fingerprints,
        "source_commit": source_commit,
        "data_fingerprints": data_fingerprints,
        "configuration_sha256": manifest_hash,
        "seeds": [34_001, 34_007],
        "created_at_utc": datetime.now(UTC).isoformat(),
        "expected_coverage": {
            "policy_ids": policy_ids,
            "component_ids": component_ids,
            "allowed_horizons": sorted(
                {str(row["horizon"]) for row in datasets["episodes"]}
            ),
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
            "required_episode_keys": episode_keys,
            "allow_additional_episode_keys": True,
        },
        "manifest_hash": manifest_hash,
    }
    now = datetime.now(UTC).isoformat()
    provenance_checksums = {
        "configuration": manifest_hash,
        **{f"data:{name}": digest for name, digest in data_fingerprints.items()},
    }
    datasets["provenance"].append({"campaign_id": CAMPAIGN_ID, "validator_version": PILOT_VERSION, "replay_version": PILOT_VERSION, "market_data_role": "CONTAMINATED_DEVELOPMENT", "access_ledger_sha256": data_fingerprints["data_access_ledger"], "reconstruction_flag": False, "immutable_checksums": {**provenance_checksums, "evidence_basis": stable_hash(evidence_basis)}, "recorded_at_utc": now})
    compact = {
        "campaign_summary": {"seed_audit": seed, "anchor_universe": anchor, "window_cost_matrix": cost, "acquisition": acquisition, "long_sample": {key: value for key, value in long_sample.items() if key != "paired_results"}},
        "failure_vectors": {"NO_AFFORDABLE_LONG_SAMPLE": int(not acquisition.get("purchase_performed")), "LONG_SAMPLE_FALSIFIED": int(long_sample.get("decision") == "LONG_SAMPLE_SELECTIVE_OVERLAY_FALSIFIED")},
        "pareto_archive": {
            "policy": policy,
            "decision": long_sample.get("decision"),
            "paired_long_sample_outcomes": paired_rows,
        },
        "next_campaign_recommendations": {"decision": long_sample.get("decision"), "broad_purchase_authorized": False},
    }
    return identity, datasets, compact


def run_selective_veto_campaign(
    *,
    manifest: Mapping[str, Any],
    project_root: str | Path,
    output_dir: str | Path,
    contract_map_path: Path | None = None,
    cache_root: Path | None = None,
    metadata_client: Any | None = None,
    acquire: bool = True,
) -> dict[str, Any]:
    """Run seed audit, targeted cost decision, conditional acquisition and replay."""

    del contract_map_path, cache_root
    root = Path(project_root).resolve()
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    wall_start = time.perf_counter()
    economic_seconds = 0.0
    cpu_start = resource.getrusage(resource.RUSAGE_SELF)
    children_start = resource.getrusage(resource.RUSAGE_CHILDREN)
    cfg = TargetedCostConfig(
        pre_decision_seconds=int(manifest["anchor_conditioned_windows"]["pre_decision_lookback_seconds"]),
        post_decision_seconds=int(manifest["anchor_conditioned_windows"]["post_decision_safety_seconds"]),
        maximum_incremental_spend_usd=float(manifest["targeted_cost_policy"]["maximum_incremental_spend_usd"]),
        minimum_budget_reserve_usd=float(manifest["targeted_cost_policy"]["minimum_budget_reserve_usd"]),
        current_remaining_budget_usd=float(manifest["targeted_cost_policy"]["current_remaining_budget_usd"]),
    )
    cfg.validate()
    economic_phase = time.perf_counter()
    audit = run_seed_robustness_audit(
        root / SOURCE_0033 / "pilot/hybrid_pilot_summary.json",
        root / SOURCE_0033 / "decision_report.json",
    )
    economic_seconds += time.perf_counter() - economic_phase
    write_seed_audit_checkpoint(audit, output / "seed_audit_checkpoint.json")
    seed = _seed_runtime_mapping(audit)
    if audit["result"] == "SELECTIVE_VETO_SEED_FALSIFIED":
        anchor = {"anchors_generated": 0, "merged_windows_estimated": 0, "anchor_universe_hash": stable_hash([])}
        cost = _empty_cost("NOT_RUN_SEED_FALSIFIED")
        acquisition = _no_purchase_acquisition(cfg.current_remaining_budget_usd)
        long_sample = _empty_long("NOT_RUN_SEED_FALSIFIED", "LONG_SAMPLE_SELECTIVE_OVERLAY_FALSIFIED")
        forward = _diagnostic_forward("NOT_STARTED_SEED_FALSIFIED")
    else:
        economic_phase = time.perf_counter()
        anchors, anchor = build_long_anchor_universe(root)
        economic_seconds += time.perf_counter() - economic_phase
        all_windows = make_event_windows(anchors, pre_seconds=cfg.pre_decision_seconds, post_seconds=cfg.post_decision_seconds)
        anchor["merged_windows_estimated"] = len(all_windows)
        client = metadata_client or _official_client()
        cost = generate_targeted_cost_matrix(
            client.metadata if hasattr(client, "metadata") else client,
            anchors,
            audit,
            config=cfg,
            metadata_cache_path=output / "databento_metadata_estimates.jsonl",
        )
        _write_json_once(output / "targeted_event_window_cost_matrix.json", cost)
        offer = cost.get("selected_offer")
        if not isinstance(offer, Mapping) or not acquire:
            acquisition = _no_purchase_acquisition(cfg.current_remaining_budget_usd)
            long_sample = _empty_long("NOT_STARTED_NO_AFFORDABLE_SAMPLE", "LONG_SAMPLE_SELECTIVE_OVERLAY_WEAK")
            forward = _diagnostic_forward("NOT_STARTED_NO_AUTHORIZED_RESEARCH_FEED")
        else:
            receipt = _acquire_selected_offer(client, offer, root=root, manifest=manifest, config=cfg)
            spend = float(receipt["actual_spend_usd"])
            acquisition = {
                "purchase_performed": True,
                "actual_spend_usd": spend,
                "prior_budget_usd": cfg.current_remaining_budget_usd,
                "remaining_budget_usd": cfg.current_remaining_budget_usd - spend,
                "q4_accessed": False,
                "broker_connections": 0,
                "orders": 0,
                "official_estimate_fingerprint": offer["estimate_fingerprint"],
                **dict(receipt),
            }
            selected_ids = set(str(value) for value in offer["anchor_ids"])
            economic_phase = time.perf_counter()
            long_sample = evaluate_long_sample(
                [row for row in anchors if row.anchor_event_id in selected_ids],
                receipt,
                schema=str(offer["schema"]),
                eligible_session_days=anchor["eligible_session_days"],
            )
            economic_seconds += time.perf_counter() - economic_phase
            forward = _diagnostic_forward("NOT_STARTED_NO_AUTHORIZED_RESEARCH_FEED")

    identity, datasets, compact = _canonical_material(manifest, root, seed, anchor, cost, acquisition, long_sample)
    elapsed = max(time.perf_counter() - wall_start, 1e-9)
    self_after = resource.getrusage(resource.RUSAGE_SELF)
    child_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu = (
        self_after.ru_utime + self_after.ru_stime + child_after.ru_utime + child_after.ru_stime
        - cpu_start.ru_utime - cpu_start.ru_stime - children_start.ru_utime - children_start.ru_stime
    )
    feature_runtime = long_sample.get("feature_extraction_runtime")
    worker_path_executed = bool(
        isinstance(feature_runtime, Mapping)
        and feature_runtime.get("process_pool_executed") is True
        and int(feature_runtime.get("cpu_worker_count") or 0) == 2
    )
    result = {
        "seed_audit": seed,
        "anchor_universe": anchor,
        "window_cost_matrix": cost,
        "acquisition": acquisition,
        "long_sample": long_sample,
        "diagnostic_forward": forward,
        "evidence_identity": identity,
        "evidence_datasets": datasets,
        "compact_outputs": compact,
        "production_kpis": {
            "anchors_generated": int(anchor.get("anchors_generated", 0)),
            "independent_anchors_acquired": int(acquisition.get("independent_anchors_acquired", 0)),
            "paired_policy_decisions": len(long_sample.get("paired_results") or ()),
            "bounded_action_counterfactuals_available": len(long_sample.get("paired_results") or ()) * 3,
            "account_episodes_completed": sum(int(cell[h]["full_coverage_windows"]) for cell in long_sample.get("account_size_matrix") or () for h in ("p5", "p10")),
            "actual_additional_spend_usd": float(acquisition["actual_spend_usd"]),
        },
        "runtime_metrics": {
            "elapsed_seconds": elapsed,
            "cpu_seconds": cpu,
            "aggregate_cpu_utilization": min(max(cpu / (elapsed * 3.0), 0.0), 1.0),
            "economic_wall_clock_fraction": min(
                max(economic_seconds / elapsed, 0.0), 1.0
            ),
            "economic_wall_clock_seconds": economic_seconds,
            "cpu_worker_count": 2 if worker_path_executed else 0,
            "cpu_worker_count_contract_maximum": 2,
            "worker_utilization_claimed": worker_path_executed,
        },
    }
    _write_json_once(output / "selective_veto_campaign_summary.json", {key: value for key, value in result.items() if key != "evidence_datasets"})
    _write_json_once(output / "selective_veto_evidence_material.json", {"identity": identity, "datasets": datasets, "outputs": compact})
    return result


__all__ = [
    "ANCHOR_IDS_0033",
    "CAMPAIGN_ID",
    "EventWindow",
    "PILOT_VERSION",
    "PRIMARY_ACTIONS",
    "SelectiveVetoPilotError",
    "StructuralAnchor",
    "TargetedCostConfig",
    "build_long_anchor_universe",
    "evaluate_long_sample",
    "generate_targeted_cost_matrix",
    "make_event_windows",
    "run_selective_veto_campaign",
]
