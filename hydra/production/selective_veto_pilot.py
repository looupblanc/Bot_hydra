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
)
from hydra.production.selective_veto_metadata import (
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
            or self.post_decision_seconds not in {30, 60}
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
    return {
        "CROSS_ASSET_STATE": "CROSS_MARKET_DIVERGENCE",
        "DISPLACEMENT_ACCELERATION": "SESSION_TRANSITION",
        "EXHAUSTION_REVERSAL": "FAILED_BREAKOUT",
        "MULTI_TIMEFRAME_ALIGNMENT": "MULTI_TIMEFRAME_CONTINUATION",
        "RANGE_BREAKOUT_WITH_ROOM": "OPENING_RANGE",
        "DIRECTIONAL_PRESSURE_RELEASE": "COMPRESSION_TO_EXPANSION",
        "FAILED_CONTINUATION_REVERSAL": "FAILED_BREAKOUT",
        "PARTICIPATION_DENSITY": "SESSION_TRANSITION",
        "COMPRESSION_TO_EXPANSION": "COMPRESSION_TO_EXPANSION",
    }.get(mechanism, "SESSION_TRANSITION")


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
    provenance = {
        "source_candidate_ids": list(ANCHOR_IDS_0033),
        "source_candidate_count": len(ANCHOR_IDS_0033),
        "raw_event_count_before_calendar_filter": raw_before_calendar_filter,
        "raw_event_count": len(raw),
        "calibration_events_excluded": calendar_excluded,
        "anchors_generated": len(anchors),
        "duplicates_rejected": len(raw) - len(anchors),
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


def _acquire_selected_offer(
    client: Any,
    offer: Mapping[str, Any],
    *,
    root: Path,
    manifest: Mapping[str, Any],
    config: TargetedCostConfig,
) -> dict[str, Any]:
    """Acquire one manifest-bound, crash-resumable bundle of frozen windows."""

    estimate = float(offer["estimated_cost_usd"])
    if estimate > config.maximum_incremental_spend_usd + 1e-9:
        raise SelectiveVetoPilotError("selected 0034 offer exceeds USD 8")
    if config.current_remaining_budget_usd - estimate < config.minimum_budget_reserve_usd - 1e-9:
        raise SelectiveVetoPilotError("selected 0034 offer consumes USD 20 reserve")
    request_core = {
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "estimate_fingerprint": offer["estimate_fingerprint"],
        "schema": offer["schema"],
        "anchor_window_count": offer["anchor_window_count"],
        "window_requests": [row["request"] for row in offer["requests"]],
    }
    request_id = request_id_for(request_core)
    receipt_root = root / "data/cache/databento/selective_veto_0034"
    receipt_path = receipt_root / f"{request_id}_receipt.json"
    intent_path = receipt_root / f"{request_id}_intent.json"
    authorization_path = receipt_root / f"{request_id}_authorization.json"
    if receipt_path.is_file():
        receipt = _read_json(receipt_path)
        if receipt.get("request_id") != request_id or receipt.get("estimate_fingerprint") != offer["estimate_fingerprint"]:
            raise SelectiveVetoPilotError("0034 acquisition receipt drift")
        return receipt

    lock_path = root / "reports/data_access/selective_veto_0034_acquisition.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if receipt_path.is_file():
            return _read_json(receipt_path)
        budget = DatabentoBudgetConfig(
            ledger_path=str(root / "reports/data_budget/databento_spend_ledger.jsonl"),
            summary_path=str(root / "reports/data_budget/databento_budget_summary.md"),
        )
        budget_path = Path(budget.ledger_path)
        access_path = root / "reports/data_access/data_access_ledger.jsonl"

        def ledger_hash(path: Path, absent: str) -> str:
            return _sha256(path) if path.is_file() else stable_hash(absent)

        existing_budget_rows = read_ledger(budget_path)
        actual_rows = [
            row
            for row in existing_budget_rows
            if row.get("request_id") == request_id
            and row.get("download_status") == "DOWNLOADED"
        ]
        if len(actual_rows) > 1:
            raise SelectiveVetoPilotError("0034 request was charged more than once")
        if intent_path.is_file():
            intent = _read_json(intent_path)
            if intent.get("request_id") != request_id:
                raise SelectiveVetoPilotError("0034 acquisition intent identity drift")
            live_cost = float(intent["authorized_cost_usd"])
        else:
            live_cost = float(
                math.fsum(
                    float(client.metadata.get_cost(**row["request"]))
                    for row in offer["requests"]
                )
            )
            _estimated, current_actual = cumulative_spend(budget_path)
            live_remaining = float(budget.hard_cap_usd - current_actual)
            if live_cost > config.maximum_incremental_spend_usd + 1e-9:
                raise SelectiveVetoPilotError("live 0034 cost exceeds USD 8")
            if live_remaining - live_cost < config.minimum_budget_reserve_usd - 1e-9:
                raise SelectiveVetoPilotError("live 0034 cost consumes live USD 20 reserve")
            intent_core = {
                "schema": "hydra_selective_veto_purchase_intent_v1",
                "request_id": request_id,
                "campaign_id": CAMPAIGN_ID,
                "manifest_hash": str(manifest["manifest_hash"]),
                "estimate_fingerprint": str(offer["estimate_fingerprint"]),
                "authorized_cost_usd": live_cost,
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
            intent = _read_json(intent_path)

        if live_cost > config.maximum_incremental_spend_usd + 1e-9:
            raise SelectiveVetoPilotError("authorized 0034 cost exceeds USD 8")
        _estimated, current_actual = cumulative_spend(budget_path)
        outstanding_cost = 0.0 if actual_rows else live_cost
        if budget.hard_cap_usd - current_actual - outstanding_cost < config.minimum_budget_reserve_usd - 1e-9:
            raise SelectiveVetoPilotError("0034 acquisition no longer preserves live USD 20 reserve")

        access_rows = read_ledger(access_path)
        matching_access = [
            row
            for row in access_rows
            if request_id in set(str(value) for value in row.get("candidate_ids") or ())
        ]
        if not matching_access:
            enforce_data_access(
                period=(
                    f"DISJOINT_EVENT_WINDOWS:{min(row['request']['start'] for row in offer['requests'])}:"
                    f"{max(row['request']['end'] for row in offer['requests'])}"
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

        existing_budget_rows = read_ledger(budget_path)
        reserved = any(
            row.get("request_id") == request_id
            and row.get("download_status") == "ESTIMATED_ONLY"
            for row in existing_budget_rows
        )
        if not reserved and not actual_rows:
            projected, current_actual = enforce_budget(budget, live_cost)
            append_spend_record(
                budget,
                DatabentoSpendRecord(
                    request_id=request_id,
                    timestamp_utc=utc_now(),
                    dataset=DATASET,
                    schema=str(offer["schema"]),
                    symbols=sorted({str(row["contract"]) for row in offer["requests"]}),
                    stype_in="raw_symbol",
                    start=min(str(row["request"]["start"]) for row in offer["requests"]),
                    end=max(str(row["request"]["end"]) for row in offer["requests"]),
                    estimated_cost_usd=live_cost,
                    actual_cost_usd=None,
                    cumulative_estimated_spend_usd=projected,
                    cumulative_actual_spend_usd=current_actual,
                    cache_hit=False,
                    research_purpose="0034 frozen anchor-conditioned disjoint selective-veto windows",
                    candidate_tier="SELECTIVE_VETO_LONG_SAMPLE_0034",
                    approval_mode=AUTO_UNDER_HARD_CAP,
                    resulting_file=None,
                    checksum=None,
                    download_status="ESTIMATED_ONLY",
                ),
            )
        authorization_core = {
            "schema": "hydra_selective_veto_download_authorization_v1",
            "request_id": request_id,
            "manifest_hash": str(manifest["manifest_hash"]),
            "intent_fingerprint": str(intent["intent_fingerprint"]),
            "data_access_recorded_before_download": True,
            "budget_reserved_before_download": True,
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
        raw_root = root / "data/cache/databento/selective_veto_0034/raw_dbn"
        raw_root.mkdir(parents=True, exist_ok=True)
        files: list[dict[str, Any]] = []
        for index, row in enumerate(offer["requests"]):
            raw_path = raw_root / f"{request_id}_{index:04d}_{offer['schema']}.dbn.zst"
            if not raw_path.is_file():
                temp = raw_path.with_name(f".{raw_path.name}.{os.getpid()}.tmp")
                temp.unlink(missing_ok=True)
                try:
                    client.timeseries.get_range(**row["request"], stype_out="instrument_id", path=str(temp))
                    if not temp.is_file() or temp.stat().st_size <= 0:
                        raise SelectiveVetoPilotError("Databento returned an empty event-window file")
                    os.replace(temp, raw_path)
                finally:
                    temp.unlink(missing_ok=True)
            files.append(
                {
                    "raw_path": str(raw_path),
                    "raw_sha256": sha256_file(raw_path),
                    "raw_size_bytes": raw_path.stat().st_size,
                    "anchor_ids": list(row["anchor_ids"]),
                    "market": row["market"],
                    "contract": row["contract"],
                    "start": row["request"]["start"],
                    "end": row["request"]["end"],
                }
            )
        bundle_hash = stable_hash(files)
        existing_budget_rows = read_ledger(budget_path)
        actual_rows = [
            row
            for row in existing_budget_rows
            if row.get("request_id") == request_id
            and row.get("download_status") == "DOWNLOADED"
        ]
        if not actual_rows:
            _estimated, cumulative_actual = cumulative_spend(budget_path)
            append_spend_record(budget, DatabentoSpendRecord(
                request_id=request_id,
                timestamp_utc=utc_now(),
                dataset=DATASET,
                schema=str(offer["schema"]),
                symbols=sorted({str(row["contract"]) for row in offer["requests"]}),
                stype_in="raw_symbol",
                start=min(str(row["request"]["start"]) for row in offer["requests"]),
                end=max(str(row["request"]["end"]) for row in offer["requests"]),
                estimated_cost_usd=0.0,
                actual_cost_usd=live_cost,
                cumulative_estimated_spend_usd=_estimated,
                cumulative_actual_spend_usd=cumulative_actual + live_cost,
                cache_hit=False,
                research_purpose="0034 frozen anchor-conditioned disjoint selective-veto windows",
                candidate_tier="SELECTIVE_VETO_LONG_SAMPLE_0034",
                approval_mode=AUTO_UNDER_HARD_CAP,
                resulting_file=str(receipt_path),
                checksum=bundle_hash,
                download_status="DOWNLOADED",
            ))
        core = {
            "schema": "hydra_selective_veto_acquisition_receipt_v1",
            "campaign_id": CAMPAIGN_ID,
            "request_id": request_id,
            "estimate_fingerprint": offer["estimate_fingerprint"],
            "actual_spend_usd": live_cost,
            "files": files,
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
            "budget_ledger_before_sha256": str(
                intent["budget_ledger_before_sha256"]
            ),
            "budget_ledger_after_sha256": ledger_hash(
                budget_path, "ABSENT_BUDGET_LEDGER"
            ),
            "data_access_ledger_before_sha256": str(
                intent["data_access_ledger_before_sha256"]
            ),
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
        core["live_remaining_budget_usd"] = float(
            budget.hard_cap_usd - actual_final
        )
        receipt = {**core, "acquisition_receipt_fingerprint": stable_hash(core)}
        _write_json_once(receipt_path, receipt)
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        return receipt


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
    return np.asarray(index, dtype="datetime64[ns]").astype(np.int64)


def _number_column(frame: Any, names: Sequence[str], default: float = 0.0) -> np.ndarray:
    for name in names:
        if name in frame.columns:
            return np.asarray(frame[name], dtype=float)
    return np.full(len(frame), default, dtype=float)


def _feature_for_anchor(frame: Any, anchor: StructuralAnchor) -> tuple[np.ndarray, str] | None:
    if frame is None or len(frame) == 0:
        return None
    event_ns = _timestamp_ns(frame.index)
    available_ns = (
        _timestamp_ns(frame["ts_recv"])
        if "ts_recv" in frame.columns
        else event_ns
    )
    causal = np.flatnonzero(available_ns <= anchor.decision_time_ns)
    if not len(causal):
        return None
    last = int(causal[-1])
    price = _number_column(frame, ("price",))
    size = _number_column(frame, ("size",))
    sides = np.asarray(frame["side"].astype(str)) if "side" in frame.columns else np.full(len(frame), "N")
    sign = np.where(np.char.upper(sides.astype(str)) == "A", 1.0, np.where(np.char.upper(sides.astype(str)) == "B", -1.0, 0.0))
    trade = np.isfinite(price) & (price > 0.0) & (size > 0.0)
    two = (available_ns >= anchor.decision_time_ns - 2_000_000_000) & (available_ns <= anchor.decision_time_ns) & trade
    thirty = (available_ns >= anchor.decision_time_ns - 30_000_000_000) & (available_ns <= anchor.decision_time_ns) & trade
    flow2 = float(np.sum(sign[two] * size[two]))
    flow30 = float(np.sum(sign[thirty] * size[thirty]))
    bid = _number_column(frame, ("bid_px_00", "bid_price"), math.nan)
    ask = _number_column(frame, ("ask_px_00", "ask_price"), math.nan)
    bid_size = _number_column(frame, ("bid_sz_00", "bid_size"), 0.0)
    ask_size = _number_column(frame, ("ask_sz_00", "ask_size"), 0.0)
    if not (math.isfinite(float(bid[last])) and math.isfinite(float(ask[last])) and ask[last] >= bid[last]):
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
    feature_hash = stable_hash(
        {
            "anchor_event_id": anchor.anchor_event_id,
            "available_at_ns": int(available_ns[last]),
            "decision_time_ns": anchor.decision_time_ns,
            "feature_names": FEATURE_NAMES,
            "values": values.tolist(),
        }
    )
    return values, feature_hash


def _extract_features_from_frame_task(
    task: tuple[Any, tuple[StructuralAnchor, ...]],
) -> list[tuple[str, list[float], str]]:
    """Pure deterministic extraction helper shared by sequential and workers."""

    frame, anchors = task
    output: list[tuple[str, list[float], str]] = []
    for anchor in anchors:
        value = _feature_for_anchor(frame, anchor)
        if value is None:
            continue
        features, feature_hash = value
        output.append((anchor.anchor_event_id, features.tolist(), feature_hash))
    return output


def _extract_features_from_dbn_task(
    task: tuple[str, tuple[StructuralAnchor, ...]],
) -> list[tuple[str, list[float], str]]:
    """Load one immutable DBN file and extract its anchors inside a worker."""

    raw_path, anchors = task
    frame = _dataframe_from_dbn(Path(raw_path))
    return _extract_features_from_frame_task((frame, anchors))


def _load_acquired_features(
    receipt: Mapping[str, Any], anchors: Sequence[StructuralAnchor]
) -> tuple[dict[str, tuple[np.ndarray, str]], bool]:
    by_id = {row.anchor_event_id: row for row in anchors}
    tasks: list[tuple[str, tuple[StructuralAnchor, ...]]] = []
    for item in receipt.get("files") or ():
        task_anchors: list[StructuralAnchor] = []
        for anchor_id in item.get("anchor_ids") or ():
            anchor = by_id.get(str(anchor_id))
            if anchor is None:
                raise SelectiveVetoPilotError(
                    "acquired window references unknown anchor"
                )
            task_anchors.append(anchor)
        tasks.append((str(item["raw_path"]), tuple(task_anchors)))
    if not tasks:
        return {}, False

    _initialize_feature_worker()
    features: dict[str, tuple[np.ndarray, str]] = {}
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
        for anchor_id, values, feature_hash in extracted:
            features[anchor_id] = (np.asarray(values, dtype=float), feature_hash)
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
    """Resolve the frozen 1x/1.5x action into a whole micro quantity."""

    if tier not in {1.0, 1.5}:
        raise SelectiveVetoPilotError("0034 executable risk tier drift")
    return max(1, int(math.floor(anchor.quantity * tier + 0.5)))


def _causal_action_trajectory(
    anchor: StructuralAnchor, scenario: str, tier: float
) -> CausalTradeTrajectory:
    """Reconstruct an integer-sized causal trajectory from immutable 0028 marks."""

    if anchor.outcome_time_ns is None or anchor.raw_exit_price is None:
        raise SelectiveVetoPilotError("future-censored anchor cannot become an account trade")
    quantity = _exact_action_quantity(anchor, tier)
    ratio = quantity / anchor.quantity
    if scenario == "NORMAL":
        fill = anchor.normal_fill_price
        net = float(anchor.normal_net_pnl_usd or 0.0)
        worst = float(anchor.normal_worst_unrealized_pnl_usd or 0.0)
        best = anchor.normal_best_unrealized_pnl_usd
        initial = anchor.normal_initial_unrealized_pnl_usd
        source_marks = anchor.normal_marks
    elif scenario == "STRESSED_1_5X":
        fill = anchor.stressed_fill_price
        net = float(anchor.stressed_net_pnl_usd or 0.0)
        worst = float(anchor.stressed_worst_unrealized_pnl_usd or 0.0)
        best = anchor.stressed_best_unrealized_pnl_usd
        initial = anchor.stressed_initial_unrealized_pnl_usd
        source_marks = anchor.stressed_marks
    else:
        raise SelectiveVetoPilotError("unsupported 0034 cost scenario")
    gross = (
        anchor.direction
        * (float(anchor.raw_exit_price) - float(fill))
        * _point_value(anchor)
        * quantity
    )
    marks = tuple(
        CausalTradeMark(
            availability_time_ns=int(row["availability_time_ns"]),
            worst_unrealized_pnl=float(row["worst_unrealized_pnl"]) * ratio,
            best_unrealized_pnl=float(row["best_unrealized_pnl"]) * ratio,
            current_unrealized_pnl=(
                None
                if row.get("current_unrealized_pnl") is None
                else float(row["current_unrealized_pnl"]) * ratio
            ),
        )
        for row in source_marks
    )
    return CausalTradeTrajectory(
        component_id=anchor.source_candidate_id,
        market=anchor.market,
        side=anchor.direction,
        event=TradePathEvent(
            event_id=f"{anchor.anchor_event_id}:{scenario}:{quantity}",
            # The account is flat until the causal next-tradable-event fill.
            decision_ns=anchor.fill_time_ns,
            exit_ns=anchor.outcome_time_ns,
            session_day=anchor.session_day,
            net_pnl=net * ratio,
            gross_pnl=float(gross),
            worst_unrealized_pnl=worst * ratio,
            best_unrealized_pnl=best * ratio,
            quantity=quantity,
            mini_equivalent=quantity / 10.0,
            regime=anchor.structural_family,
            session_compliant=anchor.session_compliant,
            contract_limit_compliant=anchor.contract_limit_compliant,
            same_bar_ambiguous=anchor.same_bar_ambiguous,
        ),
        marks=marks,
        initial_unrealized_pnl=initial * ratio,
    )


def _scaled_value(value: float | None, quantity: int, tier: float) -> float | None:
    if value is None:
        return None
    scaled_quantity = max(1, int(math.floor(quantity * tier + 0.5)))
    return float(value) * scaled_quantity / quantity


def _outcome_row(anchor: StructuralAnchor, role: str, economic_score: float, action: str, feature_hash: str) -> dict[str, Any]:
    tier = 0.0 if action == "ABSTAIN" else 1.0 if action == "TRADE_1X" else 1.5
    baseline_normal_path = _causal_action_trajectory(anchor, "NORMAL", 1.0)
    baseline_stressed_path = _causal_action_trajectory(anchor, "STRESSED_1_5X", 1.0)
    normal_path = None if tier == 0.0 else _causal_action_trajectory(anchor, "NORMAL", tier)
    stressed_path = None if tier == 0.0 else _causal_action_trajectory(anchor, "STRESSED_1_5X", tier)
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
    baseline_duration = float(
        (anchor.outcome_time_ns - anchor.fill_time_ns) / 1e9
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
        "entry_time_ns": None if tier == 0.0 else anchor.fill_time_ns,
        "exit_time_ns": None if tier == 0.0 else anchor.outcome_time_ns,
        "normal_entry_price": None if tier == 0.0 else anchor.normal_fill_price,
        "stressed_entry_price": None if tier == 0.0 else anchor.stressed_fill_price,
        "exit_price": None if tier == 0.0 else anchor.raw_exit_price,
        "stop_price": anchor.stop_price,
        "target_price": anchor.target_price,
        "same_bar_ambiguous": anchor.same_bar_ambiguous,
        "session_compliant": anchor.session_compliant,
        "contract_limit_compliant": anchor.contract_limit_compliant,
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
    rows: Sequence[Mapping[str, Any]], final_decision: str
) -> list[dict[str, Any]]:
    heldout = sorted(
        (
            row
            for row in rows
            if row["temporal_role"] in {"VALIDATION", "FINAL_DEVELOPMENT"}
        ),
        key=lambda row: (str(row["session_id"]), int(row["decision_time_ns"])),
    )
    sessions = sorted({str(row["session_id"]) for row in heldout})
    checkpoints = sorted({value for value in (5, 10, 15, len(sessions)) if 0 < value <= len(sessions)})
    output: list[dict[str, Any]] = []
    for count in checkpoints:
        included = set(sessions[:count])
        prefix = [row for row in heldout if str(row["session_id"]) in included]
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
            and final_decision == "LONG_SAMPLE_SELECTIVE_OVERLAY_GREEN"
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
        optional_daily_loss_limit=min(3_000.0, float(snapshot["mll"])),
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
                    trajectories[str(row["source_candidate_id"])].append(
                        _causal_action_trajectory(
                            anchor, scenario, float(row["risk_tier"])
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
                    "OFFICIAL_150K_SNAPSHOT"
                    if account_label == "150K"
                    else "VERSIONED_RESEARCH_ACCOUNT_SIZE_SNAPSHOT"
                ),
            },
            "by_role": by_role,
        }
        for scenario, prefix in (("NORMAL", "normal_"), ("STRESSED_1_5X", "")):
            for horizon in (5, 10):
                cell[f"{prefix}p{horizon}"] = _episode_summary(
                    heldout[(scenario, horizon)], float(snapshot["mll"])
                )
        snapshot_payload = cell["rule_snapshot"]
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


def evaluate_long_sample(
    anchors: Sequence[StructuralAnchor],
    receipt: Mapping[str, Any],
    *,
    schema: str,
    frame_loader: Callable[[Path], Any] | None = None,
    eligible_session_days: Sequence[int] | None = None,
) -> dict[str, Any]:
    selected_ids = {value for item in receipt.get("files") or () for value in item.get("anchor_ids") or ()}
    selected = [row for row in anchors if row.anchor_event_id in selected_ids and row.stressed_net_pnl_usd is not None]
    roles = _chronological_roles(selected)
    role_by_id = {anchor_id: role for role, ids in roles.items() for anchor_id in ids}
    worker_path_executed = False
    if frame_loader is None:
        features, worker_path_executed = _load_acquired_features(receipt, selected)
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
                value = _feature_for_anchor(frame, anchor)
                if value is not None:
                    features[anchor.anchor_event_id] = value
    usable = [row for row in selected if row.anchor_event_id in features]
    discovery = [row for row in usable if role_by_id[row.anchor_event_id] == "DISCOVERY"]
    validation = [row for row in usable if role_by_id[row.anchor_event_id] == "VALIDATION"]
    final = [row for row in usable if role_by_id[row.anchor_event_id] == "FINAL_DEVELOPMENT"]
    if min(len(discovery), len(validation), len(final)) == 0:
        return _empty_long("COMPLETE", "LONG_SAMPLE_SELECTIVE_OVERLAY_FALSIFIED") | {
            "policy_frozen_before_final_development": True,
            "reason": "NO_CAUSAL_FEATURE_ROWS_IN_ONE_OR_MORE_TEMPORAL_ROLES",
            "role_counts": {"DISCOVERY": len(discovery), "VALIDATION": len(validation), "FINAL_DEVELOPMENT": len(final)},
            "feature_extraction_runtime": {
                "process_pool_executed": worker_path_executed,
                "cpu_worker_count": 2 if worker_path_executed else 0,
            },
        }
    x_discovery = np.vstack([features[row.anchor_event_id][0] for row in discovery])
    discovery_net = np.asarray(
        [float(row.stressed_net_pnl_usd or 0.0) for row in discovery], dtype=float
    )
    discovery_loss = np.asarray(discovery_net < 0.0, dtype=float)
    discovery_mae = np.asarray(
        [-min(float(row.stressed_worst_unrealized_pnl_usd or 0.0), 0.0) for row in discovery],
        dtype=float,
    )
    discovery_cost = np.asarray(
        [
            _causal_action_trajectory(row, "STRESSED_1_5X", 1.0).event.gross_pnl
            - float(row.stressed_net_pnl_usd or 0.0)
            for row in discovery
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
    matrix = _account_matrix(
        all_rows, {row.anchor_event_id: row for row in usable}, calendar
    )
    heldout = [row for row in all_rows if row["temporal_role"] in {"VALIDATION", "FINAL_DEVELOPMENT"}]
    positive_contexts = max(
        sum(_summarize_paired([row for row in heldout if row["structural_family"] == family])["stressed_net_usd"] > 0.0 for family in {row["structural_family"] for row in heldout}),
        sum(_summarize_paired([row for row in heldout if row["session_id"] == session])["stressed_net_usd"] > 0.0 for session in {row["session_id"] for row in heldout}),
    )
    no_mll = all(float(cell[horizon]["mll_breach_rate"]) <= 0.10 for cell in matrix for horizon in ("p5", "p10"))
    any_pass = any(int(cell[horizon]["pass_count"]) > 0 for cell in matrix for horizon in ("p5", "p10"))
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
        and any_pass
    )
    weak = (
        float(summaries["VALIDATION"]["paired_stressed_uplift_usd"]) > 0.0
        or float(summaries["FINAL_DEVELOPMENT"]["paired_stressed_uplift_usd"]) > 0.0
    )
    decision = "LONG_SAMPLE_SELECTIVE_OVERLAY_GREEN" if green else "LONG_SAMPLE_SELECTIVE_OVERLAY_WEAK" if weak else "LONG_SAMPLE_SELECTIVE_OVERLAY_FALSIFIED"
    viable = [cell for cell in matrix if cell["p5"]["pass_count"] or cell["p10"]["pass_count"]]
    fastest = max(viable, key=lambda cell: (cell["p5"]["pass_rate"], cell["p10"]["pass_rate"], cell["p5"]["minimum_mll_buffer_usd"]), default=None)
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
        "fastest_viable_account_size": None if fastest is None else fastest["account_label"],
        "causal_feature_row_count": len(usable),
        "role_counts": {role: sum(row["temporal_role"] == role for row in all_rows) for role in ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")},
        "positive_context_count": positive_contexts,
        "single_trade_domination_fraction": _summarize_paired(heldout)["maximum_positive_trade_fraction"],
        "mll_within_tolerance": no_mll,
        "consistency_within_tolerance": consistency,
        "heldout_trade_coverage": heldout_coverage,
        "green_requires_actual_stressed_p5_or_p10_pass": True,
        "material_stressed_target_progress_uplift_minimum": (
            MATERIAL_STRESSED_TARGET_PROGRESS_UPLIFT_MINIMUM
        ),
        "sequential_checkpoints": _sequential_evidence_checkpoints(
            all_rows, decision
        ),
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
