"""Databento metadata-only cost planning for HYDRA campaign 0031.

The module has deliberately no download capability.  It prices explicit raw
contracts, freezes chronological roles before acquisition, and returns a
bounded multi-request plan.  A separate manifest-bound acquisition command is
the only component allowed to spend from the data ledger.
"""

from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Mapping, Protocol, Sequence


DATASET = "GLBX.MDP3"
STYPE_IN = "raw_symbol"
Q4_START_UTC = "2024-10-01T00:00:00Z"
SCHEMAS = ("trades", "tbbo", "mbp-1", "mbp-10", "mbo")
SESSION_COUNTS = (5, 10, 20, 30)
DEFAULT_SELECTED_MARKETS = (("NQ", "NQU4"), ("YM", "YMU4"))
MAXIMUM_INITIAL_SPEND_USD = 10.0
MINIMUM_BUDGET_RESERVE_USD = 25.0

_RAW_CONTRACT = re.compile(r"^[A-Z]{1,4}[FGHJKMNQUVXZ][0-9]{1,2}$")


class FoundryCostError(RuntimeError):
    """The 0031 metadata matrix or bounded acquisition plan is invalid."""


class MetadataAPI(Protocol):
    def get_record_count(self, **kwargs: Any) -> int: ...

    def get_billable_size(self, **kwargs: Any) -> int: ...

    def get_cost(self, **kwargs: Any) -> float: ...


@dataclass(frozen=True, slots=True)
class ChronologicalRoles:
    discovery: tuple[str, ...]
    validation: tuple[str, ...]
    final_development: tuple[str, ...]

    @property
    def session_count(self) -> int:
        return len(self.discovery) + len(self.validation) + len(self.final_development)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CostWindow:
    session_count: int
    start: str
    end: str
    roles: ChronologicalRoles

    def __post_init__(self) -> None:
        if self.session_count not in SESSION_COUNTS:
            raise FoundryCostError("unsupported session count")
        if self.roles.session_count != self.session_count:
            raise FoundryCostError("chronological role coverage is incomplete")
        if _utc(self.start) >= _utc(self.end) or _utc(self.end) > _utc(Q4_START_UTC):
            raise FoundryCostError("cost window is invalid or enters protected Q4")
        ordered = (
            self.roles.discovery
            + self.roles.validation
            + self.roles.final_development
        )
        if len(set(ordered)) != self.session_count or ordered != tuple(sorted(ordered)):
            raise FoundryCostError("chronological roles overlap or are not ordered")


@dataclass(frozen=True, slots=True)
class CostRow:
    dataset: str
    market: str
    symbols: tuple[str, ...]
    schema: str
    session_count: int
    start: str
    end: str
    stype_in: str
    estimated_records: int
    estimated_bytes: int
    estimated_cost_usd: float

    def __post_init__(self) -> None:
        if self.dataset != DATASET or self.stype_in != STYPE_IN:
            raise FoundryCostError("Databento request identity drift")
        if self.schema not in SCHEMAS or self.session_count not in SESSION_COUNTS:
            raise FoundryCostError("schema/session drift")
        if not self.symbols or any(not _RAW_CONTRACT.fullmatch(v) for v in self.symbols):
            raise FoundryCostError("explicit raw contracts are required")
        if _utc(self.end) > _utc(Q4_START_UTC):
            raise FoundryCostError("request enters protected Q4")
        if self.estimated_records <= 0 or self.estimated_bytes <= 0:
            raise FoundryCostError("metadata estimate is empty")
        if not math.isfinite(self.estimated_cost_usd) or self.estimated_cost_usd < 0:
            raise FoundryCostError("metadata cost is invalid")

    def request(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "symbols": list(self.symbols),
            "schema": self.schema,
            "stype_in": self.stype_in,
            "start": self.start,
            "end": self.end,
        }

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CostMatrix:
    markets: tuple[tuple[str, str], ...]
    rows: tuple[CostRow, ...]
    combined_rows: tuple[CostRow, ...]

    def __post_init__(self) -> None:
        expected = len(self.markets) * len(SCHEMAS) * len(SESSION_COUNTS)
        if len(self.rows) != expected:
            raise FoundryCostError("per-market cost matrix is incomplete")
        if len(self.combined_rows) != len(SCHEMAS) * len(SESSION_COUNTS):
            raise FoundryCostError("combined-market cost matrix is incomplete")

    def to_dict(self) -> dict[str, Any]:
        return {
            "markets": [list(value) for value in self.markets],
            "rows": [row.to_dict() for row in self.rows],
            "combined_rows": [row.to_dict() for row in self.combined_rows],
        }


@dataclass(frozen=True, slots=True)
class AcquisitionPlan:
    requests: tuple[CostRow, ...]
    roles_by_request: tuple[ChronologicalRoles, ...]
    strategy: str
    projected_incremental_spend_usd: float
    projected_cumulative_spend_usd: float
    projected_remaining_usd: float
    maximum_initial_spend_usd: float = MAXIMUM_INITIAL_SPEND_USD
    minimum_budget_reserve_usd: float = MINIMUM_BUDGET_RESERVE_USD
    purchase_authorized: bool = False

    def __post_init__(self) -> None:
        if not self.requests or len(self.requests) != len(self.roles_by_request):
            raise FoundryCostError("acquisition plan is incomplete")
        if self.projected_incremental_spend_usd > self.maximum_initial_spend_usd + 1e-9:
            raise FoundryCostError("acquisition plan exceeds the initial cap")
        if self.projected_remaining_usd < self.minimum_budget_reserve_usd - 1e-9:
            raise FoundryCostError("acquisition plan consumes the protected reserve")
        if self.purchase_authorized:
            raise FoundryCostError("metadata planning cannot authorize a purchase")

    def to_dict(self) -> dict[str, Any]:
        return {
            "requests": [row.to_dict() for row in self.requests],
            "roles_by_request": [role.to_dict() for role in self.roles_by_request],
            "strategy": self.strategy,
            "projected_incremental_spend_usd": self.projected_incremental_spend_usd,
            "projected_cumulative_spend_usd": self.projected_cumulative_spend_usd,
            "projected_remaining_usd": self.projected_remaining_usd,
            "maximum_initial_spend_usd": self.maximum_initial_spend_usd,
            "minimum_budget_reserve_usd": self.minimum_budget_reserve_usd,
            "purchase_authorized": False,
        }


def frozen_cost_windows() -> tuple[CostWindow, ...]:
    """Return preregistered 5/10/20/30-session pre-Q4 windows and 60/20/20 roles."""

    sessions = _weekdays(date(2024, 7, 8), 30)
    ends = {
        5: "2024-07-12T21:00:00Z",
        10: "2024-07-19T21:00:00Z",
        20: "2024-08-02T21:00:00Z",
        30: "2024-08-16T21:00:00Z",
    }
    counts = {5: (3, 1), 10: (6, 2), 20: (12, 4), 30: (18, 6)}
    return tuple(
        _window(sessions[:count], ends[count], *counts[count])
        for count in SESSION_COUNTS
    )


def generate_cost_matrix(
    metadata: MetadataAPI,
    *,
    markets: Sequence[tuple[str, str]] = DEFAULT_SELECTED_MARKETS,
) -> CostMatrix:
    """Ask the official metadata endpoint for every frozen request cell."""

    selected = tuple((str(m), str(s)) for m, s in markets)
    if len(selected) != 2 or len({m for m, _ in selected}) != 2:
        raise FoundryCostError("exactly two distinct markets must be priced")
    if any(not _RAW_CONTRACT.fullmatch(symbol) for _, symbol in selected):
        raise FoundryCostError("invalid explicit contract in market selection")
    windows = frozen_cost_windows()
    rows = tuple(
        _estimate(metadata, market=market, symbols=(symbol,), schema=schema, window=window)
        for market, symbol in selected
        for schema in SCHEMAS
        for window in windows
    )
    combined = tuple(
        _estimate(
            metadata,
            market="+".join(m for m, _ in selected),
            symbols=tuple(s for _, s in selected),
            schema=schema,
            window=window,
        )
        for schema in SCHEMAS
        for window in windows
    )
    return CostMatrix(markets=selected, rows=rows, combined_rows=combined)


def select_bounded_acquisition_plan(
    matrix: CostMatrix,
    *,
    actual_spend_usd: float,
    total_budget_usd: float,
) -> AcquisitionPlan:
    """Choose MBO for both markets when possible, else a broad+teacher bundle.

    Ranking is frozen and outcome-independent.  A direct two-market MBO sample
    dominates because all lower schemas can be deterministically derived.  If
    no such sample fits, the planner maximises a two-market TBBO/MBP-1 sample
    plus the longest affordable one-market MBO calibration request.
    """

    if actual_spend_usd < 0 or total_budget_usd <= actual_spend_usd:
        raise FoundryCostError("budget state is invalid")
    budget = min(
        MAXIMUM_INITIAL_SPEND_USD,
        total_budget_usd - actual_spend_usd - MINIMUM_BUDGET_RESERVE_USD,
    )
    if budget < 0:
        raise FoundryCostError("protected reserve leaves no acquisition budget")
    windows = {value.session_count: value.roles for value in frozen_cost_windows()}
    direct = [
        row for row in matrix.combined_rows
        if row.schema == "mbo" and row.estimated_cost_usd <= budget + 1e-9
    ]
    if direct:
        selected = max(direct, key=lambda row: (row.session_count, -row.estimated_cost_usd))
        return _plan(
            (selected,), (windows[selected.session_count],),
            "TWO_MARKET_MBO_DERIVE_LOWER_SCHEMAS",
            actual_spend_usd, total_budget_usd,
        )

    broad = [
        row for row in matrix.combined_rows
        if row.schema in {"tbbo", "mbp-1"} and row.estimated_cost_usd <= budget + 1e-9
    ]
    teacher = [
        row for row in matrix.rows
        if row.schema == "mbo" and row.estimated_cost_usd <= budget + 1e-9
    ]
    combinations: list[tuple[CostRow, CostRow]] = []
    for primary in broad:
        for calibration in teacher:
            if (
                primary.estimated_cost_usd + calibration.estimated_cost_usd
                <= budget + 1e-9
            ):
                combinations.append((primary, calibration))
    if combinations:
        schema_rank = {"mbp-1": 2, "tbbo": 1}
        primary, calibration = max(
            combinations,
            key=lambda pair: (
                pair[0].session_count,
                schema_rank[pair[0].schema],
                pair[1].session_count,
                pair[1].market == matrix.markets[0][0],
                -(pair[0].estimated_cost_usd + pair[1].estimated_cost_usd),
            ),
        )
        return _plan(
            (primary, calibration),
            (windows[primary.session_count], windows[calibration.session_count]),
            "TWO_MARKET_DEPLOYABLE_PLUS_ONE_MARKET_MBO_TEACHER",
            actual_spend_usd, total_budget_usd,
        )

    # A deployable two-market pilot is still useful when no teacher sample can
    # fit.  It is explicitly classified without MBO and cannot emit teacher
    # claims; this is preferable to silently exceeding the budget.
    if broad:
        schema_rank = {"mbp-1": 2, "tbbo": 1}
        selected = max(
            broad,
            key=lambda row: (
                row.session_count,
                schema_rank[row.schema],
                -row.estimated_cost_usd,
            ),
        )
        return _plan(
            (selected,), (windows[selected.session_count],),
            "TWO_MARKET_DEPLOYABLE_NO_MBO_TEACHER_WITHIN_CAP",
            actual_spend_usd, total_budget_usd,
        )
    raise FoundryCostError("no useful two-market sample fits cap and reserve")


def _plan(
    requests: tuple[CostRow, ...],
    roles: tuple[ChronologicalRoles, ...],
    strategy: str,
    actual_spend: float,
    total_budget: float,
) -> AcquisitionPlan:
    incremental = math.fsum(row.estimated_cost_usd for row in requests)
    cumulative = actual_spend + incremental
    return AcquisitionPlan(
        requests=requests,
        roles_by_request=roles,
        strategy=strategy,
        projected_incremental_spend_usd=incremental,
        projected_cumulative_spend_usd=cumulative,
        projected_remaining_usd=total_budget - cumulative,
    )


def _estimate(
    metadata: MetadataAPI,
    *,
    market: str,
    symbols: tuple[str, ...],
    schema: str,
    window: CostWindow,
) -> CostRow:
    kwargs = {
        "dataset": DATASET,
        "symbols": list(symbols),
        "schema": schema,
        "stype_in": STYPE_IN,
        "start": window.start,
        "end": window.end,
    }
    return CostRow(
        dataset=DATASET,
        market=market,
        symbols=symbols,
        schema=schema,
        session_count=window.session_count,
        start=window.start,
        end=window.end,
        stype_in=STYPE_IN,
        estimated_records=int(metadata.get_record_count(**kwargs)),
        estimated_bytes=int(metadata.get_billable_size(**kwargs)),
        estimated_cost_usd=float(metadata.get_cost(**kwargs)),
    )


def _window(
    sessions: tuple[str, ...], end: str, discovery_count: int, validation_count: int
) -> CostWindow:
    roles = ChronologicalRoles(
        discovery=sessions[:discovery_count],
        validation=sessions[discovery_count : discovery_count + validation_count],
        final_development=sessions[discovery_count + validation_count :],
    )
    return CostWindow(
        session_count=len(sessions),
        start="2024-07-07T22:00:00Z",
        end=end,
        roles=roles,
    )


def _weekdays(start: date, count: int) -> tuple[str, ...]:
    values: list[str] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(current.isoformat())
        current = date.fromordinal(current.toordinal() + 1)
    return tuple(values)


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise FoundryCostError("timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)
