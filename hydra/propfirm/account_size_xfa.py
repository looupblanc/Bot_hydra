"""Account-size-aware, causal XFA lifecycle replay.

This module is the narrow executable adapter between a frozen, successful
Combine path and the official 50K/100K/150K XFA rules captured on 2026-07-19.
It intentionally does *not* mutate the Combine graduation receipt.  Instead a
separate :class:`FrozenAccountSizeXfaHandoff` binds the candidate, Combine
book, XFA book, risk profile and official rule hash before funded outcomes are
read.

The Standard and Consistency paths are alternative counterfactual account
choices.  They are always replayed separately and this API never publishes a
summed value across them.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.combine_to_xfa import XfaTerminal
from hydra.propfirm.mll_variants import advance_end_of_day_floor
from hydra.research.causal_sleeve_replay import CausalTradeTrajectory


ACCOUNT_SIZE_XFA_ENGINE_VERSION = "hydra_account_size_xfa_causal_v1"
ACCOUNT_SIZE_XFA_HANDOFF_SCHEMA = "hydra_account_size_xfa_handoff_v1"
ACCOUNT_SIZE_XFA_RESULT_SCHEMA = "hydra_account_size_xfa_alternatives_v1"
SUPPORTED_ACCOUNT_LABELS = ("50K", "100K", "150K")
SUPPORTED_PATHS = ("STANDARD", "CONSISTENCY")
DEFAULT_RULE_SNAPSHOT = Path("config/rulesets/topstep_official_2026-07-19.json")


class AccountSizeXfaError(RuntimeError):
    """The frozen handoff or causal XFA replay cannot be reconciled safely."""


@dataclass(frozen=True, slots=True)
class AccountSizeXfaRules:
    """Executable account-size projection of the authoritative JSON snapshot."""

    snapshot_id: str
    snapshot_file_sha256: str
    parsed_rule_hash: str
    account_label: str
    account_size_usd: int
    maximum_loss_limit: float
    combine_maximum_mini_equivalent: int
    xfa_starting_balance: float
    xfa_starting_floor: float
    xfa_standard_winning_days: int
    xfa_standard_winning_day_minimum: float
    later_standard_cycle_minimum_profit: float
    payout_fraction: float
    minimum_payout: float
    standard_payout_cap: float
    xfa_consistency_traded_days: int
    xfa_consistency_limit: float
    consistency_payout_cap: float
    trader_profit_split: float
    mll_floor_after_first_payout: float
    xfa_scaling_tiers: tuple[tuple[float, float], ...]
    special_contract_caps_mini_equivalent: tuple[tuple[str, float], ...]
    inactivity_calendar_days: int = 30
    engine_version: str = ACCOUNT_SIZE_XFA_ENGINE_VERSION

    def __post_init__(self) -> None:
        if self.account_label not in SUPPORTED_ACCOUNT_LABELS:
            raise AccountSizeXfaError("unsupported XFA account label")
        if self.account_size_usd != int(self.account_label[:-1]) * 1_000:
            raise AccountSizeXfaError("account label and size disagree")
        if self.xfa_starting_balance != 0.0:
            raise AccountSizeXfaError("XFA must start from a fresh zero balance")
        if self.xfa_starting_floor != -self.maximum_loss_limit:
            raise AccountSizeXfaError("XFA starting floor does not match the MLL")
        if self.mll_floor_after_first_payout != 0.0:
            raise AccountSizeXfaError("post-payout MLL floor must lock at zero")
        if self.trader_profit_split != 0.90:
            raise AccountSizeXfaError("official snapshot must retain the 90/10 split")
        if not self.xfa_scaling_tiers or self.xfa_scaling_tiers[0][0] != 0.0:
            raise AccountSizeXfaError("XFA scaling plan is incomplete")
        if any(
            right[0] <= left[0]
            for left, right in zip(self.xfa_scaling_tiers, self.xfa_scaling_tiers[1:])
        ):
            raise AccountSizeXfaError("XFA scaling thresholds are not increasing")
        if self.xfa_scaling_tiers[-1][1] != self.combine_maximum_mini_equivalent:
            raise AccountSizeXfaError("XFA scaling plan does not reach the account cap")
        if self.engine_version != ACCOUNT_SIZE_XFA_ENGINE_VERSION:
            raise AccountSizeXfaError("account-size XFA engine version drift")

    @property
    def fingerprint(self) -> str:
        return stable_hash(self._payload())

    def _payload(self) -> dict[str, Any]:
        row = asdict(self)
        row["xfa_scaling_tiers"] = [list(value) for value in self.xfa_scaling_tiers]
        row["special_contract_caps_mini_equivalent"] = [
            list(value) for value in self.special_contract_caps_mini_equivalent
        ]
        return row

    def to_dict(self) -> dict[str, Any]:
        return {**self._payload(), "fingerprint": self.fingerprint}

    def session_limit(self, opening_balance: float, market: str | None = None) -> float:
        """Return the limit frozen at session open, in mini-equivalents."""

        limit = float(self.xfa_scaling_tiers[0][1])
        for threshold, value in self.xfa_scaling_tiers:
            if opening_balance + 1e-12 < threshold:
                break
            limit = float(value)
        if market is not None:
            symbol = _market_symbol(market)
            cap = dict(self.special_contract_caps_mini_equivalent).get(symbol)
            if cap is not None:
                limit = min(limit, float(cap))
        return max(0.0, limit)


@dataclass(frozen=True, slots=True)
class FrozenAccountSizeXfaBook:
    """Immutable funded-book membership, separate from the Combine receipt."""

    candidate_id: str
    combine_book_hash: str
    account_label: str
    component_priority: tuple[str, ...]
    maximum_simultaneous_positions: int
    same_market_exclusive: bool
    book_version: str = ACCOUNT_SIZE_XFA_ENGINE_VERSION

    def __post_init__(self) -> None:
        if not self.candidate_id.strip() or not _is_sha256(self.combine_book_hash):
            raise AccountSizeXfaError("XFA book has invalid candidate/Combine binding")
        if self.account_label not in SUPPORTED_ACCOUNT_LABELS:
            raise AccountSizeXfaError("XFA book account label is unsupported")
        if not self.component_priority or len(set(self.component_priority)) != len(
            self.component_priority
        ):
            raise AccountSizeXfaError("XFA book component priority is not frozen")
        if not 1 <= self.maximum_simultaneous_positions <= len(
            self.component_priority
        ):
            raise AccountSizeXfaError("XFA book concurrency is outside membership")
        if self.book_version != ACCOUNT_SIZE_XFA_ENGINE_VERSION:
            raise AccountSizeXfaError("XFA book version drift")

    @property
    def fingerprint(self) -> str:
        return stable_hash(asdict(self))

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "component_priority": list(self.component_priority), "fingerprint": self.fingerprint}


@dataclass(frozen=True, slots=True)
class FrozenAccountSizeXfaProfile:
    """Small pre-outcome static XFA risk and payout declaration."""

    profile_id: str
    account_label: str
    official_rule_hash: str
    risk_multiplier: float
    maximum_mini_equivalent: float
    payout_paths: tuple[str, str] = SUPPORTED_PATHS
    payout_request_policy: str = "EARLIEST_ELIGIBLE_END_OF_DAY"
    clip_to_session_scaling_plan: bool = True
    outbound_order_capability: bool = False
    profile_version: str = ACCOUNT_SIZE_XFA_ENGINE_VERSION

    def __post_init__(self) -> None:
        if not self.profile_id.strip() or self.account_label not in SUPPORTED_ACCOUNT_LABELS:
            raise AccountSizeXfaError("XFA profile identity is incomplete")
        if not _is_sha256(self.official_rule_hash):
            raise AccountSizeXfaError("XFA profile rule binding is invalid")
        if not math.isfinite(self.risk_multiplier) or self.risk_multiplier <= 0.0:
            raise AccountSizeXfaError("XFA risk multiplier must be positive")
        if not math.isfinite(self.maximum_mini_equivalent) or not (
            0.0 < self.maximum_mini_equivalent <= 15.0
        ):
            raise AccountSizeXfaError("XFA mini-equivalent limit is invalid")
        if tuple(self.payout_paths) != SUPPORTED_PATHS:
            raise AccountSizeXfaError("Standard and Consistency must stay separate")
        if self.payout_request_policy != "EARLIEST_ELIGIBLE_END_OF_DAY":
            raise AccountSizeXfaError("unsupported XFA payout request policy")
        if not self.clip_to_session_scaling_plan:
            raise AccountSizeXfaError("XFA profile must enforce the scaling plan")
        if self.outbound_order_capability:
            raise AccountSizeXfaError("research XFA profile cannot submit orders")
        if self.profile_version != ACCOUNT_SIZE_XFA_ENGINE_VERSION:
            raise AccountSizeXfaError("XFA profile version drift")

    @property
    def fingerprint(self) -> str:
        return stable_hash(self._payload())

    def _payload(self) -> dict[str, Any]:
        row = asdict(self)
        row["payout_paths"] = list(self.payout_paths)
        return row

    def to_dict(self) -> dict[str, Any]:
        return {**self._payload(), "fingerprint": self.fingerprint}


@dataclass(frozen=True, slots=True)
class FrozenAccountSizeXfaHandoff:
    schema: str
    book: FrozenAccountSizeXfaBook
    profile: FrozenAccountSizeXfaProfile
    rule_snapshot_fingerprint: str
    frozen_before_xfa_outcomes: bool
    handoff_hash: str

    def __post_init__(self) -> None:
        if self.schema != ACCOUNT_SIZE_XFA_HANDOFF_SCHEMA:
            raise AccountSizeXfaError("XFA handoff schema drift")
        if self.book.account_label != self.profile.account_label:
            raise AccountSizeXfaError("XFA book/profile account mismatch")
        if self.rule_snapshot_fingerprint != self.profile.official_rule_hash:
            raise AccountSizeXfaError("XFA handoff/profile rule hash mismatch")
        if not self.frozen_before_xfa_outcomes:
            raise AccountSizeXfaError("XFA handoff must be frozen before outcomes")
        if self.handoff_hash != stable_hash(self._payload()):
            raise AccountSizeXfaError("XFA handoff hash drift")

    def _payload(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "book": self.book.to_dict(),
            "profile": self.profile.to_dict(),
            "rule_snapshot_fingerprint": self.rule_snapshot_fingerprint,
            "frozen_before_xfa_outcomes": self.frozen_before_xfa_outcomes,
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._payload(), "handoff_hash": self.handoff_hash}


@dataclass(frozen=True, slots=True)
class AccountSizeXfaPathResult:
    path: str
    terminal: XfaTerminal
    terminal_reason: str
    start_day: int
    end_day: int
    requested_horizon_days: int
    observed_days: int
    traded_days: int
    accepted_event_count: int
    skipped_event_count: int
    payout_cycles: int
    first_payout_day: int | None
    gross_payout: float
    trader_net_payout: float
    ending_balance: float
    ending_mll_floor: float
    minimum_mll_buffer: float
    post_payout_survived: bool
    qualifying_winning_days: int
    maximum_consistency_ratio: float
    maximum_mini_equivalent: float
    skipped_reasons: Mapping[str, int]
    component_contribution: Mapping[str, float]
    daily_ledger: tuple[Mapping[str, Any], ...]
    path_hash: str

    @property
    def first_payout_count(self) -> int:
        return int(self.first_payout_day is not None)

    def _payload(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "terminal": self.terminal.value,
            "terminal_reason": self.terminal_reason,
            "start_day": self.start_day,
            "end_day": self.end_day,
            "requested_horizon_days": self.requested_horizon_days,
            "observed_days": self.observed_days,
            "traded_days": self.traded_days,
            "accepted_event_count": self.accepted_event_count,
            "skipped_event_count": self.skipped_event_count,
            "payout_cycles": self.payout_cycles,
            "first_payout_day": self.first_payout_day,
            "first_payout_count": self.first_payout_count,
            "gross_payout": self.gross_payout,
            "trader_net_payout": self.trader_net_payout,
            "ending_balance": self.ending_balance,
            "ending_mll_floor": self.ending_mll_floor,
            "minimum_mll_buffer": self.minimum_mll_buffer,
            "post_payout_survived": self.post_payout_survived,
            "qualifying_winning_days": self.qualifying_winning_days,
            "maximum_consistency_ratio": self.maximum_consistency_ratio,
            "maximum_mini_equivalent": self.maximum_mini_equivalent,
            "skipped_reasons": dict(sorted(self.skipped_reasons.items())),
            "component_contribution": dict(sorted(self.component_contribution.items())),
            "daily_ledger": [dict(row) for row in self.daily_ledger],
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._payload(), "path_hash": self.path_hash}


@dataclass(frozen=True, slots=True)
class AccountSizeXfaAlternativesResult:
    schema: str
    engine_version: str
    transition_id: str
    combine_path_hash: str
    source_trajectory_hash: str
    handoff: FrozenAccountSizeXfaHandoff
    rules: AccountSizeXfaRules
    standard: AccountSizeXfaPathResult
    consistency: AccountSizeXfaPathResult
    result_hash: str

    def __post_init__(self) -> None:
        if self.schema != ACCOUNT_SIZE_XFA_RESULT_SCHEMA:
            raise AccountSizeXfaError("XFA result schema drift")
        if self.engine_version != ACCOUNT_SIZE_XFA_ENGINE_VERSION:
            raise AccountSizeXfaError("XFA result engine version drift")
        if not self.transition_id.strip() or not _is_sha256(self.combine_path_hash):
            raise AccountSizeXfaError("XFA transition binding is invalid")
        if self.standard.path != "XFA_STANDARD" or self.consistency.path != "XFA_CONSISTENCY":
            raise AccountSizeXfaError("XFA alternatives were mixed")
        if self.rules.fingerprint != self.handoff.rule_snapshot_fingerprint:
            raise AccountSizeXfaError("XFA result used a different rule snapshot")
        if self.result_hash != stable_hash(self._payload()):
            raise AccountSizeXfaError("XFA alternatives result hash drift")

    def _payload(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "engine_version": self.engine_version,
            "transition_id": self.transition_id,
            "combine_path_hash": self.combine_path_hash,
            "source_trajectory_hash": self.source_trajectory_hash,
            "handoff": self.handoff.to_dict(),
            "rules": self.rules.to_dict(),
            "alternatives": {
                "STANDARD": self.standard.to_dict(),
                "CONSISTENCY": self.consistency.to_dict(),
            },
            "standard_and_consistency_are_alternatives": True,
            "sum_standard_and_consistency_ev_allowed": False,
            "selected_path": None,
            "broker_connection_count": 0,
            "order_count": 0,
            "outbound_order_capability": False,
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._payload(), "result_hash": self.result_hash}


@dataclass(slots=True)
class _OpenPosition:
    trajectory: CausalTradeTrajectory
    quantity: int
    mini_equivalent: float
    ratio: float
    current_unrealized: float
    current_worst: float


def load_account_size_xfa_rules(
    account_label: str,
    *,
    snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> AccountSizeXfaRules:
    """Load and hash-check one account projection from the official snapshot."""

    path = Path(snapshot_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != "hydra_topstep_official_rule_snapshot_v1":
        raise AccountSizeXfaError("official rule snapshot schema drift")
    parsed_fields = tuple(str(value) for value in payload.get("parsed_rule_fields", ()))
    parsed = {key: payload[key] for key in parsed_fields}
    if stable_hash(parsed) != str(payload.get("parsed_rule_hash") or ""):
        raise AccountSizeXfaError("official parsed rule hash does not reconcile")
    if account_label not in SUPPORTED_ACCOUNT_LABELS:
        raise AccountSizeXfaError("unsupported account-size request")
    combine = dict(payload["combine"][account_label])
    xfa = dict(payload["xfa"])
    standard = dict(xfa["standard"])
    consistency = dict(xfa["consistency"])
    account_size = int(combine["account_size_usd"])
    mll = float(combine["maximum_loss_limit_usd"])
    raw_caps = dict(payload["product_restrictions"]["special_contract_caps"])
    caps: dict[str, float] = {}
    for group, by_account in raw_caps.items():
        raw_cap = float(by_account[account_label])
        for symbol in group.split("_"):
            caps[symbol] = raw_cap / 10.0 if symbol.startswith("M") else raw_cap
    tiers = tuple(
        (float(threshold), float(limit))
        for threshold, limit in xfa["scaling_plan_mini_contracts"][account_label]
    )
    return AccountSizeXfaRules(
        snapshot_id=str(payload["snapshot_id"]),
        snapshot_file_sha256=_file_sha256(path),
        parsed_rule_hash=str(payload["parsed_rule_hash"]),
        account_label=account_label,
        account_size_usd=account_size,
        maximum_loss_limit=mll,
        combine_maximum_mini_equivalent=int(combine["maximum_mini_contracts"]),
        xfa_starting_balance=float(xfa["starting_balance_usd"]),
        xfa_starting_floor=float(xfa["starting_mll_by_account_usd"][account_label]),
        xfa_standard_winning_days=int(standard["winning_days_required"]),
        xfa_standard_winning_day_minimum=float(standard["winning_day_minimum_usd"]),
        later_standard_cycle_minimum_profit=float(standard["later_cycle_profit_minimum_usd"]),
        payout_fraction=float(standard["payout_balance_fraction_cap"]),
        minimum_payout=float(standard["payout_request_minimum_usd"]),
        standard_payout_cap=float(standard["payout_cap_by_account_usd"][account_label]),
        xfa_consistency_traded_days=int(consistency["minimum_traded_days"]),
        xfa_consistency_limit=float(consistency["largest_day_fraction"]),
        consistency_payout_cap=float(consistency["payout_cap_by_account_usd"][account_label]),
        trader_profit_split=float(xfa["profit_split_trader_fraction"]),
        mll_floor_after_first_payout=float(xfa["mll_floor_after_first_payout_usd"]),
        xfa_scaling_tiers=tiers,
        special_contract_caps_mini_equivalent=tuple(sorted(caps.items())),
    )


def freeze_account_size_xfa_handoff(
    *,
    candidate_id: str,
    combine_book_hash: str,
    component_priority: Sequence[str],
    rules: AccountSizeXfaRules,
    risk_multiplier: float = 1.0,
    maximum_simultaneous_positions: int = 1,
    maximum_mini_equivalent: float | None = None,
    same_market_exclusive: bool = True,
    profile_id: str | None = None,
) -> FrozenAccountSizeXfaHandoff:
    """Freeze the funded handoff without editing the Tier-G receipt."""

    components = tuple(str(value) for value in component_priority)
    book = FrozenAccountSizeXfaBook(
        candidate_id=str(candidate_id),
        combine_book_hash=str(combine_book_hash),
        account_label=rules.account_label,
        component_priority=components,
        maximum_simultaneous_positions=int(maximum_simultaneous_positions),
        same_market_exclusive=bool(same_market_exclusive),
    )
    profile = FrozenAccountSizeXfaProfile(
        profile_id=(profile_id or f"{candidate_id}:XFA:{rules.account_label}"),
        account_label=rules.account_label,
        official_rule_hash=rules.fingerprint,
        risk_multiplier=float(risk_multiplier),
        maximum_mini_equivalent=float(
            rules.combine_maximum_mini_equivalent
            if maximum_mini_equivalent is None
            else maximum_mini_equivalent
        ),
    )
    payload = {
        "schema": ACCOUNT_SIZE_XFA_HANDOFF_SCHEMA,
        "book": book.to_dict(),
        "profile": profile.to_dict(),
        "rule_snapshot_fingerprint": rules.fingerprint,
        "frozen_before_xfa_outcomes": True,
    }
    return FrozenAccountSizeXfaHandoff(
        schema=ACCOUNT_SIZE_XFA_HANDOFF_SCHEMA,
        book=book,
        profile=profile,
        rule_snapshot_fingerprint=rules.fingerprint,
        frozen_before_xfa_outcomes=True,
        handoff_hash=stable_hash(payload),
    )


def run_account_size_xfa_alternatives(
    component_trajectories: Mapping[str, Sequence[CausalTradeTrajectory]],
    eligible_session_days: Sequence[int],
    *,
    handoff: FrozenAccountSizeXfaHandoff,
    rules: AccountSizeXfaRules,
    transition_id: str,
    combine_path_hash: str,
    start_day: int,
    horizon_days: int = 120,
) -> AccountSizeXfaAlternativesResult:
    """Replay the two frozen alternative XFA paths from a fresh zero balance."""

    if rules.fingerprint != handoff.rule_snapshot_fingerprint:
        raise AccountSizeXfaError("runtime rules differ from the frozen XFA handoff")
    if not _is_sha256(combine_path_hash) or not transition_id.strip():
        raise AccountSizeXfaError("successful Combine transition identity is invalid")
    if horizon_days < 1:
        raise AccountSizeXfaError("XFA horizon must be positive")
    days = tuple(sorted({int(day) for day in eligible_session_days}))
    if int(start_day) not in days:
        raise AccountSizeXfaError("XFA start day is absent from continuation chronology")
    components = handoff.book.component_priority
    if set(component_trajectories) != set(components):
        raise AccountSizeXfaError("XFA trajectory membership differs from frozen book")
    _validate_trajectories(component_trajectories, components, start_day=int(start_day))
    source_hash = stable_hash(
        {
            "eligible_session_days": list(days),
            "component_trajectories": {
                key: [row.to_dict() for row in component_trajectories[key]]
                for key in sorted(component_trajectories)
            },
        }
    )
    standard = _run_path(
        component_trajectories,
        days,
        handoff=handoff,
        rules=rules,
        start_day=int(start_day),
        horizon=int(horizon_days),
        path="STANDARD",
    )
    consistency = _run_path(
        component_trajectories,
        days,
        handoff=handoff,
        rules=rules,
        start_day=int(start_day),
        horizon=int(horizon_days),
        path="CONSISTENCY",
    )
    payload = {
        "schema": ACCOUNT_SIZE_XFA_RESULT_SCHEMA,
        "engine_version": ACCOUNT_SIZE_XFA_ENGINE_VERSION,
        "transition_id": str(transition_id),
        "combine_path_hash": str(combine_path_hash),
        "source_trajectory_hash": source_hash,
        "handoff": handoff.to_dict(),
        "rules": rules.to_dict(),
        "alternatives": {
            "STANDARD": standard.to_dict(),
            "CONSISTENCY": consistency.to_dict(),
        },
        "standard_and_consistency_are_alternatives": True,
        "sum_standard_and_consistency_ev_allowed": False,
        "selected_path": None,
        "broker_connection_count": 0,
        "order_count": 0,
        "outbound_order_capability": False,
    }
    return AccountSizeXfaAlternativesResult(
        schema=ACCOUNT_SIZE_XFA_RESULT_SCHEMA,
        engine_version=ACCOUNT_SIZE_XFA_ENGINE_VERSION,
        transition_id=str(transition_id),
        combine_path_hash=str(combine_path_hash),
        source_trajectory_hash=source_hash,
        handoff=handoff,
        rules=rules,
        standard=standard,
        consistency=consistency,
        result_hash=stable_hash(payload),
    )


def _run_path(
    component_trajectories: Mapping[str, Sequence[CausalTradeTrajectory]],
    eligible_days: Sequence[int],
    *,
    handoff: FrozenAccountSizeXfaHandoff,
    rules: AccountSizeXfaRules,
    start_day: int,
    horizon: int,
    path: str,
) -> AccountSizeXfaPathResult:
    if path not in SUPPORTED_PATHS:
        raise AccountSizeXfaError("unsupported XFA path")
    days = tuple(eligible_days)
    start_index = days.index(start_day)
    episode_days = days[start_index : start_index + horizon]
    last_day = int(episode_days[-1])
    selected = set(handoff.book.component_priority)
    rows = sorted(
        (
            row
            for component_id, values in component_trajectories.items()
            if component_id in selected
            for row in values
            if start_day <= int(row.event.session_day) <= last_day
        ),
        key=lambda row: (
            int(row.event.decision_ns),
            _priority(handoff.book.component_priority, row.component_id),
            row.event.event_id,
        ),
    )
    by_day: dict[int, list[CausalTradeTrajectory]] = defaultdict(list)
    for row in rows:
        by_day[int(row.event.session_day)].append(row)

    balance = rules.xfa_starting_balance
    floor = rules.xfa_starting_floor
    minimum_buffer = balance - floor
    cycle_winning_days = 0
    qualifying_winning_days = 0
    cycle_traded_days = 0
    cycle_profit = 0.0
    cycle_best_day = 0.0
    cycle_start_balance = balance
    cycles = 0
    first_payout_day: int | None = None
    gross_payout = trader_net_payout = 0.0
    traded_days = accepted = skipped = 0
    maximum_size = maximum_consistency = 0.0
    skipped_reasons: Counter[str] = Counter()
    contribution: dict[str, float] = defaultdict(float)
    ledger: list[dict[str, Any]] = []
    terminal: XfaTerminal | None = None
    terminal_reason = ""

    for elapsed, day in enumerate(episode_days, start=1):
        opening_balance = balance
        opening_floor = floor
        session_limit = min(
            handoff.profile.maximum_mini_equivalent,
            rules.session_limit(opening_balance),
        )
        open_positions: dict[str, _OpenPosition] = {}
        day_pnl = 0.0
        day_accepted = day_skipped = 0
        day_max_size = 0.0
        day_worst_equity = balance
        action_times: set[int] = set()
        marks: dict[int, list[tuple[CausalTradeTrajectory, Any]]] = defaultdict(list)
        entries: dict[int, list[CausalTradeTrajectory]] = defaultdict(list)
        exits: dict[int, list[CausalTradeTrajectory]] = defaultdict(list)
        for trajectory in by_day.get(int(day), ()):
            entries[int(trajectory.event.decision_ns)].append(trajectory)
            exits[int(trajectory.event.exit_ns)].append(trajectory)
            action_times.add(int(trajectory.event.decision_ns))
            action_times.add(int(trajectory.event.exit_ns))
            for mark in trajectory.marks:
                marks[int(mark.availability_time_ns)].append((trajectory, mark))
                action_times.add(int(mark.availability_time_ns))

        for timestamp in sorted(action_times):
            for trajectory, mark in marks.get(timestamp, ()):
                position = open_positions.get(trajectory.event.event_id)
                if position is None:
                    continue
                current = (
                    mark.current_unrealized_pnl
                    if mark.current_unrealized_pnl is not None
                    else mark.worst_unrealized_pnl
                )
                position.current_unrealized = float(current * position.ratio)
                position.current_worst = float(mark.worst_unrealized_pnl * position.ratio)
            if marks.get(timestamp) and open_positions:
                conservative_low = balance + sum(
                    min(position.current_worst, 0.0)
                    for position in open_positions.values()
                )
                day_worst_equity = min(day_worst_equity, conservative_low)
                minimum_buffer = min(minimum_buffer, conservative_low - floor)
                if conservative_low <= floor:
                    forced = _force_liquidate(open_positions, contribution)
                    balance += forced
                    day_pnl += forced
                    terminal = XfaTerminal.MLL_BREACHED
                    terminal_reason = "causal_current_bar_xfa_mll_touch_or_breach"
                    break

            for trajectory in sorted(
                exits.get(timestamp, ()),
                key=lambda row: (
                    _priority(handoff.book.component_priority, row.component_id),
                    row.event.event_id,
                ),
            ):
                position = open_positions.pop(trajectory.event.event_id, None)
                if position is None:
                    continue
                realized = float(trajectory.event.net_pnl * position.ratio)
                balance += realized
                day_pnl += realized
                contribution[trajectory.component_id] += realized
                day_worst_equity = min(day_worst_equity, balance)
                minimum_buffer = min(minimum_buffer, balance - floor)
                if balance <= floor:
                    terminal = XfaTerminal.MLL_BREACHED
                    terminal_reason = "causal_realized_xfa_mll_touch_or_breach"
                    break
            if terminal is not None:
                break

            for trajectory in sorted(
                entries.get(timestamp, ()),
                key=lambda row: (
                    _priority(handoff.book.component_priority, row.component_id),
                    row.event.event_id,
                ),
            ):
                event = trajectory.event
                if not event.session_compliant or not event.contract_limit_compliant:
                    terminal = XfaTerminal.HARD_RULE_FAILURE
                    terminal_reason = (
                        "session_policy_violation"
                        if not event.session_compliant
                        else "source_contract_limit_violation"
                    )
                    break
                if len(open_positions) >= handoff.book.maximum_simultaneous_positions:
                    skipped += 1
                    day_skipped += 1
                    skipped_reasons["MAXIMUM_SIMULTANEOUS_POSITIONS"] += 1
                    continue
                if handoff.book.same_market_exclusive and any(
                    value.trajectory.market == trajectory.market
                    for value in open_positions.values()
                ):
                    skipped += 1
                    day_skipped += 1
                    skipped_reasons["SAME_MARKET_CONFLICT"] += 1
                    continue
                used = sum(value.mini_equivalent for value in open_positions.values())
                market_limit = min(session_limit, rules.session_limit(opening_balance, trajectory.market))
                available = max(0.0, min(session_limit - used, market_limit - used))
                position = _position_with_limit(
                    trajectory,
                    available=available,
                    risk_multiplier=handoff.profile.risk_multiplier,
                )
                if position is None:
                    skipped += 1
                    day_skipped += 1
                    skipped_reasons["XFA_SCALING_PLAN"] += 1
                    continue
                open_positions[event.event_id] = position
                accepted += 1
                day_accepted += 1
                maximum_size = max(
                    maximum_size,
                    sum(value.mini_equivalent for value in open_positions.values()),
                )
                day_max_size = max(day_max_size, maximum_size)
                live_equity = balance + sum(
                    value.current_unrealized for value in open_positions.values()
                )
                day_worst_equity = min(day_worst_equity, live_equity)
                minimum_buffer = min(minimum_buffer, live_equity - floor)
                if live_equity <= floor:
                    forced = _force_liquidate(open_positions, contribution)
                    balance += forced
                    day_pnl += forced
                    terminal = XfaTerminal.MLL_BREACHED
                    terminal_reason = "causal_entry_cost_xfa_mll_touch_or_breach"
                    break
            if terminal is not None:
                break

        if terminal is None and open_positions:
            terminal = XfaTerminal.HARD_RULE_FAILURE
            terminal_reason = "open_position_remaining_after_session_close"
        traded = day_accepted > 0
        if traded:
            traded_days += 1
            cycle_traded_days += 1
        if terminal is not None:
            ledger.append(
                _ledger_row(
                    day=int(day),
                    opening_balance=opening_balance,
                    closing_balance=balance,
                    opening_floor=opening_floor,
                    closing_floor=floor,
                    session_limit=session_limit,
                    day_pnl=day_pnl,
                    day_worst_equity=day_worst_equity,
                    traded=traded,
                    accepted=day_accepted,
                    skipped=day_skipped,
                    terminal=terminal.value,
                )
            )
            break

        floor = advance_end_of_day_floor(
            floor,
            closing_balance=balance,
            distance=rules.maximum_loss_limit,
            lock=0.0,
        )
        minimum_buffer = min(minimum_buffer, balance - floor)
        if day_pnl >= rules.xfa_standard_winning_day_minimum:
            cycle_winning_days += 1
            qualifying_winning_days += 1
        cycle_profit += day_pnl
        cycle_best_day = max(cycle_best_day, day_pnl)
        consistency_ratio = (
            cycle_best_day / cycle_profit
            if cycle_profit > 0.0 and cycle_best_day > 0.0
            else math.inf
        )
        if math.isfinite(consistency_ratio):
            maximum_consistency = max(maximum_consistency, consistency_ratio)
        if path == "STANDARD":
            eligible = cycle_winning_days >= rules.xfa_standard_winning_days and (
                cycles == 0
                or balance - cycle_start_balance
                >= rules.later_standard_cycle_minimum_profit - 1e-12
            )
            payout_cap = rules.standard_payout_cap
        else:
            eligible = bool(
                cycle_traded_days >= rules.xfa_consistency_traded_days
                and cycle_profit > 0.0
                and consistency_ratio <= rules.xfa_consistency_limit + 1e-12
            )
            payout_cap = rules.consistency_payout_cap

        pre_payout_balance = balance
        fraction_limit = balance * rules.payout_fraction if eligible and balance > 0.0 else 0.0
        payout_candidate = min(fraction_limit, payout_cap) if eligible else 0.0
        payout_executed = bool(payout_candidate >= rules.minimum_payout - 1e-12)
        payout_gross = payout_candidate if payout_executed else 0.0
        payout_net = payout_gross * rules.trader_profit_split
        if payout_executed:
            cycles += 1
            if first_payout_day is None:
                first_payout_day = elapsed
            gross_payout += payout_gross
            trader_net_payout += payout_net
            balance -= payout_gross
            floor = rules.mll_floor_after_first_payout
            minimum_buffer = min(minimum_buffer, balance - floor)
            cycle_winning_days = 0
            cycle_traded_days = 0
            cycle_profit = 0.0
            cycle_best_day = 0.0
            cycle_start_balance = balance
        ledger.append(
            {
                **_ledger_row(
                    day=int(day),
                    opening_balance=opening_balance,
                    closing_balance=balance,
                    opening_floor=opening_floor,
                    closing_floor=floor,
                    session_limit=session_limit,
                    day_pnl=day_pnl,
                    day_worst_equity=day_worst_equity,
                    traded=traded,
                    accepted=day_accepted,
                    skipped=day_skipped,
                    terminal=None,
                ),
                "payout_eligible": eligible,
                "eligible_account_balance": pre_payout_balance if eligible else None,
                "payout_balance_fraction_limit": fraction_limit,
                "payout_account_size_cap": payout_cap,
                "gross_payout_request": payout_gross,
                "gross_payout": payout_gross,
                "payout_split": rules.trader_profit_split,
                "trader_net_payout": payout_net,
                "pre_payout_balance": pre_payout_balance,
                "post_payout_balance": balance,
                "mll_before_payout": opening_floor,
                "mll_after_payout": floor,
                "payout_reset_marker": payout_executed,
                "payout_cycle": cycles if payout_executed else None,
                "winning_days_in_cycle": cycle_winning_days,
                "traded_days_in_cycle": cycle_traded_days,
                "consistency_ratio_before_reset": (
                    consistency_ratio if math.isfinite(consistency_ratio) else None
                ),
            }
        )

    if terminal is None:
        if len(episode_days) < horizon:
            terminal = XfaTerminal.DATA_CENSORED
            terminal_reason = "available_chronology_ended_before_frozen_xfa_horizon"
        else:
            terminal = XfaTerminal.SURVIVED_HORIZON
            terminal_reason = "frozen_xfa_horizon_survived"
    _assert_payout_invariants(ledger, rules=rules, path=path, first_payout_day=first_payout_day)
    payload = {
        "path": f"XFA_{path}",
        "terminal": terminal.value,
        "terminal_reason": terminal_reason,
        "start_day": start_day,
        "end_day": int(ledger[-1]["session_day"]),
        "requested_horizon_days": horizon,
        "observed_days": len(ledger),
        "traded_days": traded_days,
        "accepted_event_count": accepted,
        "skipped_event_count": skipped,
        "payout_cycles": cycles,
        "first_payout_day": first_payout_day,
        "first_payout_count": int(first_payout_day is not None),
        "gross_payout": gross_payout,
        "trader_net_payout": trader_net_payout,
        "ending_balance": balance,
        "ending_mll_floor": floor,
        "minimum_mll_buffer": minimum_buffer,
        "post_payout_survived": bool(cycles > 0 and terminal is XfaTerminal.SURVIVED_HORIZON),
        "qualifying_winning_days": qualifying_winning_days,
        "maximum_consistency_ratio": maximum_consistency,
        "maximum_mini_equivalent": maximum_size,
        "skipped_reasons": dict(sorted(skipped_reasons.items())),
        "component_contribution": dict(sorted(contribution.items())),
        "daily_ledger": ledger,
    }
    return AccountSizeXfaPathResult(
        path=f"XFA_{path}",
        terminal=terminal,
        terminal_reason=terminal_reason,
        start_day=start_day,
        end_day=int(ledger[-1]["session_day"]),
        requested_horizon_days=horizon,
        observed_days=len(ledger),
        traded_days=traded_days,
        accepted_event_count=accepted,
        skipped_event_count=skipped,
        payout_cycles=cycles,
        first_payout_day=first_payout_day,
        gross_payout=float(gross_payout),
        trader_net_payout=float(trader_net_payout),
        ending_balance=float(balance),
        ending_mll_floor=float(floor),
        minimum_mll_buffer=float(minimum_buffer),
        post_payout_survived=bool(cycles > 0 and terminal is XfaTerminal.SURVIVED_HORIZON),
        qualifying_winning_days=qualifying_winning_days,
        maximum_consistency_ratio=float(maximum_consistency),
        maximum_mini_equivalent=float(maximum_size),
        skipped_reasons=dict(sorted(skipped_reasons.items())),
        component_contribution=dict(sorted(contribution.items())),
        daily_ledger=tuple(ledger),
        path_hash=stable_hash(payload),
    )


def _position_with_limit(
    trajectory: CausalTradeTrajectory,
    *,
    available: float,
    risk_multiplier: float,
) -> _OpenPosition | None:
    event = trajectory.event
    per_contract = float(event.mini_equivalent / event.quantity)
    requested = max(1, int(math.floor(event.quantity * risk_multiplier + 1e-12)))
    quantity = min(requested, int(math.floor((available + 1e-12) / per_contract)))
    if quantity < 1:
        return None
    ratio = float(quantity / event.quantity)
    initial = float(trajectory.initial_unrealized_pnl * ratio)
    return _OpenPosition(
        trajectory=trajectory,
        quantity=quantity,
        mini_equivalent=float(event.mini_equivalent * ratio),
        ratio=ratio,
        current_unrealized=initial,
        current_worst=initial,
    )


def _force_liquidate(
    positions: dict[str, _OpenPosition], contribution: dict[str, float]
) -> float:
    total = 0.0
    for position in positions.values():
        value = float(position.current_worst)
        contribution[position.trajectory.component_id] += value
        total += value
    positions.clear()
    return total


def _ledger_row(
    *,
    day: int,
    opening_balance: float,
    closing_balance: float,
    opening_floor: float,
    closing_floor: float,
    session_limit: float,
    day_pnl: float,
    day_worst_equity: float,
    traded: bool,
    accepted: int,
    skipped: int,
    terminal: str | None,
) -> dict[str, Any]:
    return {
        "session_day": day,
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
        "mll_floor_open": opening_floor,
        "mll_floor_close": closing_floor,
        "scaling_limit_mini_equivalent": session_limit,
        "day_pnl": day_pnl,
        "worst_intraday_equity": day_worst_equity,
        "traded": traded,
        "accepted_events": accepted,
        "skipped_events": skipped,
        "payout_eligible": False,
        "gross_payout_request": 0.0,
        "gross_payout": 0.0,
        "trader_net_payout": 0.0,
        "payout_reset_marker": False,
        "payout_cycle": None,
        "terminal": terminal,
    }


def _assert_payout_invariants(
    ledger: Sequence[Mapping[str, Any]],
    *,
    rules: AccountSizeXfaRules,
    path: str,
    first_payout_day: int | None,
) -> None:
    executed = [row for row in ledger if bool(row.get("payout_reset_marker"))]
    cycles = [int(row["payout_cycle"]) for row in executed]
    if cycles != list(range(1, len(executed) + 1)):
        raise AccountSizeXfaError("XFA payout cycles are duplicated or out of order")
    observed_first = (
        None
        if not executed
        else ledger.index(executed[0]) + 1
    )
    if observed_first != first_payout_day:
        raise AccountSizeXfaError("XFA first-payout identity drift")
    cap = rules.standard_payout_cap if path == "STANDARD" else rules.consistency_payout_cap
    for row in executed:
        gross = float(row["gross_payout"])
        pre = float(row["pre_payout_balance"])
        if gross < rules.minimum_payout - 1e-12:
            raise AccountSizeXfaError("subminimum payout was executed")
        if gross > cap + 1e-12 or gross > pre * rules.payout_fraction + 1e-12:
            raise AccountSizeXfaError("payout cap was exceeded")
        if not math.isclose(
            float(row["trader_net_payout"]),
            gross * rules.trader_profit_split,
            abs_tol=1e-9,
        ):
            raise AccountSizeXfaError("payout split does not reconcile")
        if not math.isclose(
            float(row["post_payout_balance"]),
            pre - gross,
            abs_tol=1e-9,
        ):
            raise AccountSizeXfaError("post-payout balance does not reconcile")
        if float(row["mll_after_payout"]) != rules.mll_floor_after_first_payout:
            raise AccountSizeXfaError("post-payout MLL reset does not reconcile")


def _validate_trajectories(
    component_trajectories: Mapping[str, Sequence[CausalTradeTrajectory]],
    components: Sequence[str],
    *,
    start_day: int,
) -> None:
    seen: set[str] = set()
    for component_id in components:
        prior: tuple[int, str] | None = None
        for row in component_trajectories[component_id]:
            if row.component_id != component_id:
                raise AccountSizeXfaError("XFA component trajectory key mismatch")
            if int(row.event.session_day) < start_day:
                raise AccountSizeXfaError("XFA continuation contains pre-start trajectory")
            key = (int(row.event.decision_ns), row.event.event_id)
            if prior is not None and key <= prior:
                raise AccountSizeXfaError("XFA component trajectory order is unstable")
            prior = key
            if row.event.event_id in seen:
                raise AccountSizeXfaError("XFA event ID is duplicated")
            seen.add(row.event.event_id)


def _priority(priority: Sequence[str], component_id: str) -> int:
    return tuple(priority).index(component_id)


def _market_symbol(value: str) -> str:
    raw = str(value).upper().strip()
    known = ("MCL", "MGC", "SIL", "MHG", "CL", "QM", "RB", "HO", "GC", "SI", "HG", "PL")
    return next((symbol for symbol in known if raw.startswith(symbol)), raw)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_sha256(value: str) -> bool:
    return len(str(value)) == 64 and all(char in "0123456789abcdef" for char in str(value))


__all__ = [
    "ACCOUNT_SIZE_XFA_ENGINE_VERSION",
    "ACCOUNT_SIZE_XFA_HANDOFF_SCHEMA",
    "ACCOUNT_SIZE_XFA_RESULT_SCHEMA",
    "AccountSizeXfaAlternativesResult",
    "AccountSizeXfaError",
    "AccountSizeXfaPathResult",
    "AccountSizeXfaRules",
    "FrozenAccountSizeXfaBook",
    "FrozenAccountSizeXfaHandoff",
    "FrozenAccountSizeXfaProfile",
    "freeze_account_size_xfa_handoff",
    "load_account_size_xfa_rules",
    "run_account_size_xfa_alternatives",
]
