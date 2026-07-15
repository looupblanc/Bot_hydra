"""Chronological Trading Combine to XFA lifecycle replay.

This module is intentionally self-contained and versioned.  It reuses the
authoritative shared-account Combine replay, then starts a *new* XFA account on
the next eligible trading day only after a Combine pass.  Combine profit is
never transferred.  Standard and Consistency payout paths are replayed
separately; this module does not choose the better path per episode.

The default rule snapshot is the no-DLL, post-12-January-2026 Topstep 150K
contract verified from the official Topstep Help Center on 15 July 2026.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import date, timedelta
from enum import StrEnum
from typing import Any, Mapping, Sequence

from hydra.account_policy.basket import (
    AccountPolicyEpisode,
    RoutedTrade,
    run_shared_account_episode,
)
from hydra.account_policy.schema import BasketPolicy, ControllerPolicy
from hydra.propfirm.combine_episode import CombineTerminal, TradePathEvent
from hydra.propfirm.mll_variants import MllMode, advance_end_of_day_floor
from hydra.propfirm.topstep_150k import Topstep150KConfig


LIFECYCLE_VERSION = "hydra_combine_to_xfa_v1"
RULE_SNAPSHOT_VERSION = "topstep_150k_2026-07-15_no_dll_post_2026-01-12_v1"
UNREALIZED_AGGREGATION_SEMANTICS = (
    "CONSERVATIVE_SUM_OF_OPEN_TRADE_EXTREMA_BOUND_V1"
)


class CombineLifecycleStatus(StrEnum):
    TARGET_REACHED = "TARGET_REACHED"
    MLL_BREACHED = "MLL_BREACHED"
    HARD_RULE_FAILURE = "HARD_RULE_FAILURE"
    DATA_CENSORED = "DATA_CENSORED"
    OPERATIONAL_HORIZON_NOT_REACHED = "OPERATIONAL_HORIZON_NOT_REACHED"


class XfaTerminal(StrEnum):
    SURVIVED_HORIZON = "SURVIVED_HORIZON"
    DATA_CENSORED = "DATA_CENSORED"
    MLL_BREACHED = "MLL_BREACHED"
    HARD_RULE_FAILURE = "HARD_RULE_FAILURE"
    INACTIVITY_RISK = "INACTIVITY_RISK"


@dataclass(frozen=True, slots=True)
class RuleSnapshot:
    """Immutable executable 150K rule snapshot with primary-source provenance."""

    rule_version: str = RULE_SNAPSHOT_VERSION
    verified_at_utc: str = "2026-07-15T00:00:00Z"
    account_size: float = 150_000.0
    combine_profit_target: float = 9_000.0
    maximum_loss_limit: float = 4_500.0
    combine_maximum_mini_equivalent: int = 15
    combine_maximum_micros: int = 150
    micro_to_mini_ratio: int = 10
    combine_consistency_limit: float = 0.50
    combine_minimum_days: int = 2
    combine_has_official_time_limit: bool = False
    xfa_starting_balance: float = 0.0
    xfa_starting_floor: float = -4_500.0
    xfa_standard_winning_days: int = 5
    xfa_standard_winning_day_minimum: float = 150.0
    xfa_consistency_traded_days: int = 3
    xfa_consistency_limit: float = 0.40
    payout_fraction: float = 0.50
    standard_payout_cap: float = 5_000.0
    consistency_payout_cap: float = 6_000.0
    minimum_payout: float = 125.0
    trader_profit_split: float = 0.90
    later_standard_cycle_minimum_profit: float = 0.01
    inactivity_calendar_days: int = 30
    no_daily_loss_limit: bool = True
    post_2026_01_12_profit_split: bool = True
    mll_mode: str = MllMode.EOD_LEVEL_RT_BREACH.value
    # (inclusive lower balance, maximum mini-equivalent), evaluated at the
    # session open.  Increases never apply mid-session.
    xfa_scaling_tiers: tuple[tuple[float, float], ...] = (
        (-1_000_000_000.0, 3.0),
        (1_500.0, 4.0),
        (2_000.0, 5.0),
        (3_000.0, 10.0),
        (4_500.0, 15.0),
    )
    # Current high-volatility CL/GC restriction, conservatively expressed in
    # mini-equivalents for the 150K XFA.  It is applied independently of the
    # ordinary scaling plan.
    restricted_market_scaling_tiers: tuple[tuple[float, float], ...] = (
        (-1_000_000_000.0, 1.0),
        (1_500.0, 2.0),
        (2_000.0, 3.0),
        (3_000.0, 6.0),
        (4_500.0, 9.0),
    )
    restricted_market_roots: tuple[str, ...] = ("CL", "GC")
    official_source_urls: tuple[str, ...] = (
        "https://help.topstep.com/en/articles/8284197-trading-combine-parameters",
        "https://help.topstep.com/en/articles/8284204-what-is-the-maximum-loss-limit",
        "https://help.topstep.com/en/articles/8284215-express-funded-account-parameters",
        "https://help.topstep.com/en/articles/8284223-what-is-the-scaling-plan",
        "https://help.topstep.com/en/articles/8284233-topstep-payout-policy",
        "https://help.topstep.com/en/articles/13613539-risk-adjustments-high-risk-high-volatility",
        "https://help.topstep.com/en/articles/10490293-daily-loss-limit-in-the-trading-combine-and-express-funded-account",
    )

    def __post_init__(self) -> None:
        if self.rule_version != RULE_SNAPSHOT_VERSION:
            raise ValueError("unexpected lifecycle rule version")
        if not self.no_daily_loss_limit:
            raise ValueError("this snapshot is the frozen no-DLL path")
        if not self.post_2026_01_12_profit_split:
            raise ValueError("legacy first-$10,000 split is outside this snapshot")
        if self.account_size != 150_000.0 or self.maximum_loss_limit != 4_500.0:
            raise ValueError("RuleSnapshot must describe the 150K account")
        if self.trader_profit_split != 0.90:
            raise ValueError("post-2026-01-12 snapshot requires the 90/10 split")
        frozen_values = (
            self.account_size,
            self.combine_profit_target,
            self.maximum_loss_limit,
            self.combine_maximum_mini_equivalent,
            self.combine_maximum_micros,
            self.micro_to_mini_ratio,
            self.combine_consistency_limit,
            self.combine_minimum_days,
            self.xfa_starting_balance,
            self.xfa_starting_floor,
            self.xfa_standard_winning_days,
            self.xfa_standard_winning_day_minimum,
            self.xfa_consistency_traded_days,
            self.xfa_consistency_limit,
            self.payout_fraction,
            self.standard_payout_cap,
            self.consistency_payout_cap,
            self.minimum_payout,
            self.trader_profit_split,
            self.later_standard_cycle_minimum_profit,
            self.inactivity_calendar_days,
            self.combine_has_official_time_limit,
            self.mll_mode,
            self.restricted_market_roots,
        )
        expected_values = (
            150_000.0,
            9_000.0,
            4_500.0,
            15,
            150,
            10,
            0.50,
            2,
            0.0,
            -4_500.0,
            5,
            150.0,
            3,
            0.40,
            0.50,
            5_000.0,
            6_000.0,
            125.0,
            0.90,
            0.01,
            30,
            False,
            MllMode.EOD_LEVEL_RT_BREACH.value,
            ("CL", "GC"),
        )
        if frozen_values != expected_values:
            raise ValueError("rule values drifted under a frozen snapshot version")
        _validate_tiers(self.xfa_scaling_tiers, expected_last=15.0)
        _validate_tiers(self.restricted_market_scaling_tiers, expected_last=9.0)
        if not self.official_source_urls or any(
            not value.startswith("https://help.topstep.com/")
            for value in self.official_source_urls
        ):
            raise ValueError("official Topstep provenance is incomplete")

    @property
    def fingerprint(self) -> str:
        return _stable_hash(self._payload())

    def _payload(self) -> dict[str, Any]:
        value = asdict(self)
        value["xfa_scaling_tiers"] = [list(row) for row in self.xfa_scaling_tiers]
        value["restricted_market_scaling_tiers"] = [
            list(row) for row in self.restricted_market_scaling_tiers
        ]
        value["restricted_market_roots"] = list(self.restricted_market_roots)
        value["official_source_urls"] = list(self.official_source_urls)
        return value

    def to_dict(self) -> dict[str, Any]:
        return {**self._payload(), "fingerprint": self.fingerprint}

    def xfa_session_limit(self, opening_balance: float, market: str | None = None) -> float:
        tiers = (
            self.restricted_market_scaling_tiers
            if market is not None and _market_root(market) in self.restricted_market_roots
            else self.xfa_scaling_tiers
        )
        return _tier_value(tiers, opening_balance)

    def combine_config(self) -> Topstep150KConfig:
        return Topstep150KConfig(
            account_size=self.account_size,
            combine_profit_target=self.combine_profit_target,
            combine_max_loss_limit=self.maximum_loss_limit,
            combine_starting_balance=self.account_size,
            mll_mode=self.mll_mode,
            no_daily_loss_limit=True,
            use_optional_daily_loss_limit=False,
            consistency_best_day_max_pct_of_profit_target=(
                self.combine_consistency_limit
            ),
            minimum_pass_days=self.combine_minimum_days,
            funded_starting_balance=self.xfa_starting_balance,
            funded_starting_mll=self.xfa_starting_floor,
            payout_eligibility_winning_days=self.xfa_standard_winning_days,
            payout_winning_day_min_profit=self.xfa_standard_winning_day_minimum,
            payout_max_pct_of_balance=self.payout_fraction,
            payout_cap=self.standard_payout_cap,
            profit_split_trader=self.trader_profit_split,
            funded_consistency_enabled=True,
            funded_consistency_largest_day_max_pct_of_total_profit=(
                self.xfa_consistency_limit
            ),
            internal_daily_stop_enabled=False,
        )


@dataclass(frozen=True, slots=True)
class FrozenRiskProfile:
    """Pre-outcome account overlay; underlying signals remain immutable."""

    profile_id: str
    risk_multiplier: float = 1.0
    maximum_simultaneous_positions: int = 4
    maximum_mini_equivalent: int = 15
    clip_to_xfa_scaling_plan: bool = True
    same_market_exclusive: bool = True
    profile_version: str = LIFECYCLE_VERSION

    def __post_init__(self) -> None:
        if not self.profile_id.strip():
            raise ValueError("profile_id must be non-empty")
        if not math.isfinite(self.risk_multiplier) or self.risk_multiplier <= 0.0:
            raise ValueError("risk_multiplier must be finite and positive")
        if self.maximum_simultaneous_positions < 1:
            raise ValueError("maximum_simultaneous_positions must be positive")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("maximum_mini_equivalent must be in [1,15]")
        if self.profile_version != LIFECYCLE_VERSION:
            raise ValueError("unexpected risk-profile version")

    @property
    def fingerprint(self) -> str:
        return _stable_hash(self._payload())

    def _payload(self) -> dict[str, Any]:
        return asdict(self)

    def to_dict(self) -> dict[str, Any]:
        return {**self._payload(), "fingerprint": self.fingerprint}


@dataclass(frozen=True, slots=True)
class XfaPathResult:
    path: str
    terminal: XfaTerminal
    terminal_reason: str
    start_day: int | None
    end_day: int | None
    requested_horizon_days: int
    observed_days: int
    traded_days: int
    event_count: int
    accepted_event_count: int
    skipped_event_count: int
    payout_eligible: bool
    payout_cycles: int
    gross_payout: float
    trader_net_payout: float
    first_payout_day: int | None
    post_payout_survived: bool
    post_payout_censored: bool
    post_payout_observed_days: int
    ending_balance: float
    ending_mll_floor: float
    minimum_mll_buffer: float
    qualifying_winning_days: int
    maximum_consistency_ratio: float
    maximum_mini_equivalent: float
    total_cost: float
    skipped_reasons: Mapping[str, int]
    component_contribution: Mapping[str, float]
    daily_ledger: tuple[Mapping[str, Any], ...]
    calendar_inactivity_auditable: bool
    payout_request_policy: str
    payout_path_selected_from_outcomes: bool
    path_hash: str

    @property
    def survived(self) -> bool:
        return self.terminal is XfaTerminal.SURVIVED_HORIZON

    @property
    def data_censored(self) -> bool:
        return self.terminal is XfaTerminal.DATA_CENSORED

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "terminal": self.terminal.value,
            "terminal_reason": self.terminal_reason,
            "start_day": self.start_day,
            "end_day": self.end_day,
            "requested_horizon_days": self.requested_horizon_days,
            "observed_days": self.observed_days,
            "traded_days": self.traded_days,
            "event_count": self.event_count,
            "accepted_event_count": self.accepted_event_count,
            "skipped_event_count": self.skipped_event_count,
            "payout_eligible": self.payout_eligible,
            "payout_cycles": self.payout_cycles,
            "gross_payout": self.gross_payout,
            "trader_net_payout": self.trader_net_payout,
            "first_payout_day": self.first_payout_day,
            "post_payout_survived": self.post_payout_survived,
            "post_payout_censored": self.post_payout_censored,
            "post_payout_observed_days": self.post_payout_observed_days,
            "survived": self.survived,
            "observed_without_failure": self.terminal in {
                XfaTerminal.SURVIVED_HORIZON,
                XfaTerminal.DATA_CENSORED,
            },
            "data_censored": self.data_censored,
            "ending_balance": self.ending_balance,
            "ending_mll_floor": self.ending_mll_floor,
            "minimum_mll_buffer": self.minimum_mll_buffer,
            "qualifying_winning_days": self.qualifying_winning_days,
            "maximum_consistency_ratio": self.maximum_consistency_ratio,
            "maximum_mini_equivalent": self.maximum_mini_equivalent,
            "total_cost": self.total_cost,
            "skipped_reasons": dict(sorted(self.skipped_reasons.items())),
            "component_contribution": dict(
                sorted(self.component_contribution.items())
            ),
            "daily_ledger": [dict(row) for row in self.daily_ledger],
            "calendar_inactivity_auditable": (
                self.calendar_inactivity_auditable
            ),
            "payout_request_policy": self.payout_request_policy,
            "payout_path_selected_from_outcomes": (
                self.payout_path_selected_from_outcomes
            ),
            "unrealized_aggregation_semantics": UNREALIZED_AGGREGATION_SEMANTICS,
            "mll_breached": self.terminal is XfaTerminal.MLL_BREACHED,
            "hard_rule_failure": self.terminal is XfaTerminal.HARD_RULE_FAILURE,
            "inactivity_risk": self.terminal is XfaTerminal.INACTIVITY_RISK,
            "account_death_reason": (
                self.terminal_reason
                if self.terminal
                in {
                    XfaTerminal.MLL_BREACHED,
                    XfaTerminal.HARD_RULE_FAILURE,
                    XfaTerminal.INACTIVITY_RISK,
                }
                else None
            ),
            "path_hash": self.path_hash,
        }


@dataclass(frozen=True, slots=True)
class CombineToXfaEpisodeResult:
    schema: str
    lifecycle_version: str
    policy_id: str
    start_day: int
    combine_horizon_days: int
    xfa_horizon_days: int
    combine_status: CombineLifecycleStatus
    combine_episode: AccountPolicyEpisode
    xfa_started: bool
    xfa_start_day: int | None
    xfa_standard: XfaPathResult | None
    xfa_consistency: XfaPathResult | None
    rule_snapshot: RuleSnapshot
    combine_profile: FrozenRiskProfile
    xfa_profile: FrozenRiskProfile
    combine_controller: ControllerPolicy | None
    source_ledger_hash: str
    evidence_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "lifecycle_version": self.lifecycle_version,
            "policy_id": self.policy_id,
            "start_day": self.start_day,
            "combine_horizon_days": self.combine_horizon_days,
            "xfa_horizon_days": self.xfa_horizon_days,
            "combine_status": self.combine_status.value,
            "combine_episode": self.combine_episode.to_dict(include_paths=True),
            "xfa_started": self.xfa_started,
            "xfa_start_day": self.xfa_start_day,
            "xfa_standard": (
                self.xfa_standard.to_dict() if self.xfa_standard else None
            ),
            "xfa_consistency": (
                self.xfa_consistency.to_dict() if self.xfa_consistency else None
            ),
            "rule_snapshot": self.rule_snapshot.to_dict(),
            "combine_profile": self.combine_profile.to_dict(),
            "xfa_profile": self.xfa_profile.to_dict(),
            "combine_controller": (
                self.combine_controller.to_dict()
                if self.combine_controller is not None
                else None
            ),
            "xfa_routing_semantics": "FROZEN_STATIC_ACCOUNT_OVERLAY",
            "source_ledger_hash": self.source_ledger_hash,
            "evidence_hash": self.evidence_hash,
            "combine_profit_transferred_to_xfa": False,
            "payout_path_oracle_used": False,
            "unrealized_aggregation_semantics": UNREALIZED_AGGREGATION_SEMANTICS,
            "development_only": True,
            "broker_connection_count": 0,
            "order_count": 0,
            "outbound_order_capability": False,
        }


@dataclass(slots=True)
class _OpenPosition:
    trade: RoutedTrade
    quantity: int
    mini_equivalent: float
    net_pnl: float
    gross_pnl: float
    worst_unrealized_pnl: float
    best_unrealized_pnl: float


def official_rule_snapshot_2026_07_15() -> RuleSnapshot:
    return RuleSnapshot()


def run_combine_to_xfa_episode(
    component_events: Mapping[str, Sequence[RoutedTrade]],
    eligible_session_days: Sequence[int],
    *,
    basket: BasketPolicy,
    combine_profile: FrozenRiskProfile,
    xfa_profile: FrozenRiskProfile,
    start_day: int,
    combine_horizon_days: int = 90,
    xfa_horizon_days: int = 120,
    rule_snapshot: RuleSnapshot | None = None,
    controller: ControllerPolicy | None = None,
    xfa_eligible_session_days: Sequence[int] | None = None,
) -> CombineToXfaEpisodeResult:
    """Replay one frozen Combine -> XFA lifecycle without an outcome oracle.

    Both XFA payout paths use the same frozen ``xfa_profile`` and source ledger.
    They are emitted independently.  Callers may aggregate a *predeclared* path
    across episodes, but must not pick the winner independently per episode.
    """

    rules = rule_snapshot or official_rule_snapshot_2026_07_15()
    if combine_horizon_days < 1 or xfa_horizon_days < 1:
        raise ValueError("lifecycle horizons must be positive")
    if not xfa_profile.clip_to_xfa_scaling_plan:
        raise ValueError("XFA profile must enforce the official scaling plan")
    days = tuple(sorted({int(value) for value in eligible_session_days}))
    if start_day not in days:
        raise ValueError("start_day must be an eligible session day")
    xfa_days = tuple(
        sorted(
            {
                int(value)
                for value in (
                    xfa_eligible_session_days
                    if xfa_eligible_session_days is not None
                    else eligible_session_days
                )
            }
        )
    )
    if not xfa_days:
        raise ValueError("XFA chronology cannot be empty")
    missing = [value for value in basket.component_ids if value not in component_events]
    if missing:
        raise ValueError(f"basket components missing from event ledger: {missing}")
    if controller is not None:
        if controller.basket_policy_id != basket.policy_id:
            raise ValueError("Combine controller does not reference the frozen basket")
        if set(controller.component_priority) != set(basket.component_ids):
            raise ValueError("Combine controller components differ from the basket")
        if (
            controller.maximum_simultaneous_positions
            > combine_profile.maximum_simultaneous_positions
            or controller.maximum_mini_equivalent
            > combine_profile.maximum_mini_equivalent
        ):
            raise ValueError("Combine controller exceeds the frozen risk-profile caps")
    _validate_source_events(component_events, basket)

    source_hash = _stable_hash(
        {
            "eligible_session_days": list(days),
            "xfa_eligible_session_days": list(xfa_days),
            "component_events": {
                key: [row.to_dict() for row in component_events[key]]
                for key in sorted(component_events)
                if key in set(basket.component_ids)
            },
        }
    )
    combine_events = _enforce_combine_market_caps(
        _scale_events(component_events, combine_profile.risk_multiplier),
        rules,
    )
    combine_basket = replace(
        basket,
        maximum_simultaneous_positions=min(
            basket.maximum_simultaneous_positions,
            combine_profile.maximum_simultaneous_positions,
        ),
        maximum_mini_equivalent=min(
            basket.maximum_mini_equivalent,
            combine_profile.maximum_mini_equivalent,
            rules.combine_maximum_mini_equivalent,
        ),
    )
    combine = run_shared_account_episode(
        combine_events,
        days,
        basket=combine_basket,
        start_day=int(start_day),
        maximum_duration_days=int(combine_horizon_days),
        controller=controller,
        config=rules.combine_config(),
    )
    available_combine_days = len(days[days.index(start_day) :])
    combine_status = _combine_status(
        combine,
        data_censored=available_combine_days < combine_horizon_days,
    )

    xfa_start_day = next((day for day in xfa_days if day > combine.end_day), None)
    standard: XfaPathResult | None = None
    consistency: XfaPathResult | None = None
    if combine.passed and xfa_start_day is not None:
        xfa_events = _scale_events(component_events, xfa_profile.risk_multiplier)
        standard = _run_xfa_path(
            xfa_events,
            xfa_days,
            basket=basket,
            profile=xfa_profile,
            rules=rules,
            start_day=xfa_start_day,
            horizon=xfa_horizon_days,
            path="STANDARD",
        )
        consistency = _run_xfa_path(
            xfa_events,
            xfa_days,
            basket=basket,
            profile=xfa_profile,
            rules=rules,
            start_day=xfa_start_day,
            horizon=xfa_horizon_days,
            path="CONSISTENCY",
        )
    elif combine.passed:
        standard = _zero_observation_xfa_path(
            path="STANDARD",
            horizon=xfa_horizon_days,
            rules=rules,
        )
        consistency = _zero_observation_xfa_path(
            path="CONSISTENCY",
            horizon=xfa_horizon_days,
            rules=rules,
        )

    result = CombineToXfaEpisodeResult(
        schema="hydra_combine_to_xfa_episode_v1",
        lifecycle_version=LIFECYCLE_VERSION,
        policy_id=basket.policy_id,
        start_day=int(start_day),
        combine_horizon_days=int(combine_horizon_days),
        xfa_horizon_days=int(xfa_horizon_days),
        combine_status=combine_status,
        combine_episode=combine,
        xfa_started=bool(combine.passed),
        xfa_start_day=(
            int(xfa_start_day)
            if combine.passed and xfa_start_day is not None
            else None
        ),
        xfa_standard=standard,
        xfa_consistency=consistency,
        rule_snapshot=rules,
        combine_profile=combine_profile,
        xfa_profile=xfa_profile,
        combine_controller=controller,
        source_ledger_hash=source_hash,
        evidence_hash="",
    )
    payload = result.to_dict()
    payload.pop("evidence_hash")
    return replace(result, evidence_hash=_stable_hash(payload))


def _run_xfa_path(
    component_events: Mapping[str, Sequence[RoutedTrade]],
    eligible_days: Sequence[int],
    *,
    basket: BasketPolicy,
    profile: FrozenRiskProfile,
    rules: RuleSnapshot,
    start_day: int,
    horizon: int,
    path: str,
) -> XfaPathResult:
    days = tuple(sorted({int(value) for value in eligible_days}))
    start_index = days.index(int(start_day))
    episode_days = days[start_index : start_index + horizon]
    selected = set(basket.component_ids)
    trades = sorted(
        (
            row
            for component_id, values in component_events.items()
            if component_id in selected
            for row in values
            if episode_days[0] <= row.event.session_day <= episode_days[-1]
        ),
        key=lambda row: (
            row.event.session_day,
            row.event.decision_ns,
            _priority(basket, row.component_id),
            row.event.event_id,
        ),
    )
    by_day: dict[int, list[RoutedTrade]] = defaultdict(list)
    for row in trades:
        by_day[int(row.event.session_day)].append(row)

    balance = rules.xfa_starting_balance
    floor = rules.xfa_starting_floor
    minimum_buffer = balance - floor
    winning_days = 0
    total_qualifying_days = 0
    traded_days_cycle = 0
    total_profit_cycle = 0.0
    best_day_cycle = 0.0
    cycle_start_balance = balance
    cycles = 0
    gross_payout = 0.0
    trader_net_payout = 0.0
    first_payout_day: int | None = None
    last_payout_elapsed: int | None = None
    traded_days = 0
    event_count = 0
    accepted = 0
    skipped = 0
    maximum_size = 0.0
    maximum_consistency = 0.0
    total_cost = 0.0
    skipped_reasons: Counter[str] = Counter()
    contribution: dict[str, float] = defaultdict(float)
    ledger: list[dict[str, Any]] = []
    terminal: XfaTerminal | None = None
    reason = ""
    last_activity_day = int(start_day)
    calendar_auditable = _parse_session_date(start_day) is not None

    for elapsed, day in enumerate(episode_days, start=1):
        gap = _calendar_gap(last_activity_day, int(day))
        calendar_auditable = calendar_auditable and gap is not None
        if gap is not None and gap > rules.inactivity_calendar_days:
            terminal = XfaTerminal.INACTIVITY_RISK
            reason = "more_than_30_calendar_days_without_xfa_trading_activity"
            ledger.append(
                {
                    "session_day": int(day),
                    "opening_balance": balance,
                    "closing_balance": balance,
                    "mll_floor_open": floor,
                    "mll_floor_close": floor,
                    "day_pnl": 0.0,
                    "traded": False,
                    "inactivity_calendar_days": gap,
                    "terminal": terminal.value,
                }
            )
            break

        opening_balance = balance
        floor_open = floor
        session_limit = min(
            float(profile.maximum_mini_equivalent),
            rules.xfa_session_limit(opening_balance),
        )
        restricted_limits = {
            market: min(
                float(profile.maximum_mini_equivalent),
                rules.xfa_session_limit(opening_balance, market),
            )
            for market in rules.restricted_market_roots
        }
        open_positions: dict[str, _OpenPosition] = {}
        day_pnl = 0.0
        day_accepted = 0
        day_skipped = 0
        day_max_size = 0.0
        day_worst_equity = balance
        actions: list[tuple[int, int, int, str, RoutedTrade]] = []
        for trade in by_day.get(int(day), ()):
            priority = _priority(basket, trade.component_id)
            actions.append((trade.event.decision_ns, 1, priority, trade.event.event_id, trade))
            actions.append((trade.event.exit_ns, 0, priority, trade.event.event_id, trade))
        actions.sort(key=lambda row: (row[0], row[1], row[2], row[3]))

        for _timestamp, kind, _priority_value, event_id, trade in actions:
            if kind == 0:
                position = open_positions.pop(event_id, None)
                if position is None:
                    continue
                balance += position.net_pnl
                day_pnl += position.net_pnl
                contribution[trade.component_id] += position.net_pnl
                minimum_buffer = min(minimum_buffer, balance - floor)
                day_worst_equity = min(day_worst_equity, balance)
                if balance <= floor:
                    terminal = XfaTerminal.MLL_BREACHED
                    reason = "realized_xfa_mll_touch_or_breach"
                    break
                continue

            event_count += 1
            event = trade.event
            if not event.session_compliant or not event.contract_limit_compliant:
                terminal = XfaTerminal.HARD_RULE_FAILURE
                reason = (
                    "session_close_or_trading_hours_violation"
                    if not event.session_compliant
                    else "source_contract_limit_violation"
                )
                break
            if len(open_positions) >= min(
                basket.maximum_simultaneous_positions,
                profile.maximum_simultaneous_positions,
            ):
                skipped += 1
                day_skipped += 1
                skipped_reasons["MAXIMUM_SIMULTANEOUS_POSITIONS"] += 1
                continue
            if profile.same_market_exclusive and any(
                value.trade.market == trade.market
                and value.trade.event.exit_ns > event.decision_ns
                for value in open_positions.values()
            ):
                skipped += 1
                day_skipped += 1
                skipped_reasons["SAME_MARKET_CONFLICT"] += 1
                continue

            used = sum(value.mini_equivalent for value in open_positions.values())
            available = max(0.0, session_limit - used)
            root = _market_root(trade.market)
            if root in rules.restricted_market_roots:
                restricted_used = sum(
                    value.mini_equivalent
                    for value in open_positions.values()
                    if _market_root(value.trade.market) == root
                )
                available = min(
                    available,
                    max(0.0, restricted_limits[root] - restricted_used),
                )
            position = _position_with_limit(trade, available)
            if position is None:
                skipped += 1
                day_skipped += 1
                skipped_reasons["XFA_SCALING_PLAN"] += 1
                continue
            open_positions[event_id] = position
            accepted += 1
            day_accepted += 1
            total_cost += max(0.0, position.gross_pnl - position.net_pnl)
            maximum_size = max(
                maximum_size,
                sum(value.mini_equivalent for value in open_positions.values()),
            )
            day_max_size = max(
                day_max_size,
                sum(value.mini_equivalent for value in open_positions.values()),
            )
            conservative_loss = sum(
                min(value.worst_unrealized_pnl, 0.0)
                for value in open_positions.values()
            )
            intraday_low = balance + conservative_loss
            day_worst_equity = min(day_worst_equity, intraday_low)
            minimum_buffer = min(minimum_buffer, intraday_low - floor)
            if intraday_low <= floor:
                terminal = XfaTerminal.MLL_BREACHED
                reason = "intraday_unrealized_xfa_mll_touch_or_breach"
                break

        if terminal is None and open_positions:
            terminal = XfaTerminal.HARD_RULE_FAILURE
            reason = "open_position_remaining_after_session_close"
        traded = day_accepted > 0
        if traded:
            traded_days += 1
            traded_days_cycle += 1
            last_activity_day = int(day)
        if terminal is not None:
            ledger.append(
                {
                    "session_day": int(day),
                    "opening_balance": opening_balance,
                    "closing_balance": balance,
                    "mll_floor_open": floor_open,
                    "mll_floor_close": floor,
                    "scaling_limit_mini_equivalent": session_limit,
                    "restricted_market_limits": dict(sorted(restricted_limits.items())),
                    "maximum_mini_equivalent": day_max_size,
                    "day_pnl": day_pnl,
                    "worst_intraday_equity": day_worst_equity,
                    "traded": traded,
                    "accepted_events": day_accepted,
                    "skipped_events": day_skipped,
                    "payout_requested": False,
                    "terminal": terminal.value,
                }
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
            winning_days += 1
            total_qualifying_days += 1
        total_profit_cycle += day_pnl
        best_day_cycle = max(best_day_cycle, day_pnl)
        consistency_ratio = (
            best_day_cycle / total_profit_cycle
            if total_profit_cycle > 0.0 and best_day_cycle > 0.0
            else math.inf
        )
        if math.isfinite(consistency_ratio):
            maximum_consistency = max(maximum_consistency, consistency_ratio)
        if path == "STANDARD":
            payout_eligible = winning_days >= rules.xfa_standard_winning_days and (
                cycles == 0
                or balance - cycle_start_balance
                >= rules.later_standard_cycle_minimum_profit - 1e-12
            )
            payout_cap = rules.standard_payout_cap
        elif path == "CONSISTENCY":
            payout_eligible = bool(
                traded_days_cycle >= rules.xfa_consistency_traded_days
                and total_profit_cycle > 0.0
                and consistency_ratio <= rules.xfa_consistency_limit + 1e-12
            )
            payout_cap = rules.consistency_payout_cap
        else:
            raise ValueError(f"unsupported XFA path: {path}")

        payout_gross = 0.0
        payout_net = 0.0
        payout_executed = False
        if payout_eligible and balance > 0.0:
            payout_gross = min(balance * rules.payout_fraction, payout_cap)
            if payout_gross >= rules.minimum_payout - 1e-12:
                payout_net = payout_gross * rules.trader_profit_split
                if first_payout_day is None:
                    first_payout_day = elapsed
                gross_payout += payout_gross
                trader_net_payout += payout_net
                balance -= payout_gross
                floor = 0.0
                minimum_buffer = min(minimum_buffer, balance - floor)
                cycles += 1
                last_payout_elapsed = elapsed
                winning_days = 0
                traded_days_cycle = 0
                total_profit_cycle = 0.0
                best_day_cycle = 0.0
                cycle_start_balance = balance
                payout_executed = True

        ledger.append(
            {
                "session_day": int(day),
                "opening_balance": opening_balance,
                "closing_balance": balance,
                "mll_floor_open": floor_open,
                "mll_floor_close": floor,
                "scaling_limit_mini_equivalent": session_limit,
                "restricted_market_limits": dict(sorted(restricted_limits.items())),
                "maximum_mini_equivalent": day_max_size,
                "day_pnl": day_pnl,
                "worst_intraday_equity": day_worst_equity,
                "traded": traded,
                "accepted_events": day_accepted,
                "skipped_events": day_skipped,
                "winning_days_in_cycle": winning_days,
                "traded_days_in_cycle": traded_days_cycle,
                "profit_since_payout": balance - cycle_start_balance,
                "consistency_ratio_before_reset": (
                    consistency_ratio if math.isfinite(consistency_ratio) else None
                ),
                "payout_eligible": payout_eligible,
                "payout_requested": payout_executed,
                "gross_payout": payout_gross,
                "trader_net_payout": payout_net,
                "payout_cycles": cycles,
                "post_payout_mll_locked_at_zero": cycles > 0,
                "scaling_limit_frozen_for_session": True,
                "terminal": None,
            }
        )

    if terminal is None:
        if len(episode_days) < horizon:
            terminal = XfaTerminal.DATA_CENSORED
            reason = "available_chronology_ended_before_frozen_xfa_horizon"
        else:
            terminal = XfaTerminal.SURVIVED_HORIZON
            reason = "frozen_xfa_horizon_survived"
    post_payout_observed_days = (
        0
        if last_payout_elapsed is None
        else max(0, len(ledger) - last_payout_elapsed)
    )
    post_payout_survived = bool(
        cycles > 0
        and post_payout_observed_days > 0
        and terminal is XfaTerminal.SURVIVED_HORIZON
    )
    post_payout_censored = bool(
        cycles > 0
        and terminal is XfaTerminal.DATA_CENSORED
    )
    observed_days = len(ledger)
    payload = {
        "path": f"XFA_{path}",
        "terminal": terminal.value,
        "terminal_reason": reason,
        "start_day": int(start_day),
        "end_day": int(ledger[-1]["session_day"]),
        "requested_horizon_days": int(horizon),
        "observed_days": observed_days,
        "traded_days": traded_days,
        "event_count": event_count,
        "accepted_event_count": accepted,
        "skipped_event_count": skipped,
        "payout_eligible": cycles > 0,
        "payout_cycles": cycles,
        "gross_payout": gross_payout,
        "trader_net_payout": trader_net_payout,
        "first_payout_day": first_payout_day,
        "post_payout_survived": post_payout_survived,
        "post_payout_censored": post_payout_censored,
        "post_payout_observed_days": post_payout_observed_days,
        "ending_balance": balance,
        "ending_mll_floor": floor,
        "minimum_mll_buffer": minimum_buffer,
        "qualifying_winning_days": total_qualifying_days,
        "maximum_consistency_ratio": maximum_consistency,
        "maximum_mini_equivalent": maximum_size,
        "total_cost": total_cost,
        "skipped_reasons": dict(sorted(skipped_reasons.items())),
        "component_contribution": dict(sorted(contribution.items())),
        "daily_ledger": ledger,
        "calendar_inactivity_auditable": calendar_auditable,
        "payout_request_policy": "EARLIEST_ELIGIBLE_END_OF_DAY",
        "payout_path_selected_from_outcomes": False,
    }
    return XfaPathResult(
        path=str(payload["path"]),
        terminal=terminal,
        terminal_reason=reason,
        start_day=int(start_day),
        end_day=int(payload["end_day"]),
        requested_horizon_days=int(horizon),
        observed_days=observed_days,
        traded_days=traded_days,
        event_count=event_count,
        accepted_event_count=accepted,
        skipped_event_count=skipped,
        payout_eligible=cycles > 0,
        payout_cycles=cycles,
        gross_payout=float(gross_payout),
        trader_net_payout=float(trader_net_payout),
        first_payout_day=first_payout_day,
        post_payout_survived=post_payout_survived,
        post_payout_censored=post_payout_censored,
        post_payout_observed_days=post_payout_observed_days,
        ending_balance=float(balance),
        ending_mll_floor=float(floor),
        minimum_mll_buffer=float(minimum_buffer),
        qualifying_winning_days=total_qualifying_days,
        maximum_consistency_ratio=float(maximum_consistency),
        maximum_mini_equivalent=float(maximum_size),
        total_cost=float(total_cost),
        skipped_reasons=dict(sorted(skipped_reasons.items())),
        component_contribution=dict(sorted(contribution.items())),
        daily_ledger=tuple(ledger),
        calendar_inactivity_auditable=calendar_auditable,
        payout_request_policy="EARLIEST_ELIGIBLE_END_OF_DAY",
        payout_path_selected_from_outcomes=False,
        path_hash=_stable_hash(payload),
    )


def _combine_status(
    episode: AccountPolicyEpisode, *, data_censored: bool
) -> CombineLifecycleStatus:
    if episode.terminal is CombineTerminal.PASSED:
        return CombineLifecycleStatus.TARGET_REACHED
    if episode.terminal is CombineTerminal.MLL_BREACH:
        return CombineLifecycleStatus.MLL_BREACHED
    if episode.terminal is CombineTerminal.COMPLIANCE_FAILURE:
        return CombineLifecycleStatus.HARD_RULE_FAILURE
    return (
        CombineLifecycleStatus.DATA_CENSORED
        if data_censored
        else CombineLifecycleStatus.OPERATIONAL_HORIZON_NOT_REACHED
    )


def _zero_observation_xfa_path(
    *, path: str, horizon: int, rules: RuleSnapshot
) -> XfaPathResult:
    """Represent a successful Combine with no remaining XFA chronology.

    Reaching the XFA state is an economic transition even when the cached
    development window ends immediately afterwards.  The path is therefore
    persisted as DATA_CENSORED, never silently dropped or reclassified as a
    failed Combine.
    """

    if path not in {"STANDARD", "CONSISTENCY"}:
        raise ValueError(f"unsupported XFA path: {path}")
    payload = {
        "path": f"XFA_{path}",
        "terminal": XfaTerminal.DATA_CENSORED.value,
        "terminal_reason": "no_post_combine_session_available_for_xfa_replay",
        "start_day": None,
        "end_day": None,
        "requested_horizon_days": int(horizon),
        "observed_days": 0,
        "traded_days": 0,
        "event_count": 0,
        "accepted_event_count": 0,
        "skipped_event_count": 0,
        "payout_eligible": False,
        "payout_cycles": 0,
        "gross_payout": 0.0,
        "trader_net_payout": 0.0,
        "first_payout_day": None,
        "post_payout_survived": False,
        "post_payout_censored": False,
        "post_payout_observed_days": 0,
        "ending_balance": rules.xfa_starting_balance,
        "ending_mll_floor": rules.xfa_starting_floor,
        "minimum_mll_buffer": (
            rules.xfa_starting_balance - rules.xfa_starting_floor
        ),
        "qualifying_winning_days": 0,
        "maximum_consistency_ratio": 0.0,
        "maximum_mini_equivalent": 0.0,
        "total_cost": 0.0,
        "skipped_reasons": {},
        "component_contribution": {},
        "daily_ledger": [],
        "calendar_inactivity_auditable": False,
        "payout_request_policy": "EARLIEST_ELIGIBLE_END_OF_DAY",
        "payout_path_selected_from_outcomes": False,
    }
    return XfaPathResult(
        path=str(payload["path"]),
        terminal=XfaTerminal.DATA_CENSORED,
        terminal_reason=str(payload["terminal_reason"]),
        start_day=None,
        end_day=None,
        requested_horizon_days=int(horizon),
        observed_days=0,
        traded_days=0,
        event_count=0,
        accepted_event_count=0,
        skipped_event_count=0,
        payout_eligible=False,
        payout_cycles=0,
        gross_payout=0.0,
        trader_net_payout=0.0,
        first_payout_day=None,
        post_payout_survived=False,
        post_payout_censored=False,
        post_payout_observed_days=0,
        ending_balance=float(rules.xfa_starting_balance),
        ending_mll_floor=float(rules.xfa_starting_floor),
        minimum_mll_buffer=float(
            rules.xfa_starting_balance - rules.xfa_starting_floor
        ),
        qualifying_winning_days=0,
        maximum_consistency_ratio=0.0,
        maximum_mini_equivalent=0.0,
        total_cost=0.0,
        skipped_reasons={},
        component_contribution={},
        daily_ledger=(),
        calendar_inactivity_auditable=False,
        payout_request_policy="EARLIEST_ELIGIBLE_END_OF_DAY",
        payout_path_selected_from_outcomes=False,
        path_hash=_stable_hash(payload),
    )


def _scale_events(
    values: Mapping[str, Sequence[RoutedTrade]], multiplier: float
) -> dict[str, tuple[RoutedTrade, ...]]:
    return {
        key: tuple(_scale_trade(row, multiplier) for row in rows)
        for key, rows in values.items()
    }


def _enforce_combine_market_caps(
    values: Mapping[str, Sequence[RoutedTrade]], rules: RuleSnapshot
) -> dict[str, tuple[RoutedTrade, ...]]:
    restricted_cap = float(rules.restricted_market_scaling_tiers[-1][1])
    output: dict[str, tuple[RoutedTrade, ...]] = {}
    for component_id, rows in values.items():
        capped: list[RoutedTrade] = []
        for row in rows:
            if _market_root(row.market) not in rules.restricted_market_roots:
                capped.append(row)
                continue
            position = _position_with_limit(row, restricted_cap)
            if position is None:
                capped.append(
                    replace(
                        row,
                        event=replace(
                            row.event,
                            contract_limit_compliant=False,
                        ),
                    )
                )
                continue
            ratio = position.quantity / row.event.quantity
            capped.append(
                replace(
                    row,
                    event=replace(
                        row.event,
                        net_pnl=float(row.event.net_pnl * ratio),
                        gross_pnl=float(row.event.gross_pnl * ratio),
                        worst_unrealized_pnl=float(
                            row.event.worst_unrealized_pnl * ratio
                        ),
                        best_unrealized_pnl=float(
                            row.event.best_unrealized_pnl * ratio
                        ),
                        quantity=position.quantity,
                        mini_equivalent=position.mini_equivalent,
                    ),
                )
            )
        output[component_id] = tuple(capped)
    return output


def _scale_trade(row: RoutedTrade, multiplier: float) -> RoutedTrade:
    quantity = max(1, int(math.floor(row.event.quantity * multiplier + 1e-12)))
    ratio = quantity / row.event.quantity
    return replace(
        row,
        event=replace(
            row.event,
            net_pnl=float(row.event.net_pnl * ratio),
            gross_pnl=float(row.event.gross_pnl * ratio),
            worst_unrealized_pnl=float(row.event.worst_unrealized_pnl * ratio),
            best_unrealized_pnl=float(row.event.best_unrealized_pnl * ratio),
            quantity=quantity,
            mini_equivalent=float(row.event.mini_equivalent * ratio),
        ),
    )


def _position_with_limit(trade: RoutedTrade, available: float) -> _OpenPosition | None:
    event = trade.event
    per_contract = event.mini_equivalent / event.quantity
    if per_contract <= 0.0:
        return None
    quantity = min(event.quantity, int(math.floor((available + 1e-12) / per_contract)))
    if quantity < 1:
        return None
    ratio = quantity / event.quantity
    return _OpenPosition(
        trade=trade,
        quantity=quantity,
        mini_equivalent=float(event.mini_equivalent * ratio),
        net_pnl=float(event.net_pnl * ratio),
        gross_pnl=float(event.gross_pnl * ratio),
        worst_unrealized_pnl=float(event.worst_unrealized_pnl * ratio),
        best_unrealized_pnl=float(event.best_unrealized_pnl * ratio),
    )


def _validate_source_events(
    component_events: Mapping[str, Sequence[RoutedTrade]], basket: BasketPolicy
) -> None:
    for component_id in basket.component_ids:
        for row in component_events[component_id]:
            if row.component_id != component_id:
                raise ValueError("event-ledger component key mismatch")
            if not isinstance(row.event, TradePathEvent):
                raise TypeError("lifecycle replay requires RoutedTrade events")


def _priority(basket: BasketPolicy, component_id: str) -> int:
    priorities = basket.component_priority or basket.component_ids
    try:
        return priorities.index(component_id)
    except ValueError:
        return len(priorities)


def _market_root(market: str) -> str:
    raw = market.upper().strip()
    aliases = {"MCL": "CL", "MGC": "GC"}
    return aliases.get(raw, raw)


def _validate_tiers(
    tiers: Sequence[tuple[float, float]], *, expected_last: float
) -> None:
    if not tiers or any(right <= 0.0 for _, right in tiers):
        raise ValueError("scaling tiers must be non-empty and positive")
    if any(right[0] <= left[0] for left, right in zip(tiers, tiers[1:])):
        raise ValueError("scaling tier balances must increase strictly")
    if float(tiers[-1][1]) != expected_last:
        raise ValueError("scaling plan terminal limit differs from frozen rule")


def _tier_value(tiers: Sequence[tuple[float, float]], balance: float) -> float:
    selected = float(tiers[0][1])
    for lower, limit in tiers:
        if balance + 1e-12 < lower:
            break
        selected = float(limit)
    return selected


def _parse_session_date(value: int) -> date | None:
    raw = str(int(value))
    if len(raw) == 8:
        try:
            return date(int(raw[:4]), int(raw[4:6]), int(raw[6:]))
        except ValueError:
            return None
    # The production kernel's canonical session-day representation is an
    # integer offset from 1970-01-01.  Support it explicitly so the 30-day XFA
    # inactivity rule remains active in real campaign replay, not just in
    # YYYYMMDD unit fixtures.
    try:
        return date(1970, 1, 1) + timedelta(days=int(value))
    except (OverflowError, ValueError):
        return None


def _calendar_gap(left: int, right: int) -> int | None:
    start = _parse_session_date(left)
    end = _parse_session_date(right)
    if start is None or end is None:
        return None
    return (end - start).days


def _stable_hash(value: Any) -> str:
    raw = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


__all__ = [
    "CombineLifecycleStatus",
    "CombineToXfaEpisodeResult",
    "FrozenRiskProfile",
    "LIFECYCLE_VERSION",
    "RULE_SNAPSHOT_VERSION",
    "RuleSnapshot",
    "UNREALIZED_AGGREGATION_SEMANTICS",
    "XfaPathResult",
    "XfaTerminal",
    "official_rule_snapshot_2026_07_15",
    "run_combine_to_xfa_episode",
]
