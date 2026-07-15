"""Portfolio-first Combine to XFA lifecycle orchestration.

The Combine and XFA account books are separate immutable declarations.  Both
books, their risk overlays, and every referenced sleeve timeline are hashed
before replay.  A successful Combine may transition to a different XFA book,
but no membership or path is selected from episode outcomes.

The account mechanics are deliberately delegated to ``combine_to_xfa`` so the
portfolio lane cannot acquire competing MLL, scaling, payout, or censoring
semantics.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, replace
from enum import StrEnum
from typing import Any, Mapping, Sequence

from hydra.account_policy.basket import (
    AccountPolicyEpisode,
    RoutedTrade,
    run_shared_account_episode,
)
from hydra.account_policy.schema import (
    AccountPolicyKind,
    BasketPolicy,
    ControllerPolicy,
    stable_hash,
)
from hydra.propfirm.combine_to_xfa import (
    CombineLifecycleStatus,
    FrozenRiskProfile,
    RuleSnapshot,
    UNREALIZED_AGGREGATION_SEMANTICS,
    XfaPathResult,
    _combine_status,
    _enforce_combine_market_caps,
    _run_xfa_path,
    _scale_events,
    _stable_hash,
    _zero_observation_xfa_path,
    official_rule_snapshot_2026_07_15,
)


PORTFOLIO_LIFECYCLE_VERSION = "hydra_portfolio_combine_to_xfa_v1"
PORTFOLIO_ACCOUNT_POLICY_VERSION = "hydra_account_policy_v7_2_portfolio_first_v1"


class PortfolioLifecycleError(ValueError):
    """Raised when a preregistered book or sleeve timeline drifts."""


class PortfolioBookRole(StrEnum):
    COMBINE_BOOK = "COMBINE_BOOK"
    XFA_BOOK = "XFA_BOOK"


@dataclass(frozen=True, slots=True)
class PortfolioBasketPolicy:
    """Portfolio-local 1..6 sleeve adapter; legacy BasketPolicy stays frozen."""

    policy_id: str
    component_ids: tuple[str, ...]
    archetype: str
    maximum_simultaneous_positions: int = 4
    maximum_mini_equivalent: int = 15
    conflict_policy: str = "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE"
    component_priority: tuple[str, ...] = ()
    policy_version: str = PORTFOLIO_ACCOUNT_POLICY_VERSION

    def __post_init__(self) -> None:
        if not self.policy_id or not 1 <= len(self.component_ids) <= 6:
            raise PortfolioLifecycleError("portfolio basket requires 1..6 sleeves")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise PortfolioLifecycleError("portfolio basket sleeves must be unique")
        priority = self.component_priority or self.component_ids
        if tuple(priority) != tuple(self.component_ids):
            raise PortfolioLifecycleError("portfolio priority must be frozen in sleeve order")
        if not 1 <= self.maximum_simultaneous_positions <= len(self.component_ids):
            raise PortfolioLifecycleError("portfolio simultaneous-position cap drift")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise PortfolioLifecycleError("portfolio mini-equivalent cap drift")
        if self.conflict_policy != "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE":
            raise PortfolioLifecycleError("portfolio conflict policy must be priority")
        if self.policy_version != PORTFOLIO_ACCOUNT_POLICY_VERSION:
            raise PortfolioLifecycleError(
                "portfolio account policy must use priority-aware V7.2 ordering"
            )

    @property
    def kind(self) -> AccountPolicyKind:
        return (
            AccountPolicyKind.INDIVIDUAL
            if len(self.component_ids) == 1
            else AccountPolicyKind.STATIC_BASKET
        )

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["component_ids"] = list(self.component_ids)
        row["component_priority"] = list(self.component_priority or self.component_ids)
        row["kind"] = self.kind.value
        return row


@dataclass(frozen=True, slots=True)
class FrozenPortfolioBook:
    """One immutable account book frozen before the first outcome is read."""

    book_id: str
    role: PortfolioBookRole
    basket: BasketPolicy | PortfolioBasketPolicy
    risk_profile: FrozenRiskProfile
    sleeve_timeline_hashes: tuple[tuple[str, str], ...]
    sleeve_risk_multipliers: tuple[tuple[str, float], ...] = ()
    controller: ControllerPolicy | None = None
    book_version: str = PORTFOLIO_LIFECYCLE_VERSION

    def __post_init__(self) -> None:
        if not self.book_id.strip():
            raise PortfolioLifecycleError("portfolio book_id must be non-empty")
        try:
            role = PortfolioBookRole(self.role)
        except ValueError as exc:
            raise PortfolioLifecycleError("unsupported portfolio book role") from exc
        object.__setattr__(self, "role", role)
        if self.book_version != PORTFOLIO_LIFECYCLE_VERSION:
            raise PortfolioLifecycleError("portfolio book version drift")
        hashes = tuple(self.sleeve_timeline_hashes)
        if hashes != tuple(sorted(hashes)):
            raise PortfolioLifecycleError("sleeve timeline hashes must be sorted")
        ids = tuple(component_id for component_id, _value in hashes)
        if ids != tuple(sorted(self.basket.component_ids)):
            raise PortfolioLifecycleError(
                "book timeline hashes must cover every basket sleeve exactly"
            )
        if len(ids) != len(set(ids)) or any(
            not _is_sha256(value) for _component_id, value in hashes
        ):
            raise PortfolioLifecycleError("invalid sleeve timeline hash declaration")
        risk = tuple(self.sleeve_risk_multipliers) or tuple(
            (component_id, 1.0) for component_id in sorted(ids)
        )
        if (
            tuple(component_id for component_id, _value in risk)
            != tuple(sorted(ids))
            or any(
                not math.isfinite(float(value)) or float(value) <= 0.0
                for _component_id, value in risk
            )
        ):
            raise PortfolioLifecycleError(
                "book risk multipliers must cover every sleeve once"
            )
        object.__setattr__(self, "sleeve_risk_multipliers", risk)
        if self.controller is not None:
            if role is PortfolioBookRole.XFA_BOOK:
                raise PortfolioLifecycleError(
                    "XFA book uses the frozen static risk overlay, not a Combine controller"
                )
            if self.controller.basket_policy_id != self.basket.policy_id:
                raise PortfolioLifecycleError(
                    "Combine controller does not reference its frozen basket"
                )
            if set(self.controller.component_priority) != set(
                self.basket.component_ids
            ):
                raise PortfolioLifecycleError(
                    "Combine controller membership differs from its frozen basket"
                )
            if (
                self.controller.maximum_simultaneous_positions
                > self.risk_profile.maximum_simultaneous_positions
                or self.controller.maximum_mini_equivalent
                > self.risk_profile.maximum_mini_equivalent
            ):
                raise PortfolioLifecycleError(
                    "Combine controller exceeds its frozen book risk caps"
                )
        if (
            role is PortfolioBookRole.XFA_BOOK
            and not self.risk_profile.clip_to_xfa_scaling_plan
        ):
            raise PortfolioLifecycleError(
                "XFA book must enforce the official session scaling plan"
            )

    @property
    def sleeve_ids(self) -> tuple[str, ...]:
        return tuple(self.basket.component_ids)

    @property
    def fingerprint(self) -> str:
        return _stable_hash(self._payload())

    def _payload(self) -> dict[str, Any]:
        return {
            "book_id": self.book_id,
            "role": self.role.value,
            "basket": self.basket.to_dict(),
            "risk_profile": self.risk_profile.to_dict(),
            "sleeve_timeline_hashes": [
                [component_id, value]
                for component_id, value in self.sleeve_timeline_hashes
            ],
            "sleeve_risk_multipliers": [
                [component_id, value]
                for component_id, value in self.sleeve_risk_multipliers
            ],
            "controller": (
                self.controller.to_dict() if self.controller is not None else None
            ),
            "book_version": self.book_version,
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._payload(), "fingerprint": self.fingerprint}


@dataclass(frozen=True, slots=True)
class PortfolioCombineToXfaResult:
    schema: str
    lifecycle_version: str
    start_day: int
    combine_horizon_days: int
    xfa_horizon_days: int
    combine_book: FrozenPortfolioBook
    xfa_book: FrozenPortfolioBook
    combine_status: CombineLifecycleStatus
    combine_episode: AccountPolicyEpisode
    xfa_started: bool
    xfa_start_day: int | None
    xfa_standard: XfaPathResult | None
    xfa_consistency: XfaPathResult | None
    rule_snapshot: RuleSnapshot
    union_timeline_hash: str
    evidence_hash: str

    def to_dict(self) -> dict[str, Any]:
        combine_ids = set(self.combine_book.sleeve_ids)
        xfa_ids = set(self.xfa_book.sleeve_ids)
        return {
            "schema": self.schema,
            "lifecycle_version": self.lifecycle_version,
            "start_day": self.start_day,
            "combine_horizon_days": self.combine_horizon_days,
            "xfa_horizon_days": self.xfa_horizon_days,
            "combine_book": self.combine_book.to_dict(),
            "xfa_book": self.xfa_book.to_dict(),
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
            "union_timeline_hash": self.union_timeline_hash,
            "evidence_hash": self.evidence_hash,
            "book_membership_changed_at_transition": combine_ids != xfa_ids,
            "xfa_book_is_subset_of_combine_book": xfa_ids.issubset(combine_ids),
            "books_frozen_before_replay": True,
            "sleeve_timelines_immutable": True,
            "timeline_hashes_verified_before_replay": True,
            "xfa_book_selected_from_outcomes": False,
            "payout_path_oracle_used": False,
            "unrealized_aggregation_semantics": UNREALIZED_AGGREGATION_SEMANTICS,
            "combine_profit_transferred_to_xfa": False,
            "underlying_sleeve_signals_mutated": False,
            "development_only": True,
            "broker_connection_count": 0,
            "order_count": 0,
            "outbound_order_capability": False,
        }


def freeze_portfolio_book(
    *,
    book_id: str,
    role: PortfolioBookRole | str,
    basket: BasketPolicy | PortfolioBasketPolicy,
    risk_profile: FrozenRiskProfile,
    sleeve_timelines: Mapping[str, Sequence[RoutedTrade]],
    sleeve_risk_multipliers: Mapping[str, float] | None = None,
    controller: ControllerPolicy | None = None,
) -> FrozenPortfolioBook:
    """Freeze a book and the exact timelines it is allowed to consume."""

    resolved_role = PortfolioBookRole(role)
    selected = _select_and_verify_timelines(sleeve_timelines, basket.component_ids)
    hashes = tuple(
        (component_id, _timeline_hash(component_id, selected[component_id]))
        for component_id in sorted(selected)
    )
    declared_risk = sleeve_risk_multipliers or {
        component_id: 1.0 for component_id in selected
    }
    if set(declared_risk) != set(selected):
        raise PortfolioLifecycleError(
            "book risk multiplier membership differs from its timelines"
        )
    return FrozenPortfolioBook(
        book_id=book_id,
        role=resolved_role,
        basket=basket,
        risk_profile=risk_profile,
        sleeve_timeline_hashes=hashes,
        sleeve_risk_multipliers=tuple(
            (component_id, float(declared_risk[component_id]))
            for component_id in sorted(declared_risk)
        ),
        controller=controller,
    )


def run_portfolio_combine_to_xfa_episode(
    sleeve_timelines: Mapping[str, Sequence[RoutedTrade]],
    eligible_session_days: Sequence[int],
    *,
    combine_book: FrozenPortfolioBook,
    xfa_book: FrozenPortfolioBook,
    start_day: int,
    combine_horizon_days: int = 90,
    xfa_horizon_days: int = 120,
    rule_snapshot: RuleSnapshot | None = None,
    xfa_eligible_session_days: Sequence[int] | None = None,
) -> PortfolioCombineToXfaResult:
    """Replay a preregistered COMBINE_BOOK -> XFA_BOOK transition.

    ``xfa_book`` is supplied and verified before the Combine replay begins.
    The only post-Combine branch is mechanical: a pass starts both frozen XFA
    payout paths; any other terminal state starts neither.
    """

    if combine_book.role is not PortfolioBookRole.COMBINE_BOOK:
        raise PortfolioLifecycleError("combine_book has the wrong role")
    if xfa_book.role is not PortfolioBookRole.XFA_BOOK:
        raise PortfolioLifecycleError("xfa_book has the wrong role")
    if combine_book.book_id == xfa_book.book_id:
        raise PortfolioLifecycleError("Combine and XFA books require distinct IDs")
    if combine_horizon_days < 1 or xfa_horizon_days < 1:
        raise PortfolioLifecycleError("portfolio lifecycle horizons must be positive")
    days = tuple(sorted({int(value) for value in eligible_session_days}))
    if start_day not in days:
        raise PortfolioLifecycleError("start_day must be an eligible session day")
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
        raise PortfolioLifecycleError("XFA chronology cannot be empty")
    rules = rule_snapshot or official_rule_snapshot_2026_07_15()

    combine_events = _verify_book_against_timelines(combine_book, sleeve_timelines)
    xfa_events = _verify_book_against_timelines(xfa_book, sleeve_timelines)
    declared_timeline_hashes = dict(combine_book.sleeve_timeline_hashes)
    for component_id, value in xfa_book.sleeve_timeline_hashes:
        if (
            component_id in declared_timeline_hashes
            and declared_timeline_hashes[component_id] != value
        ):
            raise PortfolioLifecycleError(
                f"shared sleeve timeline declaration differs: {component_id}"
            )
        declared_timeline_hashes[component_id] = value
    union_hash = _stable_hash(
        {
            "combine_book_fingerprint": combine_book.fingerprint,
            "xfa_book_fingerprint": xfa_book.fingerprint,
            "timelines": dict(sorted(declared_timeline_hashes.items())),
            "combine_eligible_session_days": list(days),
            "xfa_eligible_session_days": list(xfa_days),
        }
    )

    scaled_combine = _enforce_combine_market_caps(
        _scale_book_events(combine_events, combine_book), rules
    )
    combine_basket = replace(
        combine_book.basket,
        maximum_simultaneous_positions=min(
            combine_book.basket.maximum_simultaneous_positions,
            combine_book.risk_profile.maximum_simultaneous_positions,
        ),
        maximum_mini_equivalent=min(
            combine_book.basket.maximum_mini_equivalent,
            combine_book.risk_profile.maximum_mini_equivalent,
            rules.combine_maximum_mini_equivalent,
        ),
    )
    combine = run_shared_account_episode(
        scaled_combine,
        days,
        basket=combine_basket,
        controller=combine_book.controller,
        start_day=int(start_day),
        maximum_duration_days=int(combine_horizon_days),
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
        scaled_xfa = _scale_book_events(xfa_events, xfa_book)
        standard = _run_xfa_path(
            scaled_xfa,
            xfa_days,
            basket=xfa_book.basket,
            profile=xfa_book.risk_profile,
            rules=rules,
            start_day=xfa_start_day,
            horizon=xfa_horizon_days,
            path="STANDARD",
        )
        consistency = _run_xfa_path(
            scaled_xfa,
            xfa_days,
            basket=xfa_book.basket,
            profile=xfa_book.risk_profile,
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

    result = PortfolioCombineToXfaResult(
        schema="hydra_portfolio_combine_to_xfa_episode_v1",
        lifecycle_version=PORTFOLIO_LIFECYCLE_VERSION,
        start_day=int(start_day),
        combine_horizon_days=int(combine_horizon_days),
        xfa_horizon_days=int(xfa_horizon_days),
        combine_book=combine_book,
        xfa_book=xfa_book,
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
        union_timeline_hash=union_hash,
        evidence_hash="",
    )
    payload = result.to_dict()
    payload.pop("evidence_hash")
    return replace(result, evidence_hash=_stable_hash(payload))


def _scale_book_events(
    values: Mapping[str, Sequence[RoutedTrade]], book: FrozenPortfolioBook
) -> dict[str, tuple[RoutedTrade, ...]]:
    risk = dict(book.sleeve_risk_multipliers)
    return {
        component_id: _scale_events(
            {component_id: rows},
            float(book.risk_profile.risk_multiplier) * float(risk[component_id]),
        )[component_id]
        for component_id, rows in values.items()
    }


def _verify_book_against_timelines(
    book: FrozenPortfolioBook,
    timelines: Mapping[str, Sequence[RoutedTrade]],
) -> dict[str, tuple[RoutedTrade, ...]]:
    selected = _select_and_verify_timelines(timelines, book.sleeve_ids)
    observed = tuple(
        (component_id, _timeline_hash(component_id, selected[component_id]))
        for component_id in sorted(selected)
    )
    if observed != book.sleeve_timeline_hashes:
        raise PortfolioLifecycleError(
            f"immutable sleeve timeline drift for {book.book_id}"
        )
    return selected


def _select_and_verify_timelines(
    timelines: Mapping[str, Sequence[RoutedTrade]],
    component_ids: Sequence[str],
) -> dict[str, tuple[RoutedTrade, ...]]:
    missing = sorted(set(component_ids) - set(timelines))
    if missing:
        raise PortfolioLifecycleError(
            "portfolio book references missing sleeve timelines: " + ",".join(missing)
        )
    output: dict[str, tuple[RoutedTrade, ...]] = {}
    event_owners: dict[str, str] = {}
    for component_id in component_ids:
        raw = timelines[component_id]
        if not isinstance(raw, tuple):
            raise PortfolioLifecycleError(
                f"sleeve timeline must be an immutable tuple: {component_id}"
            )
        rows = tuple(raw)
        if any(row.component_id != component_id for row in rows):
            raise PortfolioLifecycleError("sleeve timeline component identity drift")
        sort_keys = [
            (
                int(row.event.session_day),
                int(row.event.decision_ns),
                str(row.event.event_id),
            )
            for row in rows
        ]
        if sort_keys != sorted(sort_keys) or len(sort_keys) != len(set(sort_keys)):
            raise PortfolioLifecycleError(
                f"sleeve timeline is not unique chronological evidence: {component_id}"
            )
        for row in rows:
            owner = event_owners.get(row.event.event_id)
            if owner is not None:
                raise PortfolioLifecycleError(
                    "event_id collides across portfolio sleeves: "
                    f"{row.event.event_id} ({owner}, {component_id})"
                )
            event_owners[row.event.event_id] = component_id
            values = (
                row.event.net_pnl,
                row.event.gross_pnl,
                row.event.worst_unrealized_pnl,
                row.event.best_unrealized_pnl,
                row.event.mini_equivalent,
            )
            if any(not math.isfinite(float(value)) for value in values):
                raise PortfolioLifecycleError("non-finite sleeve timeline value")
        output[component_id] = rows
    return output


def _timeline_hash(
    component_id: str, rows: Sequence[RoutedTrade]
) -> str:
    return _stable_hash(
        {
            "component_id": component_id,
            "events": [row.to_dict() for row in rows],
        }
    )


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


__all__ = [
    "FrozenPortfolioBook",
    "PORTFOLIO_ACCOUNT_POLICY_VERSION",
    "PORTFOLIO_LIFECYCLE_VERSION",
    "PortfolioBasketPolicy",
    "PortfolioBookRole",
    "PortfolioCombineToXfaResult",
    "PortfolioLifecycleError",
    "freeze_portfolio_book",
    "run_portfolio_combine_to_xfa_episode",
]
