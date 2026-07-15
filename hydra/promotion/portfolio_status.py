"""Evidence-honest sleeve and account-book promotion semantics.

Family verdicts govern further *generation*.  They are deliberately absent
from the candidate quality gates below: an immutable sleeve keeps its own
economic evidence even when the grammar that produced it is terminal.  No
state in this module authorizes orders, a broker connection, protected data,
or independent-confirmation claims.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

from hydra.economic_evolution.schema import stable_hash


PORTFOLIO_PROMOTION_VERSION = "hydra_portfolio_promotion_v1"


class PortfolioStatus(StrEnum):
    SLEEVE_ECONOMICALLY_ELIGIBLE = "SLEEVE_ECONOMICALLY_ELIGIBLE"
    SLEEVE_COMBINE_COMPONENT = "SLEEVE_COMBINE_COMPONENT"
    SLEEVE_XFA_COMPONENT = "SLEEVE_XFA_COMPONENT"
    COMBINE_BOOK_CANDIDATE = "COMBINE_BOOK_CANDIDATE"
    COMBINE_BOOK_GRADUATED = "COMBINE_BOOK_GRADUATED"
    XFA_BOOK_ACTIVE = "XFA_BOOK_ACTIVE"
    PAYOUT_PATH_CANDIDATE = "PAYOUT_PATH_CANDIDATE"
    FORWARD_SHADOW_CANDIDATE = "FORWARD_SHADOW_CANDIDATE"
    PAPER_SHADOW_READY = "PAPER_SHADOW_READY"


@dataclass(frozen=True, slots=True)
class PortfolioPromotionPolicy:
    minimum_combine_passes: int = 3
    minimum_pass_blocks: int = 2
    minimum_combine_starts: int = 48
    minimum_normal_pass_rate: float = 0.10
    maximum_mll_breach_rate: float = 0.10
    maximum_block_profit_share: float = 0.50
    maximum_sleeve_profit_share: float = 0.50
    minimum_successful_combine_paths_for_payout: int = 2
    minimum_post_payout_survival_rate: float = 0.01
    independent_forward_confirmation_required_for_paper: bool = True
    status_inheritance_allowed: bool = False
    live_trading_allowed: bool = False
    broker_connection_allowed: bool = False
    orders_allowed: bool = False

    def __post_init__(self) -> None:
        for value in (
            self.minimum_normal_pass_rate,
            self.maximum_mll_breach_rate,
            self.maximum_block_profit_share,
            self.maximum_sleeve_profit_share,
            self.minimum_post_payout_survival_rate,
        ):
            if not 0.0 <= value <= 1.0:
                raise ValueError("portfolio promotion rates must be in [0,1]")
        if min(
            self.minimum_combine_passes,
            self.minimum_pass_blocks,
            self.minimum_combine_starts,
            self.minimum_successful_combine_paths_for_payout,
        ) < 1:
            raise ValueError("portfolio promotion count gates must be positive")
        if (
            self.status_inheritance_allowed
            or self.live_trading_allowed
            or self.broker_connection_allowed
            or self.orders_allowed
        ):
            raise ValueError("unsafe portfolio promotion authority")

    def to_dict(self) -> dict[str, Any]:
        return {"version": PORTFOLIO_PROMOTION_VERSION, **asdict(self)}

    @property
    def fingerprint(self) -> str:
        return stable_hash(self.to_dict())


FROZEN_PORTFOLIO_PROMOTION_POLICY = PortfolioPromotionPolicy()


@dataclass(frozen=True, slots=True)
class SleeveEvidence:
    sleeve_id: str
    immutable_fingerprint: str
    family_id: str
    family_verdict: str
    behavioral_cluster: str
    role: str
    normal_net_pnl: float
    stressed_net_pnl: float
    mll_breach_rate: float
    event_count: int
    maximum_single_event_profit_share: float
    complete_trade_ledger: bool
    complete_evidence_bundle: bool
    executable_specification: bool
    hard_execution_or_data_defect: bool = False
    behavioral_clone: bool = False
    combine_role_supported: bool = True
    xfa_role_supported: bool = True

    def __post_init__(self) -> None:
        for name in ("sleeve_id", "family_id", "family_verdict", "behavioral_cluster", "role"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"sleeve evidence requires {name}")
        if len(self.immutable_fingerprint) != 64:
            raise ValueError("sleeve fingerprint must be SHA-256 shaped")
        if self.event_count < 1:
            raise ValueError("sleeve must contain at least one event")
        for value in (self.normal_net_pnl, self.stressed_net_pnl):
            if not math.isfinite(value):
                raise ValueError("sleeve economics must be finite")
        for value in (self.mll_breach_rate, self.maximum_single_event_profit_share):
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError("sleeve rates must be finite in [0,1]")

    @property
    def economically_eligible(self) -> bool:
        return bool(
            self.normal_net_pnl > 0.0
            and self.stressed_net_pnl >= 0.0
            and self.mll_breach_rate <= 0.15
            and self.maximum_single_event_profit_share <= 0.50
            and self.complete_trade_ledger
            and self.complete_evidence_bundle
            and self.executable_specification
            and not self.hard_execution_or_data_defect
            and not self.behavioral_clone
        )

    @property
    def statuses(self) -> tuple[PortfolioStatus, ...]:
        if not self.economically_eligible:
            return ()
        output = [PortfolioStatus.SLEEVE_ECONOMICALLY_ELIGIBLE]
        if self.combine_role_supported:
            output.append(PortfolioStatus.SLEEVE_COMBINE_COMPONENT)
        if self.xfa_role_supported:
            output.append(PortfolioStatus.SLEEVE_XFA_COMPONENT)
        return tuple(output)


@dataclass(frozen=True, slots=True)
class BookEvidence:
    book_pair_id: str
    combine_starts: int
    combine_evaluable_starts: int
    normal_combine_passes: int
    stressed_combine_passes: int
    pass_block_ids: tuple[str, ...]
    stressed_net_pnl: float
    stressed_economically_defensible: bool
    mll_breach_rate: float
    consistency_acceptable: bool
    maximum_block_profit_share: float
    maximum_sleeve_profit_share: float
    xfa_paths_started: int
    unique_xfa_start_days: tuple[int, ...]
    payout_eligible_paths: int
    payout_cycles: int
    expected_trader_net_payout_per_attempt: float
    post_payout_survival_rate: float
    complete_evidence_bundle: bool
    immutable_books_complete: bool
    hard_integrity_defect: bool = False
    forward_no_order_package_complete: bool = False
    independent_confirmation_complete: bool = False
    forward_confirmation_complete: bool = False
    paper_shadow_contract_complete: bool = False

    def __post_init__(self) -> None:
        if not self.book_pair_id.strip():
            raise ValueError("book pair identity is required")
        counts = (
            self.combine_starts,
            self.combine_evaluable_starts,
            self.normal_combine_passes,
            self.stressed_combine_passes,
            self.xfa_paths_started,
            self.payout_eligible_paths,
            self.payout_cycles,
        )
        if any(value < 0 for value in counts):
            raise ValueError("book lifecycle counts cannot be negative")
        if max(self.normal_combine_passes, self.stressed_combine_passes) > self.combine_starts:
            raise ValueError("book passes cannot exceed starts")
        if self.combine_evaluable_starts > self.combine_starts:
            raise ValueError("evaluable Combine starts cannot exceed observed starts")
        if (
            any(day < 0 for day in self.unique_xfa_start_days)
            or len(set(self.unique_xfa_start_days)) != len(self.unique_xfa_start_days)
            or len(self.unique_xfa_start_days) > self.xfa_paths_started
        ):
            raise ValueError("unique XFA start days do not reconcile")
        for value in (
            self.mll_breach_rate,
            self.maximum_block_profit_share,
            self.maximum_sleeve_profit_share,
            self.post_payout_survival_rate,
        ):
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError("book rates must be finite in [0,1]")
        if not math.isfinite(self.stressed_net_pnl) or not math.isfinite(
            self.expected_trader_net_payout_per_attempt
        ):
            raise ValueError("book economics must be finite")

    @property
    def normal_pass_rate(self) -> float:
        # Graduation is intentionally conservative: early data censoring is
        # disclosed but does not shrink the frozen-start denominator.
        return self.normal_combine_passes / max(self.combine_starts, 1)


def decide_book_statuses(
    evidence: BookEvidence,
    policy: PortfolioPromotionPolicy = FROZEN_PORTFOLIO_PROMOTION_POLICY,
) -> tuple[PortfolioStatus, ...]:
    """Return only stages actually established by candidate-level evidence."""

    if evidence.hard_integrity_defect or not (
        evidence.complete_evidence_bundle and evidence.immutable_books_complete
    ):
        return ()
    statuses = [PortfolioStatus.COMBINE_BOOK_CANDIDATE]
    graduated = bool(
        evidence.combine_starts >= policy.minimum_combine_starts
        and evidence.normal_combine_passes >= policy.minimum_combine_passes
        and len(set(evidence.pass_block_ids)) >= policy.minimum_pass_blocks
        and evidence.normal_pass_rate >= policy.minimum_normal_pass_rate
        and evidence.stressed_net_pnl > 0.0
        and evidence.stressed_economically_defensible
        and evidence.mll_breach_rate <= policy.maximum_mll_breach_rate
        and evidence.consistency_acceptable
        and evidence.maximum_block_profit_share <= policy.maximum_block_profit_share
        and evidence.maximum_sleeve_profit_share <= policy.maximum_sleeve_profit_share
    )
    if graduated:
        statuses.append(PortfolioStatus.COMBINE_BOOK_GRADUATED)
    # XFA is a lifecycle state reached mechanically by any successful Combine
    # path; it is not a claim that the book has passed the broader graduation
    # gate.  Keeping those states separate prevents family/candidate filters
    # from erasing actual transition evidence.
    if evidence.xfa_paths_started > 0:
        statuses.append(PortfolioStatus.XFA_BOOK_ACTIVE)
    if (
        len(evidence.unique_xfa_start_days)
        >= policy.minimum_successful_combine_paths_for_payout
        and evidence.payout_eligible_paths > 0
        and evidence.payout_cycles > 0
        and evidence.expected_trader_net_payout_per_attempt > 0.0
        and evidence.post_payout_survival_rate
        >= policy.minimum_post_payout_survival_rate
    ):
        statuses.append(PortfolioStatus.PAYOUT_PATH_CANDIDATE)
    if evidence.forward_no_order_package_complete:
        statuses.append(PortfolioStatus.FORWARD_SHADOW_CANDIDATE)
    if (
        evidence.independent_confirmation_complete
        and evidence.forward_confirmation_complete
        and evidence.paper_shadow_contract_complete
    ):
        statuses.append(PortfolioStatus.PAPER_SHADOW_READY)
    return tuple(statuses)


__all__ = [
    "BookEvidence",
    "FROZEN_PORTFOLIO_PROMOTION_POLICY",
    "PORTFOLIO_PROMOTION_VERSION",
    "PortfolioPromotionPolicy",
    "PortfolioStatus",
    "SleeveEvidence",
    "decide_book_statuses",
]
