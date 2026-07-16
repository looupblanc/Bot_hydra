"""Canonical, path-scoped XFA payout events and accounting reconciliation.

The XFA Standard and Consistency paths are alternative frozen choices for one
successful Combine transition.  They are deliberately keyed separately and
must never be added together as if both payouts were realised by one account.

This module derives payout events only from executed end-of-day requests.  An
eligible amount below the frozen minimum is an auditable *candidate*, not a
payout request or payout event.  A narrowly-defined compatibility flag can
recognise the historical marker defect where that sub-minimum candidate was
written into ``gross_payout`` despite no balance movement or request.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, replace
from typing import Any, Iterable, Mapping, Sequence

from hydra.propfirm.combine_to_xfa import (
    RuleSnapshot,
    official_rule_snapshot_2026_07_15,
)


CANONICAL_PAYOUT_EVENT_SCHEMA = "hydra_xfa_canonical_payout_event_v1"
CANONICAL_PAYOUT_RECONCILIATION_SCHEMA = (
    "hydra_xfa_canonical_payout_reconciliation_v1"
)


class XfaPayoutEventError(RuntimeError):
    """Raised when an XFA ledger cannot be reconciled without ambiguity."""


@dataclass(frozen=True, slots=True)
class CanonicalPayoutEvent:
    """One executed payout, keyed to exactly one frozen XFA alternative."""

    schema: str
    policy_id: str
    scenario: str
    combine_start_id: str
    xfa_path: str
    payout_cycle: int
    eligibility_timestamp: str
    eligible_account_balance: float
    gross_payout_request: float
    balance_fraction_limit: float
    account_size_payout_cap: float
    payout_split: float
    trader_net_payout: float
    costs_or_fees: float
    pre_payout_balance: float
    post_payout_balance: float
    mll_before_payout: float
    mll_after_payout: float
    reset_marker: bool
    event_fingerprint: str

    @classmethod
    def create(
        cls,
        *,
        policy_id: str,
        scenario: str,
        combine_start_id: str | int,
        xfa_path: str,
        payout_cycle: int,
        eligibility_timestamp: str | int,
        eligible_account_balance: float,
        gross_payout_request: float,
        balance_fraction_limit: float,
        account_size_payout_cap: float,
        payout_split: float,
        trader_net_payout: float,
        costs_or_fees: float,
        pre_payout_balance: float,
        post_payout_balance: float,
        mll_before_payout: float,
        mll_after_payout: float,
        reset_marker: bool,
    ) -> "CanonicalPayoutEvent":
        value = cls(
            schema=CANONICAL_PAYOUT_EVENT_SCHEMA,
            policy_id=str(policy_id),
            scenario=str(scenario),
            combine_start_id=str(combine_start_id),
            xfa_path=str(xfa_path),
            payout_cycle=int(payout_cycle),
            eligibility_timestamp=str(eligibility_timestamp),
            eligible_account_balance=float(eligible_account_balance),
            gross_payout_request=float(gross_payout_request),
            balance_fraction_limit=float(balance_fraction_limit),
            account_size_payout_cap=float(account_size_payout_cap),
            payout_split=float(payout_split),
            trader_net_payout=float(trader_net_payout),
            costs_or_fees=float(costs_or_fees),
            pre_payout_balance=float(pre_payout_balance),
            post_payout_balance=float(post_payout_balance),
            mll_before_payout=float(mll_before_payout),
            mll_after_payout=float(mll_after_payout),
            reset_marker=bool(reset_marker),
            event_fingerprint="",
        )
        value._validate()
        return replace(value, event_fingerprint=_stable_hash(value._payload()))

    @property
    def path_key(self) -> tuple[str, str, str, str]:
        return (
            self.policy_id,
            self.scenario,
            self.combine_start_id,
            self.xfa_path,
        )

    @property
    def event_key(self) -> tuple[str, str, str, str, int]:
        return (*self.path_key, self.payout_cycle)

    @property
    def is_first_payout(self) -> bool:
        return self.payout_cycle == 1

    def _payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("event_fingerprint")
        return payload

    def _validate(self) -> None:
        if self.schema != CANONICAL_PAYOUT_EVENT_SCHEMA:
            raise XfaPayoutEventError("canonical payout-event schema drift")
        if not self.policy_id or not self.scenario or not self.combine_start_id:
            raise XfaPayoutEventError("canonical payout-event identity is incomplete")
        if self.xfa_path not in {"XFA_STANDARD", "XFA_CONSISTENCY"}:
            raise XfaPayoutEventError("canonical payout-event path is invalid")
        if self.payout_cycle < 1:
            raise XfaPayoutEventError("payout cycle must be positive")
        if not self.eligibility_timestamp:
            raise XfaPayoutEventError("payout eligibility timestamp is absent")
        numeric = (
            self.eligible_account_balance,
            self.gross_payout_request,
            self.balance_fraction_limit,
            self.account_size_payout_cap,
            self.payout_split,
            self.trader_net_payout,
            self.costs_or_fees,
            self.pre_payout_balance,
            self.post_payout_balance,
            self.mll_before_payout,
            self.mll_after_payout,
        )
        if any(not math.isfinite(value) for value in numeric):
            raise XfaPayoutEventError("canonical payout-event contains non-finite data")
        if self.gross_payout_request <= 0.0:
            raise XfaPayoutEventError("executed gross payout must be positive")
        if self.balance_fraction_limit < 0.0 or self.account_size_payout_cap <= 0.0:
            raise XfaPayoutEventError("payout limits must be non-negative")
        if not 0.0 < self.payout_split <= 1.0:
            raise XfaPayoutEventError("payout split must be in (0,1]")
        if self.costs_or_fees < 0.0:
            raise XfaPayoutEventError("payout costs or fees cannot be negative")
        if not self.reset_marker:
            raise XfaPayoutEventError("an executed payout must carry a reset marker")
        _assert_close(
            self.gross_payout_request,
            min(self.balance_fraction_limit, self.account_size_payout_cap),
            label="gross payout request versus frozen limits",
        )
        _assert_close(
            self.trader_net_payout,
            self.gross_payout_request * self.payout_split - self.costs_or_fees,
            label="split-adjusted trader payout",
        )
        _assert_close(
            self.pre_payout_balance - self.gross_payout_request,
            self.post_payout_balance,
            label="post-payout balance",
        )

    def verify(self) -> None:
        self._validate()
        if self.event_fingerprint != _stable_hash(self._payload()):
            raise XfaPayoutEventError("canonical payout-event fingerprint drift")

    def to_dict(self) -> dict[str, Any]:
        self.verify()
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PayoutPathReconciliation:
    """Canonical event projection plus any recognised historical marker drift."""

    schema: str
    policy_id: str
    scenario: str
    combine_start_id: str
    xfa_path: str
    payout_events: tuple[CanonicalPayoutEvent, ...]
    legacy_subminimum_marker_amounts: tuple[float, ...]
    legacy_subminimum_marker_count: int
    legacy_subminimum_marker_gross: float
    canonical_gross_payout: float
    canonical_trader_net_payout: float

    @property
    def first_payout_count(self) -> int:
        return sum(event.is_first_payout for event in self.payout_events)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "policy_id": self.policy_id,
            "scenario": self.scenario,
            "combine_start_id": self.combine_start_id,
            "xfa_path": self.xfa_path,
            "payout_events": [event.to_dict() for event in self.payout_events],
            "legacy_subminimum_marker_amounts": list(
                self.legacy_subminimum_marker_amounts
            ),
            "legacy_subminimum_marker_count": self.legacy_subminimum_marker_count,
            "legacy_subminimum_marker_gross": self.legacy_subminimum_marker_gross,
            "canonical_gross_payout": self.canonical_gross_payout,
            "canonical_trader_net_payout": self.canonical_trader_net_payout,
            "first_payout_count": self.first_payout_count,
        }


def reconcile_payout_path(
    path: Mapping[str, Any],
    *,
    policy_id: str,
    scenario: str,
    combine_start_id: str | int,
    rule_snapshot: RuleSnapshot | None = None,
    allow_legacy_subminimum_marker: bool = False,
) -> PayoutPathReconciliation:
    """Project one immutable path into unique, executed canonical events.

    The compatibility option accepts only the exact historical defect shape:
    an eligible candidate below the minimum was written to ``gross_payout``,
    while ``payout_requested`` remained false, trader payout stayed zero, and
    the account balance did not move.  No other discrepancy is normalised.
    """

    rules = rule_snapshot or official_rule_snapshot_2026_07_15()
    path_name = str(path.get("path") or "")
    if path_name not in {"XFA_STANDARD", "XFA_CONSISTENCY"}:
        raise XfaPayoutEventError("XFA payout path identity is invalid")
    cap = (
        rules.standard_payout_cap
        if path_name == "XFA_STANDARD"
        else rules.consistency_payout_cap
    )
    ledger = path.get("daily_ledger")
    if not isinstance(ledger, Sequence) or isinstance(ledger, (str, bytes)):
        raise XfaPayoutEventError("XFA daily payout ledger is absent")

    events: list[CanonicalPayoutEvent] = []
    legacy_count = 0
    legacy_gross = 0.0
    legacy_amounts: list[float] = []
    prior_cycle = 0
    for row in ledger:
        if not isinstance(row, Mapping):
            raise XfaPayoutEventError("XFA daily payout ledger row is malformed")
        if row.get("terminal"):
            if bool(row.get("payout_requested")):
                raise XfaPayoutEventError("terminal XFA row requested a payout")
            continue
        opening = _float(row.get("opening_balance"), "opening balance")
        day_pnl = _float(row.get("day_pnl"), "daily PnL")
        pre_balance = opening + day_pnl
        closing = _float(row.get("closing_balance"), "closing balance")
        floor_open = _float(row.get("mll_floor_open"), "opening MLL floor")
        derived_floor_before = min(
            0.0,
            max(floor_open, pre_balance - rules.maximum_loss_limit),
        )
        floor_before = _float(
            row.get("mll_before_payout", derived_floor_before),
            "MLL before payout",
        )
        _assert_close(
            floor_before,
            derived_floor_before,
            label="pre-payout MLL floor",
        )
        floor_after = _float(row.get("mll_floor_close"), "MLL after payout")
        explicit_floor_after = _float(
            row.get("mll_after_payout", floor_after),
            "explicit MLL after payout",
        )
        _assert_close(
            explicit_floor_after,
            floor_after,
            label="explicit versus ledger MLL after payout",
        )
        eligible = bool(row.get("payout_eligible"))
        balance_limit = (
            max(0.0, pre_balance) * rules.payout_fraction if eligible else 0.0
        )
        candidate = min(balance_limit, cap) if eligible else 0.0
        expected_execute = bool(candidate >= rules.minimum_payout - 1e-12)
        executed = bool(row.get("payout_requested"))
        if executed != expected_execute:
            raise XfaPayoutEventError("XFA payout request timing drift")
        raw_gross = _float(row.get("gross_payout", 0.0), "daily gross payout")
        raw_net = _float(
            row.get("trader_net_payout", 0.0), "daily trader payout"
        )
        raw_fees = _float(
            row.get("payout_costs_or_fees", 0.0), "payout costs or fees"
        )

        if not executed:
            if "gross_payout_request" in row:
                _assert_close(
                    row.get("gross_payout_request"),
                    0.0,
                    label="non-executed gross payout request",
                )
            if bool(row.get("payout_reset_marker", False)):
                raise XfaPayoutEventError(
                    "non-executed payout carried a reset marker"
                )
            if "payout_candidate_gross" in row:
                _assert_close(
                    row.get("payout_candidate_gross"),
                    candidate,
                    label="non-executed payout candidate",
                )
            if "payout_balance_fraction_limit" in row:
                _assert_close(
                    row.get("payout_balance_fraction_limit"),
                    balance_limit,
                    label="non-executed 50%-of-balance limit",
                )
            if "payout_account_size_cap" in row:
                _assert_close(
                    row.get("payout_account_size_cap"),
                    cap,
                    label="non-executed account-size payout cap",
                )
            if not math.isclose(raw_gross, 0.0, abs_tol=1e-9):
                legacy_shape = bool(
                    allow_legacy_subminimum_marker
                    and eligible
                    and 0.0 < candidate < rules.minimum_payout
                    and math.isclose(raw_gross, candidate, abs_tol=1e-9)
                    and math.isclose(raw_net, 0.0, abs_tol=1e-9)
                    and math.isclose(closing, pre_balance, abs_tol=1e-9)
                )
                if not legacy_shape:
                    raise XfaPayoutEventError(
                        "non-executed XFA payout carried a gross amount"
                    )
                legacy_count += 1
                legacy_gross += raw_gross
                legacy_amounts.append(raw_gross)
            if not math.isclose(raw_net, 0.0, abs_tol=1e-9):
                raise XfaPayoutEventError(
                    "non-executed XFA payout carried a trader amount"
                )
            _assert_close(closing, pre_balance, label="non-payout closing balance")
            continue

        cycle = int(row.get("payout_cycles", -1))
        if cycle != prior_cycle + 1:
            raise XfaPayoutEventError("XFA payout cycle did not advance exactly once")
        prior_cycle = cycle
        _assert_close(raw_gross, candidate, label="executed gross payout")
        if raw_gross < rules.minimum_payout - 1e-12:
            raise XfaPayoutEventError("executed payout is below the frozen minimum")
        eligible_balance = _float(
            row.get("eligible_account_balance", pre_balance),
            "eligible account balance",
        )
        explicit_balance_limit = _float(
            row.get("payout_balance_fraction_limit", balance_limit),
            "50%-of-balance payout limit",
        )
        explicit_cap = _float(
            row.get("payout_account_size_cap", cap),
            "account-size payout cap",
        )
        explicit_request = _float(
            row.get("gross_payout_request", raw_gross),
            "gross payout request",
        )
        explicit_split = _float(
            row.get("payout_split", rules.trader_profit_split),
            "payout split",
        )
        explicit_pre_balance = _float(
            row.get("pre_payout_balance", pre_balance),
            "explicit pre-payout balance",
        )
        explicit_post_balance = _float(
            row.get("post_payout_balance", closing),
            "explicit post-payout balance",
        )
        _assert_close(
            eligible_balance,
            pre_balance,
            label="eligible versus pre-payout balance",
        )
        _assert_close(
            explicit_balance_limit,
            balance_limit,
            label="50%-of-balance payout limit",
        )
        _assert_close(explicit_cap, cap, label="account-size payout cap")
        _assert_close(explicit_request, raw_gross, label="gross payout request")
        _assert_close(
            explicit_split,
            rules.trader_profit_split,
            label="payout split",
        )
        _assert_close(
            explicit_pre_balance,
            pre_balance,
            label="explicit pre-payout balance",
        )
        _assert_close(
            explicit_post_balance,
            closing,
            label="explicit post-payout balance",
        )
        event = CanonicalPayoutEvent.create(
            policy_id=policy_id,
            scenario=scenario,
            combine_start_id=combine_start_id,
            xfa_path=path_name,
            payout_cycle=cycle,
            eligibility_timestamp=row.get(
                "payout_eligibility_timestamp", row.get("session_day")
            ),
            eligible_account_balance=eligible_balance,
            gross_payout_request=explicit_request,
            balance_fraction_limit=explicit_balance_limit,
            account_size_payout_cap=explicit_cap,
            payout_split=explicit_split,
            trader_net_payout=raw_net,
            costs_or_fees=raw_fees,
            pre_payout_balance=explicit_pre_balance,
            post_payout_balance=explicit_post_balance,
            mll_before_payout=floor_before,
            mll_after_payout=explicit_floor_after,
            reset_marker=row.get(
                "payout_reset_marker",
                row.get("post_payout_mll_locked_at_zero", False),
            ),
        )
        _assert_close(
            event.mll_after_payout,
            0.0,
            label="post-payout MLL reset",
        )
        events.append(event)

    validate_unique_payout_events(events)
    canonical_gross = sum(event.gross_payout_request for event in events)
    canonical_net = sum(event.trader_net_payout for event in events)
    _assert_close(
        canonical_gross,
        path.get("gross_payout", 0.0),
        label="path gross payout summary",
    )
    _assert_close(
        canonical_net,
        path.get("trader_net_payout", 0.0),
        label="path trader payout summary",
    )
    if int(path.get("payout_cycles", -1)) != len(events):
        raise XfaPayoutEventError("path payout-cycle summary drift")
    expected_first_day = (
        int(path.get("first_payout_day"))
        if path.get("first_payout_day") is not None
        else None
    )
    if (expected_first_day is None) != (not events):
        raise XfaPayoutEventError("path first-payout marker drift")
    return PayoutPathReconciliation(
        schema=CANONICAL_PAYOUT_RECONCILIATION_SCHEMA,
        policy_id=str(policy_id),
        scenario=str(scenario),
        combine_start_id=str(combine_start_id),
        xfa_path=path_name,
        payout_events=tuple(events),
        legacy_subminimum_marker_amounts=tuple(legacy_amounts),
        legacy_subminimum_marker_count=legacy_count,
        legacy_subminimum_marker_gross=legacy_gross,
        canonical_gross_payout=canonical_gross,
        canonical_trader_net_payout=canonical_net,
    )


def validate_unique_payout_events(
    events: Iterable[CanonicalPayoutEvent],
) -> None:
    """Reject duplicate cycles and multiple first-payout events per path."""

    event_keys: set[tuple[str, str, str, str, int]] = set()
    fingerprints: set[str] = set()
    first_path_keys: set[tuple[str, str, str, str]] = set()
    paths_by_transition: dict[tuple[str, str, str], set[str]] = {}
    for event in events:
        event.verify()
        if event.event_key in event_keys:
            raise XfaPayoutEventError("duplicate canonical XFA payout cycle")
        if event.event_fingerprint in fingerprints:
            raise XfaPayoutEventError("duplicate canonical XFA payout fingerprint")
        event_keys.add(event.event_key)
        fingerprints.add(event.event_fingerprint)
        transition = event.path_key[:3]
        paths_by_transition.setdefault(transition, set()).add(event.xfa_path)
        if event.is_first_payout:
            if event.path_key in first_path_keys:
                raise XfaPayoutEventError(
                    "multiple first-payout events for one XFA path"
                )
            first_path_keys.add(event.path_key)
    if any(len(paths) > 2 for paths in paths_by_transition.values()):
        raise XfaPayoutEventError("more than two XFA alternatives per transition")


def canonical_payout_totals(
    events: Iterable[CanonicalPayoutEvent],
) -> dict[str, Any]:
    """Aggregate unique events while keeping path alternatives separate."""

    values = tuple(events)
    validate_unique_payout_events(values)
    by_path: dict[str, dict[str, float | int]] = {
        "XFA_STANDARD": {
            "payout_event_count": 0,
            "first_payout_count": 0,
            "gross_payout": 0.0,
            "trader_net_payout": 0.0,
        },
        "XFA_CONSISTENCY": {
            "payout_event_count": 0,
            "first_payout_count": 0,
            "gross_payout": 0.0,
            "trader_net_payout": 0.0,
        },
    }
    for event in values:
        bucket = by_path[event.xfa_path]
        bucket["payout_event_count"] += 1
        bucket["first_payout_count"] += int(event.is_first_payout)
        bucket["gross_payout"] += event.gross_payout_request
        bucket["trader_net_payout"] += event.trader_net_payout
    return {
        "schema": CANONICAL_PAYOUT_RECONCILIATION_SCHEMA,
        "alternatives_are_mutually_exclusive": True,
        "paths": by_path,
    }


def _float(value: Any, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise XfaPayoutEventError(f"{label} is not numeric") from exc
    if not math.isfinite(result):
        raise XfaPayoutEventError(f"{label} is not finite")
    return result


def _assert_close(actual: Any, expected: Any, *, label: str) -> None:
    left = _float(actual, label)
    right = _float(expected, label)
    if not math.isclose(left, right, rel_tol=1e-10, abs_tol=1e-8):
        raise XfaPayoutEventError(f"{label} drift: {left} != {right}")


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "CANONICAL_PAYOUT_EVENT_SCHEMA",
    "CANONICAL_PAYOUT_RECONCILIATION_SCHEMA",
    "CanonicalPayoutEvent",
    "PayoutPathReconciliation",
    "XfaPayoutEventError",
    "canonical_payout_totals",
    "reconcile_payout_path",
    "validate_unique_payout_events",
]
