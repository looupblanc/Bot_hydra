"""Deterministic, diversity-aware proposal construction for causal account books.

This module deliberately does not replay economics.  It turns an immutable sleeve
archive into structurally unique book proposals and provides a pure decision rule
for accepting exact-replay marginal contributions.  Keeping proposal construction
separate from replay prevents approximate sleeve summaries from being mistaken for
account evidence.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Sequence


@dataclass(frozen=True)
class SprintMetrics:
    """Comparable metrics used for proposal quality and exact marginal decisions.

    Rates are fractions in ``[0, 1]``.  Target progress may be negative and may
    exceed one when an account moves beyond its target.  ``stressed_net`` is used
    only to rank proposal seeds; exact admission remains the responsibility of the
    caller's frozen economic gates.
    """

    pass_rate_5d: float
    pass_rate_10d: float
    pass_rate_20d: float
    p25_target_progress: float
    mll_survival_rate: float
    consistency_rate: float
    stressed_net: float = 0.0

    def __post_init__(self) -> None:
        for field_name in (
            "pass_rate_5d",
            "pass_rate_10d",
            "pass_rate_20d",
            "mll_survival_rate",
            "consistency_rate",
        ):
            value = float(getattr(self, field_name))
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{field_name} must be finite and in [0, 1]")
        for field_name in ("p25_target_progress", "stressed_net"):
            if not math.isfinite(float(getattr(self, field_name))):
                raise ValueError(f"{field_name} must be finite")


@dataclass(frozen=True)
class SleeveSummary:
    """Immutable input record for one behaviorally distinct executable sleeve."""

    sleeve_id: str
    qd_cell: str
    behavioral_fingerprint: str
    metrics: SprintMetrics

    def __post_init__(self) -> None:
        if not self.sleeve_id:
            raise ValueError("sleeve_id must not be empty")
        if not self.qd_cell:
            raise ValueError("qd_cell must not be empty")
        if not self.behavioral_fingerprint:
            raise ValueError("behavioral_fingerprint must not be empty")


@dataclass(frozen=True)
class GovernorProfile:
    """One member of the frozen bounded account-governor frontier."""

    profile_id: str
    signal_quality_tiers: tuple[float, ...]
    open_risk_ceiling_fraction: float
    daily_loss_budget_fraction: float
    daily_profit_lock_fraction: float
    maximum_concurrent_sleeves: int
    target_protection_fraction: float
    same_instrument_conflict_policy: str

    def __post_init__(self) -> None:
        if not self.profile_id:
            raise ValueError("profile_id must not be empty")
        if not self.signal_quality_tiers:
            raise ValueError("signal_quality_tiers must not be empty")
        if any(not math.isfinite(value) or value < 0.0 for value in self.signal_quality_tiers):
            raise ValueError("signal_quality_tiers must be finite and non-negative")
        for field_name in (
            "open_risk_ceiling_fraction",
            "daily_loss_budget_fraction",
            "daily_profit_lock_fraction",
            "target_protection_fraction",
        ):
            value = float(getattr(self, field_name))
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{field_name} must be finite and in [0, 1]")
        if not 1 <= self.maximum_concurrent_sleeves <= 4:
            raise ValueError("maximum_concurrent_sleeves must be in [1, 4]")
        if self.same_instrument_conflict_policy not in {
            "priority",
            "net_to_flat",
            "reject_both",
        }:
            raise ValueError("unsupported same_instrument_conflict_policy")

    @property
    def fingerprint(self) -> str:
        return _stable_hash(asdict(self))


@dataclass(frozen=True)
class BookProposal:
    """A structurally unique proposal with an explicit smaller predecessor."""

    book_id: str
    structural_fingerprint: str
    sleeve_ids: tuple[str, ...]
    sleeve_priority: tuple[str, ...]
    qd_cells: tuple[str, ...]
    governor_profile_id: str
    governor_profile_fingerprint: str
    predecessor_book_id: str
    predecessor_sleeve_ids: tuple[str, ...]
    added_sleeve_id: str
    construction_method: str = "constrained_diverse_beam_v1"


@dataclass(frozen=True)
class MarginalContributionThresholds:
    """Frozen tolerances for the exact replay admission decision."""

    minimum_5d_improvement: float = 1e-12
    minimum_10d_improvement: float = 1e-12
    minimum_p25_progress_improvement: float = 1e-12
    minimum_mll_survival_improvement: float = 1e-12
    minimum_consistency_improvement: float = 1e-12
    maximum_5d_degradation: float = 0.01
    maximum_10d_degradation: float = 0.015
    maximum_p25_progress_degradation: float = 0.05
    maximum_mll_survival_degradation: float = 0.01
    maximum_consistency_degradation: float = 0.02

    def __post_init__(self) -> None:
        for field_name, value in asdict(self).items():
            if not math.isfinite(float(value)) or float(value) < 0.0:
                raise ValueError(f"{field_name} must be finite and non-negative")


@dataclass(frozen=True)
class ExactBookEvaluation:
    """Exact replay result supplied to the pure marginal admission rule."""

    book_id: str
    sleeve_ids: tuple[str, ...]
    metrics: SprintMetrics


@dataclass(frozen=True)
class MarginalContributionDecision:
    accepted: bool
    candidate_book_id: str
    predecessor_book_id: str
    best_component_id: str
    retained_book_id: str
    retained_sleeve_ids: tuple[str, ...]
    improved_metrics: tuple[str, ...]
    material_degradations: tuple[str, ...]
    deltas_vs_predecessor: dict[str, float]
    deltas_vs_best_component: dict[str, float]
    reason: str


@dataclass(frozen=True)
class MatchedRandomSelection:
    sleeve_ids: tuple[str, ...]
    requested_qd_cells: tuple[str, ...]
    selected_qd_cells: tuple[str, ...]
    exact_qd_cell_match: bool
    deterministic_seed: int


@dataclass(frozen=True)
class _Membership:
    sleeve_ids: tuple[str, ...]
    predecessor_sleeve_ids: tuple[str, ...]
    added_sleeve_id: str
    qd_signature: tuple[str, ...]
    score: tuple[float, ...]


_MARGINAL_FIELDS = (
    "pass_rate_5d",
    "pass_rate_10d",
    "p25_target_progress",
    "mll_survival_rate",
    "consistency_rate",
)


def build_marginal_book_proposals(
    sleeves: Sequence[SleeveSummary],
    governor_profiles: Sequence[GovernorProfile],
    *,
    requested_count: int,
    maximum_sleeves: int = 6,
    beam_width: int | None = None,
    maximum_members_per_qd_cell: int = 2,
) -> list[BookProposal]:
    """Build deterministic, diverse proposals without using account outcomes.

    The search ranks singleton seeds by five-day utility, expands them with sleeves
    from underrepresented QD cells, and selects each beam round-robin across cell
    signatures.  Membership order is a stable economic priority order, not the
    order in which input records happened to arrive.

    The result contains up to ``requested_count`` proposals.  It reaches that count
    whenever enough constrained memberships and frozen governor profiles exist.
    """

    if requested_count < 0:
        raise ValueError("requested_count must be non-negative")
    if requested_count == 0:
        return []
    if not 2 <= maximum_sleeves <= 6:
        raise ValueError("maximum_sleeves must be in [2, 6]")
    if maximum_members_per_qd_cell < 1:
        raise ValueError("maximum_members_per_qd_cell must be positive")
    ordered = _validate_and_rank_sleeves(sleeves)
    profiles = _validate_profiles(governor_profiles)
    if len(ordered) < 2:
        return []

    # One profile per membership is normally enough.  Keeping additional
    # memberships in the beam avoids filling the archive with governor-only
    # variants when sleeve diversity is available.
    required_memberships = math.ceil(requested_count / len(profiles))
    effective_beam_width = max(
        int(beam_width or 0),
        required_memberships,
        len(ordered) * 4,
    )
    rank = {sleeve.sleeve_id: index for index, sleeve in enumerate(ordered)}
    by_id = {sleeve.sleeve_id: sleeve for sleeve in ordered}
    frontier = [
        _Membership(
            sleeve_ids=(sleeve.sleeve_id,),
            predecessor_sleeve_ids=(),
            added_sleeve_id=sleeve.sleeve_id,
            qd_signature=(sleeve.qd_cell,),
            score=_membership_score((sleeve.sleeve_id,), by_id),
        )
        for sleeve in ordered
    ]
    archive: list[_Membership] = []
    seen_memberships: set[tuple[str, ...]] = {
        membership.sleeve_ids for membership in frontier
    }

    for target_size in range(2, maximum_sleeves + 1):
        expansion_pool: list[_Membership] = []
        for parent in _round_robin_memberships(frontier):
            parent_ids = set(parent.sleeve_ids)
            parent_cells = Counter(by_id[item].qd_cell for item in parent.sleeve_ids)
            additions = sorted(
                (sleeve for sleeve in ordered if sleeve.sleeve_id not in parent_ids),
                key=lambda sleeve: _addition_sort_key(parent, sleeve, by_id),
            )
            for sleeve in additions:
                if parent_cells[sleeve.qd_cell] >= maximum_members_per_qd_cell:
                    continue
                ids = tuple(
                    sorted(
                        (*parent.sleeve_ids, sleeve.sleeve_id),
                        key=rank.__getitem__,
                    )
                )
                if ids in seen_memberships:
                    continue
                seen_memberships.add(ids)
                cells = tuple(sorted(by_id[item].qd_cell for item in ids))
                expansion_pool.append(
                    _Membership(
                        sleeve_ids=ids,
                        predecessor_sleeve_ids=parent.sleeve_ids,
                        added_sleeve_id=sleeve.sleeve_id,
                        qd_signature=cells,
                        score=_membership_score(ids, by_id),
                    )
                )
        if not expansion_pool:
            break
        frontier = _select_diverse_beam(expansion_pool, effective_beam_width)
        archive.extend(frontier)
        if len(archive) >= required_memberships and target_size >= 3:
            # At least two depths prevents a large request from degenerating into
            # only two-sleeve books while avoiding needless combinatorial work.
            break

    memberships = _round_robin_memberships(archive)
    proposals: list[BookProposal] = []
    for profile_offset in range(len(profiles)):
        for membership_index, membership in enumerate(memberships):
            profile = profiles[(membership_index + profile_offset) % len(profiles)]
            proposals.append(_book_proposal(membership, profile))
            if len(proposals) >= requested_count:
                return proposals
    return proposals


def assess_exact_marginal_contribution(
    candidate: ExactBookEvaluation,
    predecessor: ExactBookEvaluation,
    best_component: ExactBookEvaluation,
    *,
    thresholds: MarginalContributionThresholds,
) -> MarginalContributionDecision:
    """Accept an addition only when exact replay adds utility without harm.

    Improvement is measured against the stronger of the predecessor and the best
    component on each criterion.  Material degradation is also tested against that
    stronger reference.  A rejection explicitly retains the smaller predecessor,
    which prevents a larger book from receiving an automatic advantage.
    """

    if len(candidate.sleeve_ids) != len(predecessor.sleeve_ids) + 1:
        raise ValueError("candidate must contain exactly one more sleeve than predecessor")
    if not set(predecessor.sleeve_ids).issubset(candidate.sleeve_ids):
        raise ValueError("candidate must contain every predecessor sleeve")
    if len(best_component.sleeve_ids) != 1:
        raise ValueError("best_component must contain exactly one sleeve")

    minimum_gain = {
        "pass_rate_5d": thresholds.minimum_5d_improvement,
        "pass_rate_10d": thresholds.minimum_10d_improvement,
        "p25_target_progress": thresholds.minimum_p25_progress_improvement,
        "mll_survival_rate": thresholds.minimum_mll_survival_improvement,
        "consistency_rate": thresholds.minimum_consistency_improvement,
    }
    maximum_degradation = {
        "pass_rate_5d": thresholds.maximum_5d_degradation,
        "pass_rate_10d": thresholds.maximum_10d_degradation,
        "p25_target_progress": thresholds.maximum_p25_progress_degradation,
        "mll_survival_rate": thresholds.maximum_mll_survival_degradation,
        "consistency_rate": thresholds.maximum_consistency_degradation,
    }
    deltas_predecessor = {
        field: float(getattr(candidate.metrics, field))
        - float(getattr(predecessor.metrics, field))
        for field in _MARGINAL_FIELDS
    }
    deltas_component = {
        field: float(getattr(candidate.metrics, field))
        - float(getattr(best_component.metrics, field))
        for field in _MARGINAL_FIELDS
    }
    improved = []
    degraded = []
    for field in _MARGINAL_FIELDS:
        candidate_value = float(getattr(candidate.metrics, field))
        reference = max(
            float(getattr(predecessor.metrics, field)),
            float(getattr(best_component.metrics, field)),
        )
        delta = candidate_value - reference
        if delta >= minimum_gain[field]:
            improved.append(field)
        if delta < -maximum_degradation[field]:
            degraded.append(field)

    accepted = bool(improved) and not degraded
    if accepted:
        retained_book_id = candidate.book_id
        retained_sleeve_ids = candidate.sleeve_ids
        reason = "exact_marginal_utility_improved_without_material_degradation"
    else:
        retained_book_id = predecessor.book_id
        retained_sleeve_ids = predecessor.sleeve_ids
        reason = (
            "material_degradation_preserve_predecessor"
            if degraded
            else "no_exact_marginal_improvement_preserve_predecessor"
        )
    return MarginalContributionDecision(
        accepted=accepted,
        candidate_book_id=candidate.book_id,
        predecessor_book_id=predecessor.book_id,
        best_component_id=best_component.book_id,
        retained_book_id=retained_book_id,
        retained_sleeve_ids=retained_sleeve_ids,
        improved_metrics=tuple(improved),
        material_degradations=tuple(degraded),
        deltas_vs_predecessor=deltas_predecessor,
        deltas_vs_best_component=deltas_component,
        reason=reason,
    )


def select_matched_random_members(
    sleeves: Sequence[SleeveSummary],
    reference_sleeve_ids: Sequence[str],
    *,
    deterministic_seed: int,
    exclude_reference_members: bool = True,
) -> MatchedRandomSelection:
    """Select a deterministic random-control membership matched on QD cells.

    Stable SHA-256 ordering is used instead of process-global RNG state.  Exact
    cell matches are preferred; when a cell has no eligible alternative the
    selector falls back to another cell and reports that relaxation explicitly.
    """

    ordered = _validate_and_rank_sleeves(sleeves)
    by_id = {sleeve.sleeve_id: sleeve for sleeve in ordered}
    reference_ids = tuple(reference_sleeve_ids)
    if not reference_ids:
        raise ValueError("reference_sleeve_ids must not be empty")
    if len(set(reference_ids)) != len(reference_ids):
        raise ValueError("reference_sleeve_ids must be unique")
    missing = sorted(set(reference_ids) - set(by_id))
    if missing:
        raise ValueError(f"unknown reference sleeve IDs: {missing}")
    excluded = set(reference_ids) if exclude_reference_members else set()
    eligible = [sleeve for sleeve in ordered if sleeve.sleeve_id not in excluded]
    if len(eligible) < len(reference_ids):
        raise ValueError("insufficient non-reference sleeves for matched random control")

    selected: list[SleeveSummary] = []
    selected_ids: set[str] = set()
    requested_cells = tuple(by_id[item].qd_cell for item in reference_ids)
    for position, requested_cell in enumerate(requested_cells):
        same_cell = [
            sleeve
            for sleeve in eligible
            if sleeve.sleeve_id not in selected_ids and sleeve.qd_cell == requested_cell
        ]
        pool = same_cell or [
            sleeve for sleeve in eligible if sleeve.sleeve_id not in selected_ids
        ]
        chosen = min(
            pool,
            key=lambda sleeve: _stable_hash(
                {
                    "seed": int(deterministic_seed),
                    "position": position,
                    "reference": reference_ids,
                    "candidate": sleeve.sleeve_id,
                    "candidate_fingerprint": sleeve.behavioral_fingerprint,
                }
            ),
        )
        selected.append(chosen)
        selected_ids.add(chosen.sleeve_id)
    selected_cells = tuple(sleeve.qd_cell for sleeve in selected)
    return MatchedRandomSelection(
        sleeve_ids=tuple(sleeve.sleeve_id for sleeve in selected),
        requested_qd_cells=requested_cells,
        selected_qd_cells=selected_cells,
        exact_qd_cell_match=selected_cells == requested_cells,
        deterministic_seed=int(deterministic_seed),
    )


def _validate_and_rank_sleeves(
    sleeves: Sequence[SleeveSummary],
) -> list[SleeveSummary]:
    sleeve_ids = [sleeve.sleeve_id for sleeve in sleeves]
    fingerprints = [sleeve.behavioral_fingerprint for sleeve in sleeves]
    if len(set(sleeve_ids)) != len(sleeve_ids):
        raise ValueError("sleeve IDs must be unique")
    if len(set(fingerprints)) != len(fingerprints):
        raise ValueError("behavioral fingerprints must be unique")
    return sorted(sleeves, key=_sleeve_rank_key)


def _validate_profiles(
    governor_profiles: Sequence[GovernorProfile],
) -> tuple[GovernorProfile, ...]:
    profiles = tuple(governor_profiles)
    if not profiles:
        raise ValueError("at least one frozen governor profile is required")
    if len(profiles) > 32:
        raise ValueError("governor profile frontier must contain at most 32 profiles")
    if len({profile.profile_id for profile in profiles}) != len(profiles):
        raise ValueError("governor profile IDs must be unique")
    if len({profile.fingerprint for profile in profiles}) != len(profiles):
        raise ValueError("governor profiles must be structurally unique")
    return tuple(sorted(profiles, key=lambda profile: profile.profile_id))


def _sleeve_rank_key(sleeve: SleeveSummary) -> tuple[Any, ...]:
    metrics = sleeve.metrics
    return (
        -metrics.pass_rate_5d,
        -metrics.pass_rate_10d,
        -metrics.p25_target_progress,
        -metrics.mll_survival_rate,
        -metrics.consistency_rate,
        -metrics.pass_rate_20d,
        -metrics.stressed_net,
        sleeve.qd_cell,
        sleeve.behavioral_fingerprint,
        sleeve.sleeve_id,
    )


def _addition_sort_key(
    parent: _Membership,
    sleeve: SleeveSummary,
    by_id: dict[str, SleeveSummary],
) -> tuple[Any, ...]:
    existing_cells = {by_id[item].qd_cell for item in parent.sleeve_ids}
    return (
        0 if sleeve.qd_cell not in existing_cells else 1,
        *_sleeve_rank_key(sleeve),
        _stable_hash({"parent": parent.sleeve_ids, "addition": sleeve.sleeve_id}),
    )


def _membership_score(
    sleeve_ids: tuple[str, ...],
    by_id: dict[str, SleeveSummary],
) -> tuple[float, ...]:
    members = [by_id[item] for item in sleeve_ids]
    count = float(len(members))
    diversity = len({member.qd_cell for member in members}) / count
    return (
        diversity,
        sum(item.metrics.pass_rate_5d for item in members) / count,
        min(item.metrics.pass_rate_5d for item in members),
        sum(item.metrics.pass_rate_10d for item in members) / count,
        sum(item.metrics.p25_target_progress for item in members) / count,
        sum(item.metrics.mll_survival_rate for item in members) / count,
        sum(item.metrics.consistency_rate for item in members) / count,
    )


def _select_diverse_beam(
    memberships: Sequence[_Membership], limit: int
) -> list[_Membership]:
    groups: dict[tuple[str, ...], list[_Membership]] = defaultdict(list)
    for membership in memberships:
        groups[membership.qd_signature].append(membership)
    queues: list[deque[_Membership]] = []
    for signature in sorted(groups):
        rows = sorted(
            groups[signature],
            key=lambda row: (
                tuple(-value for value in row.score),
                _stable_hash(row.sleeve_ids),
            ),
        )
        queues.append(deque(rows))
    selected: list[_Membership] = []
    while queues and len(selected) < limit:
        next_queues: list[deque[_Membership]] = []
        for queue in queues:
            if queue and len(selected) < limit:
                selected.append(queue.popleft())
            if queue:
                next_queues.append(queue)
        queues = next_queues
    return selected


def _round_robin_memberships(
    memberships: Iterable[_Membership],
) -> list[_Membership]:
    groups: dict[tuple[str, ...], list[_Membership]] = defaultdict(list)
    for membership in memberships:
        groups[membership.qd_signature].append(membership)
    queues = [
        deque(
            sorted(
                groups[key],
                key=lambda row: (
                    tuple(-value for value in row.score),
                    _stable_hash(row.sleeve_ids),
                ),
            )
        )
        for key in sorted(groups)
    ]
    result: list[_Membership] = []
    while queues:
        next_queues: list[deque[_Membership]] = []
        for queue in queues:
            if queue:
                result.append(queue.popleft())
            if queue:
                next_queues.append(queue)
        queues = next_queues
    return result


def _book_proposal(
    membership: _Membership, profile: GovernorProfile
) -> BookProposal:
    structure = {
        "sleeve_ids": membership.sleeve_ids,
        "sleeve_priority": membership.sleeve_ids,
        "governor_profile_id": profile.profile_id,
        "governor_profile_fingerprint": profile.fingerprint,
        "construction_method": "constrained_diverse_beam_v1",
    }
    fingerprint = _stable_hash(structure)
    predecessor_structure = {
        **structure,
        "sleeve_ids": membership.predecessor_sleeve_ids,
        "sleeve_priority": membership.predecessor_sleeve_ids,
    }
    predecessor_fingerprint = _stable_hash(predecessor_structure)
    return BookProposal(
        book_id=f"fast_book_{fingerprint[:24]}",
        structural_fingerprint=fingerprint,
        sleeve_ids=membership.sleeve_ids,
        sleeve_priority=membership.sleeve_ids,
        qd_cells=membership.qd_signature,
        governor_profile_id=profile.profile_id,
        governor_profile_fingerprint=profile.fingerprint,
        predecessor_book_id=f"fast_book_{predecessor_fingerprint[:24]}",
        predecessor_sleeve_ids=membership.predecessor_sleeve_ids,
        added_sleeve_id=membership.added_sleeve_id,
    )


def _stable_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
