"""Isolated causal PnL-state sizing frontier for pass-observed policies.

This research adapter intentionally leaves the checksum-bound account router
and persistent service untouched.  It reuses immutable causal trajectories and
the authoritative chronological replay, temporarily injecting a process-local
entry router while an episode is evaluated.  The patch is always restored in
a ``finally`` block and cannot affect another process.

Signals, direction, entry/exit timestamps, stops and targets never change.
Only the whole-contract quantity requested at an entry can vary, using fields
already available at that decision boundary: realized day PnL, MLL buffer,
remaining target and a deterministic daily consistency headroom.
"""

from __future__ import annotations

import contextlib
import json
import math
import statistics
import time
from collections import Counter
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

import numpy as np

import hydra.account_policy.causal_active_pool_replay as causal_replay
from hydra.account_policy.active_risk_pool import ActiveRiskPoolPolicy
from hydra.account_policy.router import AccountDecisionState, EntryIntent
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence.causal_target_velocity_adapter import (
    reconstruct_exact_hazard_replay,
)
from hydra.production.autonomous_combine_pass_bank import _verify_self_hash
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_RULE_SNAPSHOT,
    HORIZONS,
    _account_config,
    _apply_session_contract,
    _declared_stop_risk_charge_per_mini,
    _inside,
    _load_banks,
    _load_frozen_grid,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
    _read_verified_event_evidence,
    _require_scenario_identity,
    _standalone_policy,
)
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.production.causal_risk_charge import require_causal_stop_risk_charge
from hydra.propfirm.combine_episode import CombineTerminal
from hydra.research.causal_target_velocity import HazardOutcome


SCHEMA = "hydra_pnl_state_risk_frontier_v1"
PROFILE_SCHEMA = "hydra_pnl_state_sizing_profile_v1"
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
DESIGN_BLOCKS = ("B1", "B2")
ALL_BLOCKS = ("B1", "B2", "B3", "B4")

DEFAULT_BRANCH_ROOT = Path(
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/"
    "branch_results/post_source_exhaustion/post_composite"
)
DEFAULT_PASS_BANK = DEFAULT_BRANCH_ROOT / "combine_pass_observed_bank.json"
DEFAULT_CANDIDATE_BANK = DEFAULT_BRANCH_ROOT / "combine_candidate_bank.json"
DEFAULT_MARGINAL_BOOKS = DEFAULT_BRANCH_ROOT / "marginal_books_composite.json"
DEFAULT_QUARANTINE = Path(
    "reports/economic_evolution/risk_corrected_complementarity_graph_v1/"
    "economic_result.json"
)
DEFAULT_RECONCILIATION = Path(
    "reports/economic_evolution/evidence_axis_reconciliation_v1/economic_result.json"
)
DEFAULT_CLEAN_LEDGER = Path(
    "reports/economic_evolution/evidence_axis_reconciliation_v1/"
    "canonical_44_policy_ledger.json"
)
DEFAULT_PROVISIONAL_RESULT = Path(
    "reports/economic_evolution/pnl_state_risk_frontier_v1/economic_result.json"
)
DEFAULT_EXACT_SOURCE_PATHS = (
    Path(
        "reports/economic_evolution/"
        "autonomous_economic_discovery_director_0035_revision_02/"
        "branch_results/epoch_0002_exact_0029_account_race.json"
    ),
    *(
        Path(
            "reports/economic_evolution/"
            "autonomous_economic_discovery_director_0035_revision_02/"
            f"branch_results/post_source_exhaustion/exact_0029_offset_{offset:04d}.json"
        )
        for offset in (32, 64, 96, 128, 160)
    ),
)
EXPECTED_PROVISIONAL_RESULT_HASH = (
    "f417aae2a932db2a70c9c4b1f85aa62d6b3d42ad748ae418317f452c84a97d94"
)
EXPLICIT_STRESSED_MLL_EXCLUSION = "hazard_367100adab5fe2a69a4f3257"


class PnLStateRiskFrontierError(RuntimeError):
    """The isolated sizing experiment failed an immutable-input contract."""


@dataclass(frozen=True, slots=True)
class PnLStateSizingProfile:
    """One small preregistered anti-martingale/guarded sizing policy."""

    profile_id: str
    base_multiplier: float
    loss_multiplier: float
    low_buffer_fraction: float
    intraday_loss_fraction_of_mll: float
    progress_step_1: float
    progress_step_1_multiplier: float
    progress_step_2: float
    progress_step_2_multiplier: float
    target_protection_fraction: float
    target_protection_multiplier: float
    consistency_headroom_fraction: float
    consistency_multiplier: float

    def __post_init__(self) -> None:
        allowed = {0.5, 0.75, 1.0, 1.25, 1.5}
        multipliers = (
            self.base_multiplier,
            self.loss_multiplier,
            self.progress_step_1_multiplier,
            self.progress_step_2_multiplier,
            self.target_protection_multiplier,
            self.consistency_multiplier,
        )
        if not self.profile_id or any(float(value) not in allowed for value in multipliers):
            raise ValueError("PnL-state profile escaped the frozen multiplier set")
        fractions = (
            self.low_buffer_fraction,
            self.intraday_loss_fraction_of_mll,
            self.progress_step_1,
            self.progress_step_2,
            self.target_protection_fraction,
            self.consistency_headroom_fraction,
        )
        if any(not 0.0 <= float(value) <= 1.0 for value in fractions):
            raise ValueError("PnL-state profile fractions must be in [0,1]")
        if self.progress_step_1 > self.progress_step_2:
            raise ValueError("PnL-state progress steps are reversed")

    @property
    def is_identity(self) -> bool:
        return self.profile_id == "pnl_state_identity"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": PROFILE_SCHEMA,
            "profile_id": self.profile_id,
            "base_multiplier": self.base_multiplier,
            "loss_multiplier": self.loss_multiplier,
            "low_buffer_fraction": self.low_buffer_fraction,
            "intraday_loss_fraction_of_mll": self.intraday_loss_fraction_of_mll,
            "progress_step_1": self.progress_step_1,
            "progress_step_1_multiplier": self.progress_step_1_multiplier,
            "progress_step_2": self.progress_step_2,
            "progress_step_2_multiplier": self.progress_step_2_multiplier,
            "target_protection_fraction": self.target_protection_fraction,
            "target_protection_multiplier": self.target_protection_multiplier,
            "consistency_headroom_fraction": self.consistency_headroom_fraction,
            "consistency_multiplier": self.consistency_multiplier,
        }


def frozen_pnl_state_profiles() -> tuple[PnLStateSizingProfile, ...]:
    """Return the complete five-profile frontier, frozen before replay."""

    profiles = (
        PnLStateSizingProfile(
            "pnl_state_identity", 1.0, 1.0, 0.0, 0.0,
            1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0,
        ),
        PnLStateSizingProfile(
            "pnl_state_guarded", 1.0, 0.5, 0.40, 0.08,
            0.35, 1.0, 0.65, 1.0, 0.15, 0.5, 0.15, 0.5,
        ),
        PnLStateSizingProfile(
            "pnl_state_profit_ladder", 1.0, 0.5, 0.45, 0.08,
            0.20, 1.25, 0.50, 1.5, 0.15, 0.5, 0.15, 0.5,
        ),
        PnLStateSizingProfile(
            "pnl_state_fast_ladder", 1.25, 0.5, 0.50, 0.06,
            0.15, 1.5, 0.45, 1.5, 0.20, 0.75, 0.20, 0.75,
        ),
        PnLStateSizingProfile(
            "pnl_state_conservative_compound", 0.75, 0.5, 0.55, 0.06,
            0.25, 1.0, 0.55, 1.25, 0.15, 0.5, 0.20, 0.5,
        ),
    )
    if len(profiles) != 5 or len({row.profile_id for row in profiles}) != 5:
        raise PnLStateRiskFrontierError("frozen sizing frontier is invalid")
    return profiles


@dataclass(frozen=True, slots=True)
class _RawComponent:
    candidate_id: str
    normal: tuple[Any, ...]
    stressed: tuple[Any, ...]
    eligible_days: frozenset[int]
    censored_days: frozenset[int]
    risk_charge_per_mini: float
    source: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _PreparedPolicy:
    policy_id: str
    source_kind: str
    evidence_tier: str
    account_label: str
    baseline_policy: ActiveRiskPoolPolicy
    trajectories: Mapping[str, Mapping[str, tuple[Any, ...]]]
    unavailable_days: frozenset[int]
    source_policy: Mapping[str, Any]
    source_metrics: Mapping[str, Any]
    source_hashes: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _ExactCellSource:
    candidate_id: str
    source_exact_result_hash: str
    source_wrapper_hash: str
    source_path: str
    candidate_result_hash: str
    cell_hash: str
    cell: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _DecisionWithSizingAudit:
    base: Any
    profile_id: str
    multiplier: float
    multiplier_reason: str
    original_quantity: int
    state_sized_quantity: int
    realized_day_pnl: float
    mll_buffer: float
    remaining_target: float
    consistency_headroom: float

    @property
    def allow(self) -> bool:
        return bool(self.base.allow)

    @property
    def quantity(self) -> int:
        return int(self.base.quantity)

    @property
    def mini_equivalent(self) -> float:
        return float(self.base.mini_equivalent)

    @property
    def reason(self) -> str:
        return str(self.base.reason)

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.base.to_dict(),
            "pnl_state_profile_id": self.profile_id,
            "pnl_state_multiplier": self.multiplier,
            "pnl_state_multiplier_reason": self.multiplier_reason,
            "original_base_quantity": self.original_quantity,
            "state_sized_quantity": self.state_sized_quantity,
            "sizing_realized_day_pnl": self.realized_day_pnl,
            "sizing_mll_buffer": self.mll_buffer,
            "sizing_remaining_target": self.remaining_target,
            "sizing_consistency_headroom": self.consistency_headroom,
            "future_outcome_fields_used_for_sizing": False,
        }


def pnl_state_multiplier(
    profile: PnLStateSizingProfile,
    state: AccountDecisionState,
    *,
    target_usd: float,
    mll_usd: float,
    consistency_fraction: float,
) -> tuple[float, str, float]:
    """Choose a multiplier from causal account state only."""

    if profile.is_identity:
        return 1.0, "IDENTITY", target_usd * consistency_fraction
    headroom = max(
        0.0,
        target_usd * consistency_fraction
        - max(0.0, float(state.daily_realized_pnl)),
    )
    if (
        float(state.mll_buffer) <= mll_usd * profile.low_buffer_fraction
        or float(state.daily_realized_pnl)
        <= -mll_usd * profile.intraday_loss_fraction_of_mll
    ):
        return profile.loss_multiplier, "LOSS_OR_LOW_MLL_BUFFER", headroom
    if (
        profile.consistency_headroom_fraction > 0.0
        and headroom <= target_usd * profile.consistency_headroom_fraction
    ):
        return profile.consistency_multiplier, "CONSISTENCY_HEADROOM", headroom
    if (
        profile.target_protection_fraction > 0.0
        and float(state.remaining_target)
        <= target_usd * profile.target_protection_fraction
    ):
        return profile.target_protection_multiplier, "TARGET_PROTECTION", headroom
    progress = max(0.0, 1.0 - float(state.remaining_target) / max(target_usd, 1.0))
    if progress >= profile.progress_step_2:
        return profile.progress_step_2_multiplier, "PROFIT_STEP_2", headroom
    if progress >= profile.progress_step_1:
        return profile.progress_step_1_multiplier, "PROFIT_STEP_1", headroom
    return profile.base_multiplier, "BASE", headroom


@contextlib.contextmanager
def _isolated_router_patch(
    profile: PnLStateSizingProfile,
    *,
    target_usd: float,
    mll_usd: float,
    consistency_fraction: float,
) -> Iterator[None]:
    """Inject the research router in this process and always restore it."""

    original_router = causal_replay.route_active_risk_entry

    def isolated_router(
        intent: EntryIntent,
        state: AccountDecisionState,
        *,
        policy: ActiveRiskPoolPolicy,
    ) -> _DecisionWithSizingAudit:
        multiplier, reason, headroom = pnl_state_multiplier(
            profile,
            state,
            target_usd=target_usd,
            mll_usd=mll_usd,
            consistency_fraction=consistency_fraction,
        )
        original_quantity = int(intent.base_quantity)
        quantity = max(1, int(math.floor(original_quantity * multiplier + 1e-12)))
        sized = replace(
            intent,
            base_quantity=quantity,
            base_mini_equivalent=(
                float(intent.base_mini_equivalent)
                * float(quantity)
                / max(original_quantity, 1)
            ),
        )
        decision = original_router(sized, state, policy=policy)
        return _DecisionWithSizingAudit(
            base=decision,
            profile_id=profile.profile_id,
            multiplier=float(multiplier),
            multiplier_reason=reason,
            original_quantity=original_quantity,
            state_sized_quantity=quantity,
            realized_day_pnl=float(state.daily_realized_pnl),
            mll_buffer=float(state.mll_buffer),
            remaining_target=float(state.remaining_target),
            consistency_headroom=float(headroom),
        )

    causal_replay.route_active_risk_entry = isolated_router
    try:
        yield
    finally:
        causal_replay.route_active_risk_entry = original_router


def build_pnl_state_risk_frontier(
    root: str | Path,
    *,
    pass_bank_path: str | Path = DEFAULT_PASS_BANK,
    candidate_bank_path: str | Path = DEFAULT_CANDIDATE_BANK,
    marginal_books_path: str | Path = DEFAULT_MARGINAL_BOOKS,
    quarantine_path: str | Path = DEFAULT_QUARANTINE,
    reconciliation_path: str | Path = DEFAULT_RECONCILIATION,
    clean_ledger_path: str | Path = DEFAULT_CLEAN_LEDGER,
    fast_pass_manifest_path: str | Path = DEFAULT_FAST_PASS_MANIFEST,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> dict[str, Any]:
    """Evaluate the bounded frontier without authoritative writes or promotion."""

    started = time.perf_counter()
    project = Path(root).resolve()
    pass_wrapper = _read_json(_inside(project, pass_bank_path))
    candidate_wrapper = _read_json(_inside(project, candidate_bank_path))
    marginal_wrapper = _read_json(_inside(project, marginal_books_path))
    quarantine = _read_json(_inside(project, quarantine_path))
    reconciliation = _read_json(_inside(project, reconciliation_path))
    clean_ledger = _read_json(_inside(project, clean_ledger_path))
    pass_bank = _unwrap(pass_wrapper, "combine_pass_observed_bank")
    candidate_bank = _unwrap(candidate_wrapper, "candidate_bank")
    marginal_books = _unwrap(marginal_wrapper, "marginal_book_composite")
    _verify_mapping_hash(quarantine)
    _verify_mapping_hash(reconciliation)
    _verify_clean_ledger(clean_ledger, reconciliation)

    clean_rows, excluded_rows = _clean_inventory(pass_bank, clean_ledger)
    if len(clean_rows) != 44:
        raise PnLStateRiskFrontierError(
            f"clean pass-observed inventory drift: expected 44, got {len(clean_rows)}"
        )

    manifest = _load_self_hashed_manifest(_inside(project, fast_pass_manifest_path))
    calendar, starts, grid_receipt = _load_frozen_grid(project, manifest)
    rules, rule_receipt = _load_rule_snapshot(_inside(project, rule_snapshot_path))
    bank_entries, bank_receipt = _load_banks(project)
    classified = {
        str(row["candidate_id"]): dict(row)
        for row in candidate_bank.get("candidates", ())
    }
    books = {
        str(row["policy_id"]): dict(row)
        for row in marginal_books.get("book_results", ())
    }
    raw_cache: dict[str, _RawComponent] = {}
    prepared: list[_PreparedPolicy] = []
    for row in clean_rows:
        prepared.append(
            _prepare_policy(
                project,
                row,
                classified=classified,
                books=books,
                bank_entries=bank_entries,
                calendar=calendar,
                rules=rules,
                raw_cache=raw_cache,
            )
        )

    profiles = frozen_pnl_state_profiles()
    profile_hash = stable_hash([row.to_dict() for row in profiles])
    policy_results: list[dict[str, Any]] = []
    baseline_reconciled = 0
    exact_episode_count = 0
    for policy in prepared:
        identity_all = _evaluate_profile(
            policy,
            profiles[0],
            blocks=ALL_BLOCKS,
            calendar=calendar,
            starts=starts,
            rule=rules[policy.account_label],
        )
        exact_episode_count += int(identity_all["exact_episode_count"])
        mismatches = _source_mismatches(policy, identity_all)
        if mismatches:
            policy_results.append(
                _mismatch_result(policy, identity_all, mismatches)
            )
            continue
        baseline_reconciled += 1

        design_results = []
        for profile in profiles:
            value = _evaluate_profile(
                policy,
                profile,
                blocks=DESIGN_BLOCKS,
                calendar=calendar,
                starts=starts,
                rule=rules[policy.account_label],
            )
            exact_episode_count += int(value["exact_episode_count"])
            design_results.append(value)
        identity_design = next(
            row for row in design_results if row["profile_id"] == "pnl_state_identity"
        )
        best_design = max(design_results, key=_design_rank)
        selected_profile = next(
            row for row in profiles if row.profile_id == best_design["profile_id"]
        )
        if not _materially_improves(best_design, identity_design):
            selected_profile = profiles[0]
            best_design = identity_design

        if selected_profile.is_identity:
            selected_all = identity_all
        else:
            selected_all = _evaluate_profile(
                policy,
                selected_profile,
                blocks=ALL_BLOCKS,
                calendar=calendar,
                starts=starts,
                rule=rules[policy.account_label],
            )
            exact_episode_count += int(selected_all["exact_episode_count"])
        policy_results.append(
            _policy_result(
                policy,
                identity_all=identity_all,
                design_results=design_results,
                selected_design=best_design,
                selected_all=selected_all,
            )
        )

    aggregates = _aggregate(policy_results)
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "status": "PNL_STATE_SIZING_FRONTIER_COMPLETE_DEVELOPMENT_DIAGNOSTIC",
        "source_pass_bank_hash": pass_bank["result_hash"],
        "source_candidate_bank_hash": candidate_bank["result_hash"],
        "source_marginal_books_hash": marginal_books["result_hash"],
        "source_quarantine_hash": quarantine["result_hash"],
        "source_evidence_axis_reconciliation_hash": reconciliation["result_hash"],
        "source_clean_44_ledger_hash": clean_ledger["ledger_hash"],
        "source_manifest_hash": manifest["manifest_hash"],
        "frozen_grid_hash": grid_receipt["grid_hash"],
        "official_rule_snapshot_hash": rule_receipt["parsed_rule_hash"],
        "source_bank_receipt": bank_receipt,
        "profiles": [row.to_dict() for row in profiles],
        "profile_hash": profile_hash,
        "causal_state_contract": {
            "allowed_fields": [
                "daily_realized_pnl",
                "mll_buffer",
                "remaining_target",
                "derived_daily_consistency_headroom",
            ],
            "future_outcome_fields_used": False,
            "signal_entry_exit_stop_target_mutated": False,
            "sizing_style": "BOUNDED_ANTI_MARTINGALE_AND_ACCOUNT_GUARDS",
        },
        "inventory": {
            "source_pass_observed_count": len(pass_bank.get("policies", ())),
            "clean_non_quarantined_count": len(clean_rows),
            "quarantined_exclusion_count": len(excluded_rows),
            "clean_standalone_count": sum(
                row["source_kind"] == "EXACT_STANDALONE" for row in clean_rows
            ),
            "clean_book_count": sum(
                row["source_kind"] == "MARGINALLY_ACCEPTED_BOOK" for row in clean_rows
            ),
            "baseline_reconciled_count": baseline_reconciled,
            "baseline_mismatch_count": len(clean_rows) - baseline_reconciled,
            "excluded": excluded_rows,
        },
        "policy_results": policy_results,
        "aggregate": aggregates,
        "counters": {
            "exact_account_episodes": exact_episode_count,
            "policies_evaluated": len(prepared),
            "profiles_in_frontier": len(profiles),
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "database_writes": 0,
            "registry_writes": 0,
            "broker_connections": 0,
            "orders": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
        },
        "normal_pass_status_contract": {
            "baseline_combine_pass_observed_status_preserved": True,
            "normal_pass_status_independent_of_stress": True,
            "stress_is_diagnostic_not_an_admission_veto": True,
            "source_bank_mutated": False,
        },
        "evidence_role": "VIEWED_DEVELOPMENT_DIAGNOSTIC_ONLY",
        "promotion_status": None,
        "runtime_seconds": time.perf_counter() - started,
        "next_action": (
            "RETAIN_DYNAMIC_WINNERS_FOR_FROZEN_CHRONOLOGICAL_VALIDATION_"
            "WITHOUT_ERASING_BASELINE_PASS_STATUS"
        ),
    }
    hash_core = {key: value for key, value in core.items() if key != "runtime_seconds"}
    return {**core, "result_hash": stable_hash(hash_core)}


def continue_contract_only_pnl_state_frontier(
    root: str | Path,
    *,
    provisional_result_path: str | Path = DEFAULT_PROVISIONAL_RESULT,
    exact_source_paths: Sequence[str | Path] = DEFAULT_EXACT_SOURCE_PATHS,
    pass_bank_path: str | Path = DEFAULT_PASS_BANK,
    candidate_bank_path: str | Path = DEFAULT_CANDIDATE_BANK,
    marginal_books_path: str | Path = DEFAULT_MARGINAL_BOOKS,
    quarantine_path: str | Path = DEFAULT_QUARANTINE,
    reconciliation_path: str | Path = DEFAULT_RECONCILIATION,
    clean_ledger_path: str | Path = DEFAULT_CLEAN_LEDGER,
    fast_pass_manifest_path: str | Path = DEFAULT_FAST_PASS_MANIFEST,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> dict[str, dict[str, Any]]:
    """Replay only the 20 source-policy divergences and merge immutable results.

    The original run used a modern actual-stop-risk adapter for 20 legacy
    ``CONTRACT_ONLY_UNIFORM_SCALE`` Tier-E cells.  Their authoritative source
    cells instead used a nominal 1e-6 risk charge and relied on exact
    chronological MLL accounting.  This continuation binds the full exact
    source cell and never replays the 24 already reconciled policies.
    """

    started = time.perf_counter()
    project = Path(root).resolve()
    provisional = _read_json(_inside(project, provisional_result_path))
    _verify_provisional_result(provisional)
    original_rows = [dict(row) for row in provisional.get("policy_results", ())]
    immutable_rows = [
        row
        for row in original_rows
        if row.get("status") != "BASELINE_REPLAY_MISMATCH_FAIL_CLOSED"
    ]
    divergent_rows = [
        row
        for row in original_rows
        if row.get("status") == "BASELINE_REPLAY_MISMATCH_FAIL_CLOSED"
    ]
    if len(immutable_rows) != 24 or len(divergent_rows) != 20:
        raise PnLStateRiskFrontierError(
            "provisional 24/20 reconciliation boundary drift"
        )
    immutable_ids = {str(row["policy_id"]) for row in immutable_rows}
    divergent_ids = {str(row["policy_id"]) for row in divergent_rows}
    if immutable_ids & divergent_ids or len(immutable_ids | divergent_ids) != 44:
        raise PnLStateRiskFrontierError("provisional policy partition drift")

    mismatch_episode_count = sum(
        int(dict(row.get("baseline_identity") or {}).get("exact_episode_count", 0))
        for row in divergent_rows
    )
    immutable_episode_count = (
        int(dict(provisional["counters"])["exact_account_episodes"])
        - mismatch_episode_count
    )
    if immutable_episode_count <= 0:
        raise PnLStateRiskFrontierError("immutable partial episode count is invalid")
    partial_core = {
        "schema": "hydra_pnl_state_risk_frontier_immutable_partial_v1",
        "status": "IMMUTABLE_PARTIAL_24_RECONCILED_NON_DECISION_GRADE",
        "source_provisional_result_hash": provisional["result_hash"],
        "source_clean_44_ledger_hash": provisional["source_clean_44_ledger_hash"],
        "policy_count": len(immutable_rows),
        "policy_ids": [str(row["policy_id"]) for row in immutable_rows],
        "policy_result_hashes": {
            str(row["policy_id"]): str(row["result_hash"])
            for row in immutable_rows
        },
        "policy_results": immutable_rows,
        "aggregate": _aggregate(immutable_rows),
        "exact_account_episodes": immutable_episode_count,
        "replayed_in_continuation": False,
        "evidence_role": "VIEWED_DEVELOPMENT_DIAGNOSTIC_ONLY",
        "promotion_status": None,
    }
    partial = {**partial_core, "result_hash": stable_hash(partial_core)}

    pass_wrapper = _read_json(_inside(project, pass_bank_path))
    candidate_wrapper = _read_json(_inside(project, candidate_bank_path))
    marginal_wrapper = _read_json(_inside(project, marginal_books_path))
    quarantine = _read_json(_inside(project, quarantine_path))
    reconciliation = _read_json(_inside(project, reconciliation_path))
    clean_ledger = _read_json(_inside(project, clean_ledger_path))
    pass_bank = _unwrap(pass_wrapper, "combine_pass_observed_bank")
    candidate_bank = _unwrap(candidate_wrapper, "candidate_bank")
    marginal_books = _unwrap(marginal_wrapper, "marginal_book_composite")
    _verify_mapping_hash(quarantine)
    _verify_mapping_hash(reconciliation)
    _verify_clean_ledger(clean_ledger, reconciliation)
    clean_rows, excluded_rows = _clean_inventory(pass_bank, clean_ledger)
    clean_by_id = {str(row["policy_id"]): row for row in clean_rows}
    if set(clean_by_id) != immutable_ids | divergent_ids:
        raise PnLStateRiskFrontierError("canonical clean ledger/provisional drift")

    exact_cells, exact_receipt = _load_exact_cell_sources(
        project, exact_source_paths
    )
    manifest = _load_self_hashed_manifest(_inside(project, fast_pass_manifest_path))
    calendar, starts, grid_receipt = _load_frozen_grid(project, manifest)
    rules, rule_receipt = _load_rule_snapshot(_inside(project, rule_snapshot_path))
    bank_entries, bank_receipt = _load_banks(project)
    classified = {
        str(row["candidate_id"]): dict(row)
        for row in candidate_bank.get("candidates", ())
    }
    books = {
        str(row["policy_id"]): dict(row)
        for row in marginal_books.get("book_results", ())
    }
    raw_cache: dict[str, _RawComponent] = {}
    prepared = []
    for policy_id in [str(row["policy_id"]) for row in divergent_rows]:
        source = clean_by_id[policy_id]
        if (
            source.get("source_kind") != "EXACT_STANDALONE"
            or source.get("evidence_tier") != "E"
        ):
            raise PnLStateRiskFrontierError(
                f"targeted continuation escaped Tier-E standalone scope: {policy_id}"
            )
        value = _prepare_policy(
            project,
            source,
            classified=classified,
            books=books,
            bank_entries=bank_entries,
            calendar=calendar,
            rules=rules,
            raw_cache=raw_cache,
            exact_cells=exact_cells,
        )
        charges = value.baseline_policy.nominal_risk_charge_map
        if (
            str(value.source_hashes.get("exact_source_result_hash"))
            != str(classified[policy_id]["source_exact_result_hash"])
            or len(charges) != 1
            or not math.isclose(
                float(next(iter(charges.values()))), 1e-6, rel_tol=0.0, abs_tol=1e-15
            )
        ):
            raise PnLStateRiskFrontierError(
                f"targeted source is not canonical CONTRACT_ONLY: {policy_id}"
            )
        prepared.append(value)

    profiles = frozen_pnl_state_profiles()
    targeted_results: list[dict[str, Any]] = []
    targeted_episode_count = 0
    for policy in prepared:
        identity_all = _evaluate_profile(
            policy,
            profiles[0],
            blocks=ALL_BLOCKS,
            calendar=calendar,
            starts=starts,
            rule=rules[policy.account_label],
        )
        targeted_episode_count += int(identity_all["exact_episode_count"])
        mismatches = _source_mismatches(policy, identity_all)
        if mismatches:
            raise PnLStateRiskFrontierError(
                f"canonical CONTRACT_ONLY baseline still diverges: {policy.policy_id}"
            )
        design_results = []
        for profile in profiles:
            value = _evaluate_profile(
                policy,
                profile,
                blocks=DESIGN_BLOCKS,
                calendar=calendar,
                starts=starts,
                rule=rules[policy.account_label],
            )
            targeted_episode_count += int(value["exact_episode_count"])
            design_results.append(value)
        identity_design = next(
            row
            for row in design_results
            if row["profile_id"] == "pnl_state_identity"
        )
        best_design = max(design_results, key=_design_rank)
        selected_profile = next(
            row for row in profiles if row.profile_id == best_design["profile_id"]
        )
        if not _materially_improves(best_design, identity_design):
            selected_profile = profiles[0]
            best_design = identity_design
        if selected_profile.is_identity:
            selected_all = identity_all
        else:
            selected_all = _evaluate_profile(
                policy,
                selected_profile,
                blocks=ALL_BLOCKS,
                calendar=calendar,
                starts=starts,
                rule=rules[policy.account_label],
            )
            targeted_episode_count += int(selected_all["exact_episode_count"])
        targeted_results.append(
            _policy_result(
                policy,
                identity_all=identity_all,
                design_results=design_results,
                selected_design=best_design,
                selected_all=selected_all,
            )
        )

    targeted_core = {
        "schema": "hydra_pnl_state_contract_only_targeted_continuation_v1",
        "status": "TARGETED_CONTRACT_ONLY_20_REPLAY_RECONCILED",
        "source_provisional_result_hash": provisional["result_hash"],
        "source_immutable_partial_hash": partial["result_hash"],
        "source_clean_44_ledger_hash": clean_ledger["ledger_hash"],
        "exact_source_receipt": exact_receipt,
        "target_policy_count": len(targeted_results),
        "target_policy_ids": [str(row["policy_id"]) for row in targeted_results],
        "source_governor_contract": (
            "CONTRACT_ONLY_UNIFORM_SCALE_WITH_1E-6_NOMINAL_CHARGE_"
            "AND_EXACT_CHRONOLOGICAL_MLL"
        ),
        "policy_results": targeted_results,
        "aggregate": _aggregate(targeted_results),
        "exact_account_episodes": targeted_episode_count,
        "source_tier_preserved": "E",
        "authoritative_promotion_count": 0,
        "evidence_role": "VIEWED_DEVELOPMENT_DIAGNOSTIC_ONLY",
        "promotion_status": None,
    }
    targeted = {**targeted_core, "result_hash": stable_hash(targeted_core)}

    replacement = {str(row["policy_id"]): row for row in targeted_results}
    merged_rows = [
        replacement.get(str(row["policy_id"]), row) for row in original_rows
    ]
    if len(merged_rows) != 44 or any(
        row.get("status") == "BASELINE_REPLAY_MISMATCH_FAIL_CLOSED"
        for row in merged_rows
    ):
        raise PnLStateRiskFrontierError("merged frontier is not fully reconciled")
    for row in immutable_rows:
        merged = next(
            value for value in merged_rows if value["policy_id"] == row["policy_id"]
        )
        if merged != row or merged["result_hash"] != row["result_hash"]:
            raise PnLStateRiskFrontierError("immutable 24-policy partial was modified")

    survivor_set, survivor_exclusions = _dynamic_survivor_set(merged_rows)
    xfa_handoff = _diagnostic_xfa_handoff(merged_rows)
    merged_core = {
        "schema": "hydra_pnl_state_risk_frontier_reconciled_v1",
        "status": "PNL_STATE_SIZING_FRONTIER_RECONCILED_DEVELOPMENT_DIAGNOSTIC",
        "source_provisional_result_hash": provisional["result_hash"],
        "source_immutable_partial_hash": partial["result_hash"],
        "source_targeted_continuation_hash": targeted["result_hash"],
        "source_pass_bank_hash": pass_bank["result_hash"],
        "source_candidate_bank_hash": candidate_bank["result_hash"],
        "source_marginal_books_hash": marginal_books["result_hash"],
        "source_quarantine_hash": quarantine["result_hash"],
        "source_evidence_axis_reconciliation_hash": reconciliation["result_hash"],
        "source_clean_44_ledger_hash": clean_ledger["ledger_hash"],
        "source_manifest_hash": manifest["manifest_hash"],
        "frozen_grid_hash": grid_receipt["grid_hash"],
        "official_rule_snapshot_hash": rule_receipt["parsed_rule_hash"],
        "source_bank_receipt": bank_receipt,
        "profiles": [row.to_dict() for row in profiles],
        "profile_hash": stable_hash([row.to_dict() for row in profiles]),
        "policy_results": merged_rows,
        "aggregate": _aggregate(merged_rows),
        "dynamic_survivor_rule": {
            "normal_pass_delta_nonnegative_at_every_horizon": True,
            "normal_pass_delta_positive_at_one_or_more_horizons": True,
            "normal_and_stressed_selected_mll_breach_count_zero": True,
            "normal_consistency_degradation_maximum": 0.05,
            "stressed_pass_result_is_advisory_not_an_admission_veto": True,
        },
        "dynamic_survivors": survivor_set,
        "dynamic_survivor_exclusions": survivor_exclusions,
        "diagnostic_xfa_handoff": xfa_handoff,
        "inventory": {
            "source_pass_observed_count": len(pass_bank.get("policies", ())),
            "clean_non_quarantined_count": len(clean_rows),
            "quarantined_exclusion_count": len(excluded_rows),
            "clean_standalone_count": sum(
                row["source_kind"] == "EXACT_STANDALONE" for row in clean_rows
            ),
            "clean_book_count": sum(
                row["source_kind"] == "MARGINALLY_ACCEPTED_BOOK"
                for row in clean_rows
            ),
            "baseline_reconciled_count": 44,
            "baseline_mismatch_count": 0,
            "immutable_partial_count": 24,
            "targeted_contract_only_count": 20,
            "excluded": excluded_rows,
        },
        "counters": {
            "exact_account_episodes": (
                immutable_episode_count + targeted_episode_count
            ),
            "policies_evaluated": 44,
            "policies_replayed_in_continuation": 20,
            "policies_not_replayed_in_continuation": 24,
            "profiles_in_frontier": len(profiles),
            "authoritative_promotion_count": 0,
            "diagnostic_xfa_handoff_count": len(xfa_handoff),
            "xfa_paths_started": 0,
            "database_writes": 0,
            "registry_writes": 0,
            "broker_connections": 0,
            "orders": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
        },
        "normal_pass_status_contract": {
            "baseline_combine_pass_observed_status_preserved": True,
            "normal_pass_status_independent_of_stress": True,
            "stress_is_diagnostic_not_an_admission_veto": True,
            "source_bank_mutated": False,
        },
        "evidence_role": "VIEWED_DEVELOPMENT_DIAGNOSTIC_ONLY",
        "promotion_status": None,
        "runtime_seconds": time.perf_counter() - started,
        "next_action": (
            "RUN_DIAGNOSTIC_XFA_ON_CLEAN_NORMAL_PASS_PATHS_AND_FREEZE_"
            "DYNAMIC_SURVIVORS_FOR_CHRONOLOGICAL_VALIDATION"
        ),
    }
    merged_hash_core = {
        key: value for key, value in merged_core.items() if key != "runtime_seconds"
    }
    merged = {**merged_core, "result_hash": stable_hash(merged_hash_core)}
    return {"partial": partial, "targeted": targeted, "reconciled": merged}


def _clean_inventory(
    pass_bank: Mapping[str, Any], clean_ledger: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    source_rows = {
        str(row["policy_id"]): dict(row) for row in pass_bank.get("policies", ())
    }
    ledger_rows = {
        str(row["policy_id"]): dict(row) for row in clean_ledger.get("policies", ())
    }
    if set(ledger_rows) != {str(value) for value in clean_ledger["policy_ids"]}:
        raise PnLStateRiskFrontierError("canonical 44 ledger ID/index drift")
    if not set(ledger_rows).issubset(source_rows):
        raise PnLStateRiskFrontierError("canonical 44 ledger is outside pass bank")
    clean = []
    for policy_id in clean_ledger["policy_ids"]:
        source = source_rows[str(policy_id)]
        ledger = ledger_rows[str(policy_id)]
        if (
            str(source.get("source_kind")) != str(ledger.get("source_kind"))
            or str(source.get("evidence_tier"))
            != str(ledger.get("source_evidence_tier"))
            or str(source.get("classification_status"))
            != str(ledger.get("classification_status"))
        ):
            raise PnLStateRiskFrontierError(
                f"canonical 44 ledger/source identity drift: {policy_id}"
            )
        clean.append({**source, "_canonical_44_ledger_row": ledger})
    excluded = [
        {
            "policy_id": policy_id,
            "reason": "EXCLUDED_BY_CANONICAL_POLICY_CELL_LEDGER",
        }
        for policy_id in sorted(set(source_rows).difference(ledger_rows))
    ]
    return clean, excluded


def _prepare_policy(
    project: Path,
    source: Mapping[str, Any],
    *,
    classified: Mapping[str, Mapping[str, Any]],
    books: Mapping[str, Mapping[str, Any]],
    bank_entries: Sequence[Mapping[str, Any]],
    calendar: Sequence[int],
    rules: Mapping[str, Mapping[str, Any]],
    raw_cache: dict[str, _RawComponent],
    exact_cells: Mapping[tuple[str, str, str], _ExactCellSource] | None = None,
) -> _PreparedPolicy:
    policy_id = str(source["policy_id"])
    kind = str(source["source_kind"])
    if kind == "EXACT_STANDALONE":
        row = dict(classified.get(policy_id) or {})
        if not row:
            raise PnLStateRiskFrontierError(f"classified candidate absent: {policy_id}")
        primary = str(dict(source["fingerprints"]).get("primary_evidence_cell") or "")
        if primary not in {"best_safe_cell", "best_observed_pass_cell"}:
            raise PnLStateRiskFrontierError("standalone primary evidence cell is absent")
        cell = dict(row.get(primary) or {})
        ledger_selected_hash = dict(source["_canonical_44_ledger_row"]).get(
            "selected_cell_hash"
        )
        if (
            not ledger_selected_hash
            or str(cell.get("cell_hash")) != str(ledger_selected_hash)
        ):
            raise PnLStateRiskFrontierError(
                f"canonical selected-cell drift: {policy_id}"
            )
        raw = _raw_component(project, row, bank_entries, raw_cache)
        tier = int(cell["integer_quantity_tier"])
        normal = tuple(
            scale_causal_trajectory(value, executable_quantity_multiplier=tier)
            for value in raw.normal
        )
        stressed = tuple(
            scale_causal_trajectory(value, executable_quantity_multiplier=tier)
            for value in raw.stressed
        )
        _require_scenario_identity(normal, stressed)
        account_label = str(cell["account_label"])
        rule = rules[account_label]
        exact_source = None
        if exact_cells is not None:
            exact_key = (
                str(row["source_exact_result_hash"]),
                policy_id,
                str(ledger_selected_hash),
            )
            exact_source = exact_cells.get(exact_key)
            if exact_source is None:
                raise PnLStateRiskFrontierError(
                    f"canonical exact source cell absent: {policy_id}"
                )
            full_cell = dict(exact_source.cell)
            for field in (
                "candidate_id",
                "account_label",
                "integer_quantity_tier",
                "horizon_trading_days",
                "risk_governor_mode",
            ):
                expected = policy_id if field == "candidate_id" else cell.get(field)
                if str(full_cell.get(field)) != str(expected):
                    raise PnLStateRiskFrontierError(
                        f"canonical exact cell field drift: {policy_id}:{field}"
                    )
            baseline_policy = ActiveRiskPoolPolicy.from_mapping(
                full_cell["account_policy"]
            )
        else:
            charge = require_causal_stop_risk_charge(
                raw.risk_charge_per_mini,
                governor_mode=str(cell["risk_governor_mode"]),
            )
            baseline_policy = _standalone_policy(
                policy_id,
                rule,
                tier=tier,
                declared_risk_charge_per_mini=charge,
                account_contract_limit=float(rule["maximum_mini_contracts"]),
                governor_mode=str(cell["risk_governor_mode"]),
            )
        unavailable = set(int(value) for value in calendar).difference(raw.eligible_days)
        unavailable.update(raw.censored_days)
        metrics = {
            str(horizon): dict(dict(source["horizons"][str(horizon)]).get("overall") or {})
            for horizon in HORIZONS
        }
        return _PreparedPolicy(
            policy_id=policy_id,
            source_kind=kind,
            evidence_tier=str(source["evidence_tier"]),
            account_label=account_label,
            baseline_policy=baseline_policy,
            trajectories={
                "NORMAL": {policy_id: normal},
                "STRESSED_1_5X": {policy_id: stressed},
            },
            unavailable_days=frozenset(unavailable),
            source_policy=dict(source),
            source_metrics=metrics,
            source_hashes={
                "event": raw.source,
                "selected_cell": cell.get("cell_hash"),
                **(
                    {}
                    if exact_source is None
                    else {
                        "exact_source_result_hash": (
                            exact_source.source_exact_result_hash
                        ),
                        "exact_source_wrapper_hash": exact_source.source_wrapper_hash,
                        "exact_candidate_result_hash": (
                            exact_source.candidate_result_hash
                        ),
                        "exact_account_policy_hash": stable_hash(
                            dict(exact_source.cell)["account_policy"]
                        ),
                    }
                ),
            },
        )

    if kind != "MARGINALLY_ACCEPTED_BOOK":
        raise PnLStateRiskFrontierError(f"unsupported pass-bank source kind: {kind}")
    book = dict(books.get(policy_id) or {})
    if not book:
        raise PnLStateRiskFrontierError(f"marginal book source absent: {policy_id}")
    if dict(source["_canonical_44_ledger_row"]).get("selected_cell_hash") is not None:
        raise PnLStateRiskFrontierError("canonical book unexpectedly binds exact cell")
    account_label = str(book["account_label"])
    normal_map: dict[str, tuple[Any, ...]] = {}
    stressed_map: dict[str, tuple[Any, ...]] = {}
    unavailable: set[int] = set()
    receipts: dict[str, Any] = {}
    for candidate_id in book["component_ids"]:
        candidate_id = str(candidate_id)
        row = dict(classified.get(candidate_id) or {})
        raw = _raw_component(project, row, bank_entries, raw_cache)
        tier = int(dict(book["component_quantity_tiers"])[candidate_id])
        normal_map[candidate_id] = tuple(
            scale_causal_trajectory(value, executable_quantity_multiplier=tier)
            for value in raw.normal
        )
        stressed_map[candidate_id] = tuple(
            scale_causal_trajectory(value, executable_quantity_multiplier=tier)
            for value in raw.stressed
        )
        _require_scenario_identity(normal_map[candidate_id], stressed_map[candidate_id])
        unavailable.update(set(int(value) for value in calendar).difference(raw.eligible_days))
        unavailable.update(raw.censored_days)
        receipts[candidate_id] = raw.source
    baseline_policy = ActiveRiskPoolPolicy.from_mapping(book["governor_policy"])
    return _PreparedPolicy(
        policy_id=policy_id,
        source_kind=kind,
        evidence_tier=str(source["evidence_tier"]),
        account_label=account_label,
        baseline_policy=baseline_policy,
        trajectories={"NORMAL": normal_map, "STRESSED_1_5X": stressed_map},
        unavailable_days=frozenset(unavailable),
        source_policy=dict(source),
        source_metrics={
            str(horizon): {
                "normal": dict(book["summaries"]["NORMAL"][str(horizon)]),
                "stressed": dict(book["summaries"]["STRESSED_1_5X"][str(horizon)]),
            }
            for horizon in HORIZONS
        },
        source_hashes={"components": receipts, "book_result_hash": book["result_hash"]},
    )


def _raw_component(
    project: Path,
    classified: Mapping[str, Any],
    bank_entries: Sequence[Mapping[str, Any]],
    cache: dict[str, _RawComponent],
) -> _RawComponent:
    candidate_id = str(classified.get("candidate_id") or "")
    if candidate_id in cache:
        return cache[candidate_id]
    bundle = dict(classified.get("compact_evidence_bundle") or {})
    matches = [
        dict(row)
        for row in bank_entries
        if str(row.get("candidate_id")) == candidate_id
        and str(row.get("candidate_fingerprint"))
        == str(classified.get("candidate_fingerprint"))
        and str(dict(row.get("event_evidence") or {}).get("sha256"))
        == str(bundle.get("source_event_file_sha256"))
    ]
    if not matches:
        raise PnLStateRiskFrontierError(f"immutable event evidence absent: {candidate_id}")
    semantic = {
        stable_hash(
            {
                key: row.get(key)
                for key in (
                    "candidate_id", "candidate", "candidate_fingerprint",
                    "eligible_session_days", "event_evidence", "exact_hashes",
                )
            }
        )
        for row in matches
    }
    if len(semantic) != 1:
        raise PnLStateRiskFrontierError(f"duplicate event evidence drift: {candidate_id}")
    entry = min(matches, key=lambda row: int(row.get("_source_wave", 0)))
    events, receipt = _read_verified_event_evidence(project, entry["event_evidence"])
    replay = reconstruct_exact_hazard_replay(
        candidate_payload=entry["candidate"],
        event_mappings=events,
        eligible_session_days=entry["eligible_session_days"],
        expected_hashes=entry["exact_hashes"],
    )
    normal, normal_violations = _apply_session_contract(replay.normal_trajectories)
    stressed, stressed_violations = _apply_session_contract(replay.stressed_trajectories)
    if normal_violations or stressed_violations:
        raise PnLStateRiskFrontierError(f"session contract drift: {candidate_id}")
    censored = frozenset(
        int(value.session_day)
        for value in replay.events
        if str(getattr(value.outcome, "value", value.outcome))
        == HazardOutcome.CENSORED_FUTURE_COVERAGE.value
    )
    output = _RawComponent(
        candidate_id=candidate_id,
        normal=normal,
        stressed=stressed,
        eligible_days=frozenset(int(value) for value in replay.eligible_session_days),
        censored_days=censored,
        risk_charge_per_mini=_declared_stop_risk_charge_per_mini(
            events, entry["candidate"]
        ),
        source={
            "event_file_sha256": receipt["sha256"],
            "event_content_sha256": receipt["uncompressed_sha256"],
            "source_exact_result_hash": classified["source_exact_result_hash"],
        },
    )
    cache[candidate_id] = output
    return output


def _evaluate_profile(
    prepared: _PreparedPolicy,
    profile: PnLStateSizingProfile,
    *,
    blocks: Sequence[str],
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, str]]],
    rule: Mapping[str, Any],
) -> dict[str, Any]:
    block_set = set(blocks)
    config = _account_config(rule)
    policy = replace(
        prepared.baseline_policy,
        policy_id=f"{prepared.policy_id}:pnl-state:{profile.profile_id}",
    )
    summaries: dict[str, dict[str, Any]] = {scenario: {} for scenario in SCENARIOS}
    exact = 0
    with _isolated_router_patch(
        profile,
        target_usd=float(rule["profit_target_usd"]),
        mll_usd=float(rule["maximum_loss_limit_usd"]),
        consistency_fraction=float(rule["consistency_target_fraction"]),
    ):
        for scenario in SCENARIOS:
            for horizon in HORIZONS:
                requested = [
                    (int(day), str(block))
                    for day, block in starts[horizon]
                    if str(block) in block_set
                ]
                valid, censored = _valid_starts(
                    requested,
                    horizon=horizon,
                    calendar=calendar,
                    unavailable=prepared.unavailable_days,
                )
                episodes = []
                for start_day, block in valid:
                    episode = causal_replay.run_causal_shared_account_episode(
                        prepared.trajectories[scenario],
                        calendar,
                        policy=policy,
                        start_day=start_day,
                        maximum_duration_days=horizon,
                        config=config,
                    )
                    episodes.append((episode, block))
                exact += len(episodes)
                summaries[scenario][str(horizon)] = _summarize(
                    episodes,
                    requested_count=len(requested),
                    censored_count=censored,
                )
    return {
        "profile_id": profile.profile_id,
        "profile_hash": stable_hash(profile.to_dict()),
        "blocks": sorted(block_set),
        "summaries": summaries,
        "exact_episode_count": exact,
        "result_hash": stable_hash(
            {
                "policy_id": prepared.policy_id,
                "profile": profile.to_dict(),
                "blocks": sorted(block_set),
                "summaries": summaries,
            }
        ),
    }


def _valid_starts(
    starts: Sequence[tuple[int, str]],
    *,
    horizon: int,
    calendar: Sequence[int],
    unavailable: frozenset[int],
) -> tuple[list[tuple[int, str]], int]:
    index = {int(day): position for position, day in enumerate(calendar)}
    valid = []
    censored = 0
    for day, block in starts:
        if day not in index:
            raise PnLStateRiskFrontierError("frozen start is absent from calendar")
        window = calendar[index[day] : index[day] + horizon]
        if len(window) != horizon or any(int(value) in unavailable for value in window):
            censored += 1
        else:
            valid.append((day, block))
    return valid, censored


def _summarize(
    values: Sequence[tuple[Any, str]], *, requested_count: int, censored_count: int
) -> dict[str, Any]:
    episodes = [value for value, _block in values]
    if not episodes:
        return {
            "requested_start_count": requested_count,
            "full_coverage_start_count": 0,
            "data_censored_count": censored_count,
            "pass_count": 0,
            "pass_rate": 0.0,
            "mll_breach_count": 0,
            "mll_breach_rate": 0.0,
            "consistency_compliance_rate": 0.0,
            "net_total_usd": 0.0,
            "net_median_usd": 0.0,
            "target_progress_p25": 0.0,
            "target_progress_median": 0.0,
            "minimum_mll_buffer_usd": None,
            "median_days_to_target": None,
            "terminal_distribution": {},
            "by_block": {},
            "sizing_multiplier_counts": {},
            "episode_path_hash": stable_hash([]),
        }
    progress = [float(value.target_progress) for value in episodes]
    net = [float(value.net_pnl) for value in episodes]
    passing_days = [
        int(value.days_to_target)
        for value in episodes
        if value.days_to_target is not None
    ]
    terminal = Counter(value.terminal.value for value in episodes)
    multipliers = Counter(
        str(row.get("pnl_state_multiplier"))
        for value in episodes
        for row in value.risk_allocation_path
        if row.get("pnl_state_multiplier") is not None
    )
    by_block = {}
    for block in sorted({block for _value, block in values}):
        selected = [value for value, observed in values if observed == block]
        by_block[block] = {
            "episode_count": len(selected),
            "pass_count": sum(bool(value.passed) for value in selected),
            "mll_breach_count": sum(bool(value.mll_breached) for value in selected),
            "net_total_usd": sum(float(value.net_pnl) for value in selected),
            "target_progress_median": statistics.median(
                float(value.target_progress) for value in selected
            ),
        }
    paths = [value.to_dict(include_paths=True) for value in episodes]
    return {
        "requested_start_count": requested_count,
        "full_coverage_start_count": len(episodes),
        "data_censored_count": censored_count,
        "pass_count": sum(bool(value.passed) for value in episodes),
        "pass_rate": sum(bool(value.passed) for value in episodes) / len(episodes),
        "mll_breach_count": sum(bool(value.mll_breached) for value in episodes),
        "mll_breach_rate": (
            sum(bool(value.mll_breached) for value in episodes) / len(episodes)
        ),
        "consistency_compliance_rate": (
            sum(bool(value.consistency_ok) for value in episodes) / len(episodes)
        ),
        "net_total_usd": sum(net),
        "net_median_usd": statistics.median(net),
        "target_progress_p25": float(np.quantile(progress, 0.25)),
        "target_progress_median": statistics.median(progress),
        "minimum_mll_buffer_usd": min(
            float(value.minimum_mll_buffer) for value in episodes
        ),
        "median_days_to_target": (
            statistics.median(passing_days) if passing_days else None
        ),
        "terminal_distribution": dict(sorted(terminal.items())),
        "by_block": by_block,
        "sizing_multiplier_counts": dict(sorted(multipliers.items())),
        "episode_path_hash": stable_hash(paths),
    }


def _source_mismatches(
    prepared: _PreparedPolicy, identity: Mapping[str, Any]
) -> list[dict[str, Any]]:
    mismatches = []
    for horizon in HORIZONS:
        expected_pair = dict(prepared.source_metrics.get(str(horizon)) or {})
        if not expected_pair:
            continue
        if prepared.source_kind == "EXACT_STANDALONE" and not expected_pair.get("normal"):
            continue
        for scenario, source_key in (("NORMAL", "normal"), ("STRESSED_1_5X", "stressed")):
            expected = dict(expected_pair.get(source_key) or {})
            if not expected:
                continue
            actual = dict(identity["summaries"][scenario][str(horizon)])
            for expected_key, actual_key in (
                ("episode_count", "full_coverage_start_count"),
                ("pass_count", "pass_count"),
                ("mll_breach_count", "mll_breach_count"),
                ("net_total_usd", "net_total_usd"),
                ("net_total", "net_total_usd"),
                ("target_progress_median", "target_progress_median"),
                ("minimum_mll_buffer_usd", "minimum_mll_buffer_usd"),
                ("minimum_mll_buffer", "minimum_mll_buffer_usd"),
            ):
                if expected_key not in expected or expected[expected_key] is None:
                    continue
                observed = actual.get(actual_key)
                reference = expected[expected_key]
                equal = (
                    int(observed) == int(reference)
                    if expected_key.endswith("count")
                    else math.isclose(float(observed), float(reference), rel_tol=1e-9, abs_tol=1e-6)
                )
                if not equal:
                    mismatches.append(
                        {
                            "horizon": horizon,
                            "scenario": scenario,
                            "field": actual_key,
                            "source": reference,
                            "replayed": observed,
                        }
                    )
    return mismatches


def _mismatch_result(
    prepared: _PreparedPolicy,
    identity: Mapping[str, Any],
    mismatches: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    core = {
        "policy_id": prepared.policy_id,
        "source_kind": prepared.source_kind,
        "baseline_evidence_tier_preserved": prepared.evidence_tier,
        "status": "BASELINE_REPLAY_MISMATCH_FAIL_CLOSED",
        "baseline_identity": dict(identity),
        "mismatches": [dict(value) for value in mismatches],
        "selected_profile_id": None,
        "normal_pass_status_erased": False,
        "authoritative_promotion_status": None,
    }
    return {**core, "result_hash": stable_hash(core)}


def _policy_result(
    prepared: _PreparedPolicy,
    *,
    identity_all: Mapping[str, Any],
    design_results: Sequence[Mapping[str, Any]],
    selected_design: Mapping[str, Any],
    selected_all: Mapping[str, Any],
) -> dict[str, Any]:
    deltas = _metric_deltas(identity_all, selected_all)
    selected_id = str(selected_all["profile_id"])
    dynamic = selected_id != "pnl_state_identity"
    normal_improved = any(
        int(deltas[str(horizon)]["normal_pass_delta"]) > 0 for horizon in HORIZONS
    )
    stress_improved = any(
        int(deltas[str(horizon)]["stressed_pass_delta"]) > 0 for horizon in HORIZONS
    )
    core = {
        "policy_id": prepared.policy_id,
        "source_kind": prepared.source_kind,
        "account_label": prepared.account_label,
        "baseline_evidence_tier_preserved": prepared.evidence_tier,
        "baseline_classification_status": prepared.source_policy.get(
            "classification_status"
        ),
        "status": (
            "DYNAMIC_SIZING_SELECTED_DEVELOPMENT_DIAGNOSTIC"
            if dynamic
            else "STATIC_BASELINE_RETAINED"
        ),
        "selected_profile_id": selected_id,
        "selected_on_blocks": list(DESIGN_BLOCKS),
        "selection_held_out_fields_used": False,
        "design_frontier": [
            {
                "profile_id": row["profile_id"],
                "summaries": row["summaries"],
                "design_rank": list(_design_rank(row)),
                "result_hash": row["result_hash"],
            }
            for row in design_results
        ],
        "selected_design_result_hash": selected_design["result_hash"],
        "baseline": dict(identity_all),
        "selected": dict(selected_all),
        "deltas_vs_frozen_baseline": deltas,
        "normal_pass_improved": normal_improved,
        "stressed_pass_improved_diagnostic": stress_improved,
        "normal_pass_status_erased": False,
        "stress_used_as_admission_veto": False,
        "source_hashes": dict(prepared.source_hashes),
        "authoritative_promotion_status": None,
    }
    return {**core, "result_hash": stable_hash(core)}


def _metric_deltas(
    baseline: Mapping[str, Any], selected: Mapping[str, Any]
) -> dict[str, Any]:
    output = {}
    for horizon in HORIZONS:
        normal_base = baseline["summaries"]["NORMAL"][str(horizon)]
        normal_new = selected["summaries"]["NORMAL"][str(horizon)]
        stress_base = baseline["summaries"]["STRESSED_1_5X"][str(horizon)]
        stress_new = selected["summaries"]["STRESSED_1_5X"][str(horizon)]
        output[str(horizon)] = {
            "normal_pass_delta": int(normal_new["pass_count"])
            - int(normal_base["pass_count"]),
            "stressed_pass_delta": int(stress_new["pass_count"])
            - int(stress_base["pass_count"]),
            "normal_mll_breach_delta": int(normal_new["mll_breach_count"])
            - int(normal_base["mll_breach_count"]),
            "stressed_mll_breach_delta": int(stress_new["mll_breach_count"])
            - int(stress_base["mll_breach_count"]),
            "normal_consistency_delta": float(
                normal_new["consistency_compliance_rate"]
            ) - float(normal_base["consistency_compliance_rate"]),
            "stressed_consistency_delta": float(
                stress_new["consistency_compliance_rate"]
            ) - float(stress_base["consistency_compliance_rate"]),
            "normal_target_progress_median_delta": float(
                normal_new["target_progress_median"]
            ) - float(normal_base["target_progress_median"]),
            "stressed_target_progress_median_delta": float(
                stress_new["target_progress_median"]
            ) - float(stress_base["target_progress_median"]),
        }
    return output


def _design_rank(value: Mapping[str, Any]) -> tuple[Any, ...]:
    normal = value["summaries"]["NORMAL"]
    stressed = value["summaries"]["STRESSED_1_5X"]
    weights = {5: 4, 10: 2, 20: 1}
    normal_mll = sum(int(normal[str(h)]["mll_breach_count"]) for h in HORIZONS)
    normal_passes = sum(
        weights[h] * int(normal[str(h)]["pass_count"]) for h in HORIZONS
    )
    stress_passes = sum(
        weights[h] * int(stressed[str(h)]["pass_count"]) for h in HORIZONS
    )
    consistency = sum(
        float(normal[str(h)]["consistency_compliance_rate"]) for h in HORIZONS
    )
    progress = sum(
        weights[h] * float(normal[str(h)]["target_progress_median"])
        for h in HORIZONS
    )
    minimum_buffer = min(
        float(normal[str(h)]["minimum_mll_buffer_usd"])
        for h in HORIZONS
        if normal[str(h)]["minimum_mll_buffer_usd"] is not None
    )
    stress_net = sum(float(stressed[str(h)]["net_total_usd"]) for h in HORIZONS)
    return (
        int(normal_mll == 0),
        -normal_mll,
        normal_passes,
        consistency,
        progress,
        minimum_buffer,
        stress_passes,
        stress_net,
    )


def _materially_improves(
    candidate: Mapping[str, Any], identity: Mapping[str, Any]
) -> bool:
    if candidate["profile_id"] == identity["profile_id"]:
        return False
    candidate_rank = _design_rank(candidate)
    identity_rank = _design_rank(identity)
    if candidate_rank <= identity_rank:
        return False
    # Avoid selecting a sizing variant on a microscopic floating-point delta.
    normal_candidate = candidate["summaries"]["NORMAL"]
    normal_identity = identity["summaries"]["NORMAL"]
    pass_delta = sum(
        int(normal_candidate[str(h)]["pass_count"])
        - int(normal_identity[str(h)]["pass_count"])
        for h in HORIZONS
    )
    mll_delta = sum(
        int(normal_candidate[str(h)]["mll_breach_count"])
        - int(normal_identity[str(h)]["mll_breach_count"])
        for h in HORIZONS
    )
    progress_delta = sum(
        float(normal_candidate[str(h)]["target_progress_median"])
        - float(normal_identity[str(h)]["target_progress_median"])
        for h in HORIZONS
    )
    return bool(pass_delta > 0 or mll_delta < 0 or progress_delta >= 0.02)


def _aggregate(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    valid = [row for row in results if row["status"] != "BASELINE_REPLAY_MISMATCH_FAIL_CLOSED"]
    output: dict[str, Any] = {
        "reconciled_policy_count": len(valid),
        "dynamic_profile_selected_count": sum(
            str(row.get("selected_profile_id")) != "pnl_state_identity" for row in valid
        ),
        "normal_pass_improved_policy_count": sum(
            bool(row.get("normal_pass_improved")) for row in valid
        ),
        "stressed_pass_improved_policy_count": sum(
            bool(row.get("stressed_pass_improved_diagnostic")) for row in valid
        ),
        "by_horizon": {},
    }
    for horizon in HORIZONS:
        row: dict[str, Any] = {}
        for label, key in (("normal", "NORMAL"), ("stressed", "STRESSED_1_5X")):
            baseline = [value["baseline"]["summaries"][key][str(horizon)] for value in valid]
            selected = [value["selected"]["summaries"][key][str(horizon)] for value in valid]
            row[label] = {
                "baseline_pass_count": sum(int(value["pass_count"]) for value in baseline),
                "selected_pass_count": sum(int(value["pass_count"]) for value in selected),
                "baseline_policies_with_pass": sum(int(value["pass_count"]) > 0 for value in baseline),
                "selected_policies_with_pass": sum(int(value["pass_count"]) > 0 for value in selected),
                "baseline_mll_breach_count": sum(int(value["mll_breach_count"]) for value in baseline),
                "selected_mll_breach_count": sum(int(value["mll_breach_count"]) for value in selected),
                "baseline_consistency_rate_mean": statistics.mean(
                    float(value["consistency_compliance_rate"]) for value in baseline
                ) if baseline else None,
                "selected_consistency_rate_mean": statistics.mean(
                    float(value["consistency_compliance_rate"]) for value in selected
                ) if selected else None,
                "baseline_target_progress_median_of_policies": statistics.median(
                    float(value["target_progress_median"]) for value in baseline
                ) if baseline else None,
                "selected_target_progress_median_of_policies": statistics.median(
                    float(value["target_progress_median"]) for value in selected
                ) if selected else None,
            }
        output["by_horizon"][str(horizon)] = row
    return output


def _dynamic_survivor_set(
    results: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    survivors = []
    exclusions = []
    for row in results:
        profile_id = str(row.get("selected_profile_id") or "")
        if profile_id in {"", "pnl_state_identity"}:
            continue
        policy_id = str(row["policy_id"])
        selected = dict(row["selected"])["summaries"]
        deltas = dict(row["deltas_vs_frozen_baseline"])
        normal_mll = sum(
            int(selected["NORMAL"][str(horizon)]["mll_breach_count"])
            for horizon in HORIZONS
        )
        stressed_mll = sum(
            int(selected["STRESSED_1_5X"][str(horizon)]["mll_breach_count"])
            for horizon in HORIZONS
        )
        normal_pass_deltas = {
            str(horizon): int(deltas[str(horizon)]["normal_pass_delta"])
            for horizon in HORIZONS
        }
        minimum_consistency_delta = min(
            float(deltas[str(horizon)]["normal_consistency_delta"])
            for horizon in HORIZONS
        )
        reasons = []
        if policy_id == EXPLICIT_STRESSED_MLL_EXCLUSION:
            reasons.append("EXPLICIT_STRESSED_MLL_BREACH_EXCLUSION")
        if normal_mll:
            reasons.append("NORMAL_MLL_BREACH")
        if stressed_mll:
            reasons.append("STRESSED_MLL_BREACH")
        if any(value < 0 for value in normal_pass_deltas.values()):
            reasons.append("NORMAL_PASS_DEGRADATION")
        if not any(value > 0 for value in normal_pass_deltas.values()):
            reasons.append("NO_NORMAL_PASS_IMPROVEMENT")
        if minimum_consistency_delta < -0.05 - 1e-12:
            reasons.append("MATERIAL_NORMAL_CONSISTENCY_DEGRADATION")
        payload = {
            "policy_id": policy_id,
            "selected_profile_id": profile_id,
            "normal_pass_deltas": normal_pass_deltas,
            "normal_mll_breach_count": normal_mll,
            "stressed_mll_breach_count": stressed_mll,
            "minimum_normal_consistency_delta": minimum_consistency_delta,
            "stress_pass_status_advisory_only": True,
            "source_evidence_tier": row["baseline_evidence_tier_preserved"],
            "authoritative_promotion_status": None,
        }
        if reasons:
            exclusions.append({**payload, "reasons": reasons})
        else:
            survivors.append(
                {
                    **payload,
                    "status": "DYNAMIC_SIZING_DEVELOPMENT_SURVIVOR",
                }
            )
    return survivors, exclusions


def _diagnostic_xfa_handoff(
    results: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    output = []
    for row in results:
        selected = dict(row["selected"])["summaries"]
        for horizon in HORIZONS:
            normal = dict(selected["NORMAL"][str(horizon)])
            stressed = dict(selected["STRESSED_1_5X"][str(horizon)])
            if (
                int(normal["pass_count"]) <= 0
                or int(normal["mll_breach_count"]) != 0
            ):
                continue
            core = {
                "policy_id": str(row["policy_id"]),
                "selected_profile_id": str(row["selected_profile_id"]),
                "horizon_trading_days": int(horizon),
                "normal_pass_count": int(normal["pass_count"]),
                "normal_full_coverage_start_count": int(
                    normal["full_coverage_start_count"]
                ),
                "normal_episode_path_hash": str(normal["episode_path_hash"]),
                "stressed_pass_count_advisory": int(stressed["pass_count"]),
                "stressed_mll_breach_count_advisory": int(
                    stressed["mll_breach_count"]
                ),
                "source_evidence_tier": row["baseline_evidence_tier_preserved"],
                "status": "COMBINE_PASS_PATH_DIAGNOSTIC_XFA_ELIGIBLE",
                "promotion_implied": False,
            }
            output.append({**core, "handoff_hash": stable_hash(core)})
    return output


def _load_exact_cell_sources(
    project: Path,
    paths: Sequence[str | Path],
) -> tuple[dict[tuple[str, str, str], _ExactCellSource], dict[str, Any]]:
    output: dict[tuple[str, str, str], _ExactCellSource] = {}
    files = []
    for path_like in paths:
        path = _inside(project, path_like)
        wrapper = _read_json(path)
        _verify_runtime_optional_result_hash(wrapper)
        if isinstance(wrapper.get("continuation_result"), Mapping):
            continuation = dict(wrapper["continuation_result"])
            _verify_runtime_optional_result_hash(continuation)
            exact = dict(continuation.get("exact_result") or {})
        else:
            exact = dict(wrapper)
        _verify_runtime_optional_result_hash(exact)
        if (
            exact.get("status") != "COMPLETE_EXACT_CAUSAL_ACCOUNT_SIZE_RACE"
            or exact.get("source_campaign_id") != "hydra_fast_pass_factory_0029"
        ):
            raise PnLStateRiskFrontierError(
                f"unexpected exact source identity: {path}"
            )
        source_hash = str(exact["result_hash"])
        candidate_count = 0
        cell_count = 0
        for candidate in exact.get("results", ()):
            candidate = dict(candidate)
            claimed_candidate_hash = str(candidate.get("candidate_result_hash") or "")
            candidate_core = {
                key: value
                for key, value in candidate.items()
                if key != "candidate_result_hash"
            }
            if (
                not claimed_candidate_hash
                or stable_hash(candidate_core) != claimed_candidate_hash
            ):
                raise PnLStateRiskFrontierError(
                    f"exact candidate hash drift: {path}"
                )
            candidate_id = str(candidate["candidate_id"])
            candidate_count += 1
            for raw_cell in candidate.get("frontier", ()):
                cell = dict(raw_cell)
                cell_hash = stable_hash(cell)
                key = (source_hash, candidate_id, cell_hash)
                value = _ExactCellSource(
                    candidate_id=candidate_id,
                    source_exact_result_hash=source_hash,
                    source_wrapper_hash=str(wrapper["result_hash"]),
                    source_path=str(path.relative_to(project)),
                    candidate_result_hash=claimed_candidate_hash,
                    cell_hash=cell_hash,
                    cell=cell,
                )
                prior = output.get(key)
                if prior is not None and prior != value:
                    raise PnLStateRiskFrontierError(
                        f"duplicate exact source-cell drift: {candidate_id}"
                    )
                output[key] = value
                cell_count += 1
        files.append(
            {
                "path": str(path.relative_to(project)),
                "wrapper_result_hash": str(wrapper["result_hash"]),
                "exact_result_hash": source_hash,
                "candidate_count": candidate_count,
                "cell_count": cell_count,
            }
        )
    receipt_core = {
        "schema": "hydra_exact_0029_source_cell_receipt_v1",
        "file_count": len(files),
        "files": files,
        "unique_cell_count": len(output),
    }
    return output, {**receipt_core, "receipt_hash": stable_hash(receipt_core)}


def _verify_runtime_optional_result_hash(value: Mapping[str, Any]) -> None:
    claimed = str(value.get("result_hash") or "")
    with_runtime = {
        key: item for key, item in value.items() if key != "result_hash"
    }
    without_runtime = dict(with_runtime)
    without_runtime.pop("runtime_seconds", None)
    if not claimed or claimed not in {
        stable_hash(with_runtime),
        stable_hash(without_runtime),
    }:
        raise PnLStateRiskFrontierError("runtime-optional source result hash drift")


def _verify_provisional_result(value: Mapping[str, Any]) -> None:
    if (
        value.get("schema") != SCHEMA
        or value.get("result_hash") != EXPECTED_PROVISIONAL_RESULT_HASH
        or int(dict(value.get("inventory") or {}).get("baseline_reconciled_count", -1))
        != 24
        or int(dict(value.get("inventory") or {}).get("baseline_mismatch_count", -1))
        != 20
    ):
        raise PnLStateRiskFrontierError("provisional result identity drift")
    core = {
        key: item
        for key, item in value.items()
        if key not in {"result_hash", "runtime_seconds"}
    }
    if stable_hash(core) != str(value["result_hash"]):
        raise PnLStateRiskFrontierError("provisional result hash drift")


def _unwrap(value: Mapping[str, Any], key: str) -> dict[str, Any]:
    _verify_self_hash(value)
    nested = value.get(key)
    if not isinstance(nested, Mapping):
        raise PnLStateRiskFrontierError(f"missing source payload: {key}")
    output = dict(nested)
    _verify_self_hash(output)
    return output


def _verify_mapping_hash(value: Mapping[str, Any]) -> None:
    claimed = str(value.get("result_hash") or "")
    core = {key: item for key, item in value.items() if key != "result_hash"}
    if not claimed or stable_hash(core) != claimed:
        raise PnLStateRiskFrontierError("source quarantine result hash mismatch")


def _verify_clean_ledger(
    ledger: Mapping[str, Any], reconciliation: Mapping[str, Any]
) -> None:
    claimed = str(ledger.get("ledger_hash") or "")
    core = {key: item for key, item in ledger.items() if key != "ledger_hash"}
    policy_ids = [str(value) for value in ledger.get("policy_ids", ())]
    rows = [dict(value) for value in ledger.get("policies", ())]
    if (
        ledger.get("schema")
        != "hydra_clean_combine_pass_observed_policy_ledger_v1"
        or ledger.get("status")
        != "CANONICAL_CLEAN_COMBINE_PASS_OBSERVED_DEVELOPMENT_LEDGER"
        or stable_hash(core) != claimed
        or claimed
        != "83bf832bb9c9c21b7249614d6dfe95bee178963e06a55ce63c40cfca51bde04c"
        or int(ledger.get("policy_count", -1)) != 44
        or len(policy_ids) != 44
        or len(set(policy_ids)) != 44
        or len(rows) != 44
        or str(ledger.get("source_reconciliation_hash"))
        != str(reconciliation.get("result_hash"))
        or str(ledger.get("quarantine_scope"))
        != "EXACT_POLICY_CELL_OR_EXACT_BOOK_ONLY"
    ):
        raise PnLStateRiskFrontierError("canonical 44-policy ledger drift")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise PnLStateRiskFrontierError(f"required source file absent: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise PnLStateRiskFrontierError(f"source JSON is not a mapping: {path}")
    return value


__all__ = [
    "PnLStateRiskFrontierError",
    "PnLStateSizingProfile",
    "SCHEMA",
    "build_pnl_state_risk_frontier",
    "continue_contract_only_pnl_state_frontier",
    "frozen_pnl_state_profiles",
    "pnl_state_multiplier",
]
