"""Deterministic Combine-pass continuation handoffs for Tier-G books.

The Tier-G development receipts deliberately contain no funded-account book or
profile.  This module leaves those receipts untouched, reconstructs the exact
causal Combine episodes that produced them, and freezes a separate, minimal
account-size-aware XFA handoff.  It performs no payout simulation and has no
writer, registry, database, broker, or order capability.

Large causal trajectory ledgers are stored once per candidate/scenario inside
the returned payload.  Each successful Combine transition references an exact
post-pass slice by hash.  :func:`materialize_transition_trajectories` verifies
and materializes that slice for an XFA engine without recalculating signals or
changing a Combine outcome.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_marginal_combine_books as books
from hydra.production import autonomous_tier_g_controls as controls
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_RULE_SNAPSHOT,
    _inside,
    _load_banks,
    _load_frozen_grid,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
    _summarize_exact_episodes,
)
from hydra.production.autonomous_tier_g_graduation import (
    verify_tier_g_development_graduation,
)
from hydra.research.causal_sleeve_replay import CausalTradeTrajectory


SCHEMA = "hydra_tier_g_combine_xfa_handoff_v1"
SOURCE_TAPE_SCHEMA = "hydra_tier_g_causal_source_tape_v1"
TRANSITION_SCHEMA = "hydra_tier_g_combine_xfa_transition_v1"
XFA_PROFILE_SCHEMA = "hydra_tier_g_frozen_xfa_profile_v1"
XFA_BOOK_SCHEMA = "hydra_tier_g_frozen_xfa_book_v1"
STATUS = "COMPLETE_IMMUTABLE_TIER_G_XFA_HANDOFF"
READY_STATUS = "READY_FOR_ACCOUNT_SIZE_AWARE_XFA_DIAGNOSTIC"
FAIL_CLOSED_STATUS = "XFA_CONTINUATION_UNAVAILABLE_FAIL_CLOSED"


class AutonomousTierGXfaHandoffError(RuntimeError):
    """The immutable Combine evidence cannot yield a safe XFA handoff."""


def build_tier_g_combine_xfa_handoffs(
    root: str | Path,
    candidate_bank: Mapping[str, Any],
    initial_exact_result: Mapping[str, Any],
    continuation_results: Sequence[Mapping[str, Any]],
    tier_g_graduation: Mapping[str, Any],
    *,
    fast_pass_manifest_path: str | Path = DEFAULT_FAST_PASS_MANIFEST,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> dict[str, Any]:
    """Reconstruct all successful Tier-G Combine paths without side effects."""

    graduation = dict(tier_g_graduation)
    verify_tier_g_development_graduation(graduation)
    if str(graduation.get("evidence_tier")) != "G_DEVELOPMENT_ONLY":
        raise AutonomousTierGXfaHandoffError(
            "only graduated development books may enter the XFA handoff"
        )
    bank = books._verify_candidate_bank(candidate_bank)
    composite, exact_results = books._verified_exact_results(
        initial_exact_result, continuation_results
    )
    if str(bank["result_hash"]) != str(graduation["source_candidate_bank_hash"]):
        raise AutonomousTierGXfaHandoffError(
            "graduation and candidate-bank provenance differ"
        )
    if str(composite["result_hash"]) != str(graduation["source_exact_composite_hash"]):
        raise AutonomousTierGXfaHandoffError(
            "graduation and exact-composite provenance differ"
        )

    project = Path(root).resolve()
    manifest = _load_self_hashed_manifest(
        _inside(project, fast_pass_manifest_path)
    )
    calendar, starts, grid_receipt = _load_frozen_grid(project, manifest)
    rules, rule_receipt = _load_rule_snapshot(
        _inside(project, rule_snapshot_path)
    )
    rule_snapshot = _load_json_object(_inside(project, rule_snapshot_path))
    bank_entries, bank_receipt = _load_banks(project)
    exact_by_hash = {str(row["result_hash"]): dict(row) for row in exact_results}
    if len(exact_by_hash) != len(exact_results):
        raise AutonomousTierGXfaHandoffError("duplicate exact-result source hash")

    graduated_rows = {
        str(row["candidate_id"]): dict(row)
        for row in graduation.get("graduated_development_books", ())
    }
    declared_ids = tuple(
        str(row)
        for row in dict(graduation.get("candidate_ids") or {}).get(
            "graduated_development_books", ()
        )
    )
    if not declared_ids or set(declared_ids) != set(graduated_rows):
        raise AutonomousTierGXfaHandoffError(
            "graduated candidate inventory is absent or inconsistent"
        )
    classified = {
        str(row["candidate_id"]): dict(row)
        for row in bank.get("candidates", ())
        if str(row.get("candidate_id")) in graduated_rows
    }
    if set(classified) != set(graduated_rows):
        raise AutonomousTierGXfaHandoffError(
            "graduated candidate is absent from the immutable bank"
        )

    prepared: list[dict[str, Any]] = []
    for candidate_id in sorted(graduated_rows):
        value = controls._prepare_candidate(
            project=project,
            classified=classified[candidate_id],
            exact_by_hash=exact_by_hash,
            bank_entries=bank_entries,
            calendar=calendar,
            starts=starts,
            rules=rules,
        )
        prepared.append(value)
    return build_tier_g_handoffs_from_prepared(
        prepared,
        graduation,
        rule_snapshot=rule_snapshot,
        source_manifest_hash=str(manifest["manifest_hash"]),
        frozen_grid=grid_receipt,
        official_rule_snapshot=rule_receipt,
        source_bank_receipt=bank_receipt,
        source_exact_composite_hash=str(composite["result_hash"]),
    )


def build_tier_g_handoffs_from_prepared(
    prepared_candidates: Sequence[Mapping[str, Any]],
    tier_g_graduation: Mapping[str, Any],
    *,
    rule_snapshot: Mapping[str, Any],
    source_manifest_hash: str,
    frozen_grid: Mapping[str, Any],
    official_rule_snapshot: Mapping[str, Any],
    source_bank_receipt: Mapping[str, Any],
    source_exact_composite_hash: str,
) -> dict[str, Any]:
    """Pure reconstruction primitive used by the artifact wrapper and tests."""

    graduation = dict(tier_g_graduation)
    verify_tier_g_development_graduation(graduation)
    graduation_rows = {
        str(row["candidate_id"]): dict(row)
        for row in graduation.get("graduated_development_books", ())
    }
    prepared = {str(row["candidate_id"]): dict(row) for row in prepared_candidates}
    if not prepared or set(prepared) != set(graduation_rows):
        raise AutonomousTierGXfaHandoffError(
            "prepared candidate inventory differs from graduation"
        )
    if str(source_exact_composite_hash) != str(
        graduation["source_exact_composite_hash"]
    ):
        raise AutonomousTierGXfaHandoffError("exact-composite binding drift")

    source_tapes: dict[str, dict[str, Any]] = {}
    candidate_handoffs: list[dict[str, Any]] = []
    transitions: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    exact_episode_count = 0

    for candidate_id in sorted(prepared):
        value = prepared[candidate_id]
        receipt = graduation_rows[candidate_id]
        _verify_prepared_receipt_binding(value, receipt, official_rule_snapshot)
        xfa_profile = _freeze_xfa_profile(value, receipt, rule_snapshot)
        xfa_book = _freeze_xfa_book(value, receipt, xfa_profile)
        candidate_transition_ids: list[str] = []
        candidate_failure_ids: list[str] = []

        for scenario_name, key in (("NORMAL", "normal"), ("STRESSED", "stressed")):
            trajectories = tuple(value[key])
            source_tape = _source_tape(
                candidate_id=candidate_id,
                scenario=scenario_name,
                trajectories=trajectories,
                eligible_session_days=value["calendar"],
                source_event_receipt=value["source_event_receipt"],
            )
            source_tape_key = f"{candidate_id}:{scenario_name}"
            source_tapes[source_tape_key] = source_tape
            episodes = [
                (
                    run_causal_shared_account_episode(
                        {candidate_id: trajectories},
                        value["calendar"],
                        policy=value["policy"],
                        start_day=int(start_day),
                        maximum_duration_days=int(value["horizon"]),
                        config=value["config"],
                    ),
                    str(block),
                )
                for start_day, block in value["starts"]
            ]
            exact_episode_count += len(episodes)
            summary = _summarize_exact_episodes(episodes)
            expected = dict(value["selected_cell"])[key]
            if stable_hash(summary) != stable_hash(expected):
                raise AutonomousTierGXfaHandoffError(
                    "Combine outcome reconstruction drift: "
                    f"{candidate_id}:{scenario_name}"
                )
            expected_path_hash = str(
                dict(receipt[f"{key}_economics"])["episode_path_hash"]
            )
            if str(summary["episode_path_hash"]) != expected_path_hash:
                raise AutonomousTierGXfaHandoffError(
                    "graduation episode-path binding drift: "
                    f"{candidate_id}:{scenario_name}"
                )

            for episode, block in episodes:
                if not episode.passed:
                    continue
                transition = _transition_handoff(
                    candidate_id=candidate_id,
                    scenario=scenario_name,
                    block=block,
                    episode=episode,
                    source_tape_key=source_tape_key,
                    source_tape=source_tape,
                    combine_book=receipt["combine_book"],
                    xfa_book=xfa_book,
                    xfa_profile=xfa_profile,
                )
                if transition["status"] == READY_STATUS:
                    transitions.append(transition)
                    candidate_transition_ids.append(str(transition["transition_id"]))
                else:
                    failures.append(transition)
                    candidate_failure_ids.append(str(transition["transition_id"]))

        expected_normal = int(dict(receipt["normal_economics"])["pass_count"])
        expected_stressed = int(dict(receipt["stressed_economics"])["pass_count"])
        observed_normal = sum(
            1
            for row in (*transitions, *failures)
            if row["candidate_id"] == candidate_id and row["scenario"] == "NORMAL"
        )
        observed_stressed = sum(
            1
            for row in (*transitions, *failures)
            if row["candidate_id"] == candidate_id
            and row["scenario"] == "STRESSED"
        )
        if (observed_normal, observed_stressed) != (
            expected_normal,
            expected_stressed,
        ):
            raise AutonomousTierGXfaHandoffError(
                f"successful Combine transition count drift: {candidate_id}"
            )
        candidate_core = {
            "candidate_id": candidate_id,
            "account_label": str(value["account_label"]),
            "account_size_usd": int(value["account_size_usd"]),
            "combine_book_hash": str(receipt["combine_book_hash"]),
            "frozen_combine_account_policy_hash": str(
                value["frozen_account_policy_hash"]
            ),
            "xfa_book": xfa_book,
            "xfa_profile": xfa_profile,
            "source_tape_keys": [
                f"{candidate_id}:NORMAL",
                f"{candidate_id}:STRESSED",
            ],
            "ready_transition_ids": candidate_transition_ids,
            "fail_closed_transition_ids": candidate_failure_ids,
            "combine_outcomes_modified": False,
            "underlying_signal_modified": False,
            "evidence_role": "DIAGNOSTIC_XFA_HANDOFF_FROM_VIEWED_DEVELOPMENT",
        }
        candidate_handoffs.append(
            {**candidate_core, "candidate_handoff_hash": stable_hash(candidate_core)}
        )

    core: dict[str, Any] = {
        "schema": SCHEMA,
        "status": STATUS,
        "source_tier_g_graduation_hash": str(graduation["result_hash"]),
        "source_candidate_bank_hash": str(graduation["source_candidate_bank_hash"]),
        "source_exact_composite_hash": str(source_exact_composite_hash),
        "source_manifest_hash": str(source_manifest_hash),
        "frozen_grid": dict(frozen_grid),
        "official_rule_snapshot": dict(official_rule_snapshot),
        "source_bank_receipt": dict(source_bank_receipt),
        "candidate_handoffs": candidate_handoffs,
        "source_tapes": source_tapes,
        "transitions": sorted(
            transitions, key=lambda row: str(row["transition_id"])
        ),
        "fail_closed_transitions": sorted(
            failures, key=lambda row: str(row["transition_id"])
        ),
        "counts": {
            "candidate_count": len(candidate_handoffs),
            "exact_combine_episode_reconstruction_count": exact_episode_count,
            "successful_combine_transition_count": len(transitions) + len(failures),
            "ready_xfa_transition_count": len(transitions),
            "fail_closed_transition_count": len(failures),
            "source_tape_count": len(source_tapes),
            "xfa_simulations_started": 0,
            "database_writes": 0,
            "registry_writes": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "combine_outcomes_modified": False,
        "graduation_receipts_modified": False,
        "independent_confirmation_claimed": False,
        "promotion_status": None,
        "next_action": (
            "RUN_SEPARATE_STANDARD_AND_CONSISTENCY_XFA_DIAGNOSTICS"
            if transitions
            else "PRESERVE_TIER_G_AND_REPORT_NO_POST_PASS_CONTINUATION"
        ),
    }
    result = {**core, "result_hash": stable_hash(core)}
    verify_tier_g_combine_xfa_handoffs(result)
    return result


def materialize_transition_trajectories(
    handoff: Mapping[str, Any], transition_id: str
) -> dict[str, tuple[CausalTradeTrajectory, ...]]:
    """Return one verified post-pass causal trajectory slice."""

    value = verify_tier_g_combine_xfa_handoffs(handoff)
    matches = [
        dict(row)
        for row in value.get("transitions", ())
        if str(row.get("transition_id")) == str(transition_id)
    ]
    if len(matches) != 1:
        raise AutonomousTierGXfaHandoffError(
            "ready transition is absent or duplicated"
        )
    transition = matches[0]
    tape = dict(dict(value["source_tapes"])[transition["source_tape_key"]])
    pass_day = int(transition["combine_pass_day"])
    rows = [
        CausalTradeTrajectory.from_mapping(row)
        for row in tape.get("completed_trajectories", ())
        if int(dict(row["event"])["session_day"]) > pass_day
    ]
    ordered = tuple(
        sorted(rows, key=lambda row: (row.event.decision_ns, row.event.event_id))
    )
    payload = [row.to_dict() for row in ordered]
    if (
        len(ordered) != int(transition["post_pass_completed_trajectory_count"])
        or stable_hash(payload) != str(transition["post_pass_trajectory_hash"])
    ):
        raise AutonomousTierGXfaHandoffError(
            "post-pass trajectory slice does not reconcile"
        )
    return {str(transition["candidate_id"]): ordered}


def verify_tier_g_combine_xfa_handoffs(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Fail closed on hash, binding, transition, or side-effect drift."""

    row = dict(value)
    claimed = row.pop("result_hash", None)
    if row.get("schema") != SCHEMA or row.get("status") != STATUS:
        raise AutonomousTierGXfaHandoffError("XFA handoff schema/status drift")
    if claimed != stable_hash(row):
        raise AutonomousTierGXfaHandoffError("XFA handoff result hash drift")
    counts = dict(row.get("counts") or {})
    for field in (
        "xfa_simulations_started",
        "database_writes",
        "registry_writes",
        "broker_connections",
        "orders",
    ):
        if int(counts.get(field, -1)) != 0:
            raise AutonomousTierGXfaHandoffError(
                f"read-only XFA handoff side effect: {field}"
            )
    tapes = dict(row.get("source_tapes") or {})
    for key, raw in tapes.items():
        tape = dict(raw)
        tape_hash = tape.pop("source_tape_hash", None)
        if tape.get("schema") != SOURCE_TAPE_SCHEMA or tape_hash != stable_hash(tape):
            raise AutonomousTierGXfaHandoffError(
                f"source-tape identity/hash drift: {key}"
            )
    handoffs = list(row.get("candidate_handoffs") or ())
    candidate_ids: set[str] = set()
    for raw in handoffs:
        candidate = dict(raw)
        candidate_hash = candidate.pop("candidate_handoff_hash", None)
        candidate_id = str(candidate.get("candidate_id") or "")
        if (
            not candidate_id
            or candidate_id in candidate_ids
            or candidate_hash != stable_hash(candidate)
        ):
            raise AutonomousTierGXfaHandoffError(
                "candidate handoff identity/hash drift or duplication"
            )
        candidate_ids.add(candidate_id)
        profile = dict(candidate.get("xfa_profile") or {})
        profile_hash = profile.pop("xfa_profile_hash", None)
        profile.pop("xfa_profile_id", None)
        if (
            profile.get("schema") != XFA_PROFILE_SCHEMA
            or profile_hash != stable_hash(profile)
        ):
            raise AutonomousTierGXfaHandoffError(
                f"XFA profile identity/hash drift: {candidate_id}"
            )
        book = dict(candidate.get("xfa_book") or {})
        book_hash = book.pop("xfa_book_hash", None)
        if book.get("schema") != XFA_BOOK_SCHEMA or book_hash != stable_hash(book):
            raise AutonomousTierGXfaHandoffError(
                f"XFA book identity/hash drift: {candidate_id}"
            )
    all_transitions = [
        *list(row.get("transitions") or ()),
        *list(row.get("fail_closed_transitions") or ()),
    ]
    transition_ids: set[str] = set()
    for raw in all_transitions:
        transition = dict(raw)
        transition_hash = transition.pop("transition_hash", None)
        transition_id = str(transition.get("transition_id") or "")
        if (
            transition.get("schema") != TRANSITION_SCHEMA
            or not transition_id
            or transition_id in transition_ids
            or transition_hash != stable_hash(transition)
        ):
            raise AutonomousTierGXfaHandoffError(
                "transition identity/hash drift or duplication"
            )
        transition_ids.add(transition_id)
        tape_key = str(transition.get("source_tape_key") or "")
        if tape_key not in tapes or str(tapes[tape_key]["source_tape_hash"]) != str(
            transition.get("source_tape_hash")
        ):
            raise AutonomousTierGXfaHandoffError("transition source-tape drift")
        if transition.get("status") not in {READY_STATUS, FAIL_CLOSED_STATUS}:
            raise AutonomousTierGXfaHandoffError("transition status drift")
    if any(
        row.get("status") != READY_STATUS for row in value.get("transitions", ())
    ) or any(
        row.get("status") != FAIL_CLOSED_STATUS
        for row in value.get("fail_closed_transitions", ())
    ):
        raise AutonomousTierGXfaHandoffError("transition list/status separation drift")
    if int(counts.get("candidate_count", -1)) != len(handoffs) or int(
        counts.get("source_tape_count", -1)
    ) != len(tapes):
        raise AutonomousTierGXfaHandoffError("XFA handoff count drift")
    if int(counts.get("successful_combine_transition_count", -1)) != len(
        all_transitions
    ):
        raise AutonomousTierGXfaHandoffError("Combine transition count drift")
    if int(counts.get("ready_xfa_transition_count", -1)) != len(
        row.get("transitions") or ()
    ) or int(counts.get("fail_closed_transition_count", -1)) != len(
        row.get("fail_closed_transitions") or ()
    ):
        raise AutonomousTierGXfaHandoffError("ready/fail-closed count drift")
    return {**row, "result_hash": claimed}


def _verify_prepared_receipt_binding(
    prepared: Mapping[str, Any],
    receipt: Mapping[str, Any],
    official_rule_snapshot: Mapping[str, Any],
) -> None:
    candidate_id = str(prepared["candidate_id"])
    bindings = (
        (receipt.get("candidate_id"), candidate_id, "candidate"),
        (receipt.get("graduation_status"), "GRADUATED_DEVELOPMENT_BOOK", "status"),
        (receipt.get("account_label"), prepared.get("account_label"), "account"),
        (receipt.get("account_size_usd"), prepared.get("account_size_usd"), "size"),
        (
            receipt.get("selected_cell_hash"),
            prepared.get("selected_cell_hash"),
            "selected cell",
        ),
        (
            receipt.get("frozen_account_policy_hash"),
            prepared.get("frozen_account_policy_hash"),
            "Combine policy",
        ),
        (
            receipt.get("source_exact_result_hash"),
            prepared.get("source_exact_result_hash"),
            "exact source",
        ),
    )
    for left, right, label in bindings:
        if str(left) != str(right):
            raise AutonomousTierGXfaHandoffError(
                f"prepared/graduation {label} binding drift: {candidate_id}"
            )
    combine_book = dict(receipt.get("combine_book") or {})
    claimed_book_hash = combine_book.pop("combine_book_hash", None)
    if claimed_book_hash != stable_hash(combine_book):
        raise AutonomousTierGXfaHandoffError(
            f"Combine-book hash drift: {candidate_id}"
        )
    if str(claimed_book_hash) != str(receipt.get("combine_book_hash")):
        raise AutonomousTierGXfaHandoffError(
            f"Combine-book receipt drift: {candidate_id}"
        )
    parsed_hash = str(official_rule_snapshot.get("parsed_rule_hash") or "")
    if parsed_hash != str(prepared["official_rule_snapshot_hash"]):
        raise AutonomousTierGXfaHandoffError(
            f"official rule-snapshot binding drift: {candidate_id}"
        )


def _freeze_xfa_profile(
    prepared: Mapping[str, Any],
    receipt: Mapping[str, Any],
    rule_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    account_label = str(prepared["account_label"])
    xfa = dict(rule_snapshot.get("xfa") or {})
    plans = dict(xfa.get("scaling_plan_mini_contracts") or {})
    raw_plan = plans.get(account_label)
    starting_mll = dict(xfa.get("starting_mll_by_account_usd") or {}).get(
        account_label
    )
    if not raw_plan or starting_mll is None:
        raise AutonomousTierGXfaHandoffError(
            f"official XFA rule snapshot does not support {account_label}"
        )
    plan = [[float(level), float(limit)] for level, limit in raw_plan]
    if any(
        right[0] <= left[0] or right[1] < left[1]
        for left, right in zip(plan, plan[1:])
    ):
        raise AutonomousTierGXfaHandoffError("XFA scaling plan is not monotonic")
    source_policy = dict(prepared["frozen_account_policy"])
    core: dict[str, Any] = {
        "schema": XFA_PROFILE_SCHEMA,
        "candidate_id": str(prepared["candidate_id"]),
        "account_label": account_label,
        "account_size_usd": int(prepared["account_size_usd"]),
        "official_rule_snapshot_hash": str(
            prepared["official_rule_snapshot_hash"]
        ),
        "source_combine_book_hash": str(receipt["combine_book_hash"]),
        "source_combine_account_policy_hash": str(
            prepared["frozen_account_policy_hash"]
        ),
        "starting_balance_usd": float(xfa.get("starting_balance_usd", 0.0)),
        "starting_mll_usd": float(starting_mll),
        "scaling_plan_mini_contracts": plan,
        "maximum_mini_equivalent": float(plan[-1][1]),
        "maximum_concurrent_sleeves": 1,
        "component_priority": [str(prepared["candidate_id"])],
        "same_market_conflict_rule": "PRIORITY",
        "risk_multiplier": 1.0,
        "quantity_contract": (
            "PRESERVE_FROZEN_CAUSAL_QUANTITY_THEN_CAP_TO_CURRENT_XFA_SCALING_TIER"
        ),
        "underlying_signal_mutation": False,
        "standard_and_consistency_are_alternative_paths": True,
        "funded_path_alternatives": ["STANDARD", "CONSISTENCY"],
        "payout_split_trader_fraction": float(
            xfa.get("profit_split_trader_fraction", 0.9)
        ),
        "post_first_payout_mll_floor_usd": float(
            xfa.get("mll_floor_after_first_payout_usd", 0.0)
        ),
        "source_maximum_mini_equivalent": float(
            source_policy["maximum_mini_equivalent"]
        ),
        "frozen_before_xfa_outcomes": True,
        "outbound_order_capability": False,
    }
    profile_hash = stable_hash(core)
    return {
        **core,
        "xfa_profile_id": f"xfa-profile-{profile_hash[:24]}",
        "xfa_profile_hash": profile_hash,
    }


def _freeze_xfa_book(
    prepared: Mapping[str, Any],
    receipt: Mapping[str, Any],
    profile: Mapping[str, Any],
) -> dict[str, Any]:
    core = {
        "schema": XFA_BOOK_SCHEMA,
        "candidate_id": str(prepared["candidate_id"]),
        "account_label": str(prepared["account_label"]),
        "account_size_usd": int(prepared["account_size_usd"]),
        "component_ids": [str(prepared["candidate_id"])],
        "component_priority": [str(prepared["candidate_id"])],
        "source_combine_book_hash": str(receipt["combine_book_hash"]),
        "source_candidate_fingerprint": str(prepared["candidate_fingerprint"]),
        "source_event_receipt": dict(prepared["source_event_receipt"]),
        "xfa_profile_hash": str(profile["xfa_profile_hash"]),
        "underlying_signal_and_trade_path_immutable": True,
        "combine_profit_transferred": False,
        "fresh_xfa_starts_at_zero": True,
        "frozen_before_xfa_outcomes": True,
    }
    return {**core, "xfa_book_hash": stable_hash(core)}


def _source_tape(
    *,
    candidate_id: str,
    scenario: str,
    trajectories: Sequence[Any],
    eligible_session_days: Sequence[int],
    source_event_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    ordered = sorted(
        trajectories,
        key=lambda row: (row.event.decision_ns, row.event.event_id),
    )
    completed = [row.to_dict() for row in ordered if getattr(row, "completed", True)]
    censored = [row.to_dict() for row in ordered if not getattr(row, "completed", True)]
    core = {
        "schema": SOURCE_TAPE_SCHEMA,
        "candidate_id": candidate_id,
        "scenario": scenario,
        "eligible_session_days": sorted({int(day) for day in eligible_session_days}),
        "completed_trajectories": completed,
        "censored_trajectories": censored,
        "completed_trajectory_count": len(completed),
        "censored_trajectory_count": len(censored),
        "source_event_receipt": dict(source_event_receipt),
        "source_signal_recalculation_performed": False,
        "combine_outcomes_modified": False,
    }
    return {**core, "source_tape_hash": stable_hash(core)}


def _transition_handoff(
    *,
    candidate_id: str,
    scenario: str,
    block: str,
    episode: Any,
    source_tape_key: str,
    source_tape: Mapping[str, Any],
    combine_book: Mapping[str, Any],
    xfa_book: Mapping[str, Any],
    xfa_profile: Mapping[str, Any],
) -> dict[str, Any]:
    combine_path = episode.to_dict(include_paths=True)
    combine_path_hash = stable_hash(combine_path)
    start_payload = {
        "candidate_id": candidate_id,
        "scenario": scenario,
        "start_day": int(episode.start_day),
        "block": str(block),
        "combine_book_hash": str(combine_book["combine_book_hash"]),
    }
    combine_start_id = f"combine-start-{stable_hash(start_payload)[:24]}"
    pass_day = int(episode.end_day)
    full_days = [int(day) for day in source_tape["eligible_session_days"]]
    post_days = [day for day in full_days if day > pass_day]
    completed = [
        dict(row)
        for row in source_tape["completed_trajectories"]
        if int(dict(row["event"])["session_day"]) > pass_day
    ]
    censored = [
        dict(row)
        for row in source_tape["censored_trajectories"]
        if int(dict(row["event"])["session_day"]) > pass_day
    ]
    pass_actions = [
        dict(row)
        for row in episode.risk_allocation_path
        if int(row.get("session_day", -1)) == pass_day
    ]
    pass_time_ns = max(
        (
            max(int(row.get("decision_ns", 0)), int(row.get("exit_ns", 0)))
            for row in pass_actions
        ),
        default=None,
    )
    failure_reason = None if post_days else "NO_ELIGIBLE_SESSION_DAY_AFTER_COMBINE_PASS"
    status = READY_STATUS if post_days else FAIL_CLOSED_STATUS
    identity = {
        **start_payload,
        "combine_path_hash": combine_path_hash,
        "combine_pass_day": pass_day,
    }
    transition_id = f"xfa-transition-{stable_hash(identity)[:24]}"
    core: dict[str, Any] = {
        "schema": TRANSITION_SCHEMA,
        "status": status,
        "failure_reason": failure_reason,
        "transition_id": transition_id,
        "candidate_id": candidate_id,
        "scenario": scenario,
        "temporal_block": str(block),
        "combine_start_id": combine_start_id,
        "combine_start_day": int(episode.start_day),
        "combine_pass_day": pass_day,
        "combine_pass_time_ns": pass_time_ns,
        "combine_pass_time_basis": (
            "LATEST_ACCOUNT_ACTION_BOUNDARY_ON_PASS_DAY_BEFORE_EOD_GATE"
        ),
        "combine_path_hash": combine_path_hash,
        "combine_terminal": str(episode.terminal.value),
        "combine_days_to_target": int(episode.days_to_target),
        "combine_net_pnl_usd": float(episode.net_pnl),
        "combine_minimum_mll_buffer_usd": float(episode.minimum_mll_buffer),
        "account_label": str(xfa_profile["account_label"]),
        "account_size_usd": int(xfa_profile["account_size_usd"]),
        "combine_book_hash": str(combine_book["combine_book_hash"]),
        "xfa_book_hash": str(xfa_book["xfa_book_hash"]),
        "xfa_profile_hash": str(xfa_profile["xfa_profile_hash"]),
        "xfa_start_day": post_days[0] if post_days else None,
        "eligible_session_days": post_days,
        "source_tape_key": source_tape_key,
        "source_tape_hash": str(source_tape["source_tape_hash"]),
        "post_pass_completed_trajectory_count": len(completed),
        "post_pass_censored_trajectory_count": len(censored),
        "post_pass_trajectory_hash": stable_hash(completed),
        "post_pass_censor_hash": stable_hash(censored),
        "post_pass_first_event_ns": (
            int(dict(completed[0]["event"])["decision_ns"]) if completed else None
        ),
        "post_pass_last_event_ns": (
            int(dict(completed[-1]["event"])["exit_ns"]) if completed else None
        ),
        "combine_outcome_replayed_but_not_modified": True,
        "combine_profit_transferred": False,
        "fresh_xfa_balance_usd": 0.0,
        "independent_confirmation_claimed": False,
    }
    return {**core, "transition_hash": stable_hash(core)}


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AutonomousTierGXfaHandoffError(
            f"cannot read official rule snapshot: {path}"
        ) from exc
    if not isinstance(value, dict):
        raise AutonomousTierGXfaHandoffError("official rule snapshot is not an object")
    return value


__all__ = [
    "AutonomousTierGXfaHandoffError",
    "FAIL_CLOSED_STATUS",
    "READY_STATUS",
    "SCHEMA",
    "SOURCE_TAPE_SCHEMA",
    "STATUS",
    "TRANSITION_SCHEMA",
    "XFA_BOOK_SCHEMA",
    "XFA_PROFILE_SCHEMA",
    "build_tier_g_combine_xfa_handoffs",
    "build_tier_g_handoffs_from_prepared",
    "materialize_transition_trajectories",
    "verify_tier_g_combine_xfa_handoffs",
]
