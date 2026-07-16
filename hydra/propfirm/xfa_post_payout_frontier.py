"""Deterministic six-book XFA-only post-payout frontier runner.

Only the sealed ``lifecycle_rows`` arrays are read from Stage 3/4/5.  The much
larger ``evidence_raw`` arrays are scanned past but never decoded or retained.
Combine paths are transition provenance; all counterfactual work starts from
the already frozen XFA start day and the immutable source-event tape.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import math
import os
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from hydra.account_policy.active_risk_pool import policy_from_mapping
from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.combine_to_xfa import FrozenRiskProfile, XfaTerminal
from hydra.propfirm.xfa_post_payout import (
    DllScenario,
    FrontierRole,
    FrozenXfaTransition,
    XfaPostPayoutPolicy,
    XfaPostPayoutResult,
    preregistered_post_payout_frontier,
    run_xfa_only_from_transition,
)
from hydra.propfirm.xfa_source_tape import (
    XfaSourceTape,
    build_xfa_source_tape,
    write_xfa_source_tape,
)


FRONTIER_RESULT_SCHEMA = "hydra_xfa_post_payout_frontier_result_v1"
FRONTIER_EVENT_TAPE_SCHEMA = "hydra_xfa_post_payout_event_tape_v1"
MAXIMUM_BOOKS = 6
PROFILES_PER_BOOK = 24
BASELINE_FLOAT_TOLERANCE = 1e-8
SELECTION_RULE = {
    "version": "hydra_xfa_post_payout_pareto_selection_v1",
    "outcome_scope": "BOOK_LEVEL_AGGREGATE_NO_PER_TRANSITION_ORACLE",
    "selected_xfa_path_frozen_from_pre_frontier_decision_report": True,
    "optional_dll_is_sensitivity_only": True,
    "selection_eligible_dll": DllScenario.NO_DLL.value,
    "pareto_dimensions_stressed_1_5x": [
        "MAX_UNCONDITIONAL_POST_PAYOUT_SURVIVAL_RATE",
        "MIN_MLL_BREACH_RATE",
        "MAX_EXPECTED_TRADER_NET_PAYOUT_PER_TRANSITION",
        "MAX_FIRST_PAYOUT_RATE",
        "MAX_PAYOUT_CYCLES_PER_TRANSITION",
        "MAX_P10_MINIMUM_MLL_BUFFER",
    ],
    "transparent_tiebreak_order": [
        "UNCONDITIONAL_POST_PAYOUT_SURVIVAL_RATE_DESC",
        "MLL_BREACH_RATE_ASC",
        "EXPECTED_TRADER_NET_PAYOUT_PER_TRANSITION_DESC",
        "FIRST_PAYOUT_RATE_DESC",
        "PAYOUT_CYCLES_PER_TRANSITION_DESC",
        "P10_MINIMUM_MLL_BUFFER_DESC",
        "POST_PAYOUT_RISK_SCALE_ASC",
        "ROLE_BALANCED_THEN_LONGEVITY_THEN_HARVEST",
        "POLICY_ID_ASC",
    ],
}


class XfaPostPayoutFrontierError(RuntimeError):
    """The bounded frontier cannot reconcile its frozen inputs."""


def run_six_book_frontier(
    *,
    project_root: str | Path,
    selection_path: str | Path,
    decision_report_path: str | Path,
    halving_root: str | Path,
    stage_cache_root: str | Path,
    evidence_bundle_path: str | Path,
    feature_cache_root: str | Path,
    runtime_summaries_path: str | Path,
    source_tape_output_dir: str | Path,
    payout_event_tape_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    selection_file = _resolve(root, selection_path)
    decision_file = _resolve(root, decision_report_path)
    selection = _json(selection_file)
    _verify_selection(selection)
    selected = list(selection.get("selected_books") or ())
    if len(selected) != MAXIMUM_BOOKS:
        raise XfaPostPayoutFrontierError("frontier requires exactly six frozen books")
    policy_ids = tuple(str(row["policy_id"]) for row in selected)
    if len(set(policy_ids)) != MAXIMUM_BOOKS:
        raise XfaPostPayoutFrontierError("frozen book IDs are not unique")

    tape = build_xfa_source_tape(
        evidence_bundle_path=_resolve(root, evidence_bundle_path),
        feature_cache_root=_resolve(root, feature_cache_root),
        runtime_summaries_path=_resolve(root, runtime_summaries_path),
        campaign_id=str(selection["campaign_id"]),
    )
    tape_manifest = write_xfa_source_tape(
        tape, _resolve(root, source_tape_output_dir)
    )
    source_tape_sha = str(tape_manifest["event_file"]["sha256"])

    paths = _selected_xfa_paths(_json(decision_file), policy_ids)
    if set(paths.values()) != {"CONSISTENCY"}:
        raise XfaPostPayoutFrontierError(
            "the six pre-frontier path decisions must remain XFA_CONSISTENCY"
        )
    source_rows, partition_receipts = load_sealed_lifecycle_rows(
        selected_policy_ids=policy_ids,
        halving_root=_resolve(root, halving_root),
        stage_cache_root=_resolve(root, stage_cache_root),
    )
    transition_count = sum(len(rows) for rows in source_rows.values())
    if transition_count != 2_028:
        raise XfaPostPayoutFrontierError(
            f"transition count drift: {transition_count} != 2028"
        )

    event_target = _resolve(root, payout_event_tape_path)
    event_target.parent.mkdir(parents=True, exist_ok=True)
    event_tmp = event_target.with_name(f".{event_target.name}.{os.getpid()}.tmp")
    fingerprints: set[str] = set()
    event_count = 0
    evaluation_count = 0
    book_artifacts: list[dict[str, Any]] = []
    try:
        with event_tmp.open("wb") as raw:
            with gzip.GzipFile(
                filename="xfa_post_payout_events.jsonl",
                mode="wb",
                fileobj=raw,
                mtime=0,
            ) as compressed:
                with io.TextIOWrapper(
                    compressed, encoding="utf-8", newline="\n"
                ) as event_stream:
                    for frozen_book in selected:
                        policy_id = str(frozen_book["policy_id"])
                        rows = source_rows[policy_id]
                        print(
                            f"XFA_FRONTIER_BOOK_START {policy_id} "
                            f"transitions={len(rows)} profiles={PROFILES_PER_BOOK}",
                            flush=True,
                        )
                        artifact, evaluated, written = _run_book_frontier(
                            tape=tape,
                            frozen_book=frozen_book,
                            lifecycle_rows=rows,
                            selected_path=paths[policy_id],
                            event_stream=event_stream,
                            event_fingerprints=fingerprints,
                        )
                        book_artifacts.append(artifact)
                        evaluation_count += evaluated
                        event_count += written
                        print(
                            f"XFA_FRONTIER_BOOK_COMPLETE {policy_id} "
                            f"evaluations={evaluated} payout_events={written} "
                            f"baseline={artifact['baseline_reconciliation']['status']}",
                            flush=True,
                        )
            raw.flush()
            os.fsync(raw.fileno())

        expected_evaluations = transition_count * PROFILES_PER_BOOK
        if evaluation_count != expected_evaluations or evaluation_count != 48_672:
            raise XfaPostPayoutFrontierError(
                f"frontier evaluation count drift: {evaluation_count}"
            )
        event_sha = _sha256(event_tmp)
        if event_target.exists():
            if _sha256(event_target) != event_sha:
                raise XfaPostPayoutFrontierError(
                    "immutable post-payout event tape drift"
                )
            event_tmp.unlink()
        else:
            os.replace(event_tmp, event_target)

        payload = {
            "schema": FRONTIER_RESULT_SCHEMA,
            "campaign_id": str(selection["campaign_id"]),
            "development_only": True,
            "independent_confirmation": False,
            "paper_shadow_ready": False,
            "book_count": MAXIMUM_BOOKS,
            "profiles_per_book": PROFILES_PER_BOOK,
            "transition_count": transition_count,
            "alternative_reference_xfa_path_count": transition_count * 2,
            "frontier_evaluation_count": evaluation_count,
            "source_event_tape_sha256": source_tape_sha,
            "source_event_tape": tape_manifest,
            "canonical_payout_event_tape": {
                "schema": FRONTIER_EVENT_TAPE_SCHEMA,
                "path": str(event_target.relative_to(root)),
                "sha256": _sha256(event_target),
                "event_count": event_count,
                "unique_event_fingerprint_count": len(fingerprints),
                "gzip_mtime": 0,
            },
            "frozen_selection": {
                "path": str(selection_file.relative_to(root)),
                "file_sha256": _sha256(selection_file),
                "selection_manifest_hash": selection["selection_manifest_hash"],
                "source_freeze_timestamp_utc": selection.get(
                    "selection_completed_at_utc"
                ),
            },
            "source_partitions": partition_receipts,
            "selection_rule": SELECTION_RULE,
            "books": book_artifacts,
            "invariants": {
                "market_signals_replayed": False,
                "combine_paths_replayed": False,
                "evidence_raw_decoded": False,
                "daily_ledgers_persisted_in_final_json": False,
                "book_limit_respected": len(book_artifacts) == 6,
                "profiles_per_book_exact": all(
                    len(row["profiles"]) == 24 for row in book_artifacts
                ),
                "baseline_reconciliation_exact": all(
                    row["baseline_reconciliation"]["mismatch_count"] == 0
                    for row in book_artifacts
                ),
                "standard_consistency_alternatives_not_added": True,
                "no_broker": True,
                "no_orders": True,
                "no_q4_access": True,
                "no_data_purchase": True,
            },
        }
        payload["result_hash"] = stable_hash(payload)
        _atomic_write_json(_resolve(root, output_path), payload)
        return payload
    finally:
        event_tmp.unlink(missing_ok=True)


def load_sealed_lifecycle_rows(
    *,
    selected_policy_ids: Sequence[str],
    halving_root: str | Path,
    stage_cache_root: str | Path,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    halving = Path(halving_root)
    cache = Path(stage_cache_root)
    output: dict[str, list[dict[str, Any]]] = {
        policy_id: [] for policy_id in selected_policy_ids
    }
    receipts: list[dict[str, Any]] = []
    seen: set[tuple[str, str, int]] = set()
    for stage, source_stage in ((3, 2), (4, 3), (5, 4)):
        selection = _json(halving / f"stage{source_stage}.json")
        ordered = [str(value) for value in selection["selected_policy_ids"]]
        for policy_id in selected_policy_ids:
            try:
                index = ordered.index(policy_id)
            except ValueError as exc:
                raise XfaPostPayoutFrontierError(
                    f"{policy_id} absent from Stage {source_stage} selection"
                ) from exc
            path = cache / f"stage{stage}_active_batches" / f"batch_{index:06d}.json"
            rows = extract_lifecycle_rows(path)
            counts = Counter(str(row["scenario"]) for row in rows)
            for row in rows:
                if str(row.get("policy_id")) != policy_id:
                    raise XfaPostPayoutFrontierError("stage partition policy drift")
                key = (
                    policy_id,
                    str(row["scenario"]),
                    int(row["combine_start_day"]),
                )
                if key in seen:
                    raise XfaPostPayoutFrontierError("duplicate lifecycle transition")
                seen.add(key)
                output[policy_id].append(row)
            receipts.append(
                {
                    "policy_id": policy_id,
                    "stage": stage,
                    "path": str(path),
                    "lifecycle_row_count": len(rows),
                    "scenario_counts": dict(sorted(counts.items())),
                }
            )
    for rows in output.values():
        rows.sort(key=lambda row: (str(row["scenario"]), int(row["combine_start_day"])))
    flat_count = sum(len(rows) for rows in output.values())
    if flat_count != len(seen):
        raise XfaPostPayoutFrontierError("transition union drift")
    return output, receipts


def extract_lifecycle_rows(path: str | Path) -> list[dict[str, Any]]:
    """Decode only the named lifecycle array from a huge pretty-printed batch."""

    marker = '"lifecycle_rows": ['
    chunks: list[str] = []
    found = False
    depth = 0
    in_string = False
    escaped = False
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            if not found:
                position = line.find(marker)
                if position < 0:
                    continue
                found = True
                line = line[line.index("[", position) :]
            chunks.append(line)
            for character in line:
                if in_string:
                    if escaped:
                        escaped = False
                    elif character == "\\":
                        escaped = True
                    elif character == '"':
                        in_string = False
                elif character == '"':
                    in_string = True
                elif character == "[":
                    depth += 1
                elif character == "]":
                    depth -= 1
            if found and depth == 0:
                break
    if not found or depth != 0:
        raise XfaPostPayoutFrontierError(f"lifecycle_rows unavailable: {path}")
    decoded, _end = json.JSONDecoder().raw_decode("".join(chunks))
    if not isinstance(decoded, list):
        raise XfaPostPayoutFrontierError("lifecycle_rows must be an array")
    compact: list[dict[str, Any]] = []
    for raw in decoded:
        if not isinstance(raw, Mapping):
            raise XfaPostPayoutFrontierError("invalid lifecycle row")
        claimed = str(raw.get("source_lifecycle_sha256") or "")
        semantic = dict(raw)
        semantic.pop("source_lifecycle_sha256", None)
        if not claimed or stable_hash(semantic) != claimed:
            raise XfaPostPayoutFrontierError("lifecycle source hash drift")
        compact.append(_compact_lifecycle_row(raw))
    return compact


def _compact_lifecycle_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    scalar = {
        key: raw[key]
        for key in (
            "policy_id",
            "scenario",
            "combine_start_day",
            "combine_end_day",
            "combine_status",
            "combine_horizon",
            "xfa_start_day",
            "xfa_horizon_days",
            "source_lifecycle_sha256",
        )
    }
    scalar["xfa_profile"] = dict(raw["xfa_profile"])
    scalar["rule_snapshot_fingerprint"] = str(raw["rule_snapshot"]["fingerprint"])
    scalar["standard"] = _compact_baseline(raw["standard"])
    scalar["consistency"] = _compact_baseline(raw["consistency"])
    return scalar


def _compact_baseline(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value.get(key)
        for key in (
            "path",
            "path_hash",
            "start_day",
            "end_day",
            "terminal",
            "observed_days",
            "traded_days",
            "event_count",
            "accepted_event_count",
            "skipped_event_count",
            "payout_cycles",
            "gross_payout",
            "trader_net_payout",
            "first_payout_day",
            "ending_balance",
            "ending_mll_floor",
            "minimum_mll_buffer",
            "post_payout_survived",
            "post_payout_censored",
            "post_payout_observed_days",
        )
    }


def _run_book_frontier(
    *,
    tape: XfaSourceTape,
    frozen_book: Mapping[str, Any],
    lifecycle_rows: Sequence[Mapping[str, Any]],
    selected_path: str,
    event_stream: io.TextIOBase,
    event_fingerprints: set[str],
) -> tuple[dict[str, Any], int, int]:
    policy_id = str(frozen_book["policy_id"])
    specification = frozen_book["frozen_policy_specification"]
    account_policy = policy_from_mapping(specification["active_risk_policy"])
    path_key = selected_path.lower()
    xfa_book = specification[f"xfa_{path_key}_book"]
    profile_value = dict(xfa_book["xfa_profile"])
    profile_fingerprint = str(profile_value.pop("fingerprint"))
    profile = FrozenRiskProfile(**profile_value)
    if profile.fingerprint != profile_fingerprint:
        raise XfaPostPayoutFrontierError("frozen XFA profile hash drift")
    path_name = f"XFA_{selected_path}"
    policies = preregistered_post_payout_frontier(policy_id, path=path_name)
    if len(policies) != PROFILES_PER_BOOK:
        raise XfaPostPayoutFrontierError("preregistered profile count drift")
    baseline_policy = next(
        row
        for row in policies
        if row.role is FrontierRole.HARVEST
        and row.post_payout_risk_scale == 1.0
        and row.dll_scenario is DllScenario.NO_DLL
    )
    accumulators = {row.policy_id: _new_accumulator() for row in policies}
    baseline_mismatches: list[dict[str, Any]] = []
    baseline_matches = 0
    event_count = 0
    evaluation_count = 0
    result_hashes: dict[str, list[str]] = defaultdict(list)
    expected_profile = profile.to_dict()

    for source in lifecycle_rows:
        _validate_source_transition(source, policy_id, expected_profile)
        transition = _transition_from_source(source)
        baseline = source[path_key]
        for frontier_policy in policies:
            result = run_xfa_only_from_transition(
                tape,
                basket=account_policy,
                frozen_xfa_profile=profile,
                transition=transition,
                policy=frontier_policy,
                horizon_days=int(source["xfa_horizon_days"]),
            )
            evaluation_count += 1
            _accumulate(accumulators[frontier_policy.policy_id], result)
            result_hashes[frontier_policy.policy_id].append(result.result_hash)
            for event in result.payout_events:
                fingerprint = str(event["event_fingerprint"])
                if fingerprint in event_fingerprints:
                    raise XfaPostPayoutFrontierError("duplicate canonical payout event")
                event_fingerprints.add(fingerprint)
                event_stream.write(
                    json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n"
                )
                event_count += 1
            if frontier_policy.policy_id == baseline_policy.policy_id:
                differences = _baseline_differences(result, baseline)
                if differences:
                    baseline_mismatches.append(
                        {
                            "transition_id": transition.transition_id,
                            "differences": differences,
                        }
                    )
                else:
                    baseline_matches += 1

    profiles = []
    for policy in policies:
        summary = _summarize_accumulator(accumulators[policy.policy_id])
        profiles.append(
            {
                "policy": policy.to_dict(),
                "selection_eligible": policy.dll_scenario is DllScenario.NO_DLL,
                "summary": summary,
                "result_hashes_sha256": stable_hash(
                    result_hashes[policy.policy_id]
                ),
            }
        )
    _assign_pareto_layers(profiles)
    selected_profile = _select_profile(profiles)
    counts = Counter(str(row["scenario"]) for row in lifecycle_rows)
    artifact = {
        "policy_id": policy_id,
        "selection_role": str(frozen_book["selection_role"]),
        "economic_behavior_cluster": str(
            frozen_book["expanded_economic_behavior_cluster"]
        ),
        "selected_xfa_path": selected_path,
        "frozen_book_fingerprint": str(
            frozen_book["frozen_policy_specification_sha256"]
        ),
        "frozen_xfa_profile_fingerprint": profile.fingerprint,
        "transition_count": len(lifecycle_rows),
        "transition_counts_by_scenario": dict(sorted(counts.items())),
        "baseline_policy_id": baseline_policy.policy_id,
        "baseline_reconciliation": {
            "status": (
                "EXACT" if not baseline_mismatches else "MISMATCH"
            ),
            "matched_transition_count": baseline_matches,
            "mismatch_count": len(baseline_mismatches),
            "mismatches": baseline_mismatches,
            "float_tolerance": BASELINE_FLOAT_TOLERANCE,
        },
        "profiles": profiles,
        "selected_profile": selected_profile,
    }
    if baseline_mismatches:
        raise XfaPostPayoutFrontierError(
            f"official XFA baseline failed for {policy_id}"
        )
    return artifact, evaluation_count, event_count


def _validate_source_transition(
    value: Mapping[str, Any], policy_id: str, expected_profile: Mapping[str, Any]
) -> None:
    if value.get("combine_status") != "TARGET_REACHED":
        raise XfaPostPayoutFrontierError("non-passing Combine entered XFA")
    if int(value["xfa_start_day"]) <= int(value["combine_end_day"]):
        raise XfaPostPayoutFrontierError("XFA start did not follow Combine end")
    if int(value["standard"]["start_day"]) != int(value["xfa_start_day"]):
        raise XfaPostPayoutFrontierError("Standard XFA start drift")
    if int(value["consistency"]["start_day"]) != int(value["xfa_start_day"]):
        raise XfaPostPayoutFrontierError("Consistency XFA start drift")
    actual = dict(value["xfa_profile"])
    if actual != dict(expected_profile):
        raise XfaPostPayoutFrontierError(f"XFA profile drift for {policy_id}")


def _transition_from_source(value: Mapping[str, Any]) -> FrozenXfaTransition:
    semantic = {
        key: value[key]
        for key in (
            "policy_id",
            "scenario",
            "combine_start_day",
            "combine_end_day",
            "combine_status",
            "combine_horizon",
        )
    }
    return FrozenXfaTransition(
        book_id=str(value["policy_id"]),
        scenario=str(value["scenario"]),
        combine_start_id=(
            f"{value['policy_id']}:{value['scenario']}:{value['combine_start_day']}"
        ),
        combine_start_day=int(value["combine_start_day"]),
        xfa_start_day=int(value["xfa_start_day"]),
        combine_path_hash=stable_hash(semantic),
    )


def _baseline_differences(
    result: XfaPostPayoutResult, expected: Mapping[str, Any]
) -> dict[str, Any]:
    actual = {
        "path": result.path,
        "start_day": result.start_day,
        "end_day": result.end_day,
        "terminal": result.terminal.value,
        "observed_days": result.observed_days,
        "traded_days": result.traded_days,
        "event_count": result.event_count,
        "accepted_event_count": result.accepted_event_count,
        "skipped_event_count": result.skipped_event_count,
        "payout_cycles": result.payout_cycles,
        "gross_payout": result.gross_payout,
        "trader_net_payout": result.trader_net_payout,
        "first_payout_day": result.first_payout_day,
        "ending_balance": result.ending_balance,
        "ending_mll_floor": result.ending_mll_floor,
        "minimum_mll_buffer": result.minimum_mll_buffer,
        "post_payout_survived": result.post_payout_survived,
        "post_payout_censored": result.post_payout_censored,
        "post_payout_observed_days": result.post_payout_observed_days,
    }
    differences = {}
    for key, actual_value in actual.items():
        expected_value = expected.get(key)
        if isinstance(actual_value, float) or isinstance(expected_value, float):
            equal = math.isclose(
                float(actual_value),
                float(expected_value),
                rel_tol=1e-12,
                abs_tol=BASELINE_FLOAT_TOLERANCE,
            )
        else:
            equal = actual_value == expected_value
        if not equal:
            differences[key] = {"expected": expected_value, "actual": actual_value}
    return differences


def _new_accumulator() -> dict[str, Any]:
    return defaultdict(
        lambda: {
            "count": 0,
            "first_payout_count": 0,
            "payout_cycles": 0,
            "payout_cycle_values": [],
            "at_least_two_payout_count": 0,
            "gross_payout": 0.0,
            "trader_net_payout": 0.0,
            "mll_breach_count": 0,
            "post_payout_survival_count": 0,
            "closure_before_first_payout_count": 0,
            "dll_trigger_count": 0,
            "first_payout_days": [],
            "minimum_mll_buffers": [],
            "terminals": Counter(),
            "survival_horizons": {
                30: {"evaluable": 0, "survived": 0},
                60: {"evaluable": 0, "survived": 0},
                90: {"evaluable": 0, "survived": 0},
            },
        }
    )


def _scenario_key(value: str) -> str:
    return "stressed_1_5x" if value != "NORMAL" else "normal"


def _accumulate(accumulator: Mapping[str, Any], result: XfaPostPayoutResult) -> None:
    row = accumulator[_scenario_key(result.scenario)]
    row["count"] += 1
    row["payout_cycles"] += result.payout_cycles
    row["payout_cycle_values"].append(result.payout_cycles)
    if result.payout_cycles >= 2:
        row["at_least_two_payout_count"] += 1
    row["gross_payout"] += result.gross_payout
    row["trader_net_payout"] += result.trader_net_payout
    row["dll_trigger_count"] += result.dll_trigger_count
    row["minimum_mll_buffers"].append(result.minimum_mll_buffer)
    row["terminals"][result.terminal.value] += 1
    if result.terminal is XfaTerminal.MLL_BREACHED:
        row["mll_breach_count"] += 1
    if result.first_payout_day is not None:
        row["first_payout_count"] += 1
        row["first_payout_days"].append(result.first_payout_day)
    elif result.terminal in {
        XfaTerminal.MLL_BREACHED,
        XfaTerminal.HARD_RULE_FAILURE,
        XfaTerminal.INACTIVITY_RISK,
    }:
        row["closure_before_first_payout_count"] += 1
    if result.post_payout_survived:
        row["post_payout_survival_count"] += 1
    if result.first_payout_day is not None:
        post_first_days = result.observed_days - result.first_payout_day
        failed = result.terminal in {
            XfaTerminal.MLL_BREACHED,
            XfaTerminal.HARD_RULE_FAILURE,
            XfaTerminal.INACTIVITY_RISK,
        }
        for horizon, values in row["survival_horizons"].items():
            if post_first_days >= horizon or failed:
                values["evaluable"] += 1
                if post_first_days >= horizon:
                    values["survived"] += 1


def _summarize_accumulator(accumulator: Mapping[str, Any]) -> dict[str, Any]:
    return {
        scenario: _summarize_scenario(accumulator[scenario])
        for scenario in ("normal", "stressed_1_5x")
    }


def _summarize_scenario(row: Mapping[str, Any]) -> dict[str, Any]:
    count = int(row["count"])
    first = int(row["first_payout_count"])
    buffers = sorted(float(value) for value in row["minimum_mll_buffers"])
    p10 = buffers[max(0, math.ceil(len(buffers) * 0.10) - 1)] if buffers else None
    return {
        "transition_count": count,
        "first_payout_count": first,
        "first_payout_rate": first / count if count else 0.0,
        "median_days_to_first_payout": (
            statistics.median(row["first_payout_days"])
            if row["first_payout_days"]
            else None
        ),
        "payout_cycles": int(row["payout_cycles"]),
        "median_payout_cycles_before_closure_or_censoring": (
            statistics.median(row["payout_cycle_values"])
            if row["payout_cycle_values"]
            else 0.0
        ),
        "payout_cycles_per_transition": (
            row["payout_cycles"] / count if count else 0.0
        ),
        "at_least_two_payout_count": int(row["at_least_two_payout_count"]),
        "probability_at_least_two_payouts": (
            row["at_least_two_payout_count"] / count if count else 0.0
        ),
        "gross_payout": float(row["gross_payout"]),
        "trader_net_payout": float(row["trader_net_payout"]),
        "expected_trader_net_payout_per_transition": (
            row["trader_net_payout"] / count if count else 0.0
        ),
        "mll_breach_count": int(row["mll_breach_count"]),
        "mll_breach_rate": row["mll_breach_count"] / count if count else 0.0,
        "post_payout_survival_count": int(row["post_payout_survival_count"]),
        "unconditional_post_payout_survival_rate": (
            row["post_payout_survival_count"] / count if count else 0.0
        ),
        "post_payout_survival_rate_conditional_on_first_payout": (
            row["post_payout_survival_count"] / first if first else 0.0
        ),
        "closure_before_first_payout_count": int(
            row["closure_before_first_payout_count"]
        ),
        "dll_trigger_count": int(row["dll_trigger_count"]),
        "minimum_mll_buffer_min": min(buffers) if buffers else None,
        "minimum_mll_buffer_p10": p10,
        "terminal_distribution": dict(sorted(row["terminals"].items())),
        "post_payout_survival_horizons": {
            str(horizon): {
                "evaluable_count": int(values["evaluable"]),
                "survival_count": int(values["survived"]),
                "survival_rate": (
                    values["survived"] / values["evaluable"]
                    if values["evaluable"]
                    else None
                ),
            }
            for horizon, values in sorted(row["survival_horizons"].items())
        },
    }


def _pareto_vector(profile: Mapping[str, Any]) -> tuple[float, ...]:
    stressed = profile["summary"]["stressed_1_5x"]
    return (
        float(stressed["unconditional_post_payout_survival_rate"]),
        -float(stressed["mll_breach_rate"]),
        float(stressed["expected_trader_net_payout_per_transition"]),
        float(stressed["first_payout_rate"]),
        float(stressed["payout_cycles_per_transition"]),
        float(stressed["minimum_mll_buffer_p10"] or -math.inf),
    )


def _dominates(left: Sequence[float], right: Sequence[float]) -> bool:
    return all(a >= b for a, b in zip(left, right)) and any(
        a > b for a, b in zip(left, right)
    )


def _assign_pareto_layers(profiles: list[dict[str, Any]]) -> None:
    remaining = [row for row in profiles if row["selection_eligible"]]
    layer = 1
    while remaining:
        frontier = [
            row
            for row in remaining
            if not any(
                _dominates(_pareto_vector(other), _pareto_vector(row))
                for other in remaining
                if other is not row
            )
        ]
        if not frontier:
            raise XfaPostPayoutFrontierError("empty Pareto layer")
        for row in frontier:
            row["pareto_layer"] = layer
        frontier_ids = {id(row) for row in frontier}
        remaining = [row for row in remaining if id(row) not in frontier_ids]
        layer += 1
    for row in profiles:
        if not row["selection_eligible"]:
            row["pareto_layer"] = None


def _select_profile(profiles: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    frontier = [
        row
        for row in profiles
        if row["selection_eligible"] and row["pareto_layer"] == 1
    ]
    role_order = {
        FrontierRole.BALANCED.value: 0,
        FrontierRole.LONGEVITY.value: 1,
        FrontierRole.HARVEST.value: 2,
    }

    def key(row: Mapping[str, Any]) -> tuple[Any, ...]:
        stressed = row["summary"]["stressed_1_5x"]
        policy = row["policy"]
        return (
            -float(stressed["unconditional_post_payout_survival_rate"]),
            float(stressed["mll_breach_rate"]),
            -float(stressed["expected_trader_net_payout_per_transition"]),
            -float(stressed["first_payout_rate"]),
            -float(stressed["payout_cycles_per_transition"]),
            -float(stressed["minimum_mll_buffer_p10"] or -math.inf),
            float(policy["post_payout_risk_scale"]),
            role_order[str(policy["role"])],
            str(policy["policy_id"]),
        )

    selected = min(frontier, key=key)
    return {
        **selected["policy"],
        "profile_family": f"XFA_{selected['policy']['role']}_PROFILE",
        "selected_xfa_path": "XFA_" + str(selected["policy"]["path"]).removeprefix("XFA_"),
        "pareto_layer": 1,
        "selection_rule_version": SELECTION_RULE["version"],
        "summary": selected["summary"],
    }


def _selected_xfa_paths(
    report: Mapping[str, Any], policy_ids: Sequence[str]
) -> dict[str, str]:
    rows = {
        str(row["policy_id"]): row
        for row in report["expanded_development_finalists"]["rows"]
    }
    output = {}
    for policy_id in policy_ids:
        xfa = rows[policy_id]["expanded_standard_consistency_xfa_lifecycle_exact"]
        standard = float(
            xfa["stressed"]["standard"]["unconditional_lower_bound"]
            ["expected_trader_payout_per_combine_attempt"]
        )
        consistency = float(
            xfa["stressed"]["consistency"]["unconditional_lower_bound"]
            ["expected_trader_payout_per_combine_attempt"]
        )
        output[policy_id] = "CONSISTENCY" if consistency > standard else "STANDARD"
    return output


def _verify_selection(value: Mapping[str, Any]) -> None:
    semantic = dict(value)
    claimed = str(semantic.pop("selection_manifest_hash", ""))
    if not claimed or stable_hash(semantic) != claimed:
        raise XfaPostPayoutFrontierError("frozen selection hash drift")
    if value.get("schema") != "hydra_frozen_book_selection_v1":
        raise XfaPostPayoutFrontierError("unsupported frozen selection")
    if int(value.get("primary_count", 0)) != 5 or int(
        value.get("backup_count", 0)
    ) != 1:
        raise XfaPostPayoutFrontierError("frozen 5+1 selection drift")


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    content = json.dumps(payload, sort_keys=True, indent=2) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != content:
            raise XfaPostPayoutFrontierError(f"immutable result drift: {path}")
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _resolve(root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise XfaPostPayoutFrontierError(f"JSON object expected: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "FRONTIER_EVENT_TAPE_SCHEMA",
    "FRONTIER_RESULT_SCHEMA",
    "SELECTION_RULE",
    "XfaPostPayoutFrontierError",
    "extract_lifecycle_rows",
    "load_sealed_lifecycle_rows",
    "run_six_book_frontier",
]
