from __future__ import annotations

import hashlib
import json
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.propfirm.combine_episode import TradePathEvent, run_combine_episode
from hydra.research import v71_cross_clock_flow_grammar as grammar4
from hydra.research import v71_event_mechanism_grammar as grammar1
from hydra.research import v71_event_time_grammar as grammar3
from hydra.research import v71_opportunity_density_grammar as grammar2
from hydra.research import v71_trade_size_composition as grammar6
from hydra.validation.v71_opportunity_density_tripwire import build_candidate_events
from hydra.validation.v7_d1_new_dataset_tripwire import _eligible_days_by_year
from hydra.validation.v7_report_schema import validate_v7_report_text


POLICY_PATH = "WORM/v7.2-pareto-crossfit-account-policy-0001-2026-07-13.json"
POLICY_SHA256 = "94f4ad89a2ae2ea347f1fce4a9cb4682690652429f34e42e72edf79e03da6677"
POWER_AUDIT_PATHS = (
    "reports/v7_1/power_aware_0001/v71_power_aware_candidate_audit_result.json",
    "reports/v7_1/discovery_0004/v71_cross_clock_flow_power_audit_result.json",
    "reports/v7_1/discovery_0006/v71_trade_size_composition_power_audit_result.json",
    "reports/v7_1/discovery_0008/v71_intraminute_flow_power_audit_result.json",
)
G5_FUNNEL_PATH = (
    "reports/v7_1/discovery_0005/"
    "v71_cross_clock_speed_leadership_funnel_result.json"
)
G9_FUNNEL_PATH = (
    "reports/v7_1/discovery_0009/"
    "v71_aggressor_run_topology_funnel_result.json"
)
COMPONENT_BANK_PATH = "WORM/v7.2-component-bank-0001-2026-07-13.json"


class V72ComponentBankError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ComponentEventPaths:
    candidate_id: str
    grammar_id: str
    specification_hash: str
    signal_path_hash: str
    sides: tuple[int, ...]
    base_events: tuple[TradePathEvent, ...]
    stress_events: tuple[TradePathEvent, ...]


def build_v72_component_bank(
    *,
    project_root: str | Path = ".",
    output_dir: str | Path = "reports/v7_2/component_bank",
    worm_output_path: str | Path = COMPONENT_BANK_PATH,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy, inventory, g9 = _load_and_reconcile_inventory(root)
    eligible_rows = [
        row
        for row in inventory
        if _eligible_by_frozen_policy(row, policy["component_bank"])
    ]
    if len(eligible_rows) != 16:
        raise V72ComponentBankError(
            f"eligible underpowered inventory drift: {len(eligible_rows)} != 16"
        )
    event_paths, eligible_days = load_v72_component_event_paths(
        root, eligible_rows
    )
    comparisons = build_behavioral_comparisons(
        event_paths,
        eligible_days=eligible_days,
        minimum_overlap_days=int(
            policy["component_bank"]["behavioral_cluster_thresholds"]
            ["minimum_overlap_days_for_correlation"]
        ),
    )
    clusters = build_behavioral_clusters(
        eligible_rows,
        comparisons,
        maximum_absolute_correlation=float(
            policy["component_bank"]["behavioral_cluster_thresholds"]
            ["maximum_absolute_daily_pnl_correlation"]
        ),
        maximum_signal_jaccard=float(
            policy["component_bank"]["behavioral_cluster_thresholds"]
            ["maximum_signal_timestamp_jaccard"]
        ),
    )
    selections = select_cluster_representatives(eligible_rows, clusters)
    primary_ids = tuple(row["primary_candidate_id"] for row in selections)
    if not (
        int(policy["component_bank"]["target_primary_count_minimum"])
        <= len(primary_ids)
        <= int(policy["component_bank"]["target_primary_count_maximum"])
    ):
        raise V72ComponentBankError(
            f"component bank primary count outside frozen bounds: {len(primary_ids)}"
        )
    role_map = policy["component_bank"]["role_map"]
    by_id = {str(row["candidate_id"]): row for row in inventory}
    primary_components = [
        _component_manifest_row(
            by_id[candidate_id],
            event_paths[candidate_id],
            cluster_id=next(
                row["cluster_id"]
                for row in selections
                if row["primary_candidate_id"] == candidate_id
            ),
            role=str(role_map[str(by_id[candidate_id]["family_id"])]),
            bank_status="COMPONENT_ELIGIBLE",
        )
        for candidate_id in primary_ids
    ]
    backup_components = [
        _component_manifest_row(
            by_id[str(selection["backup_candidate_id"])],
            event_paths[str(selection["backup_candidate_id"])],
            cluster_id=str(selection["cluster_id"]),
            role=str(
                role_map[
                    str(by_id[str(selection["backup_candidate_id"])]["family_id"])
                ]
            ),
            bank_status="PROMISING_UNDERPOWERED_COMPONENT",
        )
        for selection in selections
        if selection["backup_candidate_id"] is not None
    ]
    inventory_rows = [
        _inventory_status_row(row, primary_ids, selections) for row in inventory
    ]
    status_counts: defaultdict[str, int] = defaultdict(int)
    for row in inventory_rows:
        status_counts[str(row["v72_status"])] += 1

    manifest: dict[str, Any] = {
        "schema": "hydra_v7_2_frozen_component_bank_v1",
        "bank_id": "hydra_v7_2_component_bank_0001",
        "policy_path": POLICY_PATH,
        "policy_sha256": POLICY_SHA256,
        "created_before_any_v72_basket_result": True,
        "source_walk_forward_positive_count": len(inventory_rows),
        "source_power_audited_count": sum(
            row["source_decision"] == "CANDIDATE_SPECIFIC_POWER"
            for row in inventory_rows
        ),
        "source_geometry_tombstone_count": sum(
            row["v72_status"] == "TOMBSTONED" for row in inventory_rows
        ),
        "unaccounted_candidate_count": 0,
        "status_counts": dict(sorted(status_counts.items())),
        "behavioral_cluster_count": len(selections),
        "primary_component_count": len(primary_components),
        "backup_component_count": len(backup_components),
        "primary_components": primary_components,
        "backup_components": backup_components,
        "cluster_selections": selections,
        "behavioral_comparisons": comparisons,
        "inventory": inventory_rows,
        "g9_terminal": g9,
        "eligible_session_day_count": len(eligible_days),
        "data_role": "D1_DEVELOPMENT_ONLY",
        "underpowered_components_validated": False,
        "basket_results_observed": False,
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "Every primary component was selected after positive D1 walk-forward "
            "screening; behavioral diversity reduces clone inflation but does not "
            "convert development evidence into independent validation."
        ),
        "prochaine_action": "reserve_basket_multiplicity_and_run_leave_one_block_out_cross_fit",
    }
    manifest["component_bank_hash"] = _stable_hash(manifest)
    worm_path = Path(worm_output_path)
    if not worm_path.is_absolute():
        worm_path = root / worm_path
    _write_once_json(worm_path, manifest)
    result = {
        **manifest,
        "component_bank_path": str(worm_path.relative_to(root)),
        "component_bank_sha256": _sha256(worm_path),
    }
    return _write_report(result, root, Path(output_dir))


def load_v72_component_event_paths(
    root: str | Path,
    candidate_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, ComponentEventPaths], tuple[int, ...]]:
    project = Path(root).resolve()
    wanted: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in candidate_rows:
        wanted[str(row["grammar_id"])].append(row)
    costs = load_cost_model()
    output: dict[str, ComponentEventPaths] = {}
    minute_reference = None
    for grammar_id, rows in sorted(wanted.items()):
        if grammar_id == grammar1.GRAMMAR_ID:
            minute = grammar1.load_v71_minute_features(project)
            specs = {row.candidate_id: row for row in grammar1.candidate_specs(project)}
            signals = grammar1.generate_signal_population(
                minute, project_root=project, graveyard_path=None
            )
        elif grammar_id == grammar2.GRAMMAR_ID:
            minute = grammar2.load_v71_minute_features(project)
            specs = {row.candidate_id: row for row in grammar2.candidate_specs(project)}
            signals = grammar2.generate_signal_population(
                minute, project_root=project, graveyard_path=None
            )
        elif grammar_id == grammar3.GRAMMAR_ID:
            minute, event, _ = grammar3.load_event_time_sources(project)
            specs = {row.candidate_id: row for row in grammar3.candidate_specs(project)}
            signals = grammar3.generate_signal_population(
                minute, event, project_root=project, graveyard_path=None
            )
        elif grammar_id == grammar4.GRAMMAR_ID:
            minute, pairs, _ = grammar4.load_cross_clock_sources(project)
            specs = {row.candidate_id: row for row in grammar4.candidate_specs(project)}
            signals = grammar4.generate_signal_population(
                minute, pairs, project_root=project, graveyard_path=None
            )
        elif grammar_id == grammar6.GRAMMAR_ID:
            minute, states, _ = grammar6.load_trade_size_composition_sources(project)
            specs = {row.candidate_id: row for row in grammar6.candidate_specs(project)}
            signals = grammar6.generate_signal_population(
                states, project_root=project, graveyard_path=None
            )
        else:
            raise V72ComponentBankError(f"unsupported component grammar: {grammar_id}")
        if minute_reference is None:
            minute_reference = minute
        elif not np.array_equal(
            minute_reference["minute_start_ns"].to_numpy(np.int64),
            minute["minute_start_ns"].to_numpy(np.int64),
        ):
            raise V72ComponentBankError("component execution minute sources differ")
        selected_specs: dict[str, Any] = {}
        selected_signals: dict[str, Sequence[Any]] = {}
        for frozen in rows:
            candidate_id = str(frozen["candidate_id"])
            spec = specs[candidate_id]
            if spec.specification_hash != str(frozen["specification_hash"]):
                raise V72ComponentBankError(f"{candidate_id} specification drift")
            actual_signal_hash = grammar1.signal_path_hash(signals[candidate_id])
            if actual_signal_hash != str(frozen["signal_path_hash"]):
                raise V72ComponentBankError(f"{candidate_id} signal path drift")
            selected_specs[candidate_id] = spec
            selected_signals[candidate_id] = signals[candidate_id]
        base = build_candidate_events(
            minute,
            selected_signals,
            selected_specs,
            costs,
            stress=CostStress.BASE,
        )
        stressed = build_candidate_events(
            minute,
            selected_signals,
            selected_specs,
            costs,
            stress=CostStress.STRESS_1_5X,
        )
        for frozen in rows:
            candidate_id = str(frozen["candidate_id"])
            candidate_signals = tuple(selected_signals[candidate_id])
            output[candidate_id] = ComponentEventPaths(
                candidate_id=candidate_id,
                grammar_id=grammar_id,
                specification_hash=str(frozen["specification_hash"]),
                signal_path_hash=str(frozen["signal_path_hash"]),
                sides=tuple(int(row.side) for row in candidate_signals),
                base_events=tuple(base[candidate_id]),
                stress_events=tuple(stressed[candidate_id]),
            )
            if not (
                len(output[candidate_id].sides)
                == len(output[candidate_id].base_events)
                == len(output[candidate_id].stress_events)
            ):
                raise V72ComponentBankError(f"{candidate_id} event/side alignment drift")
    if set(output) != {str(row["candidate_id"]) for row in candidate_rows}:
        raise V72ComponentBankError("component event population drift")
    if minute_reference is None:
        raise V72ComponentBankError("component event source is empty")
    eligible_by_year = _eligible_days_by_year(minute_reference)
    eligible_days = tuple(
        sorted(
            int(day)
            for year in sorted(eligible_by_year)
            for day in eligible_by_year[year]
        )
    )
    return output, eligible_days


def build_behavioral_comparisons(
    paths: Mapping[str, ComponentEventPaths],
    *,
    eligible_days: Sequence[int],
    minimum_overlap_days: int = 5,
) -> list[dict[str, Any]]:
    identifiers = sorted(paths)
    account_paths = {
        candidate_id: _account_path(paths[candidate_id].stress_events, eligible_days)
        for candidate_id in identifiers
    }
    comparisons: list[dict[str, Any]] = []
    for left_position, left_id in enumerate(identifiers):
        for right_id in identifiers[left_position + 1 :]:
            left = paths[left_id].stress_events
            right = paths[right_id].stress_events
            left_daily = _daily_pnl(left)
            right_daily = _daily_pnl(right)
            days = sorted(set(left_daily) | set(right_daily))
            left_values = np.asarray([left_daily.get(day, 0.0) for day in days])
            right_values = np.asarray([right_daily.get(day, 0.0) for day in days])
            correlation_defined = bool(
                len(days) >= minimum_overlap_days
                and np.std(left_values) > 0.0
                and np.std(right_values) > 0.0
            )
            correlation = (
                float(np.corrcoef(left_values, right_values)[0, 1])
                if correlation_defined
                else 0.0
            )
            left_signals = {int(row.decision_ns) for row in left}
            right_signals = {int(row.decision_ns) for row in right}
            left_losses = {day for day, value in left_daily.items() if value < 0.0}
            right_losses = {day for day, value in right_daily.items() if value < 0.0}
            left_tail = _tail_days(left_daily)
            right_tail = _tail_days(right_daily)
            left_account = account_paths[left_id]
            right_account = account_paths[right_id]
            account_days = sorted(set(left_account) | set(right_account))
            progress_corr, mll_corr = _account_path_correlations(
                left_account, right_account, account_days, minimum_overlap_days
            )
            comparisons.append(
                {
                    "left_candidate_id": left_id,
                    "right_candidate_id": right_id,
                    "daily_union_day_count": len(days),
                    "daily_pnl_correlation_defined": correlation_defined,
                    "daily_pnl_correlation": correlation,
                    "signal_timestamp_jaccard": _jaccard(left_signals, right_signals),
                    "shared_loss_day_jaccard": _jaccard(left_losses, right_losses),
                    "tail_event_day_jaccard": _jaccard(left_tail, right_tail),
                    "target_progress_path_correlation": progress_corr,
                    "mll_buffer_path_correlation": mll_corr,
                }
            )
    return comparisons


def build_behavioral_clusters(
    candidate_rows: Sequence[Mapping[str, Any]],
    comparisons: Sequence[Mapping[str, Any]],
    *,
    maximum_absolute_correlation: float,
    maximum_signal_jaccard: float,
) -> dict[str, str]:
    identifiers = sorted(str(row["candidate_id"]) for row in candidate_rows)
    families = {str(row["candidate_id"]): str(row["family_id"]) for row in candidate_rows}
    parent = {candidate_id: candidate_id for candidate_id in identifiers}

    def find(value: str) -> str:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str) -> None:
        a, b = find(left), find(right)
        if a != b:
            first, second = sorted((a, b))
            parent[second] = first

    for left_index, left in enumerate(identifiers):
        for right in identifiers[left_index + 1 :]:
            if families[left] == families[right]:
                union(left, right)
    for row in comparisons:
        left = str(row["left_candidate_id"])
        right = str(row["right_candidate_id"])
        too_correlated = bool(row["daily_pnl_correlation_defined"]) and abs(
            float(row["daily_pnl_correlation"])
        ) > maximum_absolute_correlation
        too_overlapping = (
            float(row["signal_timestamp_jaccard"]) > maximum_signal_jaccard
        )
        if too_correlated or too_overlapping:
            union(left, right)
    groups: defaultdict[str, list[str]] = defaultdict(list)
    for candidate_id in identifiers:
        groups[find(candidate_id)].append(candidate_id)
    mapping: dict[str, str] = {}
    for position, members in enumerate(sorted(groups.values()), start=1):
        cluster_id = f"V72_BEHAVIOR_{position:03d}"
        for candidate_id in sorted(members):
            mapping[candidate_id] = cluster_id
    return mapping


def select_cluster_representatives(
    candidate_rows: Sequence[Mapping[str, Any]],
    clusters: Mapping[str, str],
) -> list[dict[str, Any]]:
    rows_by_id = {str(row["candidate_id"]): row for row in candidate_rows}
    members: defaultdict[str, list[str]] = defaultdict(list)
    for candidate_id, cluster_id in clusters.items():
        members[str(cluster_id)].append(str(candidate_id))
    selections: list[dict[str, Any]] = []
    for cluster_id, candidate_ids in sorted(members.items()):
        ranked = sorted(candidate_ids, key=lambda value: _primary_rank(rows_by_id[value]))
        selections.append(
            {
                "cluster_id": cluster_id,
                "member_candidate_ids": ranked,
                "primary_candidate_id": ranked[0],
                "backup_candidate_id": ranked[1] if len(ranked) > 1 else None,
                "excluded_additional_member_ids": ranked[2:],
                "selection_rule": (
                    "positive_calendar_year_fraction_desc,stress_2x_mean_net_desc,"
                    "effective_independent_event_count_desc,candidate_id_asc"
                ),
            }
        )
    return selections


def _load_and_reconcile_inventory(
    root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    if _sha256(root / POLICY_PATH) != POLICY_SHA256:
        raise V72ComponentBankError("V7.2 WORM policy drift")
    policy = json.loads((root / POLICY_PATH).read_text(encoding="utf-8"))
    expected = {row["path"]: row["sha256"] for row in policy["frozen_sources"]}
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V72ComponentBankError("frozen V7.2 source drift: " + ",".join(drift))
    power_rows: list[dict[str, Any]] = []
    for path in POWER_AUDIT_PATHS:
        payload = json.loads((root / path).read_text(encoding="utf-8"))
        power_rows.extend(dict(row) for row in payload["candidate_results"])
    if len(power_rows) != 22:
        raise V72ComponentBankError("power-audited inventory must contain 22 candidates")
    for row in power_rows:
        row["source_decision"] = "CANDIDATE_SPECIFIC_POWER"
    g5 = json.loads((root / G5_FUNNEL_PATH).read_text(encoding="utf-8"))
    geometry = [
        dict(row) for row in g5["candidate_results"] if bool(row["walk_forward_positive"])
    ]
    if len(geometry) != 2:
        raise V72ComponentBankError("G5 geometry terminal inventory drift")
    for row in geometry:
        row.update(
            {
                "grammar_id": "hydra_v7_1_cross_clock_speed_leadership_grammar_0005",
                "status": "GEOMETRY_ONLY_CLASS_TOMBSTONED_NO_POWER_AUDIT",
                "source_decision": "CLASS_LEVEL_TRIPWIRE_TERMINAL",
            }
        )
    inventory = sorted(power_rows + geometry, key=lambda row: str(row["candidate_id"]))
    if len(inventory) != 24 or len({str(row["candidate_id"]) for row in inventory}) != 24:
        raise V72ComponentBankError("24-candidate inventory is not one-to-one")
    g9_payload = json.loads((root / G9_FUNNEL_PATH).read_text(encoding="utf-8"))
    g9_rows = g9_payload["candidate_results"]
    if len(g9_rows) != 4 or any(
        row["classification"] != "FORMULATION_FALSIFIED" for row in g9_rows
    ):
        raise V72ComponentBankError("G9 terminal classification drift")
    g9 = {
        "classification": "G9_FORMULATION_FALSIFIED",
        "candidate_count": 4,
        "candidate_ids": sorted(str(row["candidate_id"]) for row in g9_rows),
        "walk_forward_positive_count": 0,
        "basket_component_eligible_count": 0,
    }
    return policy, inventory, g9


def _eligible_by_frozen_policy(
    row: Mapping[str, Any], component_policy: Mapping[str, Any]
) -> bool:
    return bool(
        row.get("status") == component_policy["eligible_power_status"]
        and float(row["cost_results"]["STRESS_1_5X"]["mean_net"]) > 0.0
        and float(row["cost_results"]["STRESS_2X"]["mean_net"]) > 0.0
        and float(row["best_event_removed_net"]) > 0.0
        and float(row["top_event_concentration"])
        <= float(component_policy["maximum_top_event_concentration"])
    )


def _component_manifest_row(
    row: Mapping[str, Any],
    paths: ComponentEventPaths,
    *,
    cluster_id: str,
    role: str,
    bank_status: str,
) -> dict[str, Any]:
    return {
        "candidate_id": str(row["candidate_id"]),
        "grammar_id": str(row["grammar_id"]),
        "family_id": str(row["family_id"]),
        "mechanism": str(row["motif"]),
        "direction_policy": str(row["direction_policy"]),
        "market": "ES",
        "execution_contract_policy": "explicit_ES_contract_from_frozen_D1_signal",
        "timeframe": f"event_state_to_{int(row['holding_minutes'])}m_exit",
        "session": "RTH",
        "holding_minutes": int(row["holding_minutes"]),
        "role": role,
        "behavioral_cluster": cluster_id,
        "bank_status": bank_status,
        "prior_power_status": str(row["status"]),
        "statistically_validated": False,
        "specification_hash": str(row["specification_hash"]),
        "signal_path_hash": str(row["signal_path_hash"]),
        "base_event_path_hash": _event_path_hash(paths.base_events, paths.sides),
        "stress_1_5x_event_path_hash": _event_path_hash(paths.stress_events, paths.sides),
        "full_D1_event_count": len(paths.base_events),
        "walk_forward_event_count": int(row["raw_event_count"]),
        "effective_independent_event_count": float(
            row["effective_sample"]["effective_independent_event_count"]
        ),
        "walk_forward_STRESS_1_5X_mean_net": float(
            row["cost_results"]["STRESS_1_5X"]["mean_net"]
        ),
        "walk_forward_STRESS_2X_mean_net": float(
            row["cost_results"]["STRESS_2X"]["mean_net"]
        ),
        "best_event_removed_net": float(row["best_event_removed_net"]),
        "top_event_concentration": float(row["top_event_concentration"]),
        "positive_calendar_year_fraction": float(
            row["stability"]["calendar_year"]["positive_fraction"]
        ),
        "deterministic_implementation": True,
        "hard_invalidation": False,
        "status_inherited": False,
    }


def _inventory_status_row(
    row: Mapping[str, Any],
    primary_ids: Sequence[str],
    selections: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    candidate_id = str(row["candidate_id"])
    backup_ids = {
        str(value["backup_candidate_id"])
        for value in selections
        if value["backup_candidate_id"] is not None
    }
    if candidate_id in primary_ids:
        status = "COMPONENT_ELIGIBLE"
    elif candidate_id in backup_ids or row.get("status") == "PROMISING_UNDERPOWERED":
        status = "PROMISING_UNDERPOWERED_COMPONENT"
    elif row.get("status") == "WF_POSITIVE_BUT_FRAGILE":
        status = "FRAGILE_RESEARCH_ONLY"
    elif row.get("status") == "GEOMETRY_ONLY_CLASS_TOMBSTONED_NO_POWER_AUDIT":
        status = "TOMBSTONED"
    else:
        status = "PENDING_EVIDENCE"
    cluster_id = next(
        (
            str(value["cluster_id"])
            for value in selections
            if candidate_id in value["member_candidate_ids"]
        ),
        None,
    )
    return {
        "candidate_id": candidate_id,
        "grammar_id": str(row.get("grammar_id") or "UNKNOWN"),
        "family_id": str(row.get("family_id") or "UNKNOWN"),
        "prior_status": str(row.get("status") or "UNCLASSIFIED"),
        "source_decision": str(row["source_decision"]),
        "behavioral_cluster": cluster_id,
        "v72_status": status,
        "terminal_or_pending_status_present": True,
    }


def _primary_rank(row: Mapping[str, Any]) -> tuple[float, float, float, str]:
    return (
        -float(row["stability"]["calendar_year"]["positive_fraction"]),
        -float(row["cost_results"]["STRESS_2X"]["mean_net"]),
        -float(row["effective_sample"]["effective_independent_event_count"]),
        str(row["candidate_id"]),
    )


def _daily_pnl(events: Sequence[TradePathEvent]) -> dict[int, float]:
    output: defaultdict[int, float] = defaultdict(float)
    for event in events:
        output[int(event.session_day)] += float(event.net_pnl)
    return dict(output)


def _tail_days(daily: Mapping[int, float]) -> set[int]:
    if not daily:
        return set()
    threshold = float(np.percentile(np.asarray(list(daily.values()), dtype=float), 10))
    return {day for day, value in daily.items() if value <= threshold}


def _account_path(
    events: Sequence[TradePathEvent], eligible_days: Sequence[int]
) -> dict[int, tuple[float, float]]:
    result = run_combine_episode(
        events,
        eligible_days,
        start_day=int(eligible_days[0]),
        maximum_duration_days=len(eligible_days),
    )
    return {
        int(row["session_day"]): (
            (float(row["balance"]) - 150_000.0) / 9_000.0,
            float(row["balance"]) - float(row["mll_floor"]),
        )
        for row in result.daily_path
    }


def _account_path_correlations(
    left: Mapping[int, tuple[float, float]],
    right: Mapping[int, tuple[float, float]],
    days: Sequence[int],
    minimum: int,
) -> tuple[float | None, float | None]:
    common = [day for day in days if day in left and day in right]
    if len(common) < minimum:
        return None, None
    left_progress = np.asarray([left[day][0] for day in common])
    right_progress = np.asarray([right[day][0] for day in common])
    left_buffer = np.asarray([left[day][1] for day in common])
    right_buffer = np.asarray([right[day][1] for day in common])
    return (
        _correlation_or_none(left_progress, right_progress),
        _correlation_or_none(left_buffer, right_buffer),
    )


def _correlation_or_none(left: np.ndarray, right: np.ndarray) -> float | None:
    if np.std(left) <= 0.0 or np.std(right) <= 0.0:
        return None
    value = float(np.corrcoef(left, right)[0, 1])
    return value if math.isfinite(value) else None


def _jaccard(left: set[Any], right: set[Any]) -> float:
    union = left | right
    return float(len(left & right) / len(union)) if union else 0.0


def _event_path_hash(events: Sequence[TradePathEvent], sides: Sequence[int]) -> str:
    return _stable_hash(
        [
            {"side": int(side), **event.to_dict()}
            for side, event in zip(sides, events, strict=True)
        ]
    )


def _write_report(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v72_component_bank_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    displayed = result_path.relative_to(root)
    report_path = destination / "v72_component_bank_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.2 — Frozen component bank",
            "",
            "[HYDRA-V7] phase=4 step=183 verdict=GREEN",
            f"gate=V72_COMPONENT_BANK_FREEZE preuve={displayed}#{result_hash[:8]} tests=24_candidate_reconciliation_plus_behavioral_clustering",
            "budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263902 burned=1",
            "diff_validation=hydra/validation/v72_component_bank.py CONTRE=biais_de_selection_D1_et_blocs_courts",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Positifs WF réconciliés: `{result['source_walk_forward_positive_count']}`",
            f"- Non comptabilisés: `{result['unaccounted_candidate_count']}`",
            f"- Clusters économiques/comportementaux: `{result['behavioral_cluster_count']}`",
            f"- Composants primaires gelés: `{result['primary_component_count']}`",
            f"- Backups gelés: `{result['backup_component_count']}`",
            f"- Statuts: `{json.dumps(result['status_counts'], sort_keys=True)}`",
            f"- G9: `{result['g9_terminal']['classification']}`",
            "- Achats data: `0`",
            "- Accès Q4 additionnel: `0`",
            "- Ordres broker: `0`",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )
    validate_v7_report_text(report)
    report_path.write_text(report, encoding="utf-8")
    return {
        **result,
        "result_path": str(result_path),
        "result_sha256": result_hash,
        "report_path": str(report_path),
    }


def _write_once_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != serialized:
            raise V72ComponentBankError(f"WORM component bank already exists with drift: {path}")
        return
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(serialized, encoding="utf-8")
    os.replace(temporary, path)


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "ComponentEventPaths",
    "V72ComponentBankError",
    "build_behavioral_clusters",
    "build_behavioral_comparisons",
    "build_v72_component_bank",
    "load_v72_component_event_paths",
    "select_cluster_representatives",
]
