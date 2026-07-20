"""Build a read-only operational research bank from HYDRA evidence.

The bank is a derived lifecycle view.  It never writes the registry, mission
database, controller state, or candidate status.  In particular it keeps four
facts separate:

* an observed development Combine pass;
* exact-cell quarantine;
* development evidence tier;
* diagnostic XFA alternatives.

STANDARD and CONSISTENCY XFA paths are alternatives and are never added.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from hydra.economic_evolution.schema import stable_hash


SCHEMA = "hydra_operational_candidate_bank_v2"
SUMMARY_SCHEMA = "hydra_operational_candidate_bank_summary_v2"
DEFAULT_OUTPUT_DIR = Path(
    "reports/economic_evolution/operational_candidate_bank_v2"
)
PASS_BANK_PATH = Path(
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/"
    "branch_results/post_source_exhaustion/post_composite/"
    "combine_pass_observed_bank.json"
)
CANDIDATE_BANK_PATH = PASS_BANK_PATH.with_name("combine_candidate_bank.json")
RECONCILIATION_PATH = Path(
    "reports/economic_evolution/evidence_axis_reconciliation_v1/economic_result.json"
)
DYNAMIC_PATH = Path(
    "reports/economic_evolution/pnl_state_risk_frontier_v1/"
    "economic_result_reconciled.json"
)
XFA_HANDOFF_PATH = Path(
    "reports/economic_evolution/pnl_state_xfa_diagnostic_v1/"
    "xfa_all_clean_handoffs.json"
)
XFA_SUMMARY_PATH = Path(
    "reports/economic_evolution/pnl_state_xfa_diagnostic_v1/"
    "xfa_decision_summary.json"
)
XFA_FRONTIER_PATH = Path(
    "reports/economic_evolution/xfa_post_payout_survival_frontier_v2/"
    "economic_result.json"
)
CONFIRMATION_PATH = Path(
    "reports/economic_evolution/fresh_confirmation_replication_2021_h1_v1/"
    "decision_report.json"
)
STAGE1_BATCH_GLOB = (
    "data/cache/economic_production/hydra_fast_pass_factory_0029/"
    "wave_*/stage1_batches/*.jsonl"
)


class OperationalBankError(RuntimeError):
    """Source evidence cannot be reconciled without inflating a status."""


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OperationalBankError(f"cannot read JSON: {path}") from exc
    if not isinstance(value, dict):
        raise OperationalBankError(f"JSON object required: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _with_hash(value: dict[str, Any]) -> dict[str, Any]:
    core = dict(value)
    core.pop("result_hash", None)
    core["result_hash"] = stable_hash(core)
    return core


def verify_hashed(value: Mapping[str, Any]) -> None:
    core = dict(value)
    claimed = core.pop("result_hash", None)
    if not isinstance(claimed, str) or claimed != stable_hash(core):
        raise OperationalBankError("derived artifact result_hash drift")


def _unwrap(value: Mapping[str, Any], key: str) -> dict[str, Any]:
    nested = value.get(key)
    if not isinstance(nested, Mapping):
        raise OperationalBankError(f"missing source artifact: {key}")
    return dict(nested)


def _compact_horizons(policy: Mapping[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for raw_horizon, raw_metrics in sorted(
        dict(policy.get("horizons") or {}).items(), key=lambda item: int(item[0])
    ):
        metrics = dict(raw_metrics or {})
        horizon_row: dict[str, Any] = {
            "evaluation_status": metrics.get("evaluation_status"),
        }
        for role in ("overall", "held_out_development"):
            role_metrics = metrics.get(role)
            if not isinstance(role_metrics, Mapping):
                continue
            role_row: dict[str, Any] = {}
            for scenario in ("normal", "stressed"):
                raw = role_metrics.get(scenario)
                if not isinstance(raw, Mapping):
                    continue
                row = dict(raw)
                role_row[scenario] = {
                    "full_coverage_start_count": int(
                        row.get("full_coverage_start_count") or 0
                    ),
                    "data_censored_count": int(row.get("data_censored_count") or 0),
                    "pass_count": int(row.get("pass_count") or 0),
                    "pass_rate": float(row.get("pass_rate") or 0.0),
                    "net_total_usd": float(row.get("net_total_usd") or 0.0),
                    "target_progress_median": row.get("target_progress_median"),
                    "target_progress_p25": row.get("target_progress_p25"),
                    "median_days_to_target": row.get("median_days_to_target"),
                    "mll_breach_count": int(row.get("mll_breach_count") or 0),
                    "mll_breach_rate": float(row.get("mll_breach_rate") or 0.0),
                    "minimum_mll_buffer_usd": row.get("minimum_mll_buffer_usd"),
                    "consistency_compliance_rate": row.get(
                        "consistency_rate",
                        row.get("consistency_compliance_rate"),
                    ),
                    "blocks_with_passes": sorted(row.get("blocks_with_passes") or []),
                    "episode_path_hash": row.get("episode_path_hash"),
                }
            horizon_row[role] = role_row
        output[str(raw_horizon)] = horizon_row
    return output


def _load_candidate_specs(
    root: Path, candidate_ids: Iterable[str]
) -> dict[str, dict[str, Any]]:
    pending = set(candidate_ids)
    found: dict[str, dict[str, Any]] = {}
    for path in sorted(root.glob(STAGE1_BATCH_GLOB)):
        if not pending:
            break
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip() or not any(value in line for value in pending):
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                candidate_id = str(row.get("candidate_id") or "")
                if candidate_id not in pending:
                    continue
                spec = row.get("candidate")
                if isinstance(spec, Mapping):
                    found[candidate_id] = dict(spec)
                    pending.remove(candidate_id)
    return found


def _policy_niches(
    components: Iterable[str], specs: Mapping[str, Mapping[str, Any]]
) -> dict[str, list[Any]]:
    rows = [dict(specs[value]) for value in components if value in specs]
    return {
        "markets": sorted({str(row["market"]) for row in rows if row.get("market")}),
        "execution_markets": sorted(
            {str(row["execution_market"]) for row in rows if row.get("execution_market")}
        ),
        "sessions": sorted(
            {int(row["session_code"]) for row in rows if row.get("session_code") is not None}
        ),
        "timeframes": sorted(
            {str(row["timeframe"]) for row in rows if row.get("timeframe")}
        ),
        "mechanisms": sorted(
            {str(row["mechanism"]) for row in rows if row.get("mechanism")}
        ),
        "holding_horizons": sorted(
            {str(row["horizon"]) for row in rows if row.get("horizon") is not None}
        ),
    }


def _dynamic_horizons(result: Mapping[str, Any]) -> dict[str, Any]:
    selected = dict(result.get("selected") or {})
    summaries = dict(selected.get("summaries") or {})
    output: dict[str, Any] = {}
    for raw_horizon in ("5", "10", "20"):
        normal = dict(dict(summaries.get("NORMAL") or {}).get(raw_horizon) or {})
        stressed = dict(
            dict(summaries.get("STRESSED_1_5X") or {}).get(raw_horizon) or {}
        )
        if not normal and not stressed:
            continue
        output[raw_horizon] = {
            "evaluation_status": "EXACT_DYNAMIC_ACCOUNT_REPLAY_DEVELOPMENT_DIAGNOSTIC",
            "overall": {
                scenario: {
                    "full_coverage_start_count": int(row.get("full_coverage_start_count") or 0),
                    "data_censored_count": int(row.get("data_censored_count") or 0),
                    "pass_count": int(row.get("pass_count") or 0),
                    "pass_rate": float(row.get("pass_rate") or 0.0),
                    "net_total_usd": float(row.get("net_total_usd") or 0.0),
                    "target_progress_median": row.get("target_progress_median"),
                    "target_progress_p25": row.get("target_progress_p25"),
                    "median_days_to_target": row.get("median_days_to_target"),
                    "mll_breach_count": int(row.get("mll_breach_count") or 0),
                    "mll_breach_rate": float(row.get("mll_breach_rate") or 0.0),
                    "minimum_mll_buffer_usd": row.get("minimum_mll_buffer_usd"),
                    "consistency_compliance_rate": row.get("consistency_compliance_rate"),
                    "episode_path_hash": row.get("episode_path_hash"),
                }
                for scenario, row in (("normal", normal), ("stressed", stressed))
            },
        }
    return output


def _dynamic_behavior_hash(result: Mapping[str, Any]) -> str:
    hashes: list[str] = []
    selected = dict(result.get("selected") or {})
    for scenario in dict(selected.get("summaries") or {}).values():
        if not isinstance(scenario, Mapping):
            continue
        for metrics in scenario.values():
            if isinstance(metrics, Mapping) and metrics.get("episode_path_hash"):
                hashes.append(str(metrics["episode_path_hash"]))
    return stable_hash({"episode_path_hashes": sorted(hashes)})


def _xfa_by_policy(xfa: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    transitions_by_policy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in xfa.get("transitions", ()):
        row = dict(raw)
        transitions_by_policy[str(row["policy_id"])].append(row)

    records: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for raw in xfa.get("path_records", ()):
        row = dict(raw)
        records[(str(row["policy_id"]), str(row["path"]))].append(row)

    aggregates: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for raw in xfa.get("aggregates", ()):
        row = dict(raw)
        aggregates[(str(row["policy_id"]), str(row["path"]))].append(row)

    output: dict[str, dict[str, Any]] = {}
    for policy_id, transitions in transitions_by_policy.items():
        profile_ids = sorted({str(row["selected_profile_id"]) for row in transitions})
        if len(profile_ids) != 1:
            raise OperationalBankError(f"mixed XFA profiles for {policy_id}")
        alternatives: dict[str, Any] = {}
        for path in ("STANDARD", "CONSISTENCY"):
            path_records = records[(policy_id, path)]
            terminals = Counter(str(row["terminal"]) for row in path_records)
            path_aggregates = sorted(
                aggregates[(policy_id, path)], key=lambda row: int(row["horizon_trading_days"])
            )
            alternatives[path] = {
                "path_count": len(path_records),
                "first_payout_count": sum(
                    int(row.get("first_payout_count") or 0) for row in path_records
                ),
                "first_payout_rate_per_successful_combine": (
                    sum(int(row.get("first_payout_count") or 0) for row in path_records)
                    / len(path_records)
                    if path_records
                    else 0.0
                ),
                "payout_cycles_total": sum(
                    int(row.get("payout_cycles") or 0) for row in path_records
                ),
                "trader_net_payout_total_usd": sum(
                    float(row.get("trader_net_payout_usd") or 0.0)
                    for row in path_records
                ),
                "minimum_mll_buffer_usd": min(
                    (float(row["minimum_mll_buffer_usd"]) for row in path_records),
                    default=None,
                ),
                "post_payout_survived_120d_count": sum(
                    bool(row.get("post_payout_survived")) for row in path_records
                ),
                "terminal_distribution": dict(sorted(terminals.items())),
                "by_horizon": path_aggregates,
            }
        output[policy_id] = {
            "status": "XFA_DEVELOPMENT_DIAGNOSTIC_ONLY",
            "selected_profile_id": profile_ids[0],
            "combine_transition_count": len(transitions),
            "standard_and_consistency_are_alternatives": True,
            "sum_standard_and_consistency_ev_allowed": False,
            "alternatives": alternatives,
        }
    return output


def _confirmation_by_candidate(
    confirmation: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for raw in confirmation.get("audited_clean_tier_g", ()):
        row = dict(raw)
        candidate_id = str(row["candidate_id"])
        item: dict[str, Any] = {
            "status": row["terminal_confirmation_status"],
            "tier_c_gate_passed": False,
            "first_confirmation_2025_h1": row["first_confirmation_2025_h1"],
        }
        if candidate_id == confirmation.get("second_replication", {}).get("candidate_id"):
            cells = dict(dict(confirmation["second_replication"]).get("cells") or {})
            item["second_replication_2021_h1"] = {
                horizon: {
                    scenario: {
                        "full_coverage_start_count": int(cell.get("full_coverage_start_count") or 0),
                        "pass_count": int(dict(cell.get(scenario) or {}).get("pass_count") or 0),
                        "net_total_usd": float(
                            dict(cell.get(scenario) or {}).get("net_total_usd") or 0.0
                        ),
                        "mll_breach_rate": float(
                            dict(cell.get(scenario) or {}).get("mll_breach_rate") or 0.0
                        ),
                    }
                    for scenario in ("normal", "stressed")
                }
                for horizon, cell in sorted(cells.items(), key=lambda item: int(item[0]))
            }
        output[candidate_id] = item
    return output


def _xfa_frontier_by_policy(
    frontier: Mapping[str, Any],
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    aggregate_by_hash = {
        str(row["aggregate_hash"]): dict(row) for row in frontier.get("aggregates", ())
    }
    selected: list[dict[str, Any]] = []
    by_policy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in frontier.get("pareto_selected_profiles", ()):
        selection = dict(raw)
        aggregate = aggregate_by_hash.get(str(selection["aggregate_hash"]))
        if aggregate is None:
            raise OperationalBankError("selected XFA frontier aggregate missing")
        row = dict(aggregate)
        row["selection_is_development_diagnostic_only"] = True
        row["standard_and_consistency_are_alternatives"] = True
        row["sum_standard_and_consistency_ev_allowed"] = False
        selected.append(row)
        by_policy[str(row["policy_id"])].append(row)
    selected.sort(
        key=lambda row: (
            str(row["policy_id"]),
            str(row["path"]),
            int(row["horizon_trading_days"]),
        )
    )
    for values in by_policy.values():
        values.sort(key=lambda row: (str(row["path"]), int(row["horizon_trading_days"])))
    return dict(by_policy), selected


def _flatten_csv_row(row: Mapping[str, Any]) -> dict[str, Any]:
    horizons = dict(row.get("horizons") or {})
    normal_passes = sum(
        int(dict(dict(value).get("overall") or {}).get("normal", {}).get("pass_count") or 0)
        for value in horizons.values()
    )
    stressed_passes = sum(
        int(dict(dict(value).get("overall") or {}).get("stressed", {}).get("pass_count") or 0)
        for value in horizons.values()
    )
    xfa = dict(row.get("xfa") or {})
    alternatives = dict(xfa.get("alternatives") or {})
    standard = dict(alternatives.get("STANDARD") or {})
    consistency = dict(alternatives.get("CONSISTENCY") or {})
    niches = dict(row.get("niches") or {})
    return {
        "configuration_id": row["configuration_id"],
        "base_policy_id": row["base_policy_id"],
        "record_type": row["record_type"],
        "operational_status": row["operational_status"],
        "evidence_tier": row.get("evidence_tier"),
        "classification_status": row["classification_status"],
        "behavior_cluster_id": row["behavior_cluster_id"],
        "markets": "|".join(niches.get("markets") or []),
        "sessions": "|".join(str(value) for value in niches.get("sessions") or []),
        "timeframes": "|".join(niches.get("timeframes") or []),
        "mechanisms": "|".join(niches.get("mechanisms") or []),
        "normal_passes_all_horizons": normal_passes,
        "stressed_passes_all_horizons": stressed_passes,
        "confirmation_status": dict(row.get("confirmation") or {}).get("status"),
        "xfa_profile_id": xfa.get("selected_profile_id"),
        "xfa_standard_paths": standard.get("path_count", 0),
        "xfa_standard_first_payouts": standard.get("first_payout_count", 0),
        "xfa_consistency_paths": consistency.get("path_count", 0),
        "xfa_consistency_first_payouts": consistency.get("first_payout_count", 0),
        "next_evidence_action": row["next_evidence_action"],
    }


def build_bank(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    pass_wrapper = _read(root / PASS_BANK_PATH)
    pass_bank = _unwrap(pass_wrapper, "combine_pass_observed_bank")
    candidate_bank = _unwrap(_read(root / CANDIDATE_BANK_PATH), "candidate_bank")
    reconciliation = _read(root / RECONCILIATION_PATH)
    dynamic = _read(root / DYNAMIC_PATH)
    xfa = _read(root / XFA_HANDOFF_PATH)
    xfa_summary = _read(root / XFA_SUMMARY_PATH)
    xfa_frontier = _read(root / XFA_FRONTIER_PATH)
    confirmation = _read(root / CONFIRMATION_PATH)

    source_policies = [dict(row) for row in pass_bank.get("policies", ())]
    if len(source_policies) != 50:
        raise OperationalBankError("expected the 50-policy observed-pass source bank")
    behavior_hashes = [
        str(dict(row.get("fingerprints") or {}).get("episode_behavior_hash") or "")
        for row in source_policies
    ]
    if not all(behavior_hashes) or len(set(behavior_hashes)) != len(behavior_hashes):
        raise OperationalBankError("source bank behavior hashes are not unique")

    clean_ids = {
        str(row["policy_id"])
        for row in reconciliation.get("combine_pass_observed_development", ())
    }
    quarantine_by_id = {
        str(row["policy_id"]): dict(row)
        for row in reconciliation.get("quarantined_observed_policies", ())
    }
    if len(clean_ids) != 44 or len(quarantine_by_id) != 6:
        raise OperationalBankError("44 clean plus 6 quarantined reconciliation required")
    if clean_ids | set(quarantine_by_id) != {str(row["policy_id"]) for row in source_policies}:
        raise OperationalBankError("50-to-44 reconciliation does not cover the source bank")

    candidate_by_id = {
        str(row["candidate_id"]): dict(row)
        for row in candidate_bank.get("candidates", ())
    }
    tier_g_by_id = {
        str(row["candidate_id"]): dict(row)
        for row in reconciliation.get("tier_g_development_books", ())
    }
    xfa_by_id = _xfa_by_policy(xfa)
    xfa_frontier_by_id, xfa_frontier_selected = _xfa_frontier_by_policy(xfa_frontier)
    confirmation_by_id = _confirmation_by_candidate(confirmation)
    dynamic_result_by_id = {
        str(row["policy_id"]): dict(row) for row in dynamic.get("policy_results", ())
    }
    dynamic_survivor_ids = {
        str(row["policy_id"]) for row in dynamic.get("dynamic_survivors", ())
    }

    component_ids: set[str] = set()
    for policy in source_policies:
        component_ids.update(str(value) for value in policy.get("components", ()))
    specs = _load_candidate_specs(root, component_ids)

    rows: list[dict[str, Any]] = []
    for policy in sorted(source_policies, key=lambda row: str(row["policy_id"])):
        policy_id = str(policy["policy_id"])
        components = [str(value) for value in policy.get("components", ())]
        fingerprints = dict(policy.get("fingerprints") or {})
        clean = policy_id in clean_ids
        effective_tier = str(policy["evidence_tier"])
        if policy_id in tier_g_by_id:
            effective_tier = "G"
        behavior_hash = str(fingerprints["episode_behavior_hash"])
        profile_id = "pnl_state_identity"
        related_xfa = xfa_by_id.get(policy_id)
        xfa_direct = None
        xfa_related = None
        if related_xfa:
            if related_xfa["selected_profile_id"] == profile_id:
                xfa_direct = related_xfa
            else:
                xfa_related = related_xfa
        direct_frontier = xfa_frontier_by_id.get(policy_id) if xfa_direct else None
        related_frontier = xfa_frontier_by_id.get(policy_id) if xfa_related else None
        if not clean:
            next_action = "DO_NOT_REUSE_EXACT_QUARANTINED_CELL_NEW_FINGERPRINT_REQUIRED"
        elif effective_tier == "G":
            next_action = "PRESERVE_DEVELOPMENT_ARTIFACT_CONFIRMATION_BRANCH_CLOSED"
        elif effective_tier == "Q":
            next_action = "BLOCK_DIVERSE_GRADUATION_WITHOUT_RETUNE"
        else:
            next_action = "QUALIFICATION_REPLAY_WITHOUT_STATUS_INHERITANCE"
        rows.append(
            {
                "configuration_id": policy_id,
                "base_policy_id": policy_id,
                "record_type": "BASE_POLICY",
                "immutable_fingerprint": fingerprints.get("policy_spec_hash"),
                "episode_behavior_hash": behavior_hash,
                "behavior_cluster_id": f"exact_account_path_{behavior_hash[:16]}",
                "cluster_contract": "EXACT_EPISODE_BEHAVIOR_HASH_IDENTITY_ONLY",
                "lineage": {
                    "campaign": "HYDRA_FAST_PASS_FACTORY_0029",
                    "source_kind": policy["source_kind"],
                    "components": components,
                    "governor_profile_id": fingerprints.get("governor_profile_id"),
                },
                "niches": _policy_niches(components, specs),
                "account": dict(policy.get("account") or {}),
                "operational_status": (
                    "ACTIVE_RESEARCH_BANK" if clean else "QUARANTINED_EXACT_POLICY_CELL"
                ),
                "operationally_usable_for_research": clean,
                "classification_status": policy["classification_status"],
                "evidence_tier": effective_tier,
                "source_evidence_tier": policy["evidence_tier"],
                "independent_confirmation_claimed": False,
                "promotion_status": None,
                "quarantine": quarantine_by_id.get(policy_id),
                "horizons": _compact_horizons(policy),
                "confirmation": confirmation_by_id.get(
                    policy_id,
                    {"status": "NOT_INDEPENDENTLY_CONFIRMED", "tier_c_gate_passed": False},
                ),
                "xfa": xfa_direct,
                "related_xfa_diagnostic_different_profile": xfa_related,
                "xfa_post_payout_frontier": direct_frontier,
                "related_xfa_post_payout_frontier_different_profile": related_frontier,
                "next_evidence_action": next_action,
            }
        )

    dynamic_rows: list[dict[str, Any]] = []
    for policy_id in sorted(dynamic_survivor_ids):
        if policy_id not in clean_ids:
            raise OperationalBankError("dynamic survivor descends from quarantined policy")
        result = dynamic_result_by_id[policy_id]
        source = next(row for row in source_policies if row["policy_id"] == policy_id)
        profile_id = str(result["selected_profile_id"])
        behavior_hash = _dynamic_behavior_hash(result)
        selected = dict(result.get("selected") or {})
        baseline = dict(result.get("baseline") or {})
        selected_path_hashes = sorted(
            str(metrics["episode_path_hash"])
            for scenario in dict(selected.get("summaries") or {}).values()
            if isinstance(scenario, Mapping)
            for metrics in scenario.values()
            if isinstance(metrics, Mapping) and metrics.get("episode_path_hash")
        )
        baseline_path_hashes = sorted(
            str(metrics["episode_path_hash"])
            for scenario in dict(baseline.get("summaries") or {}).values()
            if isinstance(scenario, Mapping)
            for metrics in scenario.values()
            if isinstance(metrics, Mapping) and metrics.get("episode_path_hash")
        )
        if not selected_path_hashes or selected_path_hashes == baseline_path_hashes:
            raise OperationalBankError("dynamic survivor is execution-equivalent to its base")
        direct_xfa = xfa_by_id.get(policy_id)
        if direct_xfa and direct_xfa["selected_profile_id"] != profile_id:
            direct_xfa = None
        dynamic_rows.append(
            {
                "configuration_id": f"{policy_id}::{profile_id}",
                "base_policy_id": policy_id,
                "record_type": "DYNAMIC_ACCOUNT_POLICY_VARIANT",
                "immutable_fingerprint": stable_hash(
                    {
                        "policy_id": policy_id,
                        "profile_id": profile_id,
                        "profile_hash": selected.get("profile_hash"),
                        "selected_result_hash": selected.get("result_hash"),
                    }
                ),
                "episode_behavior_hash": behavior_hash,
                "behavior_cluster_id": f"exact_account_path_{behavior_hash[:16]}",
                "cluster_contract": "DYNAMIC_EPISODE_PATH_HASH_IDENTITY_ONLY",
                "nonclone_evidence": {
                    "selected_profile_is_non_identity": profile_id != "pnl_state_identity",
                    "selected_episode_path_hashes_differ_from_baseline": True,
                    "selected_result_hash": selected.get("result_hash"),
                    "baseline_result_hash": baseline.get("result_hash"),
                },
                "lineage": {
                    "campaign": "PNL_STATE_RISK_FRONTIER_V1",
                    "source_kind": source["source_kind"],
                    "components": [str(value) for value in source.get("components", ())],
                    "source_policy_id": policy_id,
                    "governor_profile_id": profile_id,
                },
                "niches": _policy_niches(source.get("components", ()), specs),
                "account": {"label": result.get("account_label")},
                "operational_status": "ACTIVE_DEVELOPMENT_DIAGNOSTIC",
                "operationally_usable_for_research": True,
                "classification_status": "DYNAMIC_SIZING_DEVELOPMENT_SURVIVOR",
                "evidence_tier": "E",
                "source_evidence_tier": source["evidence_tier"],
                "independent_confirmation_claimed": False,
                "promotion_status": None,
                "quarantine": None,
                "horizons": _dynamic_horizons(result),
                "confirmation": {
                    "status": "NOT_INDEPENDENTLY_CONFIRMED",
                    "tier_c_gate_passed": False,
                },
                "xfa": direct_xfa,
                "related_xfa_diagnostic_different_profile": None,
                "xfa_post_payout_frontier": (
                    xfa_frontier_by_id.get(policy_id) if direct_xfa else None
                ),
                "related_xfa_post_payout_frontier_different_profile": None,
                "next_evidence_action": "BLOCK_DIVERSE_GRADUATION_WITHOUT_RETUNE",
            }
        )
    if len(dynamic_rows) != 3:
        raise OperationalBankError("expected three clean dynamic account-policy survivors")
    if len({row["episode_behavior_hash"] for row in dynamic_rows}) != 3:
        raise OperationalBankError("dynamic survivor account paths are not distinct")
    rows.extend(dynamic_rows)
    rows.sort(key=lambda row: str(row["configuration_id"]))

    clean_base = [
        row for row in rows
        if row["record_type"] == "BASE_POLICY" and row["operationally_usable_for_research"]
    ]
    tier_counts = Counter(str(row["evidence_tier"]) for row in clean_base)
    tier_counts.update({"C": 0, "F": 0})
    niche_counts = {
        field: dict(sorted(Counter(value for row in clean_base for value in row["niches"][field]).items()))
        for field in ("markets", "sessions", "timeframes", "mechanisms")
    }
    safe_xfa_cells = list(xfa_summary.get("nonnegative_buffer_payout_policy_horizons") or [])
    best_xfa_ev = max(
        xfa_frontier_selected,
        key=lambda row: float(row["expected_trader_net_payout_per_new_combine_attempt_usd"]),
    )
    best_xfa_survival = max(
        xfa_frontier_selected,
        key=lambda row: (
            float(
                dict(dict(row["post_payout_survival"])["checkpoints"]["30"])[
                    "demonstrated_survival_rate_all_first_payout_paths"
                ]
            ),
            float(row["minimum_mll_buffer_usd"]),
        ),
    )

    matrix = _with_hash(
        {
            "schema": SCHEMA,
            "status": "DERIVED_READ_ONLY_OPERATIONAL_RESEARCH_BANK",
            "authoritative_state_modified": False,
            "promotion_status": None,
            "standard_and_consistency_are_alternatives": True,
            "sum_standard_and_consistency_ev_allowed": False,
            "rows": rows,
        }
    )
    summary = _with_hash(
        {
            "schema": SUMMARY_SCHEMA,
            "status": "OPERATIONAL_CANDIDATE_BANK_V2_MATERIALIZED",
            "source_reconciliation": {
                "observed_pass_source_policy_count": 50,
                "observed_pass_unique_episode_behavior_hash_count": 50,
                "clean_operational_base_policy_count": len(clean_base),
                "quarantined_exact_policy_cell_count": len(quarantine_by_id),
                "explanation": (
                    "The 50-policy source bank records distinct observed development pass "
                    "paths. A later exact-cell quarantine excludes six configurations from "
                    "operational use, leaving 44 clean bases; neither fact is erased."
                ),
                "quarantined_policy_ids": sorted(quarantine_by_id),
            },
            "bank_counts": {
                "lifecycle_matrix_row_count": len(rows),
                "clean_underlying_strategy_count": len(clean_base),
                "clean_dynamic_nonclone_account_policy_variant_count": len(dynamic_rows),
                "usable_research_configuration_count": len(clean_base) + len(dynamic_rows),
                "quarantined_preserved_count": len(quarantine_by_id),
                "shortage_to_50_clean_underlying_strategies": max(0, 50 - len(clean_base)),
                "shortage_to_50_usable_account_policy_configurations": max(
                    0, 50 - len(clean_base) - len(dynamic_rows)
                ),
                "exact_behavior_clusters_base_source": 50,
                "dynamic_exact_account_path_clusters": len(dynamic_rows),
            },
            "active_clean_base_tier_counts": {
                tier: int(tier_counts.get(tier, 0)) for tier in ("E", "Q", "G", "C", "F")
            },
            "source_tier_counts_before_quarantine": dict(
                sorted(Counter(str(row["evidence_tier"]) for row in source_policies).items())
            ),
            "confirmation": {
                "tier_c_count": 0,
                "tier_f_count": 0,
                "tier_g_terminal_statuses": confirmation_by_id,
                "strongest_candidate_second_replication_status": confirmation.get(
                    "economic_verdict"
                ),
            },
            "xfa": {
                "status": "DIAGNOSTIC_ONLY_NO_PROMOTION",
                "combine_transition_count": int(xfa["counts"]["clean_normal_combine_transition_count"]),
                "alternative_path_count": int(xfa["counts"]["alternative_path_count"]),
                "standard": xfa_summary["alternative_summaries"]["STANDARD"],
                "consistency": xfa_summary["alternative_summaries"]["CONSISTENCY"],
                "standard_and_consistency_are_alternatives": True,
                "sum_standard_and_consistency_ev_allowed": False,
                "nonnegative_buffer_payout_policy_horizon_count": len(safe_xfa_cells),
                "nonnegative_buffer_payout_policy_horizons": safe_xfa_cells,
                "maximum_observed_post_payout_trading_days": xfa_summary.get(
                    "maximum_observed_post_payout_trading_days"
                ),
                "post_payout_frontier": {
                    "status": xfa_frontier["status"],
                    "evidence_role": xfa_frontier["evidence_role"],
                    "promotion_status": xfa_frontier["promotion_status"],
                    "eligible_cell_count": int(xfa_frontier["eligible_cell_count"]),
                    "evaluation_count": int(xfa_frontier["evaluation_count"]),
                    "evaluation_count_by_path": xfa_frontier["evaluation_count_by_path"],
                    "baseline_reconciliation": xfa_frontier["baseline_reconciliation"],
                    "pareto_selected_profiles": xfa_frontier_selected,
                    "best_expected_value_profile": best_xfa_ev,
                    "best_observed_30d_survival_profile": best_xfa_survival,
                    "standard_and_consistency_are_alternatives": True,
                    "sum_standard_and_consistency_ev_allowed": False,
                },
            },
            "niche_counts_clean_base": niche_counts,
            "replacement_priority_queue": [
                {
                    "priority": 1,
                    "objective": "ADD_SIX_CLEAN_BEHAVIORALLY_DISTINCT_UNDERLYING_POLICIES",
                    "slots": max(0, 50 - len(clean_base)),
                    "gate": "OBSERVED_COMBINE_PASS_WITH_NONQUARANTINED_CAUSAL_EVIDENCE",
                },
                {
                    "priority": 2,
                    "objective": "CREATE_FIRST_INDEPENDENTLY_CONFIRMED_TIER_C_POLICY",
                    "slots": 1,
                    "gate": "ONE_SHOT_FROZEN_CONFIRMATION_WITHOUT_RETUNE",
                },
                {
                    "priority": 3,
                    "objective": "IMPROVE_XFA_POST_PAYOUT_SURVIVAL",
                    "slots": 1,
                    "gate": "SEPARATE_STANDARD_AND_CONSISTENCY_30_60_90_DAY_SURVIVAL",
                },
                {
                    "priority": 4,
                    "objective": "FILL_UNDERREPRESENTED_MARKET_SESSION_MECHANISM_NICHES",
                    "slots": max(0, 50 - len(clean_base)),
                    "gate": "DISTINCT_ACCOUNT_PATH_AND_POSITIVE_MARGINAL_TARGET_VELOCITY",
                },
            ],
            "source_hashes": {
                str(path): _sha256(root / path)
                for path in (
                    PASS_BANK_PATH,
                    CANDIDATE_BANK_PATH,
                    RECONCILIATION_PATH,
                    DYNAMIC_PATH,
                    XFA_HANDOFF_PATH,
                    XFA_SUMMARY_PATH,
                    XFA_FRONTIER_PATH,
                    CONFIRMATION_PATH,
                )
            },
            "lifecycle_matrix_hash": matrix["result_hash"],
            "authoritative_state_modified": False,
            "promotion_status": None,
        }
    )
    verify_bank(matrix, summary)
    return matrix, summary


def verify_bank(matrix: Mapping[str, Any], summary: Mapping[str, Any]) -> None:
    verify_hashed(matrix)
    verify_hashed(summary)
    rows = [dict(row) for row in matrix.get("rows", ())]
    if len(rows) != 53:
        raise OperationalBankError("expected 50 base rows plus three dynamic variants")
    configuration_ids = [str(row["configuration_id"]) for row in rows]
    if len(set(configuration_ids)) != len(configuration_ids):
        raise OperationalBankError("duplicate lifecycle configuration ID")
    if any(row.get("independent_confirmation_claimed") for row in rows):
        raise OperationalBankError("independent confirmation status inflation")
    counts = dict(summary["bank_counts"])
    if counts["clean_underlying_strategy_count"] != 44:
        raise OperationalBankError("clean base count drift")
    if counts["quarantined_preserved_count"] != 6:
        raise OperationalBankError("quarantine count drift")
    xfa = dict(summary["xfa"])
    if xfa["combine_transition_count"] != 71 or xfa["alternative_path_count"] != 142:
        raise OperationalBankError("XFA path-count reconciliation drift")
    if not xfa["standard_and_consistency_are_alternatives"]:
        raise OperationalBankError("XFA alternatives contract missing")
    if xfa["sum_standard_and_consistency_ev_allowed"]:
        raise OperationalBankError("XFA alternative EV must never be additive")
    frontier = dict(xfa["post_payout_frontier"])
    if frontier["status"] != "COMPLETE_BOUNDED_XFA_POST_PAYOUT_DEVELOPMENT_DIAGNOSTIC":
        raise OperationalBankError("XFA post-payout frontier is not complete")
    if frontier["evaluation_count"] != 108 or len(frontier["pareto_selected_profiles"]) != 7:
        raise OperationalBankError("XFA post-payout frontier count drift")


def write_bank(
    root: Path, output_dir: Path = DEFAULT_OUTPUT_DIR
) -> tuple[Path, Path, Path]:
    matrix, summary = build_bank(root)
    target = root / output_dir
    target.mkdir(parents=True, exist_ok=True)
    matrix_path = target / "lifecycle_matrix.json"
    summary_path = target / "bank_summary.json"
    csv_path = target / "lifecycle_matrix.csv"
    matrix_path.write_text(
        json.dumps(matrix, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    csv_rows = [_flatten_csv_row(row) for row in matrix["rows"]]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(csv_rows[0]), lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(csv_rows)
    return matrix_path, csv_path, summary_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    paths = write_bank(args.root.resolve(), args.output_dir)
    print(json.dumps({"outputs": [str(path) for path in paths]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
