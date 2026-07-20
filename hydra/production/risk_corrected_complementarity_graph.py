"""Risk-corrected complementarity graph over immutable causal 0029 ledgers.

The module is intentionally read-only with respect to HYDRA's authoritative
state.  It reconstructs the 24 Tier-Q component inventory and the 50-policy
pass-observed development bank, rejects every inherited epsilon-risk status,
and evaluates newly frozen two-to-four sleeve account books.  Membership is
selected with B1/B2 only; B3/B4 are opened only after the finalist inventory is
frozen.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_graduation_cohort as cohort
from hydra.production import autonomous_marginal_combine_books as books
from hydra.production.autonomous_exact_replay import _account_config


SCHEMA = "hydra_risk_corrected_complementarity_graph_result_v1"
MANIFEST_SCHEMA = "hydra_risk_corrected_complementarity_graph_manifest_v1"
DEFAULT_MANIFEST = Path(
    "config/research/risk_corrected_complementarity_graph_v1.json"
)
DESIGN_BLOCKS = ("B1", "B2")
HELD_OUT_BLOCKS = ("B3", "B4")
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
HORIZONS = (5, 10, 20)


class RiskCorrectedComplementarityError(RuntimeError):
    """The bounded graph cannot run without weakening its frozen contract."""


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RiskCorrectedComplementarityError(f"cannot read JSON: {path}") from exc
    if not isinstance(value, dict):
        raise RiskCorrectedComplementarityError(f"JSON object required: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_self_hash(value: Mapping[str, Any], *, field: str = "result_hash") -> None:
    core = dict(value)
    claimed = core.pop(field, None)
    if not isinstance(claimed, str) or claimed != stable_hash(core):
        raise RiskCorrectedComplementarityError(f"{field} drift")


def _load_manifest(path: Path) -> dict[str, Any]:
    value = _read(path)
    if value.get("schema") != MANIFEST_SCHEMA:
        raise RiskCorrectedComplementarityError("unexpected graph manifest schema")
    core = {key: row for key, row in value.items() if key != "manifest_hash"}
    if value.get("manifest_hash") != stable_hash(core):
        raise RiskCorrectedComplementarityError("graph manifest self-hash drift")
    return value


def _inside(project: Path, relative: str | Path) -> Path:
    path = (project / relative).resolve()
    try:
        path.relative_to(project)
    except ValueError as exc:
        raise RiskCorrectedComplementarityError("source path escapes project") from exc
    return path


def _source_context(
    project: Path, manifest: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], Any, list[dict[str, Any]]]:
    source = dict(manifest["source_contract"])
    candidate_path = _inside(project, source["candidate_bank_path"])
    pass_path = _inside(project, source["pass_observed_bank_path"])
    cohort_manifest_path = _inside(project, source["cohort_manifest_path"])
    for path, expected in (
        (candidate_path, source["candidate_bank_file_sha256"]),
        (pass_path, source["pass_observed_bank_file_sha256"]),
        (cohort_manifest_path, source["cohort_manifest_file_sha256"]),
    ):
        if _sha256(path) != str(expected):
            raise RiskCorrectedComplementarityError(f"source SHA drift: {path}")

    candidate_wrapper = _read(candidate_path)
    _verify_self_hash(candidate_wrapper)
    candidate_bank = books._verify_candidate_bank(
        dict(candidate_wrapper["candidate_bank"])
    )
    if candidate_bank["result_hash"] != source["candidate_bank_hash"]:
        raise RiskCorrectedComplementarityError("candidate-bank logical hash drift")

    pass_wrapper = _read(pass_path)
    _verify_self_hash(pass_wrapper)
    pass_bank = dict(pass_wrapper["combine_pass_observed_bank"])
    _verify_self_hash(pass_bank)
    if (
        pass_bank.get("status")
        != "COMBINE_PASS_OBSERVED_DEVELOPMENT_BANK_TARGET_REACHED"
        or pass_bank["result_hash"] != source["pass_observed_bank_hash"]
        or int(dict(pass_bank["counts"])["bank_policy_count"]) != 50
        or len(pass_bank["policies"]) != 50
    ):
        raise RiskCorrectedComplementarityError("pass-observed bank contract drift")

    cohort_manifest = cohort._load_cohort_manifest(cohort_manifest_path)
    artifacts = cohort._load_replay_artifacts(project, cohort_manifest)
    tier_q_rows = [
        dict(row)
        for row in artifacts["bank"]["candidates"]
        if row.get("tier_q_contract_cleared") is True
    ]
    if len(tier_q_rows) != 24:
        raise RiskCorrectedComplementarityError("authoritative Tier-Q count is not 24")
    context = books._prepare_replay_context(
        project,
        tuple(tier_q_rows),
        artifacts["exact_results"],
        fast_pass_manifest_path=artifacts["fast_manifest_path"],
        rule_snapshot_path=artifacts["rule_snapshot_path"],
    )
    books._verify_context_matches_bank(context, tier_q_rows)
    return candidate_bank, pass_bank, context, tier_q_rows


def _exact_candidate_cell_policy_id(cell: Mapping[str, Any]) -> str:
    return (
        f"exact-0029:{cell['candidate_id']}:{int(cell['account_size_usd'])}:"
        f"{int(cell['integer_quantity_tier'])}:{cell['risk_governor_mode']}"
    )


def _epsilon_status_audit(
    project: Path,
    manifest: Mapping[str, Any],
    tier_q_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Reconcile legacy epsilon-risk evidence across all promotion layers.

    The source artifacts remain immutable.  The receipt quarantines (1) exact
    candidate cells whose selected governor mode used the legacy contract-only
    epsilon fallback, (2) marginal-book policies carrying an explicit epsilon
    charge, and (3) graduated Tier-G books carrying an explicit epsilon charge.
    """

    source = dict(manifest["source_contract"])

    exact_cells: list[dict[str, Any]] = []
    for source_row in tier_q_rows:
        row = dict(source_row)
        cell = dict(row.get("best_safe_cell") or {})
        if cell.get("risk_governor_mode") != "CONTRACT_ONLY_UNIFORM_SCALE":
            continue
        candidate_id = str(row.get("candidate_id") or "")
        if not candidate_id or cell.get("candidate_id") != candidate_id:
            raise RiskCorrectedComplementarityError(
                "contract-only candidate-cell identity drift"
            )
        exact_cells.append(
            {
                "candidate_id": candidate_id,
                "candidate_fingerprint": row.get("candidate_fingerprint"),
                "policy_id": _exact_candidate_cell_policy_id(cell),
                "selected_cell_hash": cell.get("cell_hash"),
                "source_exact_result_hash": row.get("source_exact_result_hash"),
                "account_size_usd": int(cell["account_size_usd"]),
                "integer_quantity_tier": int(cell["integer_quantity_tier"]),
                "horizon_trading_days": int(cell["horizon_trading_days"]),
                "risk_governor_mode": cell["risk_governor_mode"],
                "status": "QUARANTINED_CONTRACT_ONLY_EXACT_CELL_STATUS",
                "reason": "LEGACY_MODE_DEPENDED_ON_EPSILON_NOMINAL_RISK_FALLBACK",
            }
        )
    exact_cells.sort(key=lambda row: row["candidate_id"])
    if len(exact_cells) != 6:
        raise RiskCorrectedComplementarityError(
            "authoritative contract-only Tier-Q cell count is not 6"
        )

    path = _inside(project, source["marginal_book_path"])
    if _sha256(path) != str(source["marginal_book_file_sha256"]):
        raise RiskCorrectedComplementarityError("marginal source SHA drift")
    wrapper = _read(path)
    marginal = dict(wrapper["semantic_marginal_book_composite"])
    _verify_self_hash(marginal)
    marginal_statuses: list[dict[str, Any]] = []
    seen: set[str] = set()
    for field in ("standalone_results", "supporting_policy_results", "book_results"):
        for raw in marginal.get(field, ()):
            row = dict(raw)
            policy = dict(row.get("governor_policy") or {})
            charges = dict(policy.get("nominal_risk_charge_per_mini") or {})
            epsilon = {
                str(key): float(value)
                for key, value in charges.items()
                if math.isfinite(float(value)) and float(value) < 1.0
            }
            policy_id = str(row.get("policy_id") or "")
            if epsilon and policy_id and policy_id not in seen:
                seen.add(policy_id)
                marginal_statuses.append(
                    {
                        "policy_id": policy_id,
                        "source_field": field,
                        "component_ids": sorted(epsilon),
                        "epsilon_charges": epsilon,
                        "status": "QUARANTINED_EPSILON_NOMINAL_RISK_STATUS",
                    }
                )
    marginal_statuses.sort(key=lambda row: row["policy_id"])
    marginal_components = sorted(
        {value for row in marginal_statuses for value in row["component_ids"]}
    )
    if "hazard_020ae195ccef8e39b1907e38" not in marginal_components:
        raise RiskCorrectedComplementarityError("known epsilon candidate absent")

    tier_g_path = _inside(project, source["tier_g_graduation_path"])
    if _sha256(tier_g_path) != str(source["tier_g_graduation_file_sha256"]):
        raise RiskCorrectedComplementarityError("Tier-G source SHA drift")
    tier_g_wrapper = _read(tier_g_path)
    _verify_self_hash(tier_g_wrapper)
    tier_g = dict(tier_g_wrapper["tier_g_development_graduation"])
    _verify_self_hash(tier_g)
    if tier_g["result_hash"] != source["tier_g_graduation_hash"]:
        raise RiskCorrectedComplementarityError("Tier-G logical hash drift")
    tier_g_statuses: list[dict[str, Any]] = []
    for raw in tier_g.get("graduated_development_books", ()):
        row = dict(raw)
        book = dict(row.get("combine_book") or {})
        policy = dict(book.get("frozen_account_policy") or {})
        charges = dict(policy.get("nominal_risk_charge_per_mini") or {})
        epsilon = {
            str(key): float(value)
            for key, value in charges.items()
            if math.isfinite(float(value)) and float(value) < 1.0
        }
        if epsilon:
            tier_g_statuses.append(
                {
                    "candidate_id": str(row["candidate_id"]),
                    "policy_id": str(policy["policy_id"]),
                    "combine_book_hash": str(row["combine_book_hash"]),
                    "graduation_evidence_hash": str(row["graduation_evidence_hash"]),
                    "frozen_account_policy_hash": str(
                        row["frozen_account_policy_hash"]
                    ),
                    "epsilon_charges": epsilon,
                    "prior_graduation_status": row.get("graduation_status"),
                    "status": "QUARANTINED_TIER_G_EPSILON_RISK_STATUS",
                }
            )
    tier_g_statuses.sort(key=lambda row: row["policy_id"])
    if len(tier_g_statuses) != 1 or tier_g_statuses[0]["candidate_id"] != (
        "hazard_020ae195ccef8e39b1907e38"
    ):
        raise RiskCorrectedComplementarityError(
            "authoritative Tier-G epsilon scope is not the expected singleton"
        )

    layer_candidate_ids = {
        "exact_candidate_cells": sorted(row["candidate_id"] for row in exact_cells),
        "marginal_book_policies": marginal_components,
        "tier_g_books": sorted(row["candidate_id"] for row in tier_g_statuses),
    }
    layer_policy_ids = {
        "exact_candidate_cells": sorted(row["policy_id"] for row in exact_cells),
        "marginal_book_policies": sorted(
            row["policy_id"] for row in marginal_statuses
        ),
        "tier_g_books": sorted(row["policy_id"] for row in tier_g_statuses),
    }
    unique_candidate_ids = sorted(
        {value for rows in layer_candidate_ids.values() for value in rows}
    )
    unique_policy_ids = sorted(
        {value for rows in layer_policy_ids.values() for value in rows}
    )
    return {
        "scope": "THREE_LAYER_LEGACY_EPSILON_RISK_EVIDENCE_RECONCILIATION",
        "source_candidate_bank_hash": manifest["source_contract"][
            "candidate_bank_hash"
        ],
        "source_marginal_book_hash": marginal["result_hash"],
        "source_tier_g_graduation_hash": tier_g["result_hash"],
        "layers": {
            "exact_candidate_cells": {
                "status_count": len(exact_cells),
                "candidate_ids": layer_candidate_ids["exact_candidate_cells"],
                "policy_ids": layer_policy_ids["exact_candidate_cells"],
                "statuses": exact_cells,
            },
            "marginal_book_policies": {
                "status_count": len(marginal_statuses),
                "candidate_ids": layer_candidate_ids["marginal_book_policies"],
                "policy_ids": layer_policy_ids["marginal_book_policies"],
                "statuses": marginal_statuses,
            },
            "tier_g_books": {
                "status_count": len(tier_g_statuses),
                "candidate_ids": layer_candidate_ids["tier_g_books"],
                "policy_ids": layer_policy_ids["tier_g_books"],
                "statuses": tier_g_statuses,
            },
        },
        "layer_status_count": len(exact_cells)
        + len(marginal_statuses)
        + len(tier_g_statuses),
        "unique_candidate_count": len(unique_candidate_ids),
        "unique_candidate_ids": unique_candidate_ids,
        "unique_policy_count": len(unique_policy_ids),
        "unique_policy_ids": unique_policy_ids,
        "source_artifacts_mutated": False,
        "inherited_statuses_used": False,
    }


def _strict_risk_charges(context: Any) -> dict[str, float]:
    charges: dict[str, float] = {}
    for candidate_id, component in sorted(context.components.items()):
        charge = float(component.declared_risk_charge_per_mini)
        if not math.isfinite(charge) or charge < 1.0:
            raise RiskCorrectedComplementarityError(
                f"epsilon/nonfinite causal risk charge: {candidate_id}"
            )
        charges[candidate_id] = charge
    return charges


def _market(component: Any) -> str:
    markets = {
        str(row.market)
        for row in (*component.normal_trajectories, *component.stressed_trajectories)
    }
    if len(markets) != 1:
        raise RiskCorrectedComplementarityError("component market is not unique")
    return next(iter(markets))


def _block_boundaries(context: Any) -> dict[str, tuple[int, int]]:
    starts = {
        block: sorted(
            day
            for values in context.starts.values()
            for day, observed in values
            if observed == block
        )
        for block in (*DESIGN_BLOCKS, *HELD_OUT_BLOCKS)
    }
    if any(not values for values in starts.values()):
        raise RiskCorrectedComplementarityError("temporal block starts are incomplete")
    first = {block: min(values) for block, values in starts.items()}
    order = (*DESIGN_BLOCKS, *HELD_OUT_BLOCKS)
    output: dict[str, tuple[int, int]] = {}
    for index, block in enumerate(order):
        lower = first[block]
        upper = first[order[index + 1]] if index + 1 < len(order) else max(context.calendar) + 1
        output[block] = (lower, upper)
    return output


def _pearson(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right) or not left:
        return 0.0
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    left_delta = [value - left_mean for value in left]
    right_delta = [value - right_mean for value in right]
    denominator = math.sqrt(
        sum(value * value for value in left_delta)
        * sum(value * value for value in right_delta)
    )
    if denominator <= 1e-12:
        return 0.0
    return max(-1.0, min(1.0, sum(a * b for a, b in zip(left_delta, right_delta)) / denominator))


def _jaccard(left: set[int], right: set[int]) -> float:
    union = left | right
    return len(left & right) / len(union) if union else 0.0


def _clusters(
    ids: Sequence[str], similarity: Mapping[tuple[str, str], float], threshold: float
) -> dict[str, str]:
    parent = {value: value for value in ids}

    def find(value: str) -> str:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str) -> None:
        a, b = find(left), find(right)
        if a != b:
            parent[max(a, b)] = min(a, b)

    for (left, right), value in similarity.items():
        if value >= threshold:
            union(left, right)
    groups: dict[str, list[str]] = defaultdict(list)
    for value in ids:
        groups[find(value)].append(value)
    output: dict[str, str] = {}
    for members in groups.values():
        cluster_id = "cluster_" + stable_hash(sorted(members))[:16]
        for value in members:
            output[value] = cluster_id
    return output


def _component_graph(context: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    bounds = _block_boundaries(context)
    design_lower = bounds["B1"][0]
    design_upper = bounds["B2"][1]
    design_days = [day for day in context.calendar if design_lower <= day < design_upper]
    raw: dict[str, dict[str, Any]] = {}
    for candidate_id, component in sorted(context.components.items()):
        pnl = defaultdict(float)
        active: set[int] = set()
        for trajectory in component.stressed_trajectories:
            day = int(trajectory.event.session_day)
            if design_lower <= day < design_upper:
                pnl[day] += float(trajectory.event.net_pnl)
                active.add(day)
        vector = [float(pnl[day]) for day in design_days]
        loss = {day for day in design_days if pnl[day] < 0.0}
        design = dict(component.source_receipt["design_metrics"])
        stressed = dict(design["stressed"])
        normal = dict(design["normal"])
        utility = (
            6.0 * float(stressed["pass_rate"])
            + 3.0 * float(normal["pass_rate"])
            - 4.0 * float(stressed["mll_breach_rate"])
            + math.tanh(float(stressed["net_total_usd"]) / 10_000.0)
        )
        raw[candidate_id] = {
            "candidate_id": candidate_id,
            "account_label": component.account_label,
            "market": _market(component),
            "behavioral_fingerprint": component.behavioral_fingerprint,
            "qd_cell": component.qd_cell,
            "declared_risk_charge_per_mini": float(component.declared_risk_charge_per_mini),
            "source_governor_mode": component.source_governor_mode,
            "design_utility": utility,
            "design_metrics": design,
            "pnl_vector": vector,
            "loss_days": loss,
            "active_days": active,
        }
    ids = sorted(raw)
    pnl_corr: dict[tuple[str, str], float] = {}
    loss_overlap: dict[tuple[str, str], float] = {}
    active_overlap: dict[tuple[str, str], float] = {}
    edges: list[dict[str, Any]] = []
    for left, right in itertools.combinations(ids, 2):
        a, b = raw[left], raw[right]
        key = (left, right)
        pnl_corr[key] = _pearson(a["pnl_vector"], b["pnl_vector"])
        loss_overlap[key] = _jaccard(a["loss_days"], b["loss_days"])
        active_overlap[key] = _jaccard(a["active_days"], b["active_days"])
        score = (
            0.35 * ((1.0 - pnl_corr[key]) / 2.0)
            + 0.30 * (1.0 - loss_overlap[key])
            + 0.20 * (1.0 - active_overlap[key])
            + 0.15 * float(a["market"] != b["market"])
        )
        edges.append(
            {
                "left": left,
                "right": right,
                "same_account": a["account_label"] == b["account_label"],
                "same_market": a["market"] == b["market"],
                "daily_pnl_correlation": pnl_corr[key],
                "loss_day_jaccard": loss_overlap[key],
                "active_day_jaccard": active_overlap[key],
                "complementarity_score": score,
            }
        )
    path_clusters = _clusters(ids, pnl_corr, 0.90)
    loss_clusters = _clusters(ids, loss_overlap, 0.75)
    nodes: list[dict[str, Any]] = []
    for candidate_id in ids:
        row = raw[candidate_id]
        nodes.append(
            {
                key: value
                for key, value in row.items()
                if key not in {"pnl_vector", "loss_days", "active_days"}
            }
            | {
                "account_path_cluster": path_clusters[candidate_id],
                "loss_day_cluster": loss_clusters[candidate_id],
                "design_active_day_count": len(row["active_days"]),
                "design_loss_day_count": len(row["loss_days"]),
                "design_daily_path_hash": stable_hash(row["pnl_vector"]),
            }
        )
    return nodes, edges


def _graph_proposals(
    nodes: Sequence[Mapping[str, Any]],
    edges: Sequence[Mapping[str, Any]],
    manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    by_id = {str(row["candidate_id"]): dict(row) for row in nodes}
    edge = {
        tuple(sorted((str(row["left"]), str(row["right"])))): float(
            row["complementarity_score"]
        )
        for row in edges
    }
    by_account: dict[str, list[str]] = defaultdict(list)
    for candidate_id, row in by_id.items():
        by_account[str(row["account_label"])].append(candidate_id)
    inventory: list[dict[str, Any]] = []
    for account_label, ids in sorted(by_account.items()):
        for size in range(2, int(manifest["selection_contract"]["maximum_sleeves"]) + 1):
            for members in itertools.combinations(sorted(ids), size):
                market_counts: dict[str, int] = defaultdict(int)
                for value in members:
                    market_counts[str(by_id[value]["market"])] += 1
                if max(market_counts.values()) > int(
                    manifest["selection_contract"]["maximum_components_per_market"]
                ):
                    continue
                pairs = list(itertools.combinations(members, 2))
                complementarity = statistics.fmean(
                    edge[tuple(sorted(pair))] for pair in pairs
                )
                utility = sum(float(by_id[value]["design_utility"]) for value in members)
                score = utility + 2.0 * complementarity + 0.25 * len(market_counts)
                priority = sorted(
                    members,
                    key=lambda value: (-float(by_id[value]["design_utility"]), value),
                )
                core = {
                    "account_label": account_label,
                    "component_ids": list(priority),
                    "component_set": sorted(members),
                    "component_count": size,
                    "markets": sorted(market_counts),
                    "account_path_clusters": sorted(
                        {str(by_id[value]["account_path_cluster"]) for value in members}
                    ),
                    "loss_day_clusters": sorted(
                        {str(by_id[value]["loss_day_cluster"]) for value in members}
                    ),
                    "graph_complementarity_score": complementarity,
                    "graph_design_score": score,
                    "selection_uses_blocks": list(DESIGN_BLOCKS),
                    "held_out_fields_used": False,
                }
                inventory.append(
                    {
                        **core,
                        "policy_id": "risk_corrected_graph_" + stable_hash(core)[:24],
                        "proposal_hash": stable_hash(core),
                    }
                )
    selected: list[dict[str, Any]] = []
    per_size = int(manifest["selection_contract"]["exact_design_proposals_per_size"])
    for size in (2, 3, 4):
        values = sorted(
            (row for row in inventory if int(row["component_count"]) == size),
            key=lambda row: (-float(row["graph_design_score"]), str(row["policy_id"])),
        )
        selected.extend(values[:per_size])
    selected.sort(key=lambda row: str(row["policy_id"]))
    return selected


def _policy(context: Any, proposal: Mapping[str, Any], manifest: Mapping[str, Any]) -> ActiveRiskPoolPolicy:
    members = tuple(str(value) for value in proposal["component_ids"])
    account_label = str(proposal["account_label"])
    if not 2 <= len(members) <= 4 or any(
        context.components[value].account_label != account_label for value in members
    ):
        raise RiskCorrectedComplementarityError("book membership/account drift")
    rule = dict(context.rules[account_label])
    frozen = dict(manifest["governor_contract"])
    charges = tuple(
        (value, float(context.components[value].declared_risk_charge_per_mini))
        for value in members
    )
    if any(value < 1.0 or not math.isfinite(value) for _key, value in charges):
        raise RiskCorrectedComplementarityError("epsilon charge reached book policy")
    target = float(rule["profit_target_usd"])
    mll = float(rule["maximum_loss_limit_usd"])
    return ActiveRiskPoolPolicy(
        policy_id=str(proposal["policy_id"]),
        component_priority=members,
        nominal_risk_charge_per_mini=charges,
        maximum_concurrent_sleeves=min(len(members), int(frozen["maximum_concurrent_sleeves"])),
        aggregate_open_risk_ceiling=mll * float(frozen["aggregate_open_risk_fraction"]),
        maximum_mll_buffer_fraction=float(frozen["aggregate_open_risk_fraction"]),
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=float(rule["maximum_mini_contracts"]),
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=mll * float(frozen["daily_loss_fraction"]),
        daily_consistency_profit_guard=target
        * float(rule["consistency_target_fraction"])
        * float(frozen["consistency_guard_fraction"]),
        target_protection_distance=target * float(frozen["target_protection_distance_fraction"]),
        target_protection_mode=TargetProtectionMode.SCALE_50,
        static_risk_tier=1.0,
    )


def _evaluate_role(
    context: Any,
    proposal: Mapping[str, Any],
    manifest: Mapping[str, Any],
    blocks: Sequence[str],
) -> dict[str, Any]:
    policy = _policy(context, proposal, manifest)
    members = tuple(policy.component_priority)
    account_label = str(proposal["account_label"])
    config = _account_config(context.rules[account_label])
    trajectories = {
        "NORMAL": {
            value: context.components[value].normal_trajectories for value in members
        },
        "STRESSED_1_5X": {
            value: context.components[value].stressed_trajectories for value in members
        },
    }
    unavailable: set[int] = set()
    calendar_set = set(context.calendar)
    for value in members:
        component = context.components[value]
        unavailable.update(calendar_set.difference(component.eligible_session_days))
        unavailable.update(component.censored_session_days)
    index = {day: offset for offset, day in enumerate(context.calendar)}
    summaries: dict[str, dict[str, Any]] = {scenario: {} for scenario in SCENARIOS}
    receipts: list[dict[str, Any]] = []
    for scenario in SCENARIOS:
        for horizon in HORIZONS:
            values: list[tuple[Any, str]] = []
            requested = 0
            censored = 0
            for start_day, block in context.starts[horizon]:
                if block not in blocks:
                    continue
                requested += 1
                offset = index[start_day]
                window = context.calendar[offset : offset + horizon]
                if len(window) != horizon or any(day in unavailable for day in window):
                    censored += 1
                    continue
                episode = run_causal_shared_account_episode(
                    trajectories[scenario],
                    context.calendar,
                    policy=policy,
                    start_day=start_day,
                    maximum_duration_days=horizon,
                    config=config,
                )
                values.append((episode, block))
                receipts.append(
                    {
                        "scenario": scenario,
                        "horizon_trading_days": horizon,
                        "start_day": start_day,
                        "temporal_block": block,
                        "terminal": episode.terminal.value,
                        "passed": bool(episode.passed),
                        "mll_breached": bool(episode.mll_breached),
                        "consistency_ok": bool(episode.consistency_ok),
                        "net_pnl_usd": float(episode.net_pnl),
                        "target_progress": float(episode.target_progress),
                        "minimum_mll_buffer_usd": float(episode.minimum_mll_buffer),
                        "episode_path_hash": stable_hash(episode.to_dict(include_paths=True)),
                    }
                )
            summaries[scenario][str(horizon)] = cohort._summarize_cohort_episodes(
                values,
                requested_start_count=requested,
                data_censored_count=censored,
                policy=policy,
            )
    return {
        "policy_id": proposal["policy_id"],
        "account_label": account_label,
        "component_ids": list(members),
        "proposal_hash": proposal["proposal_hash"],
        "blocks": list(blocks),
        "governor_policy": policy.to_dict(),
        "governor_policy_hash": stable_hash(policy.to_dict()),
        "summaries": summaries,
        "episode_count": len(receipts),
        "episode_receipt_hash": stable_hash(receipts),
    }


def _design_rank(value: Mapping[str, Any]) -> tuple[Any, ...]:
    best: tuple[Any, ...] | None = None
    for horizon in HORIZONS:
        normal = dict(value["summaries"]["NORMAL"][str(horizon)])
        stressed = dict(value["summaries"]["STRESSED_1_5X"][str(horizon)])
        row = (
            4 * int(stressed["pass_count"]) + 2 * int(normal["pass_count"]),
            len(stressed.get("blocks_with_passes") or ()),
            -float(stressed["mll_breach_rate"]),
            float(stressed["target_progress_p25"]),
            float(stressed["target_progress_median"]),
            float(stressed["net_total_usd"]),
            -horizon,
        )
        if best is None or row > best:
            best = row
    assert best is not None
    return (*best, str(value["policy_id"]))


def _gate(heldout: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, dict[str, bool]]:
    frozen = dict(manifest["development_gate"])
    output: dict[str, dict[str, bool]] = {}
    for horizon in HORIZONS:
        normal = dict(heldout["summaries"]["NORMAL"][str(horizon)])
        stressed = dict(heldout["summaries"]["STRESSED_1_5X"][str(horizon)])
        output[str(horizon)] = {
            "minimum_normal_passes": int(normal["pass_count"])
            >= int(frozen["minimum_normal_passes"]),
            "minimum_stressed_passes": int(stressed["pass_count"])
            >= int(frozen["minimum_stressed_passes"]),
            "passes_in_two_stressed_blocks": len(stressed.get("blocks_with_passes") or ())
            >= int(frozen["minimum_stressed_pass_blocks"]),
            "positive_stressed_net": float(stressed["net_total_usd"]) > 0.0,
            "stressed_mll_at_most_10pct": float(stressed["mll_breach_rate"])
            <= float(frozen["maximum_stressed_mll_breach_rate"]),
            "all_passing_paths_consistency_compliant": bool(
                stressed["all_passing_paths_consistency_compliant"]
            ),
        }
    return output


def run(
    root: str | Path, *, manifest_path: str | Path = DEFAULT_MANIFEST
) -> dict[str, Any]:
    project = Path(root).resolve()
    manifest = _load_manifest(_inside(project, manifest_path))
    candidate_bank, pass_bank, context, tier_q_rows = _source_context(project, manifest)
    epsilon = _epsilon_status_audit(project, manifest, tier_q_rows)
    charges = _strict_risk_charges(context)
    corrected_replay_charges = {
        candidate_id: charges[candidate_id]
        for candidate_id in epsilon["unique_candidate_ids"]
        if candidate_id in charges
    }
    corrected_quarantine_core = {
        **epsilon,
        "risk_corrected_replay_charges": corrected_replay_charges,
        "risk_corrected_replay_candidate_ids": sorted(corrected_replay_charges),
        "quarantined_not_replayed_candidate_ids": sorted(
            set(epsilon["unique_candidate_ids"]) - set(corrected_replay_charges)
        ),
        "old_promotional_evidence_status": (
            "QUARANTINED_NOT_DELETED_NOT_INHERITED"
        ),
        "candidate_reuse_condition": (
            "NEW_EXACT_REPLAY_WITH_MAX_CAUSAL_DECLARED_STOP_RISK_PER_MINI"
        ),
    }
    epsilon = {
        **corrected_quarantine_core,
        "quarantine_receipt_hash": stable_hash(corrected_quarantine_core),
    }
    nodes, edges = _component_graph(context)
    proposals = _graph_proposals(nodes, edges, manifest)
    if not proposals:
        raise RiskCorrectedComplementarityError("complementarity graph is empty")

    # B1/B2 are evaluated first and are the only outcomes used to freeze the
    # finalist list.  No B3/B4 replay occurs before this selection completes.
    design_results = [
        _evaluate_role(context, row, manifest, DESIGN_BLOCKS) for row in proposals
    ]
    finalist_count = int(manifest["selection_contract"]["held_out_finalist_count"])
    finalists = sorted(design_results, key=_design_rank, reverse=True)[:finalist_count]
    finalist_ids = [str(row["policy_id"]) for row in finalists]
    frozen_finalist_hash = stable_hash(
        [
            {
                "policy_id": row["policy_id"],
                "proposal_hash": row["proposal_hash"],
                "design_evidence_hash": row["episode_receipt_hash"],
            }
            for row in finalists
        ]
    )
    proposal_by_id = {str(row["policy_id"]): row for row in proposals}

    heldout_results: list[dict[str, Any]] = []
    for design in finalists:
        proposal = proposal_by_id[str(design["policy_id"])]
        heldout = _evaluate_role(context, proposal, manifest, HELD_OUT_BLOCKS)
        gates = _gate(heldout, manifest)
        qualified = [
            int(horizon) for horizon, values in gates.items() if all(values.values())
        ]
        heldout_results.append(
            {
                **heldout,
                "design_result": design,
                "development_gate_results": gates,
                "qualified_horizons": qualified,
                "computed_evidence_tier": (
                    "G_DEVELOPMENT_ONLY" if qualified else "Q_BOOK_DIAGNOSTIC"
                ),
                "independent_confirmation_claimed": False,
            }
        )
    qualified = [row for row in heldout_results if row["qualified_horizons"]]
    best = max(heldout_results, key=_design_rank)
    core = {
        "schema": SCHEMA,
        "status": (
            "RISK_CORRECTED_COMPLEMENTARITY_GRAPH_QUALIFIER_FOUND"
            if qualified
            else "RISK_CORRECTED_COMPLEMENTARITY_GRAPH_NO_QUALIFIER"
        ),
        "manifest_hash": manifest["manifest_hash"],
        "source_candidate_bank_hash": candidate_bank["result_hash"],
        "source_pass_observed_bank_hash": pass_bank["result_hash"],
        "source_inventory": {
            "tier_q_candidate_count": len(tier_q_rows),
            "pass_observed_policy_count": len(pass_bank["policies"]),
            "b1_b2_replay_eligible_component_count": len(context.components),
            "b1_b2_exclusions": dict(sorted(context.design_cell_exclusions.items())),
        },
        "epsilon_status_quarantine": epsilon,
        "risk_charge_contract": {
            "derivation": "MAX_CAUSAL_DECLARED_STOP_RISK_PER_MINI",
            "minimum_accepted_charge_usd": 1.0,
            "candidate_charges": charges,
            "epsilon_fallback_used": False,
            "future_outcomes_used": False,
        },
        "graph": {
            "nodes": nodes,
            "edges": edges,
            "node_count": len(nodes),
            "edge_count": len(edges),
            "selection_blocks": list(DESIGN_BLOCKS),
            "held_out_fields_used": False,
        },
        "selected_graph_proposals": proposals,
        "exact_design_results": design_results,
        "frozen_finalist_ids": finalist_ids,
        "frozen_finalist_hash": frozen_finalist_hash,
        "held_out_results": heldout_results,
        "qualified_policy_ids": [str(row["policy_id"]) for row in qualified],
        "qualified_policy_count": len(qualified),
        "best_held_out_policy_id": str(best["policy_id"]),
        "counts": {
            "graph_proposal_count": len(proposals),
            "exact_design_policy_count": len(design_results),
            "held_out_policy_count": len(heldout_results),
            "exact_account_episode_count": sum(
                int(row["episode_count"]) for row in design_results + heldout_results
            ),
            "registry_writes": 0,
            "database_writes": 0,
            "xfa_paths_started": 0,
            "confirmation_partition_reads": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "data_purchases": 0,
        },
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "independent_confirmation_claimed": False,
        "selection_contract": {
            "design_blocks": list(DESIGN_BLOCKS),
            "held_out_development_blocks": list(HELD_OUT_BLOCKS),
            "selection_completed_before_held_out_replay": True,
            "finalist_hash": frozen_finalist_hash,
        },
        "next_action": (
            "FREEZE_QUALIFIER_AND_RUN_REQUIRED_CONTROLS_WITHOUT_XFA"
            if qualified
            else "CLOSE_RISK_CORRECTED_Q_COMPLEMENTARITY_BRANCH_AND_REALLOCATE"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def verify_result(value: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(value)
    _verify_self_hash(row)
    if row.get("schema") != SCHEMA or row.get("evidence_role") != "VIEWED_DEVELOPMENT_ONLY":
        raise RiskCorrectedComplementarityError("unexpected graph result contract")
    counts = dict(row.get("counts") or {})
    for field in (
        "registry_writes",
        "database_writes",
        "xfa_paths_started",
        "confirmation_partition_reads",
        "q4_access_count_delta",
        "broker_connections",
        "orders",
        "data_purchases",
    ):
        if int(counts.get(field, -1)) != 0:
            raise RiskCorrectedComplementarityError("forbidden graph side effect")
    if row.get("independent_confirmation_claimed") is not False:
        raise RiskCorrectedComplementarityError("development evidence inflated")
    if dict(row["risk_charge_contract"]).get("epsilon_fallback_used") is not False:
        raise RiskCorrectedComplementarityError("epsilon risk fallback leaked")
    return row


__all__ = [
    "DEFAULT_MANIFEST",
    "MANIFEST_SCHEMA",
    "SCHEMA",
    "RiskCorrectedComplementarityError",
    "run",
    "verify_result",
]
