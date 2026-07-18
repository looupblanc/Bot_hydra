"""Read-only robustness audit for the two frozen campaign-0033 seeds.

This module deliberately consumes the immutable compact evidence produced by
campaign 0033.  It does not replay market data, refit a threshold, or alter a
selected action.  Its only job is to stress the already selected paired
opportunity outcomes before campaign 0034 is allowed to estimate or purchase
additional data.

The primary 0034 action lane is narrower than the historical 0033 lattice:
``ABSTAIN``, ``TRADE_1X`` and ``TRADE_1_5X``.  The secondary seed is therefore
audited exactly as frozen, but is marked primary-lane-ineligible because it
contains the historical A3 execution action.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash


SEED_AUDIT_VERSION = "hydra_selective_veto_seed_robustness_audit_v1"
SEED_AUDIT_STATUSES = (
    "SELECTIVE_VETO_SEED_ROBUST",
    "SELECTIVE_VETO_SEED_FRAGILE",
    "SELECTIVE_VETO_SEED_FALSIFIED",
)
PRIMARY_SEED_ID = "hybrid_0033_01_f0345ecb99af8c25"
SECONDARY_SEED_ID = "hybrid_0033_07_5f93891cf737e51a"
HELDOUT_ROLES = ("VALIDATION", "FINAL_DEVELOPMENT")
PRIMARY_LANE_ACTIONS = frozenset(
    {"A0_BASELINE_IMMEDIATE", "A1_ABSTAIN"}
)


class SelectiveVetoSeedAuditError(RuntimeError):
    """The immutable 0033 material or the frozen 0034 audit contract drifted."""


@dataclass(frozen=True, slots=True)
class AccountRuleSnapshot:
    """Minimal versioned Combine snapshot needed by the seed audit."""

    account_label: str
    account_size_usd: float
    profit_target_usd: float
    maximum_loss_limit_usd: float
    maximum_micro_contracts: int
    consistency_limit: float = 0.50
    session_close_required: bool = True
    rule_version: str = "TOPSTEP_COMBINE_RESEARCH_SNAPSHOT_2026_07_15"

    def validate(self) -> None:
        if self.account_label not in {"50K", "100K", "150K"}:
            raise SelectiveVetoSeedAuditError("unsupported account-size label")
        if min(
            self.account_size_usd,
            self.profit_target_usd,
            self.maximum_loss_limit_usd,
            float(self.maximum_micro_contracts),
        ) <= 0.0:
            raise SelectiveVetoSeedAuditError("invalid account-rule snapshot")
        if not 0.0 < self.consistency_limit <= 1.0:
            raise SelectiveVetoSeedAuditError("invalid consistency limit")
        if not self.session_close_required or not self.rule_version:
            raise SelectiveVetoSeedAuditError("account snapshot is not frozen")

    @property
    def fingerprint(self) -> str:
        return stable_hash(asdict(self))


def default_account_rule_snapshots() -> tuple[AccountRuleSnapshot, ...]:
    """Return the three exact snapshots already used by the causal pilots."""

    return (
        AccountRuleSnapshot("50K", 50_000.0, 3_000.0, 2_000.0, 50),
        AccountRuleSnapshot("100K", 100_000.0, 6_000.0, 3_000.0, 100),
        AccountRuleSnapshot("150K", 150_000.0, 9_000.0, 4_500.0, 150),
    )


@dataclass(frozen=True, slots=True)
class SeedAuditConfig:
    campaign_id: str = "hydra_selective_order_flow_veto_expansion_0034"
    source_campaign_id: str = "hydra_hybrid_structural_alpha_order_flow_0033"
    seed_ids: tuple[str, str] = (PRIMARY_SEED_ID, SECONDARY_SEED_ID)
    heldout_roles: tuple[str, str] = HELDOUT_ROLES
    cost_stress_multiplier: float = 1.25
    maximum_single_opportunity_positive_profit_fraction: float = 0.25
    minimum_positive_context_count: int = 2
    account_rules: tuple[AccountRuleSnapshot, ...] = field(
        default_factory=default_account_rule_snapshots
    )

    def validate(self) -> None:
        if self.seed_ids != (PRIMARY_SEED_ID, SECONDARY_SEED_ID):
            raise SelectiveVetoSeedAuditError("0034 seed inventory drift")
        if self.heldout_roles != HELDOUT_ROLES:
            raise SelectiveVetoSeedAuditError("0034 held-out role contract drift")
        if not math.isclose(self.cost_stress_multiplier, 1.25):
            raise SelectiveVetoSeedAuditError("0034 cost-stress multiplier drift")
        if not math.isclose(
            self.maximum_single_opportunity_positive_profit_fraction, 0.25
        ):
            raise SelectiveVetoSeedAuditError("0034 concentration gate drift")
        labels = tuple(rule.account_label for rule in self.account_rules)
        if labels != ("50K", "100K", "150K"):
            raise SelectiveVetoSeedAuditError("0034 account-size frontier drift")
        for rule in self.account_rules:
            rule.validate()


_EXPECTED_EVIDENCE: Mapping[str, Mapping[str, Any]] = {
    PRIMARY_SEED_ID: {
        "active_action_id": "A0_BASELINE_IMMEDIATE",
        "active_risk_tier": 1.0,
        "deployability": "L1_DEPLOYABLE",
        "roles": {
            "VALIDATION": {
                "normal_net": 793.82,
                "stressed_net": 776.32,
                "paired_stressed_uplift": 1120.58,
                "abstained_fraction": 0.5555555555555556,
                "minimum_mll_buffer": 4141.30,
            },
            "FINAL_DEVELOPMENT": {
                "normal_net": 1324.78,
                "stressed_net": 1314.78,
                "paired_stressed_uplift": 499.28,
                "abstained_fraction": 0.20833333333333334,
                "minimum_mll_buffer": 4331.04,
            },
        },
    },
    SECONDARY_SEED_ID: {
        "active_action_id": "A3_PULLBACK_MARKETABLE_LIMIT",
        "active_risk_tier": 1.5,
        "deployability": "L1_DEPLOYABLE_OR_L2_DEPLOYABLE",
        "roles": {
            "VALIDATION": {
                "normal_net": 972.70,
                "stressed_net": 930.70,
                "paired_stressed_uplift": 1274.96,
                "abstained_fraction": 0.5555555555555556,
                "minimum_mll_buffer": 3922.08,
            },
            "FINAL_DEVELOPMENT": {
                "normal_net": 1077.46,
                "stressed_net": 1046.96,
                "paired_stressed_uplift": 231.46,
                "abstained_fraction": 0.20833333333333334,
                "minimum_mll_buffer": 4219.56,
            },
        },
    },
}


def _read_json(path: str | Path) -> dict[str, Any]:
    source = Path(path).resolve()
    if not source.is_file():
        raise SelectiveVetoSeedAuditError(f"required 0033 source absent: {source}")
    value = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SelectiveVetoSeedAuditError("0033 source must be a JSON object")
    return value


def _close(left: Any, right: Any, *, tolerance: float = 0.011) -> bool:
    return math.isclose(
        float(left), float(right), rel_tol=1e-10, abs_tol=tolerance
    )


def _validate_reported_seed(policy: Mapping[str, Any]) -> dict[str, Any]:
    policy_id = str(policy.get("policy_id") or "")
    expected = _EXPECTED_EVIDENCE.get(policy_id)
    if expected is None:
        raise SelectiveVetoSeedAuditError(f"unexpected seed policy: {policy_id}")
    if (
        policy.get("active_action_id") != expected["active_action_id"]
        or not _close(policy.get("active_risk_tier"), expected["active_risk_tier"])
        or policy.get("below_threshold_action") != "A1_ABSTAIN"
    ):
        raise SelectiveVetoSeedAuditError(f"frozen action mapping drift: {policy_id}")
    role_results = policy.get("role_results") or {}
    attribution = policy.get("paired_stressed_uplift_attribution") or {}
    verified: dict[str, Any] = {}
    for role, role_expected in expected["roles"].items():
        result = role_results.get(role) or {}
        normal = result.get("normal_account") or {}
        stressed = result.get("stressed_account") or {}
        paired = attribution.get(role) or {}
        actual = {
            "normal_net": normal.get("net_pnl_usd"),
            "stressed_net": stressed.get("net_pnl_usd"),
            "paired_stressed_uplift": paired.get("paired_stressed_uplift_usd"),
            "abstained_fraction": paired.get("abstained_fraction"),
            "minimum_mll_buffer": stressed.get("minimum_mll_buffer_usd"),
        }
        if any(
            not _close(actual[name], expected_value)
            for name, expected_value in role_expected.items()
        ):
            raise SelectiveVetoSeedAuditError(
                f"reported 0033 evidence drift for {policy_id}/{role}"
            )
        verified[role] = actual
    return {
        "reported_evidence_exact": True,
        "deployability": expected["deployability"],
        "role_results": verified,
    }


def _selected_rows(
    policy: Mapping[str, Any],
    outcomes_by_hash: Mapping[str, Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    selected: list[Mapping[str, Any]] = []
    opportunities: set[str] = set()
    for selection in policy.get("selected_actions") or ():
        digest = str(selection.get("outcome_hash") or "")
        row = outcomes_by_hash.get(digest)
        if row is None:
            raise SelectiveVetoSeedAuditError("seed selection outcome is absent")
        if (
            row.get("opportunity_id") != selection.get("opportunity_id")
            or row.get("role") != selection.get("role")
            or row.get("action_id") != selection.get("selected_action_id")
            or not _close(
                row.get("risk_tier"), selection.get("selected_risk_tier"), tolerance=1e-12
            )
        ):
            raise SelectiveVetoSeedAuditError("seed selection/outcome mismatch")
        opportunity_id = str(row["opportunity_id"])
        if opportunity_id in opportunities:
            raise SelectiveVetoSeedAuditError("duplicate selected structural opportunity")
        opportunities.add(opportunity_id)
        selected.append(row)
    selected.sort(
        key=lambda row: (
            str(row["session_id"]),
            int(row.get("joined_decision_time_ns", 0)),
            str(row["opportunity_id"]),
        )
    )
    roles = {role: sum(row["role"] == role for row in selected) for role in (
        "DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT"
    )}
    if roles != {"DISCOVERY": 30, "VALIDATION": 18, "FINAL_DEVELOPMENT": 24}:
        raise SelectiveVetoSeedAuditError("0033 seed opportunity denominator drift")
    return selected


def _validate_selected_reconciliation(
    selected: Sequence[Mapping[str, Any]],
    verified: Mapping[str, Any],
) -> None:
    """Prove that selected compact rows reproduce every reported held-out total."""

    for role in HELDOUT_ROLES:
        rows = [row for row in selected if row["role"] == role]
        expected = verified["role_results"][role]
        actual = {
            "normal_net": math.fsum(
                float(_scenario(row, "normal")["net_pnl_usd"]) for row in rows
            ),
            "stressed_net": math.fsum(
                float(_scenario(row, "stressed")["net_pnl_usd"]) for row in rows
            ),
            "paired_stressed_uplift": math.fsum(
                float(row["stressed_delta_vs_a0_usd"]) for row in rows
            ),
            "abstained_fraction": (
                sum(row["action_id"] == "A1_ABSTAIN" for row in rows) / len(rows)
                if rows
                else 0.0
            ),
        }
        if any(
            not _close(actual[name], expected[name])
            for name in actual
        ):
            raise SelectiveVetoSeedAuditError(
                f"selected compact rows do not reconcile with {role} evidence"
            )


def _scenario(row: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = row.get(name)
    if not isinstance(value, Mapping):
        raise SelectiveVetoSeedAuditError(f"missing {name} scenario")
    return value


def _stressed_125_net(row: Mapping[str, Any], multiplier: float) -> float:
    """Apply an explicit conservative 1.25x increment to stored stressed cost.

    The compact 0033 record embeds adverse fill slippage in gross PnL and keeps
    commission in ``costs_usd``.  It has no zero-slippage oracle.  The all-in
    proxy is therefore the explicit stressed cost plus the absolute observed
    normal-to-stressed net degradation.  Multiplying that non-negative proxy
    avoids granting a credit in rare paths where shifted exits made the
    stressed scenario look better.
    """

    normal = _scenario(row, "normal")
    stressed = _scenario(row, "stressed")
    stress_cost_proxy = float(stressed["costs_usd"]) + abs(
        float(normal["net_pnl_usd"]) - float(stressed["net_pnl_usd"])
    )
    return float(stressed["net_pnl_usd"]) - (multiplier - 1.0) * stress_cost_proxy


def _summarize_rows(
    rows: Sequence[Mapping[str, Any]], *, cost_stress_multiplier: float
) -> dict[str, Any]:
    positive = [
        max(0.0, float(_scenario(row, "stressed")["net_pnl_usd"]))
        for row in rows
    ]
    positive_total = float(math.fsum(positive))
    maximum_positive = max(positive, default=0.0)
    normal_net = float(
        math.fsum(float(_scenario(row, "normal")["net_pnl_usd"]) for row in rows)
    )
    stressed_net = float(
        math.fsum(float(_scenario(row, "stressed")["net_pnl_usd"]) for row in rows)
    )
    explicit_normal_costs = float(
        math.fsum(float(_scenario(row, "normal")["costs_usd"]) for row in rows)
    )
    explicit_stressed_costs = float(
        math.fsum(float(_scenario(row, "stressed")["costs_usd"]) for row in rows)
    )
    return {
        "opportunity_count": len(rows),
        "normal_fill_count": sum(
            int(_scenario(row, "normal")["quantity"]) > 0 for row in rows
        ),
        "stressed_fill_count": sum(
            int(_scenario(row, "stressed")["quantity"]) > 0 for row in rows
        ),
        "abstained_count": sum(row["action_id"] == "A1_ABSTAIN" for row in rows),
        "normal_net_usd": normal_net,
        "stressed_net_usd": stressed_net,
        "stressed_costs_1_25x_net_usd": float(
            math.fsum(
                _stressed_125_net(row, cost_stress_multiplier) for row in rows
            )
        ),
        "paired_normal_uplift_usd": float(
            math.fsum(float(row["normal_delta_vs_a0_usd"]) for row in rows)
        ),
        "paired_stressed_uplift_usd": float(
            math.fsum(float(row["stressed_delta_vs_a0_usd"]) for row in rows)
        ),
        "normal_explicit_costs_usd": explicit_normal_costs,
        "stressed_explicit_costs_usd": explicit_stressed_costs,
        "normal_to_stressed_net_degradation_usd": normal_net - stressed_net,
        "positive_stressed_profit_usd": positive_total,
        "largest_positive_opportunity_usd": maximum_positive,
        "largest_positive_opportunity_fraction": (
            maximum_positive / positive_total if positive_total > 0.0 else 0.0
        ),
    }


def _group_attribution(
    rows: Sequence[Mapping[str, Any]],
    key: str,
    *,
    multiplier: float,
) -> list[dict[str, Any]]:
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row[key]), []).append(row)
    return [
        {
            key: group,
            **_summarize_rows(values, cost_stress_multiplier=multiplier),
        }
        for group, values in sorted(groups.items())
    ]


def _leave_one_out(
    rows: Sequence[Mapping[str, Any]], *, multiplier: float
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for omitted in rows:
        retained = [
            row for row in rows if row["opportunity_id"] != omitted["opportunity_id"]
        ]
        results.append(
            {
                "omitted_opportunity_id": omitted["opportunity_id"],
                "omitted_stressed_net_usd": float(
                    _scenario(omitted, "stressed")["net_pnl_usd"]
                ),
                **_summarize_rows(retained, cost_stress_multiplier=multiplier),
            }
        )
    stressed = [float(row["stressed_net_usd"]) for row in results]
    return {
        "result_count": len(results),
        "minimum_remaining_stressed_net_usd": min(stressed, default=0.0),
        "all_remaining_stressed_positive": bool(stressed) and min(stressed) > 0.0,
        "results": results,
    }


def _top_trade_removal(
    rows: Sequence[Mapping[str, Any]], *, multiplier: float
) -> list[dict[str, Any]]:
    executed = sorted(
        (
            row
            for row in rows
            if int(_scenario(row, "stressed")["quantity"]) > 0
        ),
        key=lambda row: (
            float(_scenario(row, "stressed")["net_pnl_usd"]),
            str(row["opportunity_id"]),
        ),
        reverse=True,
    )
    results: list[dict[str, Any]] = []
    for count in (1, 2, 3):
        omitted = executed[:count]
        omitted_ids = {str(row["opportunity_id"]) for row in omitted}
        retained = [row for row in rows if row["opportunity_id"] not in omitted_ids]
        results.append(
            {
                "removed_trade_count": len(omitted),
                "removed_opportunity_ids": sorted(omitted_ids),
                "removed_stressed_net_usd": float(
                    math.fsum(
                        float(_scenario(row, "stressed")["net_pnl_usd"])
                        for row in omitted
                    )
                ),
                **_summarize_rows(retained, cost_stress_multiplier=multiplier),
            }
        )
    return results


def _leave_one_family_out(
    rows: Sequence[Mapping[str, Any]], *, multiplier: float
) -> dict[str, Any]:
    families = sorted({str(row["mechanism"]) for row in rows})
    results = []
    for family in families:
        removed = [row for row in rows if row["mechanism"] == family]
        retained = [row for row in rows if row["mechanism"] != family]
        results.append(
            {
                "omitted_anchor_family": family,
                "omitted_opportunity_count": len(removed),
                "omitted_stressed_net_usd": float(
                    math.fsum(
                        float(_scenario(row, "stressed")["net_pnl_usd"])
                        for row in removed
                    )
                ),
                **_summarize_rows(retained, cost_stress_multiplier=multiplier),
            }
        )
    stressed = [float(row["stressed_net_usd"]) for row in results]
    return {
        "family_count": len(families),
        "minimum_remaining_stressed_net_usd": min(stressed, default=0.0),
        "positive_after_every_family_removal": bool(stressed) and min(stressed) > 0.0,
        "results": results,
    }


def _feature_dependency(policy: Mapping[str, Any]) -> dict[str, Any]:
    dependencies = [
        {"feature": "flow_2s", "minimum_schema": "TRADES_ONLY"},
        {"feature": "flow_30s", "minimum_schema": "TRADES_ONLY"},
        {"feature": "bbo_imbalance", "minimum_schema": "TBBO"},
        {"feature": "microprice_deviation", "minimum_schema": "TBBO"},
        {"feature": "spread_ticks", "minimum_schema": "TBBO"},
    ]
    action = str(policy["active_action_id"])
    if action == "A3_PULLBACK_MARKETABLE_LIMIT":
        dependencies.append(
            {
                "feature": "quote_update_path_within_pullback_window",
                "minimum_schema": "MBP-1",
            }
        )
    schemas = {str(row["minimum_schema"]) for row in dependencies}
    return {
        "features": dependencies,
        "trades_only_feature_count": sum(
            row["minimum_schema"] == "TRADES_ONLY" for row in dependencies
        ),
        "tbbo_feature_count": sum(
            row["minimum_schema"] == "TBBO" for row in dependencies
        ),
        "mbp_1_feature_count": sum(
            row["minimum_schema"] == "MBP-1" for row in dependencies
        ),
        "mbo_teacher_only_feature_count": 0,
        "minimum_complete_schema": "MBP-1" if "MBP-1" in schemas else "TBBO",
        "hard_deployability_defect": False,
        "primary_lane_action_supported": action in PRIMARY_LANE_ACTIONS,
    }


def _account_path(
    rows: Sequence[Mapping[str, Any]],
    scenario: str,
    rule: AccountRuleSnapshot,
) -> dict[str, Any]:
    sessions = sorted({str(row["session_id"]) for row in rows})
    timeline: list[tuple[int, int, Mapping[str, Any]]] = []
    contract_violations: list[str] = []
    for row in rows:
        execution = _scenario(row, scenario)
        quantity = int(execution["quantity"])
        if quantity <= 0:
            continue
        if quantity > rule.maximum_micro_contracts:
            contract_violations.append(str(row["opportunity_id"]))
            continue
        fill_time = execution.get("fill_time_ns")
        exit_time = execution.get("exit_time_ns")
        if fill_time is None or exit_time is None:
            raise SelectiveVetoSeedAuditError("filled seed action lacks timestamps")
        timeline.append((int(fill_time), 1, row))
        timeline.append((int(exit_time), 0, row))
    timeline.sort(key=lambda value: (value[0], value[1], str(value[2]["opportunity_id"])))

    realized = 0.0
    trailing_high = 0.0
    minimum_buffer = rule.maximum_loss_limit_usd
    active_unrealized: dict[str, float] = {}
    daily = {session: 0.0 for session in sessions}
    mll_breached = False
    first_target_session: str | None = None
    for _timestamp, event_type, row in timeline:
        execution = _scenario(row, scenario)
        opportunity_id = str(row["opportunity_id"])
        if event_type == 0:
            active_unrealized.pop(opportunity_id, None)
            net = float(execution["net_pnl_usd"])
            realized += net
            daily[str(row["session_id"])] += net
            if realized >= rule.profit_target_usd and first_target_session is None:
                first_target_session = str(row["session_id"])
        else:
            active_unrealized[opportunity_id] = float(
                execution["minimum_unrealized_pnl_usd"]
            )
        equity = realized + float(math.fsum(active_unrealized.values()))
        trailing_high = max(trailing_high, realized, equity)
        buffer = equity - (trailing_high - rule.maximum_loss_limit_usd)
        minimum_buffer = min(minimum_buffer, buffer)
        mll_breached = mll_breached or buffer < -1e-9

    best_day = max((max(value, 0.0) for value in daily.values()), default=0.0)
    consistency_ratio = best_day / realized if realized > 0.0 else None
    consistency_ok = (
        consistency_ratio is not None
        and consistency_ratio <= rule.consistency_limit + 1e-12
    )
    target_reached = first_target_session is not None
    days_to_target = (
        None
        if first_target_session is None
        else 1 + sum(session < first_target_session for session in sessions)
    )
    average_daily = realized / len(sessions) if sessions else 0.0
    projected = (
        rule.profit_target_usd / average_daily if average_daily > 0.0 else None
    )
    return {
        "account_label": rule.account_label,
        "rule_version": rule.rule_version,
        "rule_fingerprint": rule.fingerprint,
        "scenario": scenario.upper(),
        "full_coverage_session_count": len(sessions),
        "p5_full_coverage": len(sessions) >= 5,
        "p5_pass_count": int(
            len(sessions) >= 5
            and target_reached
            and consistency_ok
            and not mll_breached
            and not contract_violations
        ),
        "p5_denominator": int(len(sessions) >= 5),
        "p10_status": "FULL_COVERAGE" if len(sessions) >= 10 else "DATA_CENSORED",
        "p10_pass_count": (
            int(target_reached and consistency_ok and not mll_breached)
            if len(sessions) >= 10
            else None
        ),
        "p10_denominator": int(len(sessions) >= 10),
        "net_pnl_usd": realized,
        "target_progress": realized / rule.profit_target_usd,
        "target_reached": target_reached,
        "empirical_days_to_target": days_to_target,
        "projected_days_to_target": projected,
        "projection_is_pass_evidence": False,
        "mll_breached": mll_breached,
        "minimum_mll_buffer_usd": minimum_buffer,
        "consistency_ratio": consistency_ratio,
        "consistency_compliant": consistency_ok,
        "maximum_micro_contracts": rule.maximum_micro_contracts,
        "contract_limit_violation_count": len(contract_violations),
        "contract_limit_violation_opportunity_ids": contract_violations,
        "daily_path": [
            {"session_id": session, "net_pnl_usd": value}
            for session, value in sorted(daily.items())
        ],
    }


def _account_size_matrix(
    rows: Sequence[Mapping[str, Any]], rules: Sequence[AccountRuleSnapshot]
) -> list[dict[str, Any]]:
    return [
        {
            "account_label": rule.account_label,
            "normal": _account_path(rows, "normal", rule),
            "stressed": _account_path(rows, "stressed", rule),
        }
        for rule in rules
    ]


def _risk_tier_attribution(
    rows: Sequence[Mapping[str, Any]],
    outcomes_by_key: Mapping[tuple[str, str, float], Mapping[str, Any]],
) -> dict[str, Any]:
    selected_15 = [
        row
        for row in rows
        if math.isclose(float(row["risk_tier"]), 1.5, abs_tol=1e-12)
        and row["action_id"] != "A1_ABSTAIN"
    ]
    delta = 0.0
    missing: list[str] = []
    for row in selected_15:
        unit = outcomes_by_key.get(
            (str(row["opportunity_id"]), str(row["action_id"]), 1.0)
        )
        if unit is None:
            missing.append(str(row["opportunity_id"]))
            continue
        delta += float(_scenario(row, "stressed")["net_pnl_usd"]) - float(
            _scenario(unit, "stressed")["net_pnl_usd"]
        )
    if missing:
        raise SelectiveVetoSeedAuditError("1.50x seed lacks paired 1.00x reference")
    return {
        "selected_1_5x_opportunity_count": len(selected_15),
        "paired_stressed_risk_1_5x_contribution_usd": float(delta),
    }


def audit_seed_robustness(
    pilot: Mapping[str, Any],
    *,
    decision_report: Mapping[str, Any] | None = None,
    config: SeedAuditConfig | None = None,
) -> dict[str, Any]:
    """Audit both frozen seeds without opening market data or fitting a model."""

    cfg = config or SeedAuditConfig()
    cfg.validate()
    if pilot.get("campaign_id") != cfg.source_campaign_id:
        raise SelectiveVetoSeedAuditError("0033 source campaign identity drift")
    if decision_report is not None:
        if (
            decision_report.get("campaign_id") != cfg.source_campaign_id
            or decision_report.get("decision_grade_status") != "HYBRID_OVERLAY_WEAK"
        ):
            raise SelectiveVetoSeedAuditError("0033 terminal decision drift")
        retained = {
            str(row.get("policy_id"))
            for row in decision_report.get("retained_diagnostic_policies") or ()
        }
        if retained != set(cfg.seed_ids):
            raise SelectiveVetoSeedAuditError("0033 retained seed inventory drift")

    outcomes = list(
        (pilot.get("compact_outputs") or {}).get("paired_counterfactual_outcomes")
        or ()
    )
    if not outcomes:
        raise SelectiveVetoSeedAuditError("0033 paired counterfactual ledger absent")
    outcomes_by_hash: dict[str, Mapping[str, Any]] = {}
    outcomes_by_key: dict[tuple[str, str, float], Mapping[str, Any]] = {}
    for row in outcomes:
        digest = str(row.get("outcome_hash") or "")
        key = (
            str(row.get("opportunity_id") or ""),
            str(row.get("action_id") or ""),
            round(float(row.get("risk_tier", -1.0)), 8),
        )
        if not digest or digest in outcomes_by_hash or key in outcomes_by_key:
            raise SelectiveVetoSeedAuditError("0033 paired outcome identity collision")
        outcomes_by_hash[digest] = row
        outcomes_by_key[key] = row
    policies_by_id = {
        str(row.get("policy_id") or ""): row
        for row in pilot.get("policy_results") or ()
    }
    if any(seed_id not in policies_by_id for seed_id in cfg.seed_ids):
        raise SelectiveVetoSeedAuditError("frozen 0033 seed policy absent")

    audited: list[dict[str, Any]] = []
    for seed_id in cfg.seed_ids:
        policy = policies_by_id[seed_id]
        verified = _validate_reported_seed(policy)
        selected = _selected_rows(policy, outcomes_by_hash)
        _validate_selected_reconciliation(selected, verified)
        heldout = [row for row in selected if row["role"] in cfg.heldout_roles]
        summary = _summarize_rows(
            heldout, cost_stress_multiplier=cfg.cost_stress_multiplier
        )
        market = _group_attribution(
            heldout,
            "market",
            multiplier=cfg.cost_stress_multiplier,
        )
        family = _group_attribution(
            heldout,
            "mechanism",
            multiplier=cfg.cost_stress_multiplier,
        )
        session = _group_attribution(
            heldout,
            "session_id",
            multiplier=cfg.cost_stress_multiplier,
        )
        opportunity = [
            {
                "opportunity_id": row["opportunity_id"],
                "market": row["market"],
                "anchor_family": row["mechanism"],
                "session_id": row["session_id"],
                "role": row["role"],
                "selected_action": row["action_id"],
                "risk_tier": row["risk_tier"],
                **_summarize_rows(
                    [row], cost_stress_multiplier=cfg.cost_stress_multiplier
                ),
            }
            for row in heldout
        ]
        loo = _leave_one_out(heldout, multiplier=cfg.cost_stress_multiplier)
        top = _top_trade_removal(heldout, multiplier=cfg.cost_stress_multiplier)
        loaf = _leave_one_family_out(heldout, multiplier=cfg.cost_stress_multiplier)
        features = _feature_dependency(policy)
        positive_contexts = max(
            sum(float(row["stressed_net_usd"]) > 0.0 for row in family),
            sum(float(row["stressed_net_usd"]) > 0.0 for row in session),
        )
        best_trade_positive = bool(top) and float(top[0]["stressed_net_usd"]) > 0.0
        concentration_safe = (
            float(summary["largest_positive_opportunity_fraction"])
            <= cfg.maximum_single_opportunity_positive_profit_fraction + 1e-12
        )
        cost_stress_positive = float(summary["stressed_costs_1_25x_net_usd"]) > 0.0
        deployable = not bool(features["hard_deployability_defect"])
        criteria = {
            "positive_stressed_after_best_trade_removal": best_trade_positive,
            "positive_context_count": positive_contexts,
            "multiple_positive_contexts": positive_contexts
            >= cfg.minimum_positive_context_count,
            "single_opportunity_profit_concentration_safe": concentration_safe,
            "no_hard_data_or_deployability_defect": deployable,
            "positive_under_1_25x_stressed_cost_proxy": cost_stress_positive,
            "primary_lane_action_supported": bool(
                features["primary_lane_action_supported"]
            ),
        }
        required = (
            criteria["positive_stressed_after_best_trade_removal"]
            and criteria["multiple_positive_contexts"]
            and criteria["single_opportunity_profit_concentration_safe"]
            and criteria["no_hard_data_or_deployability_defect"]
            and criteria["positive_under_1_25x_stressed_cost_proxy"]
        )
        if required:
            status = "SELECTIVE_VETO_SEED_ROBUST"
        elif float(summary["stressed_net_usd"]) > 0.0 or float(
            summary["paired_stressed_uplift_usd"]
        ) > 0.0:
            status = "SELECTIVE_VETO_SEED_FRAGILE"
        else:
            status = "SELECTIVE_VETO_SEED_FALSIFIED"
        audited.append(
            {
                "policy_id": seed_id,
                "assigned_status": "SELECTIVE_VETO_DIAGNOSTIC_SEED",
                "robustness_status": status,
                "frozen_policy": {
                    "policy_fingerprint": policy["policy_fingerprint"],
                    "active_action_id": policy["active_action_id"],
                    "active_risk_tier": policy["active_risk_tier"],
                    "below_threshold_action": policy["below_threshold_action"],
                    "quality_quantile": policy["quality_quantile"],
                    "quality_threshold": policy["quality_threshold"],
                },
                "reported_evidence_verification": verified,
                "heldout_summary": summary,
                "market_attribution": market,
                "anchor_family_attribution": family,
                "session_attribution": session,
                "opportunity_attribution": opportunity,
                "abstention_contribution": {
                    role: policy["paired_stressed_uplift_attribution"][role][
                        "avoiding_trade_usd"
                    ]
                    for role in cfg.heldout_roles
                },
                "risk_tier_contribution": _risk_tier_attribution(
                    heldout, outcomes_by_key
                ),
                "leave_one_opportunity_out": loo,
                "top_trade_removal": top,
                "leave_one_anchor_family_out": loaf,
                "feature_dependency": features,
                "account_size_matrix": _account_size_matrix(
                    selected, cfg.account_rules
                ),
                "robustness_criteria": criteria,
            }
        )

    statuses = [str(row["robustness_status"]) for row in audited]
    if "SELECTIVE_VETO_SEED_ROBUST" in statuses:
        overall = "SELECTIVE_VETO_SEED_ROBUST"
    elif "SELECTIVE_VETO_SEED_FRAGILE" in statuses:
        overall = "SELECTIVE_VETO_SEED_FRAGILE"
    else:
        overall = "SELECTIVE_VETO_SEED_FALSIFIED"
    result = {
        "schema": "hydra_selective_veto_seed_audit_v1",
        "audit_version": SEED_AUDIT_VERSION,
        "campaign_id": cfg.campaign_id,
        "source_campaign_id": cfg.source_campaign_id,
        "source_decision_status": "HYBRID_OVERLAY_WEAK",
        "source_evidence_fingerprint": stable_hash(
            {
                "source_campaign_id": cfg.source_campaign_id,
                "seed_policy_fingerprints": {
                    seed_id: policies_by_id[seed_id]["policy_fingerprint"]
                    for seed_id in cfg.seed_ids
                },
                "paired_outcome_hashes": sorted(outcomes_by_hash),
            }
        ),
        "result": overall,
        "seed_count": len(audited),
        "seeds": audited,
        "cost_stress_contract": {
            "multiplier": cfg.cost_stress_multiplier,
            "formula": (
                "stressed_net - 0.25 * "
                "(explicit_stressed_cost + abs(normal_net-stressed_net))"
            ),
            "reason": "compact evidence has no zero-slippage oracle",
            "pessimistic_non_negative_cost_increment": True,
        },
        "targeted_cost_matrix_authorized": overall
        != "SELECTIVE_VETO_SEED_FALSIFIED",
        "data_purchase_authorized_by_audit": False,
        "data_purchase_requires_separate_frozen_cost_decision": True,
        "market_replay_performed": False,
        "policy_refit_performed": False,
        "new_data_accessed": False,
        "new_data_purchased": False,
        "q4_accessed": False,
        "broker_connections": 0,
        "orders": 0,
    }
    result["audit_fingerprint"] = stable_hash(result)
    return result


def run_seed_robustness_audit(
    pilot_summary_path: str | Path,
    decision_report_path: str | Path,
    *,
    config: SeedAuditConfig | None = None,
) -> dict[str, Any]:
    """Load immutable 0033 JSON and return the bounded read-only audit."""

    return audit_seed_robustness(
        _read_json(pilot_summary_path),
        decision_report=_read_json(decision_report_path),
        config=config,
    )


def write_seed_audit_checkpoint(
    result: Mapping[str, Any], output_path: str | Path
) -> Path:
    """Write one immutable seed-audit checkpoint, refusing divergent overwrite."""

    target = Path(output_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        dict(result), sort_keys=True, separators=(",", ":"), allow_nan=False
    ) + "\n"
    if target.exists():
        if target.read_text(encoding="utf-8") != encoded:
            raise SelectiveVetoSeedAuditError(
                f"immutable seed-audit checkpoint differs: {target}"
            )
        return target
    target.write_text(encoded, encoding="utf-8")
    return target


__all__ = [
    "AccountRuleSnapshot",
    "PRIMARY_SEED_ID",
    "SECONDARY_SEED_ID",
    "SEED_AUDIT_STATUSES",
    "SEED_AUDIT_VERSION",
    "SeedAuditConfig",
    "SelectiveVetoSeedAuditError",
    "audit_seed_robustness",
    "default_account_rule_snapshots",
    "run_seed_robustness_audit",
    "write_seed_audit_checkpoint",
]
