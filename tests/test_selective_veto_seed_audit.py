from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production.selective_veto_seed_audit import (
    PRIMARY_SEED_ID,
    SECONDARY_SEED_ID,
    SelectiveVetoSeedAuditError,
    audit_seed_robustness,
    write_seed_audit_checkpoint,
)


_ROLE_LAYOUT = (
    ("DISCOVERY", "2024-07-08", 10),
    ("DISCOVERY", "2024-07-09", 10),
    ("DISCOVERY", "2024-07-10", 10),
    ("VALIDATION", "2024-07-11", 18),
    ("FINAL_DEVELOPMENT", "2024-07-12", 24),
)

_EVIDENCE = {
    PRIMARY_SEED_ID: {
        "action": "A0_BASELINE_IMMEDIATE",
        "tier": 1.0,
        "roles": {
            "VALIDATION": (793.82, 776.32, 1120.58, 10, 4141.30),
            "FINAL_DEVELOPMENT": (1324.78, 1314.78, 499.28, 5, 4331.04),
        },
    },
    SECONDARY_SEED_ID: {
        "action": "A3_PULLBACK_MARKETABLE_LIMIT",
        "tier": 1.5,
        "roles": {
            "VALIDATION": (972.70, 930.70, 1274.96, 10, 3922.08),
            "FINAL_DEVELOPMENT": (1077.46, 1046.96, 231.46, 5, 4219.56),
        },
    },
}


def _execution(net: float, quantity: int, time_ns: int) -> dict[str, object]:
    if quantity <= 0:
        return {
            "quantity": 0,
            "net_pnl_usd": 0.0,
            "gross_pnl_usd": 0.0,
            "costs_usd": 0.0,
            "minimum_unrealized_pnl_usd": 0.0,
            "fill_time_ns": None,
            "exit_time_ns": None,
        }
    return {
        "quantity": quantity,
        "net_pnl_usd": net,
        "gross_pnl_usd": net + 2.0,
        "costs_usd": 2.0,
        "minimum_unrealized_pnl_usd": min(-10.0, net - 20.0),
        "fill_time_ns": time_ns,
        "exit_time_ns": time_ns + 10,
    }


def _outcome(
    *,
    opportunity_id: str,
    action: str,
    tier: float,
    role: str,
    session: str,
    ordinal: int,
    normal_net: float,
    stressed_net: float,
    baseline_normal: float,
    baseline_stressed: float,
) -> dict[str, object]:
    quantity = 0 if action == "A1_ABSTAIN" else max(1, round(4 * tier))
    body: dict[str, object] = {
        "opportunity_id": opportunity_id,
        "anchor_id": f"anchor_{ordinal % 4}",
        "mechanism": f"FAMILY_{ordinal % 3}",
        "market": "NQ" if ordinal % 2 == 0 else "YM",
        "execution_market": "MNQ" if ordinal % 2 == 0 else "MYM",
        "session_id": session,
        "role": role,
        "action_id": action,
        "risk_tier": tier,
        "joined_decision_time_ns": ordinal * 100,
        "normal": _execution(normal_net, quantity, ordinal * 100 + 1),
        "stressed": _execution(stressed_net, quantity, ordinal * 100 + 1),
        "baseline_normal_net_pnl": baseline_normal,
        "baseline_stressed_net_pnl": baseline_stressed,
        "normal_delta_vs_a0_usd": normal_net - baseline_normal,
        "stressed_delta_vs_a0_usd": stressed_net - baseline_stressed,
    }
    body["outcome_hash"] = stable_hash(body)
    return body


def _fixture() -> tuple[dict[str, object], dict[str, object]]:
    opportunities: list[tuple[str, str, str, int]] = []
    ordinal = 0
    for role, session, count in _ROLE_LAYOUT:
        for _ in range(count):
            opportunities.append((f"opp_{ordinal:03d}", role, session, ordinal))
            ordinal += 1

    lattice: dict[tuple[str, str, float], dict[str, object]] = {}
    selections: dict[str, list[dict[str, object]]] = {
        PRIMARY_SEED_ID: [],
        SECONDARY_SEED_ID: [],
    }
    policy_rows: list[dict[str, object]] = []

    for policy_id, evidence in _EVIDENCE.items():
        selected_by_role: dict[str, list[tuple[str, str, str, int]]] = {}
        for item in opportunities:
            selected_by_role.setdefault(item[1], []).append(item)
        for role, items in selected_by_role.items():
            role_evidence = evidence["roles"].get(role)
            if role_evidence is None:
                normal_total, stressed_total, uplift, abstain_count = 100.0, 90.0, 20.0, 3
            else:
                normal_total, stressed_total, uplift, abstain_count, _buffer = role_evidence
            active_count = len(items) - abstain_count
            for offset, (opportunity_id, _, session, index) in enumerate(items):
                abstain = offset < abstain_count
                action = "A1_ABSTAIN" if abstain else str(evidence["action"])
                tier = 0.0 if abstain else float(evidence["tier"])
                normal_net = 0.0 if abstain else normal_total / active_count
                stressed_net = 0.0 if abstain else stressed_total / active_count
                primary_a0 = lattice.get(
                    (opportunity_id, "A0_BASELINE_IMMEDIATE", 1.0)
                )
                if policy_id == SECONDARY_SEED_ID and primary_a0 is not None:
                    baseline_normal = float(primary_a0["normal"]["net_pnl_usd"])
                    baseline_stressed = float(
                        primary_a0["stressed"]["net_pnl_usd"]
                    )
                else:
                    baseline_normal = (
                        -uplift / abstain_count if abstain else normal_net
                    )
                    baseline_stressed = (
                        -uplift / abstain_count if abstain else stressed_net
                    )
                key = (opportunity_id, action, tier)
                row = lattice.get(key)
                if row is None or policy_id == SECONDARY_SEED_ID and not abstain:
                    row = _outcome(
                        opportunity_id=opportunity_id,
                        action=action,
                        tier=tier,
                        role=role,
                        session=session,
                        ordinal=index,
                        normal_net=normal_net,
                        stressed_net=stressed_net,
                        baseline_normal=baseline_normal,
                        baseline_stressed=baseline_stressed,
                    )
                    lattice[key] = row
                selections[policy_id].append(
                    {
                        "opportunity_id": opportunity_id,
                        "role": role,
                        "selected_action_id": action,
                        "selected_risk_tier": tier,
                        "outcome_hash": row["outcome_hash"],
                    }
                )

        # Add A0 and paired A3 1.00x references required by the audit.
        for opportunity_id, role, session, index in opportunities:
            selected = next(
                row
                for row in selections[policy_id]
                if row["opportunity_id"] == opportunity_id
            )
            chosen = next(
                row
                for row in lattice.values()
                if row["outcome_hash"] == selected["outcome_hash"]
            )
            baseline_normal = float(chosen["baseline_normal_net_pnl"])
            baseline_stressed = float(chosen["baseline_stressed_net_pnl"])
            a0_key = (opportunity_id, "A0_BASELINE_IMMEDIATE", 1.0)
            lattice.setdefault(
                a0_key,
                _outcome(
                    opportunity_id=opportunity_id,
                    action="A0_BASELINE_IMMEDIATE",
                    tier=1.0,
                    role=role,
                    session=session,
                    ordinal=index,
                    normal_net=baseline_normal,
                    stressed_net=baseline_stressed,
                    baseline_normal=baseline_normal,
                    baseline_stressed=baseline_stressed,
                ),
            )
            a3_key = (opportunity_id, "A3_PULLBACK_MARKETABLE_LIMIT", 1.0)
            lattice.setdefault(
                a3_key,
                _outcome(
                    opportunity_id=opportunity_id,
                    action="A3_PULLBACK_MARKETABLE_LIMIT",
                    tier=1.0,
                    role=role,
                    session=session,
                    ordinal=index,
                    normal_net=baseline_normal,
                    stressed_net=baseline_stressed,
                    baseline_normal=baseline_normal,
                    baseline_stressed=baseline_stressed,
                ),
            )

        role_results = {}
        paired = {}
        for role in ("VALIDATION", "FINAL_DEVELOPMENT"):
            normal, stressed, uplift, abstain, buffer = evidence["roles"][role]
            denominator = 18 if role == "VALIDATION" else 24
            role_results[role] = {
                "normal_account": {
                    "net_pnl_usd": normal,
                    "minimum_mll_buffer_usd": buffer + 2.5,
                },
                "stressed_account": {
                    "net_pnl_usd": stressed,
                    "minimum_mll_buffer_usd": buffer,
                },
            }
            paired[role] = {
                "paired_stressed_uplift_usd": uplift,
                "abstained_fraction": abstain / denominator,
                "avoiding_trade_usd": uplift,
            }
        policy_rows.append(
            {
                "policy_id": policy_id,
                "policy_fingerprint": stable_hash(policy_id),
                "active_action_id": evidence["action"],
                "active_risk_tier": evidence["tier"],
                "below_threshold_action": "A1_ABSTAIN",
                "quality_quantile": 0.35,
                "quality_threshold": -1.2,
                "selected_actions": selections[policy_id],
                "role_results": role_results,
                "paired_stressed_uplift_attribution": paired,
            }
        )

    pilot = {
        "campaign_id": "hydra_hybrid_structural_alpha_order_flow_0033",
        "policy_results": policy_rows,
        "compact_outputs": {
            "paired_counterfactual_outcomes": list(lattice.values())
        },
    }
    decision = {
        "campaign_id": "hydra_hybrid_structural_alpha_order_flow_0033",
        "decision_grade_status": "HYBRID_OVERLAY_WEAK",
        "retained_diagnostic_policies": [
            {"policy_id": PRIMARY_SEED_ID},
            {"policy_id": SECONDARY_SEED_ID},
        ],
    }
    return pilot, decision


def test_bounded_seed_audit_reconciles_and_reports_all_stresses() -> None:
    pilot, decision = _fixture()

    result = audit_seed_robustness(pilot, decision_report=decision)

    assert result["result"] == "SELECTIVE_VETO_SEED_ROBUST"
    assert result["market_replay_performed"] is False
    assert result["new_data_purchased"] is False
    assert len(result["seeds"]) == 2
    primary, secondary = result["seeds"]
    assert primary["policy_id"] == PRIMARY_SEED_ID
    assert primary["feature_dependency"]["minimum_complete_schema"] == "TBBO"
    assert primary["feature_dependency"]["primary_lane_action_supported"] is True
    assert secondary["feature_dependency"]["minimum_complete_schema"] == "MBP-1"
    assert secondary["feature_dependency"]["primary_lane_action_supported"] is False
    assert primary["leave_one_opportunity_out"]["result_count"] == 42
    assert [row["removed_trade_count"] for row in primary["top_trade_removal"]] == [1, 2, 3]
    assert {row["account_label"] for row in primary["account_size_matrix"]} == {
        "50K",
        "100K",
        "150K",
    }
    assert all(
        row["normal"]["p5_denominator"] == 1
        and row["normal"]["p10_status"] == "DATA_CENSORED"
        for row in primary["account_size_matrix"]
    )


def test_reported_seed_evidence_drift_fails_closed() -> None:
    pilot, decision = _fixture()
    pilot["policy_results"][0]["role_results"]["VALIDATION"]["normal_account"][
        "net_pnl_usd"
    ] += 1.0

    with pytest.raises(SelectiveVetoSeedAuditError, match="reported 0033 evidence drift"):
        audit_seed_robustness(pilot, decision_report=decision)


def test_selected_outcome_drift_fails_reconciliation() -> None:
    pilot, decision = _fixture()
    selected_hash = pilot["policy_results"][0]["selected_actions"][-1]["outcome_hash"]
    outcome = next(
        row
        for row in pilot["compact_outputs"]["paired_counterfactual_outcomes"]
        if row["outcome_hash"] == selected_hash
    )
    outcome["stressed"]["net_pnl_usd"] += 5.0

    with pytest.raises(SelectiveVetoSeedAuditError, match="do not reconcile"):
        audit_seed_robustness(pilot, decision_report=decision)


def test_immutable_checkpoint_refuses_divergent_overwrite(tmp_path: Path) -> None:
    pilot, decision = _fixture()
    result = audit_seed_robustness(pilot, decision_report=decision)
    path = tmp_path / "seed_audit.json"

    assert write_seed_audit_checkpoint(result, path) == path
    assert write_seed_audit_checkpoint(result, path) == path
    changed = copy.deepcopy(result)
    changed["result"] = "SELECTIVE_VETO_SEED_FALSIFIED"
    with pytest.raises(SelectiveVetoSeedAuditError, match="checkpoint differs"):
        write_seed_audit_checkpoint(changed, path)
    assert json.loads(path.read_text())["result"] == "SELECTIVE_VETO_SEED_ROBUST"
