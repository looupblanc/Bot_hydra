"""Preregistered admission of at most one post-mutation shadow candidate.

This module is intentionally disconnected from mission state and activation.
It verifies immutable halving artefacts, applies the frozen twelve-gate policy,
and can emit one zero-order ``SHADOW_RESEARCH_CANDIDATE`` configuration.  The
existing generic activation workflow remains the only activation authority.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any, Iterable, Mapping

from hydra.shadow.prior_trade_guard import PriorTradeGuardSpecification
from hydra.shadow.specification import ShadowSpecification


POLICY_VERSION = "hydra_post_mutation_shadow_admission_v1"
PREREGISTRATION_SHA256 = "d18078d3a45bbcd18a96f5623ad46cdbfa70ec08813852e8530ca1a763e540b6"
MAXIMUM_ADMISSIONS = 1
SOURCE_STATUS = "PROMISING_RESEARCH_CANDIDATE"
OUTPUT_STATUS = "SHADOW_RESEARCH_CANDIDATE"
OBJECTIVE_POOL = "COMBINE_PASSER_POOL"


class PostMutationShadowAdmissionError(RuntimeError):
    """A frozen input or admission invariant was violated."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _verify(path: Path, expected_sha256: str, label: str) -> None:
    if not path.is_file():
        raise PostMutationShadowAdmissionError(f"Missing frozen {label}: {path}")
    actual = _sha256(path)
    if actual != str(expected_sha256):
        raise PostMutationShadowAdmissionError(
            f"Frozen {label} hash drift: expected {expected_sha256}, observed {actual}"
        )


def _write_immutable(path: Path, content: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != content:
            raise PostMutationShadowAdmissionError(
                f"Refusing divergent immutable admission artifact: {path}"
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PostMutationShadowAdmissionError(f"Invalid frozen {label} JSON") from exc
    if not isinstance(value, dict):
        raise PostMutationShadowAdmissionError(f"Frozen {label} is not an object")
    return value


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            if not raw.strip():
                continue
            try:
                row = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise PostMutationShadowAdmissionError(
                    f"Invalid candidate evidence JSONL at line {line_number}"
                ) from exc
            if not isinstance(row, dict):
                raise PostMutationShadowAdmissionError(
                    f"Non-object candidate evidence at line {line_number}"
                )
            rows.append(row)
    if not rows:
        raise PostMutationShadowAdmissionError("Candidate evidence is empty")
    return rows


def _load_shadow_specification(path: Path) -> ShadowSpecification:
    payload = _load_json(path, "parent shadow configuration")
    supplied_hash = str(payload.pop("configuration_hash", ""))
    for field in ("feature_versions", "markets", "timeframes", "kill_conditions"):
        if field in payload:
            payload[field] = tuple(payload[field])
    try:
        specification = ShadowSpecification(**payload)
        specification.validate()
    except (TypeError, ValueError) as exc:
        raise PostMutationShadowAdmissionError(
            "Parent shadow configuration is not a valid fail-closed specification"
        ) from exc
    if supplied_hash != specification.configuration_hash:
        raise PostMutationShadowAdmissionError("Parent configuration semantic hash drift")
    return specification


def _parent_source_coverage(
    source: Mapping[str, Any], *, source_sha256: str
) -> dict[str, dict[str, Any]]:
    """Prove full registered-development coverage for source parent strategies.

    Merely observing a 2023 trade is not evidence that 2023 was replayed.  This
    audit therefore binds the child to the frozen parent result, its explicit
    data-access period, data fingerprint, roll map and no-lookahead proof.
    """

    if str(source.get("schema") or "") != "equity_open_gap_continuation_pilot_v1":
        raise PostMutationShadowAdmissionError("Unexpected frozen parent-result schema")
    access = dict(source.get("data_access_record") or {})
    provenance = dict(source.get("data_provenance") or {})
    integrity = dict(source.get("integrity_proof") or {})
    if (
        str(access.get("data_role") or "") != "DEVELOPMENT"
        or str(access.get("period_accessed") or "") != "2023-01-01:2024-10-01"
        or str(provenance.get("period_start") or "") != "2023-01-01"
        or str(provenance.get("period_end_exclusive") or "") != "2024-10-01"
    ):
        raise PostMutationShadowAdmissionError(
            "Frozen parent result does not prove complete registered development coverage"
        )
    required_integrity = {
        "decision_after_source_close",
        "exact_future_horizon",
        "finite_primary_costs",
        "nonempty_event_source",
        "one_event_per_market_session",
        "past_only_threshold",
        "q4_excluded",
        "reference_strictly_past",
        "same_explicit_contract_reference",
    }
    if not all(bool(integrity.get(field)) for field in required_integrity):
        raise PostMutationShadowAdmissionError("Frozen parent integrity proof is incomplete")
    data_fingerprint = str(provenance.get("data_fingerprint") or "")
    contract_map_sha256 = str(provenance.get("contract_map_sha256") or "")
    if len(data_fingerprint) != 64 or len(contract_map_sha256) != 64:
        raise PostMutationShadowAdmissionError("Frozen parent data fingerprints are incomplete")
    files = list(provenance.get("files") or [])
    if not files or any(len(str(row.get("sha256") or "")) != 64 for row in files):
        raise PostMutationShadowAdmissionError("Frozen parent file provenance is incomplete")
    symbols = {str(value) for value in provenance.get("symbols") or []}
    coverage: dict[str, dict[str, Any]] = {}
    for candidate in source.get("candidates") or []:
        candidate_id = str(candidate.get("candidate_id") or "")
        primary = str(candidate.get("primary_market") or "")
        execution = str(candidate.get("execution_market") or "")
        folds = dict(candidate.get("fold_results") or {})
        has_2023_events = any(
            str(name).startswith("2023") and int((row or {}).get("events") or 0) > 0
            for name, row in folds.items()
        )
        hard_invalidations = list(
            (candidate.get("shadow_evidence") or {}).get("hard_invalidations") or []
        )
        if (
            candidate_id
            and primary in symbols
            and execution in symbols
            and has_2023_events
            and not hard_invalidations
        ):
            coverage[candidate_id] = {
                "source_result_sha256": source_sha256,
                "data_role": "DEVELOPMENT",
                "period_start": "2023-01-01",
                "period_end_exclusive": "2024-10-01",
                "data_fingerprint": data_fingerprint,
                "contract_map_sha256": contract_map_sha256,
                "file_sha256s": sorted(str(row["sha256"]) for row in files),
                "primary_market": primary,
                "execution_market": execution,
                "full_2023_source_replay": True,
                "source_no_lookahead_proof": True,
            }
    if not coverage:
        raise PostMutationShadowAdmissionError(
            "Frozen parent result contains no fully covered parent candidate"
        )
    return coverage


def _verify_halving_chain(
    result: dict[str, Any],
    *,
    result_hash: str,
    manifest: dict[str, Any],
    manifest_hash: str,
    manifest_sha256: str,
    evidence: list[dict[str, Any]],
    evidence_sha256: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    if result.get("schema") != "hydra_post_mutation_successive_halving_result_v1":
        raise PostMutationShadowAdmissionError("Unexpected halving-result schema")
    if str(result.get("result_hash") or "") != str(result_hash):
        raise PostMutationShadowAdmissionError("Halving semantic result hash mismatch")
    if str(result.get("development_end_exclusive")) != "2024-10-01":
        raise PostMutationShadowAdmissionError("Halving changed the protected boundary")
    for field in ("q4_access_count", "network_requests", "paid_data_requests"):
        if int(result.get(field) or 0) != 0:
            raise PostMutationShadowAdmissionError(f"Halving reports prohibited {field}")
    if bool(result.get("order_capability")) or int(result.get("paper_shadow_ready") or 0):
        raise PostMutationShadowAdmissionError("Halving exceeded its no-order status ceiling")
    artifacts = dict(result.get("artifacts") or {})
    if str((artifacts.get("elite_manifest") or {}).get("sha256") or "") != manifest_sha256:
        raise PostMutationShadowAdmissionError("Halving/elite-manifest hash mismatch")
    if str((artifacts.get("candidate_evidence") or {}).get("sha256") or "") != evidence_sha256:
        raise PostMutationShadowAdmissionError("Halving/candidate-evidence hash mismatch")

    if manifest.get("schema") != "hydra_post_mutation_elite_manifest_v1":
        raise PostMutationShadowAdmissionError("Unexpected elite-manifest schema")
    semantic_manifest = dict(manifest)
    recorded_manifest_hash = str(semantic_manifest.pop("manifest_hash", ""))
    if (
        recorded_manifest_hash != str(manifest_hash)
        or _canonical_hash(semantic_manifest) != recorded_manifest_hash
    ):
        raise PostMutationShadowAdmissionError("Elite-manifest semantic hash drift")
    if (
        int(manifest.get("q4_access_count") or 0) != 0
        or bool(manifest.get("order_capability"))
        or int(manifest.get("paper_shadow_ready") or 0) != 0
    ):
        raise PostMutationShadowAdmissionError("Elite manifest contains a protected status")

    candidates = list(result.get("candidates") or [])
    by_id = {str(row.get("candidate_id") or ""): row for row in candidates}
    evidence_by_id = {str(row.get("candidate_id") or ""): row for row in evidence}
    if (
        "" in by_id
        or len(by_id) != len(candidates)
        or set(evidence_by_id) != set(by_id)
        or len(evidence_by_id) != len(evidence)
    ):
        raise PostMutationShadowAdmissionError("Candidate evidence population mismatch")
    for candidate_id in sorted(by_id):
        if _canonical_hash(by_id[candidate_id]) != _canonical_hash(evidence_by_id[candidate_id]):
            raise PostMutationShadowAdmissionError(
                f"Candidate evidence drift for {candidate_id}"
            )
    selected_ids = sorted(str(value) for value in manifest.get("selected_candidate_ids") or [])
    if selected_ids != sorted(str(value) for value in result.get("selected_candidate_ids") or []):
        raise PostMutationShadowAdmissionError("Manifest/result elite selection mismatch")
    if int(manifest.get("selected_count") or 0) != len(selected_ids):
        raise PostMutationShadowAdmissionError("Elite-manifest selected count mismatch")
    selected = []
    for candidate_id in selected_ids:
        candidate = by_id.get(candidate_id)
        if candidate is None or not bool(candidate.get("selected_elite")):
            raise PostMutationShadowAdmissionError("Manifest references a non-elite candidate")
        selected.append(candidate)
    return selected, selected_ids


def _guard_specification(candidate: Mapping[str, Any]) -> PriorTradeGuardSpecification | None:
    propagated = dict(candidate.get("mutation_evidence") or {})
    evidence_hash = str(propagated.pop("evidence_hash", ""))
    if not evidence_hash or evidence_hash != _canonical_hash(propagated):
        return None
    hypothesis = dict(propagated.get("hypothesis") or {})
    guard = dict(propagated.get("guard") or {})
    if (
        str(hypothesis.get("child_candidate_id") or "") != str(candidate.get("candidate_id") or "")
        or str(hypothesis.get("parent_candidate_id") or "")
        != str(candidate.get("parent_candidate_id") or "")
        or "PRIOR_EQUITY" not in str(hypothesis.get("mutation_class") or "").upper()
        or bool(hypothesis.get("status_inheritance_allowed"))
        or bool(hypothesis.get("q4_access_allowed"))
        or bool(hypothesis.get("live_or_broker_allowed"))
    ):
        return None
    try:
        specification = PriorTradeGuardSpecification(
            trailing_window=int(guard.get("trailing_window") or 0),
            minimum_prior_observations=int(guard.get("minimum_prior_observations") or 0),
            warmup_completed_trades=int(guard.get("training_count") or 0),
            frozen_threshold=float(guard.get("frozen_threshold")),
            activation_shift_periods=int(guard.get("activation_shift_periods") or 0),
            current_event_outcome_used=bool(guard.get("current_event_outcome_used", True)),
        )
        specification.validate()
        return specification
    except (TypeError, ValueError):
        return None


def _gate(name: str, passed: bool, detail: Any) -> dict[str, Any]:
    return {"gate": name, "passed": bool(passed), "detail": detail}


def _implementation_precheck(
    candidate: Mapping[str, Any],
    parent: ShadowSpecification,
    guard: PriorTradeGuardSpecification | None,
    source_coverage: Mapping[str, Any],
) -> dict[str, Any]:
    expected_parent = str(candidate.get("parent_candidate_id") or "")
    checks = {
        "parent_configuration_matches": parent.strategy_id == expected_parent,
        "guard_specification_valid": guard is not None,
        "registered_source_coverage": bool(
            source_coverage.get("full_2023_source_replay")
            and source_coverage.get("source_no_lookahead_proof")
        ),
        "feature_versions_pinned": bool(parent.feature_versions),
        "closed_bar_timeframes_pinned": bool(parent.timeframes),
        "explicit_markets_pinned": bool(parent.markets),
        "session_and_clock_policy_pinned": bool(parent.session_rules),
        "stale_data_fail_closed": parent.stale_data_seconds > 0,
        "duplicate_signal_policy_pinned": parent.duplicate_signal_window_seconds > 0,
        "risk_limits_pinned": bool(
            parent.maximum_exposure > 0
            and parent.internal_daily_risk_limit > 0
            and parent.simulated_mll_floor < 0
        ),
        "startup_fail_closed": str(parent.reconciliation.get("startup") or "").lower()
        == "fail_closed",
        "signals_and_virtual_fills_logged": bool(
            parent.logging.get("signals") and parent.logging.get("virtual_fills")
        ),
        "orders_disabled": not parent.outbound_orders_enabled,
    }
    return {"passed": all(checks.values()), "checks": checks}


def _evaluate(
    candidate: dict[str, Any],
    parent_source_coverage: Mapping[str, Mapping[str, Any]],
    parent_specification: ShadowSpecification,
) -> dict[str, Any]:
    candidate_id = str(candidate.get("candidate_id") or "")
    propagated = dict(candidate.get("mutation_evidence") or {})
    guard_spec = _guard_specification(candidate)
    folds = dict(candidate.get("folds") or {})
    fold_values = {
        str(name): float((value or {}).get("net_pnl") or 0.0)
        for name, value in folds.items()
    }
    pooled = float(candidate.get("pooled_net_pnl") or 0.0)
    weak = [(name, value) for name, value in fold_values.items() if value <= 0.0]
    account = dict(candidate.get("account_path") or {})
    topstep = dict(propagated.get("topstep") or {})
    retained = float(propagated.get("retained_fraction") or 0.0)
    adjusted_p = float(candidate.get("candidate_null_bh_adjusted_p") or 1.0)
    no_account_breach = bool(
        not account.get("mll_breached") and not account.get("contract_limit_breached")
    )
    weak_noncatastrophic = bool(
        len(weak) <= 1
        and (
            not weak
            or (
                abs(float(weak[0][1])) <= 0.50 * abs(pooled)
                and no_account_breach
                and guard_spec is not None
            )
        )
    )
    micro_safe = bool(
        topstep.get("micro_one_contract_mll_safe")
        and float(topstep.get("micro_one_contract_min_mll_buffer") or 0.0) >= 1_000.0
    )
    combine_path = bool(
        topstep.get("path_candidate")
        and (topstep.get("ten_micro_combine") or {}).get("passed")
        and not (topstep.get("ten_micro_combine") or {}).get("mll_breached")
        and (topstep.get("ten_micro_combine") or {}).get("consistency_ok")
    )
    parent_id = str(candidate.get("parent_candidate_id") or "")
    source_coverage = dict(parent_source_coverage.get(parent_id) or {})
    implementation = _implementation_precheck(
        candidate, parent_specification, guard_spec, source_coverage
    )
    full_2023 = bool(
        propagated.get("full_2023_replay_available")
        and int((folds.get("2023_AVAILABLE") or {}).get("events") or 0) > 0
        and source_coverage.get("full_2023_source_replay")
        and source_coverage.get("source_no_lookahead_proof")
        and "FULL_2023_REPLAY_UNAVAILABLE" not in set(candidate.get("uncertainty_flags") or [])
    )
    hypothesis = dict(propagated.get("hypothesis") or {})
    realtime = bool(
        guard_spec is not None
        and hypothesis.get("training_policy")
        and str(hypothesis.get("exact_change") or "").strip()
        and implementation["passed"]
    )
    gates = [
        _gate(
            "01_COMPLETE_2023_REPLAY",
            full_2023,
            {
                "fold": folds.get("2023_AVAILABLE"),
                "source_coverage": source_coverage,
            },
        ),
        _gate("02_BH_ADJUSTED_NULL", adjusted_p <= 0.10, {"adjusted_p": adjusted_p, "ceiling": 0.10}),
        _gate("03_POSITIVE_POOLED_ECONOMICS", pooled > 0.0, {"pooled_net_pnl": pooled}),
        _gate(
            "04_POSITIVE_AT_1_5X_COSTS",
            float((candidate.get("cost_stress") or {}).get("1.5x") or 0.0) > 0.0,
            {"net_pnl": float((candidate.get("cost_stress") or {}).get("1.5x") or 0.0)},
        ),
        _gate(
            "05_MAX_ONE_NONCATASTROPHIC_WEAK_FOLD",
            weak_noncatastrophic,
            {"weak_folds": weak, "maximum_loss_fraction_of_pooled": 0.50},
        ),
        _gate(
            "06_MICRO_MLL_SAFE",
            micro_safe,
            {
                "micro_one_contract_mll_safe": topstep.get("micro_one_contract_mll_safe"),
                "minimum_mll_buffer": topstep.get("micro_one_contract_min_mll_buffer"),
            },
        ),
        _gate("07_TOPSTEP_COMBINE_PATH", combine_path, {"topstep_path": topstep.get("path_candidate")}),
        _gate("08_RETAINED_AT_LEAST_HALF", retained >= 0.50, {"retained_fraction": retained}),
        _gate(
            "09_STRUCTURAL_AND_BEHAVIORAL_NONDUPLICATE",
            not bool(candidate.get("behaviorally_duplicate"))
            and bool(str(propagated.get("structural_fingerprint") or ""))
            and str(propagated.get("source_behavior_fingerprint") or "")
            != str(propagated.get("parent_behavior_fingerprint") or ""),
            {
                "behaviorally_duplicate": bool(candidate.get("behaviorally_duplicate")),
                "structural_fingerprint": propagated.get("structural_fingerprint"),
            },
        ),
        _gate(
            "10_PRIOR_COMPLETED_TRADE_GUARD",
            guard_spec is not None,
            {"guard_specification_hash": guard_spec.specification_hash if guard_spec else None},
        ),
        _gate(
            "11_REALTIME_DETERMINISTIC_IMPLEMENTATION",
            realtime,
            {
                "prior_completed_trades_only": guard_spec is not None,
                "implementation_precheck": implementation,
            },
        ),
        _gate(
            "12_ZERO_ORDER_FAIL_CLOSED_CONTRACT",
            bool(
                candidate.get("objective_pool") == OBJECTIVE_POOL
                and not candidate.get("order_capability")
                and int(candidate.get("q4_access_count") or 0) == 0
                and not candidate.get("paper_shadow_ready")
                and not candidate.get("shadow_research_active")
                and implementation["passed"]
            ),
            {
                "objective_pool": candidate.get("objective_pool"),
                "orders": bool(candidate.get("order_capability")),
                "q4_access_count": int(candidate.get("q4_access_count") or 0),
                "implementation_precheck": implementation,
            },
        ),
    ]
    passed = bool(candidate.get("status") == SOURCE_STATUS and all(row["passed"] for row in gates))
    first_failed = next((row["gate"] for row in gates if not row["passed"]), None)
    tie_break = {
        "adjusted_p": adjusted_p,
        "micro_min_mll_buffer": float(topstep.get("micro_one_contract_min_mll_buffer") or 0.0),
        "one_and_half_cost_net": float((candidate.get("cost_stress") or {}).get("1.5x") or 0.0),
        "retained_fraction": retained,
        "weak_fold_absolute_loss": abs(float(weak[0][1])) if weak else 0.0,
        "candidate_id": candidate_id,
    }
    return {
        "candidate_id": candidate_id,
        "parent_candidate_id": str(candidate.get("parent_candidate_id") or ""),
        "source_status": candidate.get("status"),
        "all_gates_passed": passed,
        "first_failed_gate": first_failed if candidate.get("status") == SOURCE_STATUS else "SOURCE_STATUS_NOT_PROMISING",
        "gates": gates,
        "tie_break": tie_break,
        "guard_specification": guard_spec,
        "source_coverage": source_coverage,
    }


def _rank(evaluations: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    passing = [row for row in evaluations if row["all_gates_passed"]]
    return sorted(
        passing,
        key=lambda row: (
            float(row["tie_break"]["adjusted_p"]),
            -float(row["tie_break"]["micro_min_mll_buffer"]),
            -float(row["tie_break"]["one_and_half_cost_net"]),
            -float(row["tie_break"]["retained_fraction"]),
            float(row["tie_break"]["weak_fold_absolute_loss"]),
            str(row["candidate_id"]),
        ),
    )


def _derive_configuration(
    *,
    parent: ShadowSpecification,
    child_id: str,
    guard: PriorTradeGuardSpecification,
    candidate: Mapping[str, Any],
    source_coverage: Mapping[str, Any],
    selection_decision_hash: str,
) -> ShadowSpecification:
    entry_rules = dict(parent.entry_rules)
    entry_rules["prior_trade_guard"] = guard.to_dict()
    entry_rules["missing_prior_trade_state_policy"] = "fail_closed_no_signal"
    entry_rules["current_trade_outcome_available_to_guard"] = False
    mutation_evidence = dict(candidate.get("mutation_evidence") or {})
    hypothesis = dict(mutation_evidence.get("hypothesis") or {})
    entry_rules["mutation_provenance"] = {
        "parent_candidate_id": str(candidate.get("parent_candidate_id") or ""),
        "child_candidate_id": child_id,
        "hypothesis_id": str(hypothesis.get("hypothesis_id") or ""),
        "hypothesis_hash": str(hypothesis.get("hypothesis_hash") or ""),
        "structural_fingerprint": str(
            mutation_evidence.get("structural_fingerprint") or ""
        ),
        "parent_behavior_fingerprint": str(
            mutation_evidence.get("parent_behavior_fingerprint") or ""
        ),
        "child_behavior_fingerprint": str(
            mutation_evidence.get("source_behavior_fingerprint") or ""
        ),
        "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
        "development_end_exclusive": "2024-10-01",
        "source_data_fingerprint": str(source_coverage.get("data_fingerprint") or ""),
        "source_contract_map_sha256": str(
            source_coverage.get("contract_map_sha256") or ""
        ),
        "selection_decision_hash": selection_decision_hash,
    }
    costs = dict(parent.costs)
    costs["admission_evidence"] = {
        "registered_cost_multiplier": 1.0,
        "stress_cost_multiplier": 1.5,
        "pooled_net_at_registered_cost": float(candidate.get("pooled_net_pnl") or 0.0),
        "pooled_net_at_1_5x_cost": float(
            (candidate.get("cost_stress") or {}).get("1.5x") or 0.0
        ),
    }
    sizing = dict(parent.sizing)
    sizing["objective_pool"] = OBJECTIVE_POOL
    sizing["micro_one_contract_mll_safe"] = True
    logging = dict(parent.logging)
    logging["prior_trade_guard_decisions"] = True
    logging["prior_trade_guard_state_hash"] = True
    reconciliation = dict(parent.reconciliation)
    reconciliation["prior_trade_guard_restart"] = "verify_state_hash_or_fail_closed"
    specification = replace(
        parent,
        strategy_id=child_id,
        strategy_version=f"{parent.strategy_version}__post_mutation_shadow_candidate_v1",
        feature_versions=tuple(parent.feature_versions) + (guard.version,),
        entry_rules=entry_rules,
        costs=costs,
        sizing=sizing,
        logging=logging,
        reconciliation=reconciliation,
        source_manifest_hash=selection_decision_hash,
        outbound_orders_enabled=False,
    )
    specification.validate()
    return specification


def run_post_mutation_shadow_admission(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    halving_result_path: str | Path,
    halving_result_sha256: str,
    halving_result_hash: str,
    elite_manifest_path: str | Path,
    elite_manifest_sha256: str,
    elite_manifest_hash: str,
    candidate_evidence_path: str | Path,
    candidate_evidence_sha256: str,
    parent_source_result_path: str | Path,
    parent_source_result_sha256: str,
    parent_shadow_configuration_path: str | Path,
    parent_shadow_configuration_sha256: str,
    parent_shadow_configuration_hash: str,
    code_commit: str,
) -> dict[str, Any]:
    """Apply the frozen audit and emit at most one inactive shadow candidate."""

    if not str(code_commit).strip():
        raise PostMutationShadowAdmissionError("code_commit is required")
    task_path = Path(engineering_task_path)
    result_path = Path(halving_result_path)
    manifest_path = Path(elite_manifest_path)
    evidence_path = Path(candidate_evidence_path)
    parent_source_path = Path(parent_source_result_path)
    parent_path = Path(parent_shadow_configuration_path)
    _verify(task_path, engineering_task_sha256, "engineering preregistration")
    if engineering_task_sha256 != PREREGISTRATION_SHA256:
        raise PostMutationShadowAdmissionError("Unexpected shadow-admission preregistration")
    for path, expected, label in (
        (result_path, halving_result_sha256, "halving result"),
        (manifest_path, elite_manifest_sha256, "elite manifest"),
        (evidence_path, candidate_evidence_sha256, "candidate evidence"),
        (parent_source_path, parent_source_result_sha256, "parent source result"),
        (parent_path, parent_shadow_configuration_sha256, "parent shadow configuration"),
    ):
        _verify(path, expected, label)

    halving = _load_json(result_path, "halving result")
    manifest = _load_json(manifest_path, "elite manifest")
    evidence = _load_jsonl(evidence_path)
    parent_source = _load_json(parent_source_path, "parent source result")
    source_coverage = _parent_source_coverage(
        parent_source, source_sha256=parent_source_result_sha256
    )
    selected, selected_ids = _verify_halving_chain(
        halving,
        result_hash=halving_result_hash,
        manifest=manifest,
        manifest_hash=elite_manifest_hash,
        manifest_sha256=elite_manifest_sha256,
        evidence=evidence,
        evidence_sha256=candidate_evidence_sha256,
    )
    parent_before = _sha256(parent_path)
    parent_specification = _load_shadow_specification(parent_path)
    if parent_specification.configuration_hash != str(parent_shadow_configuration_hash):
        raise PostMutationShadowAdmissionError("Supplied parent semantic hash mismatch")
    evaluations = [
        _evaluate(candidate, source_coverage, parent_specification)
        for candidate in selected
    ]
    ranked = _rank(evaluations)
    winner = ranked[0] if ranked else None
    selected_candidate = (
        next(row for row in selected if row["candidate_id"] == winner["candidate_id"])
        if winner
        else None
    )

    if winner and parent_specification.strategy_id != winner["parent_candidate_id"]:
        raise PostMutationShadowAdmissionError(
            "Frozen parent configuration does not belong to the outcome-independent winner"
        )

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    protocol_chain = {
        "policy_version": POLICY_VERSION,
        "engineering_task_sha256": engineering_task_sha256,
        "halving_result_sha256": halving_result_sha256,
        "halving_result_hash": halving_result_hash,
        "elite_manifest_sha256": elite_manifest_sha256,
        "elite_manifest_hash": elite_manifest_hash,
        "candidate_evidence_sha256": candidate_evidence_sha256,
        "parent_source_result_sha256": parent_source_result_sha256,
        "parent_shadow_configuration_sha256": parent_shadow_configuration_sha256,
        "parent_shadow_configuration_hash": parent_shadow_configuration_hash,
    }
    evidence_chain_hash = _canonical_hash(protocol_chain)

    serializable_audit = []
    for row in evaluations:
        cleaned = {key: value for key, value in row.items() if key != "guard_specification"}
        serializable_audit.append(cleaned)
    selection_decision = {
        "schema": "hydra_post_mutation_shadow_selection_v1",
        "policy_version": POLICY_VERSION,
        "population_fingerprint": _canonical_hash(selected_ids),
        "evidence_chain_hash": evidence_chain_hash,
        "frozen_elite_count": len(selected),
        "evaluations": sorted(serializable_audit, key=lambda row: str(row["candidate_id"])),
        "passing_candidate_ids_in_tie_break_order": [row["candidate_id"] for row in ranked],
        "selected_candidate_id": winner["candidate_id"] if winner else "NONE",
        "admission_cap": MAXIMUM_ADMISSIONS,
    }
    selection_decision["selection_decision_hash"] = _canonical_hash(selection_decision)

    shadow_configurations: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    if winner and selected_candidate:
        guard = winner["guard_specification"]
        if not isinstance(guard, PriorTradeGuardSpecification):
            raise PostMutationShadowAdmissionError("Passing winner lacks a valid frozen guard")
        specification = _derive_configuration(
            parent=parent_specification,
            child_id=str(winner["candidate_id"]),
            guard=guard,
            candidate=selected_candidate,
            source_coverage=dict(winner.get("source_coverage") or {}),
            selection_decision_hash=str(
                selection_decision["selection_decision_hash"]
            ),
        )
        configuration_path = (
            destination / "shadow_configurations" / f"{winner['candidate_id']}.json"
        )
        specification.write_immutable(configuration_path)
        if specification.outbound_orders_enabled:
            raise PostMutationShadowAdmissionError("Derived configuration enabled orders")
        topstep = dict((selected_candidate.get("mutation_evidence") or {}).get("topstep") or {})
        implementation_contract = dict(
            winner["gates"][10]["detail"]["implementation_precheck"]
        )
        candidate = {
            "candidate_id": winner["candidate_id"],
            "parent_candidate_id": winner["parent_candidate_id"],
            "status": OUTPUT_STATUS,
            "operational_classification": "INACTIVE_SHADOW_RESEARCH_CANDIDATE",
            "status_inherited": False,
            "inherited_passes": [],
            "objective_pool": OBJECTIVE_POOL,
            "net_pnl": float(selected_candidate.get("pooled_net_pnl") or 0.0),
            "micro_net_pnl": float(selected_candidate.get("pooled_net_pnl") or 0.0),
            "admission": {
                "policy_version": POLICY_VERSION,
                "permits_zero_risk_shadow": True,
                "fatal_reasons": [],
                "activation_requires_generic_workflow": True,
                "selection_decision_hash": selection_decision[
                    "selection_decision_hash"
                ],
            },
            "shadow_evidence": {
                "hard_invalidations": [],
                "account_mll_safe": True,
                "deterministic_signals": True,
                "realtime_features_available": True,
                "observability_complete": True,
                "implementation_contract": implementation_contract,
                "implementation_scope": (
                    "CONFIGURATION_COMPLETE_GENERIC_ACTIVATION_MUST_AUDIT_RUNNER_WIRING"
                ),
                "candidate_null_pass": True,
                "candidate_null_probability": winner["tie_break"]["adjusted_p"],
                "uncertainty_flags": list(selected_candidate.get("uncertainty_flags") or []),
            },
            "topstep": topstep,
            "configuration_hash": specification.configuration_hash,
            "shadow_research_active": False,
            "paper_shadow_ready": False,
            "q4_access_count": 0,
            "order_capability": False,
        }
        candidates.append(candidate)
        shadow_configurations.append(
            {
                "candidate_id": winner["candidate_id"],
                "status": OUTPUT_STATUS,
                "path": str(configuration_path.resolve()),
                "sha256": _sha256(configuration_path),
                "configuration_hash": specification.configuration_hash,
                "outbound_orders_enabled": False,
                "activation_required_separately": True,
            }
        )

    if len(candidates) > MAXIMUM_ADMISSIONS or len(shadow_configurations) > MAXIMUM_ADMISSIONS:
        raise PostMutationShadowAdmissionError("Admission cap exceeded")
    if _sha256(parent_path) != parent_before:
        raise PostMutationShadowAdmissionError("Parent shadow configuration changed")

    decision = {
        **selection_decision,
        "schema": "hydra_post_mutation_shadow_admission_decision_v1",
        "policy_version": POLICY_VERSION,
        "admission_count": len(candidates),
        "derived_configuration_hash": (
            shadow_configurations[0]["configuration_hash"]
            if shadow_configurations
            else None
        ),
        "q4_access_count": 0,
        "fresh_forward_data_access_count": 0,
        "orders": 0,
    }
    decision["decision_hash"] = _canonical_hash(decision)
    decision_path = destination / "post_mutation_shadow_admission_decision.json"
    _write_immutable(decision_path, json.dumps(decision, indent=2, sort_keys=True) + "\n")

    payload: dict[str, Any] = {
        "schema": "hydra_post_mutation_shadow_admission_result_v1",
        "policy_version": POLICY_VERSION,
        "scientific_conclusion": (
            "ONE_POST_MUTATION_SHADOW_RESEARCH_CANDIDATE_ADMITTED"
            if candidates
            else "POST_MUTATION_SHADOW_ADMISSION_INSUFFICIENT_EVIDENCE"
        ),
        "interpretation_boundary": (
            "Admission authorizes immutable zero-order shadow packaging only. It does not "
            "activate the strategy, open Q4, confer PAPER_SHADOW_READY, or prove funded edge."
        ),
        "code_commit": code_commit,
        "source_halving_result_hash": halving_result_hash,
        "source_elite_manifest_hash": elite_manifest_hash,
        "source_candidate_evidence_sha256": candidate_evidence_sha256,
        "decision_hash": decision["decision_hash"],
        "candidate_count": 0,
        "candidates": candidates,
        "shadow_configurations": shadow_configurations,
        "promising_candidates": len(candidates),
        "shadow_candidates": len(candidates),
        "shadow_research_active": 0,
        "paper_shadow_ready": 0,
        "topstep_path_candidates": len(candidates),
        "q4_access_count": 0,
        "fresh_forward_data_access_count": 0,
        "network_requests": 0,
        "paid_data_requests": 0,
        "order_capability": False,
        "broker_connections_allowed": 0,
        "next_recommended_action": (
            "RUN_GENERIC_ZERO_ORDER_SHADOW_ACTIVATION_SAFETY_AUDIT"
            if candidates
            else "CONTINUE_DISCOVERY_PROMOTION_AND_TARGETED_MUTATION"
        ),
        "artifacts": {
            "decision": {
                "path": str(decision_path.resolve()),
                "sha256": _sha256(decision_path),
            },
            "shadow_configurations": shadow_configurations,
        },
    }
    payload["result_hash"] = _canonical_hash(payload)
    result_output_path = destination / "post_mutation_shadow_admission_result.json"
    _write_immutable(result_output_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    report_path = destination / "post_mutation_shadow_admission_report.md"
    report = "\n".join(
        [
            "# HYDRA post-mutation shadow admission",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Frozen elites audited: `{len(selected)}`",
            f"- Passing before cap: `{len(ranked)}`",
            f"- Admitted: `{len(candidates)}`",
            f"- Selected: `{decision['selected_candidate_id']}`",
            "- SHADOW_RESEARCH_ACTIVE: `0`",
            "- PAPER_SHADOW_READY: `0`",
            "- Q4 / fresh feed / orders: `0 / 0 / 0`",
            "",
            payload["interpretation_boundary"],
            "",
        ]
    )
    _write_immutable(report_path, report)
    returned = dict(payload)
    returned["artifacts"] = {
        **payload["artifacts"],
        "result": {"path": str(result_output_path.resolve()), "sha256": _sha256(result_output_path)},
        "report": {"path": str(report_path.resolve()), "sha256": _sha256(report_path)},
    }
    returned["report_path"] = str(report_path.resolve())
    return returned
