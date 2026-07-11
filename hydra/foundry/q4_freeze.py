from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any

from hydra.governance.kernel import check_governance_kernel
from hydra.utils.config import project_path


VERSION = "hydra_q4_candidate_freeze_v1"


class Q4CandidateFreezeError(RuntimeError):
    pass


def run_q4_candidate_freeze(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    source_continuation_result_path: str | Path,
    source_continuation_result_sha256: str,
    source_continuation_result_hash: str,
    source_trade_ledger_path: str | Path,
    source_trade_ledger_sha256: str,
    source_shadow_configuration_path: str | Path,
    source_shadow_configuration_sha256: str,
    source_shadow_configuration_hash: str,
    candidate_id: str,
    code_commit: str,
    governance_baseline_commit: str,
    remaining_databento_budget_usd: float,
) -> dict[str, Any]:
    sources = {
        "engineering_task": (Path(engineering_task_path), engineering_task_sha256),
        "continuation_result": (
            Path(source_continuation_result_path),
            source_continuation_result_sha256,
        ),
        "trade_ledger": (Path(source_trade_ledger_path), source_trade_ledger_sha256),
        "shadow_configuration": (
            Path(source_shadow_configuration_path),
            source_shadow_configuration_sha256,
        ),
    }
    for label, (path, expected) in sources.items():
        if not path.is_file() or _sha256(path) != expected:
            raise Q4CandidateFreezeError(f"Frozen {label} is missing or changed: {path}")
    if len(code_commit) == 40:
        actual_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual_commit != code_commit:
            raise Q4CandidateFreezeError("Freeze worker commit differs from queued specification.")
    continuation = json.loads(sources["continuation_result"][0].read_text(encoding="utf-8"))
    if continuation.get("result_hash") != source_continuation_result_hash:
        raise Q4CandidateFreezeError("Continuation result hash does not match its frozen payload.")
    if int((continuation.get("governance") or {}).get("q4_access_count_delta", -1)) != 0:
        raise Q4CandidateFreezeError("Continuation source already touched Q4.")
    eligible = list(continuation.get("q4_freeze_eligible_candidate_ids") or [])
    if candidate_id not in eligible:
        raise Q4CandidateFreezeError("Candidate was not declared Q4-freeze eligible.")
    candidates = {
        str(row.get("candidate_id")): row for row in continuation.get("candidates") or []
    }
    candidate = candidates.get(candidate_id)
    if candidate is None:
        raise Q4CandidateFreezeError("Selected candidate dossier is missing.")
    _validate_candidate(candidate)
    ranked = sorted(
        (candidates[item] for item in eligible),
        key=lambda row: (
            float((row.get("null_evidence") or {}).get("family_adjusted_probability", 1.0)),
            -int(row.get("supportive_temporal_folds", 0)),
            -float(row.get("net_pnl", 0.0)),
            str(row.get("candidate_id")),
        ),
    )
    if not ranked or str(ranked[0].get("candidate_id")) != candidate_id:
        raise Q4CandidateFreezeError("Candidate selection differs from the frozen ranking rule.")
    shadow_configuration = json.loads(
        sources["shadow_configuration"][0].read_text(encoding="utf-8")
    )
    configuration_payload = {
        key: value
        for key, value in shadow_configuration.items()
        if key != "configuration_hash"
    }
    calculated_configuration_hash = hashlib.sha256(
        json.dumps(
            configuration_payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
    ).hexdigest()
    if (
        shadow_configuration.get("configuration_hash")
        != source_shadow_configuration_hash
        or calculated_configuration_hash != source_shadow_configuration_hash
        or shadow_configuration.get("strategy_id") != candidate_id
        or bool(shadow_configuration.get("outbound_orders_enabled"))
    ):
        raise Q4CandidateFreezeError("Shadow configuration identity or zero-order guard failed.")
    governance = check_governance_kernel(
        baseline_commit=governance_baseline_commit,
        remaining_budget_usd=remaining_databento_budget_usd,
    )
    if not governance.passed or int(governance.details.get("q4_access_count", -1)) != 0:
        raise Q4CandidateFreezeError(f"Pre-Q4 governance failed: {governance.to_dict()}")
    preregistration_path = Path(str(continuation.get("preregistration_path") or ""))
    if not preregistration_path.is_file():
        raise Q4CandidateFreezeError("Continuation preregistration artifact is missing.")
    preregistration = json.loads(preregistration_path.read_text(encoding="utf-8"))
    if preregistration.get("preregistration_hash") != continuation.get("preregistration_hash"):
        raise Q4CandidateFreezeError("Continuation preregistration hash mismatch.")
    code_paths = [
        project_path("hydra", "research", "equity_open_gap_reversal.py"),
        project_path("hydra", "research", "equity_open_gap_continuation.py"),
        project_path("hydra", "foundry", "status.py"),
        project_path("hydra", "shadow", "specification.py"),
        project_path("hydra", "shadow", "runner.py"),
    ]
    if not all(path.is_file() for path in code_paths):
        raise Q4CandidateFreezeError("Required frozen strategy/shadow source is missing.")
    manifest: dict[str, Any] = {
        "schema": VERSION,
        "candidate_id": candidate_id,
        "candidate_status_at_freeze": candidate["status"],
        "mechanism_family": candidate["mechanism_family"],
        "lineage_policy": "exact_version_and_family_cannot_mutate_then_reuse_q4",
        "code_commit": code_commit,
        "source_code_sha256": {str(path): _sha256(path) for path in code_paths},
        "source_artifacts": {
            label: {"path": str(path), "sha256": digest}
            for label, (path, digest) in sources.items()
        },
        "continuation_result_hash": source_continuation_result_hash,
        "preregistration": {
            "path": str(preregistration_path),
            "sha256": _sha256(preregistration_path),
            "preregistration_hash": continuation["preregistration_hash"],
        },
        "shadow_configuration": {
            "path": str(sources["shadow_configuration"][0]),
            "sha256": source_shadow_configuration_sha256,
            "configuration_hash": source_shadow_configuration_hash,
            "configuration_hash_recomputed": calculated_configuration_hash,
            "outbound_orders_enabled": False,
        },
        "frozen_strategy_contract": {
            "direction": preregistration["direction"],
            "primary_market": candidate["primary_market"],
            "execution_market": candidate["execution_market"],
            "primary_horizon_minutes": preregistration["primary_horizon_minutes"],
            "primary_threshold_quantile": preregistration["primary_threshold_quantile"],
            "minimum_prior_sessions": preregistration["minimum_prior_sessions"],
            "decision_time_chicago": preregistration["decision_time_chicago"],
            "cost_usd": preregistration["costs"][candidate["execution_market"]],
            "explicit_contract_policy": "date_aware_definitions_v2_roll_guarded",
            "sizing": shadow_configuration["sizing"],
            "timeframes": shadow_configuration["timeframes"],
            "session_rules": shadow_configuration["session_rules"],
        },
        "candidate_evidence": candidate,
        "data_fingerprint": (continuation.get("data_provenance") or {}).get(
            "data_fingerprint"
        ),
        "development_end_exclusive": "2024-10-01",
        "governance": {
            "passed": governance.passed,
            "checks": governance.checks,
            "protected_manifest_hash": governance.details.get("protected_manifest_hash"),
            "registry_integrity": governance.details.get("registry_integrity_result"),
            "q4_access_count_at_freeze": governance.details.get("q4_access_count"),
            "remaining_databento_budget_usd": remaining_databento_budget_usd,
        },
        "one_shot_q4_protocol": {
            "period_start": "2024-10-01",
            "period_end_exclusive": "2025-01-01",
            "allowed_outcomes": [
                "Q4_LOCKBOX_PASS",
                "Q4_LOCKBOX_FAIL",
                "Q4_LOCKBOX_INSUFFICIENT",
            ],
            "mutation_after_observation": False,
            "same_lineage_q4_reuse": False,
            "paper_shadow_ready_requires_non_event_dominated_pass": True,
            "does_not_authorize_live_or_broker_execution": True,
        },
    }
    manifest["freeze_manifest_hash"] = _stable_hash(manifest)
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    manifest_path = destination / f"q4_freeze_manifest_{candidate_id}.json"
    result_path = destination / "q4_candidate_freeze_result.json"
    report_path = destination / "q4_candidate_freeze_report.md"
    _write_immutable(manifest_path, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    result: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": "Q4_FREEZE_READY",
        "candidate_id": candidate_id,
        "candidate_status": candidate["status"],
        "freeze_manifest_path": str(manifest_path),
        "freeze_manifest_hash": manifest["freeze_manifest_hash"],
        "q4_access_count_delta": 0,
        "market_data_reads": 0,
        "incremental_databento_spend_usd": 0.0,
        "outbound_order_capability": False,
        "governance_passed": True,
        "governance_change_required_before_q4": (
            "Protected mission safety governor currently prohibits all Q4_ACCESS actions; "
            "freeze completion does not bypass that invariant."
        ),
        "next_recommended_action": "GOVERNANCE_CHANGE_REQUIRED_FOR_FROZEN_ONE_SHOT_Q4",
    }
    result["result_hash"] = _stable_hash(result)
    _write_immutable(result_path, json.dumps(result, indent=2, sort_keys=True) + "\n")
    _write_immutable(
        report_path,
        "# Q4 Candidate Freeze\n\n"
        f"- Conclusion: `{result['scientific_conclusion']}`\n"
        f"- Candidate: `{candidate_id}`\n"
        f"- Manifest: `{manifest['freeze_manifest_hash']}`\n"
        "- Q4 access: `0`\n"
        "- Data spend: `$0`\n"
        "- Outbound orders: `0`\n"
        "- Next boundary: `GOVERNANCE_CHANGE_REQUIRED_FOR_FROZEN_ONE_SHOT_Q4`\n",
    )
    return {
        **result,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "freeze_manifest_path": str(manifest_path),
        },
        "report_path": str(report_path),
    }


def _validate_candidate(candidate: dict[str, Any]) -> None:
    evidence = candidate.get("shadow_evidence") or {}
    attacks = candidate.get("attacks") or {}
    required = {
        "status": candidate.get("status") == "SHADOW_RESEARCH_CANDIDATE",
        "positive_net": float(candidate.get("net_pnl", 0.0)) > 0.0,
        "temporal_support": int(candidate.get("supportive_temporal_folds", 0)) >= 1,
        "no_catastrophic_transfer": not bool(evidence.get("catastrophic_transfer")),
        "candidate_null": bool(evidence.get("candidate_null_pass")),
        "parameter_stability": bool(evidence.get("parameter_stable")),
        "contract_evidence": bool(evidence.get("contract_evidence")),
        "mll_safe": bool(evidence.get("account_mll_safe")),
        "shadow_spec_complete": bool(evidence.get("shadow_spec_complete")),
        "not_event_dominated": not bool(attacks.get("event_dominated", True)),
        "zero_hard_invalidations": not list(evidence.get("hard_invalidations") or []),
        "no_holdout_claim": not bool(evidence.get("untouched_holdout_passed")),
    }
    failed = [name for name, passed in required.items() if not passed]
    if failed:
        raise Q4CandidateFreezeError(f"Candidate freeze requirements failed: {failed}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _write_immutable(path: Path, content: str) -> None:
    if path.exists() and path.read_text(encoding="utf-8") != content:
        raise Q4CandidateFreezeError(f"Refusing divergent immutable artifact: {path}")
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)
