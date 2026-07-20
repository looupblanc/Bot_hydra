"""One frozen causal shared-account replay for a complementary 50K pair.

This adapter reuses the exact ledgers and frozen non-overlapping starts from
the sealed autonomous graduation cohort.  It changes neither component.  The
only new executable object is the preregistered deterministic active-pool
governor in the self-hashed manifest.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Mapping

from hydra.account_policy.active_risk_pool import ActiveRiskPoolPolicy
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_graduation_cohort as cohort
from hydra.production.autonomous_exact_replay import (
    _declared_stop_risk_charge_per_mini,
    _read_verified_event_evidence,
)


SCHEMA = "hydra_autonomous_complement_pair_result_v1"
PREFLIGHT_SCHEMA = "hydra_autonomous_complement_pair_preflight_v1"
MANIFEST_SCHEMA = "hydra_autonomous_complement_pair_manifest_v1"
DEFAULT_MANIFEST = Path("config/research/autonomous_complement_pair_replay_v1.json")
HORIZONS = (5, 10, 20)
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
BLOCKS = ("B1", "B2", "B3", "B4")


class AutonomousComplementPairError(RuntimeError):
    """The frozen pair cannot be evaluated without contract drift."""


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AutonomousComplementPairError(f"cannot read JSON: {path}") from exc
    if not isinstance(value, dict):
        raise AutonomousComplementPairError(f"JSON object required: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_manifest(path: Path) -> dict[str, Any]:
    value = _read(path)
    claimed = value.get("manifest_hash")
    core = {key: row for key, row in value.items() if key != "manifest_hash"}
    if (
        value.get("schema") != MANIFEST_SCHEMA
        or not isinstance(claimed, str)
        or claimed != stable_hash(core)
    ):
        raise AutonomousComplementPairError("pair manifest self-hash drift")
    return value


def _verify_result_hash(value: Mapping[str, Any], field: str = "result_hash") -> None:
    core = dict(value)
    claimed = core.pop(field, None)
    if claimed != stable_hash(core):
        raise AutonomousComplementPairError(f"{field} drift")


def _source_rows(
    project: Path, manifest: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    source = dict(manifest["source_contract"])
    for path_key, sha_key in (
        ("cohort_manifest_path", "cohort_manifest_file_sha256"),
        ("cohort_preflight_path", "cohort_preflight_file_sha256"),
        ("cohort_result_path", "cohort_result_file_sha256"),
    ):
        path = project / str(source[path_key])
        if _sha256(path) != str(source[sha_key]):
            raise AutonomousComplementPairError(f"source SHA drift: {path_key}")
    source_manifest = cohort._load_cohort_manifest(
        project / str(source["cohort_manifest_path"])
    )
    source_preflight = cohort.verify_autonomous_graduation_preflight(
        _read(project / str(source["cohort_preflight_path"]))
    )
    source_result = _read(project / str(source["cohort_result_path"]))
    _verify_result_hash(source_result)
    if (
        str(source_preflight["preflight_hash"])
        != str(source["cohort_preflight_hash"])
        or str(source_result["result_hash"]) != str(source["cohort_result_hash"])
        or str(source_preflight["frozen_grid_hash"])
        != str(source["frozen_grid_hash"])
        or str(source_preflight["official_rule_snapshot_hash"])
        != str(source["official_rule_snapshot_hash"])
    ):
        raise AutonomousComplementPairError("source contract hash drift")
    return source_manifest, source_preflight, source_result


def _policy(manifest: Mapping[str, Any]) -> ActiveRiskPoolPolicy:
    policy = ActiveRiskPoolPolicy.from_mapping(dict(manifest["governor"]))
    if policy.outbound_order_capability:
        raise AutonomousComplementPairError("outbound order capability is forbidden")
    return policy


def _require_causal_risk_charges(
    policy: ActiveRiskPoolPolicy, derived: Mapping[str, float]
) -> None:
    """Reject epsilon/identity charges and require the frozen causal maximum."""

    if set(derived) != set(policy.component_priority):
        raise AutonomousComplementPairError("causal risk charge membership drift")
    for candidate_id in policy.component_priority:
        observed = float(derived[candidate_id])
        frozen = float(policy.nominal_risk_charge_map[candidate_id])
        if observed < 1.0 or frozen < 1.0:
            raise AutonomousComplementPairError("epsilon risk charge is forbidden")
        if abs(observed - frozen) > 1e-9:
            raise AutonomousComplementPairError(
                "governor risk charge differs from causal max"
            )


def _prepared_pair(
    project: Path,
    manifest: Mapping[str, Any],
    source_manifest: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    artifacts = cohort._load_replay_artifacts(project, source_manifest)
    prepared = cohort._prepare_all(project, source_manifest, artifacts)
    by_id = {str(row["candidate_id"]): row for row in prepared}
    ids = tuple(str(value) for value in manifest["candidate_ids"])
    if len(ids) != 2 or len(set(ids)) != 2 or not set(ids).issubset(by_id):
        raise AutonomousComplementPairError("frozen pair membership drift")
    left, right = (by_id[value] for value in ids)
    if any(str(row["account_label"]) != str(manifest["account_label"]) for row in (left, right)):
        raise AutonomousComplementPairError("pair mixes account sizes")
    policy = _policy(manifest)
    if tuple(policy.component_priority) != ids:
        raise AutonomousComplementPairError("governor priority differs from pair")
    risk_contract = dict(manifest["risk_charge_contract"])
    if (
        risk_contract.get("derivation")
        != "MAX_CAUSAL_DECLARED_STOP_RISK_PER_MINI"
        or risk_contract.get("statistic") != "MAX_NOT_P99"
        or risk_contract.get("future_outcomes_used") is not False
        or risk_contract.get("stop_inputs_available_at_decision_time") is not True
    ):
        raise AutonomousComplementPairError("risk-charge derivation is not causal")
    bank_entries = list(artifacts["bank_entries"])
    observed_charges: dict[str, float] = {}
    for candidate_id in ids:
        matches = [
            dict(row)
            for row in bank_entries
            if str(row.get("candidate_id")) == candidate_id
            and stable_hash(dict(row.get("candidate") or {}))
            == str(dict(risk_contract["sources"])[candidate_id]["candidate_payload_hash"])
        ]
        if not matches:
            raise AutonomousComplementPairError("risk-charge source candidate absent")
        entry = min(matches, key=lambda row: int(row.get("_source_wave", 0)))
        events, receipt = _read_verified_event_evidence(
            project, dict(entry["event_evidence"])
        )
        frozen_source = dict(dict(risk_contract["sources"])[candidate_id])
        derived = float(
            _declared_stop_risk_charge_per_mini(events, dict(entry["candidate"]))
        )
        if (
            int(receipt["record_count"]) != int(frozen_source["event_record_count"])
            or str(receipt["sha256"]) != str(frozen_source["event_file_sha256"])
            or stable_hash(events) != str(frozen_source["event_content_hash"])
            or abs(derived - float(frozen_source["derived_max_risk_charge_per_mini"]))
            > 1e-9
        ):
            raise AutonomousComplementPairError("causal risk-charge source drift")
        observed_charges[candidate_id] = derived
    _require_causal_risk_charges(policy, observed_charges)
    eligible = set(left["eligible_session_days"]).intersection(
        right["eligible_session_days"]
    )
    censored = set(left["censored_session_days"]).union(
        right["censored_session_days"]
    )
    combined = {
        "kind": "FROZEN_COMPLEMENT_PAIR",
        "candidate_id": policy.policy_id,
        "candidate_fingerprint": policy.structural_fingerprint,
        "behavioral_fingerprint": stable_hash(
            [left["behavioral_fingerprint"], right["behavioral_fingerprint"]]
        ),
        "qd_cell": "complement_pair_50k_11d0fa_1f3ff3",
        "account_label": str(manifest["account_label"]),
        "component_ids": ids,
        "frozen_policy_hash": stable_hash(policy.to_dict()),
        "source_evidence_hash": stable_hash(
            {row["candidate_id"]: row["source_evidence_hash"] for row in (left, right)}
        ),
        "risk_charge_source_hash": stable_hash(risk_contract),
        "calendar": left["calendar"],
        "eligible_session_days": frozenset(eligible),
        "censored_session_days": frozenset(censored),
        "trajectories": {
            scenario: {
                candidate_id: by_id[candidate_id]["trajectories"][scenario][candidate_id]
                for candidate_id in ids
            }
            for scenario in SCENARIOS
        },
        "policy": policy,
        "config": left["config"],
    }
    if left["calendar"] != right["calendar"] or left["config"] != right["config"]:
        raise AutonomousComplementPairError("pair calendar/account config drift")
    return combined, artifacts, by_id


def _runtime_provenance(project: Path, manifest_path: Path) -> dict[str, Any]:
    module = Path(__file__).resolve()
    runner = project / "scripts/run_autonomous_complement_pair_replay.py"
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=project,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise AutonomousComplementPairError("cannot bind source commit") from exc
    core = {
        "source_commit": commit,
        "manifest_file_sha256": _sha256(manifest_path),
        "adapter_module_sha256": _sha256(module),
        "runner_script_sha256": _sha256(runner),
    }
    return {**core, "runtime_provenance_hash": stable_hash(core)}


def build_preflight(
    root: str | Path, *, manifest_path: str | Path = DEFAULT_MANIFEST
) -> dict[str, Any]:
    project = Path(root).resolve()
    manifest_file = (project / manifest_path).resolve()
    manifest = _load_manifest(manifest_file)
    source_manifest, source_preflight, source_result = _source_rows(project, manifest)
    pair, artifacts, by_id = _prepared_pair(project, manifest, source_manifest)
    ids = tuple(str(value) for value in manifest["candidate_ids"])
    expected_hashes = dict(manifest["source_contract"]["component_result_hashes"])
    result_by_id = {
        str(row["candidate_id"]): row for row in source_result["candidate_results"]
    }
    for candidate_id in ids:
        if (
            str(result_by_id[candidate_id]["result_hash"])
            != str(expected_hashes[candidate_id])
            or str(result_by_id[candidate_id]["frozen_policy_hash"])
            != str(by_id[candidate_id]["frozen_policy_hash"])
        ):
            raise AutonomousComplementPairError("component result/policy hash drift")
    coverage = cohort._coverage_for_prepared(pair, artifacts["starts"])
    core = {
        "schema": PREFLIGHT_SCHEMA,
        "status": "PASS_FROZEN_COMPLEMENT_PAIR_PREFLIGHT",
        "manifest_hash": str(manifest["manifest_hash"]),
        "source_cohort_result_hash": str(source_result["result_hash"]),
        "source_cohort_preflight_hash": str(source_preflight["preflight_hash"]),
        "candidate_ids": list(ids),
        "source_candidate_result_hashes": expected_hashes,
        "source_candidate_frozen_policy_hashes": {
            candidate_id: str(by_id[candidate_id]["frozen_policy_hash"])
            for candidate_id in ids
        },
        "pair_policy": pair["policy"].to_dict(),
        "pair_policy_hash": pair["frozen_policy_hash"],
        "risk_charge_contract_hash": stable_hash(manifest["risk_charge_contract"]),
        "coverage": coverage,
        "coverage_hash": stable_hash(coverage),
        "runtime_provenance": _runtime_provenance(project, manifest_file),
        "confirmation_partition_reads": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "registry_writes": 0,
        "database_writes": 0,
    }
    core["runtime_provenance_hash"] = core["runtime_provenance"][
        "runtime_provenance_hash"
    ]
    return {**core, "preflight_hash": stable_hash(core)}


def _verify_preflight(value: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(value)
    claimed = row.pop("preflight_hash", None)
    if (
        row.get("schema") != PREFLIGHT_SCHEMA
        or row.get("status") != "PASS_FROZEN_COMPLEMENT_PAIR_PREFLIGHT"
        or claimed != stable_hash(row)
    ):
        raise AutonomousComplementPairError("pair preflight hash drift")
    for field in (
        "confirmation_partition_reads",
        "q4_access_count_delta",
        "broker_connections",
        "orders",
        "registry_writes",
        "database_writes",
    ):
        if int(row.get(field, -1)) != 0:
            raise AutonomousComplementPairError("pair preflight side-effect drift")
    return {**row, "preflight_hash": claimed}


def verify_result(value: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(value)
    _verify_result_hash(row)
    if row.get("schema") != SCHEMA or row.get("status") != (
        "COMPLETE_FROZEN_COMPLEMENT_PAIR_REPLAY"
    ):
        raise AutonomousComplementPairError("pair result schema/status drift")
    if row.get("evidence_role") != "VIEWED_DEVELOPMENT_ONLY" or row.get(
        "independent_confirmation_claimed"
    ) is not False:
        raise AutonomousComplementPairError("pair result evidence role inflated")
    for field in (
        "confirmation_partition_reads",
        "q4_access_count_delta",
        "broker_connections",
        "orders",
        "registry_writes",
        "database_writes",
        "xfa_paths_started",
    ):
        if int(row.get(field, -1)) != 0:
            raise AutonomousComplementPairError("pair result side-effect drift")
    if len(row.get("component_ids") or ()) != 2:
        raise AutonomousComplementPairError("pair result membership drift")
    return row


def execute(
    root: str | Path,
    preflight: Mapping[str, Any],
    *,
    manifest_path: str | Path = DEFAULT_MANIFEST,
) -> dict[str, Any]:
    project = Path(root).resolve()
    manifest_file = (project / manifest_path).resolve()
    frozen = _verify_preflight(preflight)
    manifest = _load_manifest(manifest_file)
    runtime = _runtime_provenance(project, manifest_file)
    if (
        str(frozen["manifest_hash"]) != str(manifest["manifest_hash"])
        or runtime != frozen["runtime_provenance"]
        or str(runtime["runtime_provenance_hash"])
        != str(frozen["runtime_provenance_hash"])
    ):
        raise AutonomousComplementPairError("runtime differs from frozen preflight")
    source_manifest, _source_preflight, source_result = _source_rows(project, manifest)
    pair, artifacts, _by_id = _prepared_pair(project, manifest, source_manifest)
    coverage = cohort._coverage_for_prepared(pair, artifacts["starts"])
    if stable_hash(coverage) != str(frozen["coverage_hash"]):
        raise AutonomousComplementPairError("runtime coverage differs from preflight")

    summaries: dict[str, dict[str, Any]] = {scenario: {} for scenario in SCENARIOS}
    receipts: list[dict[str, Any]] = []
    unique_passes: dict[str, dict[str, Any]] = {}
    for scenario in SCENARIOS:
        for horizon in HORIZONS:
            values: list[tuple[Any, str]] = []
            for frozen_start in coverage[str(horizon)]["full_coverage_starts"]:
                start_day = int(frozen_start["session_day"])
                block = str(frozen_start["temporal_block"])
                episode = run_causal_shared_account_episode(
                    pair["trajectories"][scenario],
                    pair["calendar"],
                    policy=pair["policy"],
                    start_day=start_day,
                    maximum_duration_days=horizon,
                    config=pair["config"],
                )
                values.append((episode, block))
                path_hash = stable_hash(episode.to_dict(include_paths=True))
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
                        "episode_path_hash": path_hash,
                    }
                )
                if episode.passed:
                    key = stable_hash(
                        {
                            "scenario": scenario,
                            "start_day": start_day,
                            "episode_path_hash": path_hash,
                        }
                    )
                    unique_passes.setdefault(
                        key,
                        {
                            "unique_combine_path_id": key,
                            "scenario": scenario,
                            "start_day": start_day,
                            "pass_day": int(episode.end_day),
                            "first_observed_horizon_trading_days": horizon,
                            "temporal_block": block,
                            "episode_path_hash": path_hash,
                        },
                    )
            row = coverage[str(horizon)]
            summaries[scenario][str(horizon)] = cohort._summarize_cohort_episodes(
                values,
                requested_start_count=int(row["requested_start_count"]),
                data_censored_count=int(row["data_censored_start_count"]),
                policy=pair["policy"],
            )

    concentration = cohort._unique_trajectory_concentration(pair)
    gates = cohort._development_gates(
        summaries,
        concentration,
        dict(manifest["development_gate"]),
    )
    qualified = [int(horizon) for horizon, checks in gates.items() if all(checks.values())]
    source_by_id = {
        str(row["candidate_id"]): row for row in source_result["candidate_results"]
    }
    standalone = {
        candidate_id: {
            "candidate_result_hash": source_by_id[candidate_id]["result_hash"],
            "summaries": source_by_id[candidate_id]["summaries"],
        }
        for candidate_id in manifest["candidate_ids"]
    }
    core = {
        "schema": SCHEMA,
        "status": "COMPLETE_FROZEN_COMPLEMENT_PAIR_REPLAY",
        "manifest_hash": str(manifest["manifest_hash"]),
        "preflight_hash": str(frozen["preflight_hash"]),
        "runtime_provenance": runtime,
        "runtime_provenance_hash": runtime["runtime_provenance_hash"],
        "candidate_id": pair["candidate_id"],
        "candidate_fingerprint": pair["candidate_fingerprint"],
        "component_ids": list(pair["component_ids"]),
        "account_label": pair["account_label"],
        "governor_policy": pair["policy"].to_dict(),
        "frozen_policy_hash": pair["frozen_policy_hash"],
        "risk_charge_contract_hash": str(frozen["risk_charge_contract_hash"]),
        "coverage": coverage,
        "summaries": summaries,
        "episode_receipts": receipts,
        "episode_receipt_hash": stable_hash(receipts),
        "unique_combine_paths": sorted(
            unique_passes.values(), key=lambda row: str(row["unique_combine_path_id"])
        ),
        "unique_combine_path_count": len(unique_passes),
        "unique_trajectory_concentration": concentration,
        "development_gate_results": gates,
        "qualified_horizons": qualified,
        "computed_evidence_tier": (
            "G_DEVELOPMENT_ONLY" if qualified else "Q_PAIR_DIAGNOSTIC"
        ),
        "standalone_source_comparisons": standalone,
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "independent_confirmation_claimed": False,
        "xfa_paths_started": 0,
        "xfa_reason": "BOUNDED_PAIR_COMBINE_REPLAY_FIRST",
        "confirmation_partition_reads": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "registry_writes": 0,
        "database_writes": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


__all__ = [
    "AutonomousComplementPairError",
    "DEFAULT_MANIFEST",
    "MANIFEST_SCHEMA",
    "PREFLIGHT_SCHEMA",
    "SCHEMA",
    "build_preflight",
    "execute",
    "verify_result",
]
