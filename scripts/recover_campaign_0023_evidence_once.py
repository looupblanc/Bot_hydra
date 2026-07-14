"""One-time, fail-closed evidence reconstruction for development campaign 0023.

This executable is intentionally campaign-specific.  It may run once, from the
exact source tree that launched campaign 0023.  It rebuilds the omitted event
and episode ledgers with the frozen implementation, reconciles every persisted
campaign artifact, and publishes a content-addressed development bundle only
when every material check is exact.

It does not mutate mission state, registries, databases, queues, budgets, data
access ledgers, or services.  A consumed-attempt marker is created with O_EXCL
before any reconstruction so an unsuccessful attempt cannot silently be retried.
"""

from __future__ import annotations

import argparse
import gc
import gzip
import hashlib
import json
import os
import shutil
import statistics
import subprocess
import sys
import traceback
from collections import Counter
from contextlib import ExitStack
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import hydra.economic_evolution.account_elite_robustness_evaluation as shared_eval
from hydra.economic_evolution.account_complementary_sleeve import (
    generate_complementary_sleeve_population,
)
from hydra.economic_evolution.account_evaluation import (
    ExactSleeveRuntime,
    _restress_routed_trade,
)
from hydra.economic_evolution.account_static_parent_basket_evaluation import (
    DEFAULT_REAL_POLICY_BATCH_SIZE,
    STATIC_PARENT_BASKET_EVALUATION_VERSION,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.economic_evolution.screen import CheapScreenPolicy, run_ultra_cheap_screen
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, select_episode_starts
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.research.economic_evolution_account_timeline_campaign import (
    account_timeline_final_result,
    account_timeline_paired_tripwire,
)
from hydra.research.economic_evolution_elite_robustness_campaign import (
    SOURCE_COVERAGE_PARENT_ID,
    SOURCE_PARENT_CAMPAIGN_ID,
    SOURCE_POPULATION_CAMPAIGN_ID,
    SOURCE_SIZING_PARENT_ID,
    _mutation_family_economics,
    _targeted_mutations,
)
from hydra.research.economic_evolution_pilot import (
    _bind_selected,
    _build_exact_runtimes,
    _common_days,
    _runtime_row,
)
from hydra.research.economic_evolution_static_parent_basket_campaign import (
    STATIC_PARENT_BASKET_ENGINE_VERSION,
    _patched_static_parent_campaign,
    _static_parent_next_action,
)


CAMPAIGN_ID = "hydra_economic_evolution_static_parent_basket_0023"
SOURCE_COMMIT = "51201969cf0a9abf055fa03ca1104604e8efb497"
IMPLEMENTATION_COMMIT = "831457dcdcc41851e388abd1bf80d3bbb27eb315"
STATUS_AUTHORITATIVE = "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION"
STATUS_FAILED = "SUMMARY_ONLY_NONCONFIRMATORY"
SCHEMA = "hydra_campaign_0023_authoritative_development_reconstruction_v1"
RECOVERY_VERSION = "hydra_campaign_0023_one_time_evidence_recovery_v1"
EXPECTED_RUNTIME_COUNT = 48
EXPECTED_EVENT_COUNT = 3_778
EXPECTED_PAIR_COUNT = 128
EXPECTED_START_COUNT = 48
EXPECTED_REAL_EPISODES_PER_SCENARIO = 6_144


class RecoveryError(RuntimeError):
    """The one-time recovery cannot produce authoritative evidence."""


class ReconciliationError(RecoveryError):
    """A reconstructed value differs materially from persisted evidence."""

    def __init__(self, check: str, expected: Any, actual: Any) -> None:
        super().__init__(f"material reconciliation failure: {check}")
        self.check = check
        self.expected = expected
        self.actual = actual


def canonical_bytes(value: Any, *, pretty: bool = False) -> bytes:
    kwargs: dict[str, Any] = {
        "sort_keys": True,
        "ensure_ascii": True,
        "allow_nan": False,
    }
    if pretty:
        kwargs["indent"] = 2
    else:
        kwargs["separators"] = (",", ":")
    return (json.dumps(value, **kwargs) + "\n").encode("ascii")


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value).rstrip(b"\n")).hexdigest()


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            if line.strip():
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise RecoveryError(f"non-object JSONL row at {path}:{number}")
                rows.append(value)
    return rows


def assert_exact(check: str, expected: Any, actual: Any) -> None:
    if expected != actual:
        raise ReconciliationError(
            check,
            {"hash": canonical_hash(expected), "value": _bounded(expected)},
            {"hash": canonical_hash(actual), "value": _bounded(actual)},
        )


def _bounded(value: Any, limit: int = 2_000) -> Any:
    encoded = json.dumps(value, sort_keys=True, default=str)
    if len(encoded) <= limit:
        return value
    return {"truncated": True, "json_prefix": encoded[:limit], "length": len(encoded)}


class DeterministicGzipJsonl:
    """Streaming deterministic JSONL writer with a final row count and hash."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._raw = self.path.open("xb")
        self._gzip = gzip.GzipFile(
            filename="", fileobj=self._raw, mode="wb", mtime=0
        )
        self.count = 0

    def write(self, value: Mapping[str, Any]) -> None:
        self._gzip.write(canonical_bytes(dict(value)))
        self.count += 1

    def close(self) -> dict[str, Any]:
        self._gzip.close()
        self._raw.flush()
        os.fsync(self._raw.fileno())
        self._raw.close()
        return {
            "path": self.path.name,
            "sha256": file_sha256(self.path),
            "size_bytes": self.path.stat().st_size,
            "row_count": self.count,
            "compression": "gzip_mtime_0",
        }

    def __enter__(self) -> "DeterministicGzipJsonl":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if not self._gzip.closed:
            self._gzip.close()
        if not self._raw.closed:
            self._raw.close()


def locate_feature_bundles(
    root: Path, expected: Mapping[str, str]
) -> dict[str, Path]:
    inverse = {str(digest): str(market) for market, digest in expected.items()}
    matches: dict[str, list[Path]] = {str(market): [] for market in expected}
    for manifest in root.rglob("manifest.json"):
        market = inverse.get(file_sha256(manifest))
        if market is not None:
            matches[market].append(manifest.parent.resolve())
    missing = sorted(key for key, values in matches.items() if not values)
    duplicate = sorted(key for key, values in matches.items() if len(values) != 1)
    if missing or duplicate:
        raise RecoveryError(
            f"frozen feature lookup failed: missing={missing} duplicate={duplicate}"
        )
    return {key: values[0] for key, values in sorted(matches.items())}


def verify_source_tree(root: Path, prereg: Mapping[str, Any]) -> dict[str, Any]:
    head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    if head != SOURCE_COMMIT:
        raise RecoveryError(f"recovery HEAD {head} is not launch state {SOURCE_COMMIT}")
    implementation = {}
    for relative, expected in sorted(prereg["implementation_files"].items()):
        path = root / str(relative)
        actual = file_sha256(path)
        if actual != str(expected):
            raise RecoveryError(f"implementation checksum drift: {relative}")
        implementation[str(relative)] = actual
    launch_config = root / "config/v7/economic_evolution_static_parent_basket_0023.json"
    if file_sha256(launch_config) != "4319939f6b0b945a80256861f4bb90b265c97aad94e00e27e2fb82a8c28715e9":
        raise RecoveryError("launch preregistration file checksum drift")
    return {
        "launch_source_commit": head,
        "campaign_implementation_commit": IMPLEMENTATION_COMMIT,
        "implementation_files": implementation,
        "launch_preregistration_sha256": file_sha256(launch_config),
    }


def verify_reference(root: Path, reference: Mapping[str, Any], semantic_key: str) -> Any:
    path = root / str(reference["path"])
    if file_sha256(path) != str(reference["file_sha256"]):
        raise RecoveryError(f"frozen reference checksum drift: {reference['path']}")
    value = read_json(path)
    if value.get(semantic_key) != reference["semantic_hash"]:
        raise RecoveryError(f"frozen reference semantic drift: {reference['path']}")
    return value


def runtime_rows(runtimes: Mapping[str, ExactSleeveRuntime]) -> list[dict[str, Any]]:
    return sorted(
        (_runtime_row(row) for row in runtimes.values()),
        key=lambda row: str(row["sleeve_id"]),
    )


def _component_evidence_rows(
    runtimes: Mapping[str, ExactSleeveRuntime],
) -> Iterator[
    tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]
]:
    for component_id in sorted(runtimes):
        runtime = runtimes[component_id]
        for event in sorted(
            runtime.events,
            key=lambda row: (row.event.session_day, row.event.decision_ns, row.event.event_id),
        ):
            signal = {
                "schema": "hydra_reconstructed_component_signal_v1",
                "campaign_id": CAMPAIGN_ID,
                "component_id": component_id,
                "specification_hash": runtime.specification_hash,
                "signal_market": runtime.signal_market,
                "execution_market": runtime.execution_market,
                "role": runtime.role.value,
                "source_campaign": runtime.source_campaign,
                "event_id": event.event.event_id,
                "decision_ns": event.event.decision_ns,
                "session_day": event.event.session_day,
                "regime": event.event.regime,
                "side": event.side,
                "signal_reconstructed": True,
                "development_only": True,
            }
            trade = {
                "schema": "hydra_reconstructed_component_trade_v1",
                "campaign_id": CAMPAIGN_ID,
                "component_id": component_id,
                "specification_hash": runtime.specification_hash,
                "signal_market": runtime.signal_market,
                "execution_market": event.market,
                "role": runtime.role.value,
                "source_campaign": runtime.source_campaign,
                "side": event.side,
                **event.event.to_dict(),
                "round_turn_cost": float(event.event.gross_pnl - event.event.net_pnl),
                "entry_semantics": "ONE_BAR_DELAY_FROZEN_FEATURE_ENTRY_PRICE",
                "exit_semantics": runtime.exit_implementation,
                "development_only": True,
            }
            entry = {
                "schema": "hydra_reconstructed_component_entry_v1",
                "campaign_id": CAMPAIGN_ID,
                "component_id": component_id,
                "specification_hash": runtime.specification_hash,
                "event_id": event.event.event_id,
                "signal_market": runtime.signal_market,
                "execution_market": event.market,
                "side": event.side,
                "decision_ns": event.event.decision_ns,
                "session_day": event.event.session_day,
                "quantity": event.event.quantity,
                "mini_equivalent": event.event.mini_equivalent,
                "regime": event.event.regime,
                "entry_semantics": trade["entry_semantics"],
                "development_only": True,
            }
            exit_row = {
                "schema": "hydra_reconstructed_component_exit_v1",
                "campaign_id": CAMPAIGN_ID,
                "component_id": component_id,
                "specification_hash": runtime.specification_hash,
                "event_id": event.event.event_id,
                "execution_market": event.market,
                "exit_ns": event.event.exit_ns,
                "session_day": event.event.session_day,
                "quantity": event.event.quantity,
                "gross_pnl": event.event.gross_pnl,
                "net_pnl": event.event.net_pnl,
                "cost": trade["round_turn_cost"],
                "worst_unrealized_pnl": event.event.worst_unrealized_pnl,
                "best_unrealized_pnl": event.event.best_unrealized_pnl,
                "exit_semantics": runtime.exit_implementation,
                "development_only": True,
            }
            yield signal, entry, exit_row, trade


def _membership_rows(pairs: Sequence[Any]) -> list[dict[str, Any]]:
    policies: dict[str, dict[str, Any]] = {}
    for pair in pairs:
        for role, policy in (
            ("REAL", pair.real_policy),
            ("MATCHED_CONTROL", pair.matched_control_policy),
        ):
            row = policy.to_dict()
            policy_id = str(row["policy_id"])
            value = {
                "schema": "hydra_reconstructed_basket_membership_v1",
                "campaign_id": CAMPAIGN_ID,
                "policy_role": role,
                "policy_id": policy_id,
                "policy": row,
                "component_ids": list(row["component_ids"]),
                "component_count": len(row["component_ids"]),
                "conflict_policy": "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
                "cost_scenarios": [1.0, 1.5],
                "development_only": True,
            }
            if policy_id in policies:
                assert_exact(f"membership.{policy_id}", policies[policy_id], value)
            policies[policy_id] = value
    return [policies[key] for key in sorted(policies)]


def _scenario_event_index(
    policy: Any,
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    stress: bool,
) -> dict[tuple[str, int, int], Any]:
    output: dict[tuple[str, int, int], Any] = {}
    for component_id in policy.component_ids:
        values = runtimes[component_id].events
        if stress:
            values = tuple(_restress_routed_trade(row, cost_stress=1.5) for row in values)
        for value in values:
            key = (component_id, int(value.event.session_day), int(value.event.decision_ns))
            if key in output:
                raise RecoveryError(f"non-unique routed event decision: {key}")
            output[key] = value
    return output


def _episode_and_path_rows(
    *,
    policy: Any,
    policy_role: str,
    scenario: str,
    summary: Any,
    runtimes: Mapping[str, ExactSleeveRuntime],
) -> Iterator[tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]]:
    stressed = scenario == "STRESS_1_5X"
    event_index = _scenario_event_index(policy, runtimes, stress=stressed)
    rules = Topstep150KConfig()
    for episode in summary.episodes:
        episode_key = f"{policy.policy_id}:{scenario}:{episode.start_day}"
        episode_row = episode.to_dict(include_paths=False)
        episode_row.update(
            {
                "schema": "hydra_reconstructed_account_episode_v1",
                "campaign_id": CAMPAIGN_ID,
                "episode_key": episode_key,
                "policy_role": policy_role,
                "scenario": scenario,
                "cost_multiplier": 1.5 if stressed else 1.0,
                "temporal_block": _historical_interleaved_block(
                    summary.episode_start_days, episode.start_day
                ),
                "development_only": True,
                "reconstruction_flag": True,
                "censored_state": (
                    "OPERATIONAL_HORIZON_NOT_REACHED"
                    if episode.terminal.value == "TIMEOUT"
                    else "NOT_CENSORED"
                ),
                "normal_costs": not stressed,
                "stressed_costs": stressed,
            }
        )
        executions: list[dict[str, Any]] = []
        for ordinal, decision in enumerate(episode.risk_allocation_path):
            key = (
                str(decision["component_id"]),
                int(decision["session_day"]),
                int(decision["decision_ns"]),
            )
            routed = event_index.get(key)
            if routed is None:
                raise RecoveryError(f"account decision has no component event: {key}")
            quantity = int(decision["quantity"])
            ratio = quantity / routed.event.quantity if decision["allow"] else 0.0
            executions.append(
                {
                    "schema": "hydra_reconstructed_account_execution_v1",
                    "campaign_id": CAMPAIGN_ID,
                    "episode_key": episode_key,
                    "policy_id": policy.policy_id,
                    "policy_role": policy_role,
                    "scenario": scenario,
                    "ordinal": ordinal,
                    "component_id": routed.component_id,
                    "event_id": routed.event.event_id,
                    "market": routed.market,
                    "side": routed.side,
                    "decision_ns": routed.event.decision_ns,
                    "exit_ns": routed.event.exit_ns,
                    "session_day": routed.event.session_day,
                    "allow": bool(decision["allow"]),
                    "reason": str(decision["reason"]),
                    "quantity": quantity,
                    "mini_equivalent": float(decision["mini_equivalent"]),
                    "realized_net_pnl_at_exit": float(routed.event.net_pnl * ratio),
                    "gross_pnl_at_exit": float(routed.event.gross_pnl * ratio),
                    "worst_unrealized_pnl": float(
                        routed.event.worst_unrealized_pnl * ratio
                    ),
                    "best_unrealized_pnl": float(
                        routed.event.best_unrealized_pnl * ratio
                    ),
                    "cost": float(
                        (routed.event.gross_pnl - routed.event.net_pnl) * ratio
                    ),
                    "development_only": True,
                }
            )
        daily: list[dict[str, Any]] = []
        executions_by_day: dict[int, list[dict[str, Any]]] = {}
        for row in executions:
            executions_by_day.setdefault(int(row["session_day"]), []).append(row)
        best_day = 0.0
        required_target = float(rules.combine_profit_target)
        for elapsed, value in enumerate(episode.daily_path, start=1):
            day_executions = executions_by_day.get(int(value["session_day"]), [])
            allowed = [row for row in day_executions if row["allow"]]
            component_attribution: dict[str, float] = {}
            for row in allowed:
                component = str(row["component_id"])
                component_attribution[component] = component_attribution.get(
                    component, 0.0
                ) + float(row["realized_net_pnl_at_exit"])
            open_positions: dict[str, dict[str, Any]] = {}
            maximum_open_mini = 0.0
            maximum_directional_mini = 0.0
            minimum_open_unrealized = 0.0
            maximum_open_unrealized = 0.0
            actions: list[tuple[int, int, str, dict[str, Any]]] = []
            for row in allowed:
                actions.append((int(row["decision_ns"]), 1, str(row["event_id"]), row))
                actions.append((int(row["exit_ns"]), 0, str(row["event_id"]), row))
            for _timestamp, kind, event_id, row in sorted(actions):
                if kind == 0:
                    open_positions.pop(event_id, None)
                    continue
                open_positions[event_id] = row
                total_mini = sum(
                    float(item["mini_equivalent"])
                    for item in open_positions.values()
                )
                directional = sum(
                    float(item["mini_equivalent"]) * int(item["side"])
                    for item in open_positions.values()
                )
                worst_open = sum(
                    min(float(item["worst_unrealized_pnl"]), 0.0)
                    for item in open_positions.values()
                )
                best_open = sum(
                    max(float(item["best_unrealized_pnl"]), 0.0)
                    for item in open_positions.values()
                )
                maximum_open_mini = max(maximum_open_mini, total_mini)
                maximum_directional_mini = max(
                    maximum_directional_mini, abs(directional)
                )
                minimum_open_unrealized = min(minimum_open_unrealized, worst_open)
                maximum_open_unrealized = max(maximum_open_unrealized, best_open)
            best_day = max(best_day, float(value["day_pnl"]))
            if (
                best_day
                > rules.combine_profit_target
                * rules.consistency_best_day_max_pct_of_profit_target
            ):
                required_target = max(
                    required_target,
                    best_day
                    / rules.consistency_best_day_max_pct_of_profit_target,
                )
            realized = float(value["balance"] - rules.combine_starting_balance)
            concentration = best_day / realized if realized > 0 else 0.0
            daily.append(
                {
                    "schema": "hydra_reconstructed_daily_account_path_v1",
                    "campaign_id": CAMPAIGN_ID,
                    "episode_key": episode_key,
                    "policy_id": policy.policy_id,
                    "policy_role": policy_role,
                    "scenario": scenario,
                    "elapsed_trading_days": elapsed,
                    "session_day": int(value["session_day"]),
                    "daily_realized_pnl": float(value["day_pnl"]),
                    "cumulative_realized_pnl": realized,
                    "end_of_day_unrealized_pnl": 0.0,
                    "minimum_open_unrealized_pnl": minimum_open_unrealized,
                    "maximum_open_unrealized_pnl": maximum_open_unrealized,
                    "balance": float(value["balance"]),
                    "mll_floor": float(value["mll_floor"]),
                    "mll_buffer": float(value["balance"] - value["mll_floor"]),
                    "required_target": required_target,
                    "target_progress": realized / max(required_target, 1.0),
                    "best_day_profit": best_day,
                    "best_day_concentration": concentration,
                    "consistency_ok": bool(
                        realized <= 0
                        or concentration
                        <= rules.consistency_best_day_max_pct_of_profit_target + 1e-12
                    ),
                    "dll_triggered": bool(value["dll_triggered"]),
                    "daily_cost": float(sum(float(row["cost"]) for row in allowed)),
                    "accepted_event_count": len(allowed),
                    "skipped_event_count": len(day_executions) - len(allowed),
                    "conflict_count": sum(
                        "CONFLICT" in str(row["reason"]) for row in day_executions
                    ),
                    "maximum_open_mini_equivalent": maximum_open_mini,
                    "maximum_net_directional_exposure": maximum_directional_mini,
                    "component_attribution": dict(sorted(component_attribution.items())),
                    "development_only": True,
                }
            )
        yield episode_row, executions, daily


def _historical_interleaved_block(starts: Sequence[int], start: int) -> str:
    ordered = sorted(int(value) for value in starts)
    return f"B{ordered.index(int(start)) % 4 + 1}"


def reconstruct_pair_evidence(
    pairs: Sequence[Any],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
    writers: Mapping[str, DeterministicGzipJsonl],
    worker_count: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    ordered = tuple(sorted(pairs, key=lambda row: row.pair_id))
    controls = {
        row.matched_control_policy.policy_id: row.matched_control_policy
        for row in ordered
    }
    parent_use_count = Counter(row.parent_policy_id for row in ordered)
    control_evaluations = shared_eval._evaluate_unique_policies(
        tuple(sorted(controls.values(), key=lambda row: row.policy_id)),
        runtimes,
        starts=starts,
        episode_policy=episode_policy,
        worker_count=worker_count,
    )
    counts = Counter()
    for policy_id in sorted(control_evaluations):
        policy = controls[policy_id]
        evaluated = control_evaluations[policy_id]
        for scenario, key in (("NORMAL", "normal"), ("STRESS_1_5X", "stress")):
            for episode, executions, daily in _episode_and_path_rows(
                policy=policy,
                policy_role="MATCHED_CONTROL",
                scenario=scenario,
                summary=evaluated[key],
                runtimes=runtimes,
            ):
                writers["episodes"].write(episode)
                for row in executions:
                    writers["account_executions"].write(row)
                for row in daily:
                    writers["daily_paths"].write(row)
                counts[f"control_{scenario.lower()}_episodes"] += 1

    compact: list[dict[str, Any]] = []
    for offset in range(0, len(ordered), DEFAULT_REAL_POLICY_BATCH_SIZE):
        batch = ordered[offset : offset + DEFAULT_REAL_POLICY_BATCH_SIZE]
        real_evaluations = shared_eval._evaluate_unique_policies(
            tuple(row.real_policy for row in batch),
            runtimes,
            starts=starts,
            episode_policy=episode_policy,
            worker_count=worker_count,
        )
        for pair in batch:
            evaluated = real_evaluations[pair.real_policy.policy_id]
            for scenario, key in (("NORMAL", "normal"), ("STRESS_1_5X", "stress")):
                for episode, executions, daily in _episode_and_path_rows(
                    policy=pair.real_policy,
                    policy_role="REAL",
                    scenario=scenario,
                    summary=evaluated[key],
                    runtimes=runtimes,
                ):
                    writers["episodes"].write(episode)
                    for row in executions:
                        writers["account_executions"].write(row)
                    for row in daily:
                        writers["daily_paths"].write(row)
                    counts[f"real_{scenario.lower()}_episodes"] += 1
            row = shared_eval._pair_result(
                pair,
                real=evaluated,
                control=control_evaluations[pair.matched_control_policy.policy_id],
                control_reused=parent_use_count[pair.parent_policy_id] > 1,
            )
            row["unique_control_evaluation_count"] = len(controls)
            row["unique_real_evaluation_count"] = len(ordered)
            row["memory_bounded_real_policy_batch_size"] = (
                DEFAULT_REAL_POLICY_BATCH_SIZE
            )
            row["execution_policy_version"] = STATIC_PARENT_BASKET_EVALUATION_VERSION
            row["underlying_signals_changed"] = False
            compact.append(row)
        del real_evaluations
        gc.collect()
    del control_evaluations
    gc.collect()
    return compact, dict(sorted(counts.items()))


def reconstruct_final_result(
    prereg: Mapping[str, Any],
    *,
    population_summary: Mapping[str, Any],
    pair_rows: Sequence[Mapping[str, Any]],
    starts: Sequence[int],
    tripwire: Mapping[str, Any],
    historical: Mapping[str, Any],
) -> dict[str, Any]:
    result = account_timeline_final_result(
        prereg,
        population_summary=population_summary,
        screen_summary=historical["cheap_screen"],
        exact_runtime_count=EXPECTED_RUNTIME_COUNT,
        exact_failure_count=0,
        pair_rows=pair_rows,
        starts=starts,
        tripwire=tripwire,
        elapsed_seconds=float(
            historical["wall_clock_accounting"]["total_seconds_to_result_assembly"]
        ),
        phase_seconds=historical["wall_clock_accounting"]["phase_seconds"],
    )
    result.update(
        {
            "schema": "hydra_elite_robustness_result_v1",
            "engine_version": STATIC_PARENT_BASKET_ENGINE_VERSION,
            "class_id": prereg["class_id"],
            "normal_plus_stressed_real_episode_count": (
                EXPECTED_PAIR_COUNT * EXPECTED_START_COUNT * 2
            ),
            "unique_matched_parent_evaluation_count": len(
                {row["parent_policy_id"] for row in pair_rows}
            ),
            "mutation_family_economics": _mutation_family_economics(pair_rows),
            "next_action": _static_parent_next_action(result),
            "CONTRE": str(prereg["CONTRE"]),
        }
    )
    result["completed_at_utc"] = historical["completed_at_utc"]
    result["account_policy_economics"]["targeted_mutations_selected"] = (
        _targeted_mutations(
            result["account_policy_economics"][
                "economic_failure_vector_distribution"
            ]
        )
    )
    result.pop("result_sha256", None)
    result["result_sha256"] = stable_hash(result)
    return result


def execute(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.source_root).resolve()
    historical_root = Path(args.historical_root).resolve()
    output = Path(args.output_dir).resolve()
    staging = output.with_name(output.name + ".staging")
    marker = output.with_name(output.name + ".ONE_TIME_ATTEMPT_CONSUMED.json")
    if output.exists() or staging.exists() or marker.exists():
        raise RecoveryError("campaign 0023 one-time attempt was already consumed")
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker_payload = {
        "schema": "hydra_campaign_0023_recovery_attempt_marker_v1",
        "campaign_id": CAMPAIGN_ID,
        "source_commit": SOURCE_COMMIT,
        "attempt": 1,
        "maximum_attempts": 1,
        "started_at_utc": datetime.now(UTC).isoformat(),
        "pid": os.getpid(),
    }
    fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
    with os.fdopen(fd, "wb") as handle:
        handle.write(canonical_bytes(marker_payload, pretty=True))
        handle.flush()
        os.fsync(handle.fileno())

    try:
        staging.mkdir(parents=True, exist_ok=False)
        prereg_path = root / "config/v7/economic_evolution_static_parent_basket_0023.json"
        prereg = read_json(prereg_path)
        if prereg.get("campaign_id") != CAMPAIGN_ID:
            raise RecoveryError("recovery is restricted to campaign 0023")
        unhashed = dict(prereg)
        claimed = unhashed.pop("preregistration_hash", None)
        if claimed != stable_hash(unhashed):
            raise RecoveryError("campaign preregistration self-hash drift")
        source_provenance = verify_source_tree(root, prereg)
        hypothesis = verify_reference(root, prereg["hypothesis_worm"], "hypothesis_hash")
        elite_manifest = verify_reference(root, prereg["elite_manifest"], "manifest_hash")
        parent_bank = verify_reference(root, prereg["parent_bank"], "bank_hash")
        if hypothesis["source_elite_manifest_hash"] != elite_manifest["manifest_hash"]:
            raise RecoveryError("hypothesis and elite manifest disagree")
        seed_path = root / str(prereg["source_seed"]["path"])
        if file_sha256(seed_path) != str(prereg["source_seed"]["file_sha256"]):
            raise RecoveryError("source seed checksum drift")
        seed = load_and_verify_seed_archive(seed_path)
        if seed["archive_hash"] != prereg["source_seed"]["archive_hash"]:
            raise RecoveryError("source seed semantic drift")

        historical_dir = historical_root / "reports/economic_evolution/static_parent_basket_0023"
        historical_population = read_json(historical_dir / "elite_robustness_population.json")
        historical_components = sorted(
            read_jsonl(historical_dir / "exact_component_results.jsonl"),
            key=lambda row: str(row["sleeve_id"]),
        )
        historical_screen = read_jsonl(historical_dir / "source_component_screen.jsonl")
        historical_screen_summary = read_json(
            historical_dir / "source_component_screen_summary.json"
        )
        historical_pairs = read_jsonl(
            historical_dir / "elite_robustness_pair_results.jsonl"
        )
        historical_tripwire = read_json(historical_dir / "family_tripwire.json")
        historical_result = read_json(historical_dir / "elite_robustness_result.json")
        historical_hashes = {
            path.name: file_sha256(path)
            for path in sorted(historical_dir.iterdir())
            if path.is_file()
            and path.name
            in {
                "elite_robustness_population.json",
                "exact_component_results.jsonl",
                "source_component_screen.jsonl",
                "source_component_screen_summary.json",
                "elite_robustness_pair_results.jsonl",
                "family_tripwire.json",
                "elite_robustness_result.json",
            }
        }
        contract_map = Path(args.contract_map).resolve()
        if file_sha256(contract_map) != str(prereg["data"]["contract_map_sha256"]):
            raise RecoveryError("contract map checksum drift")
        feature_paths = locate_feature_bundles(
            Path(args.feature_cache).resolve(), prereg["data"]["feature_manifest_sha256"]
        )
        matrices = {
            market: FeatureMatrix.open(path, mmap=True)
            for market, path in feature_paths.items()
        }
        for market, matrix in matrices.items():
            manifest = matrix.manifest
            provenance = dict(manifest.get("provenance") or {})
            key = dict(manifest.get("key") or {})
            if (
                provenance.get("data_fingerprint")
                != prereg["data"]["feature_source_fingerprint"]
                or key.get("source_data_sha256")
                != prereg["data"]["feature_source_fingerprint"]
                or provenance.get("q4_access_count_delta") != 0
                or provenance.get("outbound_order_capability") is not False
            ):
                raise RecoveryError(f"feature provenance drift: {market}")

        with _patched_static_parent_campaign(parent_bank):
            source = generate_complementary_sleeve_population(
                seed,
                campaign_id=SOURCE_POPULATION_CAMPAIGN_ID,
                parent_campaign_id=SOURCE_PARENT_CAMPAIGN_ID,
                sizing_parent_campaign_id=SOURCE_SIZING_PARENT_ID,
                coverage_parent_campaign_id=SOURCE_COVERAGE_PARENT_ID,
                policy_pair_count=512,
                maximum_components=48,
                minimum_component_events=20,
            )
            assert_exact(
                "source_component_manifest_hash",
                prereg["structural_population"]["source_component_manifest_hash"],
                source.manifest_hash,
            )
            from hydra.economic_evolution.account_static_parent_basket import (
                generate_static_parent_basket_population,
            )

            population = generate_static_parent_basket_population(
                elite_manifest,
                [row.to_dict() for row in source.components],
                campaign_id=CAMPAIGN_ID,
                proposal_count=int(prereg["structural_population"]["proposal_count"]),
                deep_pair_count=int(prereg["structural_population"]["policy_pair_count"]),
                parent_bank=parent_bank,
            )
            reconstructed_population = {
                **population.summary(),
                "components": [asdict(row) for row in population.components],
                "pairs": [row.to_dict() for row in population.pairs],
            }
            assert_exact("population_manifest_and_fingerprints", historical_population, reconstructed_population)

            sleeves = tuple(row.sleeve for row in source.components)
            screen_policy = CheapScreenPolicy(**prereg["cheap_screen_policy"])
            component_screen = run_ultra_cheap_screen(
                sleeves, matrices, policy=screen_policy
            )
            assert_exact(
                "source_component_screen_summary",
                historical_screen_summary,
                component_screen.summary(),
            )
            assert_exact(
                "source_component_screen_rows",
                historical_screen,
                list(component_screen.rows),
            )
            bound = _bind_selected(sleeves, matrices, policy=screen_policy)
            runtimes, failures = _build_exact_runtimes(
                bound,
                matrices,
                start_inclusive=str(prereg["exact_replay_period"][0]),
                end_exclusive=str(prereg["exact_replay_period"][1]),
                worker_count=int(prereg["compute"]["exact_worker_count"]),
            )
            if failures:
                raise ReconciliationError("exact_component_failures", [], failures)
            assert_exact("exact_runtime_count", EXPECTED_RUNTIME_COUNT, len(runtimes))
            actual_runtime_rows = runtime_rows(runtimes)
            assert_exact("all_48_component_metadata", historical_components, actual_runtime_rows)
            total_events = sum(row.event_count for row in runtimes.values())
            assert_exact("component_event_count", EXPECTED_EVENT_COUNT, total_events)

            component_signals = DeterministicGzipJsonl(
                staging / "component_signal_ledger.jsonl.gz"
            )
            component_entries = DeterministicGzipJsonl(
                staging / "component_entry_ledger.jsonl.gz"
            )
            component_exits = DeterministicGzipJsonl(
                staging / "component_exit_ledger.jsonl.gz"
            )
            component_trades = DeterministicGzipJsonl(
                staging / "component_trade_ledger.jsonl.gz"
            )
            chronological = DeterministicGzipJsonl(
                staging / "chronological_trade_ledger.jsonl.gz"
            )
            membership = DeterministicGzipJsonl(
                staging / "account_policy_membership_ledger.jsonl.gz"
            )
            episodes = DeterministicGzipJsonl(
                staging / "account_episode_ledger.jsonl.gz"
            )
            account_executions = DeterministicGzipJsonl(
                staging / "account_execution_ledger.jsonl.gz"
            )
            daily_paths = DeterministicGzipJsonl(
                staging / "account_daily_path_ledger.jsonl.gz"
            )
            writers = {
                "episodes": episodes,
                "account_executions": account_executions,
                "daily_paths": daily_paths,
            }
            all_trades: list[dict[str, Any]] = []
            for signal, entry, exit_row, trade in _component_evidence_rows(runtimes):
                component_signals.write(signal)
                component_entries.write(entry)
                component_exits.write(exit_row)
                component_trades.write(trade)
                all_trades.append(trade)
            for trade in sorted(
                all_trades,
                key=lambda row: (
                    int(row["session_day"]),
                    int(row["decision_ns"]),
                    str(row["component_id"]),
                    str(row["event_id"]),
                ),
            ):
                chronological.write(
                    {
                        **trade,
                        "schema": "hydra_reconstructed_chronological_trade_v1",
                    }
                )
            del all_trades
            for row in _membership_rows(population.pairs):
                membership.write(row)

            common_days = _common_days(list(runtimes.values()))
            episode_policy = EpisodeStartPolicy(**prereg["rolling_episode_policy"])
            starts = select_episode_starts(common_days, policy=episode_policy)
            assert_exact(
                "episode_starts", historical_result["global_episode_starts"], list(starts)
            )
            compact_pairs, episode_counts = reconstruct_pair_evidence(
                population.pairs,
                runtimes,
                starts=starts,
                episode_policy=episode_policy,
                writers=writers,
                worker_count=int(prereg["compute"]["account_worker_count"]),
            )
            assert_exact("all_128_pair_summaries", historical_pairs, compact_pairs)
            assert_exact("pair_count", EXPECTED_PAIR_COUNT, len(compact_pairs))
            assert_exact(
                "real_normal_episode_count",
                EXPECTED_REAL_EPISODES_PER_SCENARIO,
                episode_counts.get("real_normal_episodes", 0),
            )
            assert_exact(
                "real_stressed_episode_count",
                EXPECTED_REAL_EPISODES_PER_SCENARIO,
                episode_counts.get("real_stress_1_5x_episodes", 0),
            )
            reconstructed_tripwire = account_timeline_paired_tripwire(
                compact_pairs, prereg["family_tripwire"]
            )
            reconstructed_tripwire["class_id"] = prereg["class_id"]
            assert_exact("family_tripwire", historical_tripwire, reconstructed_tripwire)
            reconstructed_result = reconstruct_final_result(
                prereg,
                population_summary=population.summary(),
                pair_rows=compact_pairs,
                starts=starts,
                tripwire=reconstructed_tripwire,
                historical=historical_result,
            )
            assert_exact("terminal_campaign_result", historical_result, reconstructed_result)

        receipts = {
            "component_signal_ledger": component_signals.close(),
            "component_entry_ledger": component_entries.close(),
            "component_exit_ledger": component_exits.close(),
            "component_trade_ledger": component_trades.close(),
            "chronological_trade_ledger": chronological.close(),
            "account_policy_membership_ledger": membership.close(),
            "account_episode_ledger": episodes.close(),
            "account_execution_ledger": account_executions.close(),
            "account_daily_path_ledger": daily_paths.close(),
        }
        summary_payload = {
            "schema": SCHEMA,
            "recovery_version": RECOVERY_VERSION,
            "campaign_id": CAMPAIGN_ID,
            "evidence_status": STATUS_AUTHORITATIVE,
            "development_only": True,
            "independent_holdout": False,
            "reconstruction_flag": True,
            "attempt": 1,
            "maximum_attempts": 1,
            "source": {
                **source_provenance,
                "preregistration_hash": prereg["preregistration_hash"],
                "hypothesis_hash": hypothesis["hypothesis_hash"],
                "elite_manifest_hash": elite_manifest["manifest_hash"],
                "parent_bank_hash": parent_bank["bank_hash"],
                "source_seed_hash": seed["archive_hash"],
                "contract_map_sha256": file_sha256(contract_map),
                "feature_source_fingerprint": prereg["data"]["feature_source_fingerprint"],
                "feature_manifest_sha256": dict(
                    sorted(prereg["data"]["feature_manifest_sha256"].items())
                ),
                "historical_artifact_sha256": historical_hashes,
            },
            "reconciliation": {
                "all_material_checks_exact": True,
                "numerical_tolerance": 0.0,
                "component_metadata_expected": EXPECTED_RUNTIME_COUNT,
                "component_metadata_exact_matches": EXPECTED_RUNTIME_COUNT,
                "component_event_count": total_events,
                "candidate_fingerprints_exact": True,
                "population_manifest_hash": population.manifest_hash,
                "pair_summary_count": len(compact_pairs),
                "pair_summaries_exact": True,
                "episode_counts": episode_counts,
                "episode_starts": list(starts),
                "normal_pass_policy_count": historical_result[
                    "account_policy_economics"
                ]["policies_passing_at_least_one_combine_episode"],
                "stressed_pass_policy_count": historical_result[
                    "account_policy_economics"
                ]["stressed_policies_passing_at_least_one_combine_episode"],
                "maximum_normal_pass_rate": historical_result[
                    "account_policy_economics"
                ]["combine_pass_probability"]["maximum"],
                "maximum_stressed_pass_rate": historical_result[
                    "account_policy_economics"
                ]["stressed_combine_pass_probability"]["maximum"],
                "maximum_mll_breach_rate": historical_result[
                    "account_policy_economics"
                ]["mll_breach_rate_distribution"]["maximum"],
                "terminal_result_exact": True,
                "terminal_result_sha256": historical_result["result_sha256"],
                "family_verdict": historical_tripwire["verdict"],
            },
            "artifacts": receipts,
            "governance": {
                "q4_access_delta": 0,
                "new_data_purchase_count": 0,
                "broker_connections": 0,
                "orders": 0,
                "mission_db_writes": 0,
                "registry_writes": 0,
                "queue_writes": 0,
                "service_changes": 0,
            },
            "selector_eligibility": (
                "DEVELOPMENT_EVIDENCE_ONLY_NOT_INDEPENDENT_HOLDOUT"
            ),
        }
        summary_payload["bundle_hash"] = canonical_hash(summary_payload)
        manifest_path = staging / "authoritative_development_reconstruction_manifest.json"
        manifest_path.write_bytes(canonical_bytes(summary_payload, pretty=True))
        with manifest_path.open("rb") as handle:
            os.fsync(handle.fileno())
        directory_fd = os.open(staging, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        os.replace(staging, output)
        return {
            "status": STATUS_AUTHORITATIVE,
            "output_dir": str(output),
            "manifest": str(
                output / "authoritative_development_reconstruction_manifest.json"
            ),
            "bundle_hash": summary_payload["bundle_hash"],
            "event_count": total_events,
            "episode_counts": episode_counts,
        }
    except Exception as exc:
        if staging.exists():
            shutil.rmtree(staging)
        output.mkdir(parents=True, exist_ok=False)
        failure = {
            "schema": "hydra_campaign_0023_recovery_failure_v1",
            "campaign_id": CAMPAIGN_ID,
            "evidence_status": STATUS_FAILED,
            "attempt": 1,
            "maximum_attempts": 1,
            "retry_allowed": False,
            "source_commit": SOURCE_COMMIT,
            "failure_type": type(exc).__name__,
            "failure": str(exc),
            "material_check": getattr(exc, "check", None),
            "expected": getattr(exc, "expected", None),
            "actual": getattr(exc, "actual", None),
            "traceback": traceback.format_exc(),
            "historical_summaries_preserved": True,
            "nested_selector_0023_allowed": False,
            "q4_access_delta": 0,
            "new_data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
        }
        (output / "summary_only_nonconfirmatory.json").write_bytes(
            canonical_bytes(failure, pretty=True)
        )
        raise


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser()
    value.add_argument("--source-root", required=True)
    value.add_argument("--historical-root", required=True)
    value.add_argument("--contract-map", required=True)
    value.add_argument("--feature-cache", required=True)
    value.add_argument("--output-dir", required=True)
    return value


def main() -> int:
    args = parser().parse_args()
    try:
        result = execute(args)
    except Exception as exc:
        print(
            json.dumps(
                {"status": STATUS_FAILED, "error": str(exc)}, sort_keys=True
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
