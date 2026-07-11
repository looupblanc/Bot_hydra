from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hydra.data.multitimeframe import resample_closed_bars
from hydra.factory.quality_diversity import (
    ArchiveCandidate,
    ArchiveNiche,
    QualityDiversityArchive,
    structural_fingerprint,
)
from hydra.foundry.status import ShadowEvidence, calibrate_shadow_policy
from hydra.foundry.tournament import run_structural_tournament
from hydra.shadow.runner import ShadowRunner
from hydra.shadow.signal_bus import ShadowSignal
from hydra.shadow.specification import ShadowSpecification


VERSION = "hydra_foundry_bootstrap_v1"


class FoundryBootstrapError(RuntimeError):
    pass


def run_foundry_bootstrap(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    tournament_preregistration_path: str | Path,
    tournament_preregistration_sha256: str,
    tournament_report_path: str | Path,
    tournament_report_sha256: str,
    tournament_checkpoint_path: str | Path,
    tournament_checkpoint_sha256: str,
    code_commit: str,
) -> dict[str, Any]:
    sources = {
        "engineering_task": (Path(engineering_task_path), engineering_task_sha256),
        "tournament_preregistration": (
            Path(tournament_preregistration_path),
            tournament_preregistration_sha256,
        ),
        "tournament_report": (Path(tournament_report_path), tournament_report_sha256),
        "tournament_checkpoint": (
            Path(tournament_checkpoint_path),
            tournament_checkpoint_sha256,
        ),
    }
    for label, (path, expected) in sources.items():
        if not path.is_file() or _sha256(path) != expected:
            raise FoundryBootstrapError(f"Frozen {label} is missing or changed: {path}")
    preregistration = json.loads(sources["tournament_preregistration"][0].read_text())
    report = _markdown_json(sources["tournament_report"][0])
    if int(preregistration.get("atom_count", -1)) != 120:
        raise FoundryBootstrapError("Unexpected direct-tournament atom count.")
    expected_zero = {
        "fully_validated_edge_atoms": 0,
        "strategies_assembled": 0,
        "topstep_path_candidates": 0,
        "topstep_compatible_strategies": 0,
    }
    if any(int(report.get(key, -1)) != value for key, value in expected_zero.items()):
        raise FoundryBootstrapError("Direct-tournament conclusion changed.")
    calibration = _calibration()
    if not calibration["passed"]:
        raise FoundryBootstrapError(f"Shadow policy calibration failed: {calibration}")
    multitf = _multitimeframe_smoke()
    archive = _archive_smoke()
    production = run_structural_tournament()
    shadow = _shadow_smoke()
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": "FOUNDRY_CORE_CALIBRATED_TOURNAMENT_RECONCILED",
        "interpretation_boundary": (
            "This calibrates production infrastructure and reconciles a zero-survivor tournament. "
            "It validates no strategy and authorizes no Q4, broker or live execution."
        ),
        "code_commit": code_commit,
        "source_hashes": {label: digest for label, (_path, digest) in sources.items()},
        "tournament": {
            "atoms_preregistered": int(report.get("atoms_preregistered", 0)),
            "atoms_falsified": int(report.get("atoms_falsified", 0)),
            "atoms_insufficient": int(report.get("atoms_insufficient", 0)),
            "fully_validated_edge_atoms": 0,
            "strategies_assembled": 0,
        },
        "shadow_policy_calibration": calibration,
        "multitimeframe_smoke": multitf,
        "quality_diversity_smoke": archive,
        "production_tournament": {
            key: value for key, value in production.items() if key != "prototypes"
        },
        "shadow_runtime_smoke": shadow,
        "foundry_status": {
            "strategy_prototypes_generated": 120 + production["accepted_prototypes"],
            "strategies_screened": 120 + production["accepted_prototypes"],
            "promising_candidates": 0,
            "shadow_candidates": 0,
            "paper_shadow_ready": 0,
            "shadow_active": 0,
            "mechanisms_represented": len(report.get("atom_families_represented") or [])
            + len(production["allocations"]["families"]),
            "market_ecologies_represented": len(
                production["allocations"]["market_ecologies"]
            ),
            "timeframes_represented": len(
                production["allocations"]["timeframe_profiles"]
            ),
            "strategies_killed": int(report.get("atoms_falsified", 0)),
            "lineages_frozen": 0,
            "q4_candidates": 0,
            "model_quota_state": "AVAILABLE_FOR_ENGINEERING",
        },
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "governance": {
            "q4_access_delta": 0,
            "paid_data_cost_usd": 0.0,
            "outbound_order_capability": False,
        },
        "next_recommended_action": "QUEUE_EQUITY_OPEN_GAP_REVERSAL_EVENT_STRATEGY_PILOT",
    }
    payload["result_hash"] = _stable_hash(payload)
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "foundry_bootstrap_result.json"
    report_path = destination / "foundry_bootstrap_report.md"
    population_path = destination / "foundry_stage0_population.json"
    _write_immutable(
        population_path,
        json.dumps(
            {
                "schema": "hydra_foundry_stage0_population_v1",
                "population_sha256": production["population_sha256"],
                "prototypes": production["prototypes"],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(
        report_path,
        "# HYDRA Foundry Bootstrap\n\n"
        f"- Conclusion: `{payload['scientific_conclusion']}`\n"
        f"- Shadow policy: `{calibration}`\n"
        f"- MTF smoke: `{multitf}`\n"
        f"- QD smoke: `{archive}`\n"
        f"- Stage-0 tournament: `{production['accepted_prototypes']}` accepted prototypes; "
        f"population `{production['population_sha256']}`\n"
        f"- Shadow smoke: `{shadow}`\n",
    )
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "stage0_population_path": str(population_path),
        },
        "report_path": str(report_path),
    }


def _calibration() -> dict[str, object]:
    common = dict(
        data_integrity=True,
        no_lookahead=True,
        deterministic_signals=True,
        execution_possible=True,
        candidate_null_pass=True,
        parameter_stable=True,
        contract_evidence=True,
        account_mll_safe=True,
        realtime_features_available=True,
        shadow_spec_complete=True,
        observability_complete=True,
    )
    negatives = [
        ShadowEvidence(candidate_id=f"null_{index}", net_after_costs=-1.0, **common)
        for index in range(20)
    ]
    weak = [
        ShadowEvidence(
            candidate_id=f"weak_{index}",
            net_after_costs=1.0,
            supportive_temporal_folds=1,
            null_probability=0.10,
            sample_size=20,
            uncertainty="weak_real_control",
            **common,
        )
        for index in range(10)
    ]
    strong = [
        ShadowEvidence(
            candidate_id=f"strong_{index}",
            net_after_costs=3.0,
            supportive_temporal_folds=3,
            null_probability=0.01,
            sample_size=100,
            untouched_holdout_passed=True,
            uncertainty="injected_strong_control",
            **common,
        )
        for index in range(10)
    ]
    return calibrate_shadow_policy(negatives, weak, strong)


def _multitimeframe_smoke() -> dict[str, object]:
    timestamps = pd.date_range("2024-03-10T06:55:00Z", periods=20, freq="1min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": "ES",
            "active_contract": "ESH4",
            "open": range(20),
            "high": [value + 1 for value in range(20)],
            "low": [value - 1 for value in range(20)],
            "close": [value + 0.5 for value in range(20)],
            "volume": 1.0,
        }
    )
    bars = resample_closed_bars(frame, 5, as_of="2024-03-10T07:07:00Z")
    passed = len(bars) == 2 and bars["availability_timestamp"].max() <= pd.Timestamp(
        "2024-03-10T07:07:00Z"
    )
    return {
        "passed": bool(passed),
        "closed_5m_bars": len(bars),
        "incomplete_bars_exposed": 0 if passed else 1,
        "dst_boundary_date": "2024-03-10",
    }


def _archive_smoke() -> dict[str, object]:
    archive = QualityDiversityArchive()
    accepted = 0
    for index in range(16):
        ecology = ("equity", "metal", "energy", "relative_value")[index % 4]
        family = ("state_machine", "hazard", "geometry", "counterfactual")[index % 4]
        niche = ArchiveNiche(
            ecology,
            ("1m-15m", "5m-60m")[index % 2],
            ("intraday", "session")[index % 2],
            ("rth", "globex")[index % 2],
            family,
            ("alpha", "defensive")[index % 2],
            ("low", "moderate")[index % 2],
            f"cluster_{index}",
        )
        spec = {"structure": index, "family": family, "ecology": ecology}
        decision = archive.insert(
            ArchiveCandidate(
                f"control_{index}",
                structural_fingerprint(spec),
                f"lineage_{index}",
                family,
                niche,
                {"net_economics": float(index), "complexity": 1.0},
                spec,
            )
        )
        accepted += int(decision.accepted)
    duplicate = archive.insert(archive.candidates[0])
    summary = archive.summary()
    return {
        **summary,
        "accepted_controls": accepted,
        "duplicate_rejected": not duplicate.accepted,
        "passed": accepted >= 4 and not duplicate.accepted,
    }


def _shadow_smoke() -> dict[str, object]:
    specification = ShadowSpecification(
        strategy_id="synthetic_control",
        strategy_version="v1",
        feature_versions=("synthetic_v1",),
        markets=("MES",),
        timeframes=("1m", "15m"),
        session_rules={"timezone": "America/Chicago", "flatten": "15:10"},
        entry_rules={"type": "synthetic_control"},
        exit_rules={"holding_bars": 10},
        sizing={"contracts": 1},
        costs={"round_turn": 4.5},
        stale_data_seconds=90,
        expected_update_seconds=60,
        duplicate_signal_window_seconds=60,
        maximum_exposure=1.0,
        simulated_mll_floor=-2500.0,
        internal_daily_risk_limit=800.0,
        kill_conditions=("stale_data", "mll_floor"),
        logging={"jsonl": True},
        reconciliation={"startup": "fail_closed"},
        source_manifest_hash="synthetic_control_manifest",
    )
    runner = ShadowRunner(specification)
    now = datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc)
    signal = ShadowSignal("synthetic_control", "MES", 1, 1, now, now, 5000.0)
    filled = runner.process(
        signal,
        now=now,
        latest_data_at=now,
        market_price=5000.0,
        proposed_exposure=1.0,
        session_open=True,
        simulated_mll=-100.0,
        daily_pnl=0.0,
        slippage_per_unit=0.25,
        round_turn_cost=4.5,
    )
    duplicate = runner.process(
        signal,
        now=now,
        latest_data_at=now,
        market_price=5000.0,
        proposed_exposure=1.0,
        session_open=True,
        simulated_mll=-100.0,
        daily_pnl=0.0,
        slippage_per_unit=0.25,
        round_turn_cost=4.5,
    )
    stale_signal = ShadowSignal(
        "synthetic_control", "MES", -1, 1, now + timedelta(minutes=2), now, 5000.0
    )
    stale = runner.process(
        stale_signal,
        now=now + timedelta(minutes=2),
        latest_data_at=now,
        market_price=5000.0,
        proposed_exposure=-1.0,
        session_open=True,
        simulated_mll=-100.0,
        daily_pnl=0.0,
        slippage_per_unit=0.25,
        round_turn_cost=4.5,
    )
    return {
        "configuration_hash": specification.configuration_hash,
        "virtual_fill_passed": filled["status"] == "VIRTUAL_FILLED",
        "duplicate_fail_closed": duplicate.get("reason") == "duplicate_signal",
        "stale_data_fail_closed": stale.get("reason") == "stale_data",
        "outbound_orders": 0,
        "passed": all(
            [
                filled["status"] == "VIRTUAL_FILLED",
                duplicate.get("reason") == "duplicate_signal",
                stale.get("reason") == "stale_data",
            ]
        ),
    }


def _markdown_json(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    marker = "```json\n"
    start = text.find(marker)
    end = text.find("\n```", start + len(marker))
    if start < 0 or end < 0:
        raise FoundryBootstrapError(f"No JSON block in {path}")
    return json.loads(text[start + len(marker) : end])


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _write_immutable(path: Path, content: str) -> None:
    if path.exists() and path.read_text(encoding="utf-8") != content:
        raise FoundryBootstrapError(f"Refusing divergent immutable artifact: {path}")
    if path.exists():
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)
