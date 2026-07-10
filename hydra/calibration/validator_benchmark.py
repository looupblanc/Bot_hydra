from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.calibration.cost_hurdle_calibration import calibrated_atom_cost_policy
from hydra.calibration.injected_edges import inject_edge
from hydra.calibration.negative_controls import apply_negative_signal, negative_control_specs
from hydra.calibration.positive_controls import positive_control_specs
from hydra.calibration.power_analysis import approximate_power
from hydra.calibration.synthetic_markets import SyntheticMarketConfig, generate_synthetic_market
from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso


CALIBRATION_VERSION = "validator_calibration_v1"


ATTACK_CLASSIFICATION = {
    "target_leakage": "FATAL_MANDATORY",
    "lookahead": "FATAL_MANDATORY",
    "delayed_signal": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "sign_flipped_signal": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "block_shuffled_signal": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "event_time_jitter": "ROBUSTNESS_DIAGNOSTIC",
    "best_event_removed": "ROBUSTNESS_DIAGNOSTIC",
    "cost_stress": "ROBUSTNESS_DIAGNOSTIC",
    "momentum_baseline": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "mean_reversion_baseline": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "session_only_baseline": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "volatility_only_baseline": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "opportunity_count_matched_random": "FATAL_MANDATORY",
    "placebo_market": "INFORMATIONAL_ONLY",
}


@dataclass(frozen=True)
class ControlDecision:
    control_id: str
    expected_positive: bool
    effect: float
    observations: int
    null_effect: float
    signal_to_noise: float
    passed: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ValidatorCalibrationResult:
    version: str
    created_at_utc: str
    negative_controls: tuple[ControlDecision, ...]
    positive_controls: tuple[ControlDecision, ...]
    false_positive_rate: float
    power_on_meaningful_effects: float
    precision: float
    recall: float
    cost_policy: dict[str, Any]
    attack_classification: dict[str, str]
    zero_pass_diagnosis: dict[str, Any]
    passed: bool

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["negative_controls"] = [item.to_dict() for item in self.negative_controls]
        out["positive_controls"] = [item.to_dict() for item in self.positive_controls]
        return out


def benchmark_validator(*, seed: int = 9050, previous_report: str | Path | None = None) -> ValidatorCalibrationResult:
    base = generate_synthetic_market(SyntheticMarketConfig("SYN", rows=6000, seed=seed))
    negative: list[ControlDecision] = []
    positive: list[ControlDecision] = []
    for index, spec in enumerate(negative_control_specs()):
        frame = apply_negative_signal(base, spec, seed=seed + index)
        negative.append(_decide_control(frame, f"signal_{spec.control_id}", spec.control_id, expected_positive=False, horizon=20))
    for index, spec in enumerate(positive_control_specs()):
        frame = inject_edge(base, spec, seed=seed + 100 + index)
        positive.append(_decide_control(frame, f"signal_{spec.edge_id}", spec.edge_id, expected_positive=True, horizon=spec.horizon))
    false_positive_rate = sum(1 for item in negative if item.passed) / max(len(negative), 1)
    power = sum(1 for item in positive if item.passed) / max(len(positive), 1)
    tp = sum(1 for item in positive if item.passed)
    fp = sum(1 for item in negative if item.passed)
    fn = sum(1 for item in positive if not item.passed)
    precision = tp / max(tp + fp, 1)
    recall = tp / max(tp + fn, 1)
    zero_pass = audit_previous_zero_pass(previous_report)
    passed = false_positive_rate <= 0.20 and power >= 0.80 and precision >= 0.80 and recall >= 0.80
    return ValidatorCalibrationResult(
        version=CALIBRATION_VERSION,
        created_at_utc=utc_now_iso(),
        negative_controls=tuple(negative),
        positive_controls=tuple(positive),
        false_positive_rate=float(false_positive_rate),
        power_on_meaningful_effects=float(power),
        precision=float(precision),
        recall=float(recall),
        cost_policy=calibrated_atom_cost_policy().to_dict(),
        attack_classification=dict(ATTACK_CLASSIFICATION),
        zero_pass_diagnosis=zero_pass,
        passed=passed,
    )


def write_calibration_report(result: ValidatorCalibrationResult, *, tag: str = "validator_calibration_v1") -> Path:
    stamp = utc_now_iso().replace("-", "").replace(":", "").replace("+00:00", "Z")
    path = project_path("reports", "calibration", f"validator_calibration_{stamp}_{tag}.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "# HYDRA Validator Calibration\n\n"
        "Historical research only. No live trading approval.\n\n"
        "```json\n"
        + json.dumps(result.to_dict(), indent=2, sort_keys=True, default=str)
        + "\n```\n",
        encoding="utf-8",
    )
    return path


def _decide_control(frame: pd.DataFrame, signal_col: str, control_id: str, *, expected_positive: bool, horizon: int) -> ControlDecision:
    signal = frame[signal_col].astype(float)
    future = frame["close"].pct_change(horizon).shift(-horizon)
    events = pd.DataFrame({"signal": signal, "future": future}).dropna()
    events = events[events["signal"] != 0]
    if events.empty:
        return ControlDecision(control_id, expected_positive, 0.0, 0, 0.0, 0.0, False, "no_events")
    signed = np.sign(events["signal"]) * events["future"]
    effect = float(signed.mean())
    shuffled = signed.sample(frac=1.0, random_state=17).reset_index(drop=True)
    null_effect = float(shuffled.iloc[::2].mean() - shuffled.iloc[1::2].mean())
    sigma = float(signed.std(ddof=1) or 1e-12)
    power = approximate_power(effect, len(events), sigma).approximate_power
    signal_to_noise = abs(effect) / max(sigma / (len(events) ** 0.5), 1e-12)
    passed = signal_to_noise >= 2.0 and abs(effect) > abs(null_effect) * 1.25 and power >= 0.50
    if expected_positive and passed:
        reason = "known_injected_effect_detected"
    elif expected_positive:
        reason = "known_injected_effect_missed"
    elif passed:
        reason = "false_positive_control_passed"
    else:
        reason = "null_control_rejected"
    return ControlDecision(control_id, expected_positive, effect, int(len(events)), null_effect, float(signal_to_noise), bool(passed), reason)


def audit_previous_zero_pass(previous_report: str | Path | None) -> dict[str, Any]:
    if previous_report is None:
        previous_report = "reports/edge_atom_lab/edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md"
    path = project_path(str(previous_report))
    if not path.exists():
        return {"status": "previous_report_missing", "cause": "UNRESOLVED"}
    match = re.search(r"```json\n(.*)\n```", path.read_text(encoding="utf-8"), re.S)
    if not match:
        return {"status": "previous_report_unparseable", "cause": "UNRESOLVED"}
    summary = json.loads(match.group(1))
    top = summary.get("top_atom_results", [])
    failed_attacks = Counter()
    cost_hurdle_failures = 0
    direction_failures = 0
    for row in top:
        for attack in row.get("adversarial", {}).get("attacks_failed", []):
            failed_attacks[str(attack)] += 1
        if row.get("effect_after_cost_hurdle", 0.0) < 0:
            cost_hurdle_failures += 1
        if row.get("failure_reason") == "effect_direction_opposes_preregistered_direction":
            direction_failures += 1
    universal_or_common = failed_attacks.most_common(8)
    if cost_hurdle_failures >= max(1, len(top) // 2):
        cause = "MULTIPLE_CAUSES_COST_HURDLE_AND_OVERSTRICT_ATTACK_POLICY"
    elif universal_or_common and universal_or_common[0][1] >= max(1, len(top) // 2):
        cause = "OVERSTRICT_OR_UNCALIBRATED_MANDATORY_ATTACK_POLICY"
    else:
        cause = "LIKELY_REAL_NEGATIVE_WITH_VALIDATOR_CALIBRATION_REQUIRED"
    return {
        "status": "audited",
        "cause": cause,
        "atoms_reported": summary.get("atoms_screened"),
        "adversarial_passes": summary.get("adversarial_passes"),
        "top_atoms_audited": len(top),
        "cost_hurdle_failures_in_top_atoms": cost_hurdle_failures,
        "direction_failures_in_top_atoms": direction_failures,
        "common_failed_attacks": universal_or_common,
        "attack_policy_diagnosis": "Previous validator required all listed attacks for all atom families; calibrated policy separates fatal, hypothesis-specific, diagnostic, and informational attacks.",
        "cost_policy_diagnosis": "Previous atom-stage hurdle compared raw atom effects against an executable strategy-like cost envelope; calibrated policy separates atom statistical evidence from strategy execution cost.",
    }

