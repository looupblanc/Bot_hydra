from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np
import pandas as pd

from hydra.atoms.atom_library import atom_signal, future_return
from hydra.atoms.schema import AtomTestResult, EdgeAtomHypothesis
from hydra.validation.evidence_scope import ComputationMode, EvidenceScope
from hydra.validation.status_provenance import make_status_provenance


ATOM_VALIDATION_VERSION = "edge_atom_tester_v1"
ATOM_POLICY_VERSION = "edge_atom_policy_v1"


def test_atom(
    atom: EdgeAtomHypothesis,
    frame: pd.DataFrame,
    *,
    code_commit: str,
    data_fingerprint: str,
    family_effective_trials: int,
) -> AtomTestResult:
    subset = frame[frame["symbol"].isin(atom.target_markets)].copy()
    if subset.empty:
        return _empty_result(atom, "ATOM_INSUFFICIENT_EVIDENCE", "target_market_missing", code_commit, data_fingerprint)
    signal = atom_signal(subset, atom.feature_key, atom.expected_direction, str(atom.parameters.get("threshold", "moderate")))
    fwd = future_return(subset, atom.horizon_bars)
    aligned = pd.concat(
        [
            subset[["timestamp", "symbol", "close"]].reset_index(drop=True),
            signal.reset_index(drop=True).rename("signal"),
            fwd.reset_index(drop=True).rename("future_return"),
        ],
        axis=1,
    ).replace([np.inf, -np.inf], np.nan).dropna()
    events = aligned[aligned["signal"] != 0].copy()
    if len(events) < 50:
        return _empty_result(atom, "ATOM_INSUFFICIENT_EVIDENCE", "valid_event_count_below_50", code_commit, data_fingerprint, observations=len(events))
    signed = np.sign(events["signal"].astype(float)) * events["future_return"].astype(float)
    raw_effect = float(signed.mean())
    stderr = float(signed.std(ddof=1) / np.sqrt(len(signed))) if len(signed) > 1 else 0.0
    low = raw_effect - 1.96 * stderr
    high = raw_effect + 1.96 * stderr
    fold_results = _by_fold(events)
    market_results = _by_key(events, "symbol")
    contract_results = _contract_proxy(events)
    direction_ok = raw_effect * atom.expected_direction > 0 if atom.expected_direction != 0 else raw_effect > 0
    folds_positive = sum(1 for item in fold_results.values() if item["effect"] * atom.expected_direction > 0)
    markets_positive = sum(1 for item in market_results.values() if item["effect"] * atom.expected_direction > 0)
    contracts_positive = sum(1 for item in contract_results.values() if item["effect"] * atom.expected_direction > 0)
    concentration = _top_event_concentration(signed.tolist())
    cost_hurdle = _cost_hurdle(events)
    evidence_strength = max(0.0, abs(raw_effect) / max(stderr, 1e-9)) if stderr else 0.0
    adjusted = evidence_strength / max(np.sqrt(max(family_effective_trials, 1)), 1.0)
    failure_reason = None
    status = "ATOM_VALID"
    if not direction_ok:
        status = "ATOM_FALSIFIED"
        failure_reason = "effect_direction_opposes_preregistered_direction"
    elif abs(raw_effect) <= max(abs(cost_hurdle), atom.minimum_effect):
        status = "ATOM_FALSIFIED"
        failure_reason = "effect_below_cost_or_minimum_effect"
    elif folds_positive < 3:
        status = "ATOM_INSUFFICIENT_EVIDENCE"
        failure_reason = "temporal_replication_below_default_requirement"
    provenance = make_status_provenance(
        status=status,
        scope=EvidenceScope.EDGE_ATOM,
        payload=atom.to_dict() | {"events": len(events), "raw_effect": raw_effect},
        code_commit=code_commit,
        data_fingerprint=data_fingerprint,
        validation_version=ATOM_VALIDATION_VERSION,
        policy_version=ATOM_POLICY_VERSION,
        computation_mode=ComputationMode.FULL,
        evidence_strength=adjusted,
        passed=status == "ATOM_VALID",
    )
    return AtomTestResult(
        atom_id=atom.atom_id,
        family=atom.family,
        status=status,
        valid_observations=int(len(events)),
        state_frequency=float(len(events) / max(len(aligned), 1)),
        raw_effect=float(raw_effect),
        cost_hurdle=float(cost_hurdle),
        effect_after_cost_hurdle=float(abs(raw_effect) - abs(cost_hurdle)),
        confidence_low=float(low),
        confidence_high=float(high),
        direction_ok=bool(direction_ok),
        folds_positive=int(folds_positive),
        fold_count=len(fold_results),
        markets_positive=int(markets_positive),
        market_count=len(market_results),
        contracts_positive=int(contracts_positive),
        contract_count=len(contract_results),
        top_event_concentration=float(concentration),
        evidence_strength=float(evidence_strength),
        fdr_adjusted_evidence=float(adjusted),
        simplest_competing_explanation="unresolved_until_adversarial_validation",
        failure_reason=failure_reason,
        provenance=provenance.to_dict(),
        fold_results=fold_results,
        market_results=market_results,
        contract_results=contract_results,
        adversarial={},
    )


def _empty_result(
    atom: EdgeAtomHypothesis,
    status: str,
    reason: str,
    code_commit: str,
    data_fingerprint: str,
    observations: int = 0,
) -> AtomTestResult:
    provenance = make_status_provenance(
        status=status,
        scope=EvidenceScope.EDGE_ATOM,
        payload=atom.to_dict() | {"empty_reason": reason},
        code_commit=code_commit,
        data_fingerprint=data_fingerprint,
        validation_version=ATOM_VALIDATION_VERSION,
        policy_version=ATOM_POLICY_VERSION,
        computation_mode=ComputationMode.FULL,
        evidence_strength=0.0,
        passed=False,
    )
    return AtomTestResult(atom.atom_id, atom.family, status, observations, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, False, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, "not_testable", reason, provenance.to_dict(), {}, {}, {}, {})


def _by_fold(events: pd.DataFrame) -> dict[str, Any]:
    ts = pd.to_datetime(events["timestamp"], utc=True)
    folds = {
        "2023_h1": (ts >= "2023-01-01") & (ts < "2023-07-01"),
        "2023_h2": (ts >= "2023-07-01") & (ts < "2024-01-01"),
        "2024_q1": (ts >= "2024-01-01") & (ts < "2024-04-01"),
        "2024_q2": (ts >= "2024-04-01") & (ts < "2024-07-01"),
        "2024_q3": (ts >= "2024-07-01") & (ts < "2024-10-01"),
    }
    out = {}
    for name, mask in folds.items():
        subset = events.loc[mask]
        if len(subset):
            signed = np.sign(subset["signal"].astype(float)) * subset["future_return"].astype(float)
            out[name] = {"observations": int(len(subset)), "effect": float(signed.mean())}
    return out


def _by_key(events: pd.DataFrame, key: str) -> dict[str, Any]:
    out = {}
    for value, subset in events.groupby(key, sort=True):
        signed = np.sign(subset["signal"].astype(float)) * subset["future_return"].astype(float)
        out[str(value)] = {"observations": int(len(subset)), "effect": float(signed.mean())}
    return out


def _contract_proxy(events: pd.DataFrame) -> dict[str, Any]:
    ts = pd.to_datetime(events["timestamp"], utc=True)
    labels = ts.dt.year.astype(str) + "Q" + ts.dt.quarter.astype(str)
    out = {}
    for label, subset in events.groupby(labels, sort=True):
        signed = np.sign(subset["signal"].astype(float)) * subset["future_return"].astype(float)
        out[str(label)] = {"observations": int(len(subset)), "effect": float(signed.mean())}
    return out


def _top_event_concentration(values: list[float]) -> float:
    positives = sorted([value for value in values if value > 0], reverse=True)
    total = sum(positives)
    return float(positives[0] / total) if total > 0 and positives else 0.0


def _cost_hurdle(events: pd.DataFrame) -> float:
    hurdles = []
    for symbol, subset in events.groupby("symbol", sort=True):
        price = float(subset["close"].median())
        if price <= 0:
            continue
        if symbol.startswith("M"):
            hurdles.append(4.50 / max(price, 1.0))
        else:
            hurdles.append(9.00 / max(price, 1.0))
    return float(np.mean(hurdles)) if hurdles else 0.0
