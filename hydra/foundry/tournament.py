from __future__ import annotations

import hashlib
import json
from collections import Counter
from typing import Any

from hydra.factory.quality_diversity import (
    ArchiveCandidate,
    ArchiveNiche,
    QualityDiversityArchive,
    structural_fingerprint,
)


ENGINE_FAMILIES = (
    ("ENGINE_A_DIRECT_STATE_MACHINE", "direct_state_machine"),
    ("ENGINE_B_DISTRIBUTIONAL_HAZARD", "distributional_hazard"),
    ("ENGINE_C_MARKET_STATE_GEOMETRY", "market_state_geometry"),
    ("ENGINE_D_COUNTERFACTUAL_EVENTS", "counterfactual_events"),
    ("ENGINE_E_INVARIANT_SEARCH", "invariant_mechanism"),
    ("ENGINE_F_RELATIVE_VALUE", "relative_value"),
    ("ENGINE_G_CONSTRAINED_ML", "constrained_ml"),
    ("ENGINE_H_DEFENSIVE_PORTFOLIO", "defensive_portfolio"),
    ("ENGINE_J_INVENTED_METHODS", "invented_method"),
)

ECOLOGIES = (
    ("equity_indices", ("ES", "MES", "NQ", "MNQ", "RTY", "M2K", "YM", "MYM")),
    ("metals", ("GC", "MGC")),
    ("energy", ("CL", "MCL")),
    ("relative_value", ("ES-NQ", "RTY-YM", "GC-CL")),
)

TIMEFRAME_PROFILES = (
    "1m",
    "1m_execution_5m_state",
    "1m_execution_15m_state",
    "5m_execution_30m_state",
    "5m_execution_60m_state",
    "intraday_execution_session_state",
    "intraday_execution_daily_state",
    "multi_session_context",
)

HORIZONS = ("5m", "15m", "30m", "60m", "session", "multi_day")
SESSIONS = ("rth_open", "rth_mid", "rth_close", "globex", "overnight", "all_liquid")
ROLES = ("trend", "reversal", "relative_value", "risk_off", "tail_guard", "diversifier")


def run_structural_tournament(prototype_count: int = 576) -> dict[str, Any]:
    """Generate a preregisterable Stage-0 quality-diversity population.

    This is intentionally outcome-free. It proves diversified production,
    fingerprinting and allocation before expensive market replay; it does not
    promote a candidate or assert economics.
    """
    if prototype_count < 1:
        raise ValueError("A production tournament must contain prototypes.")
    archive = QualityDiversityArchive(niche_capacity=3)
    prototypes: list[dict[str, Any]] = []
    rejected = Counter()
    for index in range(prototype_count):
        engine, family = ENGINE_FAMILIES[index % len(ENGINE_FAMILIES)]
        ecology, markets = ECOLOGIES[index % len(ECOLOGIES)]
        timeframe = TIMEFRAME_PROFILES[index % len(TIMEFRAME_PROFILES)]
        horizon = HORIZONS[(index // len(TIMEFRAME_PROFILES)) % len(HORIZONS)]
        session = SESSIONS[(index // len(ENGINE_FAMILIES)) % len(SESSIONS)]
        role = ROLES[(index // len(ECOLOGIES)) % len(ROLES)]
        candidate_id = f"foundry_p0_{index:04d}_{family}"
        lineage_id = f"lineage_p0_{index:04d}"
        specification = {
            "engine": engine,
            "family": family,
            "market_ecology": ecology,
            "markets": markets,
            "timeframe_profile": timeframe,
            "holding_horizon": horizon,
            "session": session,
            "portfolio_role": role,
            "representation_seed": index // len(ENGINE_FAMILIES),
            "causal_availability": "closed_bars_only",
            "contract_policy": "explicit_roll_aware",
            "data_role": "development_only",
        }
        fingerprint = structural_fingerprint(specification)
        niche = ArchiveNiche(
            ecology,
            timeframe,
            horizon,
            session,
            family,
            role,
            "low" if horizon in {"session", "multi_day"} else "moderate",
            f"p0_cluster_{index:04d}",
        )
        decision = archive.insert(
            ArchiveCandidate(
                candidate_id,
                fingerprint,
                lineage_id,
                family,
                niche,
                {
                    "behavioral_novelty": 1.0,
                    "execution_confidence": 0.5 if ecology != "relative_value" else 0.25,
                    "complexity": 1.0 if engine == "ENGINE_A_DIRECT_STATE_MACHINE" else 2.0,
                },
                specification,
            )
        )
        if not decision.accepted:
            rejected[decision.reason] += 1
            continue
        prototypes.append(
            {
                "candidate_id": candidate_id,
                "lineage_id": lineage_id,
                "structural_fingerprint": fingerprint,
                "niche_key": niche.key,
                "specification": specification,
                "stage": "STAGE_0_VALIDITY_PENDING_MARKET_REPLAY",
            }
        )
    summary = archive.summary()
    allocations = {
        "engines": dict(Counter(row["specification"]["engine"] for row in prototypes)),
        "families": dict(Counter(row["specification"]["family"] for row in prototypes)),
        "market_ecologies": dict(
            Counter(row["specification"]["market_ecology"] for row in prototypes)
        ),
        "timeframe_profiles": dict(
            Counter(row["specification"]["timeframe_profile"] for row in prototypes)
        ),
        "portfolio_roles": dict(
            Counter(row["specification"]["portfolio_role"] for row in prototypes)
        ),
    }
    raw = json.dumps(prototypes, sort_keys=True, separators=(",", ":"), default=str)
    return {
        "requested_prototypes": prototype_count,
        "accepted_prototypes": len(prototypes),
        "rejected_prototypes": sum(rejected.values()),
        "rejection_reasons": dict(rejected),
        "archive": summary,
        "allocations": allocations,
        "population_sha256": hashlib.sha256(raw.encode()).hexdigest(),
        "prototypes": prototypes,
        "economic_claims": 0,
        "promotions": 0,
    }
