from __future__ import annotations

import itertools
from collections import Counter

from hydra.atoms.schema import EdgeAtomHypothesis
from hydra.utils.time import utc_now_iso


FAMILY_SPECS = {
    "session_inventory_acceptance": {
        "features": ("overnight_displacement", "overnight_participation", "acceptance_rejection", "prior_value_distance"),
        "mechanism": "Inventory accumulated outside regular liquidity may be accepted or rejected after the RTH liquidity regime changes.",
        "participants": "overnight inventory holders, opening liquidity providers, forced short-horizon risk reducers",
    },
    "effort_vs_progress": {
        "features": ("effort_progress_ratio", "repeated_extension_failure", "directional_pressure_without_progress"),
        "mechanism": "Large signed path effort with weak realized progress can identify absorption or exhaustion visible in OHLCV path geometry.",
        "participants": "short-horizon momentum traders, liquidity providers absorbing pressure",
    },
    "accepted_price_migration": {
        "features": ("accepted_center_slope", "extreme_dwell", "failed_relocation", "old_region_reentry"),
        "mechanism": "Movement or failure of accepted price regions can precede continuation or return to prior value.",
        "participants": "intraday inventory managers, benchmark and VWAP execution flow",
    },
    "volatility_path_shape": {
        "features": ("rv_short_long_ratio", "compression_persistence", "failed_expansion", "vol_without_displacement"),
        "mechanism": "Volatility shape contains information about whether risk transfer is building or decaying.",
        "participants": "volatility-sensitive execution, market makers adjusting spread and inventory",
    },
    "session_transition_state": {
        "features": ("open_to_mid_efficiency_shift", "mid_to_late_pressure_decay", "transition_participation_shift"),
        "mechanism": "Return efficiency and participation changes across session boundaries can persist briefly after transition.",
        "participants": "session-specific liquidity demand and execution benchmark flow",
    },
    "contract_roll_invariant_relative_state": {
        "features": ("micro_mini_basis_pressure", "same_family_residual_z", "roll_excluded_residual_state"),
        "mechanism": "Explicit synchronized contracts can reveal transient relative state without reusing killed NQ/ES directional proxy logic.",
        "participants": "spread traders and hedgers maintaining contract-equivalent exposure",
    },
    "cross_market_risk_transfer": {
        "features": ("lagged_index_to_metal_response", "lagged_index_to_energy_response", "largecap_smallcap_lagged_residual"),
        "mechanism": "Risk transfer can propagate across ecologies with lag when one market reprices before another.",
        "participants": "macro hedgers, cross-asset allocators, index arbitrage desks",
    },
    "distribution_tail_state": {
        "features": ("downside_upside_variance_ratio", "tail_cluster_state", "recovery_speed_after_loss"),
        "mechanism": "Recent asymmetry and tail clustering may predict elevated future path risk or rebound behavior.",
        "participants": "risk managers and option-hedging flow reacting to realized tail state",
    },
    "calendar_participation_structure": {
        "features": ("weekday_state_interaction", "session_volume_state_interaction", "calendar_volatility_interaction"),
        "mechanism": "Calendar effects are only admissible when interacting with state-dependent participation or volatility.",
        "participants": "scheduled institutional execution flow and liquidity provision calendars",
    },
    "defensive_portfolio_atom": {
        "features": ("drawdown_risk_state", "shared_loss_risk_state", "tail_avoidance_state"),
        "mechanism": "States predicting poor future path quality can be valuable as risk-off atoms even without direct alpha.",
        "participants": "prop-firm account risk managers and volatility-sensitive allocators",
    },
}


def generate_edge_atom_hypotheses(
    *,
    markets: list[str],
    code_commit: str,
    max_atoms: int,
    max_family_share: float,
    max_variants: int,
    seed: int = 0,
) -> list[EdgeAtomHypothesis]:
    del seed
    timestamp = utc_now_iso()
    families = list(FAMILY_SPECS)
    per_family_cap = max(1, int(max_atoms * max_family_share))
    out: list[EdgeAtomHypothesis] = []
    family_counts: Counter[str] = Counter()
    horizons = (15, 30, 60)
    thresholds = ("moderate", "high", "extreme", "low")
    for family, spec in itertools.cycle(FAMILY_SPECS.items()):
        if len(out) >= max_atoms:
            break
        if family_counts[family] >= per_family_cap:
            if all(family_counts[item] >= per_family_cap for item in families):
                break
            continue
        feature_index = family_counts[family] % len(spec["features"])
        variant = family_counts[family] // len(spec["features"])
        if variant >= max_variants:
            family_counts[family] = per_family_cap
            continue
        feature = spec["features"][feature_index]
        horizon = horizons[(feature_index + variant) % len(horizons)]
        market = markets[(len(out) + feature_index) % len(markets)]
        target_markets = _target_markets_for_family(family, markets, len(out), feature_index)
        threshold = thresholds[variant % len(thresholds)]
        atom_id = f"atom_{family}_{feature}_{market}_{horizon}_{threshold}_v1"
        out.append(
            EdgeAtomHypothesis(
                atom_id=atom_id,
                family=family,
                feature_key=feature,
                economic_mechanism=spec["mechanism"],
                participants=spec["participants"],
                information_set="Past OHLCV bars, explicit-contract identity, session timestamp, and roll-exclusion flag only.",
                target_variable="future_return_and_path_quality",
                expected_direction=_expected_direction(feature),
                horizon_bars=horizon,
                target_markets=target_markets or (market,),
                favorable_regimes="Mechanism-specific state occurrence with sufficient liquidity and no roll-adjacent event.",
                failure_regimes="Roll windows, low opportunity count, state instability, or simpler baseline explaining the effect.",
                transaction_cost_hurdle=0.0,
                roll_sensitivity="roll-adjacent events excluded; explicit contract required",
                minimum_effect=0.00003,
                primary_null="opportunity_count_matched_random_events",
                mandatory_nulls=(
                    "delayed_signal",
                    "sign_flipped_signal",
                    "block_shuffled_signal",
                    "best_event_removed",
                    "momentum_baseline",
                    "mean_reversion_baseline",
                    "session_only_baseline",
                    "volatility_only_baseline",
                ),
                replication_requirement="Expected direction in at least 3 meaningful temporal folds and no dominant event concentration.",
                falsification_rule="Falsify if mandatory nulls explain the effect, sign is unstable, or effect fails cost/evidence thresholds.",
                max_parameter_degrees=2,
                timestamp_utc=timestamp,
                code_commit=code_commit,
                parameters={"threshold": threshold, "horizon_bars": horizon},
            )
        )
        family_counts[family] += 1
    return out


def _expected_direction(feature: str) -> int:
    reverse_features = {
        "effort_progress_ratio",
        "repeated_extension_failure",
        "failed_relocation",
        "vol_without_displacement",
        "drawdown_risk_state",
        "shared_loss_risk_state",
        "tail_avoidance_state",
        "downside_upside_variance_ratio",
    }
    return -1 if feature in reverse_features else 1


def _target_markets_for_family(family: str, markets: list[str], offset: int, feature_index: int) -> tuple[str, ...]:
    if not markets:
        return ()
    if family in {"cross_market_risk_transfer", "contract_roll_invariant_relative_state"} and len(markets) >= 2:
        first = markets[(offset + feature_index) % len(markets)]
        second = markets[(offset + feature_index + 1) % len(markets)]
        if first == second and len(markets) > 1:
            second = markets[(offset + feature_index + 2) % len(markets)]
        return tuple(dict.fromkeys((first, second)))
    if family == "defensive_portfolio_atom" and len(markets) >= 3:
        start = (offset + feature_index) % len(markets)
        return tuple(markets[start:] + markets[:start])[: min(3, len(markets))]
    return (markets[(offset + feature_index) % len(markets)],)
