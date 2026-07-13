from __future__ import annotations

import hashlib
import itertools
import json
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence


TOURNAMENT_ID = "hydra_v7_boosted_dual_track_tournament_0001"
GRAMMAR_VERSION = "hydra_v7_boosted_mechanism_grammar_v1"
HOLDING_MINUTES = (15, 30, 60, 90)


@dataclass(frozen=True, slots=True)
class MechanismMotif:
    motif: str
    direction_policy: str
    condition_id: str
    structural_rationale: str


@dataclass(frozen=True, slots=True)
class MechanismFamily:
    family_id: str
    economic_hypothesis: str
    payer: str
    persistence_rationale: str
    cemetery_distance: str
    motifs: tuple[MechanismMotif, ...]


@dataclass(frozen=True, slots=True)
class TournamentCandidateSpec:
    candidate_id: str
    family_id: str
    motif: str
    direction_policy: str
    condition_id: str
    holding_minutes: int
    structure_id: str
    specification_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def stable_hash(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def mechanism_families() -> tuple[MechanismFamily, ...]:
    return (
        MechanismFamily(
            family_id="TRAPPED_IMMEDIACY_UNWIND",
            economic_hypothesis=(
                "Aggressive traders who pay immediacy without obtaining price "
                "progress become trapped inventory and fund the subsequent unwind."
            ),
            payer="late aggressive takers whose completed flow failed to displace price",
            persistence_rationale=(
                "Inventory transfer and risk limits recur across sessions even when "
                "the exact participation threshold changes."
            ),
            cemetery_distance=(
                "Uses joint completed-minute price impact, arrival, occupancy and "
                "session state; it is not the old single-minute effort/progress or "
                "static occupancy grammar."
            ),
            motifs=(
                _motif("RATE_LOW_PROGRESS", "REVERSAL"),
                _motif("VOLUME_LOW_PROGRESS", "REVERSAL"),
                _motif("FLOW_LOW_EFFICIENCY", "REVERSAL"),
                _motif("BURST_LOW_DISPLACEMENT", "REVERSAL"),
                _motif("PATH_RECAPTURE", "REVERSAL"),
                _motif("REVISIT_FLOW_STALL", "REVERSAL"),
                _motif("FLOW_FLIP_AFTER_STALL", "REVERSAL"),
                _motif("SESSION_EXTREME_STALL", "REVERSAL"),
            ),
        ),
        MechanismFamily(
            family_id="INFORMED_SPLIT_ORDER_PERSISTENCE",
            economic_hypothesis=(
                "A large informed participant splitting an order over completed "
                "event intervals leaves persistent signed participation that slower "
                "liquidity providers fund until the inventory transfer completes."
            ),
            payer="passive liquidity and late counter-trend traders during order splitting",
            persistence_rationale=(
                "Large orders must still be fragmented to limit market impact and "
                "therefore create serially persistent observable participation."
            ),
            cemetery_distance=(
                "Tests multi-minute persistence and completion, not mini/micro latency, "
                "a raw flow-sign sequence, or contemporaneous arrival topology."
            ),
            motifs=(
                _motif("DISTRIBUTED_FLOW", "CONTINUATION"),
                _motif("CONCENTRATED_FLOW", "CONTINUATION"),
                _motif("TAIL_RUN_FLOW_ALIGN", "CONTINUATION"),
                _motif("HALF_MINUTE_FLOW_PERSIST", "CONTINUATION"),
                _motif("MODE_FLOW_ALIGN", "CONTINUATION"),
                _motif("CUMULATIVE_FLOW_ACCELERATION", "CONTINUATION"),
                _motif("VOLUME_WITH_PROGRESS", "CONTINUATION"),
                _motif("SIZE_INTENSITY_PERSISTENCE", "CONTINUATION"),
            ),
        ),
        MechanismFamily(
            family_id="SESSION_INVENTORY_HANDOFF",
            economic_hypothesis=(
                "Inventory accumulated during one fixed RTH phase is transferred to "
                "the next participant cohort, creating either underreaction catch-up "
                "or a priced-in inventory unwind."
            ),
            payer="participants forced to transfer opening or midday inventory across session cohorts",
            persistence_rationale=(
                "RTH staffing, benchmark and risk-transfer phases recur at stable "
                "exchange times even as directional news changes."
            ),
            cemetery_distance=(
                "Uses completed D1 event-state inventory and fixed phase handoffs, not "
                "legacy OHLC session geometry or unconstrained time-of-day thresholds."
            ),
            motifs=(
                _motif("OPEN_FLOW_UNDERREACTION", "CONTINUATION"),
                _motif("OPEN_FLOW_OVERREACTION", "REVERSAL"),
                _motif("OPEN_RANGE_ACCEPTANCE", "CONTINUATION"),
                _motif("OPEN_RANGE_REJECTION", "REVERSAL"),
                _motif("MIDDAY_FLOW_CATCHUP", "CONTINUATION"),
                _motif("MIDDAY_INVENTORY_UNWIND", "REVERSAL"),
                _motif("LATE_FLOW_RELEASE", "CONTINUATION"),
                _motif("LATE_INVENTORY_REBALANCE", "REVERSAL"),
            ),
        ),
        MechanismFamily(
            family_id="MULTISCALE_EVENT_STATE_TRANSITION",
            economic_hypothesis=(
                "A newly completed short event-state either confirms or breaks a "
                "slower inventory state, changing continuation versus reversal hazard."
            ),
            payer="slow participants whose inventory state is challenged by a faster completed flow transition",
            persistence_rationale=(
                "Execution horizons remain heterogeneous, so fast and slow event "
                "states can carry different inventory information."
            ),
            cemetery_distance=(
                "Uses causal 5/15/30-minute completed-state transitions and hazard "
                "direction; it is not a single-clock topology or parameter neighbor."
            ),
            motifs=(
                _motif("FLOW_5_BREAKS_30", "REVERSAL"),
                _motif("FLOW_5_CONFIRMS_30", "CONTINUATION"),
                _motif("RATE_5_ACCELERATES_15", "CONTINUATION"),
                _motif("RATE_5_DECAYS_30", "REVERSAL"),
                _motif("PATH_5_EXPANDS_30", "CONTINUATION"),
                _motif("PATH_5_COMPRESSES_30", "REVERSAL"),
                _motif("FLOW_DISAGREEMENT_FAST", "REVERSAL"),
                _motif("FLOW_DISAGREEMENT_SLOW", "CONTINUATION"),
            ),
        ),
        MechanismFamily(
            family_id="LIQUIDITY_SHOCK_RECOVERY_HAZARD",
            economic_hypothesis=(
                "A completed execution shock reveals whether liquidity replenished; "
                "recovery transfers losses to exhausted takers while non-recovery "
                "continues against passive liquidity that withdrew."
            ),
            payer="exhausted aggressive takers on recovery or withdrawn passive liquidity on continuation",
            persistence_rationale=(
                "Finite liquidity and replenishment latency recur whenever order flow "
                "arrives faster than passive risk can recycle."
            ),
            cemetery_distance=(
                "Conditions on post-shock recovery state and target/adverse hazard, not "
                "raw sweep shape, old print absorption, or static range expansion."
            ),
            motifs=(
                _motif("PATH_SHOCK_RECOVERY", "REVERSAL"),
                _motif("PATH_SHOCK_CONTINUATION", "CONTINUATION"),
                _motif("VOLUME_SHOCK_RECOVERY", "REVERSAL"),
                _motif("RATE_SHOCK_RECOVERY", "REVERSAL"),
                _motif("EXTREME_CLOSE_RECAPTURE", "REVERSAL"),
                _motif("EXTREME_CLOSE_ACCEPTANCE", "CONTINUATION"),
                _motif("TAIL_RUN_EXHAUSTION", "REVERSAL"),
                _motif("TAIL_RUN_BREAKOUT", "CONTINUATION"),
            ),
        ),
        MechanismFamily(
            family_id="AUCTION_VALUE_MIGRATION_SEQUENCE",
            economic_hypothesis=(
                "Persistent migration of executed value across completed minutes "
                "marks inventory acceptance; failed migration marks rejection and "
                "forces the initiating inventory to unwind."
            ),
            payer="inventory holders caught on the wrong side of accepted or rejected executed value",
            persistence_rationale=(
                "Auctions must continually discover a price that clears inventory, "
                "creating repeated acceptance and rejection sequences."
            ),
            cemetery_distance=(
                "Tests temporal value migration and acceptance sequences; G12 tested "
                "the exact static occupancy topology and remains tombstoned."
            ),
            motifs=(
                _motif("MODE_MIGRATION_3", "CONTINUATION"),
                _motif("MODE_MIGRATION_5", "CONTINUATION"),
                _motif("MODE_PRICE_ACCEPTANCE", "CONTINUATION"),
                _motif("MODE_PRICE_REJECTION", "REVERSAL"),
                _motif("BIMODAL_TO_UNIMODAL", "CONTINUATION"),
                _motif("UNIMODAL_TO_BIMODAL", "REVERSAL"),
                _motif("REVISIT_DECAY_RELEASE", "CONTINUATION"),
                _motif("REVISIT_SURGE_REJECTION", "REVERSAL"),
            ),
        ),
        MechanismFamily(
            family_id="INDEPENDENT_OPPORTUNITY_CONFLUENCE",
            economic_hypothesis=(
                "When independent completed event states agree, the density of useful "
                "opportunities rises faster than adverse-excursion hazard because the "
                "same inventory conclusion is visible through different observables."
            ),
            payer="participants reacting to only one of several independent inventory observables",
            persistence_rationale=(
                "Participation, impact, arrival and value are distinct constraints and "
                "their causal agreement can reduce false single-feature states."
            ),
            cemetery_distance=(
                "Requires cross-domain agreement with pre-fixed components; it is not "
                "the earlier event-cluster count or a score-optimized conjunction."
            ),
            motifs=(
                _motif("FLOW_RATE_PATH", "CONTINUATION"),
                _motif("VOLUME_MODE_MIGRATION", "CONTINUATION"),
                _motif("FLOW_SESSION_TREND", "CONTINUATION"),
                _motif("ARRIVAL_FLOW", "CONTINUATION"),
                _motif("TAIL_MODE", "CONTINUATION"),
                _motif("INTRAMINUTE_FLOW", "CONTINUATION"),
                _motif("LOW_RISK_DENSITY", "CONTINUATION"),
                _motif("HIGH_OPPORTUNITY_DENSITY", "CONTINUATION"),
            ),
        ),
        MechanismFamily(
            family_id="SCHEDULED_LOW_TURNOVER_RELEASE",
            economic_hypothesis=(
                "At fixed RTH risk-transfer windows, one high-quality completed state "
                "can capture inventory release while avoiding the cost and correlation "
                "of repeated intraday entries."
            ),
            payer="participants forced to rebalance inventory at scheduled RTH handoffs",
            persistence_rationale=(
                "Benchmark, staffing and risk-cut windows recur; limiting each motif to "
                "one signal per session keeps the edge operationally tradable."
            ),
            cemetery_distance=(
                "Combines pre-fixed event evidence with one-shot scheduling and explicit "
                "cost control; it is not legacy scheduled OHLC geometry."
            ),
            motifs=(
                _motif("OPEN_RELEASE", "CONTINUATION"),
                _motif("MIDMORNING_RELEASE", "CONTINUATION"),
                _motif("MIDDAY_RELEASE", "CONTINUATION"),
                _motif("AFTERNOON_RELEASE", "CONTINUATION"),
                _motif("OPEN_REVERSAL", "REVERSAL"),
                _motif("MIDDAY_REVERSAL", "REVERSAL"),
                _motif("AFTERNOON_CONTINUATION", "CONTINUATION"),
                _motif("CLOSE_REBALANCE", "REVERSAL"),
            ),
        ),
    )


def candidate_specs() -> tuple[TournamentCandidateSpec, ...]:
    rows: list[TournamentCandidateSpec] = []
    for family in mechanism_families():
        for motif in family.motifs:
            structure_payload = {
                "grammar_version": GRAMMAR_VERSION,
                "family_id": family.family_id,
                "motif": motif.motif,
                "condition_id": motif.condition_id,
                "direction_policy": motif.direction_policy,
                "feature_availability": "completed_minute_shift_one",
                "entry": "next_completed_minute_open",
                "session": "ES_RTH",
            }
            structure_hash = stable_hash(structure_payload)
            structure_id = f"v7bt_{family.family_id.lower()}_{motif.motif.lower()}"
            for holding_minutes in HOLDING_MINUTES:
                payload = {
                    **structure_payload,
                    "structure_id": structure_id,
                    "holding_minutes": holding_minutes,
                    "cost_profiles": ["BASE", "STRESS_1_5X", "STRESS_2X"],
                    "sizing": "one_ES_contract_research_only",
                }
                spec_hash = stable_hash(payload)
                rows.append(
                    TournamentCandidateSpec(
                        candidate_id=f"{structure_id}_h{holding_minutes}",
                        family_id=family.family_id,
                        motif=motif.motif,
                        direction_policy=motif.direction_policy,
                        condition_id=motif.condition_id,
                        holding_minutes=holding_minutes,
                        structure_id=structure_id,
                        specification_hash=spec_hash,
                    )
                )
    ordered = tuple(sorted(rows, key=lambda row: row.candidate_id))
    if len(ordered) != 256:
        raise RuntimeError(f"boosted tournament candidate-count drift: {len(ordered)}")
    if len({row.candidate_id for row in ordered}) != len(ordered):
        raise RuntimeError("boosted tournament candidate IDs are not unique")
    if len({row.specification_hash for row in ordered}) != len(ordered):
        raise RuntimeError("boosted tournament specifications are not unique")
    return ordered


def bounded_basket_structures(
    component_ids: Sequence[str],
    *,
    new_component_ids: Sequence[str],
    role_map: Mapping[str, str],
    maximum_count: int = 320,
) -> tuple[dict[str, Any], ...]:
    components = tuple(sorted(set(str(value) for value in component_ids)))
    new_ids = frozenset(str(value) for value in new_component_ids)
    if len(components) < 4 or not new_ids.issubset(components):
        raise ValueError("invalid boosted component bank")
    rows: list[dict[str, Any]] = []
    for size in range(2, 5):
        for members in itertools.combinations(components, size):
            if not new_ids.intersection(members):
                continue
            profiles = ["UNIT_EQUAL"]
            if any(role_map.get(member) == "TARGET_VELOCITY" for member in members):
                profiles.append("TARGET_VELOCITY_TILT")
            for profile in profiles:
                payload = {
                    "component_ids": list(members),
                    "allocation_profile": profile,
                    "risk_units": "one_each_plus_one_target_velocity_when_tilted",
                    "maximum_simultaneous_positions": 2,
                    "conflict_priority": "design_STRESS_1_5X_net_desc_then_id",
                    "policy_version": "hydra_v7_boosted_static_basket_v1",
                }
                fingerprint = stable_hash(payload)
                rows.append(
                    {
                        **payload,
                        "basket_structure_id": f"v7bt_basket_{fingerprint[:20]}",
                        "structural_hash": fingerprint,
                    }
                )
    ordered = sorted(rows, key=lambda row: str(row["structural_hash"]))
    selected = tuple(ordered[:maximum_count])
    if len(selected) != maximum_count:
        raise RuntimeError("boosted basket population is below the frozen target")
    return selected


def _motif(name: str, direction: str) -> MechanismMotif:
    return MechanismMotif(
        motif=name,
        direction_policy=direction,
        condition_id=f"{name}_V1",
        structural_rationale=(
            "A distinct pre-fixed logical graph; holding horizon is the only bounded "
            "alternative and no threshold is selected from outcomes."
        ),
    )


__all__ = [
    "GRAMMAR_VERSION",
    "HOLDING_MINUTES",
    "MechanismFamily",
    "MechanismMotif",
    "TOURNAMENT_ID",
    "TournamentCandidateSpec",
    "bounded_basket_structures",
    "candidate_specs",
    "mechanism_families",
    "stable_hash",
]
