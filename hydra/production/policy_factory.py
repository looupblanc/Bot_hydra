from __future__ import annotations

import hashlib
import json
import random
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from hydra.economic_evolution.schema import EconomicRole, SleeveSpec, stable_hash
from hydra.production.manifest import POLICY_CLASSES, ProductionManifestError
from hydra.research.economic_evolution_campaign import _sleeve_from_dict


@dataclass(frozen=True, slots=True)
class ComponentCandidate:
    sleeve: SleeveSpec
    development_evidence: dict[str, Any]
    source_roles: tuple[str, ...]
    primary_parent_ids: tuple[str, ...]

    @property
    def stressed_net(self) -> float:
        return float(self.development_evidence.get("cost_stress_1_5x_net") or 0.0)

    @property
    def normal_net(self) -> float:
        return float(self.development_evidence.get("net_pnl") or 0.0)

    @property
    def event_count(self) -> int:
        return int(
            self.development_evidence.get("events")
            or self.development_evidence.get("event_count")
            or 0
        )

    @property
    def priority(self) -> tuple[float, float, int, str]:
        return (
            self.stressed_net,
            self.normal_net,
            self.event_count,
            self.sleeve.sleeve_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "sleeve": self.sleeve.to_dict(),
            "development_evidence": dict(self.development_evidence),
            "source_roles": list(self.source_roles),
            "primary_parent_ids": list(self.primary_parent_ids),
            "status_inheritance": False,
            "validated": False,
        }


@dataclass(frozen=True, slots=True)
class ProductionPolicy:
    policy_id: str
    mechanism: str
    sleeve_ids: tuple[str, ...]
    component_priority: tuple[str, ...]
    risk_level: float
    risk_micro_units: int
    maximum_simultaneous_positions: int
    maximum_mini_equivalent: int
    conflict_policy: str
    route_parameters: tuple[tuple[str, str | int | float | bool], ...]
    parent_policy_ids: tuple[str, ...]
    structural_fingerprint: str
    behavioral_fingerprint: str
    source_campaign: str = "ECONOMIC_PRODUCTION_MANIFEST"
    development_only: bool = True
    validated: bool = False
    inherited_status: None = None
    baseline_role: str | None = None
    random_seed: int | None = None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["sleeve_ids"] = list(self.sleeve_ids)
        value["component_priority"] = list(self.component_priority)
        value["route_parameters"] = [list(row) for row in self.route_parameters]
        value["parent_policy_ids"] = list(self.parent_policy_ids)
        return value

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ProductionPolicy":
        return cls(
            policy_id=str(value["policy_id"]),
            mechanism=str(value["mechanism"]),
            sleeve_ids=tuple(str(row) for row in value["sleeve_ids"]),
            component_priority=tuple(str(row) for row in value["component_priority"]),
            risk_level=float(value["risk_level"]),
            risk_micro_units=int(value["risk_micro_units"]),
            maximum_simultaneous_positions=int(value["maximum_simultaneous_positions"]),
            maximum_mini_equivalent=int(value["maximum_mini_equivalent"]),
            conflict_policy=str(value["conflict_policy"]),
            route_parameters=tuple(
                (str(row[0]), row[1]) for row in value.get("route_parameters") or ()
            ),
            parent_policy_ids=tuple(str(row) for row in value.get("parent_policy_ids") or ()),
            structural_fingerprint=str(value["structural_fingerprint"]),
            behavioral_fingerprint=str(value["behavioral_fingerprint"]),
            source_campaign=str(value.get("source_campaign") or "ECONOMIC_PRODUCTION_MANIFEST"),
            development_only=bool(value.get("development_only", True)),
            validated=bool(value.get("validated", False)),
            inherited_status=None,
            baseline_role=(
                None
                if value.get("baseline_role") is None
                else str(value["baseline_role"])
            ),
            random_seed=(
                None if value.get("random_seed") is None else int(value["random_seed"])
            ),
        )


@dataclass(frozen=True, slots=True)
class PolicyPopulation:
    policies: tuple[ProductionPolicy, ...]
    requested_count: int
    attempted_count: int
    rejected_duplicate_count: int
    rejected_incompatible_count: int
    mechanism_counts: dict[str, int]

    @property
    def duplicate_rejection_rate(self) -> float:
        return self.rejected_duplicate_count / max(self.attempted_count, 1)

    def summary(self) -> dict[str, Any]:
        return {
            "requested_count": self.requested_count,
            "policy_count": len(self.policies),
            "attempted_count": self.attempted_count,
            "rejected_duplicate_count": self.rejected_duplicate_count,
            "rejected_incompatible_count": self.rejected_incompatible_count,
            "duplicate_rejection_rate": self.duplicate_rejection_rate,
            "mechanism_counts": dict(sorted(self.mechanism_counts.items())),
        }


def build_predeclared_control_bank(
    component_ids: Sequence[str],
    manifest: Mapping[str, Any],
) -> tuple[ProductionPolicy, ...]:
    """Freeze every matched-control identity before any 0024 outcome is read.

    The bank is deliberately small and enumerable: every singleton/static-risk
    parent, lexical equal-risk baskets of sizes 2--4, and fixed-seed random
    baskets of the same sizes.  LOBO may select only from these identities.
    """

    eligible = tuple(sorted({str(value) for value in component_ids if str(value)}))
    if len(eligible) < 4:
        raise ProductionManifestError(
            "predeclared control bank requires at least four eligible components"
        )
    frontier = tuple(
        zip(
            (float(value) for value in manifest["static_risk_frontier"]["normalized_levels"]),
            (int(value) for value in manifest["static_risk_frontier"]["micro_units"]),
            strict=True,
        )
    )
    seeds = tuple(int(value) for value in manifest["matched_controls"]["random_seeds"])
    output: list[ProductionPolicy] = []
    for risk_level, risk_micro_units in frontier:
        for component_id in eligible:
            output.append(
                _control_policy(
                    manifest=manifest,
                    role="BEST_PARENT_CANDIDATE",
                    sleeve_ids=(component_id,),
                    risk_level=risk_level,
                    risk_micro_units=risk_micro_units,
                )
            )
        for size in (2, 3, 4):
            output.append(
                _control_policy(
                    manifest=manifest,
                    role="EQUAL_RISK",
                    sleeve_ids=eligible[:size],
                    risk_level=risk_level,
                    risk_micro_units=risk_micro_units,
                )
            )
            for seed in seeds:
                chooser = random.Random(seed)
                chosen = tuple(chooser.sample(list(eligible), size))
                output.append(
                    _control_policy(
                        manifest=manifest,
                        role="RANDOM_SELECTION",
                        sleeve_ids=chosen,
                        risk_level=risk_level,
                        risk_micro_units=risk_micro_units,
                        random_seed=seed,
                    )
                )
    by_id = {row.policy_id: row for row in output}
    by_behavior = {row.behavioral_fingerprint: row for row in output}
    if len(by_id) != len(output) or len(by_behavior) != len(output):
        raise ProductionManifestError("predeclared control bank contains a duplicate identity")
    return tuple(sorted(output, key=lambda row: row.policy_id))


def _control_policy(
    *,
    manifest: Mapping[str, Any],
    role: str,
    sleeve_ids: tuple[str, ...],
    risk_level: float,
    risk_micro_units: int,
    random_seed: int | None = None,
) -> ProductionPolicy:
    route: dict[str, str | int | float | bool] = {
        "control_role": role,
        "component_order_policy": "LEXICAL_FROZEN_BEFORE_OUTCOMES",
        "basket_size": len(sleeve_ids),
    }
    if random_seed is not None:
        route["random_seed"] = int(random_seed)
    payload = {
        "campaign_id": str(manifest["campaign_id"]),
        "role": role,
        "sleeve_ids": list(sleeve_ids),
        "risk_level": float(risk_level),
        "risk_micro_units": int(risk_micro_units),
        "maximum_simultaneous_positions": len(sleeve_ids),
        "maximum_mini_equivalent": int(
            manifest["account_parameters"]["maximum_mini_equivalent"]
        ),
        "conflict_policy": "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        "route": route,
        "version": 1,
    }
    structural = stable_hash(payload)
    behavioral = stable_hash({**payload, "campaign_id": None})
    return ProductionPolicy(
        policy_id=f"control_{role.lower()}_{structural[:24]}",
        mechanism="FIXED_STATIC_RISK_FRONTIER",
        sleeve_ids=sleeve_ids,
        component_priority=sleeve_ids,
        risk_level=float(risk_level),
        risk_micro_units=int(risk_micro_units),
        maximum_simultaneous_positions=len(sleeve_ids),
        maximum_mini_equivalent=int(
            manifest["account_parameters"]["maximum_mini_equivalent"]
        ),
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        route_parameters=tuple(sorted(route.items())),
        parent_policy_ids=(),
        structural_fingerprint=structural,
        behavioral_fingerprint=behavioral,
        source_campaign=str(manifest["campaign_id"]),
        baseline_role=role,
        random_seed=random_seed,
    )


def load_component_candidates(
    manifest: Mapping[str, Any], project_root: str | Path
) -> tuple[ComponentCandidate, ...]:
    root = Path(project_root).resolve()
    bank = dict(manifest["component_bank"])
    sources = dict(bank["sources"])
    seed = _load_json(root / str(sources["seed_archive"]["path"]))
    claimed = str(seed.pop("archive_hash", ""))
    if not claimed or stable_hash(seed) != claimed:
        raise ProductionManifestError("0024 seed archive semantic hash drift")
    seed["archive_hash"] = claimed
    elite = _load_json(root / str(sources["canonical_0018_elites"]["path"]))
    selected_ids = tuple(bank["primary_passing_policy_ids"]) + tuple(
        bank["primary_near_elite_policy_ids"]
    )
    selected = {
        str(row["policy_id"]): row
        for row in elite.get("policies") or []
        if str(row.get("policy_id")) in selected_ids
    }
    fallback_components = set(
        str(row) for row in bank["diagnostic_fallback_0020"].get("component_ids") or ()
    )
    parents_by_sleeve: dict[str, set[str]] = {}
    for policy_id, row in selected.items():
        for sleeve_id in row.get("component_ids") or ():
            parents_by_sleeve.setdefault(str(sleeve_id), set()).add(policy_id)

    candidates: list[ComponentCandidate] = []
    for row in seed.get("sleeves") or ():
        specification = dict(row["specification"])
        evidence = dict(row.get("development_evidence") or row.get("pilot_evidence") or {})
        sleeve = _sleeve_from_dict(specification)
        parent_ids = tuple(sorted(parents_by_sleeve.get(sleeve.sleeve_id, ())))
        useful = (
            evidence.get("incremental_status") == "MICRO_EDGE_USEFUL"
            and float(evidence.get("cost_stress_1_5x_net") or 0.0) > 0.0
            and int(evidence.get("events") or evidence.get("event_count") or 0) >= 20
        )
        if not parent_ids and not useful:
            continue
        roles: list[str] = []
        if parent_ids:
            roles.append("PRIMARY_0018_COMPONENT")
        if useful:
            roles.append("NON_TOMBSTONED_WALK_FORWARD_POSITIVE_MICRO_EDGE")
        if sleeve.sleeve_id in fallback_components:
            roles.append("DIAGNOSTIC_0020_FALLBACK_COMPONENT")
        candidates.append(
            ComponentCandidate(
                sleeve=sleeve,
                development_evidence=evidence,
                source_roles=tuple(roles),
                primary_parent_ids=parent_ids,
            )
        )
    candidates.sort(key=lambda row: row.sleeve.sleeve_id)
    if len(candidates) < 24:
        raise ProductionManifestError("0024 component bank is too small")
    if not set(parents_by_sleeve).issubset({row.sleeve.sleeve_id for row in candidates}):
        raise ProductionManifestError("an 0018 primary component is absent from the seed archive")
    return tuple(candidates)


def generate_policy_population(
    components: Sequence[ComponentCandidate],
    manifest: Mapping[str, Any],
    *,
    count: int | None = None,
) -> PolicyPopulation:
    target = int(count or manifest["successive_halving"]["stage0_proposals"])
    if target < 1:
        raise ValueError("policy proposal count must be positive")
    classes = tuple(str(value) for value in manifest["policy_classes"])
    if set(classes) != POLICY_CLASSES:
        raise ProductionManifestError("policy class declaration drift")
    risk_levels = tuple(float(row) for row in manifest["static_risk_frontier"]["normalized_levels"])
    micro_units = tuple(int(row) for row in manifest["static_risk_frontier"]["micro_units"])
    generator_seed = int(manifest["generator"]["seed"])
    choice_namespace = f"{manifest['campaign_id']}|generator_seed={generator_seed}"
    ordered = tuple(
        sorted(
            components,
            key=lambda row: (-row.priority[0], -row.priority[1], -row.priority[2], row.priority[3]),
        )
    )
    if len(ordered) < 8:
        raise ValueError("policy generation requires at least eight components")

    policies: list[ProductionPolicy] = []
    seen_structural: set[str] = set()
    seen_behavioral: set[str] = set()
    duplicate = incompatible = 0
    attempts = 0
    mechanism_counts: dict[str, int] = {}
    maximum_attempts = max(target * 80, 10_000)
    while len(policies) < target and attempts < maximum_attempts:
        attempt = attempts
        attempts += 1
        mechanism = classes[attempt % len(classes)]
        size = 2 + _choice(choice_namespace, attempt, "size") % 3
        selected = _select_components(
            ordered,
            mechanism=mechanism,
            size=size,
            campaign_id=choice_namespace,
            attempt=attempt,
        )
        if len(selected) != size or not _compatible(selected, mechanism):
            incompatible += 1
            continue
        risk_index = _choice(choice_namespace, attempt, "risk") % len(risk_levels)
        max_positions = min(
            size,
            1 + _choice(choice_namespace, attempt, "positions") % 3,
        )
        selected_ids = tuple(row.sleeve.sleeve_id for row in selected)
        priority = tuple(
            row.sleeve.sleeve_id
            for row in sorted(
                selected,
                key=lambda row: (-row.stressed_net, row.sleeve.sleeve_id),
            )
        )
        route = _route_parameters(mechanism, selected, attempt, choice_namespace)
        parents = tuple(
            sorted(
                {
                    value
                    for row in selected
                    for value in row.primary_parent_ids
                }
            )
        )
        structural_payload = {
            "mechanism": mechanism,
            "sleeves": selected_ids,
            "priority": priority,
            "risk_level": risk_levels[risk_index],
            "risk_micro_units": micro_units[risk_index],
            "maximum_positions": max_positions,
            "maximum_mini_equivalent": 15,
            "conflict_policy": "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
            "route": route,
            "campaign": manifest["campaign_id"],
            "generator_seed": generator_seed,
            "version": 1,
        }
        structural = stable_hash(structural_payload)
        behavioral = stable_hash(
            {
                **structural_payload,
                "sleeves": sorted(row.sleeve.behavioral_fingerprint for row in selected),
                "priority": [
                    next(
                        row.sleeve.behavioral_fingerprint
                        for row in selected
                        if row.sleeve.sleeve_id == sleeve_id
                    )
                    for sleeve_id in priority
                ],
                "campaign": None,
            }
        )
        if structural in seen_structural or behavioral in seen_behavioral:
            duplicate += 1
            continue
        seen_structural.add(structural)
        seen_behavioral.add(behavioral)
        policy_id = f"economic_policy_{structural[:24]}"
        policies.append(
            ProductionPolicy(
                policy_id=policy_id,
                mechanism=mechanism,
                sleeve_ids=selected_ids,
                component_priority=priority,
                risk_level=risk_levels[risk_index],
                risk_micro_units=micro_units[risk_index],
                maximum_simultaneous_positions=max_positions,
                maximum_mini_equivalent=15,
                conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
                route_parameters=tuple(sorted(route.items())),
                parent_policy_ids=parents,
                structural_fingerprint=structural,
                behavioral_fingerprint=behavioral,
                source_campaign=str(manifest["campaign_id"]),
            )
        )
        mechanism_counts[mechanism] = mechanism_counts.get(mechanism, 0) + 1
    if len(policies) != target:
        raise RuntimeError(
            f"could not generate requested unique policy population: {len(policies)}/{target}"
        )
    return PolicyPopulation(
        policies=tuple(policies),
        requested_count=target,
        attempted_count=attempts,
        rejected_duplicate_count=duplicate,
        rejected_incompatible_count=incompatible,
        mechanism_counts=mechanism_counts,
    )


def fast_economic_screen(
    policies: Sequence[ProductionPolicy],
    components: Mapping[str, ComponentCandidate],
    *,
    survivor_limit: int,
) -> tuple[list[dict[str, Any]], tuple[ProductionPolicy, ...]]:
    """Run a transparent prior-evidence proxy screen, never an exact replay."""

    rows: list[dict[str, Any]] = []
    by_id = {row.policy_id: row for row in policies}
    for policy in policies:
        selected = [components[value] for value in policy.sleeve_ids]
        scale = policy.risk_micro_units / 4.0
        normal = sum(row.normal_net for row in selected) * scale
        stress = sum(row.stressed_net for row in selected) * scale
        events = sum(row.event_count for row in selected)
        coverage = len({(row.sleeve.market, row.sleeve.session_code) for row in selected})
        maximum_component_share = max(
            (max(row.stressed_net, 0.0) for row in selected), default=0.0
        ) / max(sum(max(row.stressed_net, 0.0) for row in selected), 1e-12)
        drawdown = sum(
            abs(float(row.development_evidence.get("maximum_drawdown") or 0.0))
            for row in selected
        ) * scale
        cost_margin = normal - stress
        target_proxy = min(2.0, max(-1.0, stress / 9_000.0))
        finite = all(
            value == value and abs(value) != float("inf")
            for value in (normal, stress, drawdown, cost_margin, target_proxy)
        )
        eligible = bool(
            finite
            and stress > 0.0
            and normal > 0.0
            and events >= 40
            and maximum_component_share <= 0.65
            and coverage >= 2
            and len({row.sleeve.behavioral_fingerprint for row in selected})
            == len(selected)
        )
        rows.append(
            {
                "policy_id": policy.policy_id,
                "mechanism": policy.mechanism,
                "risk_level": policy.risk_level,
                "component_count": len(selected),
                "prior_normal_net_proxy": normal,
                "prior_stressed_net_proxy": stress,
                "target_progress_proxy": target_proxy,
                "opportunity_count_proxy": events,
                "coverage_cell_count": coverage,
                "drawdown_proxy": drawdown,
                "cost_margin_proxy": cost_margin,
                "maximum_component_share_proxy": maximum_component_share,
                "fast_screen_survivor": eligible,
                "screen_scope": "DEVELOPMENT_PRIOR_HEURISTIC_NOT_ACCOUNT_REPLAY",
                "outcome_claim": False,
                "opaque_score_used": False,
                "validated": False,
            }
        )
    selected_rows = _stratified_pareto_selection(
        [row for row in rows if row["fast_screen_survivor"]],
        limit=survivor_limit,
    )
    selected_ids = {str(row["policy_id"]) for row in selected_rows}
    for row in rows:
        row["selected_for_exact_replay"] = row["policy_id"] in selected_ids
    selected = tuple(by_id[str(row["policy_id"])] for row in selected_rows)
    return rows, selected


def _stratified_pareto_selection(
    rows: Sequence[Mapping[str, Any]], *, limit: int
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, float], list[dict[str, Any]]] = {}
    for source in rows:
        row = dict(source)
        groups.setdefault((str(row["mechanism"]), float(row["risk_level"])), []).append(row)
    for values in groups.values():
        values.sort(
            key=lambda row: (
                -float(row["prior_stressed_net_proxy"]),
                -float(row["target_progress_proxy"]),
                float(row["drawdown_proxy"]),
                float(row["maximum_component_share_proxy"]),
                int(row["component_count"]),
                str(row["policy_id"]),
            )
        )
    output: list[dict[str, Any]] = []
    keys = sorted(groups)
    cursor = 0
    while keys and len(output) < limit:
        key = keys[cursor % len(keys)]
        values = groups[key]
        row = values.pop(0)
        row["selection_policy"] = "STRATIFIED_TRANSPARENT_PARETO_LEXICOGRAPHIC_V1"
        row["selection_rank_within_mechanism_risk"] = 1 + sum(
            value["mechanism"] == row["mechanism"] and value["risk_level"] == row["risk_level"]
            for value in output
        )
        output.append(row)
        if not values:
            keys.remove(key)
            cursor = 0
        else:
            cursor += 1
    return output


def _select_components(
    components: Sequence[ComponentCandidate],
    *,
    mechanism: str,
    size: int,
    campaign_id: str,
    attempt: int,
) -> tuple[ComponentCandidate, ...]:
    offset = _choice(campaign_id, attempt, mechanism) % len(components)
    ordered = tuple(components[offset:]) + tuple(components[:offset])
    if mechanism == "TARGET_VELOCITY_MLL_PROTECTION":
        accelerators = sorted(
            components,
            key=lambda row: (
                -(row.stressed_net / max(row.event_count, 1)),
                row.sleeve.sleeve_id,
            ),
        )
        defensive = [
            row
            for row in components
            if row.sleeve.role
            in {EconomicRole.MLL_STABILIZER, EconomicRole.CONSISTENCY_SMOOTHER}
        ]
        ordered = tuple(accelerators[: max(1, len(accelerators) // 2)]) + tuple(defensive) + ordered
    selected: list[ComponentCandidate] = []
    seen: set[str] = set()
    for candidate in ordered:
        if candidate.sleeve.sleeve_id in seen:
            continue
        if not _can_add(selected, candidate, mechanism):
            continue
        selected.append(candidate)
        seen.add(candidate.sleeve.sleeve_id)
        if len(selected) == size:
            break
    return tuple(selected)


def _can_add(
    selected: Sequence[ComponentCandidate],
    candidate: ComponentCandidate,
    mechanism: str,
) -> bool:
    if any(row.sleeve.behavioral_fingerprint == candidate.sleeve.behavioral_fingerprint for row in selected):
        return False
    if sum(row.sleeve.market == candidate.sleeve.market for row in selected) >= 2:
        return False
    if mechanism == "SESSION_SPECIALIZED_ROUTING" and selected:
        if candidate.sleeve.session_code in {row.sleeve.session_code for row in selected}:
            return False
    if mechanism in {"OPPORTUNITY_DENSITY", "MARKET_ROLE_ROTATION"} and selected:
        if candidate.sleeve.market in {row.sleeve.market for row in selected}:
            return False
    return True


def _compatible(selected: Sequence[ComponentCandidate], mechanism: str) -> bool:
    if len(selected) < 2:
        return False
    if len({row.sleeve.behavioral_fingerprint for row in selected}) != len(selected):
        return False
    if len({row.sleeve.market for row in selected}) < 2 and mechanism != "REGIME_GATED_SLEEVES":
        return False
    if mechanism == "SESSION_SPECIALIZED_ROUTING" and len(
        {row.sleeve.session_code for row in selected}
    ) < 2:
        return False
    if mechanism == "TARGET_VELOCITY_MLL_PROTECTION":
        roles = {row.sleeve.role for row in selected}
        if not roles.intersection({EconomicRole.MLL_STABILIZER, EconomicRole.CONSISTENCY_SMOOTHER}):
            return False
    if mechanism == "NEW_MICRO_EDGE_ASSEMBLY" and len(
        {row.sleeve.role for row in selected}
    ) < 2:
        return False
    return True


def _route_parameters(
    mechanism: str,
    selected: Sequence[ComponentCandidate],
    attempt: int,
    campaign_id: str,
) -> dict[str, str | int | float | bool]:
    if mechanism == "REGIME_GATED_SLEEVES":
        return {
            "gate": "TRAILING_CLOSED_TRADE_NET_POSITIVE",
            "lookback_closed_trades": (5, 10, 20)[_choice(campaign_id, attempt, "gate") % 3],
            "minimum_closed_trades": 5,
            "past_only": True,
        }
    if mechanism == "NEW_MICRO_EDGE_ASSEMBLY":
        return {
            "assembly": "COMPLEMENTARY_MICRO_EDGE_WITH_CAUSAL_QUALITY_VETO",
            "filter": "TRAILING_CLOSED_TRADE_NET_AND_LOSS_SHARE",
            "lookback_closed_trades": (8, 12, 20)[
                _choice(campaign_id, attempt, "micro_edge_filter") % 3
            ],
            "minimum_closed_trades": 5,
            "maximum_trailing_loss_share": (0.60, 0.70)[
                _choice(campaign_id, attempt, "micro_edge_veto") % 2
            ],
            "exit_policy": "FROZEN_SOURCE_COMPONENT_EXIT",
            "risk_modifier": "FROZEN_STATIC_FRONTIER_ONLY",
            "past_only": True,
        }
    if mechanism == "SESSION_SPECIALIZED_ROUTING":
        return {
            "route": "FROZEN_COMPONENT_SESSION",
            "session_codes": ",".join(str(row.sleeve.session_code) for row in selected),
            "past_only": True,
        }
    if mechanism == "OPPORTUNITY_DENSITY":
        return {
            "gate": "PRIOR_OR_SIMULTANEOUS_INDEPENDENT_SIGNAL_COUNT",
            "lookback_minutes": (30, 60, 120)[_choice(campaign_id, attempt, "density") % 3],
            "minimum_independent_sources": 2,
            "past_only": True,
        }
    if mechanism == "MARKET_ROLE_ROTATION":
        return {
            "route": "TRAILING_CLOSED_TRADE_MARKET_RANK",
            "lookback_sessions": (10, 20)[_choice(campaign_id, attempt, "rotation") % 2],
            "active_market_count": 1,
            "past_only": True,
        }
    if mechanism == "TARGET_VELOCITY_MLL_PROTECTION":
        return {
            "assembly": "ONE_VELOCITY_PLUS_ONE_OR_MORE_DEFENSIVE",
            "dynamic_ratchet": False,
            "past_only": True,
        }
    return {
        "sizing": "FROZEN_STATIC_MICRO_UNITS",
        "dynamic_ratchet": False,
        "past_only": True,
    }


def _choice(campaign_id: str, attempt: int, label: str) -> int:
    digest = hashlib.sha256(f"{campaign_id}|{attempt}|{label}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ProductionManifestError(f"expected JSON object: {path}")
    return value


__all__ = [
    "ComponentCandidate",
    "PolicyPopulation",
    "ProductionPolicy",
    "build_predeclared_control_bank",
    "fast_economic_screen",
    "generate_policy_population",
    "load_component_candidates",
]
