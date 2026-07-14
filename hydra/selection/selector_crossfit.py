"""Leakage-resistant ledger recovery and temporal partitions for HYDRA V7.3.

Campaign 0023 deliberately omitted component events from its normal report rows.
This module supports one controlled reconstruction from the already-frozen feature
matrices, proves that every reconstructed runtime agrees with the 48 persisted
metadata rows, and then writes a content-addressed event ledger.  Normal selector
work consumes that ledger only; loading it cannot build features or signals.

The temporal plan treats the block, not the many starts inside a block, as the
unit of inference.  Four 52-session blocks are separated by three ten-session
embargoes.  Twelve starts in the first twelve sessions of each block retain a
complete 40-session primary observation window.  Starts inside a block are
explicitly marked as dependent observations.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from hydra.account_policy.basket import RoutedTrade
from hydra.data.contract_mapping import (
    RollMap,
    active_contract,
    is_unsafe_roll_window,
    load_roll_map,
)
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.economic_evolution.schema import EconomicRole


RECOVERED_LEDGER_SCHEMA = "hydra_v73_recovered_component_event_ledger_v1"
RECOVERY_MANIFEST_SCHEMA = "hydra_v73_recovered_component_event_manifest_v1"
TEMPORAL_BLOCK_SCHEMA = "hydra_v73_purged_temporal_block_v1"
TEMPORAL_PLAN_SCHEMA = "hydra_v73_purged_temporal_plan_v1"
OUTER_FOLD_SCHEMA = "hydra_v73_leave_one_block_out_fold_v1"
RECOVERY_VERSION = "hydra_v73_campaign_0023_controlled_recovery_v1"
EXPECTED_CAMPAIGN_ID = "hydra_economic_evolution_static_parent_basket_0023"


class SelectorCrossfitError(RuntimeError):
    """Raised when immutable provenance or temporal isolation fails closed."""


def canonical_hash(value: Any) -> str:
    """Return HYDRA's deterministic JSON SHA-256 representation."""

    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class RecoveredCampaignLedger:
    """Verified in-memory view of the content-addressed recovered ledger."""

    manifest_path: Path
    ledger_path: Path
    manifest: Mapping[str, Any]
    runtimes: Mapping[str, ExactSleeveRuntime]

    @property
    def ledger_sha256(self) -> str:
        return str(self.manifest["ledger_sha256"])

    @property
    def common_session_days(self) -> tuple[int, ...]:
        return _common_session_days(self.runtimes.values())


def recover_campaign_0023_event_ledger(
    *,
    preregistration_path: str | Path,
    campaign_output_dir: str | Path,
    contract_map_path: str | Path,
    frozen_feature_cache_root: str | Path,
    recovered_output_dir: str | Path,
    worker_count: int = 3,
) -> RecoveredCampaignLedger:
    """Refuse implicit campaign-0023 signal/trade reconstruction.

    Campaign 0023 omitted its event ledger.  Rebuilding it from feature matrices
    would recalculate calibration thresholds, signals, entries, and exits, which
    the V7.3 sprint contract explicitly forbids.  The retained implementation
    below is unreachable documentation of the audited legacy recovery idea; an
    authoritative pre-existing ledger must be supplied instead.
    """

    raise SelectorCrossfitError(
        "campaign 0023 has no authoritative immutable event ledger; "
        "signal/entry/exit reconstruction is disabled by the V7.3 contract"
    )

    from hydra.economic_evolution.account_complementary_sleeve import (
        generate_complementary_sleeve_population,
    )
    from hydra.economic_evolution.screen import CheapScreenPolicy
    from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive
    from hydra.features.feature_matrix import FeatureMatrix
    from hydra.research.economic_evolution_elite_robustness_campaign import (
        SOURCE_COVERAGE_PARENT_ID,
        SOURCE_PARENT_CAMPAIGN_ID,
        SOURCE_POPULATION_CAMPAIGN_ID,
        SOURCE_SIZING_PARENT_ID,
    )
    from hydra.research.economic_evolution_pilot import (
        _bind_selected,
        _build_exact_runtimes,
        _runtime_row,
    )

    prereg_path = Path(preregistration_path).resolve()
    campaign_dir = Path(campaign_output_dir).resolve()
    contract_path = Path(contract_map_path).resolve()
    cache_root = Path(frozen_feature_cache_root).resolve()
    prereg = _load_json(prereg_path)
    _verify_self_hash(prereg, "preregistration_hash", label="preregistration")
    if prereg.get("campaign_id") != EXPECTED_CAMPAIGN_ID:
        raise SelectorCrossfitError("recovery is restricted to frozen campaign 0023")
    if worker_count != int(prereg["compute"]["exact_worker_count"]) or worker_count != 3:
        raise SelectorCrossfitError("campaign 0023 recovery requires its frozen 3 workers")
    if file_sha256(contract_path) != str(prereg["data"]["contract_map_sha256"]):
        raise SelectorCrossfitError("campaign 0023 contract map checksum drift")

    root = _project_root(prereg_path)
    seed_reference = dict(prereg["source_seed"])
    seed_path = root / str(seed_reference["path"])
    if file_sha256(seed_path) != str(seed_reference["file_sha256"]):
        raise SelectorCrossfitError("campaign 0023 seed file checksum drift")
    seed = load_and_verify_seed_archive(seed_path)
    if seed.get("archive_hash") != seed_reference["archive_hash"]:
        raise SelectorCrossfitError("campaign 0023 seed semantic hash drift")

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
    expected_manifest = str(
        prereg["structural_population"]["source_component_manifest_hash"]
    )
    if source.manifest_hash != expected_manifest:
        raise SelectorCrossfitError("campaign 0023 source component population drift")
    if len(source.components) != 48:
        raise SelectorCrossfitError("campaign 0023 recovery requires all 48 components")

    market_paths = locate_frozen_feature_bundles(
        cache_root, prereg["data"]["feature_manifest_sha256"]
    )
    matrices = {
        market: FeatureMatrix.open(path, mmap=True)
        for market, path in sorted(market_paths.items())
    }
    expected_source = str(prereg["data"]["feature_source_fingerprint"])
    for market, matrix in matrices.items():
        provenance = dict(matrix.manifest.get("provenance") or {})
        key = dict(matrix.manifest.get("key") or {})
        if (
            provenance.get("data_fingerprint") != expected_source
            or key.get("source_data_sha256") != expected_source
            or provenance.get("q4_access_count_delta") != 0
            or provenance.get("outbound_order_capability") is not False
        ):
            raise SelectorCrossfitError(f"frozen feature provenance drift: {market}")

    sleeves = tuple(row.sleeve for row in source.components)
    bound = _bind_selected(
        sleeves,
        matrices,
        policy=CheapScreenPolicy(**prereg["cheap_screen_policy"]),
    )
    runtimes, failures = _build_exact_runtimes(
        bound,
        matrices,
        start_inclusive=str(prereg["exact_replay_period"][0]),
        end_exclusive=str(prereg["exact_replay_period"][1]),
        worker_count=worker_count,
    )
    if failures or len(runtimes) != 48:
        raise SelectorCrossfitError(
            f"controlled recovery incomplete: runtimes={len(runtimes)} failures={failures}"
        )

    exact_metadata_path = campaign_dir / "exact_component_results.jsonl"
    expected_rows = sorted(
        _read_jsonl(exact_metadata_path), key=lambda row: str(row["sleeve_id"])
    )
    actual_rows = sorted(
        (_runtime_row(runtime) for runtime in runtimes.values()),
        key=lambda row: str(row["sleeve_id"]),
    )
    assert_exact_runtime_metadata(expected_rows, actual_rows)

    component_specs = {
        sleeve.sleeve_id: sleeve.to_dict() for sleeve in sorted(sleeves, key=lambda x: x.sleeve_id)
    }
    provenance = {
        "campaign_id": EXPECTED_CAMPAIGN_ID,
        "recovery_version": RECOVERY_VERSION,
        "controlled_reconstruction_due_to_0023_ledger_omission": True,
        "source_preregistration_path": _root_relative(prereg_path, root),
        "source_preregistration_sha256": file_sha256(prereg_path),
        "source_preregistration_semantic_hash": prereg["preregistration_hash"],
        "source_exact_metadata_path": _root_relative(exact_metadata_path, root),
        "source_exact_metadata_sha256": file_sha256(exact_metadata_path),
        "source_component_manifest_hash": source.manifest_hash,
        "feature_source_fingerprint": expected_source,
        "feature_manifest_sha256": dict(
            sorted(prereg["data"]["feature_manifest_sha256"].items())
        ),
        "contract_map_path": _root_relative(contract_path, root),
        "contract_map_sha256": file_sha256(contract_path),
        "exact_replay_period": list(prereg["exact_replay_period"]),
        "runtime_metadata_reconciliation": {
            "expected_count": 48,
            "actual_count": 48,
            "exact_match_count": 48,
            "all_fields_equal": True,
            "expected_metadata_hash": canonical_hash(expected_rows),
            "actual_metadata_hash": canonical_hash(actual_rows),
        },
        "data_role": "DEVELOPMENT_ONLY_Q4_EXCLUDED",
        "new_data_purchase": False,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "feature_builder_called": False,
        "signals_mutated": False,
    }
    return write_recovered_event_ledger(
        runtimes,
        output_dir=recovered_output_dir,
        provenance=provenance,
        component_specs=component_specs,
    )


def locate_frozen_feature_bundles(
    cache_root: str | Path, expected_manifest_sha256: Mapping[str, str]
) -> dict[str, Path]:
    """Find immutable feature bundles by frozen manifest hashes only."""

    expected = {str(k): str(v) for k, v in expected_manifest_sha256.items()}
    matches: dict[str, list[Path]] = {market: [] for market in expected}
    inverse = {digest: market for market, digest in expected.items()}
    for manifest_path in Path(cache_root).rglob("manifest.json"):
        digest = file_sha256(manifest_path)
        market = inverse.get(digest)
        if market is not None:
            matches[market].append(manifest_path.parent.resolve())
    missing = sorted(market for market, paths in matches.items() if not paths)
    duplicate = sorted(market for market, paths in matches.items() if len(paths) != 1)
    if missing or duplicate:
        raise SelectorCrossfitError(
            f"frozen feature bundle lookup failed: missing={missing} duplicate={duplicate}"
        )
    return {market: paths[0] for market, paths in sorted(matches.items())}


def write_recovered_event_ledger(
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    output_dir: str | Path,
    provenance: Mapping[str, Any],
    component_specs: Mapping[str, Mapping[str, Any]] | None = None,
) -> RecoveredCampaignLedger:
    """Persist verified runtimes as deterministic, content-addressed artifacts."""

    ordered = [runtimes[key] for key in sorted(runtimes)]
    if not ordered or len({row.sleeve_id for row in ordered}) != len(ordered):
        raise SelectorCrossfitError("recovered event ledger requires unique runtimes")
    lines = [
        json.dumps(
            {
                "schema": RECOVERED_LEDGER_SCHEMA,
                "runtime": runtime.to_dict(include_events=True),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        for runtime in ordered
    ]
    ledger_bytes = ("\n".join(lines) + "\n").encode("utf-8")
    ledger_sha = hashlib.sha256(ledger_bytes).hexdigest()
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    ledger_path = output / f"campaign_0023_component_events_{ledger_sha[:16]}.jsonl"
    _write_immutable_bytes(ledger_path, ledger_bytes)

    metadata_rows = [runtime.to_dict(include_events=False) for runtime in ordered]
    common_days = _common_session_days(ordered)
    payload: dict[str, Any] = {
        "schema": RECOVERY_MANIFEST_SCHEMA,
        "recovery_version": RECOVERY_VERSION,
        "campaign_id": str(provenance.get("campaign_id") or EXPECTED_CAMPAIGN_ID),
        "ledger_file": ledger_path.name,
        "ledger_sha256": ledger_sha,
        "ledger_schema": RECOVERED_LEDGER_SCHEMA,
        "runtime_count": len(ordered),
        "event_count": sum(row.event_count for row in ordered),
        "runtime_metadata_hash": canonical_hash(metadata_rows),
        "common_session_day_count": len(common_days),
        "common_session_days_hash": canonical_hash(list(common_days)),
        "common_session_day_min": min(common_days),
        "common_session_day_max": max(common_days),
        "component_specs": dict(sorted((component_specs or {}).items())),
        "provenance": dict(provenance),
        "immutable": True,
        "load_recomputes_features_or_signals": False,
    }
    payload["manifest_hash"] = canonical_hash(payload)
    manifest_bytes = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    manifest_path = output / f"campaign_0023_component_events_manifest_{payload['manifest_hash'][:16]}.json"
    _write_immutable_bytes(manifest_path, manifest_bytes)
    return load_recovered_event_ledger(manifest_path)


def load_recovered_event_ledger(
    manifest_path: str | Path,
) -> RecoveredCampaignLedger:
    """Load a recovered ledger without importing or invoking feature builders."""

    path = Path(manifest_path).resolve()
    manifest = _load_json(path)
    if manifest.get("schema") != RECOVERY_MANIFEST_SCHEMA:
        raise SelectorCrossfitError("recovered ledger manifest schema drift")
    claimed = str(manifest.get("manifest_hash") or "")
    unhashed = dict(manifest)
    unhashed.pop("manifest_hash", None)
    if not claimed or canonical_hash(unhashed) != claimed:
        raise SelectorCrossfitError("recovered ledger manifest hash drift")
    ledger_path = path.parent / str(manifest["ledger_file"])
    if file_sha256(ledger_path) != str(manifest["ledger_sha256"]):
        raise SelectorCrossfitError("recovered event ledger checksum drift")

    runtimes: dict[str, ExactSleeveRuntime] = {}
    for value in _read_jsonl(ledger_path):
        if value.get("schema") != RECOVERED_LEDGER_SCHEMA:
            raise SelectorCrossfitError("recovered event ledger row schema drift")
        runtime = _runtime_from_dict(value["runtime"])
        if runtime.sleeve_id in runtimes:
            raise SelectorCrossfitError("duplicate runtime in recovered ledger")
        runtimes[runtime.sleeve_id] = runtime
    runtimes = dict(sorted(runtimes.items()))
    metadata = [row.to_dict(include_events=False) for row in runtimes.values()]
    common_days = _common_session_days(runtimes.values())
    if (
        len(runtimes) != int(manifest["runtime_count"])
        or sum(row.event_count for row in runtimes.values()) != int(manifest["event_count"])
        or canonical_hash(metadata) != str(manifest["runtime_metadata_hash"])
        or canonical_hash(list(common_days)) != str(manifest["common_session_days_hash"])
    ):
        raise SelectorCrossfitError("recovered event ledger semantic drift")
    return RecoveredCampaignLedger(
        manifest_path=path,
        ledger_path=ledger_path,
        manifest=manifest,
        runtimes=runtimes,
    )


@dataclass(frozen=True, slots=True)
class TemporalBlock:
    block_id: str
    ordinal: int
    session_days: tuple[int, ...]
    episode_start_days: tuple[int, ...]
    embargo_before_days: tuple[int, ...]
    embargo_after_days: tuple[int, ...]
    start_date: str
    end_date: str
    trading_day_count: int
    event_count: int
    signal_markets: tuple[str, ...]
    execution_markets: tuple[str, ...]
    session_codes: tuple[int, ...]
    volatility_regime_counts: Mapping[str, int]
    contracts_by_market: Mapping[str, tuple[str, ...]]
    roll_transition_dates_by_market: Mapping[str, tuple[str, ...]]
    unsafe_roll_session_dates_by_market: Mapping[str, tuple[str, ...]]
    contamination_history: tuple[Mapping[str, Any], ...]
    primary_horizon_sessions: int = 40
    inference_unit: str = "TEMPORAL_BLOCK"
    within_block_starts_independent: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": TEMPORAL_BLOCK_SCHEMA,
            "block_id": self.block_id,
            "ordinal": self.ordinal,
            "dates": [self.start_date, self.end_date],
            "session_days": list(self.session_days),
            "episode_start_days": list(self.episode_start_days),
            "embargo_before_days": list(self.embargo_before_days),
            "embargo_after_days": list(self.embargo_after_days),
            "trading_day_count": self.trading_day_count,
            "event_count": self.event_count,
            "signal_markets": list(self.signal_markets),
            "execution_markets": list(self.execution_markets),
            "markets": sorted(set(self.signal_markets) | set(self.execution_markets)),
            "session_codes": list(self.session_codes),
            "volatility_regime_counts": dict(sorted(self.volatility_regime_counts.items())),
            "contracts_by_market": {
                key: list(value) for key, value in sorted(self.contracts_by_market.items())
            },
            "roll_transition_dates_by_market": {
                key: list(value)
                for key, value in sorted(self.roll_transition_dates_by_market.items())
            },
            "unsafe_roll_session_dates_by_market": {
                key: list(value)
                for key, value in sorted(self.unsafe_roll_session_dates_by_market.items())
            },
            "contamination_history": [dict(value) for value in self.contamination_history],
            "primary_horizon_sessions": self.primary_horizon_sessions,
            "primary_horizon_complete_for_every_start": True,
            "inference_unit": self.inference_unit,
            "within_block_starts_independent": self.within_block_starts_independent,
            "overlapping_episode_starts_counted_as_independent": False,
        }


@dataclass(frozen=True, slots=True)
class TemporalBlockPlan:
    source_ledger_sha256: str
    source_common_session_days_hash: str
    contract_map_sha256: str
    blocks: tuple[TemporalBlock, ...]
    embargo_gaps: tuple[tuple[int, ...], ...]
    trailing_unused_days: tuple[int, ...]
    plan_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": TEMPORAL_PLAN_SCHEMA,
            "source_ledger_sha256": self.source_ledger_sha256,
            "source_common_session_days_hash": self.source_common_session_days_hash,
            "contract_map_sha256": self.contract_map_sha256,
            "construction": {
                "block_count": 4,
                "sessions_per_block": 52,
                "embargo_sessions_between_blocks": 10,
                "starts_per_block": 12,
                "start_policy": "FIRST_12_UNIQUE_SESSIONS_PER_BLOCK",
                "primary_horizon_sessions": 40,
                "inference_unit": "TEMPORAL_BLOCK",
                "within_block_starts_independent": False,
            },
            "blocks": [block.to_dict() for block in self.blocks],
            "embargo_gaps": [list(value) for value in self.embargo_gaps],
            "trailing_unused_days": list(self.trailing_unused_days),
            "plan_hash": self.plan_hash,
        }


def build_purged_temporal_block_plan(
    ledger: RecoveredCampaignLedger,
    *,
    contract_map_path: str | Path,
    contamination_history_by_block: Mapping[str, Sequence[Mapping[str, Any]]],
) -> TemporalBlockPlan:
    """Build the frozen maximum-defensible four-block development partition."""

    days = ledger.common_session_days
    if len(days) != 240:
        raise SelectorCrossfitError(
            f"campaign 0023 temporal plan requires exactly 240 common days, got {len(days)}"
        )
    contract_path = Path(contract_map_path).resolve()
    expected_contract_hash = str(
        dict(ledger.manifest.get("provenance") or {}).get("contract_map_sha256") or ""
    )
    actual_contract_hash = file_sha256(contract_path)
    if expected_contract_hash and actual_contract_hash != expected_contract_hash:
        raise SelectorCrossfitError("temporal block contract map differs from recovered ledger")
    roll_map = load_roll_map(contract_path)
    component_specs = dict(ledger.manifest.get("component_specs") or {})
    block_ids = tuple(f"V73_B{index}" for index in range(1, 5))
    if set(contamination_history_by_block) != set(block_ids):
        raise SelectorCrossfitError("complete contamination history is required for all blocks")
    if any(not contamination_history_by_block[block_id] for block_id in block_ids):
        raise SelectorCrossfitError("empty contamination history is not an audit")

    slices: list[tuple[int, ...]] = []
    gaps: list[tuple[int, ...]] = []
    cursor = 0
    for ordinal in range(4):
        slices.append(tuple(days[cursor : cursor + 52]))
        cursor += 52
        if ordinal < 3:
            gaps.append(tuple(days[cursor : cursor + 10]))
            cursor += 10
    trailing = tuple(days[cursor:])
    if len(trailing) != 2 or any(len(value) != 10 for value in gaps):
        raise SelectorCrossfitError("purged block geometry drift")

    blocks: list[TemporalBlock] = []
    for index, (block_id, block_days) in enumerate(zip(block_ids, slices, strict=True)):
        starts = tuple(block_days[:12])
        if any(block_days.index(start) + 40 > len(block_days) for start in starts):
            raise SelectorCrossfitError("a frozen start lacks its 40-session observation")
        before = gaps[index - 1] if index else ()
        after = gaps[index] if index < len(gaps) else ()
        history = tuple(dict(value) for value in contamination_history_by_block[block_id])
        blocks.append(
            _temporal_block(
                block_id=block_id,
                ordinal=index + 1,
                session_days=block_days,
                episode_start_days=starts,
                embargo_before_days=tuple(before),
                embargo_after_days=tuple(after),
                runtimes=ledger.runtimes,
                component_specs=component_specs,
                roll_map=roll_map,
                contamination_history=history,
            )
        )

    _validate_block_isolation(tuple(blocks), tuple(gaps))
    payload = {
        "schema": TEMPORAL_PLAN_SCHEMA,
        "source_ledger_sha256": ledger.ledger_sha256,
        "source_common_session_days_hash": canonical_hash(list(days)),
        "contract_map_sha256": actual_contract_hash,
        "construction": {
            "block_count": 4,
            "sessions_per_block": 52,
            "embargo_sessions_between_blocks": 10,
            "starts_per_block": 12,
            "start_policy": "FIRST_12_UNIQUE_SESSIONS_PER_BLOCK",
            "primary_horizon_sessions": 40,
            "inference_unit": "TEMPORAL_BLOCK",
            "within_block_starts_independent": False,
        },
        "blocks": [block.to_dict() for block in blocks],
        "embargo_gaps": [list(value) for value in gaps],
        "trailing_unused_days": list(trailing),
    }
    return TemporalBlockPlan(
        source_ledger_sha256=ledger.ledger_sha256,
        source_common_session_days_hash=canonical_hash(list(days)),
        contract_map_sha256=actual_contract_hash,
        blocks=tuple(blocks),
        embargo_gaps=tuple(gaps),
        trailing_unused_days=trailing,
        plan_hash=canonical_hash(payload),
    )


@dataclass(frozen=True, slots=True)
class OuterFold:
    fold_id: str
    design_block_ids: tuple[str, ...]
    held_out_block_id: str
    source_plan_hash: str
    fold_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": OUTER_FOLD_SCHEMA,
            "fold_id": self.fold_id,
            "design_block_ids": list(self.design_block_ids),
            "held_out_block_id": self.held_out_block_id,
            "selection_role": "DESIGN_ONLY",
            "evaluation_role": "HELD_OUT_EXACTLY_ONCE",
            "retuning_after_held_out": False,
            "source_plan_hash": self.source_plan_hash,
            "fold_hash": self.fold_hash,
        }


def leave_one_block_out_folds(plan: TemporalBlockPlan) -> tuple[OuterFold, ...]:
    """Create exactly one held-out rotation per independent block."""

    ids = tuple(block.block_id for block in plan.blocks)
    folds: list[OuterFold] = []
    for index, held_out in enumerate(ids, start=1):
        payload = {
            "schema": OUTER_FOLD_SCHEMA,
            "fold_id": f"V73_OUTER_{index}",
            "design_block_ids": [value for value in ids if value != held_out],
            "held_out_block_id": held_out,
            "selection_role": "DESIGN_ONLY",
            "evaluation_role": "HELD_OUT_EXACTLY_ONCE",
            "retuning_after_held_out": False,
            "source_plan_hash": plan.plan_hash,
        }
        folds.append(
            OuterFold(
                fold_id=str(payload["fold_id"]),
                design_block_ids=tuple(payload["design_block_ids"]),
                held_out_block_id=held_out,
                source_plan_hash=plan.plan_hash,
                fold_hash=canonical_hash(payload),
            )
        )
    if {fold.held_out_block_id for fold in folds} != set(ids):
        raise SelectorCrossfitError("outer rotation does not hold out every block once")
    return tuple(folds)


@dataclass(frozen=True, slots=True)
class DesignLedgerView:
    block_ids: tuple[str, ...]
    eligible_session_days: tuple[int, ...]
    events_by_component: Mapping[str, tuple[RoutedTrade, ...]]
    view_hash: str


@dataclass(frozen=True, slots=True)
class HeldOutLedgerView:
    block_id: str
    eligible_session_days: tuple[int, ...]
    episode_start_days: tuple[int, ...]
    events_by_component: Mapping[str, tuple[RoutedTrade, ...]]
    view_hash: str


@dataclass(frozen=True, slots=True)
class IsolatedOuterFoldLedger:
    fold: OuterFold
    design: DesignLedgerView
    held_out: HeldOutLedgerView


def isolate_outer_fold_ledger(
    ledger: RecoveredCampaignLedger,
    plan: TemporalBlockPlan,
    fold: OuterFold,
) -> IsolatedOuterFoldLedger:
    """Return disjoint, role-typed design and held-out event views."""

    by_id = {block.block_id: block for block in plan.blocks}
    if fold.held_out_block_id not in by_id or set(fold.design_block_ids) != (
        set(by_id) - {fold.held_out_block_id}
    ):
        raise SelectorCrossfitError("outer fold does not match temporal plan")
    design_days = tuple(
        sorted(
            day
            for block_id in fold.design_block_ids
            for day in by_id[block_id].session_days
        )
    )
    held_block = by_id[fold.held_out_block_id]
    held_days = held_block.session_days
    design_events = _events_on_days(ledger.runtimes, design_days)
    held_events = _events_on_days(ledger.runtimes, held_days)
    design_keys = _event_keys(design_events)
    held_keys = _event_keys(held_events)
    if set(design_days) & set(held_days) or design_keys & held_keys:
        raise SelectorCrossfitError("design and held-out ledgers are contaminated")
    design_payload = _ledger_view_payload(fold.design_block_ids, design_days, design_events)
    held_payload = _ledger_view_payload((held_block.block_id,), held_days, held_events)
    return IsolatedOuterFoldLedger(
        fold=fold,
        design=DesignLedgerView(
            block_ids=fold.design_block_ids,
            eligible_session_days=design_days,
            events_by_component=design_events,
            view_hash=canonical_hash(design_payload),
        ),
        held_out=HeldOutLedgerView(
            block_id=held_block.block_id,
            eligible_session_days=held_days,
            episode_start_days=held_block.episode_start_days,
            events_by_component=held_events,
            view_hash=canonical_hash(held_payload),
        ),
    )


def _temporal_block(
    *,
    block_id: str,
    ordinal: int,
    session_days: tuple[int, ...],
    episode_start_days: tuple[int, ...],
    embargo_before_days: tuple[int, ...],
    embargo_after_days: tuple[int, ...],
    runtimes: Mapping[str, ExactSleeveRuntime],
    component_specs: Mapping[str, Mapping[str, Any]],
    roll_map: RollMap,
    contamination_history: tuple[Mapping[str, Any], ...],
) -> TemporalBlock:
    day_set = set(session_days)
    events = [
        event
        for runtime in runtimes.values()
        for event in runtime.events
        if event.event.session_day in day_set
    ]
    signal_markets = tuple(sorted({runtime.signal_market for runtime in runtimes.values()}))
    execution_markets = tuple(
        sorted({runtime.execution_market for runtime in runtimes.values()})
    )
    session_codes = tuple(
        sorted(
            {
                int(value["session_code"])
                for value in component_specs.values()
                if value.get("session_code") is not None
            }
        )
    )
    regime_counts: dict[str, int] = {}
    for event in events:
        regime = str(event.event.regime)
        regime_counts[regime] = regime_counts.get(regime, 0) + 1

    contracts: dict[str, tuple[str, ...]] = {}
    roll_dates: dict[str, tuple[str, ...]] = {}
    unsafe: dict[str, tuple[str, ...]] = {}
    start = _session_date(session_days[0])
    end = _session_date(session_days[-1])
    for market in execution_markets:
        contracts[market] = tuple(
            sorted(
                {
                    active_contract(roll_map, market, _session_date(day)).contract
                    for day in session_days
                }
            )
        )
        roll_dates[market] = tuple(
            sorted(
                {
                    str(value.roll_date)[:10]
                    for value in roll_map.contracts
                    if value.root == market
                    and start <= str(value.roll_date)[:10] <= end
                }
            )
        )
        unsafe[market] = tuple(
            _session_date(day)
            for day in session_days
            if is_unsafe_roll_window(roll_map, market, _session_date(day))
        )
    return TemporalBlock(
        block_id=block_id,
        ordinal=ordinal,
        session_days=session_days,
        episode_start_days=episode_start_days,
        embargo_before_days=embargo_before_days,
        embargo_after_days=embargo_after_days,
        start_date=start,
        end_date=end,
        trading_day_count=len(session_days),
        event_count=len(events),
        signal_markets=signal_markets,
        execution_markets=execution_markets,
        session_codes=session_codes,
        volatility_regime_counts=dict(sorted(regime_counts.items())),
        contracts_by_market=contracts,
        roll_transition_dates_by_market=roll_dates,
        unsafe_roll_session_dates_by_market=unsafe,
        contamination_history=contamination_history,
    )


def _validate_block_isolation(
    blocks: tuple[TemporalBlock, ...], gaps: tuple[tuple[int, ...], ...]
) -> None:
    if len(blocks) != 4 or len(gaps) != 3:
        raise SelectorCrossfitError("four blocks and three embargoes are required")
    seen: set[int] = set()
    for index, block in enumerate(blocks):
        if len(block.session_days) != 52 or len(block.episode_start_days) != 12:
            raise SelectorCrossfitError("temporal block size or start count drift")
        if tuple(sorted(block.session_days)) != block.session_days:
            raise SelectorCrossfitError("temporal block chronology drift")
        if seen & set(block.session_days):
            raise SelectorCrossfitError("temporal blocks overlap")
        seen.update(block.session_days)
        if index:
            previous = blocks[index - 1]
            gap = gaps[index - 1]
            if not previous.session_days[-1] < gap[0] < gap[-1] < block.session_days[0]:
                raise SelectorCrossfitError("temporal embargo chronology drift")
            if len(gap) != 10 or set(gap) & seen:
                raise SelectorCrossfitError("temporal embargo isolation drift")


def _events_on_days(
    runtimes: Mapping[str, ExactSleeveRuntime], days: Sequence[int]
) -> dict[str, tuple[RoutedTrade, ...]]:
    allowed = set(days)
    return {
        sleeve_id: tuple(
            event for event in runtime.events if event.event.session_day in allowed
        )
        for sleeve_id, runtime in sorted(runtimes.items())
    }


def _event_keys(
    events_by_component: Mapping[str, Sequence[RoutedTrade]],
) -> set[tuple[str, str]]:
    return {
        (component_id, event.event.event_id)
        for component_id, events in events_by_component.items()
        for event in events
    }


def _ledger_view_payload(
    block_ids: Sequence[str],
    days: Sequence[int],
    events: Mapping[str, Sequence[RoutedTrade]],
) -> dict[str, Any]:
    return {
        "block_ids": list(block_ids),
        "eligible_session_days": list(days),
        "events": {
            key: [event.to_dict() for event in value]
            for key, value in sorted(events.items())
        },
    }


def _runtime_from_dict(value: Mapping[str, Any]) -> ExactSleeveRuntime:
    return ExactSleeveRuntime(
        sleeve_id=str(value["sleeve_id"]),
        signal_market=str(value["signal_market"]),
        execution_market=str(value["execution_market"]),
        role=EconomicRole(str(value["role"])),
        source_campaign=str(value["source_campaign"]),
        specification_hash=str(value["specification_hash"]),
        eligible_session_days=tuple(int(day) for day in value["eligible_session_days"]),
        events=tuple(RoutedTrade.from_dict(event) for event in value["events"]),
        event_count=int(value["event_count"]),
        net_pnl=float(value["net_pnl"]),
        cost_stress_1_5x_net=float(value["cost_stress_1_5x_net"]),
        maximum_drawdown=float(value["maximum_drawdown"]),
        best_positive_event_share=float(value["best_positive_event_share"]),
        exit_implementation=str(value["exit_implementation"]),
    )


def assert_exact_runtime_metadata(
    expected_rows: Sequence[Mapping[str, Any]],
    actual_rows: Sequence[Mapping[str, Any]],
) -> None:
    if len(expected_rows) != 48 or len(actual_rows) != 48:
        raise SelectorCrossfitError(
            f"metadata reconciliation requires 48/48 rows, got {len(expected_rows)}/{len(actual_rows)}"
        )
    for index, (expected, actual) in enumerate(
        zip(expected_rows, actual_rows, strict=True)
    ):
        if dict(expected) != dict(actual):
            sleeve_id = str(expected.get("sleeve_id") or actual.get("sleeve_id") or index)
            differing = sorted(
                key
                for key in set(expected) | set(actual)
                if expected.get(key) != actual.get(key)
            )
            raise SelectorCrossfitError(
                f"campaign 0023 runtime metadata mismatch for {sleeve_id}: {differing}"
            )


def _common_session_days(
    runtimes: Iterable[ExactSleeveRuntime],
) -> tuple[int, ...]:
    values = list(runtimes)
    if not values:
        raise SelectorCrossfitError("event ledger contains no runtimes")
    common = set(values[0].eligible_session_days)
    for runtime in values[1:]:
        common.intersection_update(runtime.eligible_session_days)
    days = tuple(sorted(common))
    if not days:
        raise SelectorCrossfitError("event ledger has no common chronological sessions")
    return days


def _verify_self_hash(value: Mapping[str, Any], field: str, *, label: str) -> None:
    payload = dict(value)
    claimed = str(payload.pop(field, ""))
    if not claimed or canonical_hash(payload) != claimed:
        raise SelectorCrossfitError(f"{label} semantic hash drift")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _write_immutable_bytes(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise SelectorCrossfitError(f"refusing to overwrite divergent artifact: {path}")
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _project_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "MISSION_CONTRACT.md").is_file():
            return parent
    raise SelectorCrossfitError("project root not found")


def _root_relative(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path.resolve())


def _session_date(session_day: int) -> str:
    return (date(1970, 1, 1) + timedelta(days=int(session_day))).isoformat()


__all__ = [
    "DesignLedgerView",
    "EXPECTED_CAMPAIGN_ID",
    "HeldOutLedgerView",
    "IsolatedOuterFoldLedger",
    "OuterFold",
    "RecoveredCampaignLedger",
    "SelectorCrossfitError",
    "TemporalBlock",
    "TemporalBlockPlan",
    "build_purged_temporal_block_plan",
    "assert_exact_runtime_metadata",
    "canonical_hash",
    "file_sha256",
    "isolate_outer_fold_ledger",
    "leave_one_block_out_folds",
    "load_recovered_event_ledger",
    "locate_frozen_feature_bundles",
    "recover_campaign_0023_event_ledger",
    "write_recovered_event_ledger",
]
