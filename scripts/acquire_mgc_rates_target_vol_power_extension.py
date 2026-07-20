#!/usr/bin/env python3
"""Governed acquisition for the frozen MGC rates->target-vol power extension.

The command is metadata-only unless ``--execute`` is supplied.  Its two raw
downloads are immutable and atomic.  Contract identity is recovered from the
continuous mappings embedded in the downloaded definition DBN and verified
against date-aware definition rows; no raw contract is inferred from a
calendar rule.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterator, Mapping

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.budget import (
    AUTO_UNDER_HARD_CAP,
    DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD,
    DatabentoBudgetConfig,
    DatabentoSpendRecord,
    append_spend_record,
    cumulative_spend,
    read_ledger,
    request_id_for,
    sha256_file,
    utc_now,
)
from hydra.data.contract_mapping import ContractInfo, RollMap, valid_outright_future_symbol
from hydra.data.databento_loader import _import_databento, load_api_key
from hydra.economic_evolution.schema import stable_hash
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


CARD_PATH = "config/research/mgc_rates_target_vol_power_extension_v1.json"
EXPECTED_CARD_HASH = "dc8b4c275107f5c0b6e3b7c404cecf75ba11b9e586bfc2aa837b9f487d3c52b5"
DATASET = "GLBX.MDP3"
SYMBOLS = ("ZN.c.0", "TN.c.0", "MGC.v.0")
ROOTS = ("ZN", "TN", "MGC")
START = "2015-09-01"
END = "2021-11-04"
STYPE_IN = "continuous"
STYPE_OUT = "instrument_id"
CANDIDATE_ID = "volconv_MGC_rates_target_vol_gap_oco_open_v1"
EXPECTED = {
    "ohlcv-1m": {
        "records": 4_350_698,
        "billable_bytes": 243_639_088,
        "cost_usd": 15.883460789919,
    },
    "definition": {
        "records": 5_705,
        "billable_bytes": 2_053_800,
        "cost_usd": 0.003251675516,
    },
}
EXPECTED_TOTAL_COST_USD = 15.886712465435
MINIMUM_REMAINING_RESERVE_USD = 25.0
HARD_CAP_USD = 200.720719923081
POWER_THRESHOLDS = {"DISCOVERY": 60, "VALIDATION": 12, "FINAL_DEVELOPMENT": 20}
TEMPORAL_ROLES = (
    {
        "role": "WARMUP_ONLY",
        "start": "2015-09-01",
        "end": "2015-10-01",
        "candidate_modification_allowed": False,
        "economic_outcomes_allowed": False,
    },
    {
        "role": "DISCOVERY",
        "start": "2015-10-01",
        "end": "2019-09-10",
        "candidate_modification_allowed": True,
        "economic_outcomes_allowed": True,
    },
    {
        "role": "VALIDATION",
        "start": "2019-09-10",
        "end": "2020-06-24",
        "candidate_modification_allowed": False,
        "economic_outcomes_allowed": True,
    },
    {
        "role": "FINAL_DEVELOPMENT",
        "start": "2020-06-24",
        "end": "2021-11-04",
        "candidate_modification_allowed": False,
        "economic_outcomes_allowed": True,
    },
)
PURPOSE = (
    "frozen +25% MGC rates-target-vol power extension; single unchanged "
    "candidate; pre-Q4 only; no promotion, broker, or orders"
)
DEFAULT_RECEIPT = (
    "reports/data_access/"
    "mgc_rates_target_vol_power_extension_acquisition_receipt.json"
)
ACCESS_LEDGER = "reports/data_access/data_access_ledger.jsonl"
RECEIPT_SCHEMA = "hydra_mgc_rates_target_vol_power_extension_receipt_v1"
MGC_MAP_TYPE = "EXPLICIT_DATABENTO_MGC_VOLUME_FRONT_DEFINITION_V1"
TREASURY_RECEIPT_SCHEMA = "hydra_zn_tn_explicit_delivery_sync_receipt_v1"


class MGCPowerExtensionError(RuntimeError):
    """The frozen acquisition contract cannot be completed safely."""


def load_and_validate_card(root: str | Path, path: str | Path = CARD_PATH) -> dict[str, Any]:
    project = Path(root).resolve()
    card_path = Path(path)
    if not card_path.is_absolute():
        card_path = project / card_path
    try:
        card = json.loads(card_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MGCPowerExtensionError("frozen decision card is unavailable or invalid") from exc
    core = dict(card)
    claimed = str(core.pop("card_hash", ""))
    if (
        not claimed
        or stable_hash(core) != claimed
        or claimed != EXPECTED_CARD_HASH
    ):
        raise MGCPowerExtensionError("decision-card hash drift")
    data = dict(card.get("data_contract") or {})
    frozen_candidate = dict(card.get("frozen_candidate") or {})
    budget = dict(card.get("budget_contract") or {})
    governance = dict(card.get("governance") or {})
    roll_contract = dict(card.get("roll_and_definition_contract") or {})
    if (
        data.get("dataset") != DATASET
        or tuple(data.get("symbols") or ()) != SYMBOLS
        or data.get("stype_in") != STYPE_IN
        or data.get("start") != START
        or data.get("end") != END
        or not math.isclose(
            float(data.get("combined_estimated_cost_usd", float("nan"))),
            EXPECTED_TOTAL_COST_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or frozen_candidate.get("candidate_id") != CANDIDATE_ID
        or frozen_candidate.get("candidate_count") != 1
        or frozen_candidate.get("parameter_mutation_allowed") is not False
        or card.get("power_thresholds", {}).get("DISCOVERY") != 60
        or card.get("power_thresholds", {}).get("VALIDATION") != 12
        or card.get("power_thresholds", {}).get("FINAL_DEVELOPMENT") != 20
        or list(card.get("chronological_roles") or ()) != [dict(row) for row in TEMPORAL_ROLES]
        or not math.isclose(float(budget.get("cumulative_hard_cap_usd", -1)), HARD_CAP_USD)
        or not math.isclose(
            float(budget.get("minimum_remaining_reserve_usd", -1)),
            MINIMUM_REMAINING_RESERVE_USD,
        )
        or not math.isclose(
            float(budget.get("maximum_bundle_cost_usd", -1)),
            EXPECTED_TOTAL_COST_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or roll_contract.get("raw_contracts_may_be_guessed") is not False
        or governance.get("q4_2024_access_allowed") is not False
        or governance.get("protected_holdout_access_allowed") is not False
        or governance.get("broker_connection_allowed") is not False
        or governance.get("orders_allowed") is not False
        or governance.get("outcome_read_before_acquisition_allowed") is not False
        or governance.get("mission_state_writes_allowed") is not False
        or governance.get("promotion_allowed") is not False
    ):
        raise MGCPowerExtensionError("decision-card acquisition contract drift")
    if END > "2024-10-01":
        raise MGCPowerExtensionError("frozen request enters protected Q4")
    return card


def estimate_or_acquire(
    *,
    root: str | Path,
    client: Any,
    execute: bool,
    budget: DatabentoBudgetConfig | None = None,
    card_path: str | Path = CARD_PATH,
    receipt_path: str | Path = DEFAULT_RECEIPT,
) -> dict[str, Any]:
    project = Path(root).resolve()
    card = load_and_validate_card(project, card_path)
    card_hash = str(card["card_hash"])
    cfg = _bound_budget(project, budget)
    requests = {schema: _request(schema) for schema in EXPECTED}
    bundle_id = request_id_for(
        {
            "card_hash": card_hash,
            "requests": requests,
            "candidate_id": CANDIDATE_ID,
            "purpose": PURPOSE,
        }
    )
    paths = _paths(project, bundle_id, receipt_path)

    with _optional_lock(paths["lock"], enabled=execute):
        if paths["receipt"].is_file():
            return _load_existing_receipt(
                paths["receipt"],
                bundle_id=bundle_id,
                card_hash=card_hash,
                budget=cfg,
                paths=paths,
            )

        estimates: dict[str, dict[str, Any]] = {}
        for schema, request in requests.items():
            estimates[schema] = {
                "records": int(client.metadata.get_record_count(**request)),
                "billable_bytes": int(client.metadata.get_billable_size(**request)),
                "cost_usd": float(client.metadata.get_cost(**request)),
            }
            _verify_estimate(schema, estimates[schema])
        total_cost = math.fsum(row["cost_usd"] for row in estimates.values())
        if not math.isclose(
            total_cost, EXPECTED_TOTAL_COST_USD, rel_tol=0.0, abs_tol=1e-12
        ):
            raise MGCPowerExtensionError("combined official cost drift")
        actual_before, outstanding_before = _committed_spend(cfg)
        ledger_state = _bundle_ledger_state(
            cfg, bundle_id=bundle_id, estimate=total_cost
        )
        incremental_commit = 0.0 if ledger_state != "NONE" else total_cost
        effective_after = actual_before + outstanding_before + incremental_commit
        effective_cap = min(cfg.hard_cap_usd, cfg.safety_ceiling_usd)
        if effective_cap - effective_after < MINIMUM_REMAINING_RESERVE_USD:
            raise MGCPowerExtensionError("frozen acquisition would violate reserve")

        plan = {
            "schema": "hydra_mgc_rates_target_vol_power_extension_plan_v1",
            "bundle_id": bundle_id,
            "card_hash": card_hash,
            "candidate_ids": [CANDIDATE_ID],
            "requests": requests,
            "official_estimates": estimates,
            "official_total_cost_usd": total_cost,
            "estimated_incremental_spend_usd": incremental_commit,
            "temporal_roles": [dict(row) for row in TEMPORAL_ROLES],
            "power_thresholds": dict(POWER_THRESHOLDS),
            "cumulative_actual_before_usd": actual_before,
            "outstanding_estimates_before_usd": outstanding_before,
            "cumulative_hard_cap_usd": cfg.hard_cap_usd,
            "safety_ceiling_usd": cfg.safety_ceiling_usd,
            "effective_cap_usd": effective_cap,
            "minimum_remaining_reserve_usd": MINIMUM_REMAINING_RESERVE_USD,
            "remaining_after_estimate_usd": effective_cap - effective_after,
            "q4_access_count_delta": 0,
            "protected_data_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "execute": bool(execute),
        }
        if not execute:
            return {
                **plan,
                "download_status": "DRY_RUN_ONLY",
                "network_data_request_made": False,
                "files_created": 0,
                "outcomes_read": 0,
            }

        _reserve_spend_once(cfg, bundle_id=bundle_id, estimate=total_cost)
        ledger_state = _bundle_ledger_state(
            cfg, bundle_id=bundle_id, estimate=total_cost
        )
        if ledger_state == "COMPLETED":
            for raw_path in (paths["raw_ohlcv"], paths["raw_definitions"]):
                if not raw_path.is_file() or raw_path.stat().st_size <= 0:
                    raise MGCPowerExtensionError(
                        "completed acquisition is missing an immutable raw artifact"
                    )
        ohlcv_network = _download_once(
            client, requests["ohlcv-1m"], paths["raw_ohlcv"]
        )
        definition_network = _download_once(
            client, requests["definition"], paths["raw_definitions"]
        )
        mgc_map, treasury_receipt = _build_roll_artifacts_from_dbn(
            paths["raw_definitions"], paths["raw_ohlcv"]
        )
        _persist_json_once(paths["mgc_roll_map"], mgc_map)
        _persist_json_once(paths["treasury_sync"], treasury_receipt)
        inventory = [
            _file_receipt("RAW_OHLCV_DBN", paths["raw_ohlcv"]),
            _file_receipt("RAW_DEFINITION_DBN", paths["raw_definitions"]),
            _file_receipt("MGC_VOLUME_FRONT_ROLL_MAP", paths["mgc_roll_map"]),
            _file_receipt("ZN_TN_DELIVERY_SYNC_RECEIPT", paths["treasury_sync"]),
        ]
        _complete_spend_once(
            cfg,
            bundle_id=bundle_id,
            estimate=total_cost,
            inventory=inventory,
        )
        _record_access_roles_once(
            paths["access_ledger"], bundle_id=bundle_id, card_hash=card_hash
        )
        _estimated_after, actual_after = cumulative_spend(cfg.ledger_path)
        core = {
            **plan,
            "schema": RECEIPT_SCHEMA,
            "created_at_utc": utc_now(),
            "download_status": "DOWNLOADED",
            "network_data_request_made": bool(ohlcv_network or definition_network),
            "ohlcv_network_request_made": bool(ohlcv_network),
            "definition_network_request_made": bool(definition_network),
            "actual_cost_usd": total_cost,
            "cumulative_actual_after_usd": actual_after,
            "raw_immutable": True,
            "files": inventory,
            "mgc_roll_map_hash": mgc_map["roll_map_hash"],
            "treasury_sync_hash": treasury_receipt["receipt_hash"],
            "spend_ledger_request_id": bundle_id,
            "data_access_markers": [
                _access_marker(bundle_id, row["role"]) for row in TEMPORAL_ROLES
            ],
            "outcomes_read": 0,
            "economic_replay_started": False,
            "promotion_changes": 0,
            "runtime_or_manifest_modified": False,
        }
        core.pop("execute", None)
        receipt = {**core, "receipt_hash": stable_hash(core)}
        _persist_json_once(paths["receipt"], receipt)
        return receipt


def _request(schema: str) -> dict[str, Any]:
    return {
        "dataset": DATASET,
        "schema": schema,
        "symbols": list(SYMBOLS),
        "stype_in": STYPE_IN,
        "start": START,
        "end": END,
    }


def _verify_estimate(schema: str, observed: Mapping[str, Any]) -> None:
    expected = EXPECTED[schema]
    if (
        int(observed.get("records", -1)) != expected["records"]
        or int(observed.get("billable_bytes", -1)) != expected["billable_bytes"]
        or not math.isclose(
            float(observed.get("cost_usd", float("nan"))),
            expected["cost_usd"],
            rel_tol=0.0,
            abs_tol=1e-12,
        )
    ):
        raise MGCPowerExtensionError(f"official {schema} estimate drift")


def _build_roll_artifacts_from_dbn(
    definition_path: Path, ohlcv_path: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    db = _import_databento()
    store = db.DBNStore.from_file(definition_path)
    mappings = _normalize_mappings(dict(store.metadata.mappings or {}))
    ohlcv_store = db.DBNStore.from_file(ohlcv_path)
    ohlcv_mappings = _normalize_mappings(dict(ohlcv_store.metadata.mappings or {}))
    if ohlcv_mappings != mappings:
        raise MGCPowerExtensionError(
            "OHLCV and definition continuous mappings do not reconcile"
        )
    first_tradable = _first_tradable_at_by_instrument(ohlcv_store, mappings)
    definitions = store.to_df(pretty_ts=True, map_symbols=False).reset_index()
    return build_roll_artifacts(
        mappings,
        definitions,
        first_tradable_at_by_instrument=first_tradable,
    )


def _first_tradable_at_by_instrument(
    ohlcv_store: Any,
    mappings: Mapping[str, list[dict[str, str]]],
) -> dict[str, str]:
    """Return the first actual OHLCV event for every mapped instrument.

    Only the DBN record headers (``instrument_id`` and ``ts_event``) are used.
    No price, volume, label, or economic outcome is inspected.  This makes the
    definition-availability cutoff exact even when a mapping date starts at
    midnight but its first tradable event occurs later in the session.
    """

    expected = {
        str(row["s"]): (str(row["d0"]), str(row["d1"]), symbol)
        for symbol, intervals in mappings.items()
        for row in intervals
    }
    if len(expected) != sum(len(intervals) for intervals in mappings.values()):
        raise MGCPowerExtensionError(
            "instrument ID reuse prevents an unambiguous first-tradable cutoff"
        )
    first_ns: dict[str, int] = {}
    for chunk in ohlcv_store.to_ndarray(schema="ohlcv-1m", count=250_000):
        names = set(chunk.dtype.names or ())
        if not {"instrument_id", "ts_event"}.issubset(names):
            raise MGCPowerExtensionError("OHLCV DBN lacks causal record headers")
        frame = pd.DataFrame(
            {
                "instrument_id": chunk["instrument_id"].astype(str),
                "ts_event": chunk["ts_event"].astype("uint64"),
            }
        )
        for instrument_id, timestamp in frame.groupby(
            "instrument_id", sort=False
        )["ts_event"].min().items():
            if instrument_id not in expected:
                continue
            observed = int(timestamp)
            first_ns[instrument_id] = min(
                observed, first_ns.get(instrument_id, observed)
            )

    if set(first_ns) != set(expected):
        missing = sorted(set(expected) - set(first_ns))
        raise MGCPowerExtensionError(
            f"mapped instruments lack an OHLCV event: {missing}"
        )
    output: dict[str, str] = {}
    for instrument_id, timestamp in first_ns.items():
        start, end, symbol = expected[instrument_id]
        first = pd.Timestamp(timestamp, unit="ns", tz="UTC")
        if not (
            pd.Timestamp(start, tz="UTC")
            <= first
            < pd.Timestamp(end, tz="UTC")
        ):
            raise MGCPowerExtensionError(
                "first OHLCV event is outside its mapping interval: "
                f"{symbol}/{instrument_id}/{first.isoformat()}"
            )
        output[instrument_id] = first.isoformat()
    return output


def _normalize_mappings(raw: Mapping[str, Any]) -> dict[str, list[dict[str, str]]]:
    output: dict[str, list[dict[str, str]]] = {}
    for symbol, intervals in raw.items():
        rows = []
        for interval in intervals:
            value = dict(interval)
            d0 = str(value.get("start_date") or value.get("d0") or "")[:10]
            d1 = str(value.get("end_date") or value.get("d1") or "")[:10]
            instrument_id = str(value.get("symbol") or value.get("s") or "")
            clipped_start, clipped_end = max(d0, START), min(d1, END)
            if clipped_start < clipped_end:
                rows.append({"d0": clipped_start, "d1": clipped_end, "s": instrument_id})
        rows.sort(key=lambda row: (row["d0"], row["d1"], row["s"]))
        output[str(symbol)] = rows
    if set(output) != set(SYMBOLS):
        raise MGCPowerExtensionError("definition DBN continuous mappings are incomplete")
    for symbol in SYMBOLS:
        _verify_coverage(output[symbol], label=symbol)
    return output


def build_roll_artifacts(
    mappings: Mapping[str, list[dict[str, str]]],
    definitions: pd.DataFrame,
    *,
    first_tradable_at_by_instrument: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build both roll artifacts solely from DBN mappings and definitions."""

    contracts: dict[str, list[ContractInfo]] = {root: [] for root in ROOTS}
    definition_audit: dict[str, list[dict[str, Any]]] = {
        root: [] for root in ROOTS
    }
    for symbol in SYMBOLS:
        root = symbol.split(".", 1)[0]
        intervals = list(mappings.get(symbol) or ())
        _verify_coverage(intervals, label=symbol)
        for index, interval in enumerate(intervals):
            instrument_id = str(interval["s"])
            first_tradable_at: Any | None = None
            if first_tradable_at_by_instrument is not None:
                if instrument_id not in first_tradable_at_by_instrument:
                    raise MGCPowerExtensionError(
                        f"first tradable event missing for instrument {instrument_id}"
                    )
                first_tradable_at = first_tradable_at_by_instrument[instrument_id]
                first = pd.Timestamp(first_tradable_at)
                if first.tzinfo is None:
                    first = first.tz_localize("UTC")
                else:
                    first = first.tz_convert("UTC")
                if not (
                    pd.Timestamp(str(interval["d0"]), tz="UTC")
                    <= first
                    < pd.Timestamp(str(interval["d1"]), tz="UTC")
                ):
                    raise MGCPowerExtensionError(
                        "first tradable event is outside the mapping interval for "
                        f"{symbol}/{instrument_id}"
                    )
            try:
                definition = _resolve_definition(
                    definitions,
                    instrument_id=instrument_id,
                    active_start=str(interval["d0"]),
                    root=root,
                    first_tradable_at=first_tradable_at,
                )
            except (ValueError, KeyError, TypeError) as exc:
                raise MGCPowerExtensionError(
                    f"definition resolution failed for {symbol}/{instrument_id}"
                ) from exc
            raw_symbol = str(definition["raw_symbol"]).upper()
            if not valid_outright_future_symbol(root, raw_symbol):
                raise MGCPowerExtensionError(f"invalid outright contract {raw_symbol!r}")
            month_code, year = _contract_month_and_year(
                root, raw_symbol, str(interval["d0"])
            )
            if root in {"ZN", "TN"} and month_code not in "HMUZ":
                raise MGCPowerExtensionError("Treasury definition is not quarterly")
            expiration = pd.Timestamp(definition.get("expiration"))
            if pd.isna(expiration):
                raise MGCPowerExtensionError(f"definition {raw_symbol} lacks expiration")
            tick_size = float(definition["min_price_increment"])
            point_value = float(definition.get("unit_of_measure_qty") or 0.0)
            if (
                not math.isfinite(tick_size)
                or tick_size <= 0.0
                or not math.isfinite(point_value)
                or point_value <= 0.0
            ):
                raise MGCPowerExtensionError(
                    f"definition {raw_symbol} lacks positive tick/multiplier"
                )
            if root == "MGC" and (
                not math.isclose(point_value, 10.0, rel_tol=0.0, abs_tol=1e-12)
                or not math.isclose(tick_size, 0.1, rel_tol=0.0, abs_tol=1e-12)
                or not math.isclose(
                    tick_size * point_value, 1.0, rel_tol=0.0, abs_tol=1e-12
                )
            ):
                raise MGCPowerExtensionError(
                    f"MGC definition economics drift for {raw_symbol}: "
                    f"tick={tick_size}, multiplier={point_value}, "
                    f"tick_value={tick_size * point_value}"
                )
            activation = definition.get("activation")
            definition_audit[root].append(
                {
                    "instrument_id": instrument_id,
                    "contract": raw_symbol,
                    "mapping_start": str(interval["d0"]),
                    "mapping_end": str(interval["d1"]),
                    "definition_available_at": str(
                        definition["definition_available_at"]
                    ),
                    "first_tradable_at": (
                        str(definition.get("first_tradable_at"))
                        if definition.get("first_tradable_at") is not None
                        else None
                    ),
                    "availability_precedes_first_tradable": bool(
                        definition["availability_precedes_cutoff"]
                    ),
                    "cutoff_basis": str(definition["cutoff_basis"]),
                }
            )
            contracts[root].append(
                ContractInfo(
                    root=root,
                    contract=raw_symbol,
                    month_code=month_code,
                    year=year,
                    expiry_date=str(expiration.date()),
                    last_trade_date=str(expiration.date()),
                    active_start=str(interval["d0"]),
                    active_end=str(interval["d1"]),
                    roll_date=(str(interval["d0"]) if index else str(interval["d1"])),
                    tick_size=tick_size,
                    tick_value=tick_size * point_value,
                    point_value=point_value,
                    contract_multiplier=point_value,
                    is_micro=root == "MGC",
                    instrument_id=instrument_id,
                    parent_symbol=root,
                    continuous_symbol=symbol,
                    activation_time=(
                        pd.Timestamp(activation).isoformat()
                        if activation is not None and not pd.isna(activation)
                        else None
                    ),
                    deactivation_time=str(interval["d1"]),
                    roll_reason=(
                        "databento_previous_day_volume_rank_transition"
                        if root == "MGC"
                        else "databento_continuous_front_contract_transition"
                    ),
                    transition_uncertainty="date_level_symbology_interval",
                )
            )

    mgc_roll = RollMap(
        dataset=DATASET,
        schema="ohlcv-1m",
        map_type=MGC_MAP_TYPE,
        symbols=["MGC"],
        contracts=contracts["MGC"],
        unsafe_window_days=1,
        notes=[
            "MGC.v.0 intervals came from the downloaded DBN continuous mapping.",
            "Every raw contract, tick and expiry was verified against a date-aware definition row.",
            "No calendar-derived or guessed contract is permitted.",
        ],
        source_metadata={
            "period_start": START,
            "period_end": END,
            "continuous_symbol": "MGC.v.0",
            "definition_sourced": True,
            "definition_resolution": (
                "available_at_not_after_first_actual_ohlcv_event"
                if first_tradable_at_by_instrument is not None
                else "available_at_not_after_mapping_start_midnight"
            ),
            "definition_availability_audit": definition_audit["MGC"],
            "q4_2024_excluded": True,
        },
    ).to_dict()
    sync_intervals, mismatch_intervals = _treasury_delivery_sync(
        contracts["ZN"], contracts["TN"]
    )
    treasury_core = {
        "schema": TREASURY_RECEIPT_SCHEMA,
        "dataset": DATASET,
        "period_start": START,
        "period_end": END,
        "continuous_symbols": ["ZN.c.0", "TN.c.0"],
        "policy": "EXPLICIT_CONTRACT_DELIVERY_SYNC_NO_FORWARD_FILL",
        "definition_sourced": True,
        "definition_resolution": (
            "available_at_not_after_first_actual_ohlcv_event"
            if first_tradable_at_by_instrument is not None
            else "available_at_not_after_mapping_start_midnight"
        ),
        "definition_availability_audit": [
            *definition_audit["ZN"],
            *definition_audit["TN"],
        ],
        "root_rolls": {
            root: {
                "contract_count": len(contracts[root]),
                "contracts": [asdict(item) for item in contracts[root]],
            }
            for root in ("ZN", "TN")
        },
        "delivery_sync_intervals": sync_intervals,
        "delivery_mismatch_count": len(mismatch_intervals),
        "delivery_mismatch_intervals_excluded": mismatch_intervals,
        "gap_count": 0,
        "forward_fill_rows": 0,
        "q4_access_count_delta": 0,
    }
    return mgc_roll, {**treasury_core, "receipt_hash": stable_hash(treasury_core)}


def _verify_coverage(rows: list[Mapping[str, Any]], *, label: str) -> None:
    if not rows or str(rows[0].get("d0")) != START or str(rows[-1].get("d1")) != END:
        raise MGCPowerExtensionError(f"mapping coverage boundaries drift for {label}")
    previous_end = START
    for row in rows:
        d0, d1 = str(row.get("d0")), str(row.get("d1"))
        instrument_id = str(row.get("s") or "")
        if (
            d0 != previous_end
            or d0 >= d1
            or not instrument_id.isdigit()
            or int(instrument_id) <= 0
        ):
            raise MGCPowerExtensionError(f"mapping gap, overlap, or ID defect for {label}")
        previous_end = d1


def _resolve_definition(
    frame: pd.DataFrame,
    *,
    instrument_id: str,
    active_start: str,
    root: str,
    first_tradable_at: Any | None = None,
) -> dict[str, Any]:
    required = {
        "ts_event",
        "ts_recv",
        "instrument_id",
        "raw_symbol",
        "instrument_class",
        "security_type",
        "asset",
        "min_price_increment",
        "unit_of_measure_qty",
        "expiration",
        "activation",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise MGCPowerExtensionError(f"definition history lacks columns: {missing}")
    work = frame.copy()
    work["instrument_id_key"] = work["instrument_id"].astype(str)
    work["ts_event"] = pd.to_datetime(work["ts_event"], utc=True, errors="coerce")
    work["ts_recv"] = pd.to_datetime(work["ts_recv"], utc=True, errors="coerce")
    instrument_rows = work[work["instrument_id_key"] == str(instrument_id)]
    if instrument_rows.empty:
        raise MGCPowerExtensionError(f"definition missing for instrument {instrument_id}")
    if instrument_rows[["ts_event", "ts_recv"]].isna().any(axis=None):
        raise MGCPowerExtensionError(
            f"definition availability is incomplete for instrument {instrument_id}"
        )
    work["definition_available_at"] = work[["ts_event", "ts_recv"]].max(axis=1)
    rows = work[work["instrument_id_key"] == str(instrument_id)].sort_values(
        "definition_available_at"
    )
    if first_tradable_at is None:
        cutoff = pd.Timestamp(active_start, tz="UTC")
        cutoff_basis = "MAPPING_START_MIDNIGHT"
    else:
        cutoff = pd.Timestamp(first_tradable_at)
        if cutoff.tzinfo is None:
            cutoff = cutoff.tz_localize("UTC")
        else:
            cutoff = cutoff.tz_convert("UTC")
        cutoff_basis = "FIRST_ACTUAL_OHLCV_EVENT"
    available = rows[rows["definition_available_at"] <= cutoff]
    if available.empty:
        raise MGCPowerExtensionError(
            "future-only definition for instrument "
            f"{instrument_id} at causal cutoff {cutoff.isoformat()}"
        )
    selected_available_at = available.iloc[-1]["definition_available_at"]
    selected_rows = rows[rows["definition_available_at"] == selected_available_at]
    signature_columns = [
        "raw_symbol",
        "instrument_class",
        "security_type",
        "asset",
        "min_price_increment",
        "unit_of_measure_qty",
        "expiration",
        "activation",
    ]
    if len(selected_rows[signature_columns].astype(str).drop_duplicates()) != 1:
        raise MGCPowerExtensionError(
            f"ambiguous definition for instrument {instrument_id} at {selected_available_at}"
        )
    selected = selected_rows.sort_values(
        ["ts_event", "ts_recv"], kind="mergesort"
    ).iloc[-1].to_dict()
    raw_symbol = str(selected.get("raw_symbol") or "").upper()
    if (
        not valid_outright_future_symbol(root, raw_symbol)
        or str(selected.get("instrument_class") or "") != "F"
        or str(selected.get("security_type") or "") != "FUT"
        or str(selected.get("asset") or "") != root
    ):
        raise MGCPowerExtensionError(
            f"definition identity mismatch for {root}/{instrument_id}/{raw_symbol}"
        )
    selected["raw_symbol"] = raw_symbol
    selected["ts_event"] = pd.Timestamp(selected["ts_event"]).isoformat()
    selected["ts_recv"] = pd.Timestamp(selected["ts_recv"]).isoformat()
    selected["definition_available_at"] = pd.Timestamp(
        selected_available_at
    ).isoformat()
    selected["first_tradable_at"] = (
        cutoff.isoformat() if first_tradable_at is not None else None
    )
    selected["availability_precedes_cutoff"] = bool(
        pd.Timestamp(selected_available_at) <= cutoff
    )
    selected["cutoff_basis"] = cutoff_basis
    return selected


def _contract_month_and_year(root: str, raw: str, active_start: str) -> tuple[str, int]:
    match = re.fullmatch(rf"{re.escape(root)}([FGHJKMNQUVXZ])(\d{{1,2}})", raw)
    if match is None:
        raise MGCPowerExtensionError(f"unparseable contract {raw!r}")
    code, digits = match.groups()
    active_year = int(active_start[:4])
    if len(digits) == 2:
        year = 2000 + int(digits)
    else:
        decade = active_year - active_year % 10
        year = min(
            (decade - 10 + int(digits), decade + int(digits), decade + 10 + int(digits)),
            key=lambda value: abs(value - active_year),
        )
    return code, year


def _delivery_month(contract: ContractInfo) -> str:
    month = {"H": 3, "M": 6, "U": 9, "Z": 12}.get(contract.month_code)
    if month is None:
        raise MGCPowerExtensionError("non-quarterly Treasury contract")
    return f"{contract.year:04d}{month:02d}"


def _treasury_delivery_sync(
    zn_contracts: list[ContractInfo], tn_contracts: list[ContractInfo]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    output: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    for zn in zn_contracts:
        for tn in tn_contracts:
            start, end = max(zn.active_start, tn.active_start), min(zn.active_end, tn.active_end)
            if start >= end:
                continue
            zn_delivery, tn_delivery = _delivery_month(zn), _delivery_month(tn)
            if zn_delivery != tn_delivery:
                mismatches.append(
                    {
                        "start": start,
                        "end": end,
                        "zn_contract": zn.contract,
                        "zn_delivery_month": zn_delivery,
                        "tn_contract": tn.contract,
                        "tn_delivery_month": tn_delivery,
                        "policy": "EXCLUDED_NO_FORWARD_FILL",
                    }
                )
                continue
            output.append(
                {
                    "start": start,
                    "end": end,
                    "delivery_month": zn_delivery,
                    "zn_contract": zn.contract,
                    "zn_instrument_id": zn.instrument_id,
                    "tn_contract": tn.contract,
                    "tn_instrument_id": tn.instrument_id,
                }
            )
    output.sort(key=lambda row: (row["start"], row["end"]))
    if not output:
        raise MGCPowerExtensionError("no ZN/TN same-delivery coverage remains")
    coverage_rows = [
        {"d0": row["start"], "d1": row["end"], "s": str(index + 1)}
        for index, row in enumerate(sorted([*output, *mismatches], key=lambda row: row["start"]))
    ]
    _verify_coverage(coverage_rows, label="ZN_TN_DELIVERY_SYNC")
    return output, mismatches


def _download_once(client: Any, request: Mapping[str, Any], path: Path) -> bool:
    if path.is_file():
        if path.stat().st_size <= 0:
            raise MGCPowerExtensionError(f"empty governed cache file: {path}")
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        client.timeseries.get_range(
            **dict(request), stype_out=STYPE_OUT, path=str(temporary)
        )
        if not temporary.is_file() or temporary.stat().st_size <= 0:
            raise MGCPowerExtensionError("Databento returned an empty raw file")
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return True


def _validate_spend_identity(row: Mapping[str, Any], *, bundle_id: str) -> None:
    if (
        row.get("request_id") != bundle_id
        or row.get("dataset") != DATASET
        or row.get("schema") != "ohlcv-1m+definition"
        or row.get("symbols") != list(SYMBOLS)
        or row.get("stype_in") != STYPE_IN
        or row.get("start") != START
        or row.get("end") != END
        or row.get("research_purpose") != PURPOSE
    ):
        raise MGCPowerExtensionError("existing spend record identity drift")


def _bundle_ledger_state(
    budget: DatabentoBudgetConfig,
    *,
    bundle_id: str,
    estimate: float,
    inventory_checksum: str | None = None,
) -> str:
    """Return NONE, RESERVED, or COMPLETED for the append-only journal."""

    rows = [
        row
        for row in read_ledger(budget.ledger_path)
        if row.get("request_id") == bundle_id
    ]
    if not rows:
        return "NONE"
    if len(rows) not in {1, 2}:
        raise MGCPowerExtensionError("spend reservation/completion cardinality drift")
    reservation = rows[0]
    _validate_spend_identity(reservation, bundle_id=bundle_id)
    if (
        reservation.get("download_status") != "ESTIMATED_ONLY"
        or not math.isclose(
            float(reservation.get("estimated_cost_usd") or 0.0),
            estimate,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or reservation.get("actual_cost_usd") is not None
        or reservation.get("resulting_file") is not None
        or reservation.get("checksum") is not None
    ):
        raise MGCPowerExtensionError("spend reservation drift")
    if len(rows) == 1:
        return "RESERVED"

    completion = rows[1]
    _validate_spend_identity(completion, bundle_id=bundle_id)
    checksum = str(completion.get("checksum") or "")
    if (
        completion.get("download_status") != "DOWNLOADED"
        or not math.isclose(
            float(completion.get("estimated_cost_usd") or 0.0),
            0.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or not math.isclose(
            float(completion.get("actual_cost_usd") or 0.0),
            estimate,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or not checksum
        or (inventory_checksum is not None and checksum != inventory_checksum)
    ):
        raise MGCPowerExtensionError("spend completion drift")
    return "COMPLETED"


def _reserve_spend_once(
    budget: DatabentoBudgetConfig,
    *,
    bundle_id: str,
    estimate: float,
) -> None:
    state = _bundle_ledger_state(budget, bundle_id=bundle_id, estimate=estimate)
    if state != "NONE":
        return
    actual_before, outstanding_before = _committed_spend(budget)
    effective_cap = min(budget.hard_cap_usd, budget.safety_ceiling_usd)
    projected_commitment = actual_before + outstanding_before + estimate
    if effective_cap - projected_commitment < MINIMUM_REMAINING_RESERVE_USD:
        raise MGCPowerExtensionError("spend reservation would violate reserve")
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=bundle_id,
            timestamp_utc=utc_now(),
            dataset=DATASET,
            schema="ohlcv-1m+definition",
            symbols=list(SYMBOLS),
            stype_in=STYPE_IN,
            start=START,
            end=END,
            estimated_cost_usd=estimate,
            actual_cost_usd=None,
            cumulative_estimated_spend_usd=projected_commitment,
            cumulative_actual_spend_usd=actual_before,
            cache_hit=False,
            research_purpose=PURPOSE,
            candidate_tier="TIER_H_FROZEN_POWER_EXTENSION_INPUT",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=None,
            checksum=None,
            download_status="ESTIMATED_ONLY",
        ),
    )


def _complete_spend_once(
    budget: DatabentoBudgetConfig,
    *,
    bundle_id: str,
    estimate: float,
    inventory: list[dict[str, Any]],
) -> None:
    checksum = stable_hash(inventory)
    state = _bundle_ledger_state(
        budget,
        bundle_id=bundle_id,
        estimate=estimate,
        inventory_checksum=checksum,
    )
    if state == "COMPLETED":
        return
    if state != "RESERVED":
        raise MGCPowerExtensionError("download completion lacks durable reservation")
    actual_before, outstanding_before = _committed_spend(budget)
    effective_cap = min(budget.hard_cap_usd, budget.safety_ceiling_usd)
    if effective_cap - (actual_before + outstanding_before) < MINIMUM_REMAINING_RESERVE_USD:
        raise MGCPowerExtensionError("download completion would violate reserve")
    actual_after = actual_before + estimate
    outstanding_after = max(outstanding_before - estimate, 0.0)
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=bundle_id,
            timestamp_utc=utc_now(),
            dataset=DATASET,
            schema="ohlcv-1m+definition",
            symbols=list(SYMBOLS),
            stype_in=STYPE_IN,
            start=START,
            end=END,
            # The exact estimate was durably journaled before network I/O.
            # Keeping it zero on completion avoids double-counting in the
            # generic append-only budget summary.
            estimated_cost_usd=0.0,
            actual_cost_usd=estimate,
            cumulative_estimated_spend_usd=actual_after + outstanding_after,
            cumulative_actual_spend_usd=actual_after,
            cache_hit=False,
            research_purpose=PURPOSE,
            candidate_tier="TIER_H_FROZEN_POWER_EXTENSION_INPUT",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=str(inventory[0]["path"]),
            checksum=checksum,
            download_status="DOWNLOADED",
        ),
    )


def _record_access_roles_once(path: Path, *, bundle_id: str, card_hash: str) -> None:
    rows = _jsonl(path)
    for role in TEMPORAL_ROLES:
        marker = _access_marker(bundle_id, role["role"])
        matching = [row for row in rows if marker in set(row.get("candidate_ids") or ())]
        if len(matching) > 1:
            raise MGCPowerExtensionError("duplicate access records")
        expected_role = (
            DataRole.DEVELOPMENT
            if role["role"] == "DISCOVERY"
            else DataRole.BLIND_VALIDATION
        )
        if matching:
            row = matching[0]
            if (
                row.get("period_accessed") != f"{role['start']}:{role['end']}"
                or row.get("data_role") != expected_role.value
                or row.get("freeze_manifest_hash") != card_hash
            ):
                raise MGCPowerExtensionError("existing access record drift")
            continue
        enforce_data_access(
            period=f"{role['start']}:{role['end']}",
            role=expected_role,
            requesting_module="scripts.acquire_mgc_rates_target_vol_power_extension",
            candidate_ids=[CANDIDATE_ID, bundle_id, marker],
            reason=f"{PURPOSE}; frozen role={role['role']}",
            freeze_manifest_hash=card_hash,
            ledger_path=str(path),
        )
        rows = _jsonl(path)


def _committed_spend(budget: DatabentoBudgetConfig) -> tuple[float, float]:
    rows = read_ledger(budget.ledger_path)
    actual = math.fsum(float(row.get("actual_cost_usd") or 0.0) for row in rows)
    completed = {
        str(row.get("request_id"))
        for row in rows
        if str(row.get("download_status", "")).startswith("DOWNLOADED")
    }
    outstanding_by_id: dict[str, float] = {}
    for row in rows:
        request_id = str(row.get("request_id") or "")
        if row.get("download_status") == "ESTIMATED_ONLY" and request_id not in completed:
            outstanding_by_id[request_id] = max(
                outstanding_by_id.get(request_id, 0.0),
                float(row.get("estimated_cost_usd") or 0.0),
            )
    return actual, math.fsum(outstanding_by_id.values())


def _bound_budget(
    project: Path, budget: DatabentoBudgetConfig | None
) -> DatabentoBudgetConfig:
    source = budget or DatabentoBudgetConfig()
    if (
        not math.isclose(source.hard_cap_usd, HARD_CAP_USD, rel_tol=0.0, abs_tol=1e-12)
        or source.safety_ceiling_usd > HARD_CAP_USD
        or not math.isclose(
            DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD,
            HARD_CAP_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
    ):
        raise MGCPowerExtensionError("budget authority drift")
    ledger = Path(source.ledger_path)
    summary = Path(source.summary_path)
    return DatabentoBudgetConfig(
        budget_start=source.budget_start,
        hard_cap_usd=source.hard_cap_usd,
        safety_ceiling_usd=source.safety_ceiling_usd,
        ledger_path=str(ledger if ledger.is_absolute() else project / ledger),
        summary_path=str(summary if summary.is_absolute() else project / summary),
    )


def _load_existing_receipt(
    path: Path,
    *,
    bundle_id: str,
    card_hash: str,
    budget: DatabentoBudgetConfig,
    paths: Mapping[str, Path],
) -> dict[str, Any]:
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise MGCPowerExtensionError("existing receipt is invalid") from exc
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if (
        stable_hash(core) != claimed
        or receipt.get("schema") != RECEIPT_SCHEMA
        or receipt.get("bundle_id") != bundle_id
        or receipt.get("card_hash") != card_hash
        or receipt.get("candidate_ids") != [CANDIDATE_ID]
        or receipt.get("download_status") != "DOWNLOADED"
        or receipt.get("outcomes_read") != 0
        or receipt.get("q4_access_count_delta") != 0
        or receipt.get("broker_connections") != 0
        or receipt.get("orders") != 0
        or receipt.get("requests")
        != {schema: _request(schema) for schema in EXPECTED}
        or receipt.get("temporal_roles") != [dict(row) for row in TEMPORAL_ROLES]
        or receipt.get("power_thresholds") != POWER_THRESHOLDS
    ):
        raise MGCPowerExtensionError("existing receipt drift")
    for schema in EXPECTED:
        _verify_estimate(
            schema, dict(receipt.get("official_estimates", {}).get(schema) or {})
        )
    if not math.isclose(
        float(receipt.get("official_total_cost_usd", float("nan"))),
        EXPECTED_TOTAL_COST_USD,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise MGCPowerExtensionError("existing receipt combined cost drift")
    files = list(receipt.get("files") or ())
    expected_files = {
        "RAW_OHLCV_DBN": paths["raw_ohlcv"],
        "RAW_DEFINITION_DBN": paths["raw_definitions"],
        "MGC_VOLUME_FRONT_ROLL_MAP": paths["mgc_roll_map"],
        "ZN_TN_DELIVERY_SYNC_RECEIPT": paths["treasury_sync"],
    }
    if len(files) != len(expected_files) or {
        str(row.get("kind")) for row in files
    } != set(expected_files):
        raise MGCPowerExtensionError("sealed acquisition file inventory drift")
    for row in files:
        artifact = Path(str(row.get("path") or ""))
        if (
            artifact != expected_files[str(row.get("kind"))]
            or not artifact.is_file()
            or artifact.stat().st_size != int(row.get("size_bytes") or -1)
            or sha256_file(artifact) != str(row.get("sha256") or "")
        ):
            raise MGCPowerExtensionError("sealed acquisition artifact drift")
    try:
        mgc_map = json.loads(paths["mgc_roll_map"].read_text(encoding="utf-8"))
        treasury = json.loads(paths["treasury_sync"].read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise MGCPowerExtensionError("sealed roll artifact is invalid") from exc
    rebuilt_mgc_map, rebuilt_treasury = _build_roll_artifacts_from_dbn(
        paths["raw_definitions"], paths["raw_ohlcv"]
    )
    if rebuilt_mgc_map != mgc_map or rebuilt_treasury != treasury:
        raise MGCPowerExtensionError(
            "sealed roll artifacts do not reproduce under the causal resolver"
        )
    mgc_core = dict(mgc_map)
    mgc_hash = str(mgc_core.pop("roll_map_hash", ""))
    treasury_core = dict(treasury)
    treasury_hash = str(treasury_core.pop("receipt_hash", ""))
    if (
        stable_hash(mgc_core) != mgc_hash
        or stable_hash(treasury_core) != treasury_hash
        or receipt.get("mgc_roll_map_hash") != mgc_hash
        or receipt.get("treasury_sync_hash") != treasury_hash
        or mgc_map.get("map_type") != MGC_MAP_TYPE
        or mgc_map.get("symbols") != ["MGC"]
        or treasury.get("schema") != TREASURY_RECEIPT_SCHEMA
        or treasury.get("forward_fill_rows") != 0
        or treasury.get("q4_access_count_delta") != 0
    ):
        raise MGCPowerExtensionError("sealed roll-artifact hash or policy drift")
    markers = {_access_marker(bundle_id, row["role"]) for row in TEMPORAL_ROLES}
    access = [
        row
        for row in _jsonl(paths["access_ledger"])
        if markers.intersection(set(row.get("candidate_ids") or ()))
    ]
    if (
        _bundle_ledger_state(
            budget,
            bundle_id=bundle_id,
            estimate=EXPECTED_TOTAL_COST_USD,
            inventory_checksum=stable_hash(files),
        )
        != "COMPLETED"
        or len(access) != len(TEMPORAL_ROLES)
    ):
        raise MGCPowerExtensionError("sealed receipt ledger cardinality drift")
    for role in TEMPORAL_ROLES:
        marker = _access_marker(bundle_id, role["role"])
        matching = [row for row in access if marker in set(row.get("candidate_ids") or ())]
        expected_role = (
            DataRole.DEVELOPMENT
            if role["role"] == "DISCOVERY"
            else DataRole.BLIND_VALIDATION
        )
        if len(matching) != 1 or (
            matching[0].get("period_accessed") != f"{role['start']}:{role['end']}"
            or matching[0].get("data_role") != expected_role.value
            or matching[0].get("freeze_manifest_hash") != card_hash
        ):
            raise MGCPowerExtensionError("sealed data-access ledger drift")
    return receipt


def _paths(root: Path, bundle_id: str, receipt_path: str | Path) -> dict[str, Path]:
    base = root / "data/cache/databento/mgc_rates_target_vol_power_extension" / bundle_id
    receipt = Path(receipt_path)
    if not receipt.is_absolute():
        receipt = root / receipt
    return {
        "raw_ohlcv": base / "raw_ohlcv.dbn.zst",
        "raw_definitions": base / "raw_definitions.dbn.zst",
        "mgc_roll_map": base / "mgc_volume_front_roll_map.json",
        "treasury_sync": base / "zn_tn_delivery_sync_receipt.json",
        "receipt": receipt.resolve(),
        "lock": root / "reports/data_access/mgc_rates_target_vol_power_extension.lock",
        "access_ledger": root / ACCESS_LEDGER,
    }


def _file_receipt(kind: str, path: Path) -> dict[str, Any]:
    return {
        "kind": kind,
        "path": str(path),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _persist_json_once(path: Path, payload: Mapping[str, Any]) -> None:
    content = json.dumps(dict(payload), indent=2, sort_keys=True, default=str) + "\n"
    if path.is_file():
        if path.read_text(encoding="utf-8") != content:
            raise MGCPowerExtensionError(f"refusing divergent artifact rewrite: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(content, encoding="utf-8")
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _access_marker(bundle_id: str, role: str) -> str:
    return f"{bundle_id}:{role}"


@contextmanager
def _optional_lock(path: Path, *, enabled: bool) -> Iterator[None]:
    if not enabled:
        yield
        return
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise MGCPowerExtensionError("acquisition lock is already held") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Estimate or acquire the frozen +25% MGC power extension"
    )
    parser.add_argument("--root", default=".")
    parser.add_argument("--card", default=CARD_PATH)
    parser.add_argument("--receipt", default=DEFAULT_RECEIPT)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="perform the two frozen downloads; default is metadata-only",
    )
    args = parser.parse_args()
    key = load_api_key()
    if not key:
        raise MGCPowerExtensionError(
            "DATABENTO_API_KEY is required for official metadata verification"
        )
    client = _import_databento().Historical(key)
    result = estimate_or_acquire(
        root=args.root,
        client=client,
        execute=bool(args.execute),
        card_path=args.card,
        receipt_path=args.receipt,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
