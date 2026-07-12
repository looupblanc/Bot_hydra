from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from hydra.data.budget import (
    AUTO_UNDER_HARD_CAP,
    DatabentoBudgetConfig,
    DatabentoBudgetError,
    DatabentoSpendRecord,
    append_spend_record,
    cumulative_spend,
    enforce_budget,
    request_id_for,
    sha256_file,
    utc_now,
)
from hydra.data.contract_mapping import (
    RollMap,
    build_explicit_roll_map,
    write_roll_map,
)
from hydra.utils.config import project_path


class CurrentContractMapError(RuntimeError):
    pass


@dataclass(frozen=True)
class CurrentContractMapReceipt:
    path: str
    sha256: str
    roll_map_hash: str
    definition_path: str
    definition_sha256: str
    definition_download_status: str
    incremental_spend_usd: float
    roots: tuple[str, ...]
    explicit_contracts: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "roll_map_hash": self.roll_map_hash,
            "definition_path": self.definition_path,
            "definition_sha256": self.definition_sha256,
            "definition_download_status": self.definition_download_status,
            "incremental_spend_usd": self.incremental_spend_usd,
            "roots": list(self.roots),
            "explicit_contracts": dict(self.explicit_contracts),
        }


def build_current_roll_map(
    *,
    roots: Sequence[str],
    start: str,
    end: str,
    continuous_mapping: Mapping[str, Sequence[Mapping[str, Any]]],
    raw_symbol_mapping: Mapping[str, str],
    definition_history: pd.DataFrame,
    dataset: str = "GLBX.MDP3",
    schema: str = "ohlcv-1m",
) -> RollMap:
    normalized = {
        str(key): [dict(value) for value in values]
        for key, values in continuous_mapping.items()
    }
    return build_explicit_roll_map(
        [str(value).upper() for value in roots],
        start=start,
        end=end,
        continuous_mapping=normalized,
        raw_symbol_mapping={str(key): str(value) for key, value in raw_symbol_mapping.items()},
        definition_records={},
        definition_history=definition_history,
        dataset=dataset,
        schema=schema,
    )


def ensure_current_contract_map(
    client: Any,
    *,
    roots: Sequence[str],
    start: str,
    end: str,
    budget: DatabentoBudgetConfig,
    cache_root: str | Path,
    minimum_reserve_usd: float = 30.0,
    dataset: str = "GLBX.MDP3",
    schema: str = "ohlcv-1m",
) -> CurrentContractMapReceipt:
    ordered_roots = tuple(sorted({str(value).upper() for value in roots}))
    if not ordered_roots:
        raise CurrentContractMapError("At least one forward root is required.")
    continuous_symbols = [f"{root}.c.0" for root in ordered_roots]
    continuous_raw = client.symbology.resolve(
        dataset=dataset,
        symbols=continuous_symbols,
        stype_in="continuous",
        stype_out="instrument_id",
        start_date=start,
        end_date=end,
    )
    continuous = {
        str(symbol): [dict(value) for value in values]
        for symbol, values in dict(continuous_raw.get("result") or {}).items()
    }
    missing = sorted(set(continuous_symbols) - set(continuous))
    if missing:
        raise CurrentContractMapError(
            f"Current continuous symbology is missing: {missing}"
        )
    instrument_ids = sorted(
        {
            str(value["s"])
            for values in continuous.values()
            for value in values
        },
        key=int,
    )
    raw_response = client.symbology.resolve(
        dataset=dataset,
        symbols=instrument_ids,
        stype_in="instrument_id",
        stype_out="raw_symbol",
        start_date=start,
        end_date=end,
    )
    raw_symbols = {
        str(instrument_id): str(values[0]["s"])
        for instrument_id, values in dict(raw_response.get("result") or {}).items()
        if values
    }
    if set(instrument_ids) - set(raw_symbols):
        raise CurrentContractMapError("Raw-symbol mapping is incomplete.")

    cache = Path(cache_root)
    cache.mkdir(parents=True, exist_ok=True)
    request_payload = {
        "dataset": dataset,
        "schema": "definition",
        "symbols": instrument_ids,
        "stype_in": "instrument_id",
        "start": start,
        "end": end,
        "purpose": "current explicit contracts for append-only shadow feed",
    }
    request_id = request_id_for(request_payload)
    definition_path = cache / f"definitions_{request_id}.dbn.zst"
    incremental_spend = 0.0
    if definition_path.is_file():
        download_status = "CACHE_HIT"
    else:
        estimate = float(
            client.metadata.get_cost(
                dataset=dataset,
                start=start,
                end=end,
                symbols=instrument_ids,
                schema="definition",
                stype_in="instrument_id",
            )
        )
        projected, actual = enforce_budget(budget, estimate)
        if budget.hard_cap_usd - (actual + estimate) < minimum_reserve_usd:
            raise DatabentoBudgetError(
                "Current definitions would consume the protected final-lockbox reserve."
            )
        append_spend_record(
            budget,
            DatabentoSpendRecord(
                request_id=request_id,
                timestamp_utc=utc_now(),
                dataset=dataset,
                schema="definition",
                symbols=instrument_ids,
                stype_in="instrument_id",
                start=start,
                end=end,
                estimated_cost_usd=estimate,
                actual_cost_usd=None,
                cumulative_estimated_spend_usd=projected,
                cumulative_actual_spend_usd=actual,
                cache_hit=False,
                research_purpose=(
                    "current explicit-contract definitions for append-only "
                    "zero-order shadow evidence"
                ),
                candidate_tier="SHADOW_FORWARD_DATA_INTEGRITY",
                approval_mode=AUTO_UNDER_HARD_CAP,
                resulting_file=None,
                checksum=None,
                download_status="ESTIMATED_ONLY",
            ),
        )
        client.timeseries.get_range(
            dataset=dataset,
            start=start,
            end=end,
            symbols=instrument_ids,
            schema="definition",
            stype_in="instrument_id",
            stype_out="instrument_id",
            path=definition_path,
        )
        incremental_spend = estimate
        append_spend_record(
            budget,
            DatabentoSpendRecord(
                request_id=request_id,
                timestamp_utc=utc_now(),
                dataset=dataset,
                schema="definition",
                symbols=instrument_ids,
                stype_in="instrument_id",
                start=start,
                end=end,
                estimated_cost_usd=0.0,
                actual_cost_usd=estimate,
                cumulative_estimated_spend_usd=projected,
                cumulative_actual_spend_usd=actual + estimate,
                cache_hit=False,
                research_purpose=(
                    "current explicit-contract definitions for append-only "
                    "zero-order shadow evidence"
                ),
                candidate_tier="SHADOW_FORWARD_DATA_INTEGRITY",
                approval_mode=AUTO_UNDER_HARD_CAP,
                resulting_file=str(definition_path.resolve()),
                checksum=sha256_file(definition_path),
                download_status="DOWNLOADED",
            ),
        )
        download_status = "DOWNLOADED"

    db = _import_databento()
    definition_store = db.DBNStore.from_file(definition_path)
    definition_history = definition_store.to_df(
        pretty_ts=True, map_symbols=False
    ).reset_index()
    roll_map = build_current_roll_map(
        roots=ordered_roots,
        start=start,
        end=end,
        continuous_mapping=continuous,
        raw_symbol_mapping=raw_symbols,
        definition_history=definition_history,
        dataset=dataset,
        schema=schema,
    )
    map_path, map_hash = write_roll_map(roll_map)
    contracts = {
        contract.root: contract.contract for contract in roll_map.contracts
    }
    return CurrentContractMapReceipt(
        path=str(map_path.resolve()),
        sha256=sha256_file(map_path),
        roll_map_hash=map_hash,
        definition_path=str(definition_path.resolve()),
        definition_sha256=sha256_file(definition_path),
        definition_download_status=download_status,
        incremental_spend_usd=incremental_spend,
        roots=ordered_roots,
        explicit_contracts=contracts,
    )


def current_actual_spend(budget: DatabentoBudgetConfig) -> float:
    return cumulative_spend(project_path(budget.ledger_path))[1]


def _import_databento() -> Any:
    try:
        import databento as db
    except ImportError as exc:
        raise CurrentContractMapError("Databento dependency is unavailable.") from exc
    return db


__all__ = [
    "CurrentContractMapError",
    "CurrentContractMapReceipt",
    "build_current_roll_map",
    "current_actual_spend",
    "ensure_current_contract_map",
]
