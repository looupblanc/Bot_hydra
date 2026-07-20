from __future__ import annotations

"""Bounded Databento acquisition for the options-IV teacher pilot.

The contract is intentionally small and fixed in code.  A dry run performs
official metadata estimates only.  An explicit execution reserves spend before
network I/O, writes DBN files atomically, and seals their hashes in one receipt.
The option observations are research-plane teacher inputs; they are not a
deployment dependency.
"""

import fcntl
import json
import math
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping

from hydra.data.budget import (
    AUTO_UNDER_HARD_CAP,
    DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD,
    DatabentoBudgetConfig,
    DatabentoSpendRecord,
    append_spend_record,
    read_ledger,
    request_id_for,
    sha256_file,
    utc_now,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


DATASET = "GLBX.MDP3"
SYMBOLS = ("ES.OPT", "NQ.OPT")
SCHEMAS = ("statistics", "definition")
STYPE_IN = "parent"
STYPE_OUT = "raw_symbol"
START = "2024-09-03"
END = "2024-10-01"
Q4_START = "2024-10-01"
MAX_INCREMENTAL_USD = 0.70
CAMPAIGN_ID = "OPTIONS_IV_TEACHER_STUDENT_TRIPWIRE_V1"
MANIFEST_PATH = "config/research/options_iv_teacher_student_tripwire_v1.json"
EXPECTED_MANIFEST_HASH = (
    "723019b5baaca70906a9ec90f87476f200bfc7a406d2be0f8b883f016cca58f4"
)
EXPECTED_ESTIMATES = {
    "statistics": {
        "record_count": 48_718,
        "billable_size_bytes": 3_117_952,
        "estimated_cost_usd": 0.435541570187,
    },
    "definition": {
        "record_count": 143_990,
        "billable_size_bytes": 51_836_400,
        "estimated_cost_usd": 0.082069896162,
    },
}
EXPECTED_TOTAL_COST_USD = 0.517611466349
EXPECTED_CUMULATIVE_ACTUAL_BEFORE_USD = 191.65788407326198
EXPECTED_REMAINING_BEFORE_USD = 9.062835849819
EXPECTED_PROJECTED_REMAINING_USD = 8.54522438347
CANDIDATE_TIER = "TIER_H_TEACHER_ONLY_RESEARCH_INPUT"
PURPOSE = (
    "bounded pre-Q4 options-IV teacher pilot for a futures-only deployable "
    "student; no option-feed deployment, broker, orders, XFA, or promotion"
)
RECEIPT_SCHEMA = "hydra_options_iv_teacher_acquisition_receipt_v1"
DEFAULT_CACHE_ROOT = "data/cache/databento/options_iv_teacher_pilot"
DEFAULT_RECEIPT = (
    "reports/data_access/options_iv_teacher_pilot_acquisition_receipt.json"
)
DEFAULT_ACCESS_LEDGER = "reports/data_access/data_access_ledger.jsonl"
# Reuse the mission-wide Databento acquisition mutex so the shared spend
# journal cannot race another governed downloader.
DEFAULT_LOCK = "reports/data_access/fresh_confirmation_0035_acquisition.lock"
TEMPORAL_ROLES = (
    {
        "role": "DISCOVERY",
        "sessions": [
            "2024-09-03", "2024-09-04", "2024-09-05", "2024-09-06",
            "2024-09-09", "2024-09-10", "2024-09-11", "2024-09-12",
            "2024-09-13", "2024-09-16", "2024-09-17", "2024-09-18",
        ],
        "candidate_modification_allowed": True,
    },
    {
        "role": "VALIDATION",
        "sessions": ["2024-09-19", "2024-09-20", "2024-09-23", "2024-09-24"],
        "candidate_modification_allowed": False,
    },
    {
        "role": "FINAL_DEVELOPMENT",
        "sessions": ["2024-09-25", "2024-09-26", "2024-09-27", "2024-09-30"],
        "candidate_modification_allowed": False,
    },
)
ROLE_PERIODS = {
    "DISCOVERY": ("2024-09-03", "2024-09-19"),
    "VALIDATION": ("2024-09-19", "2024-09-25"),
    "FINAL_DEVELOPMENT": ("2024-09-25", "2024-10-01"),
}


class OptionsTeacherAcquisitionError(RuntimeError):
    """The fixed teacher-input acquisition cannot proceed safely."""


def load_and_validate_manifest(
    root: str | Path,
    path: str | Path = MANIFEST_PATH,
) -> dict[str, Any]:
    """Load the one authorised manifest and reject any self-consistent mutation."""

    project = Path(root).resolve()
    manifest_path = Path(path)
    if not manifest_path.is_absolute():
        manifest_path = project / manifest_path
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OptionsTeacherAcquisitionError(
            "frozen options teacher manifest is unavailable or invalid"
        ) from exc
    core = dict(manifest)
    claimed = str(core.pop("manifest_hash", ""))
    if (
        claimed != EXPECTED_MANIFEST_HASH
        or stable_hash(core) != EXPECTED_MANIFEST_HASH
    ):
        raise OptionsTeacherAcquisitionError("frozen options teacher manifest hash drift")

    data = dict(manifest.get("data_contract") or {})
    governance = dict(manifest.get("governance") or {})
    budget = dict(manifest.get("budget") or {})
    statistics = dict(data.get("statistics_contract") or {})
    definitions = dict(data.get("definition_contract") or {})
    underlying = dict(data.get("underlying_cache") or {})
    teacher = dict(manifest.get("teacher_feature_contract") or {})
    if (
        manifest.get("schema")
        != "hydra_options_iv_teacher_student_tripwire_manifest_v1"
        or manifest.get("branch_id") != CAMPAIGN_ID
        or data.get("dataset") != DATASET
        or tuple(data.get("symbols") or ()) != SYMBOLS
        or data.get("stype_in") != STYPE_IN
        or data.get("stype_out") != STYPE_OUT
        or tuple(data.get("schemas") or ()) != SCHEMAS
        or data.get("start") != START
        or data.get("end_exclusive") != END
        or data.get("q4_2024_access") is not False
        or data.get("official_estimates") != EXPECTED_ESTIMATES
        or not math.isclose(
            float(data.get("official_total_cost_usd", float("nan"))),
            EXPECTED_TOTAL_COST_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or statistics.get("required_stat_type") != 14
        or statistics.get("meaning") != "EXCHANGE_PUBLISHED_IMPLIED_VOLATILITY"
        or statistics.get("availability_field") != "ts_recv"
        or statistics.get("non_iv_statistics_allowed_in_decision") is not False
        or definitions.get("outright_call_put_only") is not True
        or definitions.get("user_defined_instruments_allowed") is not False
        or definitions.get("spreads_allowed") is not False
        or definitions.get("definition_must_be_available_before_observation") is not True
        or underlying.get("purchase_required") is not False
        or manifest.get("chronological_roles")
        != [dict(row) for row in TEMPORAL_ROLES]
        or teacher.get("status") != "TEACHER_ONLY_UNDEPLOYABLE"
        or governance.get("maximum_cpu_workers") != 1
        or governance.get("numeric_threads_per_worker") != 1
        or governance.get("single_authoritative_writer") is not True
        or governance.get("no_broker") is not True
        or governance.get("no_orders") is not True
        or governance.get("no_live_trading") is not True
        or governance.get("no_q4_access") is not True
        or governance.get("no_xfa_before_clean_combine_survivors") is not True
        or governance.get("frozen_manifest_before_purchase") is not True
        or not math.isclose(
            float(budget.get("authoritative_cumulative_cap_usd", float("nan"))),
            DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or not math.isclose(
            float(budget.get("cumulative_actual_before_usd", float("nan"))),
            EXPECTED_CUMULATIVE_ACTUAL_BEFORE_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or not math.isclose(
            float(budget.get("remaining_before_usd", float("nan"))),
            EXPECTED_REMAINING_BEFORE_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or not math.isclose(
            float(budget.get("official_incremental_estimate_usd", float("nan"))),
            EXPECTED_TOTAL_COST_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or not math.isclose(
            float(budget.get("projected_remaining_after_usd", float("nan"))),
            EXPECTED_PROJECTED_REMAINING_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or not math.isclose(
            float(budget.get("maximum_branch_purchase_usd", float("nan"))),
            MAX_INCREMENTAL_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or budget.get("purchase_status")
        != "NOT_PURCHASED_AWAITING_FROZEN_MANIFEST"
    ):
        raise OptionsTeacherAcquisitionError("frozen options teacher manifest contract drift")
    return manifest


def frozen_requests() -> dict[str, dict[str, Any]]:
    """Return fresh copies of the only permitted Databento requests."""

    requests = {
        schema: {
            "dataset": DATASET,
            "schema": schema,
            "symbols": list(SYMBOLS),
            "stype_in": STYPE_IN,
            "start": START,
            "end": END,
        }
        for schema in SCHEMAS
    }
    validate_requests(requests)
    return requests


def validate_requests(requests: Mapping[str, Mapping[str, Any]]) -> None:
    if tuple(requests) != SCHEMAS:
        raise OptionsTeacherAcquisitionError("options request schema drift")
    if START >= END or END > Q4_START:
        raise OptionsTeacherAcquisitionError("options request enters protected Q4")
    for schema in SCHEMAS:
        request = dict(requests[schema])
        if request != {
            "dataset": DATASET,
            "schema": schema,
            "symbols": list(SYMBOLS),
            "stype_in": STYPE_IN,
            "start": START,
            "end": END,
        }:
            raise OptionsTeacherAcquisitionError("options request contract drift")


def estimate_or_acquire(
    *,
    root: str | Path,
    client: Any,
    execute: bool,
    manifest_path: str | Path = MANIFEST_PATH,
    budget: DatabentoBudgetConfig | None = None,
    cache_root: str | Path = DEFAULT_CACHE_ROOT,
    receipt_path: str | Path = DEFAULT_RECEIPT,
    access_ledger_path: str | Path = DEFAULT_ACCESS_LEDGER,
    lock_path: str | Path = DEFAULT_LOCK,
) -> dict[str, Any]:
    """Estimate or explicitly acquire the fixed two-schema option bundle."""

    project = Path(root).resolve()
    manifest = load_and_validate_manifest(project, manifest_path)
    manifest_hash = str(manifest["manifest_hash"])
    requests = frozen_requests()
    bundle_id = request_id_for(
        {
            "campaign_id": CAMPAIGN_ID,
            "manifest_hash": manifest_hash,
            "requests": requests,
            "chronological_roles": TEMPORAL_ROLES,
            "purpose": PURPOSE,
        }
    )
    paths = _paths(
        project,
        bundle_id=bundle_id,
        cache_root=cache_root,
        receipt_path=receipt_path,
        access_ledger_path=access_ledger_path,
        lock_path=lock_path,
    )
    cfg = _bound_budget(project, budget)

    with _optional_lock(paths["lock"], enabled=execute):
        if execute:
            existing = _load_existing_receipt(
                paths["receipt"],
                bundle_id=bundle_id,
                manifest_hash=manifest_hash,
                paths=paths,
                budget=cfg,
            )
            if existing is not None:
                return existing

        estimates = _official_estimates(client, requests)
        total = math.fsum(float(row["estimated_cost_usd"]) for row in estimates.values())
        if not math.isclose(
            total,
            EXPECTED_TOTAL_COST_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise OptionsTeacherAcquisitionError("official aggregate estimate drift")
        if total > MAX_INCREMENTAL_USD + 1e-12:
            raise OptionsTeacherAcquisitionError(
                f"official estimate ${total:.12f} exceeds branch cap ${MAX_INCREMENTAL_USD:.2f}"
            )
        actual, outstanding = _committed_spend(cfg)
        state = _bundle_ledger_state(cfg, bundle_id=bundle_id, estimate=total)
        incremental = total if state == "NONE" else 0.0
        effective_cap = min(cfg.hard_cap_usd, cfg.safety_ceiling_usd)
        projected_commitment = actual + outstanding + incremental
        if projected_commitment > effective_cap + 1e-12:
            raise OptionsTeacherAcquisitionError("official estimate exceeds mission budget authority")

        plan_core = {
            "schema": "hydra_options_iv_teacher_acquisition_plan_v1",
            "campaign_id": CAMPAIGN_ID,
            "bundle_id": bundle_id,
            "manifest_hash": manifest_hash,
            "requests": requests,
            "chronological_roles": [dict(row) for row in TEMPORAL_ROLES],
            "official_estimates": estimates,
            "aggregate_estimated_cost_usd": total,
            "maximum_incremental_cost_usd": MAX_INCREMENTAL_USD,
            "cumulative_actual_before_usd": actual,
            "outstanding_commitments_before_usd": outstanding,
            "projected_committed_spend_usd": projected_commitment,
            "mission_cap_usd": effective_cap,
            "q4_access_count_delta": 0,
            "teacher_only": True,
            "deployment_requires_futures_only_student": True,
            "broker_connections": 0,
            "orders": 0,
            "execute": bool(execute),
        }
        plan = {**plan_core, "plan_hash": stable_hash(plan_core)}
        if not execute:
            return {
                **plan,
                "download_status": "DRY_RUN_ONLY",
                "network_data_request_made": False,
                "files_created": 0,
            }

        if state == "NONE" and any(paths[schema].exists() for schema in SCHEMAS):
            raise OptionsTeacherAcquisitionError("unledgered immutable DBN file exists")
        _reserve_spend_once(cfg, bundle_id=bundle_id, estimate=total)

        network = False
        for schema in SCHEMAS:
            network |= _download_once(
                client,
                requests[schema],
                paths[schema],
                stype_out=STYPE_OUT,
            )
        files = [
            _file_receipt("RAW_DBN_STATISTICS", paths["statistics"]),
            _file_receipt("RAW_DBN_DEFINITION", paths["definition"]),
        ]
        inventory_hash = stable_hash(files)
        _complete_spend_once(
            cfg,
            bundle_id=bundle_id,
            estimate=total,
            inventory_hash=inventory_hash,
            first_path=paths["statistics"],
        )
        _record_access_roles_once(
            paths["access_ledger"],
            bundle_id=bundle_id,
            manifest_hash=manifest_hash,
        )
        receipt_core = {
            "schema": RECEIPT_SCHEMA,
            "campaign_id": CAMPAIGN_ID,
            "bundle_id": bundle_id,
            "created_at_utc": utc_now(),
            "manifest_hash": manifest_hash,
            "requests": requests,
            "chronological_roles": [dict(row) for row in TEMPORAL_ROLES],
            "official_estimates": estimates,
            "actual_incremental_spend_usd": total,
            "inventory_hash": inventory_hash,
            "files": files,
            "download_status": "DOWNLOADED",
            "network_data_request_made": network,
            "q4_access_count_delta": 0,
            "teacher_only": True,
            "deployment_requires_futures_only_student": True,
            "broker_connections": 0,
            "orders": 0,
        }
        receipt = {**receipt_core, "receipt_hash": stable_hash(receipt_core)}
        _persist_json_once(paths["receipt"], receipt)
        return receipt


def _official_estimates(
    client: Any, requests: Mapping[str, Mapping[str, Any]]
) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for schema in SCHEMAS:
        request = dict(requests[schema])
        cost = float(client.metadata.get_cost(**request))
        records = int(client.metadata.get_record_count(**request))
        size = int(client.metadata.get_billable_size(**request))
        if not math.isfinite(cost) or cost < 0.0 or records < 0 or size < 0:
            raise OptionsTeacherAcquisitionError("invalid official Databento estimate")
        expected = EXPECTED_ESTIMATES[schema]
        if (
            records != int(expected["record_count"])
            or size != int(expected["billable_size_bytes"])
            or not math.isclose(
                cost,
                float(expected["estimated_cost_usd"]),
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        ):
            raise OptionsTeacherAcquisitionError(
                f"official {schema} metadata estimate drift"
            )
        rows[schema] = {
            "estimated_cost_usd": cost,
            "estimated_records": records,
            "estimated_bytes": size,
        }
    return rows


def _bound_budget(
    project: Path, budget: DatabentoBudgetConfig | None
) -> DatabentoBudgetConfig:
    source = budget or DatabentoBudgetConfig()
    if (
        source.hard_cap_usd > DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD + 1e-12
        or source.safety_ceiling_usd > DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD + 1e-12
        or source.safety_ceiling_usd > source.hard_cap_usd + 1e-12
    ):
        raise OptionsTeacherAcquisitionError("budget configuration exceeds mission authority")
    ledger = Path(source.ledger_path)
    summary = Path(source.summary_path)
    return DatabentoBudgetConfig(
        budget_start=source.budget_start,
        hard_cap_usd=source.hard_cap_usd,
        safety_ceiling_usd=source.safety_ceiling_usd,
        ledger_path=str(ledger if ledger.is_absolute() else project / ledger),
        summary_path=str(summary if summary.is_absolute() else project / summary),
    )


def _committed_spend(budget: DatabentoBudgetConfig) -> tuple[float, float]:
    rows = read_ledger(budget.ledger_path)
    actual = math.fsum(float(row.get("actual_cost_usd") or 0.0) for row in rows)
    completed = {
        str(row.get("request_id") or "")
        for row in rows
        if row.get("download_status") == "DOWNLOADED"
    }
    outstanding: dict[str, float] = {}
    for row in rows:
        request_id = str(row.get("request_id") or "")
        if (
            request_id
            and row.get("download_status") == "ESTIMATED_ONLY"
            and request_id not in completed
        ):
            outstanding[request_id] = max(
                outstanding.get(request_id, 0.0),
                float(row.get("estimated_cost_usd") or 0.0),
            )
    return actual, math.fsum(outstanding.values())


def _bundle_ledger_state(
    budget: DatabentoBudgetConfig,
    *,
    bundle_id: str,
    estimate: float,
    inventory_hash: str | None = None,
) -> str:
    rows = [
        row for row in read_ledger(budget.ledger_path)
        if row.get("request_id") == bundle_id
    ]
    if not rows:
        return "NONE"
    if len(rows) not in {1, 2}:
        raise OptionsTeacherAcquisitionError("spend journal cardinality drift")
    reservation = rows[0]
    if (
        reservation.get("download_status") != "ESTIMATED_ONLY"
        or reservation.get("dataset") != DATASET
        or reservation.get("symbols") != list(SYMBOLS)
        or not math.isclose(
            float(reservation.get("estimated_cost_usd") or 0.0),
            estimate,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or reservation.get("actual_cost_usd") is not None
    ):
        raise OptionsTeacherAcquisitionError("spend reservation drift")
    if len(rows) == 1:
        return "RESERVED"
    completion = rows[1]
    if (
        completion.get("download_status") != "DOWNLOADED"
        or completion.get("dataset") != DATASET
        or completion.get("symbols") != list(SYMBOLS)
        or not math.isclose(
            float(completion.get("actual_cost_usd") or 0.0),
            estimate,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or not completion.get("checksum")
        or (inventory_hash is not None and completion.get("checksum") != inventory_hash)
    ):
        raise OptionsTeacherAcquisitionError("spend completion drift")
    return "COMPLETED"


def _reserve_spend_once(
    budget: DatabentoBudgetConfig, *, bundle_id: str, estimate: float
) -> None:
    if _bundle_ledger_state(budget, bundle_id=bundle_id, estimate=estimate) != "NONE":
        return
    actual, outstanding = _committed_spend(budget)
    projected = actual + outstanding + estimate
    if projected > min(budget.hard_cap_usd, budget.safety_ceiling_usd) + 1e-12:
        raise OptionsTeacherAcquisitionError("spend reservation exceeds mission authority")
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=bundle_id,
            timestamp_utc=utc_now(),
            dataset=DATASET,
            schema="statistics+definition",
            symbols=list(SYMBOLS),
            stype_in=STYPE_IN,
            start=START,
            end=END,
            estimated_cost_usd=estimate,
            actual_cost_usd=None,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual,
            cache_hit=False,
            research_purpose=PURPOSE,
            candidate_tier=CANDIDATE_TIER,
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
    inventory_hash: str,
    first_path: Path,
) -> None:
    state = _bundle_ledger_state(
        budget,
        bundle_id=bundle_id,
        estimate=estimate,
        inventory_hash=inventory_hash,
    )
    if state == "COMPLETED":
        return
    if state != "RESERVED":
        raise OptionsTeacherAcquisitionError("completion lacks a spend reservation")
    actual, outstanding = _committed_spend(budget)
    if actual + outstanding > min(budget.hard_cap_usd, budget.safety_ceiling_usd) + 1e-12:
        raise OptionsTeacherAcquisitionError("completion exceeds committed authority")
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=bundle_id,
            timestamp_utc=utc_now(),
            dataset=DATASET,
            schema="statistics+definition",
            symbols=list(SYMBOLS),
            stype_in=STYPE_IN,
            start=START,
            end=END,
            estimated_cost_usd=0.0,
            actual_cost_usd=estimate,
            cumulative_estimated_spend_usd=actual + outstanding,
            cumulative_actual_spend_usd=actual + estimate,
            cache_hit=False,
            research_purpose=PURPOSE,
            candidate_tier=CANDIDATE_TIER,
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=str(first_path),
            checksum=inventory_hash,
            download_status="DOWNLOADED",
        ),
    )


def _download_once(
    client: Any,
    request: Mapping[str, Any],
    path: Path,
    *,
    stype_out: str,
) -> bool:
    if path.is_file():
        if path.stat().st_size <= 0:
            raise OptionsTeacherAcquisitionError("empty immutable DBN file")
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        client.timeseries.get_range(
            **dict(request), stype_out=stype_out, path=str(temporary)
        )
        if not temporary.is_file() or temporary.stat().st_size <= 0:
            raise OptionsTeacherAcquisitionError("Databento returned an empty DBN file")
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return True


def _record_access_roles_once(
    path: Path, *, bundle_id: str, manifest_hash: str
) -> None:
    for row in TEMPORAL_ROLES:
        marker = f"{bundle_id}:{row['role']}"
        period_start, period_end = ROLE_PERIODS[str(row["role"])]
        matching = [
            value for value in _jsonl(path)
            if marker in set(value.get("candidate_ids") or ())
        ]
        if len(matching) > 1:
            raise OptionsTeacherAcquisitionError("duplicate data-access role")
        role = (
            DataRole.DEVELOPMENT
            if row["role"] == "DISCOVERY"
            else DataRole.BLIND_VALIDATION
        )
        if matching:
            existing = matching[0]
            if (
                existing.get("period_accessed") != f"{period_start}:{period_end}"
                or existing.get("data_role") != role.value
                or existing.get("freeze_manifest_hash") != manifest_hash
            ):
                raise OptionsTeacherAcquisitionError("existing data-access role drift")
            continue
        enforce_data_access(
            period=f"{period_start}:{period_end}",
            role=role,
            requesting_module="hydra.data.options_iv_teacher_acquisition",
            candidate_ids=[CAMPAIGN_ID, bundle_id, marker],
            reason=f"{PURPOSE}; frozen role={row['role']}",
            freeze_manifest_hash=manifest_hash,
            ledger_path=str(path),
        )


def _load_existing_receipt(
    path: Path,
    *,
    bundle_id: str,
    manifest_hash: str,
    paths: Mapping[str, Path],
    budget: DatabentoBudgetConfig,
) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise OptionsTeacherAcquisitionError("existing receipt is invalid") from exc
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if (
        not claimed
        or stable_hash(core) != claimed
        or receipt.get("schema") != RECEIPT_SCHEMA
        or receipt.get("bundle_id") != bundle_id
        or receipt.get("manifest_hash") != manifest_hash
        or receipt.get("requests") != frozen_requests()
        or receipt.get("chronological_roles") != [dict(row) for row in TEMPORAL_ROLES]
        or receipt.get("download_status") != "DOWNLOADED"
        or receipt.get("q4_access_count_delta") != 0
    ):
        raise OptionsTeacherAcquisitionError("existing receipt semantic drift")
    expected = {
        "RAW_DBN_STATISTICS": paths["statistics"],
        "RAW_DBN_DEFINITION": paths["definition"],
    }
    files = list(receipt.get("files") or ())
    if (
        len(files) != 2
        or {str(row.get("kind")) for row in files} != set(expected)
        or receipt.get("inventory_hash") != stable_hash(files)
    ):
        raise OptionsTeacherAcquisitionError("existing receipt inventory drift")
    for row in files:
        artifact = Path(str(row.get("path") or "")).resolve()
        target = expected[str(row["kind"])].resolve()
        if (
            artifact != target
            or not artifact.is_file()
            or artifact.stat().st_size != int(row.get("size_bytes") or -1)
            or sha256_file(artifact) != str(row.get("sha256") or "")
        ):
            raise OptionsTeacherAcquisitionError("immutable DBN artifact drift")
    estimate = float(receipt.get("actual_incremental_spend_usd") or -1.0)
    if _bundle_ledger_state(
        budget,
        bundle_id=bundle_id,
        estimate=estimate,
        inventory_hash=str(receipt["inventory_hash"]),
    ) != "COMPLETED":
        raise OptionsTeacherAcquisitionError("sealed spend journal is incomplete")
    for role in TEMPORAL_ROLES:
        marker = f"{bundle_id}:{role['role']}"
        if sum(
            marker in set(row.get("candidate_ids") or ())
            for row in _jsonl(paths["access_ledger"])
        ) != 1:
            raise OptionsTeacherAcquisitionError("sealed access ledger is incomplete")
    return receipt


def _paths(
    project: Path,
    *,
    bundle_id: str,
    cache_root: str | Path,
    receipt_path: str | Path,
    access_ledger_path: str | Path,
    lock_path: str | Path,
) -> dict[str, Path]:
    cache = _resolve(project, cache_root) / bundle_id
    return {
        "statistics": cache / "raw_statistics.dbn.zst",
        "definition": cache / "raw_definition.dbn.zst",
        "receipt": _resolve(project, receipt_path),
        "access_ledger": _resolve(project, access_ledger_path),
        "lock": _resolve(project, lock_path),
    }


def _resolve(project: Path, value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (project / path).resolve()


def _file_receipt(kind: str, path: Path) -> dict[str, Any]:
    return {
        "kind": kind,
        "path": str(path),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _persist_json_once(path: Path, payload: Mapping[str, Any]) -> None:
    content = json.dumps(dict(payload), indent=2, sort_keys=True) + "\n"
    if path.is_file():
        if path.read_text(encoding="utf-8") != content:
            raise OptionsTeacherAcquisitionError("refusing to rewrite divergent receipt")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


@contextmanager
def _optional_lock(path: Path, *, enabled: bool) -> Iterator[None]:
    if not enabled:
        yield
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise OptionsTeacherAcquisitionError(
                "another options-teacher acquisition holds the lock"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def default_project_root() -> Path:
    return project_path().resolve()
