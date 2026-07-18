from __future__ import annotations

"""Governed, manifest-bound Databento acquisition for HYDRA campaign 0031.

The command is dry-run by default.  With ``--execute`` it re-estimates every
frozen request under one lock, enforces the aggregate USD 10 cap and USD 25
reserve, writes immutable DBN files through temp-to-rename, appends the shared
spend/access ledgers, and seals one bundle receipt.
"""

import argparse
import fcntl
import json
import os
import re
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.budget import (
    AUTO_UNDER_HARD_CAP,
    DatabentoBudgetConfig,
    DatabentoSpendRecord,
    append_spend_record,
    cumulative_spend,
    enforce_budget,
    read_ledger,
    request_id_for,
    sha256_file,
    utc_now,
)
from hydra.data.databento_loader import load_api_key
from hydra.economic_evolution.schema import stable_hash
from hydra.production.manifest import load_and_validate_production_manifest
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


CAMPAIGN_ID = "hydra_microstructure_order_flow_foundry_0031"
CAMPAIGN_MODE = "MICROSTRUCTURE_ORDER_FLOW_FOUNDRY"
Q4_START = datetime(2024, 10, 1, tzinfo=timezone.utc)
RAW_CONTRACT = re.compile(r"^[A-Z]{1,4}[FGHJKMNQUVXZ][0-9]{1,2}$")
RECEIPT_SCHEMA = "hydra_microstructure_0031_acquisition_bundle_receipt_v1"
PURPOSE = (
    "bounded campaign-0031 event-sourced order-flow development pilot; "
    "chronological roles frozen; protected Q4 excluded"
)
CANDIDATE_TIER = "MICROSTRUCTURE_ORDER_FLOW_PILOT_0031"


class FoundryAcquisitionError(RuntimeError):
    """The bounded acquisition cannot proceed without contract drift."""


def load_frozen_inputs(
    manifest_path: str | Path,
    *,
    cost_report_path: str | Path | None = None,
) -> tuple[Path, dict[str, Any], Path, dict[str, Any]]:
    path = Path(manifest_path).resolve()
    manifest = load_and_validate_production_manifest(path)
    root = path.parents[2]
    cost_path = (
        Path(cost_report_path).resolve()
        if cost_report_path
        else root
        / str(manifest["runtime"]["output_dir"])
        / "databento_microstructure_cost_matrix.json"
    )
    try:
        report = json.loads(cost_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FoundryAcquisitionError("immutable 0031 cost report is absent") from exc
    return root, manifest, cost_path, report


def validate_frozen_requests(
    manifest: Mapping[str, Any],
    cost_report: Mapping[str, Any],
    *,
    root: str | Path,
) -> tuple[dict[str, Any], ...]:
    root_path = Path(root).resolve()
    if (
        manifest.get("campaign_id") != CAMPAIGN_ID
        or manifest.get("campaign_mode") != CAMPAIGN_MODE
        or manifest.get("development_only") is not True
    ):
        raise FoundryAcquisitionError("0031 manifest identity drift")
    manifest_hash = str(manifest.get("manifest_hash") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", manifest_hash):
        raise FoundryAcquisitionError("0031 manifest hash is absent")
    acquisition = manifest.get("bounded_acquisition")
    if not isinstance(acquisition, Mapping) or (
        acquisition.get("provider") != "Databento"
        or acquisition.get("dataset") != "GLBX.MDP3"
        or acquisition.get("q4_access_allowed") is not False
        or acquisition.get("broad_historical_purchase_allowed") is not False
        or float(acquisition.get("maximum_initial_spend_usd", -1.0)) != 10.0
        or float(acquisition.get("minimum_budget_reserve_usd", -1.0)) != 25.0
    ):
        raise FoundryAcquisitionError("0031 acquisition guard drift")
    if (
        cost_report.get("schema") != "hydra_databento_microstructure_0031_cost_matrix_v1"
        or cost_report.get("campaign_id") != CAMPAIGN_ID
        or cost_report.get("manifest_hash") != manifest_hash
        or cost_report.get("metadata_only") is not True
        or cost_report.get("purchase_authorized") is not False
    ):
        raise FoundryAcquisitionError("0031 cost report identity/purity drift")
    claimed = str(cost_report.get("cost_matrix_hash") or "")
    core = dict(cost_report)
    core.pop("cost_matrix_hash", None)
    if stable_hash(core) != claimed:
        raise FoundryAcquisitionError("0031 cost report hash drift")
    plan = cost_report.get("acquisition_plan")
    if not isinstance(plan, Mapping) or plan.get("purchase_authorized") is not False:
        raise FoundryAcquisitionError("metadata plan cannot authorize purchase")
    selected_rows = tuple(plan.get("requests") or ())
    roles = tuple(plan.get("roles_by_request") or ())
    if not selected_rows or len(selected_rows) != len(roles):
        raise FoundryAcquisitionError("0031 acquisition request bundle is incomplete")
    matrix = cost_report.get("matrix")
    if not isinstance(matrix, Mapping):
        raise FoundryAcquisitionError("0031 cost matrix is missing")
    allowed_rows = [dict(v) for v in matrix.get("rows") or ()] + [
        dict(v) for v in matrix.get("combined_rows") or ()
    ]
    requests: list[dict[str, Any]] = []
    for position, (raw, role) in enumerate(zip(selected_rows, roles, strict=True)):
        selected = dict(raw)
        if selected not in allowed_rows:
            raise FoundryAcquisitionError("planned request is not an exact cost row")
        symbols = tuple(str(v).upper() for v in selected.get("symbols") or ())
        schema = str(selected.get("schema") or "")
        start = _utc(str(selected.get("start") or ""))
        end = _utc(str(selected.get("end") or ""))
        sessions = int(selected.get("session_count", 0))
        if (
            selected.get("dataset") != "GLBX.MDP3"
            or selected.get("stype_in") != "raw_symbol"
            or schema not in {"trades", "tbbo", "mbp-1", "mbp-10", "mbo"}
            or not 1 <= len(symbols) <= 2
            or any(not RAW_CONTRACT.fullmatch(v) for v in symbols)
            or start >= end
            or end > Q4_START
            or sessions not in {5, 10, 20, 30}
        ):
            raise FoundryAcquisitionError("planned Databento request is unsafe")
        role_values = (
            tuple(role.get("discovery") or ())
            + tuple(role.get("validation") or ())
            + tuple(role.get("final_development") or ())
        )
        if len(role_values) != sessions or len(set(role_values)) != sessions:
            raise FoundryAcquisitionError("0031 temporal roles overlap or are incomplete")
        request_core = {
            "campaign_id": CAMPAIGN_ID,
            "manifest_hash": manifest_hash,
            "dataset": "GLBX.MDP3",
            "schema": schema,
            "symbols": list(symbols),
            "stype_in": "raw_symbol",
            "stype_out": "instrument_id",
            "start": _iso(start),
            "end": _iso(end),
            "session_count": sessions,
            "chronological_roles": dict(role),
            "frozen_estimated_cost_usd": float(selected["estimated_cost_usd"]),
            "purpose": PURPOSE,
            "candidate_tier": CANDIDATE_TIER,
        }
        request_id = request_id_for(request_core)
        raw_name = f"request_{position:02d}_{request_id[:16]}_{schema}.dbn.zst"
        requests.append(
            {
                **request_core,
                "request_id": request_id,
                "raw_path": str(
                    root_path
                    / "data/cache/databento/microstructure_0031/raw_dbn"
                    / raw_name
                ),
            }
        )
    projected = sum(float(v["frozen_estimated_cost_usd"]) for v in requests)
    if projected > 10.0 + 1e-9:
        raise FoundryAcquisitionError("frozen request bundle exceeds USD 10")
    return tuple(requests)


def acquire_frozen_bundle(
    *,
    manifest: Mapping[str, Any],
    cost_report: Mapping[str, Any],
    root: str | Path,
    client: Any,
    execute: bool,
    budget: DatabentoBudgetConfig | None = None,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    requests = validate_frozen_requests(manifest, cost_report, root=root_path)
    live_costs = tuple(float(client.metadata.get_cost(**_metadata(v))) for v in requests)
    total_live = sum(live_costs)
    acquisition = dict(manifest["bounded_acquisition"])
    total_budget = float(acquisition["total_budget_usd"])
    reserve = float(acquisition["minimum_budget_reserve_usd"])
    if not 0 <= total_live <= float(acquisition["maximum_initial_spend_usd"]):
        raise FoundryAcquisitionError("live aggregate estimate exceeds USD 10")
    cfg = budget or DatabentoBudgetConfig()
    plan = {
        "schema": "hydra_microstructure_0031_acquisition_plan_v1",
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "requests": list(requests),
        "live_estimates_usd": list(live_costs),
        "aggregate_live_estimate_usd": total_live,
        "maximum_initial_spend_usd": 10.0,
        "minimum_budget_reserve_usd": reserve,
        "q4_excluded": True,
        "execute": bool(execute),
    }
    _enforce_aggregate_budget(cfg, requests, live_costs, total_budget, reserve, root_path)
    if not execute:
        return {**plan, "network_request_made": False, "download_status": "DRY_RUN_ONLY"}

    lock_path = root_path / "reports/data_access/microstructure_0031_acquisition.lock"
    access_path = root_path / "reports/data_access/data_access_ledger.jsonl"
    with _exclusive_lock(lock_path):
        _enforce_aggregate_budget(cfg, requests, live_costs, total_budget, reserve, root_path)
        downloaded: list[dict[str, Any]] = []
        any_network = False
        for request, live_cost in zip(requests, live_costs, strict=True):
            item = _acquire_one(
                request=request,
                live_cost=live_cost,
                manifest_hash=str(manifest["manifest_hash"]),
                client=client,
                budget=cfg,
                root=root_path,
                access_path=access_path,
            )
            any_network |= bool(item["network_request_made"])
            downloaded.append(item)
        receipt = _seal_bundle_receipt(
            root_path / str(manifest["runtime"]["output_dir"])
            / "microstructure_acquisition_receipt.json",
            plan=plan,
            items=downloaded,
            network_request_made=any_network,
        )
    return receipt


def _acquire_one(
    *,
    request: Mapping[str, Any],
    live_cost: float,
    manifest_hash: str,
    client: Any,
    budget: DatabentoBudgetConfig,
    root: Path,
    access_path: Path,
) -> dict[str, Any]:
    ledger = _ledger_path(budget)
    rows = [v for v in read_ledger(ledger) if v.get("request_id") == request["request_id"]]
    raw_path = Path(str(request["raw_path"]))
    complete = [v for v in rows if v.get("download_status") == "DOWNLOADED"]
    if complete:
        if not raw_path.is_file() or sha256_file(raw_path) != complete[-1].get("checksum"):
            raise FoundryAcquisitionError("completed raw cache provenance drift")
        _record_access_once(access_path, request=request, manifest_hash=manifest_hash)
        return _item_receipt(request, raw_path, float(complete[-1]["actual_cost_usd"]), False)
    if raw_path.exists() and not rows:
        raise FoundryAcquisitionError("unledgered raw cache exists")
    if not any(v.get("download_status") == "ESTIMATED_ONLY" for v in rows):
        projected, actual = enforce_budget(budget, live_cost)
        append_spend_record(
            budget,
            DatabentoSpendRecord(
                request_id=str(request["request_id"]), timestamp_utc=utc_now(),
                dataset=str(request["dataset"]), schema=str(request["schema"]),
                symbols=list(request["symbols"]), stype_in=str(request["stype_in"]),
                start=str(request["start"]), end=str(request["end"]),
                estimated_cost_usd=live_cost, actual_cost_usd=None,
                cumulative_estimated_spend_usd=projected,
                cumulative_actual_spend_usd=actual, cache_hit=False,
                research_purpose=PURPOSE, candidate_tier=CANDIDATE_TIER,
                approval_mode=AUTO_UNDER_HARD_CAP, resulting_file=None,
                checksum=None, download_status="ESTIMATED_ONLY",
            ),
        )
    network = False
    if not raw_path.is_file():
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = raw_path.with_name(f".{raw_path.name}.{os.getpid()}.tmp")
        temporary.unlink(missing_ok=True)
        try:
            client.timeseries.get_range(
                **_metadata(request), stype_out="instrument_id", path=str(temporary)
            )
            if not temporary.is_file() or temporary.stat().st_size <= 0:
                raise FoundryAcquisitionError("Databento returned an empty raw file")
            os.replace(temporary, raw_path)
            network = True
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
    checksum = sha256_file(raw_path)
    estimated, actual = cumulative_spend(ledger)
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=str(request["request_id"]), timestamp_utc=utc_now(),
            dataset=str(request["dataset"]), schema=str(request["schema"]),
            symbols=list(request["symbols"]), stype_in=str(request["stype_in"]),
            start=str(request["start"]), end=str(request["end"]),
            estimated_cost_usd=0.0, actual_cost_usd=live_cost,
            cumulative_estimated_spend_usd=estimated,
            cumulative_actual_spend_usd=actual + live_cost, cache_hit=False,
            research_purpose=PURPOSE, candidate_tier=CANDIDATE_TIER,
            approval_mode=AUTO_UNDER_HARD_CAP, resulting_file=str(raw_path),
            checksum=checksum, download_status="DOWNLOADED",
        ),
    )
    _record_access_once(access_path, request=request, manifest_hash=manifest_hash)
    return _item_receipt(request, raw_path, live_cost, network)


def _enforce_aggregate_budget(
    budget: DatabentoBudgetConfig,
    requests: tuple[dict[str, Any], ...],
    costs: tuple[float, ...],
    total_budget: float,
    reserve: float,
    root: Path,
) -> None:
    ledger = _ledger_path(budget)
    rows = read_ledger(ledger)
    completed = {str(v.get("request_id")) for v in rows if v.get("download_status") == "DOWNLOADED"}
    estimated = {str(v.get("request_id")) for v in rows if v.get("download_status") == "ESTIMATED_ONLY"}
    incremental_estimate = sum(
        cost for request, cost in zip(requests, costs, strict=True)
        if request["request_id"] not in completed and request["request_id"] not in estimated
    )
    enforce_budget(budget, incremental_estimate)
    _estimated, actual = cumulative_spend(ledger)
    outstanding = sum(
        cost for request, cost in zip(requests, costs, strict=True)
        if request["request_id"] not in completed
    )
    if total_budget - actual - outstanding < reserve - 1e-9:
        raise FoundryAcquisitionError("live request bundle would consume protected reserve")


def _record_access_once(path: Path, *, request: Mapping[str, Any], manifest_hash: str) -> None:
    rows = []
    if path.is_file():
        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    marker = str(request["request_id"])
    if any(marker in set(v.get("candidate_ids") or ()) for v in rows):
        return
    enforce_data_access(
        period=f"{request['start']}:{request['end']}",
        role=DataRole.CONTAMINATED_DEVELOPMENT,
        requesting_module="scripts.acquire_microstructure_foundry_0031",
        candidate_ids=[CAMPAIGN_ID, marker], reason=PURPOSE,
        freeze_manifest_hash=manifest_hash, ledger_path=str(path),
    )


def _item_receipt(
    request: Mapping[str, Any], raw_path: Path, actual_cost: float, network: bool
) -> dict[str, Any]:
    return {
        "request_id": request["request_id"],
        "request": dict(request),
        "actual_spend_usd": actual_cost,
        "raw_path": str(raw_path),
        "raw_size_bytes": raw_path.stat().st_size,
        "raw_sha256": sha256_file(raw_path),
        "network_request_made": network,
        "download_status": "DOWNLOADED",
    }


def _seal_bundle_receipt(
    path: Path, *, plan: Mapping[str, Any], items: list[dict[str, Any]],
    network_request_made: bool,
) -> dict[str, Any]:
    core = {
        "schema": RECEIPT_SCHEMA, "campaign_id": CAMPAIGN_ID,
        "manifest_hash": plan["manifest_hash"], "created_at_utc": utc_now(),
        "requests": items, "request_count": len(items),
        "actual_spend_usd": sum(float(v["actual_spend_usd"]) for v in items),
        "network_request_made": network_request_made,
        "data_role": DataRole.CONTAMINATED_DEVELOPMENT.value,
        "q4_access_count_delta": 0, "broker_connections": 0, "orders": 0,
        "download_status": "DOWNLOADED",
    }
    payload = {**core, "receipt_hash": stable_hash(core)}
    if path.is_file():
        existing = json.loads(path.read_text())
        old = dict(existing); claimed = old.pop("receipt_hash", "")
        expected_items = [
            {
                "request_id": item["request_id"],
                "raw_path": item["raw_path"],
                "raw_sha256": item["raw_sha256"],
            }
            for item in items
        ]
        observed_items = [
            {
                "request_id": item.get("request_id"),
                "raw_path": item.get("raw_path"),
                "raw_sha256": item.get("raw_sha256"),
            }
            for item in existing.get("requests") or ()
        ]
        if (
            stable_hash(old) != claimed
            or existing.get("manifest_hash") != plan["manifest_hash"]
            or observed_items != expected_items
        ):
            raise FoundryAcquisitionError("existing acquisition receipt drift")
        return existing
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)
    return payload


def _metadata(request: Mapping[str, Any]) -> dict[str, Any]:
    return {key: request[key] for key in ("dataset", "symbols", "schema", "stype_in", "start", "end")}


def _ledger_path(budget: DatabentoBudgetConfig) -> Path:
    path = Path(budget.ledger_path)
    return path if path.is_absolute() else project_path(str(path))


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise FoundryAcquisitionError("another 0031 acquisition holds the lock") from exc
        yield
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FoundryAcquisitionError("invalid request timestamp") from exc
    if parsed.tzinfo is None:
        raise FoundryAcquisitionError("request timestamp lacks timezone")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> int:
    parser = argparse.ArgumentParser(description="Acquire frozen Databento bundle for HYDRA 0031")
    parser.add_argument("--manifest", default="config/v7/microstructure_order_flow_foundry_0031.json")
    parser.add_argument("--cost-report")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    root, manifest, _path, report = load_frozen_inputs(args.manifest, cost_report_path=args.cost_report)
    key = load_api_key()
    if not key:
        raise FoundryAcquisitionError("DATABENTO_API_KEY is unavailable")
    import databento as db
    result = acquire_frozen_bundle(
        manifest=manifest, cost_report=report, root=root,
        client=db.Historical(key), execute=bool(args.execute),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
