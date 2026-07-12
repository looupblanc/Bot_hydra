from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from hydra.governance.proof_registry import (
    ProofRegistryError,
    load_and_verify as load_and_verify_proof_registry,
    verify_registry_prefix,
)


SCHEMA = "hydra_v7_data_lake_manifest_v1"
MARKET_PRODUCTS = (
    "ES",
    "MES",
    "NQ",
    "MNQ",
    "RTY",
    "M2K",
    "YM",
    "MYM",
    "GC",
    "MGC",
    "CL",
    "MCL",
)
_ZERO_HASH = "0" * 64


class DataManifestError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_hash(payload: Mapping[str, Any]) -> str:
    unhashed = dict(payload)
    unhashed.pop("manifest_hash", None)
    encoded = json.dumps(
        unhashed, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_v7_data_manifest(
    project_root: str | Path,
    *,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    data_root = root / "data"
    if not data_root.exists():
        raise DataManifestError("data directory is missing")
    generated_at = generated_at_utc or datetime.now(UTC).isoformat().replace(
        "+00:00", "Z"
    )

    artifacts: list[dict[str, Any]] = []
    coverage: dict[str, list[dict[str, Any]]] = {
        product: [] for product in MARKET_PRODUCTS
    }
    declared_arrays: dict[Path, dict[str, Any]] = {}

    for path in sorted(data_root.rglob("*")):
        if not path.is_file() or path.resolve() == (data_root / "manifest.json").resolve():
            continue
        relative = path.relative_to(root)
        if path.suffix == ".npy":
            continue
        if path.name.endswith(".dbn.zst"):
            artifact, rows = _dbn_artifact(path, relative)
            artifacts.append(artifact)
            for product, row in rows.items():
                coverage[product].append(row)
            continue
        if path.suffix == ".parquet":
            artifact, rows = _parquet_artifact(path, relative)
            artifacts.append(artifact)
            for product, row in rows.items():
                coverage[product].append(row)
            continue
        if path.name == "manifest.json" and "turbo_foundry_v2" in path.parts:
            artifact, array_rows = _feature_manifest_artifact(path, relative)
            artifacts.append(artifact)
            declared_arrays.update(array_rows)
            continue
        artifacts.append(_generic_artifact(path, relative))

    actual_arrays = {
        path.resolve() for path in data_root.rglob("*.npy") if path.is_file()
    }
    declared_paths = set(declared_arrays)
    missing_declared = sorted(str(path) for path in declared_paths - actual_arrays)
    undeclared_arrays = sorted(str(path) for path in actual_arrays - declared_paths)
    if missing_declared or undeclared_arrays:
        raise DataManifestError(
            "feature array inventory mismatch: "
            f"missing={len(missing_declared)} undeclared={len(undeclared_arrays)}"
        )

    product_cutoffs = derive_product_cutoffs(coverage)
    missing_products = [
        product for product in MARKET_PRODUCTS if product not in product_cutoffs
    ]
    if missing_products:
        raise DataManifestError(
            "no raw OHLCV coverage for products: " + ",".join(missing_products)
        )

    market_like = {
        path.resolve()
        for path in data_root.rglob("*")
        if path.is_file()
        and (
            path.name.endswith(".dbn.zst")
            or path.suffix
            in {".parquet", ".csv", ".feather", ".arrow", ".sqlite", ".db"}
        )
    }
    inventoried_market_like = {
        (root / row["path"]).resolve()
        for row in artifacts
        if row["kind"] in {"DATABENTO_DBN", "PARQUET", "GENERIC"}
    }
    unclassified = sorted(
        str(path.relative_to(root))
        for path in market_like - inventoried_market_like
    )

    access_ledger = root / "reports/data_access/data_access_ledger.jsonl"
    proof_registry = root / "mission/state/proof_registry.json"
    access_audit = _access_audit(access_ledger, root)
    proof_audit = _proof_audit(proof_registry, root)
    forward_audit = _forward_audit(root)

    payload: dict[str, Any] = {
        "schema": SCHEMA,
        "generated_at_utc": generated_at,
        "contract_sha256": (
            "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
        ),
        "source_branch": "mission/v7-falsification",
        "source_baseline_commit": (
            "81f05ec9d7cd796ff91fd14d15760322c2924275"
        ),
        "inventory_policy": {
            "direct_artifacts": (
                "Every non-NPY file under data/ except this manifest is hashed."
            ),
            "derived_arrays": (
                "Every NPY is declared with SHA-256 by its immutable canonical "
                "feature-bundle manifest; paths and declarations are reconciled."
            ),
            "cutoff_definition": (
                "Maximum actual ts_event/timestamp present per product, not the "
                "nominal request end."
            ),
        },
        "artifacts": artifacts,
        "artifact_count": len(artifacts),
        "derived_array_count": len(declared_arrays),
        "derived_array_inventory": [
            {
                "path": str(path.relative_to(root)),
                "sha256": row["sha256"],
                "shape": row["shape"],
                "dtype": row["dtype"],
                "declared_by": row["declared_by"],
            }
            for path, row in sorted(
                declared_arrays.items(), key=lambda item: str(item[0])
            )
        ],
        "product_cutoffs": product_cutoffs,
        "proof_roles": {
            "Q4_2024": "BURNED",
            "q4_reusable_as_proof": False,
        },
        "access_audit": access_audit,
        "proof_registry_audit": proof_audit,
        "forward_data_audit": forward_audit,
        "unclassified_market_data_files": unclassified,
        "gap_bootstrap_policy": {
            "candidate_fiche_must_be_WORM_before_ingestion": True,
            "backfill_start_is_product_gap_start_utc": True,
            "each_confirmation_window_single_use_then_BURNED": True,
            "no_gap_data_ingested_by_this_manifest_build": True,
            "virginity_status": "ELIGIBLE_PENDING_CANDIDATE_FREEZE_AND_INGESTION",
            "limitation": (
                "Filesystem and append-only access ledgers cannot prove the "
                "absence of an unlogged manual read; ingestion remains fail-closed."
            ),
        },
        "outbound_order_count": 0,
    }
    payload["manifest_hash"] = canonical_hash(payload)
    return payload


def derive_product_cutoffs(
    coverage: Mapping[str, Iterable[Mapping[str, Any]]]
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for product, rows_iter in coverage.items():
        rows = [dict(row) for row in rows_iter]
        if not rows:
            continue
        latest = max(rows, key=lambda row: int(row["max_timestamp_ns"]))
        earliest = min(rows, key=lambda row: int(row["min_timestamp_ns"]))
        cutoff_ns = int(latest["max_timestamp_ns"])
        output[str(product)] = {
            "first_timestamp_utc": _iso_ns(int(earliest["min_timestamp_ns"])),
            "cutoff_utc": _iso_ns(cutoff_ns),
            "gap_start_utc": _iso_ns(cutoff_ns + 60_000_000_000),
            "cutoff_source": str(latest["path"]),
            "source_paths": sorted({str(row["path"]) for row in rows}),
            "raw_record_count": sum(int(row["record_count"]) for row in rows),
        }
    return output


def write_v7_data_manifest(
    project_root: str | Path,
    output_path: str | Path,
    *,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    payload = build_v7_data_manifest(
        project_root, generated_at_utc=generated_at_utc
    )
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return payload


def verify_v7_data_manifest(
    project_root: str | Path,
    manifest_path: str | Path,
    *,
    verify_artifact_hashes: bool = True,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    path = Path(manifest_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != SCHEMA:
        raise DataManifestError("unexpected data manifest schema")
    expected = str(payload.get("manifest_hash") or "")
    if expected != canonical_hash(payload):
        raise DataManifestError("data manifest canonical hash mismatch")
    if payload.get("unclassified_market_data_files"):
        raise DataManifestError("unclassified market data files are present")
    if payload.get("proof_roles", {}).get("Q4_2024") != "BURNED":
        raise DataManifestError("Q4 is not marked BURNED")
    if verify_artifact_hashes:
        for artifact in payload["artifacts"]:
            artifact_path = root / artifact["path"]
            if not artifact_path.is_file():
                raise DataManifestError(
                    f"manifest artifact missing: {artifact['path']}"
                )
            if sha256_file(artifact_path) != artifact["sha256"]:
                raise DataManifestError(
                    f"manifest artifact hash mismatch: {artifact['path']}"
                )
        access_audit = payload.get("access_audit")
        if access_audit is not None:
            audit_path = root / access_audit["path"]
            if (
                not audit_path.is_file()
                or sha256_file(audit_path) != access_audit["sha256"]
            ):
                raise DataManifestError("access_audit hash mismatch")
        proof_audit = payload.get("proof_registry_audit")
        if proof_audit is not None:
            audit_path = root / proof_audit["path"]
            if not audit_path.is_file():
                raise DataManifestError("proof_registry_audit file missing")
            if sha256_file(audit_path) != proof_audit["sha256"]:
                try:
                    current = load_and_verify_proof_registry(audit_path)
                    verify_registry_prefix(
                        current,
                        entry_count=int(proof_audit["entry_count"]),
                        chain_head=str(proof_audit["chain_head"]),
                    )
                except ProofRegistryError as exc:
                    raise DataManifestError(
                        "proof_registry_audit append-only prefix mismatch"
                    ) from exc
        for row in payload.get("forward_data_audit", {}).get(
            "authorization_manifest_hashes", []
        ):
            audit_path = root / row["path"]
            if not audit_path.is_file() or sha256_file(audit_path) != row["sha256"]:
                raise DataManifestError(
                    "forward authorization manifest hash mismatch: "
                    f"{row['path']}"
                )
    return {
        "valid": True,
        "manifest_hash": expected,
        "artifact_count": int(payload["artifact_count"]),
        "derived_array_count": int(payload["derived_array_count"]),
        "product_count": len(payload["product_cutoffs"]),
    }


def render_data_manifest_report(payload: Mapping[str, Any]) -> str:
    cutoffs = payload["product_cutoffs"]
    lines = [
        "# HYDRA V7 — Data lake manifest",
        "",
        "[HYDRA-V7] phase=0 step=1 verdict=NULL",
        f"gate=BOOTSTRAP_V2 preuve=data/manifest.json#{str(payload['manifest_hash'])[:8]} tests=pending",
        "budget_llm=0.000000/8.00 budget_data=0.000000/60.00 N_trials=pending_retro_estimate burned=1",
        "diff_validation=hydra/data/v7_manifest.py,scripts/build_v7_data_manifest.py,tests/ruleset/test_v7_data_manifest.py CONTRE=un accès manuel non journalisé ne peut pas être exclu cryptographiquement par le seul inventaire du disque",
        "prochaine_action=vérifier_tous_les_hashes_puis_encoder_R16_sans_ingérer_le_gap",
        "",
        "## Cutoffs réels par produit",
        "",
        "| Produit | Premier timestamp | Cutoff réel | Début du gap |",
        "|---|---:|---:|---:|",
    ]
    for product in MARKET_PRODUCTS:
        row = cutoffs[product]
        lines.append(
            f"| {product} | {row['first_timestamp_utc']} | {row['cutoff_utc']} | {row['gap_start_utc']} |"
        )
    lines.extend(
        [
            "",
            f"Artefacts directs hachés : `{payload['artifact_count']}`.",
            f"Arrays dérivés réconciliés : `{payload['derived_array_count']}`.",
            f"Fichiers de marché non classés : `{len(payload['unclassified_market_data_files'])}`.",
            f"Barres forward présentes : `{payload['forward_data_audit']['fresh_bar_file_count']}`.",
            "",
            "Aucune ingestion du gap n'a été exécutée pendant la construction de ce manifest.",
            "",
            "## CONTRE",
            "",
            "Le manifest prouve l'état du filesystem et des ledgers connus, pas l'impossibilité absolue d'une lecture manuelle non journalisée. Le feed restera donc fail-closed et vérifiera aussi l'antériorité des fiches WORM avant chaque consommation.",
            "",
        ]
    )
    return "\n".join(lines)


def _dbn_artifact(
    path: Path, relative: Path
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    try:
        import databento as db
    except ImportError as exc:  # pragma: no cover - production dependency
        raise DataManifestError("databento package is required") from exc
    store = db.DBNStore.from_file(path)
    metadata = store.metadata
    records = store.to_ndarray()
    names = records.dtype.names or ()
    timestamp_field = "ts_event" if "ts_event" in names else "ts_recv"
    if timestamp_field not in names or len(records) == 0:
        raise DataManifestError(f"DBN has no timestamped records: {relative}")
    timestamps = records[timestamp_field].astype(np.int64, copy=False)
    min_ns = int(timestamps.min())
    max_ns = int(timestamps.max())
    schema = str(metadata.schema)
    symbols = sorted(str(value) for value in metadata.symbols)
    artifact = {
        "path": str(relative),
        "kind": "DATABENTO_DBN",
        "role": _role_for_path(relative, schema=schema),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
        "dataset": str(metadata.dataset),
        "schema_name": schema,
        "symbols": symbols,
        "request_start_utc": _iso_ns(int(metadata.start)),
        "request_end_exclusive_utc": (
            _iso_ns(int(metadata.end)) if metadata.end is not None else None
        ),
        "first_record_timestamp_utc": _iso_ns(min_ns),
        "last_record_timestamp_utc": _iso_ns(max_ns),
        "record_count": int(len(records)),
        "timestamp_field": timestamp_field,
    }
    per_product: dict[str, dict[str, Any]] = {}
    if schema == "ohlcv-1m" and "instrument_id" in names:
        mappings = store.mappings
        ids = records["instrument_id"].astype(np.int64, copy=False)
        for raw_symbol, intervals in mappings.items():
            product = _product_from_symbol(raw_symbol)
            if product not in MARKET_PRODUCTS:
                continue
            instrument_ids = {
                int(interval["symbol"])
                for interval in intervals
                if str(interval.get("symbol", "")).isdigit()
            }
            mask = np.isin(ids, tuple(instrument_ids))
            if not bool(mask.any()):
                continue
            product_timestamps = timestamps[mask]
            per_product[product] = {
                "path": str(relative),
                "min_timestamp_ns": int(product_timestamps.min()),
                "max_timestamp_ns": int(product_timestamps.max()),
                "record_count": int(mask.sum()),
            }
    return artifact, per_product


def _parquet_artifact(
    path: Path, relative: Path
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    frame = pd.read_parquet(path, columns=["timestamp", "symbol"])
    if frame.empty:
        raise DataManifestError(f"Parquet has no records: {relative}")
    timestamps = pd.to_datetime(frame["timestamp"], utc=True)
    artifact = {
        "path": str(relative),
        "kind": "PARQUET",
        "role": _role_for_path(relative, schema="ohlcv-1m"),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
        "schema_name": "ohlcv-1m-normalized",
        "symbols": sorted(frame["symbol"].astype(str).unique().tolist()),
        "first_record_timestamp_utc": _iso_timestamp(timestamps.min()),
        "last_record_timestamp_utc": _iso_timestamp(timestamps.max()),
        "record_count": int(len(frame)),
        "timestamp_field": "timestamp",
    }
    per_product: dict[str, dict[str, Any]] = {}
    for raw_symbol, group in frame.assign(_timestamp=timestamps).groupby(
        frame["symbol"].astype(str), sort=True
    ):
        product = _product_from_symbol(raw_symbol)
        if product not in MARKET_PRODUCTS:
            continue
        values = group["_timestamp"].astype("int64")
        per_product[product] = {
            "path": str(relative),
            "min_timestamp_ns": int(values.min()),
            "max_timestamp_ns": int(values.max()),
            "record_count": int(len(group)),
        }
    return artifact, per_product


def _feature_manifest_artifact(
    path: Path, relative: Path
) -> tuple[dict[str, Any], dict[Path, dict[str, Any]]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    arrays: dict[Path, dict[str, Any]] = {}
    for row in payload.get("arrays", {}).values():
        array_path = (path.parent / str(row["path"])).resolve()
        arrays[array_path] = {
            "sha256": str(row["sha256"]),
            "shape": list(row["shape"]),
            "dtype": str(row["dtype"]),
            "declared_by": str(relative),
        }
    key = payload.get("key", {})
    provenance = payload.get("provenance", {})
    return (
        {
            "path": str(relative),
            "kind": "CANONICAL_FEATURE_MANIFEST",
            "role": "DERIVED_DEVELOPMENT",
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
            "market": key.get("market"),
            "start_inclusive": key.get("start_inclusive"),
            "end_exclusive": key.get("end_exclusive"),
            "latest_timestamp_utc": (
                _iso_ns(int(provenance["latest_timestamp_ns"]))
                if provenance.get("latest_timestamp_ns") is not None
                else None
            ),
            "bundle_hash": payload.get("bundle_hash"),
            "array_count": len(arrays),
            "mutable": bool(payload.get("mutable", True)),
        },
        arrays,
    )


def _generic_artifact(path: Path, relative: Path) -> dict[str, Any]:
    if "contract_maps" in path.parts:
        role = "REFERENCE_SYMBOLOGY"
    elif "behavioral_evidence" in path.parts:
        role = "DERIVED_HISTORICAL_NOT_PROOF"
    else:
        role = "AUXILIARY_DATA_ARTIFACT"
    return {
        "path": str(relative),
        "kind": "GENERIC",
        "role": role,
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _role_for_path(relative: Path, *, schema: str) -> str:
    name = relative.name
    if "2024-10-01_2025-01-01" in name or "definition_q4" in name:
        return "BURNED_Q4"
    if schema == "definition" or "definitions" in name:
        return "REFERENCE_SYMBOLOGY"
    return "DEVELOPMENT"


def _access_audit(path: Path, root: Path) -> dict[str, Any]:
    if not path.is_file():
        raise DataManifestError("data-access ledger is missing")
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    periods = sorted({str(row.get("period_accessed")) for row in rows})
    q4_rows = [row for row in rows if row.get("data_role") == "FINAL_LOCKBOX"]
    post_q4_rows = [
        row
        for row in rows
        if _period_end_date(str(row.get("period_accessed", ""))) > "2025-01-01"
    ]
    return {
        "path": str(path.relative_to(root)),
        "sha256": sha256_file(path),
        "entry_count": len(rows),
        "latest_recorded_at_utc": max(
            (str(row.get("timestamp_utc", "")) for row in rows), default=None
        ),
        "logged_periods": periods,
        "q4_final_lockbox_record_count": len(q4_rows),
        "post_2025_01_01_market_access_record_count": len(post_q4_rows),
    }


def _proof_audit(path: Path, root: Path) -> dict[str, Any]:
    if not path.is_file():
        raise DataManifestError("proof registry is missing")
    payload = json.loads(path.read_text(encoding="utf-8"))
    q4 = [
        row
        for row in payload.get("entries", [])
        if row.get("window", {}).get("id") == "Q4_2024"
        and row.get("status") == "BURNED"
    ]
    if len(q4) != 1:
        raise DataManifestError("proof registry must contain one BURNED Q4 entry")
    return {
        "path": str(path.relative_to(root)),
        "sha256": sha256_file(path),
        "entry_count": int(payload.get("entry_count", -1)),
        "chain_head": str(payload.get("chain_head", _ZERO_HASH)),
        "q4_burned_entry_count": len(q4),
    }


def _forward_audit(root: Path) -> dict[str, Any]:
    directory = root / "shadow/state/forward_data"
    manifests = sorted((directory / "manifests").glob("*.json"))
    non_metadata = [
        path
        for path in directory.rglob("*")
        if path.is_file()
        and "manifests" not in path.parts
        and not path.name.endswith(".lock")
    ]
    bar_like = [
        path
        for path in non_metadata
        if path.suffix in {".db", ".sqlite", ".parquet", ".csv", ".jsonl"}
    ]
    return {
        "authorization_manifest_count": len(manifests),
        "authorization_manifest_hashes": [
            {
                "path": str(path.relative_to(root)),
                "sha256": sha256_file(path),
            }
            for path in manifests
        ],
        "fresh_bar_file_count": len(bar_like),
        "fresh_bar_files": [str(path.relative_to(root)) for path in bar_like],
        "fresh_bars_processed": 0 if not bar_like else None,
    }


def _period_end_date(value: str) -> str:
    matches = re.findall(r"\d{4}-\d{2}-\d{2}", value)
    return matches[-1] if matches else "0000-00-00"


def _product_from_symbol(value: str) -> str:
    return str(value).split(".", 1)[0].split("-", 1)[0].upper()


def _iso_timestamp(value: Any) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.isoformat().replace("+00:00", "Z")


def _iso_ns(value: int) -> str:
    return pd.Timestamp(value, unit="ns", tz="UTC").isoformat().replace(
        "+00:00", "Z"
    )


__all__ = [
    "DataManifestError",
    "MARKET_PRODUCTS",
    "SCHEMA",
    "build_v7_data_manifest",
    "canonical_hash",
    "derive_product_cutoffs",
    "render_data_manifest_report",
    "sha256_file",
    "verify_v7_data_manifest",
    "write_v7_data_manifest",
]
