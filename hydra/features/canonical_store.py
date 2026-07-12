from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from hydra.features.feature_matrix import FeatureMatrix, _file_sha256, _stable_hash


STORE_VERSION = "hydra_canonical_feature_store_v2"


@dataclass(frozen=True)
class CanonicalFeatureKey:
    market: str
    explicit_contract_scope: str
    start_inclusive: str
    end_exclusive: str
    source_data_sha256: str
    roll_map_hash: str
    transformation_version: str
    feature_dag_hash: str
    timeframes: tuple[str, ...]

    @property
    def digest(self) -> str:
        return _stable_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "market": self.market,
            "explicit_contract_scope": self.explicit_contract_scope,
            "start_inclusive": self.start_inclusive,
            "end_exclusive": self.end_exclusive,
            "source_data_sha256": self.source_data_sha256,
            "roll_map_hash": self.roll_map_hash,
            "transformation_version": self.transformation_version,
            "feature_dag_hash": self.feature_dag_hash,
            "timeframes": list(self.timeframes),
        }


@dataclass(frozen=True)
class FeatureStoreWriteResult:
    path: Path
    cache_hit: bool
    bundle_hash: str
    row_count: int


class CanonicalFeatureStore:
    """Content-addressed, atomic feature cache with read-only mmap consumers."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, key: CanonicalFeatureKey) -> Path:
        return self.root / key.digest

    def get(self, key: CanonicalFeatureKey, *, mmap: bool = True) -> FeatureMatrix | None:
        path = self.path_for(key)
        if not (path / "manifest.json").is_file():
            return None
        matrix = FeatureMatrix.open(path, mmap=mmap)
        if dict(matrix.manifest.get("key") or {}) != key.to_dict():
            raise ValueError("Feature-cache key does not match its content-addressed path.")
        return matrix

    def put(
        self,
        key: CanonicalFeatureKey,
        arrays: Mapping[str, np.ndarray],
        *,
        provenance: Mapping[str, Any],
        availability_contract: str = "completed_source_bar_at_or_before_decision",
    ) -> FeatureStoreWriteResult:
        existing = self.get(key)
        if existing is not None:
            return FeatureStoreWriteResult(
                path=existing.root,
                cache_hit=True,
                bundle_hash=existing.fingerprint,
                row_count=existing.row_count,
            )
        normalized = _normalize_arrays(arrays)
        row_count = len(next(iter(normalized.values()))) if normalized else 0
        temporary = Path(
            tempfile.mkdtemp(prefix=f".{key.digest}.", dir=str(self.root))
        )
        try:
            metadata: dict[str, Any] = {}
            for index, (name, value) in enumerate(sorted(normalized.items())):
                relative = f"{index:03d}_{_safe_name(name)}.npy"
                path = temporary / relative
                with path.open("wb") as handle:
                    np.save(handle, value, allow_pickle=False)
                    handle.flush()
                    os.fsync(handle.fileno())
                metadata[name] = {
                    "path": relative,
                    "sha256": _file_sha256(path),
                    "shape": list(value.shape),
                    "dtype": str(value.dtype),
                }
            manifest: dict[str, Any] = {
                "schema": STORE_VERSION,
                "key": key.to_dict(),
                "row_count": row_count,
                "arrays": metadata,
                "availability_contract": availability_contract,
                "provenance": dict(provenance),
                "writer_count": 1,
                "mutable": False,
            }
            manifest["bundle_hash"] = _stable_hash(manifest)
            manifest_path = temporary / "manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            with manifest_path.open("rb") as handle:
                os.fsync(handle.fileno())
            destination = self.path_for(key)
            try:
                os.replace(temporary, destination)
            except FileExistsError:
                # Another pure feature builder may have won the same content
                # race. Its immutable hash must still validate before reuse.
                shutil.rmtree(temporary, ignore_errors=True)
            matrix = FeatureMatrix.open(destination)
            return FeatureStoreWriteResult(
                path=destination,
                cache_hit=False,
                bundle_hash=matrix.fingerprint,
                row_count=matrix.row_count,
            )
        except Exception:
            shutil.rmtree(temporary, ignore_errors=True)
            raise


def _normalize_arrays(arrays: Mapping[str, np.ndarray]) -> dict[str, np.ndarray]:
    output: dict[str, np.ndarray] = {}
    row_count: int | None = None
    for name, raw in arrays.items():
        value = np.asarray(raw)
        if value.dtype.hasobject:
            raise TypeError(f"Object arrays are prohibited in the canonical store: {name}")
        if value.ndim != 1:
            raise ValueError(f"Feature array must be one-dimensional: {name}")
        if row_count is None:
            row_count = len(value)
        elif len(value) != row_count:
            raise ValueError("Feature arrays have inconsistent row counts.")
        contiguous = np.ascontiguousarray(value)
        contiguous.flags.writeable = False
        output[str(name)] = contiguous
    if not output:
        raise ValueError("A canonical feature bundle cannot be empty.")
    return output


def _safe_name(value: str) -> str:
    return "".join(character if character.isalnum() or character in "_-" else "_" for character in value)
