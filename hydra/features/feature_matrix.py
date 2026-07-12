from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np


@dataclass(frozen=True)
class FeatureMatrix:
    """Read-only, columnar feature bundle used by Turbo workers.

    Arrays are deliberately kept outside mission SQLite.  A worker receives a
    manifest path and opens every array read-only, so it can never mutate the
    canonical feature bundle or become an independent registry writer.
    """

    root: Path
    manifest: Mapping[str, Any]
    arrays: Mapping[str, np.ndarray]

    @property
    def row_count(self) -> int:
        return int(self.manifest["row_count"])

    @property
    def fingerprint(self) -> str:
        return str(self.manifest["bundle_hash"])

    def array(self, name: str) -> np.ndarray:
        try:
            value = self.arrays[name]
        except KeyError as exc:
            raise KeyError(f"Feature matrix has no column {name!r}.") from exc
        value.flags.writeable = False
        return value

    @classmethod
    def open(cls, root: str | Path, *, mmap: bool = True) -> "FeatureMatrix":
        directory = Path(root)
        manifest_path = directory / "manifest.json"
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        expected = str(payload.get("bundle_hash") or "")
        unhashed = dict(payload)
        unhashed.pop("bundle_hash", None)
        actual = _stable_hash(unhashed)
        if not expected or expected != actual:
            raise ValueError("Canonical feature manifest hash does not recompute.")
        arrays: dict[str, np.ndarray] = {}
        for name, metadata in sorted(dict(payload["arrays"]).items()):
            path = directory / str(metadata["path"])
            if _file_sha256(path) != str(metadata["sha256"]):
                raise ValueError(f"Canonical feature array changed: {name}")
            array = np.load(path, mmap_mode="r" if mmap else None, allow_pickle=False)
            if list(array.shape) != list(metadata["shape"]) or str(array.dtype) != str(
                metadata["dtype"]
            ):
                raise ValueError(f"Canonical feature array metadata drifted: {name}")
            array.flags.writeable = False
            arrays[name] = array
        row_count = int(payload["row_count"])
        if any(len(value) != row_count for value in arrays.values()):
            raise ValueError("Canonical feature arrays have inconsistent lengths.")
        return cls(root=directory, manifest=payload, arrays=arrays)


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
