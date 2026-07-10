from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso


MANIFEST_VERSION = "protected_manifest_v1"


@dataclass(frozen=True)
class ProtectedFileDigest:
    path: str
    sha256: str
    exists: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ProtectedManifest:
    version: str
    created_utc: str
    governance_config: str
    baseline_commit: str
    digests: tuple[ProtectedFileDigest, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "created_utc": self.created_utc,
            "governance_config": self.governance_config,
            "baseline_commit": self.baseline_commit,
            "digests": [item.to_dict() for item in self.digests],
            "manifest_hash": self.manifest_hash(),
        }

    def manifest_hash(self) -> str:
        payload = {
            "version": self.version,
            "governance_config": self.governance_config,
            "baseline_commit": self.baseline_commit,
            "digests": [item.to_dict() for item in self.digests],
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def load_governance_config(path: str | Path = "config/governance/hydra_governance_v1.yaml") -> dict[str, Any]:
    with project_path(str(path)).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_protected_manifest(
    *,
    baseline_commit: str,
    config_path: str = "config/governance/hydra_governance_v1.yaml",
) -> ProtectedManifest:
    config = load_governance_config(config_path)
    digests: list[ProtectedFileDigest] = []
    for rel_path in config.get("protected_files", []):
        path = project_path(rel_path)
        if path.exists():
            digests.append(ProtectedFileDigest(rel_path, file_sha256(path), True))
        else:
            digests.append(ProtectedFileDigest(rel_path, "", False))
    return ProtectedManifest(
        version=MANIFEST_VERSION,
        created_utc=utc_now_iso(),
        governance_config=config_path,
        baseline_commit=baseline_commit,
        digests=tuple(digests),
    )


def write_manifest(manifest: ProtectedManifest, path: str | Path) -> Path:
    target = project_path(str(path))
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(manifest.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
    return target


def read_manifest(path: str | Path) -> dict[str, Any]:
    return json.loads(project_path(str(path)).read_text(encoding="utf-8"))

