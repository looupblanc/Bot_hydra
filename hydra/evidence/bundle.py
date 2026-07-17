from __future__ import annotations

import fcntl
import gzip
import hashlib
import json
import math
import os
import re
import stat
import subprocess
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from .schema import (
    COST_SCENARIOS,
    EVIDENCE_BUNDLE_CONTRACT,
    EVIDENCE_BUNDLE_SCHEMA_VERSION,
    EVIDENCE_STATUSES,
    EvidenceContractError,
    PATH_METRIC_ABS_TOLERANCE,
    PNL_ABS_TOLERANCE,
    RECORD_SPECS,
    REQUIRED_COMPACT_OUTPUTS,
    REQUIRED_DATASETS,
    validate_compact_output,
    validate_identity,
)


class EvidenceBundleError(RuntimeError):
    """Raised when an EvidenceBundle cannot be written, sealed, or verified."""


class EvidenceBundleBusy(EvidenceBundleError):
    """Raised when another process owns the campaign evidence writer lock."""


class IncompleteEvidenceBundle(EvidenceBundleError):
    """Raised when a campaign attempts to complete with missing raw evidence."""


@dataclass(frozen=True)
class EvidencePartReceipt:
    dataset: str
    relative_path: str
    part_index: int
    batch_id: str
    row_count: int
    payload_sha256: str
    file_sha256: str
    size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "relative_path": self.relative_path,
            "part_index": self.part_index,
            "batch_id": self.batch_id,
            "row_count": self.row_count,
            "payload_sha256": self.payload_sha256,
            "file_sha256": self.file_sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True)
class EvidenceBundleReceipt:
    campaign_id: str
    bundle_path: str
    manifest_path: str
    manifest_sha256: str
    bundle_content_sha256: str
    evidence_status: str
    reconstruction_flag: bool
    dataset_row_counts: dict[str, int]
    finalized_at_utc: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract": EVIDENCE_BUNDLE_CONTRACT,
            "schema_version": EVIDENCE_BUNDLE_SCHEMA_VERSION,
            "campaign_id": self.campaign_id,
            "bundle_path": self.bundle_path,
            "manifest_path": self.manifest_path,
            "manifest_sha256": self.manifest_sha256,
            "bundle_content_sha256": self.bundle_content_sha256,
            "evidence_status": self.evidence_status,
            "reconstruction_flag": self.reconstruction_flag,
            "dataset_row_counts": dict(self.dataset_row_counts),
            "finalized_at_utc": self.finalized_at_utc,
            "large_payloads_git_ignored": True,
        }


_PART_RE = re.compile(r"^part-(\d{6})\.jsonl\.gz$")
_ATOMIC_TEMP_RE = re.compile(
    r"^\.(?P<target>.+)\.tmp-(?P<pid>[1-9][0-9]*)-(?P<nonce>[0-9a-f]{32})$"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _close(left: Any, right: Any, *, tolerance: float) -> bool:
    return math.isclose(
        float(left),
        float(right),
        rel_tol=1e-12,
        abs_tol=tolerance,
    )


def _kahan_add(state: dict[str, Any], field: str, value: float) -> None:
    compensation_field = f"{field}_compensation"
    adjusted = value - float(state[compensation_field])
    updated = float(state[field]) + adjusted
    state[compensation_field] = (updated - float(state[field])) - adjusted
    state[field] = updated


def _kahan_add_mapping(
    state: dict[str, Any], field: str, key: str, value: float
) -> None:
    values = state[field]
    compensations = state[f"{field}_compensation"]
    current = float(values.get(key, 0.0))
    compensation = float(compensations.get(key, 0.0))
    adjusted = value - compensation
    updated = current + adjusted
    compensations[key] = (updated - current) - adjusted
    values[key] = updated


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_bytes(path: Path, raw: bytes, *, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_json(path: Path, value: Any) -> None:
    _atomic_bytes(path, _canonical_bytes(value))


def _gzip_json_lines(path: Path, values: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("wb") as raw_handle:
            with gzip.GzipFile(
                filename="",
                mode="wb",
                compresslevel=6,
                fileobj=raw_handle,
                mtime=0,
            ) as compressed:
                for value in values:
                    compressed.write(_canonical_bytes(value))
            raw_handle.flush()
            os.fsync(raw_handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EvidenceBundleError(f"invalid JSON evidence artifact: {path}") from exc


def _require_git_ignored_payload_root(path: Path) -> None:
    """Reject tracked payload roots while allowing non-Git temporary filesystems."""
    git_root: Path | None = None
    for parent in (path, *path.parents):
        if (parent / ".git").exists():
            git_root = parent
            break
    if git_root is None:
        return
    completed = subprocess.run(
        ["git", "-C", str(git_root), "check-ignore", "-q", "--no-index", str(path)],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if completed.returncode != 0:
        raise EvidenceBundleError(
            "large EvidenceBundle payload root must be ignored by Git: " + str(path)
        )


def _owned_staging_temp(path: Path, staging_dir: Path) -> bool:
    match = _ATOMIC_TEMP_RE.fullmatch(path.name)
    if match is None:
        return False
    target = match.group("target")
    relative_parent = path.parent.relative_to(staging_dir)
    if relative_parent == Path("."):
        return target in {
            "identity.json",
            "checkpoint.json",
            "evidence_bundle_manifest.json",
        }
    parts = relative_parent.parts
    if len(parts) == 2 and parts[0] == "datasets" and parts[1] in RECORD_SPECS:
        return _PART_RE.fullmatch(target) is not None
    if len(parts) == 1 and parts[0] == "outputs":
        return target in {f"{name}.json" for name in REQUIRED_COMPACT_OUTPUTS}
    return False


def _clean_owned_staging_temps(staging_dir: Path) -> None:
    changed_directories: set[Path] = set()
    for path in sorted(staging_dir.rglob("*")):
        if not _owned_staging_temp(path, staging_dir):
            continue
        mode = path.lstat().st_mode
        if path.is_symlink() or not stat.S_ISREG(mode):
            raise EvidenceBundleError(
                f"owned temporary evidence path is not a regular file: {path}"
            )
        path.unlink()
        changed_directories.add(path.parent)
    for directory in sorted(changed_directories):
        _fsync_directory(directory)


def _remove_stale_staging_manifest(staging_dir: Path) -> None:
    path = staging_dir / "evidence_bundle_manifest.json"
    if not path.exists() and not path.is_symlink():
        return
    mode = path.lstat().st_mode
    if path.is_symlink() or not stat.S_ISREG(mode):
        raise EvidenceBundleError(f"stale evidence manifest is not a regular file: {path}")
    path.unlink()
    _fsync_directory(staging_dir)


def _validate_staging_layout(staging_dir: Path, *, allow_manifest: bool) -> None:
    allowed_root_files = {"identity.json", "checkpoint.json"}
    if allow_manifest:
        allowed_root_files.add("evidence_bundle_manifest.json")
    for path in sorted(staging_dir.rglob("*")):
        relative = path.relative_to(staging_dir)
        if path.is_symlink():
            raise EvidenceBundleError(f"symlinks are forbidden in EvidenceBundle staging: {relative}")
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            parts = relative.parts
            allowed = (
                parts in {("datasets",), ("outputs",)}
                or (
                    len(parts) == 2
                    and parts[0] == "datasets"
                    and parts[1] in RECORD_SPECS
                )
            )
        elif stat.S_ISREG(mode):
            parts = relative.parts
            allowed = (
                (len(parts) == 1 and parts[0] in allowed_root_files)
                or (
                    len(parts) == 3
                    and parts[0] == "datasets"
                    and parts[1] in RECORD_SPECS
                    and _PART_RE.fullmatch(parts[2]) is not None
                )
                or (
                    len(parts) == 2
                    and parts[0] == "outputs"
                    and parts[1]
                    in {f"{name}.json" for name in REQUIRED_COMPACT_OUTPUTS}
                )
            )
        else:
            allowed = False
        if not allowed:
            raise EvidenceBundleError(
                f"unexpected or unsafe EvidenceBundle staging path: {relative}"
            )


def _part_sort_key(dataset: str, row: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(str(row[field]) for field in RECORD_SPECS[dataset].sort_fields)


def _read_part(
    path: Path,
    *,
    expected_dataset: str | None = None,
    expected_campaign_id: str | None = None,
    validate_rows: bool = True,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            first = handle.readline()
            if not first:
                raise EvidenceBundleError(f"empty evidence partition: {path}")
            header_envelope = json.loads(first)
            header = header_envelope.get("_evidence_part")
            if not isinstance(header, dict):
                raise EvidenceBundleError(f"missing evidence partition envelope: {path}")
            dataset = str(header.get("dataset") or "")
            if dataset not in RECORD_SPECS:
                raise EvidenceBundleError(f"unknown partition dataset in {path}")
            if expected_dataset is not None and dataset != expected_dataset:
                raise EvidenceBundleError(f"partition dataset mismatch in {path}")
            rows = [json.loads(line) for line in handle if line.strip()]
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceBundleError(f"invalid compressed evidence partition: {path}") from exc

    if header.get("contract") != EVIDENCE_BUNDLE_CONTRACT:
        raise EvidenceBundleError(f"partition contract mismatch in {path}")
    if int(header.get("schema_version", -1)) != EVIDENCE_BUNDLE_SCHEMA_VERSION:
        raise EvidenceBundleError(f"partition schema version mismatch in {path}")
    if header.get("sort_fields") != list(RECORD_SPECS[dataset].sort_fields):
        raise EvidenceBundleError(f"partition sort contract mismatch in {path}")
    if int(header.get("row_count", -1)) != len(rows):
        raise EvidenceBundleError(f"partition row count mismatch in {path}")
    payload_sha256 = hashlib.sha256(b"".join(_canonical_bytes(row) for row in rows)).hexdigest()
    if header.get("payload_sha256") != payload_sha256:
        raise EvidenceBundleError(f"partition payload checksum mismatch in {path}")
    if rows != sorted(rows, key=lambda row: _part_sort_key(dataset, row)):
        raise EvidenceBundleError(f"partition is not deterministically sorted: {path}")
    if validate_rows:
        campaign_id = expected_campaign_id or str(header.get("campaign_id") or "")
        for row in rows:
            RECORD_SPECS[dataset].validate(row, campaign_id=campaign_id)
    return dict(header), [dict(row) for row in rows]


class EvidenceBundleWriter:
    """Single-process writer for a resumable and atomically sealed evidence bundle."""

    def __init__(
        self,
        *,
        base_dir: Path,
        identity: Mapping[str, Any],
        staging_dir: Path,
        final_dir: Path,
        lock_path: Path,
        lock_descriptor: int,
        state: Mapping[str, Any],
    ) -> None:
        self.base_dir = base_dir
        self.identity = dict(identity)
        self.campaign_id = str(identity["campaign_id"])
        self.staging_dir = staging_dir
        self.final_dir = final_dir
        self.lock_path = lock_path
        self._lock_descriptor = lock_descriptor
        self._state = dict(state)
        self._closed = False

    @classmethod
    def create(
        cls,
        base_dir: str | Path,
        identity: Mapping[str, Any],
        *,
        writer_id: str | None = None,
        require_git_ignored: bool = True,
    ) -> "EvidenceBundleWriter":
        checked_identity = validate_identity(identity)
        base = Path(base_dir).resolve()
        base.mkdir(parents=True, exist_ok=True)
        if not require_git_ignored:
            raise EvidenceContractError(
                "Git-ignore enforcement may not be disabled for EvidenceBundle v1"
            )
        _require_git_ignored_payload_root(base)
        campaign_id = str(checked_identity["campaign_id"])
        staging = base / f".{campaign_id}.evidence-v1.staging"
        final = base / f"{campaign_id}.evidence-v1"
        lock = base / f".{campaign_id}.evidence-v1.lock"
        lock_descriptor = cls._acquire_lock(lock)
        try:
            if final.exists():
                raise EvidenceBundleError(f"sealed EvidenceBundle already exists: {final}")
            if staging.exists():
                raise EvidenceBundleError(
                    f"staging EvidenceBundle already exists; call resume(): {staging}"
                )
            staging.mkdir(mode=0o700)
            (staging / "datasets").mkdir()
            (staging / "outputs").mkdir()
            _atomic_json(staging / "identity.json", checked_identity)
            now = _utc_now()
            state = {
                "contract": EVIDENCE_BUNDLE_CONTRACT,
                "schema_version": EVIDENCE_BUNDLE_SCHEMA_VERSION,
                "campaign_id": campaign_id,
                "state": "STAGING",
                "writer_id": writer_id or uuid.uuid4().hex,
                "created_at_utc": now,
                "updated_at_utc": now,
                "checkpoint_sequence": 0,
                "dataset_parts": {name: [] for name in REQUIRED_DATASETS},
                "dataset_row_counts": {name: 0 for name in REQUIRED_DATASETS},
                "compact_outputs": {},
                "checkpoint_metadata": {},
            }
            _atomic_json(staging / "checkpoint.json", state)
            _fsync_directory(staging)
            return cls(
                base_dir=base,
                identity=checked_identity,
                staging_dir=staging,
                final_dir=final,
                lock_path=lock,
                lock_descriptor=lock_descriptor,
                state=state,
            )
        except Exception:
            cls._release_descriptor(lock_descriptor)
            raise

    @classmethod
    def resume(
        cls,
        base_dir: str | Path,
        campaign_id: str,
        *,
        writer_id: str | None = None,
        expected_identity: Mapping[str, Any] | None = None,
    ) -> "EvidenceBundleWriter":
        if not campaign_id or Path(campaign_id).name != campaign_id:
            raise EvidenceBundleError("unsafe campaign_id")
        base = Path(base_dir).resolve()
        _require_git_ignored_payload_root(base)
        staging = base / f".{campaign_id}.evidence-v1.staging"
        final = base / f"{campaign_id}.evidence-v1"
        lock = base / f".{campaign_id}.evidence-v1.lock"
        lock_descriptor = cls._acquire_lock(lock)
        try:
            if final.exists():
                raise EvidenceBundleError(f"EvidenceBundle is already sealed: {final}")
            if not staging.is_dir():
                raise EvidenceBundleError(f"no staging EvidenceBundle to resume: {staging}")
            identity = validate_identity(_load_json(staging / "identity.json"))
            if identity["campaign_id"] != campaign_id:
                raise EvidenceBundleError("staging identity campaign mismatch")
            if expected_identity is not None:
                checked_expected = validate_identity(expected_identity)
                if _canonical_bytes(identity) != _canonical_bytes(checked_expected):
                    raise EvidenceBundleError(
                        "staging identity disagrees with expected resume identity"
                    )
            prior_state = _load_json(staging / "checkpoint.json")
            if (
                prior_state.get("contract") != EVIDENCE_BUNDLE_CONTRACT
                or int(prior_state.get("schema_version", -1))
                != EVIDENCE_BUNDLE_SCHEMA_VERSION
            ):
                raise EvidenceBundleError("staging checkpoint contract mismatch")
            if writer_id is not None and prior_state.get("writer_id") != writer_id:
                raise EvidenceBundleError("writer_id does not own the resumable staging bundle")
            if prior_state.get("state") in {"FINALIZING", "COMPLETE"}:
                prior_state["state"] = "STAGING"
            writer = cls(
                base_dir=base,
                identity=identity,
                staging_dir=staging,
                final_dir=final,
                lock_path=lock,
                lock_descriptor=lock_descriptor,
                state=prior_state,
            )
            writer._reconcile_staging()
            return writer
        except Exception:
            cls._release_descriptor(lock_descriptor)
            raise

    @staticmethod
    def _acquire_lock(path: Path) -> int:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise EvidenceBundleBusy(f"EvidenceBundle writer lock is held: {path}") from exc
        metadata = _canonical_bytes({"pid": os.getpid(), "acquired_at_utc": _utc_now()})
        os.ftruncate(descriptor, 0)
        os.write(descriptor, metadata)
        os.fsync(descriptor)
        return descriptor

    @staticmethod
    def _release_descriptor(descriptor: int) -> None:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    @property
    def writer_id(self) -> str:
        return str(self._state["writer_id"])

    @property
    def dataset_row_counts(self) -> dict[str, int]:
        return {
            name: int(count)
            for name, count in self._state["dataset_row_counts"].items()
        }

    def __enter__(self) -> "EvidenceBundleWriter":
        self._ensure_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if not self._closed:
            if exc_type is None:
                self.checkpoint()
            self.close()

    def close(self) -> None:
        if not self._closed:
            self._release_descriptor(self._lock_descriptor)
            self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise EvidenceBundleError("EvidenceBundle writer is closed")
        if self._state.get("state") != "STAGING":
            raise EvidenceBundleError("EvidenceBundle is not in STAGING state")

    def append_records(
        self,
        dataset: str,
        records: Iterable[Mapping[str, Any]],
        *,
        batch_id: str | None = None,
    ) -> EvidencePartReceipt:
        self._ensure_open()
        if dataset not in RECORD_SPECS:
            raise EvidenceContractError(f"unknown evidence dataset: {dataset}")
        checked = [
            RECORD_SPECS[dataset].validate(row, campaign_id=self.campaign_id)
            for row in records
        ]
        if not checked:
            raise EvidenceContractError("evidence batches may not be empty")
        checked.sort(key=lambda row: _part_sort_key(dataset, row))
        resolved_batch_id = batch_id or uuid.uuid4().hex
        if not isinstance(resolved_batch_id, str) or not resolved_batch_id or len(resolved_batch_id) > 256:
            raise EvidenceContractError("batch_id must be a non-empty string of at most 256 characters")
        payload_sha256 = hashlib.sha256(
            b"".join(_canonical_bytes(row) for row in checked)
        ).hexdigest()
        for prior in self._state["dataset_parts"][dataset]:
            if prior["batch_id"] == resolved_batch_id:
                if prior["payload_sha256"] != payload_sha256:
                    raise EvidenceBundleError(
                        f"batch_id {resolved_batch_id!r} was already committed with different evidence"
                    )
                return EvidencePartReceipt(**prior)

        part_index = len(self._state["dataset_parts"][dataset])
        relative_path = f"datasets/{dataset}/part-{part_index:06d}.jsonl.gz"
        destination = self.staging_dir / relative_path
        header = {
            "_evidence_part": {
                "contract": EVIDENCE_BUNDLE_CONTRACT,
                "schema_version": EVIDENCE_BUNDLE_SCHEMA_VERSION,
                "campaign_id": self.campaign_id,
                "dataset": dataset,
                "part_index": part_index,
                "batch_id": resolved_batch_id,
                "row_count": len(checked),
                "payload_sha256": payload_sha256,
                "sort_fields": list(RECORD_SPECS[dataset].sort_fields),
            }
        }
        _gzip_json_lines(destination, [header, *checked])
        receipt = EvidencePartReceipt(
            dataset=dataset,
            relative_path=relative_path,
            part_index=part_index,
            batch_id=resolved_batch_id,
            row_count=len(checked),
            payload_sha256=payload_sha256,
            file_sha256=_sha256(destination),
            size_bytes=destination.stat().st_size,
        )
        self._state["dataset_parts"][dataset].append(receipt.to_dict())
        self._state["dataset_row_counts"][dataset] += len(checked)
        self._persist_checkpoint()
        return receipt

    def write_compact_output(self, name: str, value: Any) -> str:
        self._ensure_open()
        validate_compact_output(name, value)
        relative_path = f"outputs/{name}.json"
        destination = self.staging_dir / relative_path
        raw = _canonical_bytes(value)
        digest = hashlib.sha256(raw).hexdigest()
        prior = self._state["compact_outputs"].get(name)
        if prior is not None:
            if prior["sha256"] != digest:
                raise EvidenceBundleError(
                    f"compact output {name} is immutable once written"
                )
            return digest
        _atomic_bytes(destination, raw)
        self._state["compact_outputs"][name] = {
            "relative_path": relative_path,
            "sha256": digest,
            "size_bytes": len(raw),
        }
        self._persist_checkpoint()
        return digest

    def checkpoint(self, metadata: Mapping[str, Any] | None = None) -> dict[str, Any]:
        self._ensure_open()
        if metadata is not None:
            try:
                json.dumps(metadata, allow_nan=False)
            except (TypeError, ValueError) as exc:
                raise EvidenceContractError("checkpoint metadata must be JSON") from exc
            self._state["checkpoint_metadata"] = dict(metadata)
        self._persist_checkpoint()
        return dict(self._state)

    def _persist_checkpoint(self) -> None:
        self._state["checkpoint_sequence"] = int(
            self._state.get("checkpoint_sequence", 0)
        ) + 1
        self._state["updated_at_utc"] = _utc_now()
        _atomic_json(self.staging_dir / "checkpoint.json", self._state)

    def _reconcile_staging(self) -> None:
        self._ensure_open()
        _clean_owned_staging_temps(self.staging_dir)
        _remove_stale_staging_manifest(self.staging_dir)
        _validate_staging_layout(self.staging_dir, allow_manifest=False)
        rebuilt_parts: dict[str, list[dict[str, Any]]] = {
            name: [] for name in REQUIRED_DATASETS
        }
        rebuilt_counts = {name: 0 for name in REQUIRED_DATASETS}
        seen_batches: set[tuple[str, str]] = set()
        for dataset in REQUIRED_DATASETS:
            directory = self.staging_dir / "datasets" / dataset
            if not directory.exists():
                continue
            paths = sorted(directory.glob("part-*.jsonl.gz"))
            for expected_index, path in enumerate(paths):
                match = _PART_RE.fullmatch(path.name)
                if match is None or int(match.group(1)) != expected_index:
                    raise EvidenceBundleError(
                        f"non-contiguous staging partitions for {dataset}"
                    )
                header, rows = _read_part(
                    path,
                    expected_dataset=dataset,
                    expected_campaign_id=self.campaign_id,
                )
                if int(header.get("part_index", -1)) != expected_index:
                    raise EvidenceBundleError(f"partition index mismatch in {path}")
                batch_key = (dataset, str(header.get("batch_id") or ""))
                if not batch_key[1] or batch_key in seen_batches:
                    raise EvidenceBundleError(f"duplicate or empty staging batch ID in {path}")
                seen_batches.add(batch_key)
                receipt = EvidencePartReceipt(
                    dataset=dataset,
                    relative_path=path.relative_to(self.staging_dir).as_posix(),
                    part_index=expected_index,
                    batch_id=batch_key[1],
                    row_count=len(rows),
                    payload_sha256=str(header["payload_sha256"]),
                    file_sha256=_sha256(path),
                    size_bytes=path.stat().st_size,
                )
                rebuilt_parts[dataset].append(receipt.to_dict())
                rebuilt_counts[dataset] += len(rows)

        rebuilt_outputs: dict[str, dict[str, Any]] = {}
        for name in REQUIRED_COMPACT_OUTPUTS:
            path = self.staging_dir / "outputs" / f"{name}.json"
            if path.exists():
                value = _load_json(path)
                validate_compact_output(name, value)
                rebuilt_outputs[name] = {
                    "relative_path": path.relative_to(self.staging_dir).as_posix(),
                    "sha256": _sha256(path),
                    "size_bytes": path.stat().st_size,
                }
        self._state["dataset_parts"] = rebuilt_parts
        self._state["dataset_row_counts"] = rebuilt_counts
        self._state["compact_outputs"] = rebuilt_outputs
        self._persist_checkpoint()
        _validate_staging_layout(self.staging_dir, allow_manifest=False)

    def _iter_staging_records(self, dataset: str) -> Iterator[dict[str, Any]]:
        for part in self._state["dataset_parts"][dataset]:
            _, rows = _read_part(
                self.staging_dir / part["relative_path"],
                expected_dataset=dataset,
                expected_campaign_id=self.campaign_id,
            )
            yield from rows

    def _validate_completeness(self) -> bool:
        missing_datasets = [
            name
            for name in REQUIRED_DATASETS
            if int(self._state["dataset_row_counts"].get(name, 0)) <= 0
        ]
        if missing_datasets:
            raise IncompleteEvidenceBundle(
                "summary-only completion forbidden; missing non-empty ledgers: "
                + ", ".join(missing_datasets)
            )
        missing_outputs = [
            name
            for name in REQUIRED_COMPACT_OUTPUTS
            if name not in self._state["compact_outputs"]
        ]
        if missing_outputs:
            raise IncompleteEvidenceBundle(
                "campaign completion missing automatic outputs: "
                + ", ".join(missing_outputs)
            )
        return _validate_relational_contract(
            identity=self.identity,
            records={name: self._iter_staging_records(name) for name in REQUIRED_DATASETS},
        )

    def finalize(
        self,
        *,
        evidence_status: str,
        lightweight_manifest_path: str | Path,
        read_only: bool = True,
    ) -> EvidenceBundleReceipt:
        self._ensure_open()
        if evidence_status not in EVIDENCE_STATUSES:
            raise EvidenceContractError(
                "evidence_status must be FRESH_DEVELOPMENT_EVIDENCE or "
                "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION"
            )
        _require_git_ignored_payload_root(self.base_dir)
        if not read_only:
            raise EvidenceContractError(
                "read-only finalization may not be disabled for EvidenceBundle v1"
            )
        lightweight = Path(lightweight_manifest_path)
        resolved_lightweight = lightweight.resolve()
        if (
            resolved_lightweight == self.staging_dir
            or self.staging_dir in resolved_lightweight.parents
            or resolved_lightweight == self.final_dir
            or self.final_dir in resolved_lightweight.parents
        ):
            raise EvidenceContractError(
                "lightweight receipt must live outside the EvidenceBundle directory"
            )
        self._reconcile_staging()
        reconstruction_flag = self._validate_completeness()
        if reconstruction_flag and evidence_status != "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION":
            raise EvidenceContractError(
                "reconstructed evidence must be sealed as "
                "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION"
            )
        if not reconstruction_flag and evidence_status == "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION":
            raise EvidenceContractError(
                "fresh evidence may not claim reconstruction provenance"
            )
        finalized_at = _utc_now()
        self._state["state"] = "COMPLETE"
        self._state["finalized_at_utc"] = finalized_at
        _atomic_json(self.staging_dir / "checkpoint.json", self._state)

        files: dict[str, dict[str, Any]] = {}
        for relative_path in ("identity.json", "checkpoint.json"):
            path = self.staging_dir / relative_path
            files[relative_path] = {
                "kind": "identity" if relative_path == "identity.json" else "checkpoint",
                "sha256": _sha256(path),
                "size_bytes": path.stat().st_size,
            }
        for dataset, parts in self._state["dataset_parts"].items():
            for part in parts:
                files[part["relative_path"]] = {
                    "kind": "dataset_partition",
                    "dataset": dataset,
                    "part_index": part["part_index"],
                    "batch_id": part["batch_id"],
                    "row_count": part["row_count"],
                    "payload_sha256": part["payload_sha256"],
                    "sha256": part["file_sha256"],
                    "size_bytes": part["size_bytes"],
                    "encoding": "canonical-jsonl+gzip-mtime-zero",
                }
        for name, output in self._state["compact_outputs"].items():
            files[output["relative_path"]] = {
                "kind": "compact_output",
                "output": name,
                "sha256": output["sha256"],
                "size_bytes": output["size_bytes"],
            }

        manifest_core = {
            "contract": EVIDENCE_BUNDLE_CONTRACT,
            "schema_version": EVIDENCE_BUNDLE_SCHEMA_VERSION,
            "status": "COMPLETE",
            "evidence_status": evidence_status,
            "campaign_id": self.campaign_id,
            "identity_sha256": _sha256(self.staging_dir / "identity.json"),
            "reconstruction_flag": reconstruction_flag,
            "created_at_utc": self._state["created_at_utc"],
            "finalized_at_utc": finalized_at,
            "dataset_row_counts": dict(self._state["dataset_row_counts"]),
            "reconciliation_tolerances": {
                "pnl_absolute": PNL_ABS_TOLERANCE,
                "path_metric_absolute": PATH_METRIC_ABS_TOLERANCE,
                "relative": 1e-12,
            },
            "datasets": {
                name: {
                    "row_count": int(self._state["dataset_row_counts"][name]),
                    "partition_count": len(self._state["dataset_parts"][name]),
                    "sort_fields": list(RECORD_SPECS[name].sort_fields),
                    "logical_order": "merge_partitions_by_sort_fields",
                }
                for name in REQUIRED_DATASETS
            },
            "compact_outputs": dict(self._state["compact_outputs"]),
            "files": files,
        }
        manifest = dict(manifest_core)
        manifest["bundle_content_sha256"] = _canonical_hash(manifest_core)
        manifest_path = self.staging_dir / "evidence_bundle_manifest.json"
        _atomic_json(manifest_path, manifest)
        manifest_sha256 = _sha256(manifest_path)

        receipt = EvidenceBundleReceipt(
            campaign_id=self.campaign_id,
            bundle_path=str(self.final_dir),
            manifest_path=str(self.final_dir / manifest_path.name),
            manifest_sha256=manifest_sha256,
            bundle_content_sha256=manifest["bundle_content_sha256"],
            evidence_status=evidence_status,
            reconstruction_flag=reconstruction_flag,
            dataset_row_counts={
                name: int(value)
                for name, value in self._state["dataset_row_counts"].items()
            },
            finalized_at_utc=finalized_at,
        )
        lightweight.parent.mkdir(parents=True, exist_ok=True)
        try:
            _validate_staging_layout(self.staging_dir, allow_manifest=True)
            verify_evidence_bundle(self.staging_dir, deep=False)
            _fsync_directory(self.staging_dir)
            os.replace(self.staging_dir, self.final_dir)
            _fsync_directory(self.base_dir)
            _make_bundle_read_only(self.final_dir)
            verify_evidence_bundle(self.final_dir, deep=False)
            _project_receipt(lightweight, receipt)
        finally:
            self.close()
        return receipt


def _validate_relational_contract(
    *,
    identity: Mapping[str, Any],
    records: Mapping[str, Iterable[Mapping[str, Any]]],
) -> bool:
    policy_ids = set(identity["policy_fingerprints"])
    component_ids = set(identity["component_fingerprints"])
    coverage = identity["expected_coverage"]
    required_episode_keys = {
        (
            str(row["policy_id"]),
            str(row["episode_id"]),
            str(row["horizon"]),
        )
        for row in coverage["required_episode_keys"]
    }
    allowed_horizons = {str(value) for value in coverage["allowed_horizons"]}
    membership_pairs: set[tuple[str, str]] = set()
    policies_with_membership: set[str] = set()
    components_with_membership: set[str] = set()
    membership_components_by_policy: dict[str, set[str]] = {}
    for row in records["account_policy_membership"]:
        pair = (str(row["policy_id"]), str(row["component_id"]))
        if pair in membership_pairs:
            raise IncompleteEvidenceBundle(f"duplicate account membership: {pair}")
        membership_pairs.add(pair)
        if pair[0] not in policy_ids or pair[1] not in component_ids:
            raise IncompleteEvidenceBundle("membership references an unknown immutable fingerprint")
        policies_with_membership.add(pair[0])
        components_with_membership.add(pair[1])
        membership_components_by_policy.setdefault(pair[0], set()).add(pair[1])
    missing_policy_definitions = policy_ids - policies_with_membership
    if missing_policy_definitions:
        raise IncompleteEvidenceBundle(
            "policies lack executable membership evidence: "
            + ", ".join(sorted(missing_policy_definitions)[:10])
        )
    missing_component_membership = component_ids - components_with_membership
    if missing_component_membership:
        raise IncompleteEvidenceBundle(
            "components lack account-policy membership evidence: "
            + ", ".join(sorted(missing_component_membership)[:10])
        )

    component_trade_rows: dict[str, dict[tuple[str, str], Mapping[str, Any]]] = {
        "component_entries": {},
        "component_exits": {},
        "component_trades": {},
    }
    signal_rows: dict[tuple[str, str], Mapping[str, Any]] = {}
    components_with_signals: set[str] = set()
    for row in records["component_signals"]:
        component_id = str(row["component_id"])
        if component_id not in component_ids:
            raise IncompleteEvidenceBundle("signal references an unknown component")
        key = (component_id, str(row["signal_id"]))
        if key in signal_rows:
            raise IncompleteEvidenceBundle(f"duplicate component signal: {key}")
        signal_rows[key] = row
        components_with_signals.add(component_id)
    missing_component_signals = component_ids - components_with_signals
    if missing_component_signals:
        raise IncompleteEvidenceBundle(
            "components lack signal evidence: "
            + ", ".join(sorted(missing_component_signals)[:10])
        )
    for dataset in component_trade_rows:
        for row in records[dataset]:
            component_id = str(row["component_id"])
            if component_id not in component_ids:
                raise IncompleteEvidenceBundle(f"{dataset} references an unknown component")
            key = (component_id, str(row["trade_id"]))
            if key in component_trade_rows[dataset]:
                raise IncompleteEvidenceBundle(f"duplicate {dataset} trade key: {key}")
            component_trade_rows[dataset][key] = row
    entry_keys = set(component_trade_rows["component_entries"])
    exit_keys = set(component_trade_rows["component_exits"])
    trade_keys = set(component_trade_rows["component_trades"])
    if exit_keys != trade_keys or not trade_keys <= entry_keys:
        raise IncompleteEvidenceBundle(
            "exit and chronological trade ledgers must reconcile exactly and "
            "every trade must have an entry"
        )
    orphan_entry_keys = entry_keys - trade_keys
    for key in orphan_entry_keys:
        entry = component_trade_rows["component_entries"][key]
        signal = signal_rows.get(key)
        if signal is None:
            raise IncompleteEvidenceBundle(
                f"orphan component entry has no corresponding signal: {key}"
            )
        # A causal position may be filled before the immutable input range
        # ends and then lack a future exit bar.  Preserve that real entry, but
        # permit no other relaxation of the entry/exit/trade invariant.
        if (
            signal.get("outcome_status") != "CENSORED_FUTURE_COVERAGE"
            or signal.get("fill_time") in {None, ""}
            or signal.get("trade_materialized") is not False
            or entry.get("outcome_status") != "CENSORED_FUTURE_COVERAGE"
            or entry.get("trade_materialized") is not False
            or entry.get("open_position_unresolved") is not True
            or entry.get("entry_time") != signal.get("fill_time")
        ):
            raise IncompleteEvidenceBundle(
                "entry without exit/trade is allowed only for an exactly linked "
                f"filled causal censor: {key}"
            )
    for dataset, keyed_rows in component_trade_rows.items():
        represented_components = {key[0] for key in keyed_rows}
        missing_components = component_ids - represented_components
        if missing_components:
            raise IncompleteEvidenceBundle(
                f"components lack {dataset} evidence: "
                + ", ".join(sorted(missing_components)[:10])
            )
    for key, trade in component_trade_rows["component_trades"].items():
        entry = component_trade_rows["component_entries"][key]
        exit_row = component_trade_rows["component_exits"][key]
        exact_entry_fields = {
            "entry_time": "entry_time",
            "market": "market",
            "contract": "contract",
            "side": "side",
        }
        for entry_field, trade_field in exact_entry_fields.items():
            if entry[entry_field] != trade[trade_field]:
                raise IncompleteEvidenceBundle(
                    f"trade {key} disagrees with entry field {entry_field}"
                )
        if exit_row["exit_time"] != trade["exit_time"]:
            raise IncompleteEvidenceBundle(
                f"trade {key} disagrees with exit field exit_time"
            )
        for entry_field, trade_field in (
            ("quantity", "quantity"),
            ("entry_price", "entry_price"),
        ):
            if not _close(
                entry[entry_field],
                trade[trade_field],
                tolerance=PATH_METRIC_ABS_TOLERANCE,
            ):
                raise IncompleteEvidenceBundle(
                    f"trade {key} disagrees with entry field {entry_field}"
                )
        if not _close(
            exit_row["exit_price"],
            trade["exit_price"],
            tolerance=PATH_METRIC_ABS_TOLERANCE,
        ):
            raise IncompleteEvidenceBundle(
                f"trade {key} disagrees with exit field exit_price"
            )
        if float(trade["quantity"]) <= 0.0:
            raise IncompleteEvidenceBundle(f"trade {key} has non-positive quantity")
        if float(trade["costs"]) < 0.0:
            raise IncompleteEvidenceBundle(f"trade {key} has negative costs")
        if not _close(
            float(trade["gross_pnl"]) - float(trade["costs"]),
            trade["net_pnl"],
            tolerance=PNL_ABS_TOLERANCE,
        ):
            raise IncompleteEvidenceBundle(
                f"trade {key} violates gross minus costs equals net"
            )
        entry_time = datetime.fromisoformat(
            str(trade["entry_time"]).replace("Z", "+00:00")
        )
        exit_time = datetime.fromisoformat(
            str(trade["exit_time"]).replace("Z", "+00:00")
        )
        if exit_time < entry_time:
            raise IncompleteEvidenceBundle(f"trade {key} exits before entry")

    episode_rows: dict[tuple[str, str, str, str], Mapping[str, Any]] = {}
    scenario_coverage: dict[tuple[str, str, str], set[str]] = {}
    policies_with_episodes: set[str] = set()
    for row in records["episodes"]:
        policy_id = str(row["policy_id"])
        if policy_id not in policy_ids:
            raise IncompleteEvidenceBundle("episode references an unknown policy")
        scenario = str(row["cost_scenario"])
        horizon = str(row["horizon"])
        if horizon not in allowed_horizons:
            raise IncompleteEvidenceBundle(
                f"episode references a horizon outside expected coverage: {horizon}"
            )
        key = (policy_id, str(row["episode_id"]), horizon, scenario)
        if key in episode_rows:
            raise IncompleteEvidenceBundle(f"duplicate episode result: {key}")
        episode_rows[key] = row
        scenario_coverage.setdefault(key[:3], set()).add(scenario)
        policies_with_episodes.add(policy_id)
    missing_episode_policies = policy_ids - policies_with_episodes
    if missing_episode_policies:
        raise IncompleteEvidenceBundle(
            "policies lack episode evidence: "
            + ", ".join(sorted(missing_episode_policies)[:10])
        )
    observed_episode_keys = set(scenario_coverage)
    missing_required_episode_keys = required_episode_keys - observed_episode_keys
    if missing_required_episode_keys:
        raise IncompleteEvidenceBundle(
            f"{len(missing_required_episode_keys)} required base episode keys are missing"
        )
    if not bool(coverage["allow_additional_episode_keys"]):
        unexpected_episode_keys = observed_episode_keys - required_episode_keys
        if unexpected_episode_keys:
            raise IncompleteEvidenceBundle(
                f"{len(unexpected_episode_keys)} undeclared episode keys are present"
            )
    for key, scenarios in scenario_coverage.items():
        if scenarios != set(COST_SCENARIOS):
            raise IncompleteEvidenceBundle(
                f"episode {key} must have exactly NORMAL and STRESSED_1_5X evidence"
            )
        normal = episode_rows[(*key, "NORMAL")]
        stressed = episode_rows[(*key, "STRESSED_1_5X")]
        for field in ("episode_start", "temporal_block"):
            if normal[field] != stressed[field]:
                raise IncompleteEvidenceBundle(
                    f"episode {key} cost scenarios disagree on {field}"
                )

    account_path_keys: set[tuple[str, str, str, str, str]] = set()
    covered_episode_keys: set[tuple[str, str, str, str]] = set()
    path_aggregates: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in records["account_daily_paths"]:
        policy_id = str(row["policy_id"])
        if not _close(
            float(row["equity"]) - float(row["mll"]),
            row["mll_buffer"],
            tolerance=PNL_ABS_TOLERANCE,
        ):
            raise IncompleteEvidenceBundle(
                "account path mll_buffer disagrees with equity minus mll"
            )
        attribution_components = set(row["component_attribution"])
        unknown_attribution = attribution_components - component_ids
        if unknown_attribution:
            raise IncompleteEvidenceBundle(
                "account path attribution references an unknown component"
            )
        nonmember_attribution = attribution_components - membership_components_by_policy.get(
            policy_id, set()
        )
        if nonmember_attribution:
            raise IncompleteEvidenceBundle(
                "account path attribution references a component outside policy membership"
            )
        episode_key = (
            policy_id,
            str(row["episode_id"]),
            str(row["horizon"]),
            str(row["cost_scenario"]),
        )
        if episode_key not in episode_rows:
            raise IncompleteEvidenceBundle("account path has no matching episode result")
        path_key = (*episode_key, str(row["trading_day"]))
        if path_key in account_path_keys:
            raise IncompleteEvidenceBundle(f"duplicate daily account path: {path_key}")
        account_path_keys.add(path_key)
        covered_episode_keys.add(episode_key)
        aggregate = path_aggregates.setdefault(
            episode_key,
            {
                "row_count": 0,
                "daily_pnl": 0.0,
                "daily_pnl_compensation": 0.0,
                "costs": 0.0,
                "costs_compensation": 0.0,
                "component_attribution": {},
                "component_attribution_compensation": {},
                "minimum_mll_buffer": math.inf,
                "final_trading_day": "",
                "final_row": None,
            },
        )
        aggregate["row_count"] += 1
        _kahan_add(aggregate, "daily_pnl", float(row["daily_pnl"]))
        _kahan_add(aggregate, "costs", float(row["costs"]))
        for component_id, value in row["component_attribution"].items():
            _kahan_add_mapping(
                aggregate,
                "component_attribution",
                str(component_id),
                float(value),
            )
        aggregate["minimum_mll_buffer"] = min(
            float(aggregate["minimum_mll_buffer"]),
            float(row["minimum_mll_buffer"]),
        )
        trading_day = str(row["trading_day"])
        if trading_day > str(aggregate["final_trading_day"]):
            aggregate["final_trading_day"] = trading_day
            aggregate["final_row"] = row
    missing_paths = set(episode_rows) - covered_episode_keys
    if missing_paths:
        raise IncompleteEvidenceBundle(
            f"{len(missing_paths)} episode results lack daily account paths"
        )
    for key, episode in episode_rows.items():
        aggregate = path_aggregates[key]
        final_row = aggregate["final_row"]
        if final_row is None:
            raise IncompleteEvidenceBundle(f"episode {key} has no terminal account path")
        if int(aggregate["row_count"]) != int(episode["duration_trading_days"]):
            raise IncompleteEvidenceBundle(
                f"episode {key} daily path length disagrees with duration"
            )
        if not _close(
            aggregate["daily_pnl"],
            episode["net_pnl"],
            tolerance=PNL_ABS_TOLERANCE,
        ):
            raise IncompleteEvidenceBundle(
                f"episode {key} cumulative daily PnL disagrees with net_pnl"
            )
        if not _close(
            float(final_row["realized_pnl"]) + float(final_row["unrealized_pnl"]),
            episode["net_pnl"],
            tolerance=PNL_ABS_TOLERANCE,
        ):
            raise IncompleteEvidenceBundle(
                f"episode {key} terminal realized/unrealized PnL disagrees with net_pnl"
            )
        if not _close(
            aggregate["costs"],
            episode["costs"],
            tolerance=PNL_ABS_TOLERANCE,
        ):
            raise IncompleteEvidenceBundle(
                f"episode {key} cumulative costs disagree with summary"
            )
        if not _close(
            final_row["target_progress"],
            episode["target_progress"],
            tolerance=PATH_METRIC_ABS_TOLERANCE,
        ):
            raise IncompleteEvidenceBundle(
                f"episode {key} terminal target progress disagrees with summary"
            )
        if not _close(
            aggregate["minimum_mll_buffer"],
            episode["minimum_mll_buffer"],
            tolerance=PNL_ABS_TOLERANCE,
        ):
            raise IncompleteEvidenceBundle(
                f"episode {key} minimum MLL buffer disagrees with path"
            )
        if bool(final_row["consistency_ok"]) != bool(episode["consistency_ok"]):
            raise IncompleteEvidenceBundle(
                f"episode {key} terminal consistency disagrees with summary"
            )
        if not _close(
            math.fsum(
                float(value)
                for value in aggregate["component_attribution"].values()
            ),
            episode["net_pnl"],
            tolerance=PNL_ABS_TOLERANCE,
        ):
            raise IncompleteEvidenceBundle(
                f"episode {key} cumulative component attribution disagrees with net_pnl"
            )
        episode_contribution = episode.get("component_contribution")
        if isinstance(episode_contribution, Mapping):
            observed_contribution = aggregate["component_attribution"]
            for component_id in set(episode_contribution) | set(observed_contribution):
                if not _close(
                    observed_contribution.get(component_id, 0.0),
                    episode_contribution.get(component_id, 0.0),
                    tolerance=PNL_ABS_TOLERANCE,
                ):
                    raise IncompleteEvidenceBundle(
                        f"episode {key} cumulative component attribution "
                        "disagrees with episode contribution"
                    )
        _validate_terminal_economics(key, episode)

    reconstruction_flags: set[bool] = set()
    required_provenance_checksums = {
        "configuration": str(identity["configuration_sha256"]),
        **{
            f"data:{name}": str(digest)
            for name, digest in identity["data_fingerprints"].items()
        },
    }
    for row in records["provenance"]:
        immutable_checksums = row["immutable_checksums"]
        for name, expected_digest in required_provenance_checksums.items():
            if immutable_checksums.get(name) != expected_digest:
                raise IncompleteEvidenceBundle(
                    f"provenance checksum is missing or disagrees with identity: {name}"
                )
        reconstruction_flags.add(bool(row["reconstruction_flag"]))
    if len(reconstruction_flags) != 1:
        raise IncompleteEvidenceBundle(
            "provenance reconstruction_flag must be explicit and consistent"
        )
    return next(iter(reconstruction_flags))


def _validate_terminal_economics(
    key: tuple[str, str, str, str],
    episode: Mapping[str, Any],
) -> None:
    terminal = str(episode["terminal_state"])
    progress = float(episode["target_progress"])
    minimum_buffer = float(episode["minimum_mll_buffer"])
    days_to_target = episode["days_to_target"]
    duration = int(episode["duration_trading_days"])

    if terminal == "TARGET_REACHED":
        if progress < 1.0 - PATH_METRIC_ABS_TOLERANCE:
            raise IncompleteEvidenceBundle(
                f"episode {key} claims TARGET_REACHED below full target progress"
            )
        if days_to_target is None or float(days_to_target) <= 0.0 or float(days_to_target) > duration:
            raise IncompleteEvidenceBundle(
                f"episode {key} TARGET_REACHED has invalid days_to_target"
            )
        if minimum_buffer <= -PNL_ABS_TOLERANCE:
            raise IncompleteEvidenceBundle(
                f"episode {key} claims TARGET_REACHED after an MLL breach"
            )
        if not bool(episode["consistency_ok"]):
            raise IncompleteEvidenceBundle(
                f"episode {key} claims TARGET_REACHED with failed consistency"
            )
        return

    if days_to_target is not None:
        raise IncompleteEvidenceBundle(
            f"episode {key} records days_to_target without TARGET_REACHED"
        )
    if terminal == "MLL_BREACHED":
        if minimum_buffer > PNL_ABS_TOLERANCE:
            raise IncompleteEvidenceBundle(
                f"episode {key} claims MLL_BREACHED with a positive buffer"
            )
        if progress >= 1.0 - PATH_METRIC_ABS_TOLERANCE:
            raise IncompleteEvidenceBundle(
                f"episode {key} reached target before the claimed MLL terminal"
            )
        return

    if minimum_buffer <= -PNL_ABS_TOLERANCE:
        raise IncompleteEvidenceBundle(
            f"episode {key} omits an observed MLL breach"
        )
    if progress >= 1.0 - PATH_METRIC_ABS_TOLERANCE:
        raise IncompleteEvidenceBundle(
            f"episode {key} omits an observed target reach"
        )


def _make_bundle_read_only(bundle_path: Path) -> None:
    for path in sorted(bundle_path.rglob("*"), reverse=True):
        if path.is_file():
            path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        elif path.is_dir():
            path.chmod(
                stat.S_IRUSR
                | stat.S_IXUSR
                | stat.S_IRGRP
                | stat.S_IXGRP
                | stat.S_IROTH
                | stat.S_IXOTH
            )
    bundle_path.chmod(
        stat.S_IRUSR
        | stat.S_IXUSR
        | stat.S_IRGRP
        | stat.S_IXGRP
        | stat.S_IROTH
        | stat.S_IXOTH
    )


def _receipt_from_manifest(bundle: Path, manifest: Mapping[str, Any]) -> EvidenceBundleReceipt:
    manifest_path = bundle / "evidence_bundle_manifest.json"
    return EvidenceBundleReceipt(
        campaign_id=str(manifest["campaign_id"]),
        bundle_path=str(bundle),
        manifest_path=str(manifest_path),
        manifest_sha256=_sha256(manifest_path),
        bundle_content_sha256=str(manifest["bundle_content_sha256"]),
        evidence_status=str(manifest["evidence_status"]),
        reconstruction_flag=bool(manifest["reconstruction_flag"]),
        dataset_row_counts={
            name: int(manifest["dataset_row_counts"][name])
            for name in REQUIRED_DATASETS
        },
        finalized_at_utc=str(manifest["finalized_at_utc"]),
    )


def _clean_owned_receipt_temps(path: Path) -> None:
    if not path.parent.exists():
        return
    changed = False
    for candidate in path.parent.iterdir():
        match = _ATOMIC_TEMP_RE.fullmatch(candidate.name)
        if match is None or match.group("target") != path.name:
            continue
        mode = candidate.lstat().st_mode
        if candidate.is_symlink() or not stat.S_ISREG(mode):
            raise EvidenceBundleError(
                f"owned temporary receipt path is not a regular file: {candidate}"
            )
        candidate.unlink()
        changed = True
    if changed:
        _fsync_directory(path.parent)


def _project_receipt(path: Path, receipt: EvidenceBundleReceipt) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _clean_owned_receipt_temps(path)
    expected = receipt.to_dict()
    if path.exists() or path.is_symlink():
        if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
            raise EvidenceBundleError(
                f"EvidenceBundle receipt is not a regular file: {path}"
            )
        observed = _load_json(path)
        if observed != expected:
            raise EvidenceBundleError(
                f"existing EvidenceBundle receipt disagrees with sealed bundle: {path}"
            )
        return
    _atomic_json(path, expected)


def recover_finalized_evidence_bundle(
    base_dir: str | Path,
    campaign_id: str,
    *,
    lightweight_manifest_path: str | Path,
    expected_identity: Mapping[str, Any] | None = None,
) -> EvidenceBundleReceipt:
    """Idempotently project a receipt for an already atomically sealed bundle."""

    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", campaign_id):
        raise EvidenceBundleError("unsafe campaign_id")
    base = Path(base_dir).resolve()
    _require_git_ignored_payload_root(base)
    final = base / f"{campaign_id}.evidence-v1"
    staging = base / f".{campaign_id}.evidence-v1.staging"
    lock = base / f".{campaign_id}.evidence-v1.lock"
    receipt_path = Path(lightweight_manifest_path)
    resolved_receipt = receipt_path.resolve()
    if resolved_receipt == final or final in resolved_receipt.parents:
        raise EvidenceContractError(
            "lightweight receipt must live outside the EvidenceBundle directory"
        )

    descriptor = EvidenceBundleWriter._acquire_lock(lock)
    try:
        if staging.exists() or staging.is_symlink():
            raise EvidenceBundleError(
                "cannot recover a finalized bundle while staging also exists"
            )
        if not final.is_dir() or final.is_symlink():
            raise EvidenceBundleError(f"sealed EvidenceBundle does not exist: {final}")
        manifest = verify_evidence_bundle(final, deep=True)
        if manifest["campaign_id"] != campaign_id:
            raise IncompleteEvidenceBundle(
                "sealed EvidenceBundle belongs to another campaign"
            )
        if expected_identity is not None:
            checked_expected = validate_identity(expected_identity)
            observed_identity = validate_identity(_load_json(final / "identity.json"))
            if _canonical_bytes(checked_expected) != _canonical_bytes(observed_identity):
                raise IncompleteEvidenceBundle(
                    "sealed EvidenceBundle identity disagrees with recovery identity"
                )
        _make_bundle_read_only(final)
        receipt = _receipt_from_manifest(final, manifest)
        _project_receipt(receipt_path, receipt)
        return receipt
    finally:
        EvidenceBundleWriter._release_descriptor(descriptor)


def iter_evidence_records(
    bundle_path: str | Path,
    dataset: str,
) -> Iterator[dict[str, Any]]:
    if dataset not in RECORD_SPECS:
        raise EvidenceContractError(f"unknown evidence dataset: {dataset}")
    bundle = Path(bundle_path)
    manifest = _load_json(bundle / "evidence_bundle_manifest.json")
    partitions = sorted(
        (
            (details, relative_path)
            for relative_path, details in manifest.get("files", {}).items()
            if details.get("kind") == "dataset_partition"
            and details.get("dataset") == dataset
        ),
        key=lambda item: int(item[0]["part_index"]),
    )
    if [int(item[0]["part_index"]) for item in partitions] != list(
        range(len(partitions))
    ):
        raise IncompleteEvidenceBundle(
            f"EvidenceBundle has non-contiguous partitions for {dataset}"
        )
    for expected_index, (details, relative_path) in enumerate(partitions):
        expected_path = f"datasets/{dataset}/part-{expected_index:06d}.jsonl.gz"
        if relative_path != expected_path:
            raise IncompleteEvidenceBundle(
                f"EvidenceBundle partition path drift for {dataset}"
            )
        header, rows = _read_part(
            bundle / relative_path,
            expected_dataset=dataset,
            expected_campaign_id=str(manifest["campaign_id"]),
        )
        if (
            int(header.get("part_index", -1)) != expected_index
            or header.get("batch_id") != details.get("batch_id")
            or int(header.get("row_count", -1)) != details.get("row_count")
            or header.get("payload_sha256") != details.get("payload_sha256")
        ):
            raise IncompleteEvidenceBundle(
                f"EvidenceBundle partition metadata drift for {dataset}"
            )
        yield from rows


def verify_evidence_bundle(
    bundle_path: str | Path,
    *,
    deep: bool = True,
) -> dict[str, Any]:
    bundle = Path(bundle_path).resolve()
    _require_git_ignored_payload_root(bundle.parent)
    manifest_path = bundle / "evidence_bundle_manifest.json"
    if not manifest_path.is_file():
        raise IncompleteEvidenceBundle("sealed EvidenceBundle manifest is missing")
    manifest = _load_json(manifest_path)
    if manifest.get("contract") != EVIDENCE_BUNDLE_CONTRACT:
        raise IncompleteEvidenceBundle("EvidenceBundle contract mismatch")
    if int(manifest.get("schema_version", -1)) != EVIDENCE_BUNDLE_SCHEMA_VERSION:
        raise IncompleteEvidenceBundle("EvidenceBundle schema version mismatch")
    if manifest.get("status") != "COMPLETE":
        raise IncompleteEvidenceBundle("EvidenceBundle is not COMPLETE")
    evidence_status = manifest.get("evidence_status")
    if evidence_status not in EVIDENCE_STATUSES:
        raise IncompleteEvidenceBundle("EvidenceBundle evidence_status is invalid")
    if not isinstance(manifest.get("reconstruction_flag"), bool):
        raise IncompleteEvidenceBundle(
            "EvidenceBundle reconstruction_flag must be a boolean"
        )
    if bool(manifest["reconstruction_flag"]) != (
        evidence_status == "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION"
    ):
        raise IncompleteEvidenceBundle(
            "EvidenceBundle evidence_status conflicts with reconstruction provenance"
        )
    manifest_core = dict(manifest)
    observed_content_hash = str(manifest_core.pop("bundle_content_sha256", ""))
    if _canonical_hash(manifest_core) != observed_content_hash:
        raise IncompleteEvidenceBundle("EvidenceBundle content manifest hash mismatch")

    file_manifest = manifest.get("files")
    if not isinstance(file_manifest, Mapping):
        raise IncompleteEvidenceBundle("EvidenceBundle file manifest is invalid")
    for relative_path, details in file_manifest.items():
        if (
            not isinstance(relative_path, str)
            or not relative_path
            or Path(relative_path).is_absolute()
            or ".." in Path(relative_path).parts
            or Path(relative_path).as_posix() != relative_path
            or not isinstance(details, Mapping)
        ):
            raise IncompleteEvidenceBundle("EvidenceBundle contains an unsafe file entry")
        if (
            not isinstance(details.get("sha256"), str)
            or _SHA256_RE.fullmatch(str(details["sha256"])) is None
            or not isinstance(details.get("size_bytes"), int)
            or isinstance(details.get("size_bytes"), bool)
            or int(details["size_bytes"]) < 0
        ):
            raise IncompleteEvidenceBundle(
                f"EvidenceBundle file metadata is invalid: {relative_path}"
            )
        kind = details.get("kind")
        if kind not in {"identity", "checkpoint", "dataset_partition", "compact_output"}:
            raise IncompleteEvidenceBundle(
                f"EvidenceBundle file kind is invalid: {relative_path}"
            )
        if kind == "identity" and relative_path != "identity.json":
            raise IncompleteEvidenceBundle("EvidenceBundle identity path is invalid")
        if kind == "checkpoint" and relative_path != "checkpoint.json":
            raise IncompleteEvidenceBundle("EvidenceBundle checkpoint path is invalid")
        if kind == "dataset_partition":
            if (
                details.get("dataset") not in RECORD_SPECS
                or not isinstance(details.get("part_index"), int)
                or isinstance(details.get("part_index"), bool)
                or not isinstance(details.get("row_count"), int)
                or isinstance(details.get("row_count"), bool)
                or int(details.get("row_count", 0)) <= 0
                or not isinstance(details.get("batch_id"), str)
                or not details.get("batch_id")
                or _SHA256_RE.fullmatch(str(details.get("payload_sha256") or ""))
                is None
            ):
                raise IncompleteEvidenceBundle(
                    f"EvidenceBundle partition declaration is invalid: {relative_path}"
                )

    expected_files = set(file_manifest) | {manifest_path.name}
    try:
        _validate_staging_layout(bundle, allow_manifest=True)
    except EvidenceBundleError as exc:
        raise IncompleteEvidenceBundle(
            "EvidenceBundle contains an unexpected or unsafe path"
        ) from exc
    tree_paths = list(bundle.rglob("*"))
    if any(path.is_symlink() for path in tree_paths):
        raise IncompleteEvidenceBundle("EvidenceBundle may not contain symlinks")
    observed_files = {
        path.relative_to(bundle).as_posix()
        for path in tree_paths
        if path.is_file()
    }
    if observed_files != expected_files:
        raise IncompleteEvidenceBundle("EvidenceBundle contains missing or unsealed files")
    for relative_path, details in file_manifest.items():
        path = bundle / relative_path
        if not path.is_file():
            raise IncompleteEvidenceBundle(f"EvidenceBundle file is missing: {relative_path}")
        if path.stat().st_size != int(details["size_bytes"]):
            raise IncompleteEvidenceBundle(f"EvidenceBundle file size drift: {relative_path}")
        if _sha256(path) != details["sha256"]:
            raise IncompleteEvidenceBundle(f"EvidenceBundle checksum drift: {relative_path}")

    identity = validate_identity(_load_json(bundle / "identity.json"))
    if identity["campaign_id"] != manifest["campaign_id"]:
        raise IncompleteEvidenceBundle("EvidenceBundle identity mismatch")
    if _sha256(bundle / "identity.json") != manifest["identity_sha256"]:
        raise IncompleteEvidenceBundle("EvidenceBundle identity checksum mismatch")
    row_counts = manifest.get("dataset_row_counts", {})
    if (
        not isinstance(row_counts, Mapping)
        or set(row_counts) != set(REQUIRED_DATASETS)
        or any(
            not isinstance(row_counts[name], int)
            or isinstance(row_counts[name], bool)
            or int(row_counts[name]) <= 0
            for name in REQUIRED_DATASETS
        )
    ):
        raise IncompleteEvidenceBundle("EvidenceBundle is summary-only or omits a required ledger")
    compact_outputs = manifest.get("compact_outputs")
    if not isinstance(compact_outputs, Mapping) or set(compact_outputs) != set(
        REQUIRED_COMPACT_OUTPUTS
    ):
        raise IncompleteEvidenceBundle("EvidenceBundle omits required automatic outputs")
    for name in REQUIRED_COMPACT_OUTPUTS:
        output = compact_outputs[name]
        relative_path = f"outputs/{name}.json"
        if (
            not isinstance(output, Mapping)
            or output.get("relative_path") != relative_path
            or relative_path not in file_manifest
            or file_manifest[relative_path].get("kind") != "compact_output"
            or file_manifest[relative_path].get("output") != name
            or output.get("sha256") != file_manifest[relative_path].get("sha256")
            or output.get("size_bytes") != file_manifest[relative_path].get("size_bytes")
        ):
            raise IncompleteEvidenceBundle(
                f"EvidenceBundle compact output metadata drift: {name}"
            )
        validate_compact_output(name, _load_json(bundle / relative_path))

    if (
        file_manifest.get("identity.json", {}).get("kind") != "identity"
        or file_manifest.get("checkpoint.json", {}).get("kind") != "checkpoint"
    ):
        raise IncompleteEvidenceBundle("EvidenceBundle core file declarations are invalid")
    datasets = manifest.get("datasets")
    if not isinstance(datasets, Mapping) or set(datasets) != set(REQUIRED_DATASETS):
        raise IncompleteEvidenceBundle("EvidenceBundle dataset declarations are invalid")
    for dataset in REQUIRED_DATASETS:
        details = datasets[dataset]
        partitions = [
            file_details
            for file_details in file_manifest.values()
            if file_details.get("kind") == "dataset_partition"
            and file_details.get("dataset") == dataset
        ]
        if (
            not isinstance(details, Mapping)
            or details.get("row_count") != row_counts[dataset]
            or details.get("partition_count") != len(partitions)
            or details.get("sort_fields") != list(RECORD_SPECS[dataset].sort_fields)
            or len(partitions) <= 0
        ):
            raise IncompleteEvidenceBundle(
                f"EvidenceBundle dataset metadata drift: {dataset}"
            )

    checkpoint = _load_json(bundle / "checkpoint.json")
    if (
        not isinstance(checkpoint, Mapping)
        or checkpoint.get("state") != "COMPLETE"
        or checkpoint.get("campaign_id") != manifest["campaign_id"]
        or checkpoint.get("dataset_row_counts") != row_counts
        or checkpoint.get("compact_outputs") != compact_outputs
    ):
        raise IncompleteEvidenceBundle("EvidenceBundle checkpoint disagrees with manifest")

    if deep:
        observed_counts: dict[str, int] = {}

        def counted_records(dataset: str) -> Iterator[dict[str, Any]]:
            count = 0
            for row in iter_evidence_records(bundle, dataset):
                count += 1
                yield row
            observed_counts[dataset] = count

        reconstruction = _validate_relational_contract(
            identity=identity,
            records={name: counted_records(name) for name in REQUIRED_DATASETS},
        )
        for dataset in REQUIRED_DATASETS:
            if observed_counts.get(dataset) != int(row_counts[dataset]):
                raise IncompleteEvidenceBundle(f"dataset row count drift: {dataset}")
        if reconstruction != bool(manifest["reconstruction_flag"]):
            raise IncompleteEvidenceBundle("reconstruction provenance mismatch")
    return manifest


def require_complete_evidence_bundle(
    bundle_path: str | Path,
    *,
    campaign_id: str | None = None,
    deep: bool = True,
) -> dict[str, Any]:
    manifest = verify_evidence_bundle(bundle_path, deep=deep)
    if campaign_id is not None and manifest["campaign_id"] != campaign_id:
        raise IncompleteEvidenceBundle("completion references another campaign's evidence")
    return manifest


def guard_campaign_completion(
    requested_status: str,
    bundle_path: str | Path | None,
    *,
    campaign_id: str | None = None,
    deep: bool = True,
) -> dict[str, Any] | None:
    if requested_status not in {"COMPLETE", "COMPLETED"}:
        return None
    if bundle_path is None:
        raise IncompleteEvidenceBundle(
            "campaign COMPLETE is forbidden without an EvidenceBundle v1"
        )
    return require_complete_evidence_bundle(
        bundle_path,
        campaign_id=campaign_id,
        deep=deep,
    )


__all__ = [
    "EvidenceBundleBusy",
    "EvidenceBundleError",
    "EvidenceBundleReceipt",
    "EvidenceBundleWriter",
    "EvidencePartReceipt",
    "IncompleteEvidenceBundle",
    "guard_campaign_completion",
    "iter_evidence_records",
    "recover_finalized_evidence_bundle",
    "require_complete_evidence_bundle",
    "verify_evidence_bundle",
]
