from __future__ import annotations

import queue
import threading
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.evidence import (
    EvidenceBundleWriter,
    guard_campaign_completion,
    verify_evidence_bundle,
)


class ProductionEvidenceError(RuntimeError):
    pass


class AsyncEvidenceBundleSink:
    """Serialize all EvidenceBundle mutations through exactly one thread."""

    def __init__(
        self,
        *,
        base_dir: str | Path,
        identity: Mapping[str, Any],
        writer_id: str,
        resume: bool,
    ) -> None:
        self._base_dir = Path(base_dir).resolve()
        self._identity = dict(identity)
        self._campaign_id = str(identity["campaign_id"])
        if resume and _has_staging_bundle(self._base_dir, self._campaign_id):
            self._writer = EvidenceBundleWriter.resume(
                self._base_dir,
                self._campaign_id,
                writer_id=writer_id,
                expected_identity=self._identity,
            )
        else:
            self._writer = EvidenceBundleWriter.create(
                self._base_dir,
                self._identity,
                writer_id=writer_id,
            )
        self._queue: queue.Queue[
            tuple[str, tuple[Any, ...], dict[str, Any], threading.Event | None, list[Any] | None]
            | None
        ] = queue.Queue()
        self._failure: BaseException | None = None
        self._finalized = False
        self._closed = False
        self._thread = threading.Thread(
            target=self._run,
            name=f"evidence-writer-{writer_id}",
            daemon=True,
        )
        self._thread.start()

    @property
    def writer_count(self) -> int:
        return 1

    def append_records(
        self,
        dataset: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        batch_id: str,
    ) -> None:
        if rows:
            self._submit(
                "append_records",
                dataset,
                [dict(row) for row in rows],
                batch_id=batch_id,
            )

    def write_compact_output(self, name: str, payload: Mapping[str, Any]) -> None:
        self._submit("write_compact_output", name, dict(payload))

    def checkpoint(self, metadata: Mapping[str, Any]) -> None:
        self._submit("checkpoint", dict(metadata))

    def flush(self) -> None:
        self._raise_if_failed()
        self._queue.join()
        self._raise_if_failed()

    def finalize(
        self,
        *,
        lightweight_manifest_path: str | Path,
        evidence_status: str = "FRESH_DEVELOPMENT_EVIDENCE",
    ) -> Any:
        receipt = self._submit_wait(
            "finalize",
            evidence_status=evidence_status,
            lightweight_manifest_path=Path(lightweight_manifest_path),
        )
        self._finalized = True
        self.flush()
        return receipt

    def guard_completion(self, bundle_path: str | Path) -> Mapping[str, Any]:
        self.flush()
        guard_campaign_completion(
            "COMPLETE",
            bundle_path,
            campaign_id=self._campaign_id,
        )
        verified = verify_evidence_bundle(bundle_path)
        if not isinstance(verified, Mapping):
            raise ProductionEvidenceError("EvidenceBundle verification returned no manifest")
        return dict(verified)

    def close(self) -> None:
        if self._closed:
            return
        self.flush()
        if not self._finalized:
            self._submit_wait("close")
        self._queue.put(None)
        self._thread.join(timeout=30.0)
        if self._thread.is_alive():
            raise ProductionEvidenceError("asynchronous evidence writer did not stop")
        self._closed = True
        self._raise_if_failed()

    def _submit(self, method: str, *args: Any, **kwargs: Any) -> None:
        self._raise_if_failed()
        self._queue.put((method, args, kwargs, None, None))

    def _submit_wait(self, method: str, *args: Any, **kwargs: Any) -> Any:
        self._raise_if_failed()
        done = threading.Event()
        result: list[Any] = []
        self._queue.put((method, args, kwargs, done, result))
        done.wait()
        self._raise_if_failed()
        if not result:
            return None
        value = result[0]
        if isinstance(value, BaseException):
            raise ProductionEvidenceError(f"evidence operation failed: {method}") from value
        return value

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                self._queue.task_done()
                return
            method, args, kwargs, done, result = item
            try:
                if self._failure is not None:
                    raise self._failure
                value = getattr(self._writer, method)(*args, **kwargs)
                if result is not None:
                    result.append(value)
            except BaseException as exc:
                self._failure = exc
                if result is not None:
                    result.append(exc)
            finally:
                if done is not None:
                    done.set()
                self._queue.task_done()

    def _raise_if_failed(self) -> None:
        if self._failure is not None:
            raise ProductionEvidenceError("authoritative EvidenceBundle writer failed") from self._failure


def _has_staging_bundle(base_dir: Path, campaign_id: str) -> bool:
    return (base_dir / f".{campaign_id}.evidence-v1.staging").is_dir()


__all__ = ["AsyncEvidenceBundleSink", "ProductionEvidenceError"]
