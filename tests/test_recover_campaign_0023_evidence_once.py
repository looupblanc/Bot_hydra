from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from scripts.recover_campaign_0023_evidence_once import (
    DeterministicGzipJsonl,
    ReconciliationError,
    assert_exact,
    canonical_hash,
)


def test_canonical_hash_is_key_order_independent() -> None:
    assert canonical_hash({"b": 2, "a": 1}) == canonical_hash({"a": 1, "b": 2})


def test_exact_reconciliation_has_zero_tolerance() -> None:
    with pytest.raises(ReconciliationError) as error:
        assert_exact("money", {"net": 1.0}, {"net": 1.0 + 1e-15})
    assert error.value.check == "money"


def test_gzip_jsonl_is_deterministic_and_counted(tmp_path: Path) -> None:
    paths = [tmp_path / "a.jsonl.gz", tmp_path / "b.jsonl.gz"]
    receipts = []
    for path in paths:
        writer = DeterministicGzipJsonl(path)
        writer.write({"z": 1, "a": 2})
        writer.write({"value": 3})
        receipts.append(writer.close())
    assert receipts[0]["sha256"] == receipts[1]["sha256"]
    assert receipts[0]["row_count"] == 2
    with gzip.open(paths[0], "rt", encoding="ascii") as handle:
        assert [json.loads(line) for line in handle] == [
            {"a": 2, "z": 1},
            {"value": 3},
        ]
