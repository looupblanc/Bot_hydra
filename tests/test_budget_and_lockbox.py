from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from hydra.data.budget import DatabentoBudgetConfig, DatabentoSpendRecord, append_spend_record, enforce_budget, read_ledger
from hydra.data.acquisition_policy import decide_databento_acquisition
from hydra.data.databento_loader import DatabentoRequest
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import LockboxViolation, enforce_data_access


class BudgetAndLockboxTests(unittest.TestCase):
    def test_budget_safety_ceiling_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = DatabentoBudgetConfig(ledger_path=str(Path(tmp) / "ledger.jsonl"), summary_path=str(Path(tmp) / "summary.md"), safety_ceiling_usd=1.0)
            with self.assertRaises(Exception):
                enforce_budget(cfg, 1.01)

    def test_ledger_append_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = DatabentoBudgetConfig(ledger_path=str(Path(tmp) / "ledger.jsonl"), summary_path=str(Path(tmp) / "summary.md"))
            record = DatabentoSpendRecord(
                request_id="abc",
                timestamp_utc="2026-07-10T00:00:00Z",
                dataset="GLBX.MDP3",
                schema="ohlcv-1m",
                symbols=["ES"],
                stype_in="continuous",
                start="2024-04-01",
                end="2024-07-01",
                estimated_cost_usd=0.1,
                actual_cost_usd=None,
                cumulative_estimated_spend_usd=0.1,
                cumulative_actual_spend_usd=0.0,
                cache_hit=False,
                research_purpose="test",
                candidate_tier="test",
                approval_mode="AUTO_UNDER_HARD_CAP",
                resulting_file=None,
                checksum=None,
                download_status="ESTIMATED_ONLY",
            )
            append_spend_record(cfg, record)
            append_spend_record(cfg, record)
            self.assertEqual(len(read_ledger(Path(tmp) / "ledger.jsonl")), 2)

    def test_duplicate_request_blocked_by_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = DatabentoBudgetConfig(ledger_path=str(Path(tmp) / "ledger.jsonl"), summary_path=str(Path(tmp) / "summary.md"))
            request = DatabentoRequest(
                dataset="GLBX.MDP3",
                schema="ohlcv-1m",
                symbols=["ES"],
                api_symbols=["ES.c.0"],
                symbol_map={"ES.c.0": "ES"},
                start="2024-04-01",
                end="2024-07-01",
                timeframe="1m",
                stype_in="continuous",
                stype_out="instrument_id",
                cache_folder=str(Path(tmp)),
                raw_output_path=str(Path(tmp) / "missing.dbn.zst"),
                output_path=str(Path(tmp) / "missing.parquet"),
            )
            first = decide_databento_acquisition(
                request,
                cfg,
                research_purpose="test",
                candidate_tier="test",
                key=None,
                estimate={"estimated_cost_usd": 0.1},
            )
            append_spend_record(
                cfg,
                DatabentoSpendRecord(
                    request_id=first.request_id,
                    timestamp_utc="2026-07-10T00:00:00Z",
                    dataset="GLBX.MDP3",
                    schema="ohlcv-1m",
                    symbols=["ES"],
                    stype_in="continuous",
                    start="2024-04-01",
                    end="2024-07-01",
                    estimated_cost_usd=0.0,
                    actual_cost_usd=0.1,
                    cumulative_estimated_spend_usd=0.1,
                    cumulative_actual_spend_usd=0.1,
                    cache_hit=False,
                    research_purpose="test",
                    candidate_tier="test",
                    approval_mode="AUTO_UNDER_HARD_CAP",
                    resulting_file=None,
                    checksum=None,
                    download_status="DOWNLOADED",
                ),
            )
            second = decide_databento_acquisition(
                request,
                cfg,
                research_purpose="test",
                candidate_tier="test",
                key=None,
                estimate={"estimated_cost_usd": 0.1},
            )
            self.assertFalse(second.may_download)
            self.assertEqual(second.reason, "duplicate_request_blocked_by_ledger")

    def test_q3_requires_manifest_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(LockboxViolation):
                enforce_data_access(
                    period="2024-07-01:2024-10-01",
                    role=DataRole.BLIND_VALIDATION,
                    requesting_module="test",
                    candidate_ids=["cand"],
                    reason="test",
                    freeze_manifest_hash=None,
                    ledger_path=str(Path(tmp) / "access.jsonl"),
                )

    def test_q4_requires_manifest_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(LockboxViolation):
                enforce_data_access(
                    period="2024-10-01:2025-01-01",
                    role=DataRole.FINAL_LOCKBOX,
                    requesting_module="test",
                    candidate_ids=["cand"],
                    reason="test",
                    freeze_manifest_hash=None,
                    ledger_path=str(Path(tmp) / "access.jsonl"),
                )


if __name__ == "__main__":
    unittest.main()
