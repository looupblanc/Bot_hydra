from __future__ import annotations

import json
from pathlib import Path

from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.xfa_post_payout_frontier import extract_lifecycle_rows


def _baseline(path: str) -> dict[str, object]:
    return {
        "path": path,
        "path_hash": f"{path}-hash",
        "start_day": 12,
        "end_day": 20,
        "terminal": "SURVIVED_HORIZON",
        "observed_days": 9,
        "traded_days": 3,
        "event_count": 4,
        "accepted_event_count": 4,
        "skipped_event_count": 0,
        "payout_cycles": 1,
        "gross_payout": 200.0,
        "trader_net_payout": 180.0,
        "first_payout_day": 5,
        "ending_balance": 50.0,
        "ending_mll_floor": 0.0,
        "minimum_mll_buffer": 100.0,
        "post_payout_survived": True,
        "post_payout_censored": False,
        "post_payout_observed_days": 4,
        "daily_ledger": [{"must_not_survive_compaction": True}],
        "component_contribution": {"sleeve": 1.0},
    }


def test_extract_lifecycle_rows_skips_raw_and_removes_daily_ledgers(
    tmp_path: Path,
) -> None:
    row = {
        "policy_id": "book",
        "scenario": "NORMAL",
        "combine_start_day": 1,
        "combine_end_day": 11,
        "combine_status": "TARGET_REACHED",
        "combine_horizon": "FULL_CHRONOLOGICAL_HORIZON",
        "xfa_start_day": 12,
        "xfa_horizon_days": 120,
        "xfa_profile": {"fingerprint": "profile"},
        "rule_snapshot": {"fingerprint": "rules"},
        "standard": _baseline("STANDARD"),
        "consistency": _baseline("CONSISTENCY"),
    }
    row["source_lifecycle_sha256"] = stable_hash(row)
    path = tmp_path / "batch.json"
    path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "evidence_raw": [
                            {"large_field_that_must_not_be_returned": list(range(100))}
                        ],
                        "lifecycle_rows": [row],
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    extracted = extract_lifecycle_rows(path)

    assert len(extracted) == 1
    assert extracted[0]["policy_id"] == "book"
    assert "daily_ledger" not in extracted[0]["standard"]
    assert "component_contribution" not in extracted[0]["consistency"]
    assert "evidence_raw" not in extracted[0]
