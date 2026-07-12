from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.v7_d1_event_store import build_date_matched_event_store


def main() -> int:
    result = build_date_matched_event_store(
        Path(__file__).resolve().parents[1]
    )
    print(
        json.dumps(
            {
                "manifest": "data/manifests/v7_d1_date_matched_event_store_v1.json",
                "minute_rows": result["minute_output"]["row_count"],
                "event_rows": result["event_output"]["row_count"],
                "event_counts": result["event_output"]["counts"],
                "minute_sha256": result["minute_output"]["sha256"],
                "event_sha256": result["event_output"]["sha256"],
                "q4_access_count_delta": result["q4_access_count_delta"],
                "forward_gap_access_count": result["forward_gap_access_count"],
                "outbound_order_count": result["outbound_order_count"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
