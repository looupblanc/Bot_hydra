from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.v7_d1_trades import acquire_d1_trades


def main() -> int:
    result = acquire_d1_trades(project_root=Path(__file__).resolve().parents[1])
    print(
        json.dumps(
            {
                "request_id": result["request_id"],
                "network_request_made": result["network_request_made"],
                "actual_spend_usd": result["actual_spend_usd"],
                "raw_output_path": result["raw_output_path"],
                "raw_size_bytes": result["raw_size_bytes"],
                "raw_sha256": result["raw_sha256"],
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
