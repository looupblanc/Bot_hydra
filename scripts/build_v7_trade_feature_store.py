from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.v7_trade_feature_store import build_feature_store


def main() -> int:
    result = build_feature_store(Path(__file__).resolve().parents[1])
    print(
        json.dumps(
            {
                "manifest": "data/manifests/v7_d1_trades_features_v1.json",
                "source_record_count": result["audit"]["source_record_count"],
                "retained_rth_record_count": result["audit"][
                    "retained_rth_record_count"
                ],
                "row_count": result["output"]["row_count"],
                "products": result["output"]["products"],
                "contracts": result["output"]["contracts"],
                "output_sha256": result["output"]["sha256"],
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
