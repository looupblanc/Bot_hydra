from __future__ import annotations

import argparse
import json

from hydra.data.v72_trade_arrival_renewal_store import (
    build_trade_arrival_renewal_store,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--chunk-size", type=int, default=1_000_000)
    args = parser.parse_args()
    result = build_trade_arrival_renewal_store(
        args.project_root, chunk_size=args.chunk_size
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
