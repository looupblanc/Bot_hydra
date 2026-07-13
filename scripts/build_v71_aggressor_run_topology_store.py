from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.v71_aggressor_run_topology_store import (
    build_aggressor_run_topology_store,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build the WORM-frozen V7.1 aggressor-run topology store."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--chunk-size", type=int, default=1_000_000)
    args = parser.parse_args()
    result = build_aggressor_run_topology_store(
        args.project_root, chunk_size=args.chunk_size
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
