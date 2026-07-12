from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.v7_graveyard import build_graveyard


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build HYDRA V7 class-only active graveyard."
    )
    parser.add_argument("--registry", default="registry/hydra_registry.db")
    parser.add_argument(
        "--phase2-result", default="reports/v7/phase2/phase2_result.json"
    )
    parser.add_argument(
        "--grammar-result",
        action="append",
        default=[],
        help="Frozen V7 grammar result to tombstone at class level (repeatable).",
    )
    parser.add_argument("--output", default="mission/state/graveyard.db")
    args = parser.parse_args()
    result = build_graveyard(
        registry_path=args.registry,
        phase2_result_path=args.phase2_result,
        output_path=args.output,
        grammar_result_paths=args.grammar_result,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
