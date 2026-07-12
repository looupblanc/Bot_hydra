from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v71_event_funnel import run_v71_development_funnel


def main() -> int:
    parser = argparse.ArgumentParser(description="Run V7.1 D1 Stage 0-2 funnel.")
    parser.add_argument("--project-root", default=".")
    args = parser.parse_args()
    result = run_v71_development_funnel(project_root=args.project_root)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
