from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v71_mechanism_forensics import run_v71_mechanism_forensics


def main() -> int:
    parser = argparse.ArgumentParser(description="Run frozen V7.1 mechanism forensics.")
    parser.add_argument("--project-root", default=".")
    args = parser.parse_args()
    print(json.dumps(run_v71_mechanism_forensics(project_root=args.project_root), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
