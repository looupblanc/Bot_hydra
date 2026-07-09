#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.execution.shadow import shadow_validate_placeholder


def main() -> None:
    result = shadow_validate_placeholder()
    print("HYDRA shadow validation placeholder")
    print("No live trades are placed. Live trading is forbidden.")
    print(result)


if __name__ == "__main__":
    main()
