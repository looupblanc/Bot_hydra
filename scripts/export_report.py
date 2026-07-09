#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.registry.db import connect
from hydra.registry.reports import build_markdown_report
from hydra.utils.config import load_config


def main() -> None:
    cfg = load_config()
    conn = connect(cfg["registry"]["path"])
    path = build_markdown_report(conn, cfg["reports"]["folder"])
    print(f"Report saved: {path}")


if __name__ == "__main__":
    main()
