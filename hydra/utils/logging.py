from __future__ import annotations

import logging
from pathlib import Path


def setup_logging(folder: str = "logs", level: str = "INFO") -> None:
    Path(folder).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(Path(folder) / "hydra.log", encoding="utf-8"),
        ],
    )
