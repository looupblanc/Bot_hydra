from __future__ import annotations

import logging
import time


def sleep_cycle(seconds: int) -> None:
    logging.getLogger(__name__).info("Sleeping for %s seconds", seconds)
    time.sleep(seconds)
