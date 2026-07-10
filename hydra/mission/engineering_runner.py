from __future__ import annotations

import shutil
import subprocess
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class EngineeringCapability:
    codex_cli_available: bool
    noninteractive_verified: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def detect_engineering_capability() -> EngineeringCapability:
    codex_path = shutil.which("codex")
    if not codex_path:
        return EngineeringCapability(False, False, "codex_cli_not_found")
    try:
        result = subprocess.run([codex_path, "--version"], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
    except Exception as exc:
        return EngineeringCapability(True, False, f"codex_cli_version_check_failed:{exc}")
    return EngineeringCapability(True, result.returncode == 0, "codex_cli_version_verified" if result.returncode == 0 else "codex_cli_version_failed")

