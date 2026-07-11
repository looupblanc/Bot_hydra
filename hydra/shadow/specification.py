from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


PROHIBITED_KEYS = frozenset(
    {"broker", "broker_credentials", "api_key", "secret", "order_endpoint", "submit_order"}
)


@dataclass(frozen=True)
class ShadowSpecification:
    strategy_id: str
    strategy_version: str
    feature_versions: tuple[str, ...]
    markets: tuple[str, ...]
    timeframes: tuple[str, ...]
    session_rules: dict[str, Any]
    entry_rules: dict[str, Any]
    exit_rules: dict[str, Any]
    sizing: dict[str, Any]
    costs: dict[str, Any]
    stale_data_seconds: int
    expected_update_seconds: int
    duplicate_signal_window_seconds: int
    maximum_exposure: float
    simulated_mll_floor: float
    internal_daily_risk_limit: float
    kill_conditions: tuple[str, ...]
    logging: dict[str, Any]
    reconciliation: dict[str, Any]
    source_manifest_hash: str
    outbound_orders_enabled: bool = False

    def validate(self) -> None:
        payload = asdict(self)
        present = PROHIBITED_KEYS & _recursive_keys(payload)
        if present:
            raise ValueError(f"Prohibited shadow keys: {sorted(present)}")
        if self.outbound_orders_enabled:
            raise ValueError("Shadow configurations cannot enable outbound orders.")
        if not self.strategy_id or not self.strategy_version or not self.source_manifest_hash:
            raise ValueError("Immutable strategy identity/provenance is required.")
        if not self.markets or not self.timeframes or not self.feature_versions:
            raise ValueError("Markets, timeframes and feature versions are required.")
        if self.stale_data_seconds <= 0 or self.expected_update_seconds <= 0:
            raise ValueError("Positive data timing limits are required.")
        if self.maximum_exposure <= 0 or self.internal_daily_risk_limit <= 0:
            raise ValueError("Fail-closed exposure and daily risk limits are required.")
        if self.simulated_mll_floor >= 0:
            raise ValueError("Simulated MLL floor must be a negative loss boundary.")

    @property
    def configuration_hash(self) -> str:
        self.validate()
        raw = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(raw.encode()).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["configuration_hash"] = self.configuration_hash
        return payload

    def write_immutable(self, path: str | Path) -> Path:
        target = Path(path)
        content = json.dumps(self.to_dict(), indent=2, sort_keys=True, default=str) + "\n"
        if target.exists() and target.read_text(encoding="utf-8") != content:
            raise RuntimeError(f"Refusing divergent shadow configuration: {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            target.write_text(content, encoding="utf-8")
        return target


def _recursive_keys(value: Any) -> set[str]:
    if isinstance(value, dict):
        return {str(key) for key in value} | {
            nested for item in value.values() for nested in _recursive_keys(item)
        }
    if isinstance(value, (list, tuple)):
        return {nested for item in value for nested in _recursive_keys(item)}
    return set()
