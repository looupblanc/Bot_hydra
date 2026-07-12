from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from hydra.research.v71_event_mechanism_grammar import V71Signal, signal_path_hash
from hydra.research.v71_event_time_grammar import (
    candidate_specs,
    generate_signal_population,
    load_event_time_sources,
)


EXECUTABLE_MANIFEST_PATH = (
    "WORM/v7.1-event-time-executable-diagnostic-0001-2026-07-12.json"
)
EXECUTABLE_MANIFEST_SHA256 = (
    "058278f8111dc35d6f19ef484ed4b0674f5bb323dbb2a941ebd9d7971080c944"
)


class V71ExecutableStrategyError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ExecutableEventTimeStrategy:
    candidate_id: str
    alias: str
    specification_hash: str
    signal_path_hash: str
    direction: str
    holding_minutes: int
    position_quantity_primary: int
    diagnostic_quantities: tuple[int, ...]
    explicit_contract_from_signal: bool
    maximum_one_open_position: bool
    session_flatten: bool
    broker_or_order_adapter: bool

    def __post_init__(self) -> None:
        if self.direction != "CONTINUATION":
            raise V71ExecutableStrategyError("unexpected frozen direction")
        if self.holding_minutes != 60:
            raise V71ExecutableStrategyError("unexpected frozen holding horizon")
        if self.position_quantity_primary != 1:
            raise V71ExecutableStrategyError("unexpected primary quantity")
        if self.broker_or_order_adapter:
            raise V71ExecutableStrategyError("broker/order adapter is prohibited")


def load_executable_strategies(
    project_root: str | Path = ".",
) -> tuple[ExecutableEventTimeStrategy, ...]:
    root = Path(project_root).resolve()
    path = root / EXECUTABLE_MANIFEST_PATH
    if _sha256(path) != EXECUTABLE_MANIFEST_SHA256:
        raise V71ExecutableStrategyError("executable manifest hash drift")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    execution = manifest["execution"]
    rows = tuple(
        ExecutableEventTimeStrategy(
            candidate_id=str(row["candidate_id"]),
            alias=str(row["alias"]),
            specification_hash=str(row["specification_hash"]),
            signal_path_hash=str(row["signal_path_hash"]),
            direction=str(row["direction"]),
            holding_minutes=60,
            position_quantity_primary=int(row["position_quantity_primary"]),
            diagnostic_quantities=tuple(
                int(value) for value in row["diagnostic_quantities"]
            ),
            explicit_contract_from_signal=bool(
                execution["explicit_contract_from_signal"]
            ),
            maximum_one_open_position=bool(
                execution["maximum_one_open_position_per_candidate"]
            ),
            session_flatten=bool(execution["session_flatten"]),
            broker_or_order_adapter=bool(manifest["broker_or_order_adapter"]),
        )
        for row in manifest["candidates"]
    )
    if len(rows) != 2 or len({row.candidate_id for row in rows}) != 2:
        raise V71ExecutableStrategyError("expected two distinct frozen strategies")
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def frozen_signal_population(
    project_root: str | Path = ".",
) -> tuple[
    Mapping[str, tuple[V71Signal, ...]],
    Mapping[str, Any],
    Any,
]:
    root = Path(project_root).resolve()
    executable = {row.candidate_id: row for row in load_executable_strategies(root)}
    minute, event, audit = load_event_time_sources(root)
    all_specs = {row.candidate_id: row for row in candidate_specs(root)}
    all_signals = generate_signal_population(
        minute,
        event,
        project_root=root,
        graveyard_path=None,
    )
    selected_signals: dict[str, tuple[V71Signal, ...]] = {}
    selected_specs: dict[str, Any] = {}
    for candidate_id, config in executable.items():
        spec = all_specs[candidate_id]
        signals = all_signals[candidate_id]
        if spec.specification_hash != config.specification_hash:
            raise V71ExecutableStrategyError("frozen specification drift")
        if signal_path_hash(signals) != config.signal_path_hash:
            raise V71ExecutableStrategyError("frozen signal path drift")
        selected_specs[candidate_id] = spec
        selected_signals[candidate_id] = signals
    return dict(sorted(selected_signals.items())), selected_specs, (minute, audit)


def assert_no_order_capability(
    strategies: tuple[ExecutableEventTimeStrategy, ...],
) -> None:
    if any(strategy.broker_or_order_adapter for strategy in strategies):
        raise V71ExecutableStrategyError("order capability detected")


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "ExecutableEventTimeStrategy",
    "V71ExecutableStrategyError",
    "assert_no_order_capability",
    "frozen_signal_population",
    "load_executable_strategies",
]
