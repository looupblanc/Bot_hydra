from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hydra.utils.config import project_path


AUTO_UNDER_HARD_CAP = "AUTO_UNDER_HARD_CAP"


@dataclass(frozen=True)
class DatabentoBudgetConfig:
    budget_start: str = "2026-07-10"
    hard_cap_usd: float = 100.0
    safety_ceiling_usd: float = 98.0
    ledger_path: str = "reports/data_budget/databento_spend_ledger.jsonl"
    summary_path: str = "reports/data_budget/databento_budget_summary.md"


@dataclass(frozen=True)
class DatabentoSpendRecord:
    request_id: str
    timestamp_utc: str
    dataset: str
    schema: str
    symbols: list[str]
    stype_in: str
    start: str
    end: str
    estimated_cost_usd: float
    actual_cost_usd: float | None
    cumulative_estimated_spend_usd: float
    cumulative_actual_spend_usd: float
    cache_hit: bool
    research_purpose: str
    candidate_tier: str
    approval_mode: str
    resulting_file: str | None
    checksum: str | None
    download_status: str

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)


class DatabentoBudgetError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def request_id_for(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:20]


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_ledger(path: str | Path) -> list[dict[str, Any]]:
    ledger = Path(path)
    if not ledger.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in ledger.read_text(encoding="utf-8").splitlines():
        if line.strip():
            records.append(json.loads(line))
    return records


def cumulative_spend(path: str | Path) -> tuple[float, float]:
    estimated = 0.0
    actual = 0.0
    for row in read_ledger(path):
        if row.get("download_status") in {"ESTIMATED_ONLY", "DOWNLOADED", "CACHE_HIT"}:
            estimated += float(row.get("estimated_cost_usd") or 0.0)
        actual += float(row.get("actual_cost_usd") or 0.0)
    return estimated, actual


def enforce_budget(config: DatabentoBudgetConfig, incremental_estimate_usd: float) -> tuple[float, float]:
    current_estimated, current_actual = cumulative_spend(project_path(config.ledger_path))
    projected = current_estimated + float(incremental_estimate_usd)
    if projected > config.hard_cap_usd:
        raise DatabentoBudgetError(
            f"Projected Databento spend ${projected:.2f} exceeds hard cap ${config.hard_cap_usd:.2f}."
        )
    if projected > config.safety_ceiling_usd:
        raise DatabentoBudgetError(
            f"Projected Databento spend ${projected:.2f} exceeds automatic safety ceiling ${config.safety_ceiling_usd:.2f}."
        )
    return projected, current_actual


def append_spend_record(config: DatabentoBudgetConfig, record: DatabentoSpendRecord) -> None:
    path = project_path(config.ledger_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(record.to_json() + "\n")
    write_budget_summary(config)


def write_budget_summary(config: DatabentoBudgetConfig) -> Path:
    ledger_path = project_path(config.ledger_path)
    summary_path = project_path(config.summary_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    records = read_ledger(ledger_path)
    estimated, actual = cumulative_spend(ledger_path)
    remaining_hard = max(config.hard_cap_usd - actual, 0.0)
    remaining_safety = max(config.safety_ceiling_usd - estimated, 0.0)
    lines = [
        "# Databento Budget Summary",
        "",
        f"- Budget start: {config.budget_start}",
        f"- Hard cap USD: {config.hard_cap_usd:.2f}",
        f"- Safety ceiling USD: {config.safety_ceiling_usd:.2f}",
        f"- Cumulative estimated spend USD: {estimated:.6f}",
        f"- Cumulative actual spend USD: {actual:.6f}",
        f"- Remaining hard-cap budget USD: {remaining_hard:.6f}",
        f"- Remaining automatic safety budget USD: {remaining_safety:.6f}",
        f"- Ledger records: {len(records)}",
        "",
        "## Recent Records",
    ]
    for row in records[-20:]:
        lines.append(
            f"- {row.get('timestamp_utc')} {row.get('dataset')} {row.get('schema')} "
            f"{row.get('symbols')} {row.get('start')} to {row.get('end')} "
            f"estimate=${float(row.get('estimated_cost_usd') or 0.0):.6f} "
            f"actual=${float(row.get('actual_cost_usd') or 0.0):.6f} "
            f"status={row.get('download_status')}"
        )
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary_path

