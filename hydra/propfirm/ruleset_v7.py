from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Mapping


class RuleStatus(StrEnum):
    VERIFIED_HUMAN = "VERIFIED_HUMAN"
    WEB_SOURCED = "WEB_SOURCED"
    CONFLICT = "CONFLICT"
    ASSUMED = "ASSUMED"


@dataclass(frozen=True, slots=True)
class ExecutableRule:
    rule_id: str
    statement: str
    source: str
    date: str
    status: RuleStatus
    parameters: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class TopstepRulesetV7:
    schema: str
    as_of_date: str
    account_size_usd: int
    deployment_ticket_blocking_statuses: tuple[RuleStatus, ...]
    rules: tuple[ExecutableRule, ...]

    @property
    def by_id(self) -> dict[str, ExecutableRule]:
        return {rule.rule_id: rule for rule in self.rules}

    @property
    def deployment_ticket_blockers(self) -> tuple[str, ...]:
        blocked = set(self.deployment_ticket_blocking_statuses)
        return tuple(
            rule.rule_id for rule in self.rules if rule.status in blocked
        )

    @property
    def deployment_ticket_allowed(self) -> bool:
        return not self.deployment_ticket_blockers

    def validate(self) -> None:
        expected = {f"R{index}" for index in range(1, 17)}
        actual = {rule.rule_id for rule in self.rules}
        if actual != expected or len(self.rules) != len(expected):
            raise ValueError("ruleset must contain exactly R1 through R16")
        for rule in self.rules:
            if not rule.statement.strip() or not rule.source.strip() or not rule.date.strip():
                raise ValueError(f"{rule.rule_id} has incomplete provenance")
            if not rule.source.startswith("https://"):
                raise ValueError(f"{rule.rule_id} source is not HTTPS")


def default_ruleset_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "rulesets" / "topstep_150k_v7.json"


def load_ruleset(path: str | Path | None = None) -> TopstepRulesetV7:
    payload = json.loads((Path(path) if path else default_ruleset_path()).read_text(encoding="utf-8"))
    ruleset = TopstepRulesetV7(
        schema=str(payload["schema"]),
        as_of_date=str(payload["as_of_date"]),
        account_size_usd=int(payload["account_size_usd"]),
        deployment_ticket_blocking_statuses=tuple(
            RuleStatus(value)
            for value in payload["deployment_ticket_blocking_statuses"]
        ),
        rules=tuple(
            ExecutableRule(
                rule_id=str(value["id"]),
                statement=str(value["statement"]),
                source=str(value["source"]),
                date=str(value["date"]),
                status=RuleStatus(value["status"]),
                parameters=dict(value["parameters"]),
            )
            for value in payload["rules"]
        ),
    )
    ruleset.validate()
    return ruleset


__all__ = [
    "ExecutableRule",
    "RuleStatus",
    "TopstepRulesetV7",
    "default_ruleset_path",
    "load_ruleset",
]
