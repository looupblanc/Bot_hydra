from __future__ import annotations

from typing import Any

from hydra.mission.mission_state import MissionPaths, append_jsonl, event_payload


def record_decision(paths: MissionPaths, decision: dict[str, Any]) -> None:
    append_jsonl(paths.decision_ledger, event_payload("decision", decision))


def record_evidence(paths: MissionPaths, evidence: dict[str, Any]) -> None:
    append_jsonl(paths.evidence_ledger, event_payload("evidence", evidence))


def record_engineering(paths: MissionPaths, task: dict[str, Any]) -> None:
    append_jsonl(paths.engineering_ledger, event_payload("engineering", task))

