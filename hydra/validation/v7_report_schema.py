from __future__ import annotations

import re


class V7ReportSchemaError(ValueError):
    pass


_HEADER = re.compile(
    r"^\[HYDRA-V7\] phase=\S+ step=\d+ "
    r"verdict=(?:GREEN|RED|ARTEFACT|BLOCKED|NULL|\S+)$"
)


def validate_v7_report_text(text: str) -> None:
    lines = text.splitlines()
    if not any(_HEADER.fullmatch(line) for line in lines):
        raise V7ReportSchemaError("HYDRA-V7 status header is missing")
    _require_line(lines, ("gate=", "preuve=", "tests="), "gate/proof/tests")
    _require_line(
        lines,
        ("budget_llm=", "budget_data=", "N_trials=", "burned="),
        "complete budget/multiplicity line",
    )
    governance = _require_line(
        lines,
        ("diff_validation=", "CONTRE="),
        "validation diff and CONTRE",
    )
    against = governance.split("CONTRE=", 1)[1].strip()
    if not against or against.lower() in {"aucun", "none", "n/a"}:
        raise V7ReportSchemaError("CONTRE must contain the strongest objection")
    _require_line(lines, ("prochaine_action=",), "next action")


def _require_line(
    lines: list[str], required: tuple[str, ...], description: str
) -> str:
    for line in lines:
        if all(token in line for token in required):
            return line
    raise V7ReportSchemaError(f"missing {description}")


__all__ = ["V7ReportSchemaError", "validate_v7_report_text"]
