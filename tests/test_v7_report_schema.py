from __future__ import annotations

import pytest

from hydra.validation.v7_report_schema import (
    V7ReportSchemaError,
    validate_v7_report_text,
)


VALID_REPORT = """[HYDRA-V7] phase=4 step=100 verdict=NULL
gate=D2 preuve=result.json#12345678 tests=10/10
budget_llm=usage_non_exposee/solde budget_data=87/125 N_trials=247892 burned=1
diff_validation=aucun CONTRE=la_fenetre_est_courte
prochaine_action=cost_check_D2
"""


def test_complete_v7_report_schema_passes() -> None:
    validate_v7_report_text(VALID_REPORT)


def test_budget_llm_line_is_mandatory() -> None:
    with pytest.raises(V7ReportSchemaError, match="budget"):
        validate_v7_report_text(VALID_REPORT.replace("budget_llm=", "llm="))


def test_contre_cannot_be_empty() -> None:
    with pytest.raises(V7ReportSchemaError, match="CONTRE"):
        validate_v7_report_text(
            VALID_REPORT.replace("CONTRE=la_fenetre_est_courte", "CONTRE=")
        )
