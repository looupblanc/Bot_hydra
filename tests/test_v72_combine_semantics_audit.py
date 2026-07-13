from __future__ import annotations

import json
from pathlib import Path

from hydra.validation.v72_combine_semantics_audit import (
    run_v72_combine_semantics_audit,
)


def test_v72_combine_semantics_audit_is_green_and_fail_closed(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    result = run_v72_combine_semantics_audit(
        project_root=root,
        output_dir=tmp_path,
    )

    assert result["verdict"] == "GREEN"
    assert all(result["checks"].values())
    assert result["legacy_evidence_mutated"] is False
    assert result["new_data_purchase_count"] == 0
    assert result["protected_holdout_access_count_delta"] == 0
    assert result["outbound_order_count"] == 0
    assert result["semantic_probes"]["statuses"]["operational_horizon"] == (
        "OPERATIONAL_HORIZON_NOT_REACHED"
    )
    assert result["semantic_probes"]["statuses"]["short_data"] == "DATA_CENSORED"
    assert result["semantic_probes"]["statuses"]["intraday_high_MFE"] == (
        "MLL_BREACHED"
    )
    stored = json.loads((tmp_path / "v72_combine_semantics_audit_result.json").read_text())
    assert stored["deployment_ticket_blockers"] == ["R2", "R6", "R7", "R11"]
    assert "## CONTRE" in (tmp_path / "v72_combine_semantics_audit_report.md").read_text()
