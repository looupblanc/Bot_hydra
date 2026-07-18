from __future__ import annotations

from pathlib import Path

from hydra.data.budget import DatabentoBudgetConfig
from hydra.economic_evolution.schema import stable_hash
from hydra.production.microstructure_foundry_cost import (
    generate_cost_matrix,
    select_bounded_acquisition_plan,
)
from scripts.acquire_microstructure_foundry_0031 import (
    acquire_frozen_bundle,
    validate_frozen_requests,
)


class _Metadata:
    def _base(self, **kwargs: object) -> float:
        sessions = {"2024-07-12T21:00:00Z": 5, "2024-07-19T21:00:00Z": 10,
                    "2024-08-02T21:00:00Z": 20, "2024-08-16T21:00:00Z": 30}
        ranks = {"trades": 1, "tbbo": 2, "mbp-1": 3, "mbp-10": 5, "mbo": 4}
        return sessions[str(kwargs["end"])] * ranks[str(kwargs["schema"])] * len(kwargs["symbols"])  # type: ignore[arg-type]

    def get_record_count(self, **kwargs: object) -> int:
        return int(self._base(**kwargs) * 1000)

    def get_billable_size(self, **kwargs: object) -> int:
        return int(self._base(**kwargs) * 10000)

    def get_cost(self, **kwargs: object) -> float:
        return self._base(**kwargs) / 5.0


class _Client:
    metadata = _Metadata()


def _inputs(root: Path) -> tuple[dict, dict]:
    manifest_hash = "a" * 64
    manifest = {
        "campaign_id": "hydra_microstructure_order_flow_foundry_0031",
        "campaign_mode": "MICROSTRUCTURE_ORDER_FLOW_FOUNDRY",
        "development_only": True,
        "manifest_hash": manifest_hash,
        "bounded_acquisition": {
            "provider": "Databento", "dataset": "GLBX.MDP3",
            "q4_access_allowed": False,
            "broad_historical_purchase_allowed": False,
            "maximum_initial_spend_usd": 10.0,
            "minimum_budget_reserve_usd": 25.0,
            "total_budget_usd": 125.0,
        },
        "runtime": {"output_dir": "reports/economic_evolution/test_0031"},
    }
    matrix = generate_cost_matrix(_Metadata())
    plan = select_bounded_acquisition_plan(
        matrix, actual_spend_usd=0.0, total_budget_usd=125.0
    )
    core = {
        "schema": "hydra_databento_microstructure_0031_cost_matrix_v1",
        "campaign_id": manifest["campaign_id"],
        "manifest_hash": manifest_hash,
        "metadata_only": True,
        "purchase_authorized": False,
        "matrix": matrix.to_dict(),
        "acquisition_plan": plan.to_dict(),
    }
    return manifest, {**core, "cost_matrix_hash": stable_hash(core)}


def test_frozen_request_is_two_market_mbo_and_dry_run_cannot_spend(tmp_path: Path) -> None:
    manifest, report = _inputs(tmp_path)
    requests = validate_frozen_requests(manifest, report, root=tmp_path)
    assert len(requests) == 1
    assert requests[0]["schema"] == "mbo"
    assert requests[0]["symbols"] == ["NQU4", "YMU4"]
    assert requests[0]["session_count"] == 5
    budget = DatabentoBudgetConfig(
        ledger_path=str(tmp_path / "spend.jsonl"),
        summary_path=str(tmp_path / "summary.md"),
    )
    result = acquire_frozen_bundle(
        manifest=manifest, cost_report=report, root=tmp_path,
        client=_Client(), execute=False, budget=budget,
    )
    assert result["download_status"] == "DRY_RUN_ONLY"
    assert result["aggregate_live_estimate_usd"] == 8.0
    assert not (tmp_path / "spend.jsonl").exists()
