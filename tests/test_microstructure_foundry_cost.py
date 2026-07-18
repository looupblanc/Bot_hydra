from __future__ import annotations

from hydra.production.microstructure_foundry_cost import (
    CostMatrix,
    CostRow,
    ChronologicalRoles,
    generate_cost_matrix,
    select_bounded_acquisition_plan,
)


class _Metadata:
    def _value(self, **kwargs: object) -> float:
        schemas = {"trades": 1, "tbbo": 2, "mbp-1": 3, "mbp-10": 5, "mbo": 4}
        days = {"2024-07-12T21:00:00Z": 5, "2024-07-19T21:00:00Z": 10,
                "2024-08-02T21:00:00Z": 20, "2024-08-16T21:00:00Z": 30}
        return schemas[str(kwargs["schema"])] * days[str(kwargs["end"])] * len(kwargs["symbols"])  # type: ignore[arg-type]

    def get_record_count(self, **kwargs: object) -> int:
        return int(self._value(**kwargs) * 1000)

    def get_billable_size(self, **kwargs: object) -> int:
        return int(self._value(**kwargs) * 10000)

    def get_cost(self, **kwargs: object) -> float:
        # Combined two-market 5-session MBO costs USD 8 and is the direct winner.
        return self._value(**kwargs) / 5.0


def test_complete_matrix_and_direct_two_market_mbo_plan() -> None:
    matrix = generate_cost_matrix(_Metadata())
    assert len(matrix.rows) == 40
    assert len(matrix.combined_rows) == 20
    plan = select_bounded_acquisition_plan(
        matrix, actual_spend_usd=87.85289960354199, total_budget_usd=125.0
    )
    assert plan.strategy == "TWO_MARKET_MBO_DERIVE_LOWER_SCHEMAS"
    assert len(plan.requests) == 1
    assert plan.requests[0].schema == "mbo"
    assert plan.requests[0].session_count == 5
    assert plan.projected_incremental_spend_usd == 8.0
    assert plan.projected_remaining_usd > 25.0
    assert plan.purchase_authorized is False
    assert tuple(len(v) for v in (
        plan.roles_by_request[0].discovery,
        plan.roles_by_request[0].validation,
        plan.roles_by_request[0].final_development,
    )) == (3, 1, 1)


def test_hybrid_plan_when_combined_mbo_is_too_expensive() -> None:
    original = generate_cost_matrix(_Metadata())
    combined = tuple(
        CostRow(**{**row.to_dict(), "estimated_cost_usd": 11.0})
        if row.schema == "mbo" else row
        for row in original.combined_rows
    )
    # One-market MBO5 remains USD4; combined TBBO5 is USD4.
    matrix = CostMatrix(markets=original.markets, rows=original.rows, combined_rows=combined)
    plan = select_bounded_acquisition_plan(
        matrix, actual_spend_usd=87.85289960354199, total_budget_usd=125.0
    )
    assert plan.strategy == "TWO_MARKET_DEPLOYABLE_PLUS_ONE_MARKET_MBO_TEACHER"
    assert {row.schema for row in plan.requests} == {"mbp-1", "mbo"}
    assert plan.projected_incremental_spend_usd <= 10.0

