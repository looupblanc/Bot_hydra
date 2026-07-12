from __future__ import annotations

from hydra.validation.v71_mechanism_forensics import run_v71_mechanism_forensics


def test_forensics_preserves_historical_status_and_kills_intra_product_arb(
    tmp_path,
) -> None:
    result = run_v71_mechanism_forensics(
        project_root=".",
        output_dir=tmp_path,
    )

    assert result["D1_0001"]["status_resurrected"] is False
    assert result["D1_0002"]["status_resurrected"] is False
    assert result["D1_0002"]["validator_calibration_affected"] is False
    assert result["MINI_MICRO_DIVERGENCE"]["operational_class"] == "ARB_INTRA_PRODUIT"
    assert result["MINI_MICRO_DIVERGENCE"]["mechanism"] == "MECHANISM_CONFIRMED_DEAD"
    assert result["MINI_MICRO_DIVERGENCE"]["reformulation_allowed"] is False
    assert len(result["ES_EXTREME_REJECTION"]["bounded_future_reformulations"]) == 3
