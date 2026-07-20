from hydra.research.cross_era_bank_sieve import (
    ACCOUNT_LABELS,
    CLOSED_EXACT_BOOKS,
    HORIZONS,
    QUARANTINED_IDS,
    _selection_cell,
)


def test_inventory_guards_are_exact_and_bounded() -> None:
    assert len(QUARANTINED_IDS) == 6
    assert len(CLOSED_EXACT_BOOKS) == 2
    assert ACCOUNT_LABELS == ("50K", "100K", "150K")
    assert HORIZONS == (5, 10, 20)


def _scenario(*, net: float, progress: float, passes: int, breaches: int = 0):
    return {
        "pass_count": passes,
        "mll_breach_count": breaches,
        "full_coverage_start_count": 2,
        "target_progress_median": progress,
        "net_total_usd": net,
        "minimum_mll_buffer_usd": 1000.0,
    }


def test_requalification_cell_requires_positive_stress_and_hard_mll() -> None:
    values = {
        "NORMAL": {str(h): _scenario(net=10, progress=.1, passes=0) for h in HORIZONS},
        "STRESSED_1_5X": {
            str(h): _scenario(net=10, progress=.1, passes=0) for h in HORIZONS
        },
    }
    config = {"account_results": [{"account_label": "50K", "evaluation": {"summaries": values}}]}
    assert _selection_cell(config, 0.2) is not None
    values["STRESSED_1_5X"]["5"]["mll_breach_count"] = 1
    values["STRESSED_1_5X"]["10"]["net_total_usd"] = -1
    values["STRESSED_1_5X"]["20"]["target_progress_median"] = 0
    assert _selection_cell(config, 0.2) is None
