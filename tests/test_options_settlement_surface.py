from __future__ import annotations

from types import SimpleNamespace
import math

import pytest

from hydra.data.options_settlement_surface import (
    PRICE_SCALE,
    SettlementSurfaceError,
    SurfaceBuildDiagnostics,
    build_surface_snapshots,
)


DAY = 86_400_000_000_000


def definition(iid, recv, market, side, underlying, expiry, strike, *, user="N", action="A"):
    return SimpleNamespace(
        instrument_id=iid, ts_recv=recv, asset=market, instrument_class=side,
        underlying=underlying, expiration=expiry, strike_price=int(strike * PRICE_SCALE),
        raw_symbol=f"{underlying} {side}{strike}", user_defined_instrument=user,
        security_update_action=action,
    )


def settlement(iid, recv, ref, price, *, stat_type=3):
    return SimpleNamespace(
        instrument_id=iid, ts_recv=recv, ts_event=recv - 1, ts_ref=ref,
        price=int(price * PRICE_SCALE), stat_type=stat_type,
    )


def _two_term_records(ref=DAY):
    defs = []
    stats = []
    iid = 1
    for term, expiry, forward in (("ESU4", 20 * DAY, 100.0), ("ESZ4", 80 * DAY, 102.0)):
        for strike in (96.0, 98.0, 100.0, 102.0, 104.0):
            # Parity is exact: K + C - P == forward.
            call = 4.0 + max(forward - strike, 0.0)
            put = call - (forward - strike)
            defs += [definition(iid, 10, "ES", "C", term, expiry, strike), definition(iid + 1, 10, "ES", "P", term, expiry, strike)]
            stats += [settlement(iid, 100 + iid, ref, call), settlement(iid + 1, 120 + iid, ref, put)]
            iid += 2
    return sorted(defs, key=lambda row: row.ts_recv), sorted(stats, key=lambda row: row.ts_recv)


def test_put_call_parity_front_next_and_max_availability():
    defs, stats = _two_term_records()
    snapshots = list(build_surface_snapshots(defs, stats, source_hashes={"s": "a", "d": "b"}, markets=("ES",)))
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.status == "COMPLETE_FRONT_NEXT"
    assert snapshot.front_term.robust_forward == pytest.approx(100.0)
    assert snapshot.next_term.robust_forward == pytest.approx(102.0)
    assert snapshot.front_term.atm_straddle == pytest.approx(8.0)
    expected_proxy = math.sqrt(math.pi / 2.0) * 0.08 / math.sqrt(19.0 / 365.25)
    assert snapshot.front_term.days_to_expiry == pytest.approx(19.0)
    assert snapshot.front_term.atm_straddle_vol_proxy == pytest.approx(expected_proxy)
    assert snapshot.front_term.wing_moneyness == 0.02
    assert snapshot.front_next_term_slope == pytest.approx(
        snapshot.next_term.atm_straddle_vol_proxy - snapshot.front_term.atm_straddle_vol_proxy
    )
    assert snapshot.available_at_ns == max(row.ts_recv for row in stats)
    assert snapshot.snapshot_hash == snapshot.snapshot_hash.lower()


def test_definition_is_asof_receive_time_and_instrument_id_reuse_is_safe():
    # Instrument 1 is an ES call at t=10, then is reused as an NQ call at t=200.
    defs = [
        definition(1, 10, "ES", "C", "ESU4", 20 * DAY, 100),
        definition(2, 10, "ES", "P", "ESU4", 20 * DAY, 100),
        definition(1, 200, "NQ", "C", "NQU4", 20 * DAY, 200),
        definition(3, 200, "NQ", "P", "NQU4", 20 * DAY, 200),
    ]
    stats = [
        settlement(1, 100, DAY, 5), settlement(2, 101, DAY, 5),
        settlement(1, 300, 2 * DAY, 8), settlement(3, 301, 2 * DAY, 8),
    ]
    snapshots = list(build_surface_snapshots(defs, stats, source_hashes={}, minimum_pairs_per_term=1))
    by_key = {(row.settlement_reference_date, row.market): row for row in snapshots}
    assert by_key[("1970-01-02", "ES")].front_term.underlying == "ESU4"
    assert by_key[("1970-01-03", "NQ")].front_term.underlying == "NQU4"
    assert by_key[("1970-01-02", "NQ")].front_term is None
    assert by_key[("1970-01-03", "ES")].front_term is None


def test_filters_non_settlement_user_defined_and_non_option_records():
    defs = [
        definition(1, 10, "ES", "C", "ESU4", 20 * DAY, 100, user="Y"),
        definition(2, 10, "ES", "P", "ESU4", 20 * DAY, 100),
    ]
    stats = [settlement(1, 100, DAY, 5), settlement(2, 101, DAY, 5), settlement(2, 102, DAY, 5, stat_type=7)]
    diagnostics = SurfaceBuildDiagnostics()
    snapshots = list(build_surface_snapshots(defs, stats, source_hashes={}, minimum_pairs_per_term=1, diagnostics=diagnostics))
    assert diagnostics.statistics_seen == 3
    assert diagnostics.settlement_records_seen == 2
    assert diagnostics.missing_definition == 0
    assert diagnostics.ineligible_definition == 1
    assert all(row.front_term is None for row in snapshots)


def test_rejects_reference_regression_instead_of_revising_emitted_snapshot():
    defs, _ = _two_term_records()
    stats = [settlement(1, 100, 2 * DAY, 5), settlement(2, 101, DAY, 5)]
    with pytest.raises(SettlementSurfaceError, match="reference timestamp regressed"):
        list(build_surface_snapshots(defs, stats, source_hashes={}, markets=("ES",), minimum_pairs_per_term=1))


def test_robust_forward_recomputed_after_ten_percent_moneyness_filter():
    defs, stats = _two_term_records()
    # Add a complete but far-away strike with a deliberately absurd parity
    # value.  It participates in the initial estimate but is excluded before
    # the frozen recomputation and constituent count.
    next_iid = 100
    defs += [
        definition(next_iid, 10, "ES", "C", "ESU4", 20 * DAY, 1000),
        definition(next_iid + 1, 10, "ES", "P", "ESU4", 20 * DAY, 1000),
    ]
    stats += [settlement(next_iid, 500, DAY, 1000), settlement(next_iid + 1, 501, DAY, 1)]
    rows = list(
        build_surface_snapshots(
            sorted(defs, key=lambda row: row.ts_recv),
            sorted(stats, key=lambda row: row.ts_recv),
            source_hashes={},
            markets=("ES",),
        )
    )
    assert rows[0].front_term.robust_forward == pytest.approx(100.0)
    assert rows[0].front_term.paired_strike_count == 5
