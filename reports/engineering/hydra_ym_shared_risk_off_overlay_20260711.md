# Immutable Research Task — YM Shared-Risk-Off Overlay v1

Task ID: `eng_ym_shared_risk_off_overlay_20260711_v1`

Frozen before executing this child formulation.

## Scientific objective

Test whether a causal shared-risk state can deactivate the frozen development
signals of `strategy_open_gap_continuation_YM_v1`, reducing MLL/drawdown and
loss-day concentration while retaining useful economics. This is a new child
ID and a defensive activation mechanism, not a mutation or Q4 retest of the
already frozen parent.

## Frozen formulation

- child ID: `strategy_open_gap_continuation_YM_riskoff_v1`;
- parent entry/exit/direction/cost/contract rules remain unchanged;
- candidate events come only from the parent's immutable pre-Q4 trade ledger;
- at each 08:31 CT decision, use past-only features on ES/NQ/RTY/YM:
  - mean 120-bar realized volatility;
  - mean downside/shared-loss state;
  - cross-market dispersion of lagged 60-bar returns;
- each component becomes an expanding percentile against prior decision days
  only, requiring 40 prior days; shared-risk score is their equal-weight mean;
- deactivate the parent signal when score >= 0.80; otherwise retain it exactly;
- no new entry is created and no size is increased;
- require at least 60% of parent events retained;
- primary utility success requires positive retained net, at least 15% lower
  maximum cumulative drawdown, non-worse worst-event MAE, no new catastrophic
  fold and either >=80% of parent net retained or higher net;
- compare with 4,096 deterministic opportunity-count-matched random skip
  controls; record utility probability and event overlap/novelty;
- folds: 2023 H2, 2024 Q1, Q2 and Q3;
- score thresholds 0.75/0.85 and equal-weight component ablations are
  diagnostic only;
- development/falsification data end exclusive 2024-10-01; Q4, paid data,
  network, broker/live and outbound orders prohibited.

## Governance boundary

The parent has a frozen Q4 manifest. This new child may not access or reuse Q4
for that lineage. It may only gather future zero-risk shadow evidence or use a
later untouched period under a distinct approved protocol.

Leakage, current/future feature use, parent-ledger changes, hidden resizing,
contract/cost changes, invalid MLL accounting, Q4 access, governance or order
capability are fatal.

## Allowed paths

- `hydra/research/ym_shared_risk_off_overlay.py`
- controller/runner/status integration
- focused tests and immutable artifacts

## Protected paths

All governance files, Q4/future lockboxes, frozen parent artifacts,
registry/mission databases, raw market data, credentials and broker/order
modules.

Expected decision information value: `0.95`; data cost `$0`. This directly
tests portfolio/MLL uncertainty and creates no additional directional exposure.
