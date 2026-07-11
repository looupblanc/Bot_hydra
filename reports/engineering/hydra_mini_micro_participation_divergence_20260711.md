# HYDRA mini/micro participation-divergence primary preregistration

- Protocol: `mini_micro_participation_divergence_primary_v1`
- Preregistered: 2026-07-11 UTC, before any result from this experiment
- Predecessor: negative `equity_preclose_inventory_dispersion_primary_v1`
- Data role: development and falsification only, strictly before `2024-10-01`
- Q4 / network / paid data / broker / orders: prohibited
- Maximum status: `PROMISING_RESEARCH_CANDIDATE`

## Decision information

Completed HYDRA reports repeatedly execute a micro contract from a signal formed
on the mini, but record `micro_signal_recomputed=false`.  No completed primary
tests whether relative mini/micro participation itself contains information.
This is a participant-segmentation representation, not another opening,
pre-close, barrier, generic hazard, geometry or scheduler threshold.

## Frozen population

Exactly 96 structures:

- pairs: ES/MES, NQ/MNQ, RTY/M2K, YM/MYM;
- participation state: `MICRO_DOMINANT` or `MINI_DOMINANT`;
- policy: continuation or reversal of the completed mini 5-minute return;
- causal absolute divergence threshold: q70 or q85;
- holding horizon: 15, 30 or 60 completed minutes.

Structural fingerprints are written before replay.  A mini/micro copy is one
economic hypothesis, never two independent strategies.

## Frozen representation and timing

Use explicit-contract, roll-guarded 1-minute OHLCV already cached.  Construct
closed 5-minute bars only.  For every pair, completed-bar phase and market:

`divergence = log1p(micro_volume / past_same_phase_micro_median_20_sessions)
              - log1p(mini_volume / past_same_phase_mini_median_20_sessions)`

The medians and threshold histories are shifted by one completed session.  The
current bar, current event outcome and later session state are excluded.  Both
mini and micro 5-minute return signs must agree.  Decisions are allowed from
09:00 through 14:00 America/Chicago.  Entry is the next 1-minute micro open,
there is at most one event per pair/session, and exit is the frozen horizon or
15:05, whichever comes first.  Mandatory account flatten remains before 15:10.

Reject missing or non-synchronized minutes, incomplete 5-minute bars, a mini
and micro mapped to inconsistent maturity, unsafe roll dates, DST/session
errors, future features or invalid multipliers.

## Successive halving

1. Logical validity and structural/behavioral dedupe.
2. 2023 screen: at least 35 events, positive 1.5x-cost net, one-micro MLL safe,
   and no catastrophic best-trade/day/month dependence.
3. Freeze at most eight QD elites using only 2023, at most two per target
   market and unique behavior fingerprints.
4. Replay frozen versions unchanged on 2024 Q1-Q3.
5. Candidate nulls, costs, +1/+5-minute delays, concentration and Topstep path.

No 2024 result may influence thresholds, structure, elite selection or sizing.

## Calibrated transfer and role

These are alpha candidates for `COMBINE_PASSER_POOL`.  They need positive
pooled economics at 1.5x costs, at least two supportive folds or one measurable
weak non-catastrophic period, account survival, and role-specific evidence.
They are not required to optimize XFA payouts or defensive utility
simultaneously.  Cross-market universality is not required; other pairs are
replication opportunities only when included in the frozen hypothesis.

Soft diagnostic weakness records uncertainty.  Hard invalidation, negative
pooled economics, catastrophic transfer, MLL breach, invalid execution or a
non-discriminative mandatory null prevents promotion.

## Mandatory nulls and baselines

- session-level circular shift of micro participation within target/phase and
  prior-volatility band;
- count/market/phase/volatility-matched opportunities;
- five-session block sign flip;
- mini-volume-only baseline;
- total-volume baseline.

Family correction covers the frozen eight-elite validation universe, while the
2023 elite selector is rerun inside the session-shift diagnostic to expose
selection-intensity uncertainty.  A candidate must add information beyond the
two volume baselines.

## Falsification and pivot

Falsify exact structures for hard integrity failure, fewer than 35 2023 events,
nonpositive 1.5x development economics, catastrophic transfer, mandatory-null
failure, cost/delay destruction, MLL breach or behavioral duplication.  Freeze
the whole family when there is no 2023 survivor or every frozen elite fails the
unchanged 2024 replay.  Do not respond with nearby thresholds.

## Required artifacts and ceilings

- immutable 96-structure manifest;
- 2023 screen and frozen elite manifest;
- complete event ledger with mini and micro contracts, closed-bar timestamps,
  feature availability and threshold-fit cutoff;
- fold, null, cost, delay, concentration and account evidence;
- QD archive and scientific report;
- absolute artifact paths and deterministic result hash.

Explicit ceilings: Q4 0, network 0, paid requests/spend 0, broker/orders 0,
`SHADOW_RESEARCH_ACTIVE=0`, `PAPER_SHADOW_READY=0`.

## Controller behavior

Queue automatically only after the frozen equity pre-close primary concludes
`PRECLOSE_PRIMARY_INSUFFICIENT_PIVOT_MARKET_ECOLOGY`.  Existing immutable
shadows continue fail-closed in parallel.  A survivor requires a new promotion
freeze.  A fully negative family requires a genuinely distinct next mechanism
or authorized forward evidence.

## Engineering boundaries

Allowed paths:

- `hydra/research/mini_micro_participation_divergence.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `tests/test_mini_micro_participation_divergence.py`
- `tests/test_portfolio_mutation_mission.py`

Protected: governance kernel, Q4/access/budget ledgers, mission/registry DBs,
cached data, active shadow configs and prior experiment artifacts.

Acceptance: deterministic 96→maximum-8 halving; causal same-phase history;
closed 5m; one event/session; mini/micro sync and roll guards; 2023-only freeze;
unchanged 2024 replay; costs/nulls/account evidence; all security ceilings; full
pytest, compileall, no-lookahead, integrity, governance, budget and secret
checks.  Roll back on nondeterminism, future information, status inheritance,
protected access or regression.

Expected decision information value: 0.94.  Incremental data cost: USD 0.
