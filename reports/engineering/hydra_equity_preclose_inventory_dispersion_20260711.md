# HYDRA equity pre-close inventory/dispersion primary preregistration

- Protocol: `equity_preclose_inventory_dispersion_primary_v1`
- Preregistered: 2026-07-11 UTC
- Research role: distinct mechanism/market-session pivot after the negative
  role-conditioned account-policy epoch
- Data role: development and falsification only
- Permitted range: `2023-01-01` through strictly before `2024-10-01`
- Q4 / network / paid data / broker / orders: prohibited
- Maximum output status: `PROMISING_RESEARCH_CANDIDATE`

## Why this pivot

The six account policies reused the same 630 elite events.  Two improved an
objective utility but worsened core drawdown, and none beat every role-specific
risk gate.  More scheduler mutations would have low decision information.

The pre-close RTH interval is absent from the completed research grammars.  It
has a distinct economic mechanism: inventory reduction, benchmark/rebalance
flow and closing-auction preparation can either pull an inefficient residual
leader/laggard back toward the index complex or extend a broad, efficient common
move.  This is not another opening gap, barrier, generic hazard or daily-lag
variant.

## Frozen universe and execution

- Signal markets: `ES`, `NQ`, `RTY`, `YM`.
- Execution instruments: `MES`, `MNQ`, `M2K`, `MYM`.
- Source: existing explicit-contract, date-aware, roll-guarded 1-minute OHLCV.
- Decision times: `14:15` and `14:45` America/Chicago only.
- Last possible exit: `15:05` America/Chicago.
- Mandatory account flatten remains before `15:10`.
- Decisions use only fully closed 1-minute and 15-minute bars.
- Cross-market joins require the same completed timestamp and trading session.
- One micro leg only; no pair legging, hedge proxy or mini/micro clone counting.

## Frozen structural population

Exactly 32 structural hypotheses:

- four target markets;
- two mechanisms:
  - `RESIDUAL_DISPERSION_CONVERGENCE`: extreme target residual versus the
    equal-risk past-only common factor, low breadth and inefficient target path;
  - `BROAD_INVENTORY_CONTINUATION`: common 4-of-4 displacement, high breadth,
    efficient paths and supportive participation;
- two decision times (`14:15`, `14:45`);
- two causal threshold levels (`q70`, `q85`).

Thresholds, risk scaling and any common-factor weights are expanding and
strictly past-only in 2023.  They freeze at 2023 year-end for every 2024 Q1-Q3
decision.  No threshold may be changed after 2024 replay.

## Features and availability

At each decision, compute from completed bars only:

- session displacement from the completed RTH open reference;
- standardized residual displacement versus a past-only equal-risk common
  factor;
- cross-market dispersion and breadth;
- signed path length and path efficiency;
- completed-volume participation versus past same-clock distributions;
- prior realized volatility and same-clock opportunity frequency.

Every event row records signal/contract, source-bar close, availability and
decision timestamps, trading session, threshold-fit cutoff, roll-map hash and
data fingerprint.  Partial bars, contemporaneous target outcomes and future
session state are prohibited.

## Direction and horizon

- Convergence: trade the target residual toward the common factor.
- Continuation: trade in the sign of the common factor only when all four
  markets agree and target participation confirms.
- Exit at the earlier of the frozen horizon (`20` or `50` completed minutes as
  implied by decision time to 15:05), invalidation state, or 15:05.
- Costs use current registered micro round-turn assumptions; replay at `1.0x`
  and `1.5x`, with diagnostic `2.0x`.

## Multi-fidelity evaluation

1. Logical validity, explicit contract/session/DST/roll checks and structural
   fingerprint deduplication.
2. 2023 screen: minimum 40 events, positive net at `1.5x` costs, no MLL breach,
   best-trade/day/month removed net not catastrophically negative.
3. Quality-diversity freeze: at most four elites, at most two per mechanism,
   at most two per decision time, no behavioral duplicate, selected using 2023
   only.
4. Replay frozen elites unchanged on 2024 Q1-Q3.
5. Candidate-level nulls, FDR/BH at the exact 32-member family scope, cost and
   1/5-minute delay, concentration, MLL and Topstep path.

Every 2024 result is development-role evidence, not a holdout.

## Controls and simpler explanations

For each frozen elite:

- matched events with the same target, clock, volatility band and opportunity
  frequency;
- time-of-day-only baseline;
- target-displacement-only baseline;
- breadth-only baseline;
- sign-flip/block null preserving session clustering.

The candidate must outperform the applicable simpler explanation; component
evidence is never inherited by a strategy.

## Promotion semantics

An exact candidate may become `PROMISING_RESEARCH_CANDIDATE` when:

- pooled 2023+2024Q1-Q3 net is positive at `1.5x` costs;
- at least two temporal folds are supportive, or exactly one weak fold is
  non-catastrophic (no MLL breach and loss no worse than 50% of pooled positive
  net);
- adjusted candidate-null probability is at most `0.10`;
- best-trade/day/month removal is non-catastrophic;
- delay, contract, session and MLL checks pass;
- no hard invalidation exists.

Not every quarter must be profitable.  Universal transfer to metals/energy is
not required because the hypothesis is equity-index pre-close inventory flow.
This capability cannot activate Shadow or confer Paper/funded status.

## Falsification

Reject exact structures when:

- 2023 has fewer than 40 events or nonpositive `1.5x` net;
- frozen 2024 replay is pooled nonpositive or catastrophically inconsistent;
- the adjusted null is nondiscriminative;
- costs/delay remove economics;
- MLL, contract limit, consistency, session or flatten fails;
- the trade path is behaviorally duplicated.

Abort the whole experiment on future bars, cross-market timestamp mismatch,
invalid contract/roll/multiplier/DST, Q4 row, impossible fill, status
inheritance, protected-path mutation, network/data spend or order capability.

## Required artifacts

- immutable 32-structure manifest and fingerprints;
- 2023 screen ledger;
- frozen elite manifest (maximum four);
- complete event/trade ledger with availability provenance;
- 2024 fold and pooled evidence;
- candidate null/FDR, baselines, cost/delay/concentration/account evidence;
- QD/Pareto archive and report;
- deterministic result hash and absolute artifact paths.

Explicit result ceilings: Q4 `0`, spend `0`, network `0`, orders `0`,
`SHADOW_RESEARCH_ACTIVE=0`, `PAPER_SHADOW_READY=0`.

## Controller behavior

After a negative role-conditioned epoch, queue this experiment automatically
while existing shadows remain fail-closed.  A positive exact candidate requires
a new frozen promotion task.  A fully negative result pivots away from this
pre-close family; it may not trigger nearby thresholds or another scheduler
mutation.

## Allowed engineering paths

- `hydra/research/equity_preclose_inventory_dispersion.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `tests/test_equity_preclose_inventory_dispersion.py`
- `tests/test_portfolio_mutation_mission.py`

## Protected paths

Governance kernel, Q4/access/budget ledgers, mission/registry databases, cached
market data, active shadow configs and all existing experiment artifacts.

## Acceptance and rollback

Acceptance requires deterministic 32→maximum-4 halving, causal closed-bar and
cross-market timing tests, DST/session/roll/flatten tests, 2023-only selection,
unchanged 2024 replay, costs/nulls/account path, no status inheritance and all
security zeros.  Run full pytest, compileall, no-lookahead, integrity,
governance, budget, secret and smoke checks.

Rollback on nondeterminism, result-aware threshold changes, future information,
infeasible execution, protected access, status inflation or regression.

Expected decision information value: `0.93`; implementation/data cost is
bounded and existing data cost is zero.
