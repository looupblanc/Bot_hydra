# HYDRA role-conditioned account-structure epoch preregistration

- Protocol: `role_conditioned_structural_epoch_v1`
- Preregistered: 2026-07-11 UTC
- Data role: `DEVELOPMENT_AND_FALSIFICATION_ONLY`
- Latest permitted timestamp: strictly before `2024-10-01T00:00:00Z`
- Q4 / network / paid data / broker / orders: prohibited
- Maximum structures: exactly six, two per objective pool
- Maximum status: `PROMISING_RESEARCH_CANDIDATE`

## Scientific objective

Test whether the failure of the first account-role study was caused by its
binary whole-day deactivation grammar rather than by an absence of executable
account utility.  This is a targeted structural epoch, not a blind tournament
and not independent temporal replication.  The 2024 development results have
already influenced this grammar and must remain labelled development-role
evidence.

The epoch keeps three distinct objectives:

- `COMBINE_PASSER_POOL`: target-before-MLL probability, target velocity,
  consistency margin, execution cost and tail risk;
- `XFA_PAYOUT_POOL`: payout cycles before ruin, qualifying-day frequency,
  payout timing, MLL survival and post-payout survival;
- `DEFENSIVE_ACCOUNT_POOL`: marginal drawdown, MLL-buffer, shared-loss-day and
  directional-conflict utility versus matched controls.

No strategy must optimize all three objectives.

## Frozen inputs

Consume only path-plus-SHA verified artifacts from:

1. `promising_lineage_mutation_v1` result and event ledger;
2. `post_mutation_successive_halving_evidence_v2` result, evidence and elite
   manifest;
3. `portfolio_role_research_v1` result;
4. `post_portfolio_mutation_meta_allocation_v1` result.

Every input must report Q4 access, network requests, paid requests and order
capability as zero.  Parent and child statuses are evidence only and cannot be
inherited.

## Six frozen structures

All fitting and ranks use 2023 completed events only.  A decision may use only
state strictly prior to its signal timestamp.

### COMBINE_PASSER_POOL

1. `combine_collision_rank_scheduler_v1`: when same-session signals collide,
   retain one using a frozen 2023 rank based on target progress per unit of
   adverse excursion; never inspect current-event outcome.
2. `combine_prior_mae_micro_budget_v1`: allocate integer micro contracts from a
   frozen 2023 per-lineage MAE budget, capped by shared contract and MLL limits.

### XFA_PAYOUT_POOL

3. `xfa_realized_qualifying_day_latch_v1`: stop opening new signals only after
   the qualifying-day threshold has already been reached by realized PnL that
   day; incomplete/open-trade PnL is prohibited.
4. `xfa_prior_qualifying_frequency_scheduler_v1`: resolve collisions with a
   frozen 2023 rank based on qualifying-day frequency and payout progress per
   drawdown unit.

### DEFENSIVE_ACCOUNT_POOL

5. `defensive_redundant_collision_suppressor_v1`: suppress only the lower-ranked
   redundant simultaneous signal; do not deactivate whole days.
6. `defensive_prior_mae_quantile_throttle_v1`: reduce to one micro only when the
   prior completed-event MAE/drawdown state exceeds the frozen 2023 quantile.

No stop/target grid, neighboring thresholds, crossover, or post-result repair
is permitted in this version.

## Evaluation

- Replay unchanged on 2023 and 2024 Q1-Q3 with explicit-contract event rows.
- Reconcile gross PnL minus costs to net PnL.
- Enforce shared MLL, contract limits, simultaneous signals, session flatten,
  conflicts, shared loss days and conservative one-micro execution.
- Report 1.0x and 1.5x costs.
- Report phase-specific account utility and every component separately.
- Use deterministic matched controls: circular session shifts for schedulers,
  count-matched random suppressions for defensive structures, and fixed seeds
  derived from the immutable structure ID.
- A role candidate is promising only when its primary utility delta is positive,
  it introduces no hard risk violation, its matched one-sided p-value is at most
  0.20, and its role-specific core risk metric does not deteriorate.
- Soft failures become `INSUFFICIENT_EVIDENCE`; hard integrity failures become
  `HARD_INTEGRITY_REJECTED`.

## Diversity and counting

Create structural and behavioral fingerprints before replay.  Reject exact
duplicates.  These are account policies, not new alpha mechanisms.  Count only
nonduplicate structures as prototypes and keep separate MAP/Pareto niches by
pool.  No family may exceed 25% of the continuing broad research allocation;
this bounded six-policy audit is exempt only from that arithmetic, not from
future allocation caps.

## Required artifacts

- immutable result JSON;
- six-policy manifest with fingerprints and source hashes;
- policy evidence JSONL;
- transformed event ledger JSONL;
- role-specific Pareto archive;
- Markdown report;
- deterministic result hash and artifact SHA-256 values.

The result must explicitly report Q4 access `0`, network `0`, incremental spend
`0`, order capability `false`, `PAPER_SHADOW_READY=0`, and
`SHADOW_RESEARCH_ACTIVE=0`.

## Controller behavior

After successful activation of the admitted mutation child, queue this epoch in
the Discovery/Portfolio pipeline.  Shadow remains fail-closed and independent.
On completion:

- if one or more structures are promising, queue a new frozen role-specific
  promotion specification requirement;
- otherwise pivot to a distinct mechanism/market-ecology action;
- never return to an unhandled scheduler blocker merely because this epoch is
  negative.

## Allowed engineering paths

- `hydra/research/role_conditioned_structural_epoch.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `tests/test_role_conditioned_structural_epoch.py`
- `tests/test_portfolio_mutation_mission.py`

## Protected paths

Governance kernel, data-role ledger, Q4/access ledgers, budget ledger, mission
database, registry database, raw/cached market data, active parent shadow
configurations and existing experiment artifacts must not be modified.

## Acceptance tests

- exact six deterministic structures and two per pool;
- 2023-only fitting and strictly prior state;
- role-separated utilities and controls;
- shared-account risk and 1.5x costs;
- fingerprint/dedup and no status inheritance;
- Q4/network/spend/order all zero;
- restart-safe immutable artifacts;
- controller automatic queue and result routing;
- full pytest, compileall, no-lookahead, integrity, governance, secret and
  deterministic smoke checks.

## Rollback conditions

Rollback if results change with identical inputs, if 2024 affects fitted ranks
or thresholds, if a current-event outcome enters a decision, if statuses are
inherited, if a structure is counted as alpha, if any protected data/network/
order capability appears, or if the mission cannot resume the next action.

## Expected decision information value

High (`0.91`): four selected mutation elites are Combine-oriented, XFA near
misses remain MLL-safe but underproduce payout cycles, and three naive defensive
deactivations failed matched controls.  This six-way audit directly tests the
account-scheduling grammar that now separates passage, payout and protection,
at zero data cost and without reopening Q4.
