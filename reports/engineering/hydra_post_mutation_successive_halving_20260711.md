# HYDRA post-mutation successive-halving preregistration

Status: `IMMUTABLE_BEFORE_MUTATION_RESULT_READ`

Date: 2026-07-11 UTC

## Scientific objective

Convert the already-preregistered promising-lineage mutation batch into a small,
diversified bank of candidates worth further frozen promotion. The procedure is
not a promotion to shadow, `PAPER_SHADOW_READY`, or funded deployment. It must
separate three interoperable account objectives:

- `COMBINE_PASSER_POOL`: target-before-MLL probability, time to target,
  consistency margin, cost and tail risk;
- `XFA_PAYOUT_POOL`: payout-cycle proxy, qualifying-day frequency, MLL and
  post-payout survival, payout timing;
- `DEFENSIVE_ACCOUNT_POOL`: marginal account utility, drawdown/MLL protection
  and shared-loss reduction versus matched controls.

No candidate is required to optimize all three objectives. A market-specific
hypothesis is not required to replicate in an unrelated ecology. One weak
development fold is admissible when pooled economics remain positive, the weak
fold is non-catastrophic, the simulated account path survives, and the failure
regime is measurable.

## Frozen inputs and provenance

`run_post_mutation_successive_halving` accepts only explicit paths and SHA-256
hashes for:

1. this engineering task;
2. the immutable promising-lineage mutation result;
3. its immutable child trade ledger;
4. the code commit used for the run.

All hashes are verified before parsing candidate evidence. The run rejects Q4
or later timestamps, nonzero Q4 access, live/broker/order capability, inherited
statuses/passes, parent mutation, missing child provenance, non-finite economics,
and malformed/ambiguous trade records. The development boundary is exclusively
`2024-10-01T00:00:00Z`.

## Frozen rounds

### Round 0 — hard integrity and behavioral deduplication

Reject fatally:

- source/hash drift;
- protected-period access;
- order capability;
- lookahead or a decision timestamp after its admitted information timestamp;
- status/pass inheritance;
- changed frozen parent;
- duplicate candidate IDs;
- duplicate trade keys inside one child;
- identical behavioral fingerprints counted as distinct strategies.

An explicitly labelled backup/risk implementation may remain in the audit but
cannot consume an elite niche. Behavioral identity is determined from the
ordered tuple `(timestamp, event_session_id, symbol, active_contract)`; the
first canonical candidate wins deterministically.

### Round 1 — vectorized economic and sample screen

Compute event count, pooled gross/net, cost, active months, win rate, average
trade, total/best-trade concentration, day/month concentration, fold economics,
and chronological account path. Negative pooled net is a scientific failure,
not an integrity failure. Low samples and concentration are recorded as
uncertainty and may continue only when later role-specific evidence is usable.

### Round 2 — temporal and cost screen

Use only preregistered development folds present in the frozen ledger. Require
positive pooled net and at least one positive fold. Permit at most one negative
fold when all hold:

- its absolute loss is no more than 75% of total positive-fold profit;
- it does not create an MLL breach on the chronological account path;
- no single best trade or day explains pooled profitability;
- its calendar/regime label is retained in the dossier.

Cost stresses are `1.0x`, `1.5x`, and `2.0x` recorded costs. Positive `1.5x`
net is required for a promising alpha/Combine/XFA child. `2.0x` is diagnostic.
Defensive children are assessed on marginal utility and matched-control evidence
rather than standalone PnL, but missing such evidence yields
`INSUFFICIENT_EVIDENCE`, never a fabricated pass.

### Round 3 — sequence, block-bootstrap and role-specific evidence

Sequence attacks remove the best trade, best day and best month separately and
apply deterministic worst-first and seeded block permutations. A circular block
bootstrap uses block length `max(2, round(sqrt(n)))`, 2,048 resamples and a seed
derived from the candidate ID. It reports the 5th/50th/95th percentiles of net,
maximum drawdown and objective utility. These are calibrated robustness
diagnostics, not universal fatal gates.

Role-specific rules:

- `COMBINE_PASSER_POOL`: rank target-before-MLL probability, median target time,
  consistency margin, cost resilience and tail drawdown. Failure to hit the
  target is insufficient evidence, not an integrity kill.
- `XFA_PAYOUT_POOL`: rank payout cycles before ruin, qualifying-day frequency,
  payout timing, MLL survival and post-payout survival. A positive alpha path
  without enough payout observations remains insufficient.
- `DEFENSIVE_ACCOUNT_POOL`: require positive marginal account utility versus a
  matched random inclusion/deactivation distribution. Standalone profit is not
  a substitute. Missing baseline/control fields is insufficient evidence.

Soft failures become uncertainty flags and risk-adjusted rank penalties. Only
hard integrity failures are fatal.

### Round 4 — maximum-feasible diversified archive

From nonduplicate candidates classified no higher than
`PROMISING_RESEARCH_CANDIDATE`, choose deterministically by Pareto dominance and
then stable role-specific score. Preserve objective-pool, mechanism-family,
market-ecology and parent-lineage niches. Apply maximum shares only when
mathematically feasible: family 25%, ecology 40%, lineage 2%; unused capacity is
redistributed and a missing ecology cannot make selection infeasible. The
archive target is the maximum feasible set up to 12 candidates. Ties resolve by
canonical candidate ID. Backups and controls do not count as elites.

## Status ceiling and prohibited conclusions

Outputs are restricted to:

- `PROMISING_RESEARCH_CANDIDATE`;
- `INSUFFICIENT_EVIDENCE`;
- `RESEARCH_REJECTED` for soft scientific failure;
- `HARD_INTEGRITY_REJECTED` for a fatal invariant.

The capability may not assign `SHADOW_RESEARCH_ACTIVE`,
`PAPER_SHADOW_READY`, `TRADING_READY_CANDIDATE`, or any funded/live status. It
may not inherit parent evidence. Q4 access count and outbound order capability
must both remain zero.

## Determinism, outputs and acceptance tests

The implementation must use vectorized arrays for bootstrap/account-path work,
fixed candidate-derived seeds, canonical JSON, atomic output writes and complete
source hashes. It emits a result JSON, candidate evidence table, selected-elite
manifest and audit report under the caller-provided output directory.

Acceptance tests cover source/hash provenance, Q4/order rejection, no-lookahead,
behavioral deduplication, one weak non-catastrophic fold, catastrophic-fold
rejection, cost stress, deterministic block bootstrap, soft-versus-hard
classification, all three objective pools, maximum-feasible diversity and the
status ceiling.

Rollback conditions are any nondeterminism, provenance drift, leakage, false
promotion, clone inflation, Q4 access, order capability, or regression of the
existing test suite.
