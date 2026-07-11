# Immutable Engineering Task — One-Shot Q4 Candidate Freeze v1

Task ID: `eng_q4_candidate_freeze_20260711_v1`

## Objective

Create an immutable, hash-addressed pre-Q4 dossier for the strongest eligible
`SHADOW_RESEARCH_CANDIDATE` emitted by the fresh equity open-gap continuation
pilot. This task freezes evidence; it does not read, acquire, summarize or
otherwise touch Q4 data.

## Deterministic selection

Only candidate IDs explicitly listed by the source result as
`q4_freeze_eligible_candidate_ids` may be selected. Rank by:

1. lowest family-adjusted candidate-null probability;
2. highest supportive temporal-fold count;
3. highest primary net after costs;
4. lexicographic candidate ID.

No threshold, market, direction, horizon, cost, sizing, validation policy or
candidate code may change after selection.

## Required freeze contents

- candidate ID/status and one-mechanism lineage;
- exact Git commit and hashes of strategy/event/status/shadow source modules;
- engineering-task, preregistration, continuation-result, trade-ledger and
  shadow-configuration paths plus SHA-256 hashes;
- exact direction, market, explicit-contract policy, entry/exit, threshold,
  minimum history, cost, sizing and timeframe definitions;
- complete fold, null, parameter, contract, concentration and Topstep evidence;
- calibrated shadow admission decision;
- data fingerprint and development end boundary;
- current governance manifest hash and Q4 access count;
- one-shot Q4 outcomes restricted to `Q4_LOCKBOX_PASS`,
  `Q4_LOCKBOX_FAIL`, or `Q4_LOCKBOX_INSUFFICIENT`;
- prohibition on mutation/reuse of Q4 for the exact candidate and lineage;
- zero outbound-order capability.

## Acceptance

- source continuation result is immutable and has zero Q4 access/spend;
- selected candidate is shadow-admitted, positive after costs, non-catastrophic,
  parameter/contract stable, MLL-safe and not event/fold dominated;
- shadow configuration hash verifies and outbound orders are disabled;
- trade ledger and all source hashes verify;
- governance and registry integrity pass;
- Q4 access count remains zero for the candidate lineage;
- freeze manifest is deterministic, immutable and self-hashed;
- no data acquisition or market-data read occurs in this task.

Rollback on any unresolved integrity issue, missing evidence, changed artifact,
Q4 access, mutable parameter, order capability or non-deterministic manifest.

Expected decision information value: `0.99`; data cost `$0`. A valid freeze is
the mandatory boundary before official cost estimation and one-shot Q4 access.
