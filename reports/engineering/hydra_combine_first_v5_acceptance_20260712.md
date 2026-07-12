# HYDRA Combine-First Evolution Factory V5 — acceptance report

- Baseline commit: `9b56ae070a43473c2951393329f773f926bd19fc`
- Engineering task SHA-256: `2a78971ff78292670261dcbdcb5eda3dfb1593b481be118aac322883f3386937`
- Protected Q4 policy changed: `false`
- Broker/order capability added: `false`
- Paid or network data used by validation smokes: `false`

## Correctness

- Full regression: `520 passed`.
- V5 focused regression: `26 passed`.
- Python compileall: passed.
- Mission DB integrity before integration: `ok`.
- Registry DB integrity before integration: `ok`.
- Governance YAML SHA-256 unchanged:
  `3c9fd63f43037c65d79ecd688ce76bc126c42cc6eeaceb6bed8636548ffaff57`.
- Governance semantic hash unchanged:
  `05810bc193e51e3c40722163a1ee3ae82fd3a8d7762c48eafa25c3a4cba1102b`.

The episode-start regime now uses the prior completed session.  A same-session
aggregate can no longer influence the state assigned to that session's start.
Overlapping rolling windows report an overlap-adjusted effective block count.

XFA Standard and XFA Consistency are both evaluated, but one aggregate policy
is selected per candidate.  The evaluator no longer uses a different
after-the-fact winner for every episode, and episodes without a payout no
longer claim post-payout survival.

## Determinism

Two isolated runs with the same population, episode policy and code produced
the same scientific result hash:

`0aeedce9a2fdd5b84dbc18cbd285b9ea6c074784da8c39e5a9e6fcfaf54ed103`

Runtime, RSS, worker scheduling and feature-cache temperature are deliberately
excluded from the scientific hash while remaining in observability fields.

## Four-vCPU capacity benchmark

The accepted first-cycle configuration is bounded at 5,000 requested new
structures.  The deterministic development-only capacity smoke completed in
approximately 75.5 seconds:

- structural proposals: `5,000`;
- Stage-1 survivors: `59`;
- exact parent replays: `119`, including `60` diversified historical seeds;
- mutation children: `40`;
- unique exact configurations: `159`;
- rolling Combine episodes: `3,816`;
- overlap-adjusted effective blocks: `936`;
- role-specific factory survivors: `28`;
- retained mutations: `6` (`15%`);
- Combine elites: `0`;
- XFA candidates: `3`;
- median MLL-breach rate: `0`;
- maximum observed MLL-breach rate: `0.25`;
- PAPER_SHADOW_READY: `0`.

The pre-backtest structural allocator proved that a fixed 10,000-candidate
request was infeasible under current family and lineage caps: maximum capacity
was 8,789.  V5 now downsizes only to the capacity reported before economics are
read.  Exhausting novel discovery structures does not stop targeted lineage
evolution.

## Persistent integration proof on a state copy

A transactionally copied mission database initialized exactly one queued
experiment:

`combine_first_evolution_v5_epoch_0000`

Its frozen specification requested 5,000 structures, 200 exact admissions, 60
targeted mutations and 24 rolling starts, with Q4, Q4 reuse, paid data,
network, broker and outbound orders disabled.  Completion routing is
idempotent and queues the next generation through the existing single writer.

This report establishes engineering acceptance, not strategy viability.  No
Combine elite or PAPER_SHADOW_READY strategy was produced by the acceptance
smokes.
