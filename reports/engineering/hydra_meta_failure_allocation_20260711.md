# HYDRA Meta Failure-Allocation Audit

- Task ID: `hydra_meta_failure_allocation_20260711`
- Created: 2026-07-11 UTC
- Pipeline: Meta-Research
- Parallel-safe: yes; no market-data or shared-ledger writes
- Expected decision information value: 0.85

## Objective

Use only frozen experiment summaries and mission counters to identify where the
next 1,000 units of research compute should be allocated.  Predict wasted-compute
risk by engine, ecology, failure reason and evidence conversion stage without
validating or rejecting any strategy.

## Frozen inputs

- 7,998 official structural prototypes;
- 16 non-killed promising candidates;
- 4 active shadow candidates;
- 3 executable shared-account basket configurations;
- 110 exact versions killed;
- Q4 access count 0;
- official completed experiment summaries supplied immutably in the queued
  specification.

## Required behavior

1. Verify the input snapshot hash supplied by the controller.
2. Aggregate prototypes, survivors, promising, shadow, Topstep and kill outcomes
   by engine and represented ecology where available.
3. Estimate conversion with Beta(1,1) shrinkage; do not report raw zero/one rates
   as certainty.
4. Penalize repeated failure reasons, high compute cost, behavioral redundancy
   and market/family concentration.
5. Preserve at least 15% exploration and cap any family at 25% and ecology at
   40% in the recommended allocation.
6. Recommend allocations across structural discovery, targeted mutation,
   multi-timeframe/cross-asset, distribution/hazard, defensive/portfolio and
   novel methods.
7. Include false-negative risk and never suppress an unexplored engine to zero.
8. Do not read market data, Q4, secrets, broker state or mutable registry files.
9. Do not validate, promote, kill or mutate candidates.

## Allowed paths

- `hydra/research/meta_failure_allocation.py`;
- controller/runner integration;
- targeted tests and generated report.

## Acceptance tests

- deterministic result for an identical frozen snapshot;
- allocation totals exactly 100%;
- exploration/family/ecology constraints hold;
- shrinkage and false-negative fields present;
- no market-data/network/paid/Q4/live/order access;
- parallel execution does not touch a shared ledger;
- full tests, compile, SQLite, governance and secret scan.

## Interpretation boundary

This meta-model guides compute only.  It cannot serve as strategy evidence,
promote or reject a candidate, open holdouts, or authorize trading.
