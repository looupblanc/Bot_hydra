# HYDRA causal transition graph — immutable engineering task

Date: 2026-07-11 UTC  
Mission: `hydra_autonomous_v1`  
Trigger: `CAUSAL_TRANSITION_GRAPH_SEARCH_REQUIRED`

## Scientific objective

Test whether a compact, interpretable graph of completed-session market states
contains transferable information for sparse next-session futures policies after
the calibrated four-market tail-hazard model failed. The experiment must decide
whether state-transition structure adds information beyond unconditional direction
and the simpler prior-session features already tested.

Expected decision information value: `0.98`. A positive result can create a newly
versioned shadow-research candidate; a negative result kills the exact graph
formulation and forces a representation/mutation pivot rather than another nearby
logistic model.

## Frozen representation

Use exactly six source states:

- `DOWN_CALM`, `DOWN_EXPANSION`;
- `BALANCED_CALM`, `BALANCED_EXPANSION`;
- `UP_CALM`, `UP_EXPANSION`.

The directional component is the prior completed RTH-session trend divided by a
20-session, one-session-shifted median absolute trend. Ratios below `-0.5`, between
`-0.5` and `0.5`, and above `0.5` map to down, balanced, and up. The volatility
component compares prior completed range to a 20-session, one-session-shifted
median range. No threshold may use the current target session.

Generate the frozen Cartesian population across:

- source market: `YM`, `RTY`, `CL`, `GC`;
- target market: `YM`, `RTY`, `CL`, `GC`;
- six source states;
- side: long or short;
- holding horizon: 60 or 120 completed 1-minute bars.

This produces `384` unique structural hypotheses. Mini and micro implementations
are one economic mechanism and do not count independently.

## Successive halving and validation

1. Logical validity, fingerprint uniqueness, prior-session availability, explicit
   contracts and no-lookahead.
2. 2023-H1 cheap screen with realistic costs and at least eight events.
3. 2023-H2 mini and synchronized-micro replay; retain only positive net and positive
   1.5x-cost economics with at most 10% unmatched execution events.
4. Freeze at most eight quality-diverse elites using only information ending before
   2024-01-01. Negative controls remain separate.
5. Replay frozen elites unchanged on 2024 Q1, Q2 and Q3.
6. Run candidate-level block sign-flip nulls with BH/FDR at elite scope, one-bar
   execution delay, best-event/day/month removal, micro transfer, parameter/state
   neighborhood diagnostics, MLL and Topstep-path simulation.

Shadow support requires no hard invalidation, positive development and 2024 pooled
net after realistic and 1.5x costs, at least two supportive quarters, BH-adjusted
probability at most `0.20`, positive delayed and concentration attacks, valid micro
execution, and safe one-micro MLL. Promotion evidence remains stricter (`0.03`) and
does not confer `PAPER_SHADOW_READY`.

## Controls and competing explanations

- Negative control: deterministic permutation of state labels must not create a
  transferable lift.
- Positive control: a synthetic transition process with a weak real state effect
  must be detected with adequate power.
- Simpler baseline: unconditional target direction and prior-trend-only behavior.
- Report transition counts, Laplace-smoothed edge probabilities and edge lift using
  2023 only; 2024 values are confirmation, never selector inputs.

## Allowed paths

- `hydra/research/causal_transition_graph.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- tests specifically covering the new engine and scheduler routing
- immutable reports under this experiment output directory
- append-only development data-access/evidence/decision ledgers through existing APIs

## Protected paths and prohibitions

- no protected-governance edit;
- no Q4 or post-2024-10-01 read;
- no network or paid request;
- no broker credential, adapter, order, or live capability;
- no mutation of active shadow versions;
- no inherited component/family/candidate status;
- no mission/registry database in Git.

## Acceptance tests

- exactly 384 unique hypotheses and deterministic fingerprints;
- six stable states with thresholds shifted by one completed session;
- source session strictly precedes every decision timestamp;
- deterministic graph counts and Laplace-smoothed probabilities;
- selector cannot read 2024 before elite freeze;
- synchronized mini/micro replay and realistic costs;
- candidate-level nulls, attacks and account replay;
- negative and injected-positive controls calibrated;
- result routing is idempotent and exact failures are killed once;
- full pytest, no-lookahead, compileall, SQLite, governance, budget, secret and
  one-writer checks pass.

## Rollback and kill conditions

Rollback on any future-session feature, source/hash mismatch, selector leakage,
non-determinism, Q4 access, duplicate writer or outbound-order capability. Kill the
exact graph candidates when economics, transfer, null evidence, execution or
concentration fail. Do not kill the broad state-transition research family solely
because this six-state formulation fails.

