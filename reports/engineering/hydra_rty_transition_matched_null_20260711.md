# HYDRA RTY transition matched-null — immutable engineering task

Date: 2026-07-11 UTC  
Mission: `hydra_autonomous_v1`  
Trigger: `CAUSAL_TRANSITION_MATCHED_NULL_AND_MUTATION_REQUIRED`

## Frozen parent evidence

- candidate: `strategy_transition_RTY_to_RTY_up_expansion_long_h60_v1`;
- source experiment: `causal_transition_graph_v1`;
- source result hash:
  `873fe9a2d4bc613ca9c0b0285e8168e1cf03a5ab25994b1aa27ca45a43bd56cf`;
- source result SHA-256:
  `bed857199813968b216046a70a595bc1477f5482d44dd777dbedb862a4300fd7`;
- elite manifest hash:
  `c4c0698c1e168b8c8b2546a58185350e0aef7cf72a3bae9e6f736197f576368f`;
- elite manifest SHA-256:
  `b4776e5f9db87350f8ad0c39900a2ecdd99210d2319685ebaa756d031eb6b8ec`;
- trade ledger SHA-256:
  `29e93fa7cfb2c0471857f9ab3468da2e92f6cd90417630c0f8639156c9c2cbc3`.

The parent remains `PROMISING_RESEARCH_CANDIDATE`. It has no hard invalidation,
but its five-session block sign-flip probability was about `0.0779` raw and
`0.6229` after the frozen eight-elite BH family correction. This task may not
erase, relabel or inherit that failed adjusted null.

## Scientific objective

Determine whether the `EXPANSION` component of the parent state explains an
incremental 60-minute M2K return beyond the simpler `UP` prior-session trend.
The decision is between:

1. retain the state mechanism for a newly versioned forward-only mutation;
2. reduce it to the simpler up-trend baseline;
3. kill the exact parent mechanism as a selected development winner.

Expected decision information value: `0.97`.

## Frozen counterfactual design

Treated events are every eligible RTY `UP_EXPANSION` prior-session state from
2023-01-01 through 2024-09-30, executed long on synchronized M2K for 60 completed
1-minute bars with the same costs as the parent.

Control events are eligible RTY `UP_CALM` states. Matching is performed before
loading any event outcome and uses only:

- calendar quarter;
- absolute prior trend ratio;
- prior range divided by its one-session-shifted 20-session median;
- prior close location;
- session ordinal within the quarter.

Within each calendar quarter, standardize matching covariates from the combined
eligible pool and greedily match treated events in timestamp order to the closest
unused control. Use a Euclidean caliper of `1.25`; unmatched treated events remain
reported and excluded. Control reuse is prohibited. Matching code must accept a
covariate-only table and return session IDs before any PnL column is joined.

## Frozen tests

- paired M2K 60-minute net-PnL difference;
- 16,384 deterministic paired sign flips;
- Hodges-Lehmann paired effect estimate and bootstrap confidence interval;
- quarter-by-quarter paired effects;
- comparison against all-up-state and unconditional-long baselines;
- one-bar delayed execution;
- 1.5x cost stress;
- removal of best pair, day and month;
- overlap, caliper and standardized-balance audit;
- deterministic label-permutation negative control;
- injected weak-real-effect positive control.

Mechanism support requires at least 24 matched pairs, positive paired effect,
positive 1.5x-cost and delayed paired effect, at least two supportive quarters,
positive effect after best-pair and best-month removal, paired sign-flip probability
at most `0.10`, and calibrated controls. This is diagnostic support, not a new
candidate-level promotion p-value.

## Mutation rule

Only if the expansion mechanism support gate passes, create exactly one fresh
child specification:

`strategy_transition_RTY_up_expansion_matched_long_h60_v2`

The child keeps the parent entry/exit logic and records the matched-null design as
its causal rationale. It starts at `RESEARCH_PROTOTYPE_FORWARD_REQUIRED`; it does
not inherit the parent status, may not be evaluated as independent on the already
observed development window, and may not be activated automatically. Its next
valid evidence source is untouched forward shadow or an authorized lockbox.

If support fails, create no child and kill/freeze the exact parent lineage version.

## Allowed paths

- `hydra/research/rty_transition_matched_null.py`;
- controller/runner integration and targeted tests;
- immutable output reports/manifests;
- append-only development data-access/evidence/decision ledgers via existing APIs.

## Protected paths and prohibitions

- no protected-governance edit;
- no Q4 or post-2024-10-01 data;
- no paid/network request;
- no broker, order or live capability;
- no outcome-aware matching;
- no automatic shadow/Paper/Topstep promotion;
- no reuse of the old candidate ID for a child;
- no inherited pass status.

## Acceptance and rollback

Tests must prove covariate-only matching, no control reuse, deterministic calipers,
prior-session availability, synchronized M2K replay, calibrated controls, immutable
parent/source hashes, no Q4/order access, idempotent routing and full mission safety.
Rollback on outcome-aware matching, hash drift, future features, non-determinism,
Q4 access, a second writer or status inheritance.

