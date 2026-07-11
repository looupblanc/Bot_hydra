# HYDRA promising-lineage and portfolio-role capability — immutable task

Date: 2026-07-11 UTC  
Trigger: `PORTFOLIO_ROLE_AND_PROMISING_LINEAGE_MUTATION_REQUIRED`

## Scientific objective

Resolve the current mission blocker by testing one causal, preregistered structural
repair for every retained promising lineage and by evaluating genuinely defensive
shared-account roles. The experiment must distinguish alpha uncertainty from MLL,
contract, concentration, execution and portfolio uncertainty. Expected decision
information value: `0.99`.

The exact sixteen parents and all source hashes are frozen in
`config/research/promising_lineage_sources_v1.json`. Active shadow parents remain
immutable. No child inherits status, evidence, Topstep result or shadow activation.

## Mutation grammar

For every parent, create exactly one primary child with a new deterministic ID and
one changed structural dimension: a prior-equity activation guard. Before each
entry the guard may use only already completed trades of that exact parent. Its
window and threshold are fitted on the earliest available development segment and
then frozen before later folds. Outcomes from the current trade may never enter its
activation decision.

Failure routing is frozen:

- MLL breach: `PRIOR_EQUITY_MLL_GUARD`;
- event/fold concentration: `PRIOR_EQUITY_CONCENTRATION_GUARD`;
- temporal collapse: `PRIOR_EQUITY_TEMPORAL_GUARD`;
- contract/micro failure: `MICRO_EXECUTION_REPAIR_REQUIRED` plus a conservative
  guard child that remains ineligible for promotion;
- null-only uncertainty: `PRIOR_EQUITY_REGIME_GUARD`;
- hazard candidates: `AVOIDED_LOSS_POLICY_GUARD` and role-specific evidence.

The guarded child must retain at least 50% of parent opportunities and never use a
parameter grid. Children with insufficient event-level 2023 coverage are explicitly
`RESEARCH_PROTOTYPE_INCOMPLETE_REPLAY`; this may not be hidden or treated as a pass.

The YM active parent additionally receives three separate fresh research children:

1. prior-equity temporal guard;
2. past-only gap-magnitude stability band fitted on 2023;
3. micro-first one-contract risk implementation.

All YM children replay available 2023 and 2024 Q1-Q3 evidence, costs, one-bar delay,
best trade/day/month removal, candidate nulls and Topstep account paths. They remain
post-parent-development mutations requiring untouched forward evidence.

## Objective-aligned account pools

No candidate is required to optimize Combine passage, XFA payout production and
defensive account utility simultaneously. Every child and portfolio policy is
assigned, before replay, to exactly one primary pool:

- `COMBINE_PASSER_POOL`: probability of target before MLL, median target time,
  consistency margin, execution cost and tail risk;
- `XFA_PAYOUT_POOL`: expected payout cycles before ruin, qualifying-day frequency,
  MLL and post-payout survival, and payout timing;
- `DEFENSIVE_ACCOUNT_POOL`: marginal account utility, drawdown/MLL protection,
  shared-loss reduction, regime avoidance and conflict reduction.

Market-specific hypotheses do not require universal cross-market replication.
Positive pooled economics may tolerate one non-catastrophic weak development period
when the account survives and the failure state is measurable. Hard integrity,
data, causality and execution failures remain fatal. Non-fatal robustness failures
are retained as uncertainty and role-specific evidence rather than silently
converted into universal rejection.

Shadow research admission is evaluated separately from funded eligibility. This
task may prepare a `SHADOW_RESEARCH_CANDIDATE`, but cannot activate or label a child
Paper-ready without a separate immutable configuration and safety review.

## Portfolio/MLL roles

Use the four immutable active-shadow trade ledgers and recompute one shared account.
Test three past-only roles:

- shared intraday loss circuit;
- shared MLL-buffer throttle;
- correlated/conflicting signal deactivation.

Each role is compared with at least 4,096 matched random deactivations removing the
same number of events. Report marginal net, maximum drawdown, minimum MLL buffer,
shared loss days, consistency, target velocity, contract conflicts and account
utility. A defensive role advances only when it improves drawdown/MLL and beats the
matched random control without catastrophic net destruction.

Phase-specific utilities must be reported separately; an improvement in one pool
must not be presented as evidence for either of the other two pools.

## Governance and safety

- development/falsification data end strictly before 2024-10-01;
- Q4 and later data are prohibited;
- no network, paid request, broker, credentials or orders;
- all output is immutable and candidate-scoped;
- behavioral duplicates are rejected before replay;
- family share <=25%, ecology share <=40%, direct-lineage share <=2%, exploration
  accounting >=15%;
- no Paper or funded status can be created by this task.

## Allowed paths

- `config/research/promising_lineage_sources_v1.json`;
- `hydra/factory/mutation_hypothesis.py`;
- `hydra/factory/promising_lineage_mutator.py`;
- `hydra/portfolio/strategy_role.py`;
- `hydra/portfolio/account_contribution.py`;
- `hydra/portfolio/mll_protection_role.py`;
- `hydra/portfolio/portfolio_role_search.py`;
- `hydra/mission/portfolio_mutation_action.py`;
- controller/runner integration and targeted tests;
- immutable experiment reports.

## Acceptance and rollback

Tests must prove all 16 parents are covered, parents remain unchanged, IDs and
lineages are new, status is never inherited, activation decisions are shifted,
duplicates are rejected, role-specific gates are applied, matched controls preserve
deactivation counts, shared MLL is recomputed, Q4/order capability is zero and
routing is idempotent. Roll back on future information, source hash drift, silent
coverage loss, status inheritance, two writers or any governance weakening.
