# HYDRA post-mutation shadow admission preregistration

- Protocol ID: `hydra_post_mutation_shadow_admission_v1`
- Preregistered: 2026-07-11 UTC
- Scope: post-mutation successive-halving output only
- Maximum admissions in this audit: 1
- Permitted target status: `SHADOW_RESEARCH_CANDIDATE`
- Explicitly prohibited statuses: `SHADOW_RESEARCH_ACTIVE`, `PAPER_SHADOW_READY`, `TRADING_READY_CANDIDATE`, `FUNDED_DEPLOYMENT_ELIGIBLE`
- Protected data access: none
- Q4 access: prohibited
- Fresh forward-data access: prohibited
- Broker or order capability: prohibited

## Scientific objective

Determine whether at most one child produced by the frozen targeted-lineage
mutation experiment has enough outcome-independent, development-only evidence
and implementation completeness to enter the no-order shadow packaging stage.
This is an admission audit, not funded-edge validation and not shadow
activation.

The audit is deliberately calibrated for no-financial-risk shadow research.
Hard integrity failures remain fatal. Soft robustness limitations are retained
as explicit uncertainty and may not be silently converted into funded-grade
claims.

## Frozen input population

The eligible population is the immutable set of non-control children emitted by
the preregistered post-mutation successive-halving experiment. Parents, active
shadow configurations, controls, manually introduced candidates, and children
created after results are read are ineligible.

No candidate may inherit a pass, status, evidence conclusion, or shadow
configuration from its parent. The parent specification and any active parent
configuration remain immutable.

## Outcome-independent selection rule

Before opening any detailed child result, the implementation must apply the
following deterministic gates in this exact order. A candidate is eligible only
if every gate passes:

1. Complete 2023 event-level replay is available under the registered data role.
2. Candidate-level mandatory-null family correction is complete and the frozen
   Benjamini-Hochberg adjusted p-value is at most `0.10`.
3. Pooled net economics are strictly positive after the registered realistic
   cost model.
4. Pooled net economics remain strictly positive at `1.5x` the registered cost
   model.
5. Across the frozen temporal folds, no more than one fold is weak. A weak fold
   is permitted only when all of the following hold:
   - its net result is non-positive;
   - it does not breach the simulated account MLL;
   - its loss is no worse than 50% of the absolute pooled positive net result;
   - it does not contain a hard integrity or execution contradiction;
   - the adverse state is measurable using information available before entry.
6. The micro-contract account replay is MLL-safe under conservative costs and
   intraday unrealized-path accounting.
7. The frozen Topstep Combine-path replay passes its registered target, MLL,
   consistency, contract-limit, session, and flatten rules.
8. At least 50% of the parent opportunities are retained.
9. The child is structurally and behaviorally nonduplicate under the registered
   specification fingerprint and trade-path fingerprint rules.
10. The activation guard is deterministic and uses prior completed trades only.
    The current trade outcome, incomplete bar, future higher-timeframe state,
    forward-filled future state, or later account state is prohibited.
11. The child is implementable in real time from registered, available features
    with explicit contracts, closed-bar timestamps, deterministic state
    recovery, and conservative virtual execution.
12. The derived configuration has zero broker credentials, zero order adapter,
    zero outbound-order method, and a fail-closed risk policy.

These thresholds, gate ordering, and definitions may not be changed after child
results are inspected.

## Deterministic tie-break and admission cap

If no child passes every gate, admit none.

If exactly one child passes, select it.

If more than one child passes, rank the passing set using only this frozen
lexicographic order:

1. lowest BH-adjusted mandatory-null p-value;
2. largest micro-account minimum MLL buffer;
3. largest pooled net at `1.5x` costs;
4. highest retained-opportunity fraction;
5. smallest absolute loss in the single allowed weak fold, or zero if no weak
   fold exists;
6. lexicographically smallest immutable child ID.

Admit only the first ranked child. The cap of one prevents outcome-driven status
inflation and keeps this audit independent of later portfolio construction.

## Role semantics

The selected child, if any, is evaluated as a `COMBINE_PASSER_POOL` research
candidate. This audit does not require it to optimize XFA payout cycles or serve
as a defensive account component. It may later interoperate with independently
validated `XFA_PAYOUT_POOL` and `DEFENSIVE_ACCOUNT_POOL` members, but evidence is
not transferable across those roles.

Market-specific hypotheses do not require universal cross-market replication.
A weak temporal fold is not automatically fatal when the frozen non-catastrophic
conditions above are met.

## Required derived artefacts

Only after this preregistration is committed, the implementation may produce:

- one immutable derived shadow-candidate configuration at most;
- a versioned prior-trade guard module;
- a machine-readable admission decision with every gate and source hash;
- a complete provenance link from parent to mutation hypothesis to child;
- a deterministic configuration hash;
- tests proving prior-only state, restart determinism, immutability, and absence
  of order capability.

The derived configuration must pin:

- parent and child IDs;
- strategy and feature versions;
- source-data fingerprints and registered temporal roles;
- cost and `1.5x` cost assumptions;
- guard window, warm-up, threshold, and update timing;
- symbol, explicit-contract policy, session, and timeframe rules;
- micro sizing and account-risk limits;
- stale-data, duplicate-signal, startup-reconciliation, session-flatten, and
  kill policies;
- virtual-fill assumptions;
- evidence and admission-decision hashes.

## Activation separation

Admission to `SHADOW_RESEARCH_CANDIDATE` does not activate shadow execution.
Activation must occur through the existing generic shadow activation workflow
and its independent safety checks. No candidate-specific activation bypass is
authorized.

The derived candidate must remain fail-closed while a lawful fresh forward-data
source is unavailable. Missing forward data must not stop Discovery or
Promotion.

## Hard invalidations

Any of the following forces rejection regardless of economics:

- lookahead, target leakage, incomplete higher-timeframe leakage, or current-
  trade outcome use;
- invalid explicit contract, roll, multiplier, session, DST, sizing, or cost;
- impossible fill or unavailable real-time feature;
- uncontrolled MLL path;
- missing deterministic restart/reconciliation behavior;
- duplicate or behaviorally equivalent child;
- mutation or threshold chosen after child outcomes were read;
- inherited parent evidence or status;
- Q4, later protected data, or unregistered forward-data access;
- broker credential, order connection, execution adapter, or outbound-order
  capability;
- governance, budget, or evidence-scope violation.

## Falsification and reporting

The audit succeeds scientifically even when zero children qualify. It must
report, for every frozen child, the first failed gate plus all subsequently
computable diagnostic gates. Missing evidence is reported as
`INSUFFICIENT_EVIDENCE`, never imputed as a pass.

The final machine-readable decision must include:

- frozen population fingerprint;
- thresholds and protocol hash;
- per-candidate gate vector;
- tie-break values for passing candidates;
- selected candidate ID or `NONE`;
- derived configuration hash when selected;
- explicit assertions: Q4 access `0`, fresh forward-data access `0`, orders `0`.

## Rollback conditions

Rollback or reject the implementation if it:

- changes this policy after result inspection;
- admits more than one child;
- makes a candidate active directly;
- modifies a parent or active shadow configuration;
- uses an uncommitted or unregistered result population;
- weakens an integrity, governance, Q4, budget, or no-order protection;
- cannot reproduce the same decision and configuration hash from identical
  frozen inputs.
