# Immutable Research Task — Causal Quality-Diversity Economic Tournament v1

Task ID: `eng_qd_economic_tournament_20260711_v1`

Frozen before any economic replay of this grammar.

## Scientific objective

Convert the outcome-free Stage-0 archive into a real multi-asset Stage-1/2
production tournament. Generate interpretable causal state-transition strategies,
select elites using 2023 only, and evaluate the frozen selections on 2024 Q1–Q3
without mutation. The experiment may create shadow-research candidates, but it
cannot create `PAPER_SHADOW_READY` because no untouched lockbox is opened.

## Frozen population

- exactly 540 structural prototypes before validity rejection;
- markets: ES/MES, NQ/MNQ, RTY/M2K, YM/MYM, GC/MGC, CL/MCL;
- nine past-only state features:
  - old-region reentry;
  - directional pressure without progress;
  - shared downside-loss state;
  - failed expansion;
  - extreme dwell;
  - short/long realized-volatility ratio;
  - lagged 60-minute return;
  - past realized volatility;
  - past participation;
- two policy directions per state: continuation and reversal relative to the
  directional feature or, for magnitude-only states, lagged 60-minute return;
- five fixed profiles:
  - RTH opening, q65 transition, 15-minute hold;
  - RTH opening, q75 transition, 30-minute hold;
  - RTH middle, q65 transition, 30-minute hold;
  - RTH late, q75 transition, 60-minute hold;
  - all liquid RTH, q85 transition, 60-minute hold;
- signals occur only on a causal crossing into the state; persistent states do
  not generate repeated entries;
- one non-overlapping position per market and session;
- exact market clocks: equity 08:30 CT, GC 07:20 CT, CL 08:00 CT;
- prior closed 1-minute bars only; decision and marketable entry proxy at the
  next bar open; exact-horizon marketable exit proxy; full round-turn cost with
  two ticks of slippage;
- explicit mapped contracts and unsafe-roll exclusions are mandatory.

## Frozen selection and validation

- 2023 H1: feature/threshold warm-up only;
- 2023 H2: discovery economics and quality-diversity selection only;
- Stage-1 minimums: 15 discovery events, positive net, positive 1.5x-cost net,
  best positive-event share <= 35%, finite path;
- deduplicate before replay by structural fingerprint;
- retain at most 24 validation elites, at most one per niche;
- caps at selection: one ecology <= 35%, one family <= 25%, one market <= 25%,
  one lineage one elite; maintain at least 15% exploration when enough survivors
  exist;
- ranking is Pareto/lexicographic over discovery net, cost resilience, drawdown,
  concentration, event count, novelty and complexity; no opaque scalar promotion;
- 2024 Q1, Q2 and Q3 are evaluated only after the selection list is frozen;
- candidate null: deterministic five-event block sign flips on validation events;
- Benjamini-Hochberg correction across all validation elites;
- mini/micro transfer is recalculated independently;
- parameter neighbors q-0.10 and q+0.10 are diagnostic; at least one positive
  neighbor plus positive 1.5x cost stress is required for shadow parameter
  stability;
- hard failures: leakage, incomplete bar, target across a session/contract gap,
  impossible timing, invalid cost/multiplier, Q4/post-2024-09-30 access, duplicate,
  hidden sizing, or order capability;
- all status decisions use candidate-level 2024 evidence and the calibrated
  shadow admission policy; no Stage-0 or component status is inherited.

## Outputs

- immutable preregistration and frozen 2023 selection manifest;
- all 540 prototype fingerprints and Stage-1 dispositions;
- selected-candidate validation dossiers and trade ledger;
- quality-diversity archive summary and cap audit;
- fail-closed shadow specifications only for honest shadow admissions;
- precise kills and next recommended research action.

## Governance boundary

- development/falsification data end exclusive 2024-10-01;
- Q4, 2025, paid data, network, live/broker and outbound orders prohibited;
- protected governance, registry/mission databases, raw data and credentials are
  protected paths;
- expected data cost: `$0`;
- expected decision information value: `0.99` because this is the first actual
  economic replay of the diversified Stage-0 production concept and separates
  representation failure from insufficient production throughput.
