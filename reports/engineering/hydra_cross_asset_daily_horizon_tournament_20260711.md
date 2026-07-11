# HYDRA Cross-Asset Daily-Horizon Tournament

- Task ID: `hydra_cross_asset_daily_horizon_tournament_20260711`
- Created: 2026-07-11 UTC
- Pipeline: Discovery and Promotion; Shadow export only after prospective gates
- Expected decision information value: 0.995

## Scientific objective

Pivot away from the exposed intraday session-geometry lineages and test whether
completed prior-session states transfer information across equity indices,
energy and metals into the next market open.  Search a bounded population of
720 structural hypotheses, select a diversified elite set using only 2023, then
replay every frozen elite unchanged on 2024 Q1-Q3.

Markets and executions:

- YM → MYM;
- RTY → M2K;
- CL → MCL;
- GC → MGC.

Each target may use its own or another ecology's completed prior-session state.
Signals use only the source session completed before the target decision.

## Frozen population

Source/target relationships: each target uses itself and the other three mini
markets (16 relationships).  Features:

- signed prior-session return;
- signed prior-session range shock;
- prior-session close location;
- source-minus-target prior-session trend, only for cross-market pairs.

Each eligible relationship/feature is crossed with:

- continuation or reversal;
- absolute rolling quantile 0.65 or 0.80;
- 30, 60 or 120 completed one-minute holding bars.

This yields exactly 720 unique structural fingerprints.  It is a bounded
structural search, not a stop/target parameter grid.

## Required behavior

1. Write the population and its hash before any replay.
2. Use 2023 H1 for Round 1 and 2023 H2 for Round 2.
3. Deduplicate before replay and retain lineage, source, target, ecology, role,
   horizon and behavioral niche provenance.
4. Replay micro execution at the exact target session/timestamp; never recompute
   the signal from the micro contract.
5. Use quality-diversity selector v2 with at most eight elites and two separate
   negative controls. Missing ecologies must remain feasible.
6. Freeze the complete elite manifest before reading 2024.
7. Validate unchanged on 2024 Q1-Q3 with realistic costs, 1.5x costs, one-bar
   delay, best-event/day/month removal, block sign-flip null, BH/FDR across the
   frozen elite family, parameter neighbors, MLL and Topstep paths.
8. Require adjusted candidate-level evidence for promotion; diagnostic attacks
   remain separately labeled.
9. Permit zero-risk shadow only when economics, temporal transfer, null,
   concentration, delay, micro matching, MLL and deterministic implementation
   all pass the prospective policy.
10. Export no more than `SHADOW_RESEARCH_CANDIDATE`; never emit
    `PAPER_SHADOW_READY` from development data.

## Data and governance

- development start: 2023-01-01;
- selection end: 2024-01-01 exclusive;
- confirmation end: 2024-10-01 exclusive;
- Q4 and later: prohibited;
- network and paid data: prohibited;
- live/broker/orders: prohibited.

Frozen inputs:

- indices/energy data SHA-256:
  `07b3093ed8ef5888898abc3e531e0b522273a6c2047489b60eb36b33afeaf374`
- indices/energy map SHA-256:
  `401ca56ebab606c3eb2cbcf6ed244204f264ed2894c2ee0eb2310998f9244fda`
- indices/energy roll-map hash:
  `705ce6fe27bac7dea9cb9d492413a5112bb60765c66aa75d03f9711bef348208`
- GC/MGC volume-front data SHA-256:
  `6bca31351820713016426286de8ae3ce9f0350b6886f780cccc5565fd65da78d`
- GC/MGC volume-front map SHA-256:
  `2ac275f4043ef210afa092be8e7f6676c0409c6e2ec5e41a01aecb37427f3815`
- GC/MGC roll-map hash:
  `01ba149449a494a7a118884813abe10de8845c215b7390dbfbfa9d9dff89de13`

## Allowed paths

- `hydra/research/cross_asset_daily_horizon_primary.py`;
- mission controller and experiment runner integration;
- targeted tests;
- this task and generated experiment reports.

## Protected paths

- governance kernel and `config/governance/**`;
- Q4/lockbox data and access policy;
- market data and contract maps;
- budget, mission and registry schemas;
- existing candidate results/manifests;
- live, broker, secret and order paths.

## Acceptance tests

- exact population count/hash and unique fingerprints;
- closed prior-session state only;
- no same-session or partial-session source data;
- exact micro timestamp matching and correct multipliers/costs;
- selector v2 feasibility, control separation and no 2024 selection fields;
- immutable elite freeze before confirmation;
- candidate-level BH/FDR, temporal/cost/concentration/delay/MLL evidence;
- no component/family evidence inheritance;
- Q4/network/paid/live/order deltas all zero;
- queue/recovery/routing, full tests, no-lookahead, compile, SQLite,
  governance and secret scan.

## Kill and pivot conditions

Kill exact elites for hard invalidation, negative confirmation economics, cost
destruction, temporal collapse, adjusted-null failure under the frozen shadow
policy, excessive concentration, delay failure, invalid micro matching or MLL
failure.  If no elite reaches shadow, retain the failure surface and pivot to a
new distribution/horizon or portfolio role; do not tune the frozen versions on
2024.

## Interpretation boundary

2024 Q1-Q3 is development confirmation for this mission.  A surviving elite may
collect forward evidence with zero orders, but no result here proves persistence,
opens Q4, grants `PAPER_SHADOW_READY`, or authorizes trading.
