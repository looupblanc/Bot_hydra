# HYDRA GC Session-Geometry Fresh Primary

- Task ID: `hydra_gc_session_geometry_fresh_primary_20260711`
- Created: 2026-07-11 UTC
- Pipeline: Discovery and Promotion; Shadow only if the prospective gates pass
- Expected decision information value: 0.997

## Scientific objective

Test the best metals diagnostic selected exclusively on data ending before
2024-01-01 without reading its 2024 result.  The frozen economic hypothesis is
that unusually large GC overnight displacement has a one-hour reversal hazard.
The liquid GC contract supplies the signal; the executable MGC contract is
matched at the exact same session and timestamp for micro-first risk control.

This is a fresh candidate and receives no status from the historical diagnostic:

`strategy_session_geometry_GC_signal_MGC_execution_overnight_displacement_reversal_q65_h60_none_v2`

Frozen structure:

- signal market: GC;
- execution market: MGC;
- feature: `overnight_displacement`;
- direction: reversal;
- quantile: 0.65;
- holding horizon: 60 minutes;
- context: none;
- selection data end: 2024-01-01 exclusive;
- validation data: 2024 Q1-Q3 only;
- Q4 and later data: prohibited.

The historical diagnostic ranked second overall and first among metals on the
frozen 2023 ranking.  Its 2023 GC and MGC variants were both net-positive after
costs.  Those values justify a prospective test but confer no pass.

## Required behavior

1. Verify the exact historical preregistration and freeze-manifest hashes.
2. Verify that the frozen ranking selected the exact old diagnostic from data
   ending before 2024-01-01.
3. Create a fresh child ID, lineage, structural fingerprint and preregistration.
4. Recompute GC signals exactly from explicit volume-front contracts.
5. Match MGC entry and exit prices only at the same session and timestamp; do
   not recompute the signal from MGC.
6. Audit every missing or mismatched micro execution and reject above 10%.
7. Report 2023 H1/H2 and untouched-with-respect-to-this-diagnostic 2024 Q1-Q3.
8. Run realistic MGC costs, 1.5x costs, one-bar delay, block sign-flip null,
   best-event/day/month removals, MLL and Topstep path replay.
9. Use alpha 0.03 for research promotion and 0.20 only for calibrated zero-risk
   shadow support.
10. Export an immutable fail-closed shadow configuration only if every shadow
    safety and evidence condition passes.
11. Never emit `PAPER_SHADOW_READY`, access Q4, purchase data, use a network, or
    enable broker/order capability.

## Frozen provenance

- source preregistration SHA-256:
  `aa0db8f5720a091576834e3e4382d691674d2ae1e86f5564418968f290a26d26`
- source preregistration semantic hash:
  `4733a540c91dd7a569b65449722867dc9aea8553c88b59f1b9b33b7513405e3a`
- source freeze SHA-256:
  `e62d8b03dd74173c66183d9bca25d27006e5c2b799d2c2aa93f544cbb2fd89d8`
- source freeze semantic hash:
  `f11a6f657e018f2d8b137eddb64cf497dcf63ed0ee17848744667fa968201d96`
- frozen population hash:
  `2c2f7b45c14dcca09014e654711c799060ef9fedd57c6799e2535216c97cc097`
- GC/MGC data SHA-256:
  `6bca31351820713016426286de8ae3ce9f0350b6886f780cccc5565fd65da78d`
- volume-front map file SHA-256:
  `2ac275f4043ef210afa092be8e7f6676c0409c6e2ec5e41a01aecb37427f3815`
- roll-map semantic hash:
  `01ba149449a494a7a118884813abe10de8845c215b7390dbfbfa9d9dff89de13`

## Allowed paths

- `hydra/research/gc_session_geometry_fresh_primary.py`;
- minimal generic synchronization changes in
  `hydra/research/energy_metals_session_execution_repair.py`;
- mission controller and experiment runner integration;
- targeted tests;
- this task and generated mission experiment reports.

## Protected paths

- `config/governance/**` and governance kernel;
- Q4/lockbox data and ledgers;
- historical result, preregistration and freeze manifest;
- market data and roll maps;
- budget, registry and mission schemas;
- live, broker, credentials and order paths.

## Acceptance tests

- immutable source/task/data/map hashes;
- source selection ended before 2024 and exact diagnostic identity is proven;
- fresh fingerprint and no inherited evidence;
- GC sessions/sides retained and MGC timestamps matched exactly;
- correct multipliers, costs, rolls and MAE;
- prospective fold/null/concentration/delay/MLL evidence;
- hard versus diagnostic outcomes remain separated;
- no Q4/network/paid data/live/broker capability;
- queue, recovery and result routing;
- full tests, compile, no-lookahead, governance, SQLite and secret scan.

## Rollback and kill conditions

Rollback the engineering change if any protected digest changes, a test
regresses, queue recovery duplicates an experiment, or the persistent service
cannot resume with one writer.  Kill the exact candidate if 2024 net or cost
stress is non-positive, fewer than two quarters support it, missing matches
exceed 10%, concentration/delay/MLL fails, or a data/timing invariant fails.

## Interpretation boundary

This prospective test can create at most a `SHADOW_RESEARCH_CANDIDATE`.  It does
not prove persistence, authorize Q4, inherit the old diagnostic's evidence,
grant `PAPER_SHADOW_READY`, or submit orders.
