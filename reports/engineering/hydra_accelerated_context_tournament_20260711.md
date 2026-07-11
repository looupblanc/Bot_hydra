# HYDRA Engineering Task — Accelerated Context Tournament v1

## Immutable identity and objective

- Experiment ID: `accelerated_context_tournament_v1`
- Objective: generate 5,000 structurally diverse proposals, select a bounded
  executable subset without outcome leakage, apply successive halving on 2023,
  freeze diversified elites, then replay unchanged on 2024 Q1–Q3.
- Data role: `DEVELOPMENT_AND_FALSIFICATION_ONLY`
- Q4, paid data, network, live trading and broker access: prohibited.

## Frozen production design

Stage 0 generates exactly 5,000 outcome-free proposals with the existing
quality-diversity structural generator across nine research engines, four market
ecologies, eight timeframe profiles, six horizons, six sessions and six
portfolio roles. Structural fingerprints are deduplicated before replay.

The bounded executable lane contains exactly 300 new `v2` hypotheses, 50 per
primary market in ES, NQ, RTY, YM, GC and CL. It is sampled deterministically by
structural hash from the Cartesian grammar:

- nine past-only state features;
- continuation or reversal;
- five session/threshold/horizon profiles;
- ten activation contexts: none, completed 5m/15m/30m/60m trend agreement,
  completed 5m/15m/30m/60m disagreement, and completed 15m volatility expansion.

Every non-null activation uses only a completed higher-timeframe bar whose
availability timestamp is at or before the decision timestamp. Every execution
retains the existing one-completed-1m-bar delay and explicit-contract guard.

## Successive-halving gates

Round 0: exact fingerprint, explicit contract, valid profile/context, no Q4.

Round 1 (`2023-01-01` to `2023-07-01`): at least 8 events, positive net after
costs, positive 1.5x-cost net, finite path and best positive-event share at most
0.45.

Round 2 (`2023-07-01` to `2024-01-01`): at least 10 events, positive net after
costs, positive 1.5x-cost net, finite path and best positive-event share at most
0.40.

Before reading 2024, selector v2 freezes at most 20 maximum-feasible diversified
elites and up to two separate negative controls. Missing ecologies cannot make
selection infeasible. No 2024 outcome participates in selection.

Round 3 replays frozen elites unchanged on 2024 Q1, Q2 and Q3, with mini/micro
transfer, realistic costs, five-event block sign-flip, BH/FDR across exact
elites, concentration, parameter diagnostics, MLL and Topstep path. No candidate
inherits evidence and no development-only result can become
`PAPER_SHADOW_READY`.

## Allowed paths

- `hydra/research/accelerated_context_tournament.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `tests/test_accelerated_context_tournament.py`
- immutable mission experiment artifacts

## Protected paths

- governance kernel and protected manifests
- existing candidates and their exact tested artifacts
- registry/mission DB except through the existing controller writer
- Q4 or later data, Databento budget ledger, secrets and order code

## Acceptance tests

- exactly 5,000 unique Stage-0 structures and 300 unique executable v2 hypotheses;
- 50 executable hypotheses per primary market;
- deterministic population and selection hashes;
- completed-bar context joins never expose partial HTF bars;
- decision timestamps dominate context availability timestamps;
- mini/micro explicit-contract and delay invariants pass;
- selector manifest exists before any 2024 replay;
- controls remain promotion-ineligible;
- no Q4, network, spend or order capability;
- full tests, no-lookahead, compile, integrity, governance and secret scan pass.

## Rollback conditions

Rollback on population drift, row-order dependence, incomplete HTF leakage,
selection using 2024, frozen-source mismatch, Q4 read, memory exhaustion without
safe batch reduction, nondeterminism or any order/broker capability.

## Expected decision information value

`0.97`: it expands representation/timeframe coverage by an order of magnitude,
uses cheap 2023 halving before expensive 2024 replay, retains multi-ecology
diversity and runs while the active shadow pipeline waits for forward evidence.
