# Immutable Research Task — Equity RTH Open-Gap Continuation v1

Task ID: `eng_equity_open_gap_continuation_20260711_v1`

Frozen after the preregistered sign-flip diagnostic of the distinct reversal
pilot, and before executing this continuation formulation as a candidate.

## Scientific objective

Test whether unusually large overnight displacement in US equity-index futures
continues after the completed 08:30 America/Chicago RTH opening bar. The prior
reversal formulation is a frozen negative result. Its opposite-sign diagnostic
is hypothesis-generating only and confers no pass, p-value or status on the new
candidate IDs.

## Frozen formulation

- new candidates: `strategy_open_gap_continuation_{ES,NQ,RTY,YM}_v1`;
- one mechanism family, with mini/micro contracts treated as contractual and
  sizing replications rather than independent mechanisms;
- explicit-contract development/falsification data ending before 2024-10-01;
- Q4, paid data, network, broker and live execution prohibited;
- decision after the completed 08:30 CT source bar, entry at that bar close;
- reference is the last completed 14:59 CT bar from the prior trading session
  in the same explicit contract;
- direction follows the signed open gap;
- primary hold is 60 completed one-minute bars;
- event threshold is the past-only expanding 75th percentile of absolute gaps
  per root, requiring 40 prior sessions;
- primary cost is conservative round-turn commission plus two ticks;
- folds are 2023 H2, 2024 Q1, Q2 and Q3;
- 30/90-minute holds, 65th/85th-percentile thresholds, one-bar delay, reversal
  control, 1.5x cost, block sign-flip null, event/fold concentration and
  mini/micro transfer are frozen attacks;
- family-wise probability adjustment covers all four market candidates;
- Topstep 150K replay uses the versioned 2026-07-10 no-DLL baseline and cannot
  override failed economics or transfer.

## Decision policy

Leakage, target contamination, invalid contract/roll handling, future bars,
wrong multipliers, impossible fills, uncontrolled sizing/MLL, Q4 access and any
order capability are fatal.

Positive pooled net alone is insufficient. Shadow research admission requires
non-catastrophic temporal support, calibrated candidate-level null evidence,
parameter and mini/micro transfer, conservative MLL safety and a complete
zero-order shadow package. `PAPER_SHADOW_READY` remains impossible before a
frozen one-shot untouched holdout.

The previous reversal status and its diagnostic p-value are not inherited. A
failure freezes this exact version; nearby parameters cannot replace it.

## Allowed paths

- `hydra/research/equity_open_gap_continuation.py`
- minimal shared gap-event helpers
- controller/runner/status integration
- focused tests and immutable experiment artifacts

## Protected paths

Q4/future lockboxes, governance roles, historical results, mission/registry
databases, raw market data, credentials and broker/order modules.

Expected decision information value: `0.97`; data cost `$0`. The prior
directional falsification makes this a more discriminative next decision than
another threshold neighbor or unrelated broad search.
