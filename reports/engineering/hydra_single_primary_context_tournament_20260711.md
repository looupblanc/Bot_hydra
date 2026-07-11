# HYDRA Engineering Task — Single-Primary Context Tournament v1

## Objective

Apply the calibrated prospective single-primary alpha policy to a fresh,
non-overlapping batch of multi-asset/multi-timeframe structures. Maintain a broad
diagnostic archive while allowing exactly one candidate to consume the 2024
Q1–Q3 confirmation decision.

## Frozen experiment

- Experiment ID: `single_primary_context_tournament_v1`
- Candidate version: new `v3` IDs only.
- Structural batch: offset 1 of the accelerated context grammar; no structural
  fingerprint may overlap the prior `v2` executable batch.
- Executable population: 300, exactly 50 per ES, NQ, RTY, YM, GC and CL and 20%
  per mechanism family.
- Context grammar: none; completed 5m/15m/30m/60m trend agreement or
  disagreement; completed 15m volatility expansion.
- Execution: explicit contracts, one completed 1m-bar delay, realistic costs.
- Data end: exclusive `2024-10-01`; Q4 prohibited.

## Early-fold selection

Round 1 on 2023 H1 and Round 2 on 2023 H2 reuse the frozen accelerated gates.
Selector v2 freezes at most 20 diversified diagnostic elites from 2023 only.

Exactly one promotion primary is then selected from those elites using 2023 H2
mini and micro evidence only. Ranking is lexicographic:

1. positive mini and micro net;
2. positive mini and micro 1.5x-cost net;
3. maximize the smaller of mini/micro net-to-drawdown ratios;
4. lower positive-event concentration;
5. more events;
6. structural fingerprint.

If no elite has positive mini and micro economics, the experiment records no
primary and 2024 promotion is not opened.

The exact primary ID, specification, source evidence, alpha `0.03`, costs and
configuration are written to an immutable manifest before any 2024 replay.

## Confirmation

Only the primary is promotion-eligible on 2024 Q1–Q3. It receives:

- mini and micro replay;
- calibrated five-event block sign-flip at alpha 0.03;
- quarter transfer and pooled economics;
- 1.5x cost stress;
- best-event/fold concentration;
- parameter-neighborhood diagnostics;
- MLL and Topstep replay;
- completed-HTF and one-bar-delay integrity proof.

The 19 other elites may be replayed only as diagnostics. Their p-values and PnL
cannot promote them, cannot be pooled with the primary and cannot inherit the
primary’s evidence.

This is still development confirmation because HYDRA has already used the broad
2024 research environment. A qualifying primary may become a zero-risk
`SHADOW_RESEARCH_CANDIDATE`, never `PAPER_SHADOW_READY`; Q4 remains required by
protected policy for the latter.

## Allowed paths

- `hydra/research/single_primary_context_tournament.py`
- additive batch support in `hydra/research/accelerated_context_tournament.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `tests/test_single_primary_context_tournament.py`
- immutable experiment artifacts

## Protected paths

- v1/v2 candidate results and structural fingerprints
- governance/Q4 and post-October data
- mission/registry DB outside the existing single writer
- budget, secrets, broker, live and order code

## Acceptance tests

- 300 unique v3 hypotheses, no overlap with v2, 50 per market, family cap 25%;
- primary ranking is deterministic and uses 2023 only;
- primary manifest precedes 2024 replay and contains alpha 0.03;
- diagnostics are promotion-ineligible;
- completed HTF bars and explicit contracts only;
- no status inheritance, Q4, spend, network or order capability;
- deterministic smoke, full tests, compile, governance and integrity pass.

## Rollback conditions

Rollback on v2 overlap, primary selection using 2024, more than one promotion
test, diagnostic promotion, alpha drift, HTF leakage, Q4 read, nondeterminism or
order capability.

## Expected decision information value

`0.995`: this is the first production experiment with a calibrated promotion lane
that simultaneously preserves broad discovery diversity, low family false
admission and useful power.
