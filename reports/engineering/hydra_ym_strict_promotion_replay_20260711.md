# HYDRA Engineering Task — Frozen YM Strict Promotion Replay

## Immutable task identity

- Task date: `2026-07-11`
- Experiment ID: `ym_open_gap_strict_promotion_v1`
- Candidate: `strategy_open_gap_continuation_YM_v1`
- Candidate version policy: the frozen parent is audited unchanged; no child is created.
- Data role: `DEVELOPMENT_AND_FALSIFICATION_ONLY`
- Latest permitted observation: `< 2024-10-01`
- Q4 access: prohibited
- Paid data, network, live trading and broker access: prohibited

## Scientific objective

Resolve whether the frozen YM opening-gap continuation parent has credible,
non-catastrophic development transfer and safe zero-order shadow semantics, while
measuring its known temporal concentration honestly. This audit cannot promote
the strategy to `PAPER_SHADOW_READY` and cannot change its entry, exit, sizing,
cost, feature, threshold or session rules.

## Frozen sources

- Parent result SHA-256: `b6a501dddd579875088d30c90fe03bb858d02489364fd41d8db48a944e7fe75d`
- Parent result hash: `5d8935510337b92c89ee4ae00ba472700c9c436fe37aadcb92d50c78cd4f68c3`
- Parent trade ledger SHA-256: `e8f90171ae9efff1dfaca67312e47d05c2dff0200a8ea7a97c911186806cfba3`
- Freeze manifest candidate hash: `6aae37537aa39b0b7ad70d00afd0526b64b9fccfcbf396e0a7941f55300bd62a`
- Frozen shadow configuration hash: `d8ab9d9741aedd8c4b2ab9609d97124d8d66752873bf53eec24f39a13c23ff10`
- Explicit-contract map SHA-256: `401ca56ebab606c3eb2cbcf6ed244204f264ed2894c2ee0eb2310998f9244fda`
- Explicit roll-map hash: `705ce6fe27bac7dea9cb9d492413a5112bb60765c66aa75d03f9711bef348208`

## Required behavior

1. Verify every frozen source and semantic hash before analysis.
2. Reconstruct the exact YM and MYM parent ledgers from the immutable ledger and
   prove counts and net PnL equal the frozen candidate evidence.
3. Report YM and MYM results for 2024 Q1, Q2 and Q3 and their sequential pool.
4. Audit existing one-bar delayed-entry evidence without changing the parent.
5. Recalculate removal of the best trade, best day and best calendar month.
6. Run a deterministic five-event block bootstrap on the pooled 2024 YM path.
7. Run a matched direction-permutation null within quarter and gap-magnitude
   strata. This is mandatory candidate-level evidence, distinct from the existing
   five-event block sign-flip null.
8. Re-report the frozen parameter neighborhood, explicit YM/MYM transfer, costs,
   event attribution, MLL, consistency and Topstep path.
9. Produce an immutable strict-replay result, report and candidate-only ledger.
10. Preserve the exact parent status. A diagnostic weakness may record
    `INSUFFICIENT_STRICT_CONFIRMATION`; only a hard integrity/execution failure may
    prohibit zero-risk shadow activation.

## Frozen interpretation gates

Strict development confirmation requires all of:

- pooled 2024 YM and MYM net after costs are positive;
- at least two of three 2024 quarters are positive on YM;
- net remains positive after removing the best calendar month;
- deterministic block-bootstrap probability of non-positive mean is at most
  `0.20`;
- matched direction-permutation probability is at most `0.20`;
- the frozen mini/micro transfer, parameter-neighborhood and one-contract MLL
  safety checks remain valid;
- no source, timing, contract, cost or execution contradiction is found.

Failure of these confirmation gates is not fabricated proof of no edge. It is
recorded as uncertainty and does not by itself prohibit immutable no-order
forward shadow research. `PAPER_SHADOW_READY` remains impossible without the
official protected holdout policy.

## Allowed paths

- `hydra/research/ym_strict_promotion.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `tests/test_ym_strict_promotion.py`
- mission experiment reports and worker envelopes

## Protected paths

- governance kernel and protected manifests
- original parent result, ledger, preregistration and freeze manifest
- registry and mission databases except through the existing single writer
- Q4 and later-period data
- Databento spend/data-access role ledgers except through existing APIs
- all broker, secret and order-submission surfaces

## Acceptance tests

- frozen source mismatch fails closed;
- exact parent ledger projection is reproduced;
- fold, concentration, bootstrap and matched-null calculations are deterministic;
- Q4 timestamps are rejected;
- no parent mutation or inherited pass is possible;
- no outbound order capability exists;
- targeted and full test suites, no-lookahead, compile, integrity, governance and
  secret checks pass.

## Rollback conditions

Rollback and do not queue the experiment if any frozen hash differs, a protected
path changes, Q4 is read, results depend on row order, or an order/broker surface
is introduced.

## Expected decision information value

`0.98`: this resolves the strongest current shadow/Topstep candidate and directly
determines whether its exact immutable version can begin no-risk forward evidence
collection, without spending data budget or contaminating Q4.
