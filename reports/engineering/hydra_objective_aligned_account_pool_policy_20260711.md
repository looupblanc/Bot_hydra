# HYDRA Objective-Aligned Account Pool Policy

## Scientific objective

Separate research utility by account phase so a candidate is not forced to
optimize Combine passage, XFA payouts, and defensive account utility
simultaneously.  Admit safe zero-order shadow research using calibrated,
role-specific evidence while retaining a stricter PAPER/funded contract.

## Required behavior

- Preserve the three immutable objective pools: `COMBINE_PASSER_POOL`,
  `XFA_PAYOUT_POOL`, and `DEFENSIVE_ACCOUNT_POOL`.
- Make legacy promotion select only the phase-specific account gates belonging
  to the candidate pool.
- Permit XFA and defensive candidates to be assessed on marginal account
  utility rather than universal standalone PnL.
- Preserve every fatal integrity, no-lookahead, execution, risk, immutable
  configuration, observability, and zero-order gate.
- Treat calibrated null, parameter-stability, and contract-transfer shortfalls
  as explicit uncertainty for zero-risk Shadow Research when the candidate has
  plausible temporal evidence and a complete safe package.
- Continue requiring the stronger statistical/robustness/holdout evidence for
  `PAPER_SHADOW_READY`.
- Replace the XFA halving payout proxy with deterministic funded-account replay
  including payouts, floor reset, and post-payout survival.
- Inherit no parent status and perform no Q4 access, data purchase, network
  request, broker connection, or order action.

## Allowed paths

- `hydra/foundry/status.py`
- `hydra/shadow/activation.py`
- `hydra/shadow/promotion.py`
- `hydra/promotion/pipeline.py`
- `hydra/promotion/readiness.py`
- `hydra/factory/post_mutation_successive_halving.py`
- focused tests for these modules
- `tests/test_shadow_activation.py`
- focused shadow-promotion tests
- this engineering specification

## Protected paths

The governance kernel and manifest, budget kernel, evidence scope, promotion
contract, lockbox guard, protected tests, persistent databases, ledgers, raw
data, and shadow configurations remain read-only.

## Acceptance tests

- Alpha/Combine behavior remains backward compatible.
- XFA and Defensive Shadow Research can pass with positive role utility even
  when standalone PnL is not their objective.
- hard invalidations and incomplete safety packages always reject admission.
- unresolved soft diagnostics are recorded and cannot yield
  `PAPER_SHADOW_READY`.
- Combine candidates do not depend on XFA/payout gates; XFA candidates do not
  depend on Combine; defensive candidates use marginal account utility.
- XFA bootstrap metrics come from actual funded replay and include payout and
  post-payout survival.
- existing and focused tests, compileall, and diff checks pass.

## Rollback conditions

Rollback if a fatal gate is softened, PAPER readiness is weakened, an unknown
pool is accepted, any protected path changes, an order/data/Q4 path appears, or
existing behavior regresses outside the intended role semantics.

## Expected information value

High.  This removes cross-objective false negatives, increases honest shadow
conversion, and directs mutation toward the account phase that can actually
benefit from each candidate.
