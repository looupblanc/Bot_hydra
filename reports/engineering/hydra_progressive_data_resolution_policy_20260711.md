# HYDRA Progressive Data Resolution Policy

## Scientific objective

Prevent expensive or selection-contaminating microstructure acquisition from
becoming a substitute for bar-level evidence.  Escalate resolution only when a
bounded, frozen research decision can change because of information that the
current tier cannot resolve.

## Required behavior

- `ohlcv-1m` remains the canonical mass-discovery and multi-timeframe source.
- `ohlcv-1s` requires serious bar-level evidence, bounded event windows, and an
  intrabar path ambiguity that can change the candidate decision.
- `trades` additionally requires a preregistered event-intensity or event-bar
  hypothesis.
- `tbbo` is restricted to serious finalists with bounded windows and an
  execution/spread ambiguity that can change the decision.
- `mbp-1` is restricted to preregistered, genuinely book-dependent finalist
  hypotheses after simpler resolutions are shown insufficient.
- `mbo` is prohibited.
- Every paid escalation requires an official cost estimate, positive expected
  decision information gain, and at least USD 30 remaining for final lockbox
  and execution validation.
- Cache presence does not waive the scientific eligibility gates.
- The policy emits a deterministic, auditable decision and performs no network
  request, data read, status promotion, Q4 access, or order action.

## Allowed paths

- `hydra/data/resolution_ladder.py`
- `tests/test_resolution_ladder.py`
- this engineering specification

## Protected paths

All governance-kernel, protected-manifest, budget-kernel, evidence-scope,
promotion-contract, lockbox-guard, mission DB, registry DB, data and ledger
paths remain read-only.

## Acceptance tests

- 1m discovery is eligible with a valid estimate and reserve.
- 1s, trades, TBBO and MBP-1 fail closed when their role-specific evidence is
  missing.
- TBBO cannot be used for broad discovery.
- MBP-1 requires both book dependence and simpler-tier insufficiency.
- MBO is always rejected.
- an absent official estimate blocks paid acquisition.
- a request that would breach the USD 30 reserve is rejected.
- cache hits still obey scientific gates but do not consume budget.
- decisions are deterministic and serializable.

## Rollback conditions

Rollback if any existing test regresses, a protected path changes, the policy
permits MBO, the reserve can be bypassed, or the module performs I/O beyond
reading its explicit inputs.

## Expected information value

High.  It preserves cheap high-throughput discovery while reserving expensive
microstructure evidence for bounded ambiguities capable of changing a serious
candidate decision.
