# HYDRA Engineering Task — Immutable YM Zero-Order Shadow Activation

## Identity and objective

- Experiment ID: `ym_immutable_shadow_activation_v1`
- Candidate: `strategy_open_gap_continuation_YM_v1`
- Operational classification: `SHADOW_RESEARCH_ACTIVE` (registry evidence tier
  remains `SHADOW_ACTIVE` for compatibility).
- Objective: activate the exact frozen configuration for forward, virtual-only
  evidence collection after the strict audit confirms no hard invalidation.
- Q4, paid data, broker connectivity, real orders and live trading: prohibited.

## Required behavior

1. Verify the strict-promotion result and its semantic result hash.
2. Require `shadow_activation_eligible=true` and zero hard invalidations.
3. Verify the frozen shadow configuration and its semantic hash.
4. Scan the shadow package and runtime surface for prohibited broker, credential
   and order-submission capabilities.
5. Export an immutable activation manifest containing the exact candidate and
   configuration hashes, stale-data behavior, MLL limits, logging and kill rules.
6. Register the active version through the existing mission controller only.
7. Tick a lightweight shadow pipeline on every controller cycle, independently
   of discovery/promotion scheduling.
8. When no fresh forward feed is available, remain operational but fail closed:
   emit no signals/fills and expose `WAITING_FOR_FRESH_FORWARD_DATA` explicitly.
9. Never modify an active configuration in place; a changed version needs a new
   candidate/version and activation manifest.

## Allowed paths

- `hydra/shadow/activation.py`
- `hydra/pipelines/shadow_pipeline.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `tests/test_shadow_activation.py`
- `tests/test_pipeline_scheduler.py`
- immutable report artifacts and untracked runtime shadow state

## Protected paths

- governance and protected manifests
- source candidate/result/configuration artifacts
- Q4 and later historical lockbox data
- mission/registry databases except through the existing single controller writer
- broker, credential, secret, API-order and live-execution code

## Acceptance tests

- altered strict result/configuration fails closed;
- hard-invalidated candidate cannot activate;
- configuration immutability is enforced;
- activation manifest contains no broker/order capability;
- missing/stale forward input produces no signal or fill;
- pipeline tick survives restart and is independent of experiment queue state;
- stop/kill state fails closed;
- full shadow, mission, no-lookahead, compile, integrity and secret tests pass.

## Rollback conditions

Rollback if any outbound order surface is introduced, active configuration can be
mutated, stale input can generate a signal, the controller creates a second
writer, Q4 is read, or runtime state is committed to Git.

## Expected decision information value

`0.99`: no financial risk and no holdout contamination; forward observation is
the fastest honest way to resolve the frozen parent’s temporal uncertainty while
the other research pipelines continue.
