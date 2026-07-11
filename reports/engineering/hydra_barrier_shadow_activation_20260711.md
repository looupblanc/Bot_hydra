# Immutable Engineering Task — Barrier Candidate Zero-Order Shadow Activation

## Objective

Activate exactly one frozen barrier-hazard candidate as `SHADOW_RESEARCH_ACTIVE` only if the official source experiment classifies it `SHADOW_RESEARCH_CANDIDATE`, exports an immutable valid configuration, and contains no hard invalidation. Activation is forward research, not validation or trading readiness.

## Required behavior

- Verify task, source-result, result-hash, configuration file, file hash, semantic configuration hash, and candidate identity.
- Require source tier `SHADOW_RESEARCH_CANDIDATE` and `permits_zero_risk_shadow=true`.
- Require positive mini and micro development evidence, safe MLL flag, deterministic signals, real-time feature specification, complete observability, and no hard invalidations.
- Audit every shadow code surface for broker/order imports, functions, methods, credentials, or endpoints.
- Emit an immutable activation manifest with `SHADOW_RESEARCH_ACTIVE`, virtual execution only, zero broker connections, zero outbound orders, stale-data fail-closed, and initial state `WAITING_FOR_FRESH_FORWARD_DATA`.
- Add the immutable candidate to the existing single-writer shadow registry; never create a second registry or service.
- Preserve the source candidate and configuration in place; any modification requires a new version and activation.
- Q4, paid data, network requests, live trading, broker connections, and outbound orders are prohibited.

## Allowed paths

- `hydra/shadow/activation.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `tests/test_shadow_activation.py`
- `tests/test_mission_scheduler.py`
- ignored activation result/state directories.

## Protected paths

- `config/governance/**`
- existing active shadow manifests/configurations;
- Q4 and data-role governance;
- broker, credential, order, and live-execution code;
- mission/registry schemas.

## Acceptance tests

- candidate/result/config identity and hashes recompute;
- source tier and admission are required;
- altered source/config is rejected;
- hard invalidation is rejected;
- code-surface order capability is rejected;
- activation manifest is immutable;
- restart is safe and registry remains one-writer;
- stale/missing data produces no signal and no fill;
- duplicate signals and MLL breaches fail closed;
- outbound orders and broker connections remain exactly zero;
- Q4 access delta, paid spend, and network requests remain zero;
- full pytest, compileall, governance, integrity, no-lookahead, and secret checks pass.

## Rollback conditions

- any identity/hash mismatch;
- source is not an official shadow-research candidate;
- any hard invalidation or incomplete risk/execution package;
- any broker/order/credential surface;
- registry mutation of an existing active version;
- protected governance change.

## Expected information value

High: activation begins forward observation of a development-supported but statistically uncertain path-hazard candidate at zero financial risk, while the stricter `p<=0.03` and holdout requirements remain unchanged for later promotion.
