# HYDRA TURBO FOUNDRY V2 — immutable engineering task specification

Status: PREREGISTERED

Preregistered at: 2026-07-12T01:18:00+00:00
Baseline commit: `2598e19ccaeef9f59cf2b6660aec63ae14b634f8`

## Scientific objective

Increase useful, development-only strategy throughput by at least 5x at the
Stage-0/Stage-1 funnel and at least 3x for exact replay while preserving the
same deterministic candidate decisions, all hard integrity gates, Q4 sealing,
single-writer mission state, and zero order capability.

The first deployed Turbo cycle must evaluate several thousand structurally
fingerprinted proposals, use a power-aware micro-batch plan, send no more than
5% to expensive replay, and publish measured conversion and resource metrics.

## Baseline

- Recent pre-close primary: 32 structures in approximately 82 seconds
  (approximately 1,405 structures/hour), one Stage-1 survivor.
- Recent mini/micro primary: 96 structures in approximately 317 seconds
  (approximately 1,090 structures/hour), five Stage-1 survivors and three full
  2024 replays.
- Accelerated context tournament: 5,000 structural descriptions, 300 executable
  Stage-1 evaluations, 21 broader replays, four promising candidates in 327.84
  seconds. Feature preparation consumed 135.99 seconds and selection/freeze
  consumed 200.14 seconds.
- Mission state at takeover: `ENGINEERING_BLOCKED`, queue empty, Q4 access zero,
  five immutable shadow configurations but no fresh forward-data heartbeat.

The benchmark suite will additionally establish a frozen scalar reference for
identical Stage-1 and exact-replay inputs. Performance claims must compare the
same inputs and assert identical outputs.

## Required behavior

1. The controller remains the control plane and the sole mission/registry
   writer.
2. Structural generation, fingerprinting and Stage-1 evaluation operate in
   bounded batches on compact arrays.
3. Reusable, past-only feature matrices are computed once per governed source
   fingerprint and shared read-only with long-lived workers for a Turbo batch.
4. Exact replay is scheduled through a bounded worker pool; workers return pure
   results and never open mission or registry SQLite databases.
5. Batched results are committed only after deterministic validation by the
   controller.
6. The power planner rejects or enlarges underpowered proposals rather than
   falsifying a family from a tiny arbitrary batch.
7. Meta-screening may prioritize compute but preserves at least 20% pure
   exploration and never supplies validation evidence.
8. Operational shadow labels reflect actual data heartbeats:
   `SHADOW_WAITING_FOR_FEED` until a fresh completed bar exists and
   `SHADOW_RESEARCH_ACTIVE` only while fresh forward data is processed.
9. No Q4, paid-data, network, live-trading, broker or order capability is added.
10. A completed Turbo cycle automatically schedules another supported research
    action or leaves a precise external authorization blocker while keeping all
    supported research progressing.

## Allowed paths

- `hydra/compute/**`
- `hydra/features/**` except changes that weaken feature availability
- `hydra/strategies/**`
- `hydra/research/**`
- `hydra/shadow/**`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `scripts/**` for status/benchmark commands
- `tests/**`
- `reports/engineering/**`

## Protected paths

- `config/governance/**`
- `hydra/governance/**`
- protected data-role and Q4 ledgers/manifests
- `mission/state/**`
- registry and mission databases
- raw/cached market data
- immutable shadow configuration files
- systemd unit identity and service count

## Acceptance tests

- Full `python -m pytest -q` passes.
- All no-lookahead, Q4, budget, single-writer and shadow no-order tests pass.
- `python -m compileall hydra scripts tests` passes.
- Mission and registry SQLite `PRAGMA integrity_check` return `ok`.
- Governance checksum and Q4 access remain valid and unchanged in policy.
- Secret scan finds no committed credential.
- A scalar/reference replay and batched replay produce identical frozen outputs.
- Measured Stage-0/Stage-1 throughput is at least 5x reference.
- Measured exact-replay throughput is at least 3x reference.
- Aggregate worker utilization is at least 80% in an active benchmark, or an
  exact, reproducible system bottleneck is reported without overstating success.
- Scheduler idle time is below 5% while eligible Turbo work exists.
- First systemd Turbo experiment contains at least several thousand proposals,
  Q4 access zero, and no outbound order capability.

## Rollback conditions

Rollback the engineering commit and restart the same service from the baseline
if deterministic outputs diverge, a protected checksum changes, Q4 is accessed,
more than one controller/writer appears, mission queue/state is lost, order
capability appears, or either SQLite integrity check fails.

If performance goals are not met but correctness holds, do not fabricate a
success: retain only changes with independently measured value, document the
bottleneck, and queue the next architecture iteration.

## Expected decision information value

High (0.98). The mission is blocked with an empty queue after an underpowered
96-structure experiment. A correct batch funnel directly tests substantially
more distinct mechanisms per unit time, while power planning prevents false
family exhaustion and honest shadow labels prevent confusing configuration
readiness with forward evidence.
