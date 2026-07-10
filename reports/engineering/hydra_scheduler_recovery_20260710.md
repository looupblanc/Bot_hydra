# Immutable Engineering Task — HYDRA Scheduler Recovery

Task ID: `eng_hydra_scheduler_recovery_20260710_v1`

Status at authoring: preregistered before implementation.

## Scientific objective

Restore the existing single-writer autonomous mission so that the selected
`calibration_affected_atom_retest_design` is durably queued, executed exactly
once, evidenced, checkpointed, and followed by a scientifically meaningful
next decision instead of an indefinite `WAIT` loop.

The first experiment must rank prior atom decisions by calibration sensitivity
and expected decision information gain, select only a bounded discriminative
set, assign fresh atom IDs, and create a fresh immutable preregistration. It
must preserve null rejection and must not inherit any historical pass status.

## Required behavior

1. Persist real experiment lifecycle states (`QUEUED`, `RUNNING`, terminal)
   and connect the planner, queue, executor, evidence ledger, heartbeat, and
   checkpoints.
2. Recover a stale `RUNNING` experiment after a controlled restart without
   duplicating a completed experiment.
3. Do not emit repeated decision-ledger `WAIT` entries when an experiment is
   actionable or when a follow-up research action can be selected.
4. Execute the calibration-affected retest design deterministically from the
   frozen historical atom report and preregistration, without Q4 or paid data.
5. Include positive controls, negative controls, calibration-invariant old
   failures, and calibration-sensitive candidates. Separate fatal,
   hypothesis-specific mandatory, diagnostic, and informational attacks.
6. Store current experiment, latest completed experiment, queue size,
   scientific conclusion, progress timestamps, and a scheduler deadline in
   structured mission state and heartbeat output.
7. Make status/doctor database reads genuinely read-only and make doctor fail
   health when scientific progress is stale with abandoned actionable work.
8. Keep no-live-trading, single-writer, budget, Q4, evidence-scope, and
   governance invariants unchanged or stricter.

## Allowed paths

- `hydra/mission/controller.py`
- `hydra/mission/decision_engine.py`
- `hydra/mission/experiment_queue.py`
- `hydra/mission/mission_state.py`
- `hydra/mission/watchdog.py`
- new non-governance modules under `hydra/mission/`
- `scripts/hydra_mission_status.py`
- `scripts/hydra_mission_doctor.py`
- new deterministic non-live research scripts under `scripts/`
- mission-specific tests under `tests/`
- versioned research/engineering reports and preregistration artifacts

## Protected paths

All paths listed in `config/governance/hydra_governance_v1.yaml`, especially:

- `config/governance/hydra_governance_v1.yaml`
- `hydra/governance/**`
- `hydra/data/budget.py`
- protected evidence-scope and lockbox modules
- `tests/test_mission_governance_and_calibration.py`

No data-role boundary, Q4 rule, budget cap, or broker/live-trading setting may
be changed.

## Acceptance tests

- Targeted scheduler/queue/recovery/design tests pass.
- A deterministic isolated smoke mission executes the design once, produces a
  fresh preregistration, completes the experiment, appends evidence, and
  selects a non-duplicate follow-up.
- Full `python -m pytest -q` is at least as green as baseline (`98 passed`).
- `python -m pytest -q tests/test_no_lookahead.py` passes (`3 passed`).
- `python -m compileall hydra scripts tests` passes.
- SQLite mission and registry `PRAGMA integrity_check` return `ok`.
- Governance hash remains
  `1af3e5d3aecd720153a8690e5fd4f99104154038e22cf732da54b12370eece08`.
- Q4 access count remains `0`; Databento spend remains
  `22.963245768100997 USD`; no paid request is made.
- Process-lock and single-writer tests pass.
- Secret scan finds no credential material or newly tracked secrets.
- After deployment there is one writer, a fresh heartbeat, preserved queue,
  no duplicate experiment, advancing cycles, a completed research experiment,
  new evidence, and a meaningful next action.

## Rollback conditions

Rollback to commit `0f9f873bd85c210f9a707b23dfaf42a13142069e` and restore the
verified snapshot if any protected digest changes, Q4 access becomes nonzero,
spend changes, registry/mission integrity fails, more than one writer appears,
an experiment is duplicated, a test regresses, or the service cannot resume
cleanly after at most two engineering attempts.

Snapshot:
`/root/hydra-mission-snapshots/takeover_20260710T115257Z`

## Expected information value

High. The present controller cannot execute any experiment after calibration,
so this repair changes the decision path from zero scientific throughput to a
bounded falsification experiment. The selected design directly resolves the
dominant validator uncertainty (whether useful atoms were falsely killed)
using cached evidence and zero data cost, while negative controls guard against
manufacturing passes. It is therefore the highest expected decision
information gain per total research cost currently available.

