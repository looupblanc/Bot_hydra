# HYDRA Gate-Aware Early-Exit Audit

Generated: 2026-07-10T02:00:00Z

## Previous Short Run

- Report: `reports/gate_aware_remediation/gate_aware_remediation_report_2026-07-10T013141Z0000_gate_aware_remediation_blind_validation_v2.md`
- Log: `logs/gate_aware_remediation_20260710T012947Z.log`
- Runtime: 113.61 seconds
- Remediation children completed: 391
- Process exit: successful script completion; no traceback or exception was recorded in the log.

Exact launched command recovered from the generated report/checkpoint:

```bash
python scripts/run_gate_aware_remediation_factory.py --registry registry/hydra_registry.db --dataset GLBX.MDP3 --symbols ES MES NQ MNQ --development-start 2024-01-01 --development-end 2024-03-29 --q2-start 2024-04-01 --q2-end 2024-07-01 --q3-start 2024-07-01 --q3-end 2024-10-01 --q4-start 2024-10-01 --q4-end 2025-01-01 --schema ohlcv-1m --databento-budget-usd 100 --databento-budget-start 2026-07-10 --auto-purchase-under-budget --budget-safety-ceiling-usd 98 --primary-topstep-mode no-dll --evaluate-xfa-standard --evaluate-xfa-consistency --evaluate-optional-dll-sensitivity --account-size 150000 --profit-target 9000 --mll-distance 4500 --workers auto --single-writer-registry --runtime-hours 6 --checkpoint-every-minutes 20 --target-economic-strategy-units 50 --strict-lockbox --conservative-intrabar --seed 5050 --report-tag gate_aware_remediation_blind_validation_v2
```

## Exit Reason

The process exited because the original orchestrator created one finite remediation child list from the initially selected parent pool and returned when that list was exhausted. `--runtime-hours 6` was implemented only as a maximum cap, not as a persistence requirement.

This was not caused by:

- target quality reached;
- valid trading-ready count reached;
- valid economic strategy unit count reached;
- an exception treated as success;
- budget failure;
- registry integrity failure.

The invalid provisional counts in the short-run report were reporting semantics, not the branch that stopped execution:

- `q1_promotion_finalists: 391` incorrectly meant selected repair parents, not frozen promotion finalists.
- `economic_strategy_units: 14835` incorrectly used coarse equivalence output, not trade-level behavioral clustering.

## Corrections

- Added explicit run-control modes: minimum runtime, maximum runtime, continue-until-deadline, minimum cycles, minimum remediation children, valid-quality-only stop, and optional exhaustion stop.
- Replaced one-shot child evaluation with a persistent adaptive cycle:
  diagnose parents -> generate children -> evaluate -> write through one SQLite connection -> refresh parent pool -> update policy outcomes -> checkpoint -> continue.
- Quality target stopping now requires validated trading-ready, Q4 pass, and execution validation counts. Provisional counts are logged and ignored.
- Q1 selected repair parents are no longer reported as Q1 promotion finalists.
- Economic strategy units are set to zero until trade-level behavioral clustering is implemented.
- Q3 is treated as quarantined confirmation/development for affected lineages. Q4 remains raw-only and uninspected.

## Smoke Test

- Command tag: `gate_aware_remediation_run_control_smoke_v1`
- Runtime: 392.66 seconds total; stop decision reached the configured max runtime at 342.22 seconds.
- Workers: 3
- Cycles completed: 40
- Remediation children completed: 1,600
- Duplicate rate: 0.0
- Parent pool refilled from 460 to 750 eligible parents.
- Last stop reason: `max_runtime_reached`
- Provisional quality target: true but ignored.
- Valid quality target: false.
- Report: `reports/gate_aware_remediation/gate_aware_remediation_report_2026-07-10T015851Z0000_gate_aware_remediation_run_control_smoke_v1.md`
- Checkpoint: `reports/checkpoints/gate_aware_remediation/gate_aware_checkpoint_2026-07-10T015851Z0000_gate_aware_remediation_run_control_smoke_v1.md`
