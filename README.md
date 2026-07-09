# HYDRA Research Factory

HYDRA is a futures strategy research factory. This repository is currently in research mode only.

Live trading is disabled. Synthetic data is used only for smoke testing the pipeline and must not be interpreted as evidence of trading edge.

## Smoke Commands

```bash
python -m compileall hydra scripts
python scripts/run_strategy_factory_v3_expansion.py --candidates 200 --symbols ES MES NQ MNQ --synthetic --seed 42
python scripts/inspect_registry.py --summary
python scripts/run_v4_risk_compression.py --min-buffer 500 --target-buffer 2500 --max-strategies 10
python scripts/export_report.py
```
