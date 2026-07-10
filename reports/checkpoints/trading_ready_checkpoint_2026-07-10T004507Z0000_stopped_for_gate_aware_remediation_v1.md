# HYDRA Trading-Ready Factory Stop Checkpoint

- Generated: 2026-07-10T00:45:07.451678+00:00
- Tag: stopped_for_gate_aware_remediation_v1
- Registry integrity: ok
- Latest consistent checkpoint: reports/checkpoints/trading_ready_checkpoint_2026-07-10T002909Z0000_overnight_resume_strategy_bank_topstep_q1_v2.md
- Total candidates: 17994
- Completed candidates this long-run continuation: 14731
- Economically viable: 403
- Topstep viable: 21
- Trading-ready: 0
- Near-miss: 160
- Best Topstep score: 1.000000
- Best promotion score: 0.884426
- Resume command: `python scripts/run_trading_ready_topstep_factory.py --symbols ES MES NQ MNQ --start 2024-01-01 --end 2024-03-28 --schema ohlcv-1m --dataset GLBX.MDP3 --use-cache-only --strict --seed 2050 --workers auto --min-total-candidates 80000 --target-trading-ready 50 --target-economically-viable 30 --target-topstep-viable 20 --target-portfolio-candidates 10 --account-size 150000 --profit-target 9000 --mll 4500 --no-daily-loss-limit --simulate-funded --simulate-payouts --checkpoint-every-minutes 20 --max-runtime-hours 6 --continue-until-quality --report-tag overnight_resume_strategy_bank_topstep_q1_v2`
