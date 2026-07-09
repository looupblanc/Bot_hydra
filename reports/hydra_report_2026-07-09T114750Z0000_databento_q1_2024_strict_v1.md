# HYDRA Research Report

Generated: 2026-07-09T11:47:50+00:00

## Run Context
- Run mode: real data strict
- Data provider: databento
- Dataset: GLBX.MDP3
- Schema: ohlcv-1m
- Requested date range: 2024-01-01 to 2024-03-31
- Actual date range: 2024-01-01T23:00:00+00:00 to 2024-03-28T20:59:00+00:00
- Requested candidate count: 500
- Symbols: ES, MES, NQ, MNQ
- Timeframes: 1m
- Seed: 43
- Report tag: databento_q1_2024_strict_v1

## Warnings
- Historical real-data research only. No live trading or broker execution was used.

## Validation Discipline
- No-lookahead audit: enabled
- Walk-forward validation: included in robustness score
- Monte Carlo robustness: included in robustness score
- Min trade count: enforced
- Profit factor threshold: enforced
- Sharpe threshold: enforced
- Max drawdown control: enforced
- MLL simulation: enforced
- MLL buffer check: enforced
- Duplicate/correlation check: enforced
- Portfolio interaction check: V4 risk compression executed

## Summary
- Total candidates: 500
- Qualified candidates: 0
- Rejected candidates: 500
- V4 selected portfolio count: 0
- MLL buffer min/avg: -87114.77 / -5468.93
- MLL breaches: 268

## Data Quality
- Bars ES: 85707
- Bars MES: 85773
- Bars MNQ: 86003
- Bars NQ: 85607

## Missing Intervals
- ES: gaps_gt_1m=313 max_gap_seconds=203460
- MES: gaps_gt_1m=258 max_gap_seconds=203460
- MNQ: gaps_gt_1m=68 max_gap_seconds=203460
- NQ: gaps_gt_1m=311 max_gap_seconds=203460

## Status Distribution
- REJECTED_NO_EDGE: 356
- REJECTED_TOO_FEW_TRADES: 144

## Top Families
- session_exhaustion_reversal: 120
- volatility_shift_continuation: 112
- multi_session_momentum_exhaustion: 100
- volatility_regime_expansion: 89
- regime_compression_breakout: 79

## Rejection Reasons
- profit_factor_or_net_profit_below_threshold: 354
- below_min_trade_count: 144
- sharpe_below_threshold: 2

## Correlation Clusters
- No correlated candidates logged.

## Best Candidates
- cand_175f55d49ad4 session_exhaustion_reversal ES 1m status=REJECTED_TOO_FEW_TRADES net=793.85 dd=0.00 buffer=4500.00 robust=0.807
- cand_8cb21be8d2fe session_exhaustion_reversal NQ 1m status=REJECTED_TOO_FEW_TRADES net=9653.46 dd=0.00 buffer=9653.46 robust=0.806
- cand_95048b5e46fc volatility_shift_continuation NQ 1m status=REJECTED_NO_EDGE net=15985.93 dd=11036.92 buffer=15985.93 robust=0.741
- cand_82c4578ddb44 volatility_shift_continuation MNQ 1m status=REJECTED_NO_EDGE net=1622.75 dd=2130.07 buffer=4352.92 robust=0.725
- cand_ccb1421c9912 volatility_shift_continuation MNQ 1m status=REJECTED_NO_EDGE net=993.66 dd=720.49 buffer=4154.91 robust=0.725
- cand_cc34cb02e87a volatility_shift_continuation NQ 1m status=REJECTED_NO_EDGE net=2919.23 dd=5155.65 buffer=3423.42 robust=0.714
- cand_4e3513d1183f session_exhaustion_reversal ES 1m status=REJECTED_TOO_FEW_TRADES net=1780.13 dd=10.10 buffer=4500.00 robust=0.706
- cand_7491b4b55e9f session_exhaustion_reversal MES 1m status=REJECTED_TOO_FEW_TRADES net=89.03 dd=12.72 buffer=4500.00 robust=0.706
- cand_942a9a746023 volatility_shift_continuation NQ 1m status=REJECTED_NO_EDGE net=5415.25 dd=5520.33 buffer=5415.25 robust=0.703
- cand_b23936e0e839 volatility_shift_continuation NQ 1m status=REJECTED_NO_EDGE net=2366.95 dd=6860.31 buffer=3300.38 robust=0.691
- cand_412db2286d3f regime_compression_breakout ES 1m status=REJECTED_TOO_FEW_TRADES net=1567.00 dd=468.33 buffer=4489.72 robust=0.680
- cand_cce7679899ef volatility_shift_continuation NQ 1m status=REJECTED_NO_EDGE net=18527.12 dd=13581.01 buffer=18527.12 robust=0.634
- cand_4be02b30fdc3 regime_compression_breakout NQ 1m status=REJECTED_TOO_FEW_TRADES net=1337.80 dd=1603.97 buffer=4367.21 robust=0.621
- cand_9eff51ccecf9 volatility_shift_continuation MNQ 1m status=REJECTED_NO_EDGE net=1351.75 dd=2305.25 buffer=3957.84 robust=0.619
- cand_9c3630421757 regime_compression_breakout MNQ 1m status=REJECTED_TOO_FEW_TRADES net=46.41 dd=67.16 buffer=4500.00 robust=0.617

## Risk-Compressed Portfolio
- No portfolio promotions yet.

## MLL Summary
- Minimum buffer: -87114.77
- Average buffer: -5468.93
- Breached candidates: 268

## Next Recommended Action
- Diagnose strict real-data rejections, then expand sample length and add out-of-sample Databento validation before any paper/shadow validation.
