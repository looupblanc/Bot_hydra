# HYDRA Research Report

Generated: 2026-07-09T11:25:04+00:00

## Run Context
- Run mode: synthetic diagnostic
- Requested candidate count: 500
- Symbols: ES, MES, NQ, MNQ
- Seed: 43
- Report tag: post_databento_scaffold_check

## Warnings
- Synthetic results are pipeline diagnostics only and must not be interpreted as real trading edge.

## Summary
- Total candidates: 500
- Qualified candidates: 22
- Rejected candidates: 478
- V4 selected portfolio count: 10
- MLL buffer min/avg: -1644.22 / 3599.80
- MLL breaches: 6

## Status Distribution
- REJECTED_NO_EDGE: 439
- REJECTED_TOO_FEW_TRADES: 38
- QUALIFIED: 12
- PROMOTED_TO_PORTFOLIO: 10
- REJECTED_CORRELATED: 1

## Top Families
- session_exhaustion_reversal: 118
- volatility_shift_continuation: 102
- multi_session_momentum_exhaustion: 102
- volatility_regime_expansion: 98
- regime_compression_breakout: 80

## Rejection Reasons
- profit_factor_or_net_profit_below_threshold: 439
- below_min_trade_count: 38
- equity_curve_correlation_too_high: 1

## Best Candidates
- cand_0b7d8ec31241 multi_session_momentum_exhaustion MES daily status=REJECTED_TOO_FEW_TRADES net=23.34 dd=6.77 buffer=4500.00 robust=0.834
- cand_48593e6dc19c multi_session_momentum_exhaustion NQ daily status=PROMOTED_TO_PORTFOLIO net=962.83 dd=727.08 buffer=4500.00 robust=0.774
- cand_a3a4bda900f1 multi_session_momentum_exhaustion NQ daily status=PROMOTED_TO_PORTFOLIO net=712.20 dd=788.58 buffer=4500.00 robust=0.772
- cand_df5e483dc4d1 multi_session_momentum_exhaustion NQ 5d status=PROMOTED_TO_PORTFOLIO net=516.38 dd=451.87 buffer=4048.13 robust=0.766
- cand_e2778af583e8 multi_session_momentum_exhaustion NQ 5d status=REJECTED_TOO_FEW_TRADES net=595.98 dd=53.35 buffer=4446.65 robust=0.754
- cand_0e291f5f58b9 session_exhaustion_reversal NQ 3d status=PROMOTED_TO_PORTFOLIO net=877.66 dd=560.16 buffer=4347.86 robust=0.748
- cand_33f5b1be7d1e multi_session_momentum_exhaustion ES daily status=PROMOTED_TO_PORTFOLIO net=452.01 dd=156.66 buffer=4468.09 robust=0.748
- cand_f2ea36b9a75e multi_session_momentum_exhaustion NQ 5d status=REJECTED_TOO_FEW_TRADES net=474.44 dd=235.28 buffer=4401.51 robust=0.731
- cand_28eca1991beb multi_session_momentum_exhaustion NQ 5d status=PROMOTED_TO_PORTFOLIO net=732.06 dd=353.02 buffer=4369.68 robust=0.706
- cand_2d737fc658c5 volatility_shift_continuation ES 3d status=PROMOTED_TO_PORTFOLIO net=343.55 dd=308.91 buffer=4405.34 robust=0.685
- cand_39c21cb68a5d volatility_shift_continuation ES 3d status=PROMOTED_TO_PORTFOLIO net=292.96 dd=201.13 buffer=4434.27 robust=0.681
- cand_86e00cd8acb2 volatility_shift_continuation ES 3d status=REJECTED_CORRELATED net=303.97 dd=208.69 buffer=4431.80 robust=0.681
- cand_a304285613fb multi_session_momentum_exhaustion ES daily status=QUALIFIED net=315.74 dd=560.72 buffer=4224.81 robust=0.631
- cand_2a738bb4bff4 session_exhaustion_reversal NQ 3d status=QUALIFIED net=523.49 dd=655.65 buffer=4246.17 robust=0.613
- cand_00c11871ca2e multi_session_momentum_exhaustion ES 2d status=PROMOTED_TO_PORTFOLIO net=277.88 dd=154.41 buffer=4410.04 robust=0.589

## Risk-Compressed Portfolio
- cand_48593e6dc19c multi_session_momentum_exhaustion NQ daily net=962.83 dd=727.08 buffer=4500.00 robust=0.774
- cand_a3a4bda900f1 multi_session_momentum_exhaustion NQ daily net=712.20 dd=788.58 buffer=4500.00 robust=0.772
- cand_df5e483dc4d1 multi_session_momentum_exhaustion NQ 5d net=516.38 dd=451.87 buffer=4048.13 robust=0.766
- cand_0e291f5f58b9 session_exhaustion_reversal NQ 3d net=877.66 dd=560.16 buffer=4347.86 robust=0.748
- cand_33f5b1be7d1e multi_session_momentum_exhaustion ES daily net=452.01 dd=156.66 buffer=4468.09 robust=0.748
- cand_28eca1991beb multi_session_momentum_exhaustion NQ 5d net=732.06 dd=353.02 buffer=4369.68 robust=0.706
- cand_2d737fc658c5 volatility_shift_continuation ES 3d net=343.55 dd=308.91 buffer=4405.34 robust=0.685
- cand_39c21cb68a5d volatility_shift_continuation ES 3d net=292.96 dd=201.13 buffer=4434.27 robust=0.681
- cand_00c11871ca2e multi_session_momentum_exhaustion ES 2d net=277.88 dd=154.41 buffer=4410.04 robust=0.589
- cand_0c40e3632425 multi_session_momentum_exhaustion ES 2d net=84.29 dd=75.25 buffer=4500.00 robust=0.543

## MLL Summary
- Minimum buffer: -1644.22
- Average buffer: 3599.80
- Breached candidates: 6

## Next Recommended Action
- Add Databento historical futures ingestion and strict no-lookahead tests before expanding real-data validation.
