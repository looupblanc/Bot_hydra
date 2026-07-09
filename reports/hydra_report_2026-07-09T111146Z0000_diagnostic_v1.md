# HYDRA Research Report

Generated: 2026-07-09T11:11:46+00:00

## Run Context
- Run mode: synthetic diagnostic
- Requested candidate count: 2000
- Symbols: ES, MES, NQ, MNQ
- Seed: 43
- Report tag: diagnostic_v1

## Warnings
- Synthetic results are pipeline diagnostics only and must not be interpreted as real trading edge.

## Summary
- Total candidates: 2000
- Qualified candidates: 81
- Rejected candidates: 1919
- V4 selected portfolio count: 10
- MLL buffer min/avg: -2837.51 / 3598.51
- MLL breaches: 32

## Status Distribution
- REJECTED_NO_EDGE: 1758
- REJECTED_TOO_FEW_TRADES: 154
- QUALIFIED: 71
- PROMOTED_TO_PORTFOLIO: 10
- REJECTED_CORRELATED: 7

## Top Families
- session_exhaustion_reversal: 435
- volatility_regime_expansion: 406
- multi_session_momentum_exhaustion: 403
- volatility_shift_continuation: 390
- regime_compression_breakout: 366

## Rejection Reasons
- profit_factor_or_net_profit_below_threshold: 1758
- below_min_trade_count: 154
- equity_curve_correlation_too_high: 7

## Best Candidates
- cand_855a5f25c65f multi_session_momentum_exhaustion NQ 5d status=REJECTED_TOO_FEW_TRADES net=362.46 dd=61.06 buffer=4465.91 robust=0.860
- cand_a03d24f4ad35 multi_session_momentum_exhaustion NQ 5d status=REJECTED_TOO_FEW_TRADES net=641.46 dd=141.45 buffer=4500.00 robust=0.850
- cand_f424bb4bd620 multi_session_momentum_exhaustion NQ 5d status=REJECTED_TOO_FEW_TRADES net=968.98 dd=213.73 buffer=4412.75 robust=0.848
- cand_13f3e1b2dc8a multi_session_momentum_exhaustion MES daily status=REJECTED_TOO_FEW_TRADES net=37.17 dd=8.03 buffer=4497.44 robust=0.836
- cand_830d4467e3b5 multi_session_momentum_exhaustion MES daily status=REJECTED_TOO_FEW_TRADES net=23.34 dd=6.77 buffer=4500.00 robust=0.834
- cand_a78be33cac61 multi_session_momentum_exhaustion MES daily status=REJECTED_TOO_FEW_TRADES net=30.62 dd=7.16 buffer=4493.38 robust=0.833
- cand_996da8eada60 multi_session_momentum_exhaustion ES daily status=PROMOTED_TO_PORTFOLIO net=764.79 dd=280.51 buffer=4347.59 robust=0.819
- cand_143284b7a0f9 multi_session_momentum_exhaustion NQ 5d status=QUALIFIED net=1016.00 dd=767.26 buffer=4219.79 robust=0.793
- cand_651a624d10b7 multi_session_momentum_exhaustion ES 2d status=PROMOTED_TO_PORTFOLIO net=515.76 dd=242.23 buffer=4402.02 robust=0.789
- cand_acb3a34526af multi_session_momentum_exhaustion NQ 5d status=PROMOTED_TO_PORTFOLIO net=286.56 dd=152.56 buffer=4441.50 robust=0.785
- cand_630322fc9069 multi_session_momentum_exhaustion NQ daily status=QUALIFIED net=962.83 dd=727.08 buffer=4500.00 robust=0.774
- cand_30f29b6e4834 multi_session_momentum_exhaustion NQ daily status=QUALIFIED net=712.20 dd=788.58 buffer=4500.00 robust=0.772
- cand_604ba5d4a4ae multi_session_momentum_exhaustion NQ 5d status=QUALIFIED net=848.79 dd=569.13 buffer=4314.87 robust=0.772
- cand_3b84548b31b1 multi_session_momentum_exhaustion NQ 5d status=QUALIFIED net=516.38 dd=451.87 buffer=4048.13 robust=0.766
- cand_7e986086f06d multi_session_momentum_exhaustion NQ 5d status=REJECTED_TOO_FEW_TRADES net=595.98 dd=53.35 buffer=4446.65 robust=0.754

## Risk-Compressed Portfolio
- cand_996da8eada60 multi_session_momentum_exhaustion ES daily net=764.79 dd=280.51 buffer=4347.59 robust=0.819
- cand_651a624d10b7 multi_session_momentum_exhaustion ES 2d net=515.76 dd=242.23 buffer=4402.02 robust=0.789
- cand_acb3a34526af multi_session_momentum_exhaustion NQ 5d net=286.56 dd=152.56 buffer=4441.50 robust=0.785
- cand_a3bd265f09b9 multi_session_momentum_exhaustion ES daily net=452.01 dd=156.66 buffer=4468.09 robust=0.748
- cand_1c8ade354c08 multi_session_momentum_exhaustion ES 2d net=472.76 dd=164.33 buffer=4470.20 robust=0.710
- cand_d60066e5b43c volatility_shift_continuation ES 3d net=292.96 dd=201.13 buffer=4434.27 robust=0.681
- cand_5de63c231e39 multi_session_momentum_exhaustion ES daily net=295.21 dd=213.13 buffer=4312.35 robust=0.669
- cand_fbb97e02f9fd multi_session_momentum_exhaustion ES 2d net=289.16 dd=213.85 buffer=4419.28 robust=0.665
- cand_c6c2cd2599b2 multi_session_momentum_exhaustion MES daily net=15.75 dd=22.64 buffer=4486.99 robust=0.642
- cand_4d74fbe429b4 multi_session_momentum_exhaustion MES daily net=22.78 dd=43.54 buffer=4480.13 robust=0.630

## MLL Summary
- Minimum buffer: -2837.51
- Average buffer: 3598.51
- Breached candidates: 32

## Next Recommended Action
- Add Databento historical futures ingestion and strict no-lookahead tests before expanding real-data validation.
