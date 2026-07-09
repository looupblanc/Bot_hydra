# HYDRA Research Report

Generated: 2026-07-09T11:09:38+00:00

## Run Context
- Run mode: synthetic diagnostic
- Requested candidate count: 200
- Symbols: ES, MES, NQ, MNQ
- Seed: 43
- Report tag: smoke_diag

## Warnings
- Synthetic results are pipeline diagnostics only and must not be interpreted as real trading edge.

## Summary
- Total candidates: 200
- Qualified candidates: 13
- Rejected candidates: 187
- V4 selected portfolio count: 10
- MLL buffer min/avg: -584.18 / 3643.34
- MLL breaches: 2

## Status Distribution
- REJECTED_NO_EDGE: 170
- REJECTED_TOO_FEW_TRADES: 17
- PROMOTED_TO_PORTFOLIO: 10
- QUALIFIED: 3

## Top Families
- session_exhaustion_reversal: 46
- volatility_shift_continuation: 43
- volatility_regime_expansion: 39
- multi_session_momentum_exhaustion: 39
- regime_compression_breakout: 33

## Rejection Reasons
- profit_factor_or_net_profit_below_threshold: 170
- below_min_trade_count: 17

## Best Candidates
- cand_7877d304146f multi_session_momentum_exhaustion MES daily status=REJECTED_TOO_FEW_TRADES net=23.34 dd=6.77 buffer=4500.00 robust=0.834
- cand_46fa89aaa266 multi_session_momentum_exhaustion ES daily status=PROMOTED_TO_PORTFOLIO net=452.01 dd=156.66 buffer=4468.09 robust=0.748
- cand_fdc13ce286c4 multi_session_momentum_exhaustion NQ 5d status=REJECTED_TOO_FEW_TRADES net=474.44 dd=235.28 buffer=4401.51 robust=0.731
- cand_128d3e92dbb2 multi_session_momentum_exhaustion NQ 5d status=PROMOTED_TO_PORTFOLIO net=732.06 dd=353.02 buffer=4369.68 robust=0.706
- cand_2c9340218aaf volatility_shift_continuation ES 3d status=PROMOTED_TO_PORTFOLIO net=343.55 dd=308.91 buffer=4405.34 robust=0.685
- cand_e1a544929908 volatility_shift_continuation ES 3d status=PROMOTED_TO_PORTFOLIO net=292.96 dd=201.13 buffer=4434.27 robust=0.681
- cand_158b8a008bd5 session_exhaustion_reversal NQ 3d status=PROMOTED_TO_PORTFOLIO net=523.49 dd=655.65 buffer=4246.17 robust=0.613
- cand_50725ff345fc multi_session_momentum_exhaustion ES intraday status=REJECTED_TOO_FEW_TRADES net=30.67 dd=55.14 buffer=4483.86 robust=0.556
- cand_995512a1056c volatility_regime_expansion NQ intraday status=PROMOTED_TO_PORTFOLIO net=491.77 dd=496.73 buffer=4190.18 robust=0.514
- cand_42044e83784c regime_compression_breakout NQ intraday status=PROMOTED_TO_PORTFOLIO net=166.72 dd=334.06 buffer=4283.87 robust=0.513
- cand_9af9c7056103 multi_session_momentum_exhaustion NQ 5d status=REJECTED_TOO_FEW_TRADES net=35.24 dd=103.79 buffer=4396.21 robust=0.510
- cand_92f10cc58daa multi_session_momentum_exhaustion NQ 3d status=PROMOTED_TO_PORTFOLIO net=97.06 dd=227.16 buffer=4487.50 robust=0.489
- cand_2cdb059fae4d volatility_regime_expansion NQ intraday status=PROMOTED_TO_PORTFOLIO net=260.01 dd=771.23 buffer=3973.29 robust=0.488
- cand_e42a4a49e0e0 volatility_shift_continuation ES 5d status=PROMOTED_TO_PORTFOLIO net=22.77 dd=253.08 buffer=4449.51 robust=0.371
- cand_c3410d964b63 volatility_regime_expansion NQ daily status=QUALIFIED net=215.64 dd=1247.44 buffer=4290.23 robust=0.329

## Risk-Compressed Portfolio
- cand_46fa89aaa266 multi_session_momentum_exhaustion ES daily net=452.01 dd=156.66 buffer=4468.09 robust=0.748
- cand_128d3e92dbb2 multi_session_momentum_exhaustion NQ 5d net=732.06 dd=353.02 buffer=4369.68 robust=0.706
- cand_2c9340218aaf volatility_shift_continuation ES 3d net=343.55 dd=308.91 buffer=4405.34 robust=0.685
- cand_e1a544929908 volatility_shift_continuation ES 3d net=292.96 dd=201.13 buffer=4434.27 robust=0.681
- cand_158b8a008bd5 session_exhaustion_reversal NQ 3d net=523.49 dd=655.65 buffer=4246.17 robust=0.613
- cand_995512a1056c volatility_regime_expansion NQ intraday net=491.77 dd=496.73 buffer=4190.18 robust=0.514
- cand_42044e83784c regime_compression_breakout NQ intraday net=166.72 dd=334.06 buffer=4283.87 robust=0.513
- cand_92f10cc58daa multi_session_momentum_exhaustion NQ 3d net=97.06 dd=227.16 buffer=4487.50 robust=0.489
- cand_2cdb059fae4d volatility_regime_expansion NQ intraday net=260.01 dd=771.23 buffer=3973.29 robust=0.488
- cand_e42a4a49e0e0 volatility_shift_continuation ES 5d net=22.77 dd=253.08 buffer=4449.51 robust=0.371

## MLL Summary
- Minimum buffer: -584.18
- Average buffer: 3643.34
- Breached candidates: 2

## Next Recommended Action
- Add Databento historical futures ingestion and strict no-lookahead tests before expanding real-data validation.
