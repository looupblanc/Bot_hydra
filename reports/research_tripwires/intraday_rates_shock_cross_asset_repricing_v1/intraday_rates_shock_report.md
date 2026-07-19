# INTRADAY RATES SHOCK × CROSS-ASSET REPRICING — decision report

- Verdict: `INTRADAY_RATES_SHOCK_CROSS_ASSET_REPRICING_FALSIFIED`
- Logical result hash: `bd272c1041e5567352b1f1628f4e01ab41f4112e2a77bc6daa19a65cc0e2910d`
- Proposals / discovery-frozen rules: 36 / 6
- Exact account cells / normal+stressed episodes: 3456 / 106200
- Evidence ceiling: Tier-E development diagnostic; no promotion, Q4, purchase, broker or order.

## Frozen candidate decisions

| Candidate | Cell frozen on discovery | Validation stressed net | Final N passes | Final S passes | Final S net | Final S MLL | Verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| rates_shock_MNQ_reversal_lb15_h60_v1 | 150K q32 / 10d | 14,400.00 | 0/12 | 0/12 | 2,688.00 | 33.33% | FAIL |
| rates_shock_MGC_continuation_lb5_h60_v1 | 50K q4 / 20d | 1,564.00 | 0/5 | 0/5 | -164.00 | 0.00% | FAIL |
| rates_shock_MNQ_continuation_lb5_h30_v1 | 150K q1 / 5d | -1,062.00 | 0/24 | 0/24 | 397.00 | 0.00% | FAIL |
| rates_shock_MGC_reversal_lb15_h60_v1 | 150K q30 / 5d | -5,430.00 | 0/23 | 0/23 | 13,980.00 | 4.35% | FAIL |
| rates_shock_MGC_catchup_lb15_h15_v1 | 150K q1 / 5d | -78.00 | 0/23 | 0/23 | -481.00 | 0.00% | FAIL |
| rates_shock_MNQ_catchup_lb15_h15_v1 | 150K q1 / 5d | -607.00 | 0/24 | 0/24 | -1,217.00 | 0.00% | FAIL |

## Final-development account-speed envelope

Each row below is the best cell after the frozen MLL ceiling (≤10%). It is diagnostic only and was not used to retune a rule.

| Horizon | Scenario | Candidate | Account / qty | Passes | MLL | Net | Median progress |
|---:|---|---|---:|---:|---:|---:|---:|
| 5d | normal | rates_shock_MGC_reversal_lb15_h60_v1 | 150K / q30 | 0/23 | 4.35% | 14,700.00 | 0.00% |
| 5d | stressed | rates_shock_MGC_reversal_lb15_h60_v1 | 150K / q30 | 0/23 | 4.35% | 13,980.00 | 0.00% |
| 10d | normal | rates_shock_MGC_reversal_lb15_h60_v1 | 150K / q16 | 0/11 | 0.00% | 5,952.00 | 0.00% |
| 10d | stressed | rates_shock_MGC_reversal_lb15_h60_v1 | 150K / q1 | 0/11 | 0.00% | 346.00 | 0.00% |
| 20d | normal | rates_shock_MGC_reversal_lb15_h60_v1 | 150K / q1 | 0/5 | 0.00% | 372.00 | 0.00% |
| 20d | stressed | rates_shock_MGC_reversal_lb15_h60_v1 | 150K / q1 | 0/5 | 0.00% | 346.00 | 0.00% |

No MLL-safe final-development cell produced a normal or stressed P5/P10/P20 pass. Passes existed only in aggressive cells whose MLL breach rates were economically inadmissible; they are not candidates.

The observed relation therefore does not provide a safe post-decision repricing edge. The exact grammar is falsified and must not be rescued by neighbouring shock or leverage thresholds.
