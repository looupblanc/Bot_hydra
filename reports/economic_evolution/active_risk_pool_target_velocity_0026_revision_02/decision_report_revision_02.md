# HYDRA Active-Risk Pool — Decision Report revision_02

Development-only, post-hoc reporting. This report does not alter the frozen manifest, selection, or promotions.

## Production context

- Source: `TWO_PREREGISTERED_DEEP_GUARDS_REUSED_PLUS_REPORT_RELATIONAL_REDERIVATION`.
- Identity audit: `PASS`.
- Current bottleneck: `active-risk counters diverge from persisted multi-horizon episodes`.
- Next autonomous action: `{'action': 'CONTINUE_FROZEN_ACTIVE_POOL_FINALISTS', 'candidate_ids': ['active_pool_014dffb40e99814612d78c51', 'active_pool_070e391c7586ba1fac2f5494', 'active_pool_142030cd5544260806c06d4d', 'active_pool_14e275fa8d869c28b1f27f78', 'active_pool_186a4177401aab223b0a21fa', 'active_pool_1d7dc1c48b533d65d6098576', 'active_pool_2287bfb0b1c6f07930150102', 'active_pool_2377af7025aadf9aaf456a7e'], 'manifest_required': True, 'new_data_purchase_authorized': False, 'q4_access_authorized': False}`.

| Proposed | Unique screened | Exact replays | Current Stage-3 | Episodes completed |
|---:|---:|---:|---:|---:|
| 20,000 | 4,096 | 1,024 | 256 | 35,328 |

Canonical account attempts and persisted episode rows have distinct units: 35,328 attempts versus 152,064 multi-horizon rows; the frozen per-stage partition formula reconciles exactly.

## Funnel

| Stage-3 policies | Promoted to 96 | Survived 96 | Finalists |
|---:|---:|---:|---:|
| 256 | 32 | 8 | 8 |

## Frozen-horizon economics

| Horizon | Cost | Passes / episodes | Pass rate LB | Pass rate evaluable | Data-censored | Operational horizon | Target P25 (policy median) | Target median (policy median) | MLL rate evaluable | Min-buffer median |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 20_TRADING_DAYS | normal | 989 / 12288 | 8.05% | 12.07% | 4096 | 7203 | 11.36% | 28.57% | 0.00% | $1,732.63 |
| 20_TRADING_DAYS | stressed | 844 / 12288 | 6.87% | 10.30% | 4096 | 7348 | 8.59% | 25.55% | 0.00% | $1,571.38 |
| 40_TRADING_DAYS | normal | 2727 / 12288 | 22.19% | 62.32% | 7912 | 1649 | 12.95% | 34.77% | 0.00% | $1,732.63 |
| 40_TRADING_DAYS | stressed | 2405 / 12288 | 19.57% | 56.34% | 8019 | 1864 | 9.84% | 30.16% | 0.00% | $1,571.38 |
| 60_TRADING_DAYS | normal | 2998 / 12288 | 24.40% | 100.00% | 9290 | 0 | 12.95% | 36.29% | 0.00% | $1,723.41 |
| 60_TRADING_DAYS | stressed | 2703 / 12288 | 22.00% | 100.00% | 9585 | 0 | 9.84% | 30.79% | 0.00% | $1,571.38 |
| 90_TRADING_DAYS | normal | 2998 / 12288 | 24.40% | 100.00% | 9290 | 0 | 12.95% | 36.29% | 0.00% | $1,723.41 |
| 90_TRADING_DAYS | stressed | 2703 / 12288 | 22.00% | 100.00% | 9585 | 0 | 9.84% | 30.79% | 0.00% | $1,571.38 |
| FULL_CHRONOLOGICAL_HORIZON | normal | 10556 / 12288 | 85.90% | 100.00% | 1732 | 0 | 100.73% | 102.83% | 0.00% | $1,695.14 |
| FULL_CHRONOLOGICAL_HORIZON | stressed | 10417 / 12288 | 84.77% | 100.00% | 1871 | 0 | 100.72% | 102.67% | 0.00% | $1,571.38 |

## Horizon duration, censoring, and subscription proxy

| Horizon | Cost | Trading days | Active days | Calendar days | Projected active days to target | Projected calendar days to target | Subscription months proxy | Data-censored | Operational horizon |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 20_TRADING_DAYS | normal | 20.00 | 19.00 | 35.00 | 50.21 | 82.46 | 2.75 | 4096 | 7203 |
| 20_TRADING_DAYS | stressed | 20.00 | 19.00 | 35.00 | 53.57 | 87.77 | 2.93 | 4096 | 7348 |
| 40_TRADING_DAYS | normal | 22.00 | 21.75 | 40.00 | 52.99 | 92.71 | 3.09 | 7912 | 1649 |
| 40_TRADING_DAYS | stressed | 23.50 | 23.00 | 42.50 | 57.35 | 99.54 | 3.32 | 8019 | 1864 |
| 60_TRADING_DAYS | normal | 22.00 | 21.75 | 40.00 | 51.15 | 93.84 | 3.13 | 9290 | 0 |
| 60_TRADING_DAYS | stressed | 23.50 | 23.00 | 42.50 | 55.39 | 101.19 | 3.37 | 9585 | 0 |
| 90_TRADING_DAYS | normal | 22.00 | 21.75 | 40.00 | 51.15 | 93.84 | 3.13 | 9290 | 0 |
| 90_TRADING_DAYS | stressed | 23.50 | 23.00 | 42.50 | 55.39 | 101.19 | 3.37 | 9585 | 0 |
| FULL_CHRONOLOGICAL_HORIZON | normal | 49.00 | 47.50 | 92.50 | 49.32 | 92.95 | 3.10 | 1732 | 0 |
| FULL_CHRONOLOGICAL_HORIZON | stressed | 50.00 | 48.00 | 93.00 | 50.48 | 95.60 | 3.19 | 1871 | 0 |

## Descriptive source blocks — canonical 90-day horizon

Blocks are contract-separated source periods; overlapping rolling episode starts are not independent observations.

| Block | Cost | Passes / episodes | Pass rate LB | Pass rate evaluable | Data-censored | Operational horizon | Target P25 | Target median | MLL rate evaluable | Min-buffer median | Net median |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| B1 | normal | 334 / 3072 | 10.87% | 100.00% | 2738 | 0 | 20.02% | 39.00% | 0.00% | $3,063.28 | $3,509.59 |
| B1 | stressed | 207 / 3072 | 6.74% | 100.00% | 2865 | 0 | 17.81% | 34.50% | 0.00% | $3,008.42 | $3,104.86 |
| B2 | normal | 0 / 3072 | 0.00% | n/a | 3072 | 0 | -2.21% | 13.51% | n/a | $2,048.90 | $1,216.30 |
| B2 | stressed | 0 / 3072 | 0.00% | n/a | 3072 | 0 | -2.47% | 11.19% | n/a | $1,867.81 | $1,006.89 |
| B3 | normal | 1232 / 3072 | 40.10% | 100.00% | 1840 | 0 | 10.94% | 79.27% | 0.00% | $3,348.04 | $7,134.53 |
| B3 | stressed | 1186 / 3072 | 38.61% | 100.00% | 1886 | 0 | 9.15% | 73.84% | 0.00% | $3,344.06 | $6,645.72 |
| B4 | normal | 1432 / 3072 | 46.61% | 100.00% | 1640 | 0 | 25.54% | 87.94% | 0.00% | $2,418.40 | $7,914.56 |
| B4 | stressed | 1310 / 3072 | 42.64% | 100.00% | 1762 | 0 | 22.75% | 76.05% | 0.00% | $2,376.01 | $6,844.54 |

## Risk utilisation and suppression

- NORMAL canonical 90-day decision-event declared nominal-risk utilisation mean: 43.06%; this is neither time-weighted actual stop-risk nor duty cycle.
- Signals emitted / accepted / rejected: 1310692 / 1119133 / 191559.
- Foregone realized PnL, ex-post diagnostic only: $18,098,422.52.

## Stage-3-only XFA lifecycle — paths reported separately

| Cost | Path | Combine attempts | XFA paths | First payouts | First-payout / attempt lower bound | First-payout / evaluable lifecycle | Expected trader payout / attempt lower bound | Post-payout survival evaluable |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| normal | standard | 12288 | 10556 | 10417 | 84.77% | 100.00% | $4,529.39 | 0.00% |
| normal | consistency | 12288 | 10556 | 10319 | 83.98% | 100.00% | $5,751.10 | 1.37% |
| stressed | standard | 12288 | 10417 | 10166 | 82.73% | 100.00% | $4,153.28 | 0.00% |
| stressed | consistency | 12288 | 10417 | 9990 | 81.30% | 100.00% | $4,787.56 | 0.30% |

## Campaign-wide sealed XFA lifecycle totals — Stage 3+4+5

These are final-result totals sealed in the EvidenceBundle campaign summary. First payouts, payout cycles, and trader payout are sums across the alternative Standard and Consistency diagnostic paths; they are not one realizable combined trader path.

| XFA transitions | First-payout observations | Payout-cycle observations | 90%-split cash observations, pre-fees/tax |
|---:|---:|---:|---:|
| 25019 | 48922 | 149254 | $288,381,857.41 |

Exact semantic audit: 25,019 unique Combine-to-XFA transitions fan out to 50,038 mutually alternative Standard/Consistency paths; 48,922 paths observe a first payout and 1,116 do not. First-payout observations are neither duplicate transitions nor simultaneously realizable payouts.

Path-specific Standard/Consistency denominators and censoring are rederived from the sealed episodes and retained in JSON. A pooled probability across both mutually exclusive modes is intentionally not reported.

## Post-hoc behavioral clusters of Stage-3 promotions

| Cluster | Representative | Members | Exact vectors |
|---|---|---:|---|
| active_behavior_03f36b7165b8a0b8ff5c | active_pool_29783e0cfe4ba25c599f8f46 | 1 | true |
| active_behavior_218d393fd31be5b714f4 | active_pool_070e391c7586ba1fac2f5494 | 1 | true |
| active_behavior_226575331c5dc4ec5835 | active_pool_2377af7025aadf9aaf456a7e | 1 | true |
| active_behavior_2b75fccde1962968ab82 | active_pool_0ce4a54af5f6bf6cc31e7fb5 | 1 | true |
| active_behavior_3684a1e577648f470450 | active_pool_147daf139dc0a77415893d07 | 1 | true |
| active_behavior_4165067c39ee63bc2de4 | active_pool_06dfce5eba7a1b46509283b2 | 1 | true |
| active_behavior_542948701c57c2d0ffcf | active_pool_2287bfb0b1c6f07930150102 | 1 | true |
| active_behavior_581cfabb04ba9e866dd3 | active_pool_0f47377396a3c53f23d792ab | 1 | true |
| active_behavior_5874475172663351666d | active_pool_1ed752839c7188ac62b6293b | 1 | true |
| active_behavior_5c83f2d3f4cd4296a09b | active_pool_289111510f24d1a2f068c719 | 1 | true |
| active_behavior_6023fdb014a99b755e58 | active_pool_14153fc01b9c1147a16dd8ab | 1 | true |
| active_behavior_66de4ecc2d844e9885a1 | active_pool_014dffb40e99814612d78c51 | 1 | true |
| active_behavior_6876b210070ca202448f | active_pool_191fa109e88d07edea12ca08 | 1 | true |
| active_behavior_694e2b5877357608774d | active_pool_1c5e3b99cf46573a9f8c1336 | 1 | true |
| active_behavior_69d72d07e5e84d864e8e | active_pool_142030cd5544260806c06d4d | 1 | true |
| active_behavior_7fcca93d4637bf61a231 | active_pool_1fee8db4b9debb41fe02d551 | 1 | true |
| active_behavior_8d348c9b31ce5485f2a5 | active_pool_035d504dd0e0c3799f293eea | 1 | true |
| active_behavior_98cdc18280d483faa91e | active_pool_1d7dc1c48b533d65d6098576 | 1 | true |
| active_behavior_9e7e308e6a85439d9456 | active_pool_051fc9883fe1572a3c5efb9c | 1 | true |
| active_behavior_b28ef18aa7b32f1b356a | active_pool_331c6344977c8b2e3cc053f8 | 1 | true |
| active_behavior_bb26bf368ac319c0027a | active_pool_0786804d0c326bfea6784400 | 1 | true |
| active_behavior_bd42fae43af52c05f554 | active_pool_18630a91f099fa9b05e275c0 | 1 | true |
| active_behavior_c04b5d7743cd56e3cc1a | active_pool_0fb2404e01bbde8acba3a49c | 2 | false |
| active_behavior_c2ef53da5397e55bd8a8 | active_pool_06bb287d128a43d890468422 | 1 | true |
| active_behavior_da1326da73df3e53151e | active_pool_0bab8ac2c328e7d36832023f | 2 | false |
| active_behavior_e182dd60ea24dc7a360f | active_pool_05911b846f03d4311c4c62d6 | 1 | true |
| active_behavior_e264e252bbe90119d771 | active_pool_186a4177401aab223b0a21fa | 1 | true |
| active_behavior_e9f3ec256eeec9bbe1e7 | active_pool_136ccc16736dcfa3d333c182 | 2 | false |
| active_behavior_eac97799a2ad102b5bdc | active_pool_14e275fa8d869c28b1f27f78 | 1 | true |

## Expanded development finalists — cumulative Stage 3+4+5

These are no-retune 192-start development trajectories over only four source blocks. Starts overlap; matched controls cover the Stage-3 48-start slice only.

| Finalist | Starts / cost | N passes | S passes | N pass rate | S pass rate | S target P25 | S target median | S net total | S MLL | Pass blocks | Economic behavior cluster | Exact account/trade cluster |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| active_pool_014dffb40e99814612d78c51 | 192 | 64 | 64 | 33.33% | 33.33% | 9.40% | 33.37% | $876,943.22 | 0.00% | 3 / 4 | expanded_economic_behavior_4d6b945455de5589cc26 | expanded_exact_account_cluster_03 |
| active_pool_070e391c7586ba1fac2f5494 | 192 | 66 | 64 | 34.38% | 33.33% | 19.66% | 45.93% | $969,063.97 | 0.00% | 3 / 4 | expanded_economic_behavior_242a1161a957ead2e87d | expanded_exact_account_cluster_01 |
| active_pool_142030cd5544260806c06d4d | 192 | 64 | 64 | 33.33% | 33.33% | 9.40% | 33.82% | $874,543.47 | 0.00% | 3 / 4 | expanded_economic_behavior_a6a3ebd217746ea17b93 | expanded_exact_account_cluster_06 |
| active_pool_14e275fa8d869c28b1f27f78 | 192 | 66 | 63 | 34.38% | 32.81% | 20.10% | 41.89% | $902,432.01 | 0.00% | 3 / 4 | expanded_economic_behavior_bb420540f4c6c9085dad | expanded_exact_account_cluster_04 |
| active_pool_186a4177401aab223b0a21fa | 192 | 66 | 65 | 34.38% | 33.85% | 21.09% | 46.98% | $984,142.25 | 0.00% | 3 / 4 | expanded_economic_behavior_9191dd8ba90e8bb261bb | expanded_exact_account_cluster_02 |
| active_pool_1d7dc1c48b533d65d6098576 | 192 | 66 | 65 | 34.38% | 33.85% | 19.16% | 46.45% | $963,789.25 | 0.00% | 3 / 4 | expanded_economic_behavior_cb72b6023cd0240750c5 | expanded_exact_account_cluster_07 |
| active_pool_2287bfb0b1c6f07930150102 | 192 | 66 | 65 | 34.38% | 33.85% | 19.92% | 44.55% | $954,288.09 | 0.00% | 3 / 4 | expanded_economic_behavior_8e7f6465a22f073e77c9 | expanded_exact_account_cluster_05 |
| active_pool_2377af7025aadf9aaf456a7e | 192 | 66 | 65 | 34.38% | 33.85% | 19.74% | 45.33% | $964,858.66 | 0.00% | 3 / 4 | expanded_economic_behavior_cb72b6023cd0240750c5 | expanded_exact_account_cluster_08 |

Expanded matched-control deltas are `UNAVAILABLE`: Stage 4/5 did not persist matched controls. The JSON retains each finalist's valid Stage-3-only 48-start deltas to static partition, best sleeve, equal-risk pool, always-on pool, and exposure-matched random priority.

## Expanded finalist XFA alternatives — exact streamed paths

| Finalist | Cost | Alternative | Combine attempts | XFA transitions | First payouts | Closure before first payout | Expected 90%-split cash / new attempt, pre-fees/tax | Post-payout survival evaluable |
|---|---|---|---:|---:|---:|---:|---:|---:|
| active_pool_014dffb40e99814612d78c51 | normal | standard | 192 | 169 | 168 | 0 | $5,280.32 | 0.00% |
| active_pool_014dffb40e99814612d78c51 | normal | consistency | 192 | 169 | 168 | 0 | $6,726.04 | 0.00% |
| active_pool_014dffb40e99814612d78c51 | stressed | standard | 192 | 169 | 157 | 0 | $5,008.35 | 0.00% |
| active_pool_014dffb40e99814612d78c51 | stressed | consistency | 192 | 169 | 156 | 0 | $5,762.96 | 0.00% |
| active_pool_070e391c7586ba1fac2f5494 | normal | standard | 192 | 169 | 168 | 0 | $4,880.34 | 0.00% |
| active_pool_070e391c7586ba1fac2f5494 | normal | consistency | 192 | 169 | 168 | 0 | $6,394.62 | 0.00% |
| active_pool_070e391c7586ba1fac2f5494 | stressed | standard | 192 | 169 | 168 | 0 | $4,701.97 | 0.00% |
| active_pool_070e391c7586ba1fac2f5494 | stressed | consistency | 192 | 169 | 168 | 0 | $6,121.61 | 0.00% |
| active_pool_142030cd5544260806c06d4d | normal | standard | 192 | 169 | 168 | 0 | $5,464.66 | 0.00% |
| active_pool_142030cd5544260806c06d4d | normal | consistency | 192 | 169 | 168 | 0 | $8,046.67 | 6.20% |
| active_pool_142030cd5544260806c06d4d | stressed | standard | 192 | 169 | 168 | 0 | $5,306.24 | 0.00% |
| active_pool_142030cd5544260806c06d4d | stressed | consistency | 192 | 169 | 168 | 0 | $6,124.31 | 0.00% |
| active_pool_14e275fa8d869c28b1f27f78 | normal | standard | 192 | 169 | 168 | 0 | $5,786.74 | 0.00% |
| active_pool_14e275fa8d869c28b1f27f78 | normal | consistency | 192 | 169 | 168 | 0 | $7,518.42 | 8.46% |
| active_pool_14e275fa8d869c28b1f27f78 | stressed | standard | 192 | 169 | 168 | 0 | $5,178.82 | 0.00% |
| active_pool_14e275fa8d869c28b1f27f78 | stressed | consistency | 192 | 169 | 168 | 0 | $6,489.40 | 0.00% |
| active_pool_186a4177401aab223b0a21fa | normal | standard | 192 | 169 | 168 | 0 | $5,584.62 | 0.00% |
| active_pool_186a4177401aab223b0a21fa | normal | consistency | 192 | 169 | 168 | 0 | $8,882.04 | 9.48% |
| active_pool_186a4177401aab223b0a21fa | stressed | standard | 192 | 169 | 168 | 0 | $5,305.99 | 0.00% |
| active_pool_186a4177401aab223b0a21fa | stressed | consistency | 192 | 169 | 168 | 0 | $7,065.75 | 3.68% |
| active_pool_1d7dc1c48b533d65d6098576 | normal | standard | 192 | 169 | 168 | 0 | $5,234.86 | 0.00% |
| active_pool_1d7dc1c48b533d65d6098576 | normal | consistency | 192 | 169 | 168 | 0 | $6,309.84 | 0.00% |
| active_pool_1d7dc1c48b533d65d6098576 | stressed | standard | 192 | 169 | 168 | 0 | $4,913.24 | 0.00% |
| active_pool_1d7dc1c48b533d65d6098576 | stressed | consistency | 192 | 169 | 168 | 0 | $6,214.77 | 0.00% |
| active_pool_2287bfb0b1c6f07930150102 | normal | standard | 192 | 169 | 168 | 0 | $5,281.43 | 0.00% |
| active_pool_2287bfb0b1c6f07930150102 | normal | consistency | 192 | 169 | 168 | 0 | $7,137.39 | 4.11% |
| active_pool_2287bfb0b1c6f07930150102 | stressed | standard | 192 | 169 | 168 | 0 | $4,785.28 | 0.00% |
| active_pool_2287bfb0b1c6f07930150102 | stressed | consistency | 192 | 169 | 168 | 0 | $6,753.51 | 0.00% |
| active_pool_2377af7025aadf9aaf456a7e | normal | standard | 192 | 169 | 168 | 0 | $5,210.76 | 0.00% |
| active_pool_2377af7025aadf9aaf456a7e | normal | consistency | 192 | 169 | 168 | 0 | $6,438.77 | 0.00% |
| active_pool_2377af7025aadf9aaf456a7e | stressed | standard | 192 | 169 | 168 | 0 | $5,596.97 | 0.00% |
| active_pool_2377af7025aadf9aaf456a7e | stressed | consistency | 192 | 169 | 168 | 0 | $6,285.58 | 0.00% |

## Candidate matrix — canonical 90-day horizon

| Candidate | N pass | S pass | N target median | S target median | S target P25 | S net median | S MLL | Min buffer | To 96 | Cluster |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| active_pool_00072974ddd686dae982b496 | 11 | 11 | 37.64% | 33.46% | 13.13% | $3,011.25 | 0.00% | $1,488.14 | false | - |
| active_pool_013e6cbe6670a1368ba806f9 | 11 | 10 | 23.06% | 18.95% | 9.84% | $1,705.79 | 0.00% | $1,571.38 | false | - |
| active_pool_014dffb40e99814612d78c51 | 17 | 17 | 37.08% | 33.17% | 7.73% | $2,985.36 | 0.00% | $924.09 | true | active_behavior_66de4ecc2d844e9885a1 |
| active_pool_015fa08c2c91d4d0b1dbb185 | 11 | 11 | 36.14% | 31.35% | 11.05% | $2,821.28 | 0.00% | $1,731.44 | false | - |
| active_pool_01642afda15d8c9a3f5a00d7 | 12 | 11 | 36.87% | 35.59% | 8.36% | $3,202.84 | 0.00% | $667.98 | false | - |
| active_pool_01a96e458edc84f3e6468c30 | 11 | 11 | 20.74% | 14.66% | 5.11% | $1,319.67 | 0.00% | $1,867.81 | false | - |
| active_pool_01c079a772652c2b7ee70f4b | 11 | 11 | 33.54% | 27.13% | 11.59% | $2,441.95 | 0.00% | $1,314.59 | false | - |
| active_pool_01cc2d53119ee24ed971cf37 | 12 | 10 | 29.49% | 26.12% | 8.88% | $2,350.47 | 0.00% | $1,545.50 | false | - |
| active_pool_023c5b36cde06db7876b6aba | 14 | 12 | 37.49% | 32.04% | 11.33% | $2,883.45 | 0.00% | $1,636.46 | false | - |
| active_pool_0263b3f888c3404e24efff13 | 11 | 10 | 24.47% | 19.82% | 9.84% | $1,783.92 | 0.00% | $1,571.38 | false | - |
| active_pool_02a28a9d089abd41a46cb332 | 13 | 11 | 37.66% | 30.76% | 9.14% | $2,768.55 | 0.00% | $1,269.99 | false | - |
| active_pool_02e96d059ddbd18f6af1db9d | 12 | 11 | 33.77% | 29.50% | 5.81% | $2,655.31 | 0.00% | $1,226.18 | false | - |
| active_pool_0313d73fd98cc16b48e5f9df | 11 | 11 | 19.20% | 13.71% | 4.95% | $1,234.00 | 0.00% | $1,867.81 | false | - |
| active_pool_035d504dd0e0c3799f293eea | 15 | 13 | 46.27% | 39.16% | 15.47% | $3,524.51 | 0.00% | $1,042.92 | true | active_behavior_8d348c9b31ce5485f2a5 |
| active_pool_0366aa646dd882e8533fc0a2 | 11 | 11 | 22.31% | 16.33% | 7.51% | $1,470.12 | 0.00% | $1,867.81 | false | - |
| active_pool_037c51d2a7c4d9f2f2ec92e6 | 11 | 11 | 29.05% | 24.11% | 8.45% | $2,169.81 | 0.00% | $1,378.59 | false | - |
| active_pool_03b14ecfd26408fd7b16f8aa | 11 | 11 | 20.19% | 14.43% | 5.72% | $1,298.88 | 0.00% | $2,052.83 | false | - |
| active_pool_03cc0003d38ade66f2e42aae | 11 | 9 | 20.48% | 16.82% | 7.01% | $1,513.35 | 0.00% | $2,231.72 | false | - |
| active_pool_03f30a6f0b9039be4f5b1b67 | 11 | 10 | 28.42% | 23.26% | 6.97% | $2,093.05 | 0.00% | $1,801.96 | false | - |
| active_pool_042580e100c986bb2d5f9475 | 11 | 11 | 34.35% | 27.17% | 12.41% | $2,444.85 | 0.00% | $1,590.58 | false | - |
| active_pool_045a50c3731f41774365e3dc | 17 | 16 | 49.92% | 43.57% | 16.16% | $3,921.63 | 0.00% | $487.86 | false | - |
| active_pool_050657284c8092dda4f295f3 | 11 | 7 | 21.95% | 18.40% | 7.01% | $1,656.27 | 0.00% | $2,391.14 | false | - |
| active_pool_051fc9883fe1572a3c5efb9c | 16 | 15 | 48.57% | 43.32% | 15.54% | $3,898.48 | 0.00% | $1,078.06 | true | active_behavior_9e7e308e6a85439d9456 |
| active_pool_0522f07bfd8a1e0ced50391c | 11 | 11 | 24.32% | 19.43% | 9.84% | $1,748.83 | 0.00% | $1,501.20 | false | - |
| active_pool_054257cfa9530fbf459a89bb | 9 | 9 | 26.31% | 23.14% | 5.90% | $2,082.27 | 0.00% | $1,576.35 | false | - |
| active_pool_054be8aa7e3f10a3ed0ca2ad | 15 | 15 | 41.56% | 36.97% | 11.33% | $3,327.22 | 0.00% | $1,075.95 | false | - |
| active_pool_05519f651be8fbb0d74b2196 | 14 | 13 | 44.63% | 36.00% | 15.55% | $3,239.64 | 0.00% | $1,573.48 | false | - |
| active_pool_05911b846f03d4311c4c62d6 | 16 | 14 | 33.70% | 29.52% | 5.54% | $2,657.07 | 0.00% | $844.35 | true | active_behavior_e182dd60ea24dc7a360f |
| active_pool_060ee99408dc32c338f012b8 | 11 | 8 | 24.31% | 22.87% | 8.51% | $2,058.43 | 0.00% | $2,095.72 | false | - |
| active_pool_0679203ca20e3716da03e4f9 | 11 | 10 | 38.31% | 34.01% | 14.07% | $3,061.31 | 0.00% | $1,031.23 | false | - |
| active_pool_06bb287d128a43d890468422 | 15 | 13 | 40.02% | 34.21% | 11.33% | $3,078.47 | 0.00% | $1,384.92 | true | active_behavior_c2ef53da5397e55bd8a8 |
| active_pool_06d8497dd3f05434e80bab71 | 1 | 0 | 21.17% | 17.61% | 7.00% | $1,584.52 | 0.00% | $2,227.69 | false | - |
| active_pool_06dfce5eba7a1b46509283b2 | 16 | 13 | 33.70% | 29.52% | 5.54% | $2,657.07 | 0.00% | $1,110.26 | true | active_behavior_4165067c39ee63bc2de4 |
| active_pool_070e391c7586ba1fac2f5494 | 17 | 17 | 49.20% | 43.62% | 16.56% | $3,925.36 | 0.00% | $1,009.67 | true | active_behavior_218d393fd31be5b714f4 |
| active_pool_076527bf632a6e1c6bbed25f | 11 | 11 | 23.94% | 17.50% | 8.38% | $1,575.11 | 0.00% | $1,867.81 | false | - |
| active_pool_0777dce00b8dd1e92e9d25bb | 11 | 11 | 36.54% | 31.70% | 11.38% | $2,853.19 | 0.00% | $1,866.44 | false | - |
| active_pool_0786804d0c326bfea6784400 | 15 | 13 | 43.29% | 39.95% | 17.23% | $3,595.16 | 0.00% | $1,629.94 | true | active_behavior_bb26bf368ac319c0027a |
| active_pool_0816d8e3f9f2c70f901693f9 | 11 | 11 | 32.79% | 31.26% | 6.73% | $2,813.45 | 0.00% | $1,732.35 | false | - |
| active_pool_0816e90ddaa93813e39b1099 | 4 | 2 | 23.24% | 19.13% | 9.30% | $1,722.05 | 0.00% | $2,163.69 | false | - |
| active_pool_082ec1dc1945c73e2af45bf2 | 1 | 0 | 24.71% | 17.19% | 6.66% | $1,546.67 | 0.00% | $2,148.38 | false | - |
| active_pool_084ce51526f588dd9abc20fc | 14 | 13 | 46.17% | 40.83% | 15.74% | $3,674.96 | 0.00% | $1,526.38 | false | - |
| active_pool_08537161dffef3e23b3ef6ed | 11 | 11 | 33.41% | 29.88% | 10.19% | $2,688.80 | 0.00% | $1,766.87 | false | - |
| active_pool_08560aba70fdb14d67356bf6 | 15 | 12 | 38.49% | 32.74% | 11.73% | $2,946.43 | 0.00% | $839.60 | false | - |
| active_pool_086c7087b2f73991f68336f6 | 15 | 13 | 40.41% | 33.88% | 11.33% | $3,049.44 | 0.00% | $1,698.82 | false | - |
| active_pool_086f93bafb45cbc371012897 | 11 | 11 | 39.31% | 35.08% | 16.03% | $3,156.78 | 0.00% | $1,031.23 | false | - |
| active_pool_08be8cfd82c5e1ee3736ff29 | 17 | 17 | 46.32% | 39.12% | 13.58% | $3,520.89 | 0.00% | $1,255.87 | false | - |
| active_pool_08f75194597155d591e3e9c5 | 11 | 10 | 39.31% | 35.08% | 16.03% | $3,156.78 | 0.00% | $1,031.23 | false | - |
| active_pool_0901350c4569cdf38f88d890 | 1 | 0 | 19.77% | 16.52% | 5.38% | $1,486.47 | 0.00% | $2,239.14 | false | - |
| active_pool_097f5ce1e24d092f2ba761e5 | 11 | 10 | 22.99% | 19.58% | 7.43% | $1,761.90 | 0.00% | $2,391.14 | false | - |
| active_pool_09801e41fb46596d198e17e8 | 17 | 14 | 45.51% | 34.64% | 6.31% | $3,117.39 | 0.00% | $1,183.80 | false | - |
| active_pool_09debf3f92eb8e71d90208e5 | 15 | 14 | 40.41% | 35.01% | 11.33% | $3,150.82 | 0.00% | $1,698.82 | true | active_behavior_da1326da73df3e53151e |
| active_pool_0a167207b5078c8cef8229a8 | 11 | 11 | 36.54% | 30.98% | 12.11% | $2,788.39 | 0.00% | $1,248.13 | false | - |
| active_pool_0ac81ade5f8176e1a88a948d | 11 | 11 | 30.54% | 26.98% | 6.72% | $2,428.25 | 0.00% | $1,829.67 | false | - |
| active_pool_0ae7985a2840d44170a2fb2a | 17 | 16 | 49.33% | 42.45% | 16.16% | $3,820.32 | 0.00% | $487.86 | false | - |
| active_pool_0b27aaa10779c5aaf5f140f8 | 3 | 0 | 24.09% | 19.63% | 9.84% | $1,766.92 | 0.00% | $1,571.38 | false | - |
| active_pool_0b3f6d6e0668b3126c59dd8f | 11 | 11 | 38.57% | 35.06% | 13.98% | $3,155.26 | 0.00% | $1,031.23 | false | - |
| active_pool_0b614476f9a6de657c7ab0c6 | 13 | 13 | 36.61% | 30.49% | 11.33% | $2,743.66 | 0.00% | $1,526.38 | false | - |
| active_pool_0b86588be430749e0f17facf | 11 | 10 | 24.47% | 19.82% | 9.84% | $1,783.92 | 0.00% | $1,571.38 | false | - |
| active_pool_0b9920f2c2e4403c33d3fa41 | 11 | 11 | 37.83% | 33.11% | 12.54% | $2,980.22 | 0.00% | $1,826.80 | false | - |
| active_pool_0bab8ac2c328e7d36832023f | 15 | 14 | 40.96% | 35.93% | 11.33% | $3,233.55 | 0.00% | $1,368.41 | true | active_behavior_da1326da73df3e53151e |
| active_pool_0bcf836f89fdd20ce5091d07 | 4 | 3 | 21.27% | 17.56% | 7.42% | $1,580.47 | 0.00% | $2,175.14 | false | - |
| active_pool_0bf5a45a8f56ce261cda69df | 11 | 11 | 31.06% | 25.50% | 9.83% | $2,295.14 | 0.00% | $1,355.69 | false | - |
| active_pool_0c4942cd05c8edfdae5e0e28 | 17 | 14 | 42.26% | 38.33% | 8.82% | $3,450.03 | 0.00% | $934.44 | false | - |
| active_pool_0ca177640e89626cbdc04ade | 9 | 8 | 22.73% | 16.90% | 7.51% | $1,521.33 | 0.00% | $1,847.98 | false | - |
| active_pool_0ce4a54af5f6bf6cc31e7fb5 | 15 | 13 | 46.97% | 40.49% | 15.47% | $3,643.99 | 0.00% | $1,075.95 | true | active_behavior_2b75fccde1962968ab82 |
| active_pool_0cfb7526d3a3571067169ba7 | 11 | 11 | 27.25% | 22.78% | 7.90% | $2,049.83 | 0.00% | $1,818.06 | false | - |
| active_pool_0d0c41bbdd2382109f103bfd | 11 | 11 | 36.35% | 30.81% | 12.24% | $2,772.90 | 0.00% | $1,672.22 | false | - |
| active_pool_0e000a312b40ac0a13af9ae5 | 13 | 11 | 44.11% | 35.81% | 15.55% | $3,222.64 | 0.00% | $1,573.48 | false | - |
| active_pool_0e0bd588ade2433410bfcc7f | 13 | 13 | 39.81% | 34.86% | 11.33% | $3,137.49 | 0.00% | $1,135.69 | false | - |
| active_pool_0e888c7a3a9814ee0f2f7de3 | 13 | 11 | 39.81% | 34.86% | 11.97% | $3,137.49 | 0.00% | $702.17 | false | - |
| active_pool_0ea8b0e7f5937ac9f1451626 | 14 | 13 | 37.49% | 32.04% | 11.33% | $2,883.45 | 0.00% | $1,636.46 | false | - |
| active_pool_0ebc7d67e8ae990b465eac27 | 17 | 16 | 36.48% | 32.39% | 7.73% | $2,915.35 | 0.00% | $1,261.54 | false | - |
| active_pool_0eea1d02829716420219467d | 16 | 14 | 45.71% | 39.52% | 14.67% | $3,556.42 | 0.00% | $1,302.05 | false | - |
| active_pool_0f47377396a3c53f23d792ab | 16 | 15 | 41.68% | 36.69% | 11.33% | $3,302.26 | 0.00% | $1,078.06 | true | active_behavior_581cfabb04ba9e866dd3 |
| active_pool_0fb2404e01bbde8acba3a49c | 15 | 13 | 47.06% | 41.26% | 15.74% | $3,713.40 | 0.00% | $1,394.77 | true | active_behavior_c04b5d7743cd56e3cc1a |
| active_pool_10ba889f2b4ee75aacec7464 | 11 | 11 | 22.52% | 18.56% | 9.46% | $1,670.70 | 0.00% | $1,501.20 | false | - |
| active_pool_10d3f004724c1f1d629daf06 | 11 | 11 | 33.07% | 28.61% | 10.99% | $2,574.54 | 0.00% | $1,447.10 | false | - |
| active_pool_1124dc733683cb5fdbffebd9 | 13 | 12 | 46.27% | 39.99% | 17.23% | $3,599.02 | 0.00% | $702.17 | false | - |
| active_pool_1161ed2dceed469904b4fb1f | 3 | 1 | 22.71% | 18.29% | 7.42% | $1,646.47 | 0.00% | $2,175.14 | false | - |
| active_pool_11a27739fc368016efdfa80c | 9 | 6 | 21.00% | 16.49% | 5.18% | $1,484.39 | 0.00% | $2,391.14 | false | - |
| active_pool_11b95ae59b5b9b7415f8cc89 | 11 | 11 | 23.06% | 18.95% | 9.84% | $1,705.79 | 0.00% | $1,571.38 | false | - |
| active_pool_11f916447e9bf80b599fd2d4 | 10 | 10 | 21.37% | 15.59% | 6.50% | $1,403.18 | 0.00% | $2,052.83 | false | - |
| active_pool_1208af085ede68b7d9b0a71b | 6 | 4 | 24.22% | 20.11% | 9.79% | $1,809.86 | 0.00% | $2,227.69 | false | - |
| active_pool_123a897b0519a8b9c27f1d77 | 9 | 5 | 20.32% | 15.82% | 5.18% | $1,423.37 | 0.00% | $2,178.60 | false | - |
| active_pool_12529d355054414f73174e7d | 10 | 8 | 23.06% | 18.95% | 9.84% | $1,705.79 | 0.00% | $1,571.38 | false | - |
| active_pool_1252e065047124f1546e9f8b | 13 | 11 | 32.68% | 27.66% | 9.47% | $2,489.59 | 0.00% | $1,810.06 | false | - |
| active_pool_129790b3959543d124b0b6d0 | 11 | 10 | 39.31% | 35.08% | 16.03% | $3,156.78 | 0.00% | $1,031.23 | false | - |
| active_pool_12b691c9fa0e1ac78bab42c1 | 11 | 9 | 21.77% | 17.67% | 8.82% | $1,589.87 | 0.00% | $1,490.95 | false | - |
| active_pool_13030287157839a12771f93c | 11 | 10 | 22.80% | 18.37% | 9.22% | $1,653.70 | 0.00% | $1,501.20 | false | - |
| active_pool_132a600ca6a59278a40b8ee1 | 11 | 11 | 33.94% | 27.93% | 12.17% | $2,513.95 | 0.00% | $1,378.59 | false | - |
| active_pool_132cd1758c0cfbb05914d5b9 | 11 | 11 | 31.07% | 29.55% | 6.51% | $2,659.14 | 0.00% | $1,914.65 | false | - |
| active_pool_133076c1587e9e69a57c6119 | 11 | 11 | 23.11% | 17.14% | 7.54% | $1,542.26 | 0.00% | $2,052.83 | false | - |
| active_pool_13486050c9ad6ddabffaaaa8 | 12 | 11 | 37.50% | 32.81% | 7.60% | $2,952.84 | 0.00% | $1,169.10 | false | - |
| active_pool_134f34862bebab6f05e5c8c5 | 15 | 15 | 40.96% | 35.93% | 11.33% | $3,233.55 | 0.00% | $1,368.41 | false | - |
| active_pool_136ba3c6c6d3848a9a4348c6 | 1 | 0 | 20.59% | 16.72% | 7.00% | $1,504.71 | 0.00% | $2,227.69 | false | - |
| active_pool_136ccc16736dcfa3d333c182 | 15 | 14 | 47.41% | 40.36% | 17.23% | $3,632.75 | 0.00% | $1,299.29 | true | active_behavior_e9f3ec256eeec9bbe1e7 |
| active_pool_13c3eaedf62f948be19d8f7f | 13 | 11 | 43.53% | 35.17% | 15.55% | $3,164.98 | 0.00% | $1,573.48 | false | - |
| active_pool_13d05087ed5ae34d2a363317 | 11 | 11 | 22.73% | 16.90% | 7.51% | $1,521.33 | 0.00% | $2,052.83 | false | - |
| active_pool_14153fc01b9c1147a16dd8ab | 15 | 15 | 47.32% | 40.37% | 15.64% | $3,633.57 | 0.00% | $1,368.41 | true | active_behavior_6023fdb014a99b755e58 |
| active_pool_141c1926b75ff9d5f5a54a17 | 13 | 12 | 45.42% | 39.11% | 15.74% | $3,520.15 | 0.00% | $1,526.38 | false | - |
| active_pool_142030cd5544260806c06d4d | 17 | 17 | 39.37% | 36.07% | 7.73% | $3,246.70 | 0.00% | $1,261.54 | true | active_behavior_69d72d07e5e84d864e8e |
| active_pool_147bf6c6327feea694eeeb35 | 11 | 9 | 38.86% | 34.66% | 12.91% | $3,119.24 | 0.00% | $1,274.00 | false | - |
| active_pool_147daf139dc0a77415893d07 | 17 | 14 | 48.28% | 41.25% | 16.81% | $3,712.60 | 0.00% | $808.87 | true | active_behavior_3684a1e577648f470450 |
| active_pool_14e275fa8d869c28b1f27f78 | 17 | 17 | 50.26% | 46.15% | 18.78% | $4,153.53 | 0.00% | $1,033.49 | true | active_behavior_eac97799a2ad102b5bdc |
| active_pool_1522bf120c6684a4dcd10439 | 14 | 13 | 43.66% | 40.14% | 17.51% | $3,612.16 | 0.00% | $1,629.94 | false | - |
| active_pool_15e8b18096b7289b377250c1 | 11 | 10 | 36.84% | 32.07% | 11.66% | $2,886.24 | 0.00% | $1,652.51 | false | - |
| active_pool_1613df81b7c62c074821e81a | 11 | 9 | 20.74% | 14.66% | 5.11% | $1,319.67 | 0.00% | $1,867.81 | false | - |
| active_pool_161a513cdc8f0bc3c100d87e | 11 | 11 | 36.24% | 31.51% | 11.66% | $2,835.58 | 0.00% | $1,652.51 | false | - |
| active_pool_162cc177a626bba5696e3b2b | 11 | 10 | 22.34% | 18.37% | 9.18% | $1,653.70 | 0.00% | $1,501.20 | false | - |
| active_pool_163b105ca54856fd2473cfc4 | 15 | 13 | 47.41% | 42.44% | 17.23% | $3,819.16 | 0.00% | $1,629.94 | true | active_behavior_e9f3ec256eeec9bbe1e7 |
| active_pool_1654084117a8be59f4729b67 | 11 | 11 | 24.09% | 19.63% | 9.84% | $1,766.92 | 0.00% | $1,571.38 | false | - |
| active_pool_16568576695d18490d5520a3 | 13 | 13 | 38.79% | 35.44% | 11.33% | $3,189.42 | 0.00% | $1,629.94 | false | - |
| active_pool_167d55726b61ab1dc24ceadd | 11 | 11 | 30.22% | 25.20% | 9.83% | $2,268.36 | 0.00% | $1,291.69 | false | - |
| active_pool_168643d63670ca2eaeda1f14 | 3 | 1 | 21.17% | 17.61% | 7.00% | $1,584.52 | 0.00% | $2,227.69 | false | - |
| active_pool_168dd0580ee3604f93184819 | 17 | 12 | 49.54% | 38.49% | 8.96% | $3,463.83 | 0.00% | $1,106.71 | false | - |
| active_pool_16b3477081374e874ee01015 | 15 | 14 | 40.96% | 35.93% | 11.33% | $3,233.55 | 0.00% | $1,368.41 | false | - |
| active_pool_16c997eed44fb533b51d3737 | 11 | 11 | 25.60% | 20.18% | 5.11% | $1,815.86 | 0.00% | $1,818.06 | false | - |
| active_pool_16e2c53173139d6b38bbc171 | 1 | 0 | 19.46% | 16.21% | 5.38% | $1,458.97 | 0.00% | $2,175.14 | false | - |
| active_pool_17222b6dcb91a1574e96302c | 15 | 13 | 36.42% | 32.50% | 11.26% | $2,924.99 | 0.00% | $1,799.18 | false | - |
| active_pool_17315c48a1e045f08054438b | 11 | 11 | 21.73% | 18.48% | 7.43% | $1,663.63 | 0.00% | $2,484.83 | false | - |
| active_pool_173aa33983e4eb8f21e8b66a | 5 | 1 | 24.28% | 18.80% | 9.26% | $1,692.44 | 0.00% | $2,278.91 | false | - |
| active_pool_174e5a3f2cc18b80c65b35cf | 11 | 11 | 32.24% | 27.09% | 9.47% | $2,438.11 | 0.00% | $1,795.39 | false | - |
| active_pool_176342a9015cd50a42950a69 | 11 | 11 | 23.47% | 22.25% | 3.81% | $2,002.68 | 0.00% | $1,868.48 | false | - |
| active_pool_17784b0a59d27fdecaa0ba1c | 13 | 11 | 44.95% | 38.86% | 15.87% | $3,497.17 | 0.00% | $1,636.46 | false | - |
| active_pool_17ae3d4fa474561907308c5f | 17 | 14 | 46.32% | 35.88% | 8.62% | $3,229.34 | 0.00% | $520.60 | false | - |
| active_pool_17d9e3c7787df03ee38e7bd9 | 3 | 1 | 23.24% | 19.13% | 9.30% | $1,722.05 | 0.00% | $2,163.69 | false | - |
| active_pool_182d94821b1f6dc21b680a94 | 9 | 7 | 27.69% | 22.13% | 6.23% | $1,991.55 | 0.00% | $1,606.73 | false | - |
| active_pool_18630a91f099fa9b05e275c0 | 15 | 15 | 47.12% | 40.23% | 15.95% | $3,620.72 | 0.00% | $1,698.82 | true | active_behavior_bd42fae43af52c05f554 |
| active_pool_1869bbb0130714f0df6b371b | 17 | 12 | 39.48% | 31.23% | 3.80% | $2,810.52 | 0.00% | $1,183.80 | false | - |
| active_pool_186a4177401aab223b0a21fa | 17 | 17 | 51.67% | 46.35% | 18.48% | $4,171.49 | 0.00% | $873.88 | true | active_behavior_e264e252bbe90119d771 |
| active_pool_191fa109e88d07edea12ca08 | 15 | 13 | 46.27% | 39.16% | 15.47% | $3,524.51 | 0.00% | $1,042.92 | true | active_behavior_6876b210070ca202448f |
| active_pool_1945b46cc23553c00bef3f65 | 10 | 9 | 24.31% | 19.75% | 9.84% | $1,777.79 | 0.00% | $1,699.38 | false | - |
| active_pool_195c5497a9f273b1d7505918 | 11 | 11 | 30.34% | 25.84% | 6.49% | $2,325.45 | 0.00% | $1,864.78 | false | - |
| active_pool_19b7e7eb5693caea76001327 | 10 | 9 | 22.99% | 17.61% | 7.96% | $1,585.31 | 0.00% | $1,867.81 | false | - |
| active_pool_19c18c5b450baf41409a10d5 | 15 | 12 | 30.40% | 27.05% | 4.04% | $2,434.08 | 0.00% | $1,107.69 | false | - |
| active_pool_1a652224333795e72da819f5 | 3 | 0 | 25.69% | 20.43% | 9.84% | $1,838.92 | 0.00% | $1,415.69 | false | - |
| active_pool_1a7420478f52cc2a817172ba | 15 | 13 | 40.95% | 36.23% | 11.97% | $3,261.10 | 0.00% | $1,629.94 | false | - |
| active_pool_1a797f87ca50b2fd56688a6c | 3 | 1 | 21.17% | 17.61% | 7.00% | $1,584.52 | 0.00% | $2,227.69 | false | - |
| active_pool_1ae51e627900b9d987682f5b | 13 | 11 | 38.79% | 35.44% | 11.33% | $3,189.42 | 0.00% | $1,629.94 | false | - |
| active_pool_1afd25a3e671244a2f3b0873 | 11 | 11 | 31.25% | 26.24% | 9.83% | $2,361.95 | 0.00% | $1,503.13 | false | - |
| active_pool_1b525c277ff8d18ea2d3a1cd | 13 | 11 | 37.39% | 30.53% | 11.06% | $2,747.61 | 0.00% | $863.61 | false | - |
| active_pool_1b81185b998dc18a5a8c3830 | 11 | 11 | 23.03% | 19.03% | 7.43% | $1,712.76 | 0.00% | $2,391.14 | false | - |
| active_pool_1bb1fdbbf6ee1595c10545c3 | 11 | 9 | 27.96% | 22.24% | 8.79% | $2,002.01 | 0.00% | $1,927.90 | false | - |
| active_pool_1bb842a9fe59895ea9ed25fc | 14 | 13 | 44.34% | 40.42% | 17.51% | $3,637.84 | 0.00% | $1,629.94 | false | - |
| active_pool_1bc851df8220c1fd4b70306f | 11 | 11 | 26.56% | 22.33% | 6.85% | $2,009.51 | 0.00% | $2,037.70 | false | - |
| active_pool_1bdf56a099ab41775f5a4728 | 11 | 10 | 20.15% | 16.31% | 6.62% | $1,467.49 | 0.00% | $1,571.38 | false | - |
| active_pool_1c38685482b1bd5c54d810ac | 14 | 13 | 45.61% | 39.73% | 15.59% | $3,575.26 | 0.00% | $1,351.89 | false | - |
| active_pool_1c38855fefd1ec0270a57874 | 13 | 11 | 43.72% | 35.36% | 15.55% | $3,181.98 | 0.00% | $1,573.48 | false | - |
| active_pool_1c5e3b99cf46573a9f8c1336 | 16 | 13 | 46.27% | 39.99% | 17.23% | $3,599.02 | 0.00% | $702.17 | true | active_behavior_694e2b5877357608774d |
| active_pool_1c802f06c6c9bac2c4c894ca | 14 | 11 | 38.63% | 33.39% | 11.27% | $3,005.40 | 0.00% | $1,478.15 | false | - |
| active_pool_1c8e3d6b15aa1adfd1ea6798 | 13 | 13 | 38.11% | 34.53% | 11.33% | $3,107.34 | 0.00% | $1,629.94 | false | - |
| active_pool_1cdc1d8f8ec9871cc9194e3e | 11 | 11 | 20.74% | 14.66% | 5.11% | $1,319.67 | 0.00% | $1,867.81 | false | - |
| active_pool_1d4dfa129ba22f08078c8167 | 11 | 11 | 37.83% | 34.16% | 12.98% | $3,074.06 | 0.00% | $1,652.51 | false | - |
| active_pool_1d7dc1c48b533d65d6098576 | 17 | 17 | 49.59% | 43.79% | 16.19% | $3,940.85 | 0.00% | $1,009.67 | true | active_behavior_98cdc18280d483faa91e |
| active_pool_1d9713b9c0cbd0f6b6b54345 | 11 | 11 | 37.56% | 32.45% | 13.62% | $2,920.11 | 0.00% | $1,731.44 | false | - |
| active_pool_1e71bef259f2e9f6d9b34adb | 11 | 11 | 35.26% | 28.80% | 12.32% | $2,592.07 | 0.00% | $1,378.59 | false | - |
| active_pool_1e9439dc59ca581303014467 | 11 | 11 | 21.57% | 17.90% | 5.60% | $1,611.12 | 0.00% | $2,391.14 | false | - |
| active_pool_1ea5204f7c5383935740fa1a | 11 | 11 | 33.39% | 26.94% | 11.54% | $2,424.95 | 0.00% | $1,314.59 | false | - |
| active_pool_1ed752839c7188ac62b6293b | 15 | 13 | 47.60% | 40.74% | 17.51% | $3,666.75 | 0.00% | $669.43 | true | active_behavior_5874475172663351666d |
| active_pool_1f6819e37a82565ee98da71a | 11 | 11 | 21.41% | 15.68% | 6.41% | $1,411.33 | 0.00% | $2,052.83 | false | - |
| active_pool_1f8b581dfab34237670b2abc | 11 | 10 | 29.47% | 27.09% | 4.56% | $2,437.81 | 0.00% | $1,943.16 | false | - |
| active_pool_1f9f6bc2a1e2a3eddf5225a2 | 13 | 11 | 40.63% | 34.14% | 11.33% | $3,072.32 | 0.00% | $1,045.03 | false | - |
| active_pool_1fc463da8739fc25a8f69bd4 | 11 | 10 | 29.17% | 22.74% | 7.62% | $2,047.03 | 0.00% | $1,488.55 | false | - |
| active_pool_1fea47abac7e7f566ae127a9 | 11 | 11 | 36.97% | 32.27% | 12.54% | $2,904.24 | 0.00% | $1,826.80 | false | - |
| active_pool_1fee8db4b9debb41fe02d551 | 16 | 14 | 47.68% | 41.56% | 17.31% | $3,740.50 | 0.00% | $837.82 | true | active_behavior_7fcca93d4637bf61a231 |
| active_pool_209e85ec5055798c5f0a7e1a | 11 | 10 | 38.08% | 33.61% | 12.91% | $3,025.22 | 0.00% | $1,716.96 | false | - |
| active_pool_20ba4c6525479aee8f9a92c1 | 17 | 13 | 46.32% | 35.93% | 8.62% | $3,233.67 | 0.00% | $520.60 | false | - |
| active_pool_2126b9c446cf3d292535e28a | 13 | 11 | 39.82% | 33.86% | 11.33% | $3,047.25 | 0.00% | $1,636.46 | false | - |
| active_pool_212fe14c882b420fdc6b5126 | 12 | 11 | 32.92% | 28.26% | 4.18% | $2,543.29 | 0.00% | $1,269.99 | false | - |
| active_pool_214f5b2f2f15c62f8dcb66b3 | 11 | 10 | 18.87% | 14.08% | 5.18% | $1,267.12 | 0.00% | $2,231.72 | false | - |
| active_pool_2188f4de129ba4851b709bc0 | 11 | 11 | 36.54% | 30.98% | 12.11% | $2,788.39 | 0.00% | $1,248.13 | false | - |
| active_pool_21c6cd3fded965469711f013 | 11 | 9 | 31.77% | 28.48% | 6.49% | $2,563.48 | 0.00% | $1,946.97 | false | - |
| active_pool_22206caf28f4201eac17f776 | 13 | 11 | 43.72% | 35.36% | 15.55% | $3,181.98 | 0.00% | $1,573.48 | false | - |
| active_pool_2258c8b4fcf453760d16ab6b | 17 | 16 | 46.49% | 41.75% | 11.15% | $3,757.51 | 0.00% | $1,033.49 | false | - |
| active_pool_22841aba7e5a27512232d25e | 11 | 10 | 24.09% | 19.63% | 9.84% | $1,766.92 | 0.00% | $1,490.95 | false | - |
| active_pool_2287bfb0b1c6f07930150102 | 17 | 17 | 50.28% | 44.52% | 17.09% | $4,006.66 | 0.00% | $621.08 | true | active_behavior_542948701c57c2d0ffcf |
| active_pool_2377af7025aadf9aaf456a7e | 17 | 17 | 52.68% | 44.15% | 16.72% | $3,973.63 | 0.00% | $487.86 | true | active_behavior_226575331c5dc4ec5835 |
| active_pool_23b85253b260fac655f1c127 | 11 | 10 | 23.11% | 17.28% | 7.54% | $1,555.33 | 0.00% | $1,847.98 | false | - |
| active_pool_23e308b42ed83fc519ff3960 | 11 | 10 | 37.86% | 32.38% | 12.49% | $2,913.80 | 0.00% | $1,274.00 | false | - |
| active_pool_2421d0c2fde251da8ed177e1 | 11 | 9 | 20.69% | 16.17% | 5.60% | $1,454.87 | 0.00% | $2,231.72 | false | - |
| active_pool_2492c04e680ed712011bda51 | 13 | 11 | 43.90% | 35.58% | 15.55% | $3,202.49 | 0.00% | $1,614.49 | false | - |
| active_pool_24ea5fb3b714a10d4926b891 | 11 | 5 | 20.15% | 16.31% | 6.62% | $1,467.49 | 0.00% | $1,571.38 | false | - |
| active_pool_252d534502a30d9257f2b604 | 11 | 11 | 38.57% | 35.06% | 14.87% | $3,155.26 | 0.00% | $1,031.23 | false | - |
| active_pool_258de9607c0f7055ff99105f | 11 | 11 | 28.79% | 24.11% | 6.56% | $2,169.81 | 0.00% | $1,378.59 | false | - |
| active_pool_25c22e423032ee1572fb5221 | 11 | 11 | 37.47% | 32.77% | 12.91% | $2,949.24 | 0.00% | $1,826.80 | false | - |
| active_pool_26150d081770119600fa6b35 | 9 | 7 | 21.15% | 17.46% | 7.01% | $1,571.52 | 0.00% | $2,231.72 | false | - |
| active_pool_26181217e692f3f14f5e8b23 | 11 | 11 | 29.47% | 26.92% | 4.56% | $2,422.81 | 0.00% | $1,976.24 | false | - |
| active_pool_261d474d5a1225b83dd7e218 | 11 | 11 | 37.04% | 31.95% | 12.49% | $2,875.80 | 0.00% | $1,826.80 | false | - |
| active_pool_263a84b655328b9fe2343823 | 17 | 13 | 46.73% | 34.55% | 7.77% | $3,109.94 | 0.00% | $524.87 | false | - |
| active_pool_2664b770d222d3d178784f51 | 14 | 13 | 36.76% | 31.29% | 7.73% | $2,816.36 | 0.00% | $1,316.03 | false | - |
| active_pool_266cfade9e9a24952ab8b87d | 11 | 10 | 20.28% | 16.49% | 6.90% | $1,484.49 | 0.00% | $1,571.38 | false | - |
| active_pool_269f36d91c6fdbb666748ef1 | 14 | 12 | 39.70% | 33.77% | 9.27% | $3,039.50 | 0.00% | $1,304.86 | false | - |
| active_pool_26cf896562f947f12a64af85 | 13 | 13 | 39.81% | 33.30% | 11.33% | $2,997.38 | 0.00% | $702.17 | false | - |
| active_pool_2708a359ddcb330195d04a90 | 11 | 10 | 21.77% | 17.67% | 8.82% | $1,589.87 | 0.00% | $1,415.69 | false | - |
| active_pool_27c16f35b6e93044ccefa34e | 11 | 10 | 22.52% | 18.76% | 9.78% | $1,688.79 | 0.00% | $1,571.38 | false | - |
| active_pool_2817c813a9e85d74272e8c63 | 8 | 5 | 20.15% | 16.31% | 6.62% | $1,467.49 | 0.00% | $1,571.38 | false | - |
| active_pool_28333c9d833567c7c3b8801a | 11 | 11 | 29.51% | 25.40% | 8.44% | $2,286.11 | 0.00% | $1,447.10 | false | - |
| active_pool_287983a9cd5cd416b963d5da | 3 | 1 | 19.37% | 16.12% | 5.38% | $1,450.47 | 0.00% | $2,175.14 | false | - |
| active_pool_289111510f24d1a2f068c719 | 17 | 16 | 49.18% | 43.52% | 19.23% | $3,917.08 | 0.00% | $910.96 | true | active_behavior_5c83f2d3f4cd4296a09b |
| active_pool_28c272411cd0e4f0d2d998a8 | 12 | 11 | 38.97% | 33.79% | 9.40% | $3,040.70 | 0.00% | $1,005.55 | false | - |
| active_pool_28e5a53a9490acf8a63bf1b1 | 1 | 0 | 23.59% | 19.06% | 7.42% | $1,714.98 | 0.00% | $2,175.14 | false | - |
| active_pool_29007e26d28b203a14a165fa | 13 | 11 | 39.15% | 34.21% | 11.33% | $3,079.20 | 0.00% | $1,351.89 | false | - |
| active_pool_2937b7809de2dbd91819b125 | 11 | 10 | 21.77% | 17.67% | 8.82% | $1,589.87 | 0.00% | $1,699.38 | false | - |
| active_pool_293a88f756546d4783488aa0 | 11 | 9 | 29.97% | 24.17% | 6.29% | $2,174.89 | 0.00% | $1,537.14 | false | - |
| active_pool_29783e0cfe4ba25c599f8f46 | 15 | 13 | 39.82% | 33.86% | 11.33% | $3,047.25 | 0.00% | $1,636.46 | true | active_behavior_03f36b7165b8a0b8ff5c |
| active_pool_29c521d5f40a923a9ad2c9f0 | 10 | 8 | 20.12% | 14.76% | 5.18% | $1,328.14 | 0.00% | $2,529.84 | false | - |
| active_pool_29cfdeaf52d86d7efa1d7589 | 11 | 11 | 35.73% | 31.09% | 13.77% | $2,798.35 | 0.00% | $1,746.92 | false | - |
| active_pool_2a2d9af1d35f81c98e1aae62 | 1 | 0 | 19.46% | 16.21% | 5.38% | $1,458.97 | 0.00% | $2,175.14 | false | - |
| active_pool_2a4627e1e9307e4c75a94c50 | 17 | 16 | 47.87% | 42.37% | 14.49% | $3,813.30 | 0.00% | $1,255.87 | false | - |
| active_pool_2ac4cb6d212951dcc037bbaa | 11 | 10 | 20.36% | 16.72% | 7.23% | $1,504.40 | 0.00% | $1,629.20 | false | - |
| active_pool_2afb8298e8f2d43792cc4a63 | 11 | 11 | 36.42% | 29.50% | 13.33% | $2,655.41 | 0.00% | $1,477.10 | false | - |
| active_pool_2b501f2a5e95effb4ed7a923 | 8 | 5 | 22.69% | 19.27% | 6.63% | $1,734.63 | 0.00% | $1,903.19 | false | - |
| active_pool_2b99d174eaac255a2631075d | 11 | 10 | 39.31% | 35.08% | 16.03% | $3,156.78 | 0.00% | $1,031.23 | false | - |
| active_pool_2c8ac57e7fa9c08ef10b7773 | 4 | 3 | 23.30% | 19.48% | 7.42% | $1,752.92 | 0.00% | $2,239.14 | false | - |
| active_pool_2cabcadee2996be55208241a | 11 | 9 | 20.19% | 14.43% | 5.72% | $1,298.88 | 0.00% | $2,052.83 | false | - |
| active_pool_2cb3f15a0420abf017317326 | 14 | 13 | 46.45% | 39.44% | 15.47% | $3,549.31 | 0.00% | $1,135.69 | false | - |
| active_pool_2cdcc9f4afc589f98960f364 | 11 | 11 | 30.81% | 25.50% | 9.83% | $2,295.14 | 0.00% | $1,355.69 | false | - |
| active_pool_2d032a15a36a37c725262fee | 13 | 12 | 46.45% | 39.44% | 15.78% | $3,549.31 | 0.00% | $1,152.00 | false | - |
| active_pool_2d5814ab36f8fdea7b832028 | 17 | 14 | 52.97% | 40.63% | 8.36% | $3,656.36 | 0.00% | $524.87 | false | - |
| active_pool_2daff3406988caa7d9e44fd6 | 11 | 10 | 35.54% | 31.29% | 12.90% | $2,816.27 | 0.00% | $1,766.87 | false | - |
| active_pool_2db462c0105ee6f0d19732d3 | 5 | 3 | 20.73% | 16.86% | 7.00% | $1,517.46 | 0.00% | $2,227.69 | false | - |
| active_pool_2e27c21cd5098175873f324e | 3 | 1 | 22.23% | 18.29% | 7.42% | $1,646.47 | 0.00% | $2,175.14 | false | - |
| active_pool_2e4c9bee71542f70b4d6f8d4 | 11 | 11 | 31.93% | 24.35% | 9.83% | $2,191.86 | 0.00% | $1,538.41 | false | - |
| active_pool_2e67650163ff627d0ed9ab3c | 11 | 11 | 21.73% | 18.48% | 7.43% | $1,663.63 | 0.00% | $2,231.72 | false | - |
| active_pool_2e7f3c57ab323a4654cf4f3c | 15 | 13 | 34.60% | 28.07% | 10.61% | $2,526.73 | 0.00% | $1,240.56 | false | - |
| active_pool_2eb6b452acf5df99d9dc5820 | 11 | 11 | 36.47% | 32.25% | 15.78% | $2,902.92 | 0.00% | $1,976.24 | false | - |
| active_pool_2f3a6543feda6d11c9c2b9f3 | 9 | 8 | 26.07% | 20.10% | 9.83% | $1,809.33 | 0.00% | $1,606.21 | false | - |
| active_pool_2fc670e69d8ebce30f3a9892 | 1 | 0 | 21.17% | 17.61% | 7.00% | $1,584.52 | 0.00% | $2,227.69 | false | - |
| active_pool_2fe1aafd3e1f569bf649ae57 | 2 | 1 | 21.25% | 15.43% | 7.00% | $1,388.27 | 0.00% | $2,319.93 | false | - |
| active_pool_2fe7a7adb0a2d8dea8efd932 | 12 | 11 | 36.53% | 28.17% | 4.41% | $2,535.56 | 0.00% | $1,209.99 | false | - |
| active_pool_3044704f75596de6d515dd9f | 15 | 13 | 46.17% | 40.83% | 15.74% | $3,674.96 | 0.00% | $1,526.38 | true | active_behavior_c04b5d7743cd56e3cc1a |
| active_pool_3081ebc94e325c1f7e419d2c | 14 | 11 | 42.59% | 36.23% | 16.67% | $3,261.05 | 0.00% | $863.61 | false | - |
| active_pool_30ad8a7270b635327dd5c1ea | 11 | 11 | 34.94% | 30.33% | 11.66% | $2,729.35 | 0.00% | $1,524.54 | false | - |
| active_pool_310fa21c3baf5aeca7e67f08 | 14 | 11 | 46.00% | 40.45% | 16.82% | $3,640.15 | 0.00% | $1,169.10 | false | - |
| active_pool_3124f458b2a81de465232bed | 11 | 11 | 29.05% | 24.11% | 8.45% | $2,169.81 | 0.00% | $1,378.59 | false | - |
| active_pool_312bdee7657ed6e16a8000bf | 14 | 13 | 46.45% | 39.44% | 15.47% | $3,549.31 | 0.00% | $1,135.69 | false | - |
| active_pool_316d0d40fafb4baea9f3f924 | 11 | 11 | 37.34% | 32.57% | 12.04% | $2,931.24 | 0.00% | $1,652.51 | false | - |
| active_pool_31af2dd2cedefee43edbf2cf | 14 | 13 | 46.45% | 40.37% | 17.51% | $3,633.02 | 0.00% | $702.17 | false | - |
| active_pool_3243eaac3abac4a971c285bb | 17 | 16 | 49.66% | 43.11% | 16.66% | $3,880.32 | 0.00% | $487.86 | false | - |
| active_pool_32635f7b2de31fb1cafb324f | 17 | 16 | 48.32% | 41.97% | 15.80% | $3,777.29 | 0.00% | $1,009.67 | false | - |
| active_pool_32a31b5d9f5fa9ee160cc8a5 | 3 | 1 | 23.40% | 18.87% | 7.42% | $1,697.98 | 0.00% | $2,175.14 | false | - |
| active_pool_32eb5cd8e66ccad08aab5ce1 | 13 | 12 | 45.63% | 39.16% | 15.69% | $3,523.99 | 0.00% | $1,351.89 | false | - |
| active_pool_3301e65f04f143976aeaa257 | 11 | 11 | 22.52% | 18.95% | 9.84% | $1,705.79 | 0.00% | $1,571.38 | false | - |
| active_pool_330a58dd30a2b0a55f8fa350 | 11 | 11 | 39.31% | 35.08% | 16.03% | $3,156.78 | 0.00% | $1,031.23 | false | - |
| active_pool_331c6344977c8b2e3cc053f8 | 15 | 14 | 47.68% | 41.56% | 17.31% | $3,740.50 | 0.00% | $837.82 | true | active_behavior_b28ef18aa7b32f1b356a |
| active_pool_33373cc824d4c4cb114451ec | 13 | 11 | 43.53% | 35.17% | 15.55% | $3,164.98 | 0.00% | $1,573.48 | false | - |
| active_pool_336733a19a5e3be5499917bc | 11 | 11 | 32.18% | 29.60% | 7.30% | $2,663.94 | 0.00% | $1,495.12 | false | - |
| active_pool_33c828ee72132afcc51ae7fd | 11 | 11 | 36.54% | 30.98% | 12.11% | $2,788.39 | 0.00% | $1,248.13 | false | - |
| active_pool_3408ecf08ed802e850ac0bca | 10 | 7 | 22.87% | 18.76% | 9.78% | $1,688.79 | 0.00% | $1,571.38 | false | - |
| active_pool_3414fadf4a4a02a468439827 | 11 | 11 | 38.31% | 34.01% | 14.07% | $3,061.31 | 0.00% | $1,031.23 | false | - |
| active_pool_342a55ba6fb0e296ae293c15 | 11 | 11 | 37.76% | 33.91% | 12.60% | $3,051.56 | 0.00% | $1,652.51 | false | - |
| active_pool_345a82e2b43dd791714e3aff | 13 | 11 | 44.76% | 38.67% | 15.77% | $3,480.17 | 0.00% | $1,636.46 | false | - |
| active_pool_348ecf3963c2ce8527ff9e8f | 16 | 13 | 48.57% | 43.32% | 15.54% | $3,898.48 | 0.00% | $1,078.06 | false | - |
| active_pool_34a5fe7580798b3a714fc5a2 | 14 | 12 | 46.45% | 39.44% | 15.47% | $3,549.31 | 0.00% | $1,135.69 | false | - |
| active_pool_34baf774dd4ba4adf771513a | 13 | 11 | 31.77% | 28.78% | 10.88% | $2,590.14 | 0.00% | $1,812.19 | false | - |
| active_pool_34ccdca87d0c8fad837e5c0d | 15 | 14 | 32.24% | 23.97% | 9.20% | $2,157.60 | 0.00% | $1,649.41 | false | - |

Complete candidate-control deltas, B1–B4 candidate rows, risk paths, suppression and provenance are in the companion JSON.

Report hash: `f74bc6a842915ad53149e6d8adcbf4daab94556f567e014ed7175ac480293bcd`
