# HYDRA Operating Package V1

- Manifest hash: `9fd671d43127004d9ab11d82a42f07a71e9d846e559b1317c53c488dd1bc0d87`
- Evidence status: development-selected; append-only confirmation pending
- Complete-book stacking: prohibited
- Broker connections / orders: `0 / 0`

## Frozen roles

- `CORE_BOOK`: `active_pool_186a4177401aab223b0a21fa` — Highest stressed book net and highest corrected stressed Consistency EV per new Combine attempt; positive evidence in B1/B3/B4.
- `SAFETY_BOOK`: `active_pool_14e275fa8d869c28b1f27f78` — Strongest observed minimum MLL buffer among primaries, positive stressed economics, acceptable passes, and lower exact-trade overlap with Core.
- `DIVERSIFIER_BOOK`: `active_pool_2287bfb0b1c6f07930150102` — Most distinct surviving primary governor subject to positive stressed economics and observed Consistency post-payout survival; diversification is routing-relative, not independent alpha.
- `BACKUP_BOOK`: `active_pool_014dffb40e99814612d78c51` — Frozen backup from a distinct economic-behavior cluster and lowest overall trajectory overlap; weaker economics are retained explicitly.

## Book evidence and selected XFA alternative

| Book | Role | N passes | S passes | S net USD | Min S MLL buffer | Std EV/attempt | Cons EV/attempt | XFA | Post-payout profile |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| `active_pool_070e391c7586ba1fac2f5494` | RESERVE_ALTERNATIVE_BOOK | 66/192 | 64/192 | 969063.97 | 1009.67 | 4701.97 | 6121.61 | CONSISTENCY | XFA_BALANCED_PROFILE |
| `active_pool_186a4177401aab223b0a21fa` | CORE_BOOK | 66/192 | 65/192 | 984142.25 | 873.88 | 5305.99 | 7065.75 | CONSISTENCY | XFA_LONGEVITY_PROFILE |
| `active_pool_14e275fa8d869c28b1f27f78` | SAFETY_BOOK | 66/192 | 63/192 | 902432.01 | 1033.49 | 5178.82 | 6489.40 | CONSISTENCY | XFA_BALANCED_PROFILE |
| `active_pool_2287bfb0b1c6f07930150102` | DIVERSIFIER_BOOK | 66/192 | 65/192 | 954288.09 | 621.08 | 4785.28 | 6753.51 | CONSISTENCY | XFA_BALANCED_PROFILE |
| `active_pool_2377af7025aadf9aaf456a7e` | RESERVE_ALTERNATIVE_BOOK | 66/192 | 65/192 | 964858.66 | 487.86 | 5596.97 | 6285.58 | CONSISTENCY | XFA_BALANCED_PROFILE |
| `active_pool_014dffb40e99814612d78c51` | BACKUP_BOOK | 64/192 | 64/192 | 876943.22 | 924.09 | 5008.35 | 5762.96 | CONSISTENCY | XFA_BALANCED_PROFILE |

## Selected post-payout frontier (stressed 1.5x)

| Book | Profile | P(>=2|XFA) | P(>=2)/attempt | Cycles/XFA | Cycles/attempt | Survival 30d | 60d | 90d | EV/XFA | EV/new attempt |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `active_pool_070e391c7586ba1fac2f5494` | XFA_BALANCED_PROFILE | 92.31% | 30.77% | 5.130 | 1.710 | 100.00% | 100.00% | 100.00% | 5661.31 | 1887.10 |
| `active_pool_186a4177401aab223b0a21fa` | XFA_LONGEVITY_PROFILE | 91.72% | 31.05% | 4.817 | 1.631 | 100.00% | 100.00% | 100.00% | 541.86 | 183.44 |
| `active_pool_14e275fa8d869c28b1f27f78` | XFA_BALANCED_PROFILE | 91.12% | 29.90% | 4.580 | 1.503 | 100.00% | 100.00% | 100.00% | 5386.47 | 1767.44 |
| `active_pool_2287bfb0b1c6f07930150102` | XFA_BALANCED_PROFILE | 88.17% | 29.85% | 4.882 | 1.653 | 100.00% | 100.00% | 100.00% | 5568.83 | 1885.28 |
| `active_pool_2377af7025aadf9aaf456a7e` | XFA_BALANCED_PROFILE | 91.72% | 31.05% | 5.349 | 1.811 | 100.00% | 100.00% | 100.00% | 5655.34 | 1914.57 |
| `active_pool_014dffb40e99814612d78c51` | XFA_BALANCED_PROFILE | 86.98% | 28.99% | 5.178 | 1.726 | 100.00% | 100.00% | 100.00% | 5224.95 | 1741.65 |

## Redundancy qualification

All six books share the same 18 immutable sleeves. Their differences are governor/path differences, not six independent alpha sources.

## Append-only forward state

- Feed: `WAITING_FOR_FIRST_POST_FREEZE_FORWARD_BAR`
- Available end: `2026-07-16T12:54:34.250710+00:00`
- Fresh bars / signals / virtual fills: `0 / 0 / 0`
- Initial spend / remaining budget: `$0.000400119` / `$37.152211`
- Processor: `WAITING_FOR_GENUINE_POST_FREEZE_COMPLETE_ROOT_BARS`
- Exact blocker: genuine post-freeze bars and proven online feature/signal equivalence.

## Safety classification

No book is PAPER_SHADOW_READY. All launch designations are research selections pending sequential forward gates.

## B2 specialist lane

Reserved at <=10% compute; no specialist is admitted until positive B2, low-overlap, outside-B2 and new-book-simulation gates all pass. It does not alter or delay the six frozen books.
