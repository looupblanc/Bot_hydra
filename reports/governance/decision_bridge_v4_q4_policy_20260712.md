# HYDRA Decision Bridge V4 — preregistered Q4 policy

Date (UTC): 2026-07-12

This policy is frozen before any Q4 market observation is decoded, summarized,
or evaluated. It applies to one cohort and one atomic run only.

## Evidence-conversion stopping rule

Stop at the earliest of: five defensible distinct PRE_HOLDOUT_READY candidates;
all detailed representatives resolved; two consecutive cohorts with zero new
distinct PRE_HOLDOUT_READY candidates; or a recorded decision-value dominance
proof. Cohorts 0001 and 0002 already satisfy the two-zero rule. Cohorts 0003 and
0004 independently confirm it. No additional candidate is generated merely to
increase cohort size.

## Cohort eligibility and selection

Every member must have completed FULL_ECONOMIC_REPLAY,
FULL_RISK_REPLAY, and FULL_PROMOTION_VALIDATION; have no hard invalidation; have
an immutable specification and trade ledger; have passed its frozen role test;
belong to a distinct Level-2 economic cluster; and have a complete executable
zero-order shadow package.

Selection is deterministic and development-only. It permits three to eight
members, one primary per Level-2 cluster, at most two per market, at most two per
primary role, and at most two closely related lineages. Backups and parameter
neighbors are excluded. Valid role diversity is preferred but no invalid role
is forced.

## Common Q4 hard failures

- lookahead or future higher-timeframe state;
- corrupted, incomplete, or wrongly attributed market data;
- invalid explicit contract, roll, multiplier, sizing, or session handling;
- impossible fill or hidden implementation discrepancy;
- catastrophic intraday MLL breach;
- event domination beyond the frozen role tolerance;
- manifest, code, policy, or governance mismatch.

## Opportunity sufficiency

For every role, fewer than five executable Q4 events is
`Q4_LOCKBOX_INSUFFICIENT` unless a common hard failure is observed. Five is a
minimum decision floor, not a target selected from Q4 results.

## COMBINE_PASSER decision

`Q4_LOCKBOX_PASS` requires all common hard gates to pass, strictly positive net
PnL after frozen costs, no MLL breach, positive target progress, frozen contract
and session compliance, and no single day contributing more than 50% of total
positive PnL. Hitting the complete Combine target during one quarter is not
required. A nonpositive net or destructive target path is a fail when the
opportunity floor is met.

## XFA_PAYOUT decision

`Q4_LOCKBOX_PASS` requires all common hard gates to pass, strictly positive net
PnL after frozen costs, no MLL breach, at least two positive qualifying days,
no single day contributing more than 50% of total positive PnL, and no
catastrophic contradiction in the frozen Standard/Consistency path. A
nonpositive net, MLL failure, or absence of qualifying-day behavior is a fail
when the opportunity floor is met.

## DEFENSIVE_ACCOUNT / PORTFOLIO_ONLY decision

The frozen base account is the cohort’s non-defensive members. The candidate is
added without rescaling or reselecting them. `Q4_LOCKBOX_PASS` requires all
common hard gates to pass and at least one material benefit: smaller maximum
drawdown, larger minimum MLL buffer, or fewer shared loss days. It must not
reduce target velocity by more than 25%, and its matched inclusion/deactivation
probability must be at most 0.10 when the control has enough permutations. With
too few account events or fewer than 32 valid controls, the result is
insufficient rather than a pass. Large standalone profit is not required.

## Atomicity and post-decision status

The runner stages every candidate result, verifies cohort completeness, and
commits one result bundle. Any interruption after the capability is consumed
quarantines temporary output, revokes access, and requires Q4_REVIEW_REQUIRED;
there is no automatic scientific retry. The authoritative access ledger records
one cohort access only.

Possible scientific results are only `Q4_LOCKBOX_PASS`, `Q4_LOCKBOX_FAIL`, and
`Q4_LOCKBOX_INSUFFICIENT`. A pass plus a complete immutable fail-closed shadow
package produces `PAPER_SHADOW_READY`, meaning zero-order forward observation;
it does not mean live, trading, or funded readiness. Independent 2025 evidence
is reserved for later promotion.
