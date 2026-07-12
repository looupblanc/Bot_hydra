# HYDRA Decision Bridge V4 — engineering authorization

Date (UTC): 2026-07-12

## Scientific objective

Convert the existing promotion evidence into one immutable, role-diverse
pre-holdout cohort; permit exactly one manifest-bound Q4 decision transaction;
package Q4 survivors for zero-order paper shadow; and establish append-only
post-freeze forward evidence without weakening any existing integrity rule.

## Explicit authority

The user explicitly authorizes the minimum protected-governance change needed
to enable one atomic Q4 run for one frozen cohort after every existing
pre-holdout condition succeeds. This authority does not permit general Q4
loading, previewing, repeated replay, post-Q4 mutation/retest, or any weakening
of no-lookahead, provenance, data-role, budget, broker, or order safeguards.

## Required behavior

1. Stop scheduling evidence-conversion cohorts once the earliest preregistered
   stopping condition is met, including two consecutive cohorts with zero new
   economically distinct PRE_HOLDOUT_READY candidates.
2. Select and hash one final cohort of three to eight distinct candidates using
   only development evidence and frozen role-specific rules.
3. Produce complete immutable no-broker/no-order shadow packages before Q4.
4. Issue a cohort-specific, single-use authorization token only after all
   integrity, provenance, manifest, access-count, and contamination checks pass.
5. Make Q4 inaccessible to general data loaders. Execute the authorized cohort
   once, stage all results, verify completeness, commit the result bundle, record
   access exactly once, revoke access, and fail closed.
6. Quarantine interrupted transactions and require Q4_REVIEW_REQUIRED; never
   retry scientifically without explicit review.
7. Admit Q4 passes to PAPER_SHADOW_READY only when their executable shadow
   package is complete. This status authorizes zero-order observation only.
8. Build the smallest append-only post-freeze feed that current authorization
   and budget allow, preserving at least USD 30 for final lockbox and execution
   validation.

## Allowed paths

- `hydra/governance/` only for the narrowly authorized one-shot capability
- `hydra/validation/q4_atomic_runner.py`
- `hydra/promotion/`
- `hydra/shadow/`
- `hydra/mission/` only for scheduling and serialized state integration
- `scripts/authorize_q4_cohort.py`
- `scripts/run_q4_one_shot.py`
- relevant tests and lightweight reports/manifests
- the governance YAML only for an explicit versioned one-shot policy clause

## Protected paths and invariants

All existing protected paths remain protected. Edits to a protected file are
limited to the explicit one-shot authorization above. General Q4 loaders,
no-lookahead checks, data-role checks, registry/mission integrity, budget floors,
single-writer rules, live-trading prohibition, broker prohibition, and order
prohibition may not be weakened.

## Acceptance tests

- Cohort-specific token; wrong manifest/commit/specification/cost rejected.
- Nonzero Q4 access count rejected.
- General Q4 loader remains rejected.
- Token is single-use and revoked on success or interruption.
- Complete cohort result is committed as one verified bundle.
- Interrupted execution is quarantined and cannot auto-retry.
- Authoritative Q4 access is counted once and only once.
- Candidate selection is deterministic, development-only, role-aware, and
  cluster-aware; backups and parameter clones are excluded.
- Shadow packages are immutable, complete, fail-closed, and contain no broker or
  order path.
- Forward feed accepts only post-freeze append-only bars, rejects stale and
  duplicate data, resolves explicit contracts, and respects budget policy.
- Full pytest, focused no-lookahead/Q4/budget/shadow suites, compileall, SQLite
  integrity, governance hashes, secret scan, and deterministic smoke replay pass.

## Rollback conditions

Rollback and keep Q4 sealed if any test fails, any protected invariant becomes
weaker, a deterministic reference result changes unexpectedly, Q4 data becomes
generally loadable, one-writer integrity is uncertain, a secret/raw Q4 payload is
staged for Git, or the transaction cannot prove one-shot crash semantics.

## Expected decision information value

Very high. Discovery is already oversupplied while the three defensible current
candidates cannot cross the protected decision boundary. This capability closes
the binding evidence/governance gap without expanding Q4 exposure and directly
enables the first legitimate PAPER_SHADOW_READY decision.

