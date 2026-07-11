# GOVERNANCE_CHANGE_REQUIRED — exclusive Q4-boundary label

- Detected: 2026-07-11T23:28 UTC
- Mission: `hydra_autonomous_v1`
- Deployed commit: `0343b8899987bc9a5a35898e29a27414ef736fbc`
- Protected invariant affected: `q4_sealed_until_freeze`
- Actual Q4 market-data rows read: **0**
- Governance counter result: **1**
- Service response: `INTEGRITY_BLOCKED` / fail closed

## Cause

The failed first attempt of
`equity_preclose_inventory_dispersion_primary_v1` appended one immutable access
record with:

- role `CONTAMINATED_DEVELOPMENT`;
- period `2023-01-01:2024-10-01_EXCLUSIVE`;
- no freeze manifest;
- line SHA-256
  `b46878b24b779638b255153e460820b60602acffd19cb34a317555e8efdb5db2`.

The protected `q4_access_count()` implementation compares the raw end token
lexicographically with `2024-10-01`.  The suffix `_EXCLUSIVE` therefore makes
the token sort after the boundary and increments the count, even though the
label explicitly describes an exclusive boundary.

The source parquets have these maximum timestamps:

- ES/MES/NQ/MNQ 2023: `2023-12-29T21:59:00Z`;
- 2024 Q1: `2024-03-28T20:59:00Z`;
- 2024 Q2: `2024-06-30T23:59:00Z`;
- 2024 Q3: `2024-09-30T23:59:00Z`;
- RTY/M2K/YM/MYM combined development cache: `2024-09-30T23:59:00Z`.

The loader additionally enforces `timestamp < 2024-10-01T00:00:00Z`.  Thus
this is a ledger-label parsing defect, not a Q4 read.

## Actions already taken outside protected governance

1. The append-only access ledger was preserved; no line was deleted, rewritten
   or truncated.
2. Both new research modules now emit the canonical existing period label
   `2023-01-01:2024-10-01`, which the protected counter already interprets as
   exclusive.
3. The independent pre-close matched-null positional-index defect was repaired
   and covered by a regression test.
4. The queued experiment remains fail-closed and no further market-data worker
   can run while governance fails.

## Minimal protected change requiring approval

Amend only the period parser in `hydra/governance/invariants.py` so a trailing
literal `_EXCLUSIVE` is parsed as boundary metadata before comparison.  It must
not suppress role-based Q4 counts and must still count:

- any `SEALED_BLIND_HOLDOUT` or `FINAL_LOCKBOX` record;
- a start at or after `2024-10-01`;
- an exclusive end strictly after `2024-10-01`;
- any ordinary end strictly after `2024-10-01`.

Add protected regression cases in
`tests/test_mission_governance_and_calibration.py`:

- `2023-01-01:2024-10-01_EXCLUSIVE` → 0;
- `2023-01-01:2024-10-02_EXCLUSIVE` → 1;
- existing three positive controls remain 3.

No ledger mutation is authorized or required.  After approval and protected
change, rerun the complete governance suite, full pytest, governance checksum,
SQLite integrity and doctor before allowing the queued development experiment
to retry.

## Rollback

Rollback the parser change if any existing positive control becomes uncounted,
if a sealed/final role can evade counting, or if the protected manifest cannot
be regenerated through the project's authorized governance-change procedure.

