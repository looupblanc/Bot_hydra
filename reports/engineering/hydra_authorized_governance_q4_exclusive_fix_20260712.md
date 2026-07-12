# Authorized governance change — Q4 exclusive-boundary label

## Human authorization

On 2026-07-12 UTC the user explicitly replied `j'autorise tout cela` to the
precise request authorizing the protected change described in
`GOVERNANCE_CHANGE_REQUIRED_q4_exclusive_label_20260711.md`:

- normalize only the literal trailing `_EXCLUSIVE` boundary marker;
- add the corresponding protected tests;
- do not modify, delete, truncate, or rebuild the append-only access ledger.

## Scientific and governance objective

Correct a false Q4-access count caused by comparing the metadata-bearing token
`2024-10-01_EXCLUSIVE` lexicographically.  Preserve fail-closed accounting for
all actual Q4 ranges and all sealed/final data roles.

## Authorized paths

- `hydra/governance/invariants.py`
- `tests/test_mission_governance_and_calibration.py`
- this authorization record

The runtime protected manifest and kernel status may be regenerated only by
the existing governance-kernel initialization after the code and tests pass.

## Prohibited changes

- no access-ledger mutation or replacement;
- no modification to the governance YAML, kernel, protected-manifest builder,
  evidence scope, status provenance, promotion contract, lockbox guard, or
  budget kernel;
- no Q4 read, data purchase, network request, live/broker capability, registry
  status promotion, mission-schema change, or duplicate service.

## Required behavior

1. Strip exactly one terminal literal `_EXCLUSIVE` marker from a period
   boundary before comparing it with `2024-10-01`.
2. `2023-01-01:2024-10-01_EXCLUSIVE` counts as zero Q4 accesses.
3. `2023-01-01:2024-10-02_EXCLUSIVE` counts as one.
4. A start at or after `2024-10-01` counts as one, including a marked start.
5. `SEALED_BLIND_HOLDOUT` and `FINAL_LOCKBOX` continue to count regardless of
   their period label.
6. Repeated or non-terminal `_EXCLUSIVE` marker labels must not gain an evasion
   path.  Broader malformed-ledger semantics are outside this narrowly
   authorized change.

## Acceptance tests

- targeted protected tests pass;
- full pytest passes against an isolated clean-state fixture;
- the real append-only ledger remains byte-identical;
- real authoritative Q4 count changes from the false positive `1` to `0`;
- SQLite DB and registry integrity are `ok`;
- governance checks and regenerated protected manifest pass;
- no-lookahead, budget, process-lock, one-writer, secret scan, compileall, and
  deterministic scheduler smoke pass;
- after merge, the unique service resumes the preserved queued experiment and
  advances its cycle/heartbeat without a duplicate writer.

## Rollback conditions

Rollback the code change and keep the service stopped if any positive Q4
control becomes uncounted, a sealed/final role can evade counting, the ledger
checksum changes, the protected manifest cannot be regenerated normally, or
any safety/regression test fails.

## Expected information value

Decisive: this removes the sole false integrity blocker while preserving every
substantive Q4 protection and allows the existing research queue to resume.
