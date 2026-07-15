# HYDRA EvidenceBundle v1

`HYDRA_EVIDENCE_BUNDLE_V1` is the mandatory evidence boundary for economic
campaigns. A campaign is not `COMPLETE` until its sealed bundle passes
`require_complete_evidence_bundle`.

The payload lives under an ignored cache directory such as
`data/cache/evidence_bundles/`. It contains deterministic gzip-compressed JSONL
partitions. Each partition carries its own contract envelope, batch ID, sort
contract, row count, uncompressed payload hash, and compressed-file hash. The
small receipt supplied to `finalize` is suitable for Git; it contains only
identity, row counts, bundle/manifest hashes, status, and the payload location.

Required ledgers are:

- `component_signals`, `component_entries`, `component_exits`, and
  `component_trades`;
- `account_policy_membership` and `account_daily_paths`;
- `episodes` with paired normal/stressed scenarios;
- `provenance`, including an explicit reconstruction flag and immutable source
  checksums.

Required compact outputs are `campaign_summary`, `failure_vectors`,
`pareto_archive`, and `next_campaign_recommendations`.

Identity includes a frozen `expected_coverage` object:

```json
{
  "policy_ids": ["policy_001"],
  "component_ids": ["component_001"],
  "required_episode_keys": [
    {"policy_id": "policy_001", "episode_id": "episode_001", "horizon": "40D"}
  ],
  "allowed_horizons": ["40D"],
  "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
  "allow_additional_episode_keys": false
}
```

The policy/component lists exactly match the immutable fingerprint maps. Required
base episode keys must all be present. Campaigns with selection-dependent later
stages may allow additional keys, but those keys remain restricted to the frozen
policy and horizon universes and every episode must have exactly the normal and
1.5x-stressed scenarios.

Only one process can write a campaign bundle. `flock` releases safely after a
process crash; `resume` rescans and validates committed partitions instead of
trusting a possibly stale checkpoint. Pass `expected_identity` to `resume` to
bind recovery to the current immutable campaign identity. Batch IDs make retries
idempotent.
Finalization validates referential integrity, paired cost scenarios, daily-path
coverage, and exact reconciliation of entry/exit/trade fields. Gross PnL minus
costs must equal net PnL. Episode summaries carry net PnL, target progress,
minimum MLL buffer, and consistency status; all four reconcile against the
complete daily path under frozen numerical tolerances. Every daily row records
both the closing `mll_buffer` (`equity - mll`) and the lowest intraday
`minimum_mll_buffer`; the episode minimum reconciles against the latter, so an
intraday excursion is never disguised as an end-of-day balance. Terminal classifications
must agree with observed progress, MLL buffer, censoring, and days to target.
`component_attribution` on an account daily row is that day's incremental
realized attribution. Its component-wise sum over the complete episode path
must reconcile to the episode contribution and net PnL; a quiet terminal day
is therefore allowed to carry an empty attribution object.
It then writes the
content-addressed manifest and atomically renames staging to the final immutable
directory. The default final payload permissions are read-only.

The atomic directory rename is the sole commit point. If a process dies after
that rename but before writing the lightweight receipt, call
`recover_finalized_evidence_bundle`. It takes the same campaign lock, deep-verifies
the final directory, checks the optional expected identity, restores read-only
permissions, and idempotently projects the receipt. It never overwrites a
conflicting receipt or an invalid final bundle.

Minimal integration:

```python
from hydra.evidence import EvidenceBundleWriter, guard_campaign_completion

writer = EvidenceBundleWriter.create(cache_root, identity, writer_id=campaign_id)
writer.append_records("component_signals", signal_rows, batch_id="signals-0000")
# append every required dataset in bounded batches
writer.write_compact_output("campaign_summary", campaign_summary)
writer.write_compact_output("failure_vectors", failure_vectors)
writer.write_compact_output("pareto_archive", pareto_archive)
writer.write_compact_output("next_campaign_recommendations", recommendations)
receipt = writer.finalize(
    evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
    lightweight_manifest_path=report_dir / "evidence_bundle_receipt.json",
)
guard_campaign_completion("COMPLETE", receipt.bundle_path, campaign_id=campaign_id)
```
