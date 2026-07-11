# HYDRA Parallel Pipeline Scheduler

- Task ID: `hydra_parallel_pipeline_scheduler_20260711`
- Created: 2026-07-11 UTC
- Objective: run independent Discovery, Promotion/Portfolio and Meta-Research
  experiments concurrently while preserving one registry/SQLite writer
- Expected decision information value: 0.98

## Required behavior

1. The controller remains the only mission DB and registry writer.
2. Claim at most the configured worker count (three on the current host).
3. Batch only experiments explicitly marked `parallel_safe=true`.
4. Use at most one experiment per pipeline in a parallel batch.
5. Validate governance before worker launch; reject Q4, paid-data, network,
   live/broker or unknown-handler specifications exactly as in sequential mode.
6. Spawn isolated worker processes concurrently and give each an immutable
   specification hash, claim token, result path and lease.
7. Renew every active lease and heartbeat while any worker runs.
8. Tick the fail-closed shadow pipeline during the batch.
9. Workers may write only their isolated output directories and explicitly
   authorized append-only ledgers.  The initial parallel batch must contain at
   most one market-data-ledger writer.
10. After all workers terminate, the controller validates each result envelope,
    commits DB results serially in deterministic claim order, and reconciles the
    evidence graph serially.
11. Failure or retry of one experiment must not discard a successful sibling.
12. On SIGTERM/manual stop, terminate every worker tree, release every claim
    without consuming a scientific retry, preserve queues and checkpoint cleanly.
13. Expose all active experiment IDs, PIDs, pipelines and leases in heartbeat
    state; retain the legacy single `current_experiment` summary for tools.
14. Fall back to the existing sequential path for non-parallel-safe work.

## Allowed paths

- `hydra/pipelines/resource_scheduler.py`;
- `hydra/mission/controller.py`;
- minimal mission status/doctor fields;
- scheduler, restart, one-writer and queue-persistence tests;
- this engineering specification.

## Protected paths

- governance/Q4/lockbox policy;
- experiment and mission DB schemas;
- evidence semantics and candidate status policy;
- budget/data-access policy;
- shadow configurations and active registry;
- live, broker, credential and order paths.

## Acceptance tests

- two or three distinct pipelines are claimed and executed concurrently;
- same-pipeline work remains queued for the next batch;
- non-`parallel_safe` work remains sequential;
- one DB writer and deterministic serial result commits;
- partial success survives sibling failure;
- clean interruption releases every claim and preserves attempt counts;
- crash recovery requeues leased work without duplicates;
- heartbeat/lease/shadow ticks remain fresh during long workers;
- full tests, no-lookahead, compile, SQLite, governance and secret scan;
- deterministic smoke batch proves overlapping worker lifetimes.

## Rollback conditions

Rollback if more than one DB/registry writer appears, result ordering changes
scientific routing, a claim is lost or duplicated, stop/restart consumes retries,
heartbeat becomes stale, a worker escapes its process group, protected policy
changes, or throughput regresses for independent workloads.

## Interpretation boundary

Parallel execution changes scheduling latency only.  It cannot alter candidate
specifications, evidence gates, holdout policy, strategy status, Q4 access,
budget authority or order capability.
