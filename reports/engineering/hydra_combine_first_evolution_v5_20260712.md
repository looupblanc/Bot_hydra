# HYDRA Combine-First Evolution Factory V5 — immutable engineering task

- Written at: `2026-07-12T09:08:00Z`
- Baseline commit: `9b56ae070a43473c2951393329f773f926bd19fc`
- Mission: `hydra_autonomous_v1`
- Expected decision information value: `0.98`

## Scientific objective

Measure and optimize the probability that an unchanged, executable futures
strategy reaches the versioned Topstep 150K Combine target before touching the
intraday MLL.  Keep Combine, XFA payout and defensive account utility as
separate objectives.  Use repeated, block-aware historical account episodes
rather than one full-period aggregate path.

## Required behavior

1. Build chronological event paths with frozen costs and conservative OHLC
   unrealized loss.
2. Select deterministic, block-aware and past-regime-aware episode starts.
3. Stop each episode on target/consistency pass, MLL touch, timeout or a frozen
   compliance terminal.
4. Report pass, breach, timeout, target velocity, MLL buffer, concentration,
   net economics and failure-regime distributions.
5. Score Combine, XFA and defensive roles independently with every component
   logged.
6. Generate versioned children from explicit failure diagnoses; inherit no
   evidence status and reject fingerprints before replay.
7. Maintain quality-diversity niches and at least 20% pure exploration.
8. Run through the existing experiment queue, worker safety model and sole
   controller writer.
9. Use development data only (`2023-01-01` through `2024-09-30`); Q4 reuse,
   network, paid data, broker and order paths are prohibited.

## Allowed paths

- `hydra/propfirm/`
- `hydra/research/`
- `hydra/factory/`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `scripts/run_rolling_combine_tournament.py`
- `tests/`
- this engineering report

## Protected paths

- `config/governance/hydra_governance_v1.yaml`
- `hydra/validation/data_roles.py`
- `hydra/validation/lockbox_guard.py`
- Q4 manifests, authorization and result state
- mission and registry databases
- raw/cached market data
- shadow ledgers and broker/order surfaces

## Acceptance tests

- deterministic and spaced episode starts;
- target, MLL, timeout and consistency terminals;
- unrealized-PnL and cost inclusion;
- contract/session limit checks;
- role-specific fitness component recomputation;
- mutation provenance and no status inheritance;
- archive niche and clone rejection;
- meta-screen allocation-only boundary and exploration floor;
- consumed-Q4 protection and no broker/order capability;
- deterministic reference tournament smoke;
- full pytest, no-lookahead, compileall, integrity, governance and secret scan.

## Rollback conditions

- any lookahead or future-regime use;
- a mismatch with the versioned Topstep rule snapshot;
- non-deterministic reference outputs;
- workers opening writable shared SQLite;
- Q4 access delta, paid-data request or outbound-order surface;
- material regression in existing Turbo throughput;
- mission resume with more than one controller/writer.

## Information-value rationale

The existing inventory is rich in positive development candidates but the old
single-path promotion funnel does not answer the account question directly.
Rolling terminal episodes discriminate target velocity from MLL risk, provide
actionable mutation diagnoses, and can convert the existing compute throughput
into measured Combine-pass probability without spending protected data.
