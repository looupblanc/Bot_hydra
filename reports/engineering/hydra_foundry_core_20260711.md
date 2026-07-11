# Immutable Engineering Task — HYDRA Strategy and Shadow Foundry Core v1

Task ID: `eng_hydra_foundry_core_20260711_v1`

Frozen before implementation. The current direct tournament produced 120
preregistered atoms, 102 falsifications, 18 insufficient results, zero fully
validated mechanisms, zero strategies and zero Topstep-path candidates.

Frozen tournament artifacts:

- preregistration SHA-256:
  `2578377a0623ae1337eef7980bcee6cd30db421810923c4ab6f2d388011960d5`
- report SHA-256:
  `49f38ef88b0142aa769677cb4f6dedb5d05089228ee1abf8f056ec115426ce88`
- checkpoint SHA-256:
  `021ad20268d4b2cd31f36039f831dabeefb85baf44a9b28c12f9da00dc09f1fb`

## Scientific and operational objective

Convert the existing binary research/near-funded funnel into a calibrated
multi-tier foundry without weakening fatal integrity gates. Implement:

1. evidence-tier statuses through `PAPER_SHADOW_READY`;
2. deterministic closed-bar multi-timeframe resampling with availability and
   decision timestamps;
3. a quality-diversity archive with niche, family, ecology and lineage caps;
4. an immutable, fail-closed, zero-order-capability shadow specification and
   virtual execution runner;
5. controller-visible foundry metrics and a governed bootstrap experiment that
   reconciles the direct tournament and calibrates shadow admission controls.

`PAPER_SHADOW_READY` means safe and credible enough to collect zero-risk
forward evidence. It does not mean funded-ready. Fatal invalidations remain
fatal: leakage, corrupt data, contract/roll defects, impossible fills,
uncontrolled sizing or MLL, prohibited sessions and governance violations.

## Allowed paths

- `hydra/foundry/**`
- `hydra/data/multitimeframe.py`
- `hydra/factory/quality_diversity.py`
- `hydra/shadow/**`
- `scripts/run_shadow_portfolio.py`
- `scripts/shadow_status.py`
- `scripts/shadow_stop.py`
- `scripts/shadow_report.py`
- mission runner/controller/reporting/status integration
- focused tests and new immutable reports

## Protected paths

Q4/future lockboxes, governance roles, existing historical atoms/results,
registry and mission databases, credentials, broker/order modules and raw
market data. No outbound order path may be added.

## Acceptance

- synthetic null admission rate <= 5%; weak-real controls are admitted to
  shadow research with calibrated uncertainty; injected strong controls reach
  paper-shadow readiness;
- incomplete higher-timeframe bars can never join a decision;
- DST/session and roll grouping are deterministic;
- quality-diversity caps prevent clone/family domination;
- shadow specifications are immutable and contain no broker/order capability;
- stale data, duplicates, session close, MLL and kill switch fail closed;
- full pytest, compileall, no-lookahead, governance, Q4, budget, lock,
  single-writer, secret scan, SQLite and deterministic smoke pass.

Rollback on any weakened fatal gate, future-data access, order capability,
duplicate registry writer, non-determinism or two failed implementation
attempts.

Expected decision information value: `1.0`.
