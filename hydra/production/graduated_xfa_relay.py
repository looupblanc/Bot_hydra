"""Fail-closed XFA relay planning for immutable Tier-G Combine books.

This module prepares no-order, read-only XFA work declarations.  It never
runs an XFA simulation and never writes a registry, database, or mission file.
The authoritative parent writer may persist the returned mapping later.

The persisted official snapshot covers 50K, 100K, and 150K commercial rules,
but the currently versioned executable lifecycle engine is deliberately a
150K engine.  Consequently 50K/100K plans are represented as deterministic
fail-closed decisions until an account-size-aware executable rule object is
verified.  They are never silently coerced to the 150K engine.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.combine_to_xfa import official_rule_snapshot_2026_07_15


SCHEMA = "hydra_graduated_xfa_relay_plan_v1"
RUN_SPEC_SCHEMA = "hydra_graduated_xfa_alternative_run_spec_v1"
DEFAULT_RULE_SNAPSHOT = Path(
    "config/rulesets/topstep_official_2026-07-19.json"
)

_ACCOUNT_LABELS = {"50K": 50_000, "100K": 100_000, "150K": 150_000}
_XFA_PATHS = ("STANDARD", "CONSISTENCY")
_RESTRICTED_MARKET_ROOTS = {
    "CL",
    "QM",
    "RB",
    "HO",
    "MCL",
    "GC",
    "MGC",
    "SI",
    "HG",
    "PL",
    "SIL",
    "MHG",
}


class GraduatedXfaRelayError(RuntimeError):
    """The requested relay is not immutable, graduated, or rule-safe."""


def prepare_graduated_xfa_relay(
    graduation_receipt: Mapping[str, Any],
    combine_paths: Sequence[Mapping[str, Any]],
    *,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> dict[str, Any]:
    """Prepare two alternative XFA lanes without executing either one.

    ``STANDARD`` and ``CONSISTENCY`` are alternative frozen choices for the
    same successful Combine transition.  The result intentionally exposes no
    combined EV field and explicitly forbids adding their expected values.
    """

    graduation = _verified_graduation_receipt(graduation_receipt)
    account_label = str(graduation["account_label"])
    account_size = int(graduation["account_size_usd"])
    expected_size = _ACCOUNT_LABELS.get(account_label)
    if expected_size is None or expected_size != account_size:
        raise GraduatedXfaRelayError(
            "graduated account label/size binding is invalid"
        )
    paths = _verified_combine_paths(combine_paths, graduation)
    snapshot, binding = _load_official_snapshot(
        Path(rule_snapshot_path),
        account_label=account_label,
        account_size=account_size,
        markets=tuple(str(value) for value in graduation["markets"]),
    )

    base = {
        "schema": SCHEMA,
        "candidate_id": str(graduation["candidate_id"]),
        "graduation_result_hash": str(graduation["result_hash"]),
        "combine_book_hash": str(graduation["combine_book_hash"]),
        "xfa_book_hash": str(graduation["xfa_book_hash"]),
        "xfa_profile_hash": str(graduation["xfa_profile_hash"]),
        "account_label": account_label,
        "account_size_usd": account_size,
        "rule_snapshot": binding,
        "combine_transition_count": len(paths),
        "combine_transition_hashes": [str(row["path_hash"]) for row in paths],
        "combine_paths_all_immutable_and_passed": True,
        "standard_and_consistency_are_alternatives": True,
        "sum_standard_and_consistency_ev_allowed": False,
        "ev_aggregation_contract": (
            "REPORT_EACH_FROZEN_XFA_PATH_SEPARATELY_NEVER_SUM_ALTERNATIVES"
        ),
        "simulation_started": False,
        "xfa_paths_started": 0,
        "broker_connections": 0,
        "orders": 0,
        "registry_writes": 0,
        "database_writes": 0,
    }
    if binding["executable_rule_status"] != "VERIFIED_EXECUTABLE_150K":
        core = {
            **base,
            "status": "XFA_RULES_UNVERIFIED_FAIL_CLOSED",
            "blocked": True,
            "blocked_reason_code": binding["blocked_reason_code"],
            "blocked_reasons": list(binding["blocked_reasons"]),
            "alternative_runs": {path: [] for path in _XFA_PATHS},
            "next_action": "VERIFY_ACCOUNT_SIZE_AWARE_EXECUTABLE_XFA_RULE_SNAPSHOT",
        }
        return {**core, "result_hash": stable_hash(core)}

    alternative_runs = {
        path: [
            _run_spec(
                graduation=graduation,
                combine_path=row,
                xfa_path=path,
                snapshot_binding=binding,
            )
            for row in paths
        ]
        for path in _XFA_PATHS
    }
    core = {
        **base,
        "status": "READY_FOR_EXPLICIT_LATER_XFA_SIMULATION",
        "blocked": False,
        "blocked_reason_code": None,
        "blocked_reasons": [],
        "alternative_runs": alternative_runs,
        "alternative_run_counts": {
            path: len(alternative_runs[path]) for path in _XFA_PATHS
        },
        "next_action": "PERSIST_PLAN_AND_WAIT_FOR_EXPLICIT_TIER_G_XFA_DISPATCH",
    }
    # Keep the loaded object live until every compatibility check above has
    # completed.  It is deliberately not returned or used to simulate here.
    del snapshot
    return {**core, "result_hash": stable_hash(core)}


def assert_graduated_xfa_relay_ready(plan: Mapping[str, Any]) -> None:
    """Fail before dispatch when a deterministic relay plan is not executable."""

    payload = dict(plan)
    claimed = str(payload.pop("result_hash", ""))
    if not claimed or stable_hash(payload) != claimed or plan.get("schema") != SCHEMA:
        raise GraduatedXfaRelayError("XFA relay plan identity/hash drift")
    if plan.get("status") != "READY_FOR_EXPLICIT_LATER_XFA_SIMULATION":
        reason = str(plan.get("blocked_reason_code") or "XFA_RELAY_NOT_READY")
        raise GraduatedXfaRelayError(f"XFA relay dispatch refused: {reason}")
    alternatives = plan.get("alternative_runs")
    if not isinstance(alternatives, Mapping) or set(alternatives) != set(_XFA_PATHS):
        raise GraduatedXfaRelayError("XFA alternative-path inventory drift")
    if plan.get("sum_standard_and_consistency_ev_allowed") is not False:
        raise GraduatedXfaRelayError("XFA alternative EV summation was enabled")
    snapshot_binding = plan.get("rule_snapshot")
    if (
        not isinstance(snapshot_binding, Mapping)
        or snapshot_binding.get("executable_rule_status")
        != "VERIFIED_EXECUTABLE_150K"
        or str(snapshot_binding.get("account_label"))
        != str(plan.get("account_label"))
        or int(snapshot_binding.get("account_size_usd", -1))
        != int(plan.get("account_size_usd", -2))
        or snapshot_binding.get("source_provenance_status") != "VERIFIED"
    ):
        raise GraduatedXfaRelayError("XFA executable rule binding drift")
    if (
        plan.get("simulation_started") is not False
        or int(plan.get("xfa_paths_started", -1)) != 0
        or int(plan.get("broker_connections", -1)) != 0
        or int(plan.get("orders", -1)) != 0
        or int(plan.get("registry_writes", -1)) != 0
        or int(plan.get("database_writes", -1)) != 0
    ):
        raise GraduatedXfaRelayError(
            "XFA relay plan contains execution or authoritative-write activity"
        )
    expected_transitions = set(plan.get("combine_transition_hashes") or ())
    if (
        len(expected_transitions) != int(plan.get("combine_transition_count", -1))
        or not expected_transitions
    ):
        raise GraduatedXfaRelayError("XFA Combine-transition inventory drift")
    observed_by_path: dict[str, set[str]] = {}
    for path in _XFA_PATHS:
        runs = alternatives[path]
        if not isinstance(runs, list) or len(runs) != len(expected_transitions):
            raise GraduatedXfaRelayError(
                f"XFA {path} alternative-run count drift"
            )
        observed: set[str] = set()
        for run in runs:
            if not isinstance(run, Mapping):
                raise GraduatedXfaRelayError(f"XFA {path} run is not an object")
            core = dict(run)
            claimed = str(core.pop("run_hash", ""))
            if (
                not claimed
                or stable_hash(core) != claimed
                or run.get("schema") != RUN_SPEC_SCHEMA
                or run.get("xfa_path") != path
                or run.get("simulation_started") is not False
                or int(run.get("broker_connections", -1)) != 0
                or int(run.get("orders", -1)) != 0
                or run.get("alternative_path_not_additive") is not True
                or str(run.get("account_label")) != str(plan.get("account_label"))
                or int(run.get("account_size_usd", -1))
                != int(plan.get("account_size_usd", -2))
                or str(run.get("candidate_id")) != str(plan.get("candidate_id"))
                or str(run.get("combine_book_hash"))
                != str(plan.get("combine_book_hash"))
                or str(run.get("xfa_book_hash"))
                != str(plan.get("xfa_book_hash"))
                or str(run.get("xfa_profile_hash"))
                != str(plan.get("xfa_profile_hash"))
                or str(run.get("rule_snapshot_file_sha256"))
                != str(snapshot_binding["snapshot_file_sha256"])
                or str(run.get("parsed_rule_hash"))
                != str(snapshot_binding["parsed_rule_hash"])
                or str(run.get("xfa_account_rule_hash"))
                != str(snapshot_binding["xfa_account_rule_hash"])
                or str(run.get("executable_rule_fingerprint"))
                != str(snapshot_binding["executable_rule_fingerprint"])
            ):
                raise GraduatedXfaRelayError(f"XFA {path} run identity drift")
            transition = str(run.get("combine_transition_hash") or "")
            if transition in observed:
                raise GraduatedXfaRelayError(
                    f"XFA {path} duplicates one Combine transition"
                )
            observed.add(transition)
        if observed != expected_transitions:
            raise GraduatedXfaRelayError(
                f"XFA {path} transition coverage drift"
            )
        observed_by_path[path] = observed
    if observed_by_path["STANDARD"] != observed_by_path["CONSISTENCY"]:
        raise GraduatedXfaRelayError("XFA alternative-path separation drift")


def _verified_graduation_receipt(value: Mapping[str, Any]) -> dict[str, Any]:
    receipt = dict(value)
    claimed = str(receipt.pop("result_hash", ""))
    if not claimed or stable_hash(receipt) != claimed:
        raise GraduatedXfaRelayError("graduation receipt hash drift")
    if (
        value.get("evidence_tier") != "G"
        or value.get("graduation_status") != "GRADUATED_DEVELOPMENT_BOOK"
        or value.get("tier_g_gate_cleared") is not True
        or value.get("frozen_before_xfa") is not True
        or value.get("promotion_status") not in {None, "TIER_G"}
    ):
        raise GraduatedXfaRelayError("XFA relay accepts only a frozen Tier-G book")
    required_hashes = (
        "combine_book_hash",
        "xfa_book_hash",
        "xfa_profile_hash",
        "graduation_evidence_hash",
    )
    if any(not _is_sha256(value.get(name)) for name in required_hashes):
        raise GraduatedXfaRelayError("graduated book provenance is incomplete")
    if not str(value.get("candidate_id") or ""):
        raise GraduatedXfaRelayError("graduated candidate ID is absent")
    markets = value.get("markets")
    if not isinstance(markets, list) or not markets or any(
        not str(item).strip() for item in markets
    ):
        raise GraduatedXfaRelayError("graduated book market inventory is absent")
    return dict(value)


def _verified_combine_paths(
    values: Sequence[Mapping[str, Any]], graduation: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if not values:
        raise GraduatedXfaRelayError(
            "XFA relay requires at least one immutable successful Combine path"
        )
    verified: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in values:
        row = dict(raw)
        claimed = str(row.pop("path_hash", ""))
        if not claimed or stable_hash(row) != claimed:
            raise GraduatedXfaRelayError("Combine path identity/hash drift")
        if (
            raw.get("immutable") is not True
            or raw.get("combine_status") != "TARGET_REACHED"
            or raw.get("passed") is not True
            or str(raw.get("candidate_id")) != str(graduation["candidate_id"])
            or str(raw.get("combine_book_hash"))
            != str(graduation["combine_book_hash"])
            or str(raw.get("account_label")) != str(graduation["account_label"])
            or int(raw.get("account_size_usd", -1))
            != int(graduation["account_size_usd"])
            or not _is_sha256(raw.get("source_ledger_hash"))
            or not _is_sha256(raw.get("combine_evidence_hash"))
        ):
            raise GraduatedXfaRelayError(
                "XFA relay received a non-passing, mutable, or mismatched Combine path"
            )
        if claimed in seen:
            raise GraduatedXfaRelayError("duplicate immutable Combine transition")
        seen.add(claimed)
        verified.append(dict(raw))
    return sorted(verified, key=lambda row: str(row["path_hash"]))


def _load_official_snapshot(
    path: Path,
    *,
    account_label: str,
    account_size: int,
    markets: Sequence[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved = path.resolve()
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise GraduatedXfaRelayError("official XFA snapshot is not an object")
    fields = payload.get("parsed_rule_fields")
    if not isinstance(fields, list) or not fields or any(
        key not in payload for key in fields
    ):
        raise GraduatedXfaRelayError(
            "official XFA parsed-field inventory is incomplete"
        )
    parsed = {str(key): payload[str(key)] for key in fields}
    parsed_hash = str(payload.get("parsed_rule_hash") or "")
    if (
        payload.get("schema") != "hydra_topstep_official_rule_snapshot_v1"
        or payload.get("retrieval_status") != "OFFICIAL_CURRENT_SNAPSHOT"
        or stable_hash(parsed) != parsed_hash
        or payload.get("interpretation_boundary")
        != (
            "Research simulation only. No broker connection, order route, "
            "or real trading authorization."
        )
    ):
        raise GraduatedXfaRelayError("official XFA snapshot identity/status drift")
    sources = payload.get("sources")
    source_provenance_issues: list[str] = []
    if not isinstance(sources, list) or not sources:
        source_provenance_issues.append("OFFICIAL_SOURCE_INVENTORY_ABSENT")
    else:
        for index, row in enumerate(sources):
            if not isinstance(row, Mapping):
                source_provenance_issues.append(
                    f"OFFICIAL_SOURCE_{index}_NOT_AN_OBJECT"
                )
                continue
            if not str(row.get("url", "")).startswith("https://help.topstep.com/"):
                source_provenance_issues.append(
                    f"OFFICIAL_SOURCE_{index}_URL_INVALID"
                )
            if not _is_sha256(row.get("document_sha256")):
                source_provenance_issues.append(
                    f"OFFICIAL_SOURCE_{index}_DOCUMENT_SHA256_INVALID"
                )
    combine = dict(payload.get("combine") or {}).get(account_label)
    xfa = dict(payload.get("xfa") or {})
    if not isinstance(combine, Mapping) or int(
        combine.get("account_size_usd", -1)
    ) != account_size:
        raise GraduatedXfaRelayError("official account-size rules are absent")
    _require_xfa_account_fields(xfa, account_label)

    binding: dict[str, Any] = {
        "snapshot_id": payload.get("snapshot_id"),
        "retrieved_at_utc": payload.get("retrieved_at_utc"),
        "snapshot_file_sha256": _sha256(resolved),
        "parsed_rule_hash": parsed_hash,
        "account_label": account_label,
        "account_size_usd": account_size,
        "combine_rule_hash": stable_hash(combine),
        "xfa_account_rule_hash": stable_hash(
            _xfa_account_projection(xfa, account_label)
        ),
        "source_provenance_status": (
            "VERIFIED"
            if not source_provenance_issues
            else "INVALID_FAIL_CLOSED"
        ),
        "source_provenance_issues": list(source_provenance_issues),
        "outbound_order_capability": False,
    }
    if account_label != "150K":
        return payload, {
            **binding,
            "executable_rule_status": "UNVERIFIED_EXECUTABLE_ACCOUNT_SIZE",
            "blocked_reason_code": (
                f"NO_VERSIONED_EXECUTABLE_{account_label}_XFA_RULE_SNAPSHOT"
            ),
            "blocked_reasons": [
                (
                    "The persisted official snapshot parses commercial 50K/100K "
                    "XFA values, but the versioned chronological lifecycle engine "
                    "accepts only 150K."
                ),
                (
                    "Winning-day lock/session attribution and inactivity-close "
                    "semantics are not bound to an executable 50K/100K "
                    "RuleSnapshot."
                ),
                (
                    "Account-specific restricted-market scaling across balance "
                    "tiers is not fully represented for 50K/100K."
                ),
                *source_provenance_issues,
            ],
        }

    if source_provenance_issues:
        return payload, {
            **binding,
            "executable_rule_status": "UNVERIFIED_OFFICIAL_PROVENANCE",
            "blocked_reason_code": (
                "OFFICIAL_RULE_SNAPSHOT_SOURCE_PROVENANCE_INVALID"
            ),
            "blocked_reasons": list(source_provenance_issues),
        }

    engine = official_rule_snapshot_2026_07_15()
    _verify_150k_engine_binding(payload, engine, markets)
    return payload, {
        **binding,
        "executable_rule_status": "VERIFIED_EXECUTABLE_150K",
        "executable_rule_version": engine.rule_version,
        "executable_rule_fingerprint": engine.fingerprint,
        "blocked_reason_code": None,
        "blocked_reasons": [],
    }


def _verify_150k_engine_binding(
    payload: Mapping[str, Any], engine: Any, markets: Sequence[str]
) -> None:
    combine = dict(dict(payload["combine"])["150K"])
    xfa = dict(payload["xfa"])
    standard = dict(xfa["standard"])
    consistency = dict(xfa["consistency"])
    expected = (
        float(combine["account_size_usd"]),
        float(combine["profit_target_usd"]),
        float(combine["maximum_loss_limit_usd"]),
        int(combine["maximum_mini_contracts"]),
        int(combine["maximum_micro_contracts"]),
        float(combine["consistency_target_fraction"]),
        int(combine["minimum_trading_days"]),
        float(xfa["starting_balance_usd"]),
        float(dict(xfa["starting_mll_by_account_usd"])["150K"]),
        int(standard["winning_days_required"]),
        float(standard["winning_day_minimum_usd"]),
        float(standard["later_cycle_profit_minimum_usd"]),
        float(standard["payout_balance_fraction_cap"]),
        float(standard["payout_request_minimum_usd"]),
        float(dict(standard["payout_cap_by_account_usd"])["150K"]),
        int(consistency["minimum_traded_days"]),
        float(consistency["largest_day_fraction"]),
        bool(consistency["consistency_resets_after_payout"]),
        float(dict(consistency["payout_cap_by_account_usd"])["150K"]),
        float(xfa["profit_split_trader_fraction"]),
        float(xfa["mll_floor_after_first_payout_usd"]),
    )
    observed = (
        engine.account_size,
        engine.combine_profit_target,
        engine.maximum_loss_limit,
        engine.combine_maximum_mini_equivalent,
        engine.combine_maximum_micros,
        engine.combine_consistency_limit,
        engine.combine_minimum_days,
        engine.xfa_starting_balance,
        engine.xfa_starting_floor,
        engine.xfa_standard_winning_days,
        engine.xfa_standard_winning_day_minimum,
        engine.later_standard_cycle_minimum_profit,
        engine.payout_fraction,
        engine.minimum_payout,
        engine.standard_payout_cap,
        engine.xfa_consistency_traded_days,
        engine.xfa_consistency_limit,
        True,  # The executable path resets all consistency-cycle accumulators.
        engine.consistency_payout_cap,
        engine.trader_profit_split,
        0.0,  # The executable ledger locks the MLL floor at zero after payout.
    )
    if expected != observed:
        raise GraduatedXfaRelayError(
            "official 150K snapshot differs from executable lifecycle rules"
        )
    official_tiers = tuple(
        (float(balance), float(limit))
        for balance, limit in dict(xfa["scaling_plan_mini_contracts"])["150K"]
    )
    executable_tiers = tuple(
        (max(float(balance), 0.0), float(limit))
        for balance, limit in engine.xfa_scaling_tiers
    )
    if official_tiers != executable_tiers:
        raise GraduatedXfaRelayError(
            "official 150K scaling plan differs from executable lifecycle rules"
        )
    roots = {_market_root(value) for value in markets}
    if roots & _RESTRICTED_MARKET_ROOTS:
        raise GraduatedXfaRelayError(
            "restricted-market XFA balance-tier scaling is not fully verified "
            "in the official parsed snapshot"
        )


def _require_xfa_account_fields(xfa: Mapping[str, Any], account_label: str) -> None:
    standard = dict(xfa.get("standard") or {})
    consistency = dict(xfa.get("consistency") or {})
    required = (
        xfa.get("starting_balance_usd"),
        dict(xfa.get("starting_mll_by_account_usd") or {}).get(account_label),
        xfa.get("mll_floor_after_first_payout_usd"),
        xfa.get("profit_split_trader_fraction"),
        standard.get("winning_days_required"),
        standard.get("winning_day_minimum_usd"),
        standard.get("later_cycle_profit_minimum_usd"),
        standard.get("payout_balance_fraction_cap"),
        standard.get("payout_request_minimum_usd"),
        dict(standard.get("payout_cap_by_account_usd") or {}).get(account_label),
        consistency.get("minimum_traded_days"),
        consistency.get("largest_day_fraction"),
        consistency.get("consistency_resets_after_payout"),
        dict(consistency.get("payout_cap_by_account_usd") or {}).get(account_label),
        dict(xfa.get("scaling_plan_mini_contracts") or {}).get(account_label),
    )
    if any(value is None for value in required):
        raise GraduatedXfaRelayError(
            f"official {account_label} XFA field inventory is incomplete"
        )


def _xfa_account_projection(
    xfa: Mapping[str, Any], account_label: str
) -> dict[str, Any]:
    standard = dict(xfa["standard"])
    consistency = dict(xfa["consistency"])
    return {
        "starting_balance_usd": xfa["starting_balance_usd"],
        "starting_mll_usd": dict(xfa["starting_mll_by_account_usd"])[
            account_label
        ],
        "mll_floor_after_first_payout_usd": xfa[
            "mll_floor_after_first_payout_usd"
        ],
        "profit_split_trader_fraction": xfa["profit_split_trader_fraction"],
        "standard": {
            **{
                key: value
                for key, value in standard.items()
                if key != "payout_cap_by_account_usd"
            },
            "payout_cap_usd": dict(standard["payout_cap_by_account_usd"])[
                account_label
            ],
        },
        "consistency": {
            **{
                key: value
                for key, value in consistency.items()
                if key != "payout_cap_by_account_usd"
            },
            "payout_cap_usd": dict(consistency["payout_cap_by_account_usd"])[
                account_label
            ],
        },
        "scaling_plan_mini_contracts": dict(
            xfa["scaling_plan_mini_contracts"]
        )[account_label],
    }


def _run_spec(
    *,
    graduation: Mapping[str, Any],
    combine_path: Mapping[str, Any],
    xfa_path: str,
    snapshot_binding: Mapping[str, Any],
) -> dict[str, Any]:
    core = {
        "schema": RUN_SPEC_SCHEMA,
        "candidate_id": graduation["candidate_id"],
        "combine_transition_hash": combine_path["path_hash"],
        "combine_start_id": combine_path["combine_start_id"],
        "combine_book_hash": graduation["combine_book_hash"],
        "xfa_book_hash": graduation["xfa_book_hash"],
        "xfa_profile_hash": graduation["xfa_profile_hash"],
        "xfa_path": xfa_path,
        "account_label": graduation["account_label"],
        "account_size_usd": graduation["account_size_usd"],
        "rule_snapshot_file_sha256": snapshot_binding[
            "snapshot_file_sha256"
        ],
        "parsed_rule_hash": snapshot_binding["parsed_rule_hash"],
        "xfa_account_rule_hash": snapshot_binding["xfa_account_rule_hash"],
        "executable_rule_fingerprint": snapshot_binding[
            "executable_rule_fingerprint"
        ],
        "immutable_successful_combine_required": True,
        "alternative_path_not_additive": True,
        "simulation_started": False,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "run_hash": stable_hash(core)}


def _market_root(value: str) -> str:
    text = str(value).upper().lstrip("/")
    # Fail closed for both root symbols (CL) and explicit futures contracts
    # (CLZ6/MCLZ6).  Longest-first prevents the micro roots from being reduced
    # to their parent prefix by accident.
    for root in sorted(_RESTRICTED_MARKET_ROOTS, key=len, reverse=True):
        if text.startswith(root):
            return root
    return "".join(character for character in text if character.isalpha())


def _is_sha256(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(
        character in "0123456789abcdef" for character in text
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "DEFAULT_RULE_SNAPSHOT",
    "GraduatedXfaRelayError",
    "RUN_SPEC_SCHEMA",
    "SCHEMA",
    "assert_graduated_xfa_relay_ready",
    "prepare_graduated_xfa_relay",
]
