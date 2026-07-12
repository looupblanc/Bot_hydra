from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from hydra.governance.q4_one_shot import AuthorizedQ4Capability
from hydra.promotion.final_cohort import stable_hash
from hydra.validation.q4_market_evaluator import evaluate_q4_frames


def _frame(market: str, *, slope: float) -> pd.DataFrame:
    rows = []
    for day in pd.bdate_range("2024-10-07", "2024-10-31", tz="UTC"):
        start_hour = 13 if market == "NQ" else 13
        start_minute = 30 if market == "NQ" else 0
        timestamps = pd.date_range(
            day + pd.Timedelta(hours=start_hour, minutes=start_minute),
            periods=390 if market == "NQ" else 330,
            freq="1min",
        )
        base = 100.0 + np.arange(len(timestamps)) * slope
        rows.append(
            pd.DataFrame(
                {
                    "timestamp": timestamps,
                    "symbol": market,
                    "active_contract": f"{market}:iid:1",
                    "open": base,
                    "high": base + abs(slope),
                    "low": base - abs(slope),
                    "close": base + slope * 0.5,
                    "volume": 1000.0 + np.arange(len(timestamps)),
                }
            )
        )
    return pd.concat(rows, ignore_index=True)


def _spec(candidate_id: str, market: str, role: int) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "family": f"family_{candidate_id}",
        "lineage_id": f"lineage_{candidate_id}",
        "market": market,
        "feature": "past_return_60",
        "operator": 2,
        "threshold": -1.0,
        "context_feature": "ctx_30m_return",
        "context_operator": 2,
        "context_threshold": -1.0,
        "holding_events": 5,
        "side": 1,
        "session_code": 0,
        "quantity": 1,
        "point_value": 20.0 if market == "NQ" else 1000.0,
        "round_turn_cost": 14.5 if market == "NQ" else 24.5,
        "timeframe": "1m|30m",
        "role": role,
        "version": 1,
    }


def test_q4_frame_evaluator_replays_frozen_specs_with_closed_features() -> None:
    roles = [
        ("alpha", "NQ", "MNQ", "COMBINE_PASSER", 2),
        ("payout", "CL", "MCL", "XFA_PAYOUT", 3),
        ("defensive", "NQ", "MNQ", "DEFENSIVE", 4),
    ]
    candidates = []
    for candidate_id, market, execution, role, role_code in roles:
        spec = _spec(candidate_id, market, role_code)
        candidates.append(
            {
                "candidate_id": candidate_id,
                "role": role,
                "primary_market": market,
                "execution_market": execution,
                "specification": spec,
                "specification_hash": hashlib.sha256(
                    __import__("json").dumps(
                        spec, sort_keys=True, separators=(",", ":")
                    ).encode()
                ).hexdigest(),
                "selected_micro_contracts": 2,
            }
        )
    manifest = {
        "cohort_id": "q4_eval_test",
        "candidate_ids": [row[0] for row in roles],
        "candidates": candidates,
        "q4_decision_policy": {
            "minimum_executable_events": 5,
            "maximum_best_day_positive_pnl_fraction": 0.50,
            "minimum_xfa_qualifying_days": 2,
            "maximum_defensive_target_velocity_loss_fraction": 0.25,
            "maximum_defensive_matched_control_probability": 0.10,
            "minimum_defensive_control_count": 32,
        },
    }
    manifest["manifest_hash"] = stable_hash(manifest)
    token_id = "token"
    commit = "a" * 40
    marker = hashlib.sha256(
        f"{token_id}:{manifest['manifest_hash']}:{commit}".encode()
    ).hexdigest()
    capability = AuthorizedQ4Capability(
        token_id=token_id,
        cohort_id="q4_eval_test",
        cohort_manifest_hash=manifest["manifest_hash"],
        source_commit=commit,
        consumption_path=str(Path("/tmp/unused-consumption")),
        _scope_marker=marker,
    )
    results = evaluate_q4_frames(
        manifest,
        capability,
        {"NQ": _frame("NQ", slope=0.5), "CL": _frame("CL", slope=0.02)},
    )
    assert {row["candidate_id"] for row in results} == {
        "alpha",
        "payout",
        "defensive",
    }
    assert all(row["metrics"]["events"] >= 5 for row in results)
    assert all(row["parameters_mutated"] is False for row in results)
    assert all(row["classification"].startswith("Q4_LOCKBOX_") for row in results)
