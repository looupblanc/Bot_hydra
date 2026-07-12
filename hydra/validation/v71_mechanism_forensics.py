from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from hydra.research.v71_event_mechanism_grammar import load_v71_minute_features
from hydra.validation.v7_report_schema import validate_v7_report_text


D1_G1_PATH = "reports/v7/data/d1_candidate_tribunal_result.json"
D1_G1_SHA256 = "fcdb9477d00c0bfa80e77ac414892fdfc9b2ebd854f19296f221f172d7ca2203"
D1_G2_PATH = "reports/v7/data/d1_grammar0002_candidate_tribunal_result.json"
D1_G2_SHA256 = "a5b6c8ac1073503f26a2ed82600bdcc6a12d873dde3611c081a2c0bd4249fc4f"
V71_PATH = "reports/v7_1/discovery/v71_development_funnel_result.json"
V71_SHA256 = "b8767eb9a2c5a8f9ef7c85d640cf5b1368f2607f49da3cc0b0c9a92a73f16fe2"


class V71MechanismForensicsError(RuntimeError):
    pass


def run_v71_mechanism_forensics(
    *,
    project_root: str | Path = ".",
    output_dir: str | Path = "reports/v7_1/forensics",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    frozen = {
        D1_G1_PATH: D1_G1_SHA256,
        D1_G2_PATH: D1_G2_SHA256,
        V71_PATH: V71_SHA256,
    }
    drift = [path for path, sha in frozen.items() if _sha256(root / path) != sha]
    if drift:
        raise V71MechanismForensicsError("frozen forensic input drift: " + ",".join(drift))
    g1 = _json(root / D1_G1_PATH)
    g2 = _json(root / D1_G2_PATH)
    v71 = _json(root / V71_PATH)
    load_v71_minute_features(root)
    minute = pd.read_parquet(
        root / "data/cache/v7_d1/date_matched_minute_print_features_v2.parquet"
    )
    extreme = next(
        row
        for row in g2["candidate_results"]
        if row["candidate_id"] == "v7d1g2_delta_extreme_rejection_ES"
    )
    divergence = next(
        row
        for row in g2["candidate_results"]
        if row["candidate_id"]
        == "v7d1g2_cross_contract_participation_divergence_ES"
    )
    extreme_v71 = [
        row
        for row in v71["candidate_results"]
        if row["family_id"] == "EXTREME_ACCEPTANCE_REJECTION"
    ]
    result = {
        "schema": "hydra_v7_1_mechanism_forensics_result_v1",
        "D1_0001": {
            "historical_verdict": g1["verdict"],
            "exact_formulation_classification": "FORMULATION_FALSIFIED",
            "mechanism_classification": "MECHANISM_REFORMULATION_ALLOWED",
            "reason": "One Stage-1 survivor and zero Stage-2 survivors under frozen costs; no mechanism-level transfer was established.",
            "status_resurrected": False,
        },
        "D1_0002": {
            "historical_verdict": g2["verdict"],
            "exact_formulation_classification": "FORMULATION_FALSIFIED",
            "mechanism_classification": "MECHANISM_WEAK_SIGNAL",
            "validator_calibration_affected": False,
            "reason": "The two best exact candidates failed walk-forward before DSR/BH, so the historical global-trial defect did not cause their rejection.",
            "status_resurrected": False,
        },
        "ES_EXTREME_REJECTION": _extreme_audit(extreme, extreme_v71),
        "MINI_MICRO_DIVERGENCE": _mini_micro_audit(minute, divergence),
        "v71_underpowered_positive_count": int(v71["walk_forward_positive_count"]),
        "v71_powered_positive_count": int(v71["powered_walk_forward_candidate_count"]),
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "The artifact and fold diagnostics use only frozen development "
            "evidence; they cannot tell whether an economically distinct future "
            "mechanism will survive fresh confirmation."
        ),
        "next_action": "preserve_11_underpowered_mechanisms_and_expand_opportunity_density_without_parameter_tuning",
    }
    return _write(result, root, Path(output_dir))


def _extreme_audit(
    row: Mapping[str, Any], v71_rows: list[Mapping[str, Any]]
) -> dict[str, Any]:
    folds = list(row["walk_forward"]["folds"])
    return {
        "candidate_id": row["candidate_id"],
        "raw_stress_1_5x_expectancy_per_trade": float(
            row["stress_1_5x"]["expectancy_per_trade"]
        ),
        "walk_forward_expectancy_per_trade": float(
            row["walk_forward"]["pooled_expectancy_per_trade"]
        ),
        "walk_forward_event_count": int(row["walk_forward"]["retained_event_count"]),
        "positive_fold_count": sum(float(fold["expectancy_per_trade"]) > 0.0 for fold in folds),
        "folds": folds,
        "year_results_stress_1_5x": row["year_results_stress_1_5x"],
        "exact_formulation": "FORMULATION_FALSIFIED",
        "mechanism": "MECHANISM_REFORMULATION_ALLOWED",
        "validator_calibration_affected": False,
        "v71_extreme_family_candidate_count": len(v71_rows),
        "v71_extreme_family_stage1_pass_count": sum(
            bool(item["stage1_pass"]) for item in v71_rows
        ),
        "v71_extreme_family_walk_forward_positive_count": sum(
            bool(item["walk_forward_positive"]) for item in v71_rows
        ),
        "bounded_future_reformulations_maximum": 3,
        "bounded_future_reformulations": [
            {
                "new_mechanism": "event_extreme_rejection_with_participation_acceleration",
                "structural_change": "Require a past-only acceleration state before the rejection geometry, not a new threshold grid.",
                "transfer_hypothesis": "Acceleration separates urgent tests from passive low-information touches.",
            },
            {
                "new_mechanism": "event_extreme_recovery_speed_reversal",
                "structural_change": "Replace the static extreme flag with completed recovery-speed geometry.",
                "transfer_hypothesis": "Fast recovery identifies exhaustion more invariantly than the raw extreme level.",
            },
            {
                "new_mechanism": "event_extreme_acceptance_delayed_continuation",
                "structural_change": "Require dwell and value migration beyond the extreme before delayed continuation.",
                "transfer_hypothesis": "Acceptance and rejection are distinct mechanisms and should not share one immediate response rule.",
            },
        ],
        "reformulations_executed_in_this_audit": 0,
    }


def _mini_micro_audit(
    minute: pd.DataFrame, row: Mapping[str, Any]
) -> dict[str, Any]:
    source = minute.sort_values(["minute_start_ns", "product"], kind="stable")
    es = source[source["product"] == "ES"].set_index("minute_start_ns")
    mes = source[source["product"] == "MES"].set_index("minute_start_ns")
    common = es.index.intersection(mes.index)
    aligned = len(common) / max(len(es.index.union(mes.index)), 1)
    es_common = es.loc[common]
    mes_common = mes.loc[common]
    contract_suffix_match = np.mean(
        [
            str(left)[2:] == str(right)[3:]
            for left, right in zip(
                es_common["contract"], mes_common["contract"], strict=True
            )
        ]
    )
    first_lag_ms = np.abs(
        es_common["first_trade_ns"].to_numpy(np.int64)
        - mes_common["first_trade_ns"].to_numpy(np.int64)
    ) / 1_000_000.0
    last_lag_ms = np.abs(
        es_common["last_trade_ns"].to_numpy(np.int64)
        - mes_common["last_trade_ns"].to_numpy(np.int64)
    ) / 1_000_000.0
    volume_ratio = np.divide(
        es_common["total_volume"].to_numpy(float),
        mes_common["total_volume"].to_numpy(float),
        out=np.full(len(common), np.nan),
        where=mes_common["total_volume"].to_numpy(float) > 0.0,
    )
    return {
        "candidate_id": row["candidate_id"],
        "raw_stress_1_5x_expectancy_per_trade": float(
            row["stress_1_5x"]["expectancy_per_trade"]
        ),
        "walk_forward_expectancy_per_trade": float(
            row["walk_forward"]["pooled_expectancy_per_trade"]
        ),
        "year_results_stress_1_5x": row["year_results_stress_1_5x"],
        "timestamp_intersection_fraction": float(aligned),
        "contract_expiry_suffix_match_fraction": float(contract_suffix_match),
        "first_trade_absolute_lag_ms_median": float(np.median(first_lag_ms)),
        "last_trade_absolute_lag_ms_median": float(np.median(last_lag_ms)),
        "ES_to_MES_volume_ratio_median": float(np.nanmedian(volume_ratio)),
        "liquidity_normalization_used_by_original": "signed_aggressor_fraction",
        "reporting_latency_directly_observable": False,
        "operational_class": "ARB_INTRA_PRODUIT",
        "exact_formulation": "FORMULATION_FALSIFIED",
        "mechanism": "MECHANISM_CONFIRMED_DEAD",
        "death_reason": "Strong year sign reversal plus non-capturable intra-product latency/stale-information risk under R14.",
        "reformulation_allowed": False,
    }


def _write(result: dict[str, Any], root: Path, output_dir: Path) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_mechanism_forensics_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    report_path = destination / "v71_mechanism_forensics_report.md"
    proof_path = (
        result_path.relative_to(root) if result_path.is_relative_to(root) else result_path
    )
    report = "\n".join(
        [
            "# HYDRA V7.1 — Mechanism forensics",
            "",
            "[HYDRA-V7] phase=4 step=122 verdict=NULL",
            f"gate=V71_FORENSICS preuve={proof_path}#{result_hash[:8]} tests=diagnostic_sans_nouveau_gate",
            "budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=262228 burned=1",
            "diff_validation=hydra/validation/v71_mechanism_forensics.py CONTRE=un_diagnostic_developpement_ne_prouve_pas_la_generalite",
            f"prochaine_action={result['next_action']}",
            "",
            f"- D1-0001: `{result['D1_0001']['exact_formulation_classification']}` / `{result['D1_0001']['mechanism_classification']}`",
            f"- D1-0002: `{result['D1_0002']['exact_formulation_classification']}` / `{result['D1_0002']['mechanism_classification']}`",
            f"- Extreme ES WF: `{result['ES_EXTREME_REJECTION']['walk_forward_expectancy_per_trade']}` USD/trade",
            f"- Mini/micro: `{result['MINI_MICRO_DIVERGENCE']['mechanism']}`",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )
    validate_v7_report_text(report)
    report_path.write_text(report, encoding="utf-8")
    result["result_path"] = str(result_path)
    result["result_sha256"] = result_hash
    result["report_path"] = str(report_path)
    return result


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = ["V71MechanismForensicsError", "run_v71_mechanism_forensics"]
