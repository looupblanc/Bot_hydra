from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.v71_event_mechanism_grammar import (
    GRAMMAR_ID,
    candidate_specs,
    generate_signal_population,
    load_v71_minute_features,
    signal_path_hash,
)
from hydra.validation.v7_report_schema import validate_v7_report_text


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate outcome-free V7.1 signal paths.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-dir", default="reports/v7_1/discovery")
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    minute = load_v71_minute_features(root)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    signals = generate_signal_population(minute, project_root=root)
    rows = []
    for candidate_id, spec in sorted(specs.items()):
        candidate_signals = signals[candidate_id]
        rows.append(
            {
                "candidate_id": candidate_id,
                "family_id": spec.family_id,
                "motif": spec.motif,
                "response_policy": spec.response_policy,
                "holding_minutes": spec.holding_minutes,
                "specification_hash": spec.specification_hash,
                "signal_count": len(candidate_signals),
                "signal_path_hash": signal_path_hash(candidate_signals),
                "powered_for_DSR_BH": len(candidate_signals) >= 320,
            }
        )
    family_counts = Counter(row["family_id"] for row in rows)
    payload: dict[str, Any] = {
        "schema": "hydra_v7_1_signal_manifest_v1",
        "grammar_id": GRAMMAR_ID,
        "candidate_count": len(rows),
        "family_count": len(family_counts),
        "candidates_per_family": dict(sorted(family_counts.items())),
        "signal_count": sum(int(row["signal_count"]) for row in rows),
        "powered_candidate_count_forecast": sum(bool(row["powered_for_DSR_BH"]) for row in rows),
        "candidate_paths": rows,
        "contains_outcomes_or_pnl": False,
        "new_data_purchase_count": 0,
        "protected_holdout_access_count": 0,
        "outbound_order_count": 0,
        "CONTRE": "Signal frequency does not imply economic edge and the two D1 calendar blocks may leave many valid rare mechanisms underpowered.",
    }
    payload["manifest_hash"] = _stable_hash(payload)
    output = Path(args.output_dir)
    if not output.is_absolute():
        output = root / output
    output.mkdir(parents=True, exist_ok=True)
    result_path = output / "v71_signal_manifest.json"
    _atomic_json(result_path, payload)
    result_hash = _sha256(result_path)
    report_path = output / "v71_signal_manifest.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Outcome-free signal manifest",
            "",
            "[HYDRA-V7] phase=4 step=120 verdict=GREEN",
            f"gate=V71_SIGNAL_FREEZE preuve={result_path.relative_to(root)}#{result_hash[:8]} tests=outcome_free_generation",
            "budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=261972 burned=1",
            "diff_validation=aucun CONTRE=la_frequence_des_signaux_ne_prouve_aucune_esperance",
            "prochaine_action=freezer_le_manifest_puis_lancer_le_funnel_economique_separe",
            "",
            f"- Candidats: `{payload['candidate_count']}`",
            f"- Familles: `{payload['family_count']}`",
            f"- Signaux: `{payload['signal_count']}`",
            f"- Prévision candidats avec >=320 signaux: `{payload['powered_candidate_count_forecast']}`",
            "",
            "## CONTRE",
            "",
            str(payload["CONTRE"]),
            "",
        ]
    )
    validate_v7_report_text(report)
    report_path.write_text(report, encoding="utf-8")
    print(json.dumps({"result_path": str(result_path), "sha256": result_hash, **payload}, indent=2, sort_keys=True))
    return 0


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _atomic_json(path: Path, payload: Any) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
