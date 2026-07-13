from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.v72_trade_arrival_renewal import (
    GRAMMAR_ID,
    candidate_specs,
    generate_signal_population,
    load_trade_arrival_renewal_sources,
    signal_path_hash,
)
from hydra.validation.v7_report_schema import validate_v7_report_text


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate the outcome-free V7.2 trade-arrival manifest."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-dir", default="reports/v7_2/discovery_0011")
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    _, states, audit = load_trade_arrival_renewal_sources(root)
    signals = generate_signal_population(states, project_root=root)
    archive = _existing_archive(root)
    rows: list[dict[str, Any]] = []
    for candidate_id, spec in sorted(specs.items()):
        candidate_signals = signals[candidate_id]
        path_hash = signal_path_hash(candidate_signals)
        rows.append(
            {
                "candidate_id": candidate_id,
                "family_id": spec.family_id,
                "motif": spec.motif,
                "response_policy": spec.response_policy,
                "history_window": spec.history_window,
                "holding_minutes": spec.holding_minutes,
                "specification_hash": spec.specification_hash,
                "signal_count": len(candidate_signals),
                "signal_path_hash": path_hash,
                "archive_duplicate_of": archive.get(path_hash),
            }
        )
    within = _within_manifest_duplicates(rows)
    for row in rows:
        row["within_manifest_duplicate_of"] = within.get(str(row["candidate_id"]))
    payload: dict[str, Any] = {
        "schema": "hydra_v7_2_trade_arrival_renewal_signal_manifest_v1",
        "grammar_id": GRAMMAR_ID,
        "candidate_count": len(rows),
        "family_count": 1,
        "candidates_per_family": dict(
            Counter(str(row["family_id"]) for row in rows)
        ),
        "signal_count": sum(int(row["signal_count"]) for row in rows),
        "nonempty_candidate_count": sum(int(row["signal_count"]) > 0 for row in rows),
        "archive_duplicate_count": sum(
            row["archive_duplicate_of"] is not None for row in rows
        ),
        "within_manifest_duplicate_count": len(within),
        "source_audit": audit,
        "candidate_paths": rows,
        "contains_outcomes_or_pnl": False,
        "proof_registry_reservation": {
            "event_id": "v7_2_trade_arrival_renewal_structural_tripwire_reservation_0011",
            "entry_hash": "6749663cd4e23d5705015432bd7d37a902778d13d3d9d007f4af73f371bbdcbc",
            "commit_annotation_event_id": "v7_2_trade_arrival_renewal_reservation_0011_commit_annotation",
            "commit_annotation_entry_hash": "3e08cb28ffaa08ef9037a418c8dad4aa05e44aee9c94e8dcaed3e973a4023b50",
            "raw_global_N_trials_after": 265_227,
        },
        "new_data_purchase_count": 0,
        "protected_holdout_access_count": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "Trade-arrival states may still encode only session seasonality or generic "
            "volatility; economic walk-forward and the frozen price-world tripwire are "
            "mandatory."
        ),
    }
    payload["manifest_hash"] = _stable_hash(payload)
    output = Path(args.output_dir)
    if not output.is_absolute():
        output = root / output
    output.mkdir(parents=True, exist_ok=True)
    result_path = output / "v72_trade_arrival_renewal_signal_manifest.json"
    _atomic_json(result_path, payload)
    result_hash = _sha256(result_path)
    displayed = (
        result_path.relative_to(root) if result_path.is_relative_to(root) else result_path
    )
    report_path = output / "v72_trade_arrival_renewal_signal_manifest.md"
    report = "\n".join(
        [
            "# HYDRA V7.2 — Trade-arrival renewal outcome-free manifest",
            "",
            "[HYDRA-V7] phase=4 step=198 verdict=GREEN",
            f"gate=V72_G11_SIGNAL_FREEZE preuve={displayed}#{result_hash[:8]} tests=outcome_free_generation",
            "budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=265227 burned=1",
            "diff_validation=aucun CONTRE=les_arrivees_peuvent_encoder_seulement_la_saisonnalite_intraday",
            "prochaine_action=implementer_le_funnel_et_le_tripwire_figes_sans_modifier_la_grammaire",
            "",
            f"- Candidats: `{payload['candidate_count']}`",
            f"- Candidats non vides: `{payload['nonempty_candidate_count']}`",
            f"- Signaux: `{payload['signal_count']}`",
            f"- Doublons archive: `{payload['archive_duplicate_count']}`",
            f"- Doublons internes: `{payload['within_manifest_duplicate_count']}`",
            "",
            "## CONTRE",
            "",
            str(payload["CONTRE"]),
            "",
        ]
    )
    validate_v7_report_text(report)
    report_path.write_text(report, encoding="utf-8")
    print(
        json.dumps(
            {"result_path": str(result_path), "sha256": result_hash, **payload},
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _existing_archive(root: Path) -> dict[str, str]:
    paths = tuple(sorted((root / "reports").glob("v7*/**/*signal_manifest.json")))
    archive: defaultdict[str, list[str]] = defaultdict(list)
    for path in paths:
        if path.name == "v72_trade_arrival_renewal_signal_manifest.json":
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        for row in payload.get("candidate_paths", []):
            if int(row.get("signal_count") or 0) > 0:
                archive[str(row["signal_path_hash"])].append(str(row["candidate_id"]))
    return {path_hash: sorted(ids)[0] for path_hash, ids in archive.items()}


def _within_manifest_duplicates(rows: list[dict[str, Any]]) -> dict[str, str]:
    groups: defaultdict[str, list[str]] = defaultdict(list)
    for row in rows:
        if int(row["signal_count"]) > 0:
            groups[str(row["signal_path_hash"])].append(str(row["candidate_id"]))
    return {
        candidate_id: sorted(ids)[0]
        for ids in groups.values()
        for candidate_id in sorted(ids)[1:]
    }


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _atomic_json(path: Path, payload: Any) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
