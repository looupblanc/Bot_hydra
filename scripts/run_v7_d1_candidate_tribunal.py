from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v7_d1_candidate_tribunal import run_d1_candidate_tribunal


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the frozen V7 D1 tribunal.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--grammar",
        default="WORM/v7-d1-microstructure-grammar-0001-2026-07-12.json",
    )
    parser.add_argument(
        "--tripwire-policy",
        default="WORM/v7-d1-new-dataset-tripwire-2026-07-12.json",
    )
    parser.add_argument(
        "--validation-policy",
        default="WORM/v7-d1-microstructure-validation-policy-2026-07-12.json",
    )
    parser.add_argument(
        "--execution-addendum",
        default="WORM/v7-d1-microstructure-execution-addendum-2026-07-12.json",
    )
    parser.add_argument(
        "--signal-manifest",
        default="reports/v7/data/d1_microstructure_grammar0001_signal_manifest.json",
    )
    parser.add_argument(
        "--tripwire-result",
        default="reports/v7/data/d1_new_dataset_tripwire_result.json",
    )
    parser.add_argument(
        "--proof-registry", default="mission/state/proof_registry.json"
    )
    parser.add_argument("--output-dir", default="reports/v7/data")
    args = parser.parse_args()
    result = run_d1_candidate_tribunal(
        project_root=args.project_root,
        grammar_path=args.grammar,
        tripwire_policy_path=args.tripwire_policy,
        validation_policy_path=args.validation_policy,
        execution_addendum_path=args.execution_addendum,
        signal_manifest_path=args.signal_manifest,
        tripwire_result_path=args.tripwire_result,
        proof_registry_path=args.proof_registry,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "verdict": result["verdict"],
                "candidate_count": result["candidate_count"],
                "signal_count": result["signal_count"],
                "stage1_survivor_count": result["stage1_survivor_count"],
                "stage2_survivor_count": result["stage2_survivor_count"],
                "candidate_null_pass_count": result["candidate_null_pass_count"],
                "DSR_positive_count": result["DSR_positive_count"],
                "BH_rejection_count": result["BH_rejection_count"],
                "SIM_EXPLOIT_count": result["SIM_EXPLOIT_count"],
                "selected_shadow_queue_candidate_ids": result[
                    "selected_shadow_queue_candidate_ids"
                ],
                "result_path": result["result_path"],
                "result_sha256": result["result_sha256"],
                "report_path": result["report_path"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
