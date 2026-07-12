from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v7_grammar_0004_validation import (
    run_grammar_0004_validation,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the WORM-frozen V7 grammar 0004 tribunal."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--preregistration",
        default="WORM/v7-grammar-0004-hypotheses-2026-07-12.json",
    )
    parser.add_argument(
        "--validation-policy",
        default="WORM/v7-grammar-0004-validation-policy-2026-07-12.json",
    )
    parser.add_argument(
        "--signal-manifest",
        default="reports/v7/phase4/grammar0004_signal_manifest.json",
    )
    parser.add_argument(
        "--tripwire-attestation",
        default="reports/v7/phase4/grammar0004_permanent_tripwire_attestation.json",
    )
    parser.add_argument(
        "--proof-registry", default="mission/state/proof_registry.json"
    )
    parser.add_argument("--output-dir", default="reports/v7/phase4")
    args = parser.parse_args()
    result = run_grammar_0004_validation(
        project_root=args.project_root,
        preregistration_path=args.preregistration,
        validation_policy_path=args.validation_policy,
        signal_manifest_path=args.signal_manifest,
        tripwire_attestation_path=args.tripwire_attestation,
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
