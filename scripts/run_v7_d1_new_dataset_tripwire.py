from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v7_d1_new_dataset_tripwire import (
    run_d1_new_dataset_tripwire,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the frozen V7 D1 tripwire.")
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
        "--proof-registry", default="mission/state/proof_registry.json"
    )
    parser.add_argument("--output-dir", default="reports/v7/data")
    args = parser.parse_args()
    result = run_d1_new_dataset_tripwire(
        project_root=args.project_root,
        grammar_path=args.grammar,
        tripwire_policy_path=args.tripwire_policy,
        validation_policy_path=args.validation_policy,
        execution_addendum_path=args.execution_addendum,
        signal_manifest_path=args.signal_manifest,
        proof_registry_path=args.proof_registry,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "verdict": result["verdict"],
                "NULL_RATIO": result["NULL_RATIO"],
                "real_episode_count": result["real"]["episode_count"],
                "real_pass_count": result["real"]["pass_count"],
                "null_episode_count": result["pooled_null"]["episode_count"],
                "null_pass_count": result["pooled_null"]["pass_count"],
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
