from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v7_phase0_divergence import run_phase0_divergence


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the WORM-preregistered HYDRA V7 Phase 0 replay."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--preregistration",
        default="WORM/bootstrap-phase0-v2-2026-07-12.json",
    )
    parser.add_argument(
        "--proof-registry", default="mission/state/proof_registry.json"
    )
    parser.add_argument("--output-dir", default="reports/v7/phase0_v2")
    args = parser.parse_args()
    result = run_phase0_divergence(
        project_root=Path(args.project_root),
        preregistration_path=Path(args.preregistration),
        proof_registry_path=Path(args.proof_registry),
        output_dir=Path(args.output_dir),
    )
    print(
        json.dumps(
            {
                "experiment_id": result["experiment_id"],
                "historical_classification": result["historical_classification"],
                "historical_default_mismatch_count": result[
                    "historical_default_mismatch_count"
                ],
                "basket_count": result["basket_count"],
                "xfa_policy_count": result["xfa_policy_count"],
                "pass_rate_delta": result["combine"]["pass_rate_delta"],
                "mll_breach_rate_delta": result["combine"][
                    "mll_breach_rate_delta"
                ],
                "result_path": result["result_path"],
                "result_sha256": result["result_sha256"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
