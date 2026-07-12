from __future__ import annotations

from pathlib import Path
from typing import Any

from hydra.research.v7_hypothesis_grammar_0004 import (
    candidate_specs,
    generate_signal_population,
    load_v7_market_bars,
)
from hydra.validation.v7_frozen_grammar_tribunal import (
    FrozenGrammarTribunalConfig,
    run_frozen_grammar_tribunal,
)


CONFIG = FrozenGrammarTribunalConfig(
    grammar_number=4,
    grammar_id="hydra_v7_grammar_0004_cross_sectional_scheduled_flow",
    result_schema="hydra_v7_grammar_0004_validation_result_v1",
    preregistration_sha256=(
        "e2d34f38635a203b1157ad78f541e53aa378f930a24a7735a3eff6020ce6ed74"
    ),
    validation_policy_sha256=(
        "595ea2968f6b0a870735063e5af007d7fcc64af778a978743f923fbd7bc8e28b"
    ),
    signal_manifest_sha256=(
        "29b364abbe182a0e628f7df57744c8964b0e172e1a46b5f32b72b66717f5e28e"
    ),
    tripwire_attestation_sha256=(
        "69631069b8a929a2683b534f92c9f191a1f269854f9de08bee125ebaf4776b71"
    ),
    n_trials=247_476,
    diff_validation=(
        "hydra/validation/v7_frozen_grammar_tribunal.py",
        "hydra/validation/v7_grammar_0004_validation.py",
        "scripts/run_v7_grammar_0004_validation.py",
        "tests/test_v7_frozen_grammar_tribunal.py",
    ),
    contre=(
        "Cross-sectional clocks and scheduled weekdays may proxy generic "
        "intraday seasonality. A promotion would still need a WORM candidate "
        "fiche and untouched forward proof."
    ),
    report_contre_slug="les_horloges_et_jours_programmes_peuvent_proxy_la_saisonnalite",
)


def run_grammar_0004_validation(
    *,
    project_root: str | Path,
    preregistration_path: str | Path,
    validation_policy_path: str | Path,
    signal_manifest_path: str | Path,
    tripwire_attestation_path: str | Path,
    proof_registry_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    return run_frozen_grammar_tribunal(
        config=CONFIG,
        project_root=project_root,
        preregistration_path=preregistration_path,
        validation_policy_path=validation_policy_path,
        signal_manifest_path=signal_manifest_path,
        tripwire_attestation_path=tripwire_attestation_path,
        proof_registry_path=proof_registry_path,
        output_dir=output_dir,
        candidate_specs_fn=candidate_specs,
        generate_signals_fn=generate_signal_population,
        load_bars_fn=load_v7_market_bars,
    )


__all__ = ["run_grammar_0004_validation"]
