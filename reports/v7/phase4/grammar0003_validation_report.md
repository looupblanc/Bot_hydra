# HYDRA V7 — Validation grammaire 0003

[HYDRA-V7] phase=4 step=7 verdict=NULL
gate=GRAMMAR_0003 preuve=reports/v7/phase4/grammar0003_validation_result.json#258fb46b tests=617/617
budget_llm=0.000000/solde budget_data=40.401063/60.00 N_trials=247366 burned=1
diff_validation=hydra/validation/v7_grammar_0003_validation.py,scripts/run_v7_grammar_0003_validation.py,tests/test_v7_grammar_0003_validation.py CONTRE=le_controle_non_signal_ne_matche_pas_tous_les_regimes_latents
prochaine_action=tombstone_grammar_classes_and_preregister_new_hypotheses

## Tripwire permanent

- Verdict : `GREEN`
- NULL_RATIO : `0.049540682415`
- 1 015 objets; preuve complète byte-identique au G1 fondateur.

## Funnel

- Structures : `19`
- Signaux : `1692`
- Stage 1 : `9`
- Stage 2 : `7`
- Null suite : `3`
- DSR positifs : `0`
- Rejets BH : `0`
- SIM_EXPLOIT : `2`
- File shadow : `0`

## CONTRE

The matched non-signal-day control preserves clock and opportunity count but cannot exactly match every latent regime. Even a promoted candidate would require its WORM fiche and untouched forward proof.
