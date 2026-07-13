# HYDRA V7.1 — Underpowered Combine research selection

[HYDRA-V7] phase=4 step=164 verdict=GREEN
gate=V71_UNDERPOWERED_COMBINE_SELECTION preuve=reports/v7_1/combine_research_0001/v71_underpowered_combine_selection_manifest.json#6c5c324b tests=deterministic_score_plus_distinctness
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263770 burned=1
diff_validation=hydra/validation/v71_underpowered_combine_selection.py CONTRE=selection_post_walk_forward
prochaine_action=freeze_exact_selected_specs_and_run_24_start_diagnostic

- Sous-puissants source: `16`
- Sélectionnés: `5`
- Non comptabilisés: `0`

## CONTRE

Selection is post-walk-forward and the weighted score can overstate differences among noisy candidates; the diagnostic cannot promote any candidate.
