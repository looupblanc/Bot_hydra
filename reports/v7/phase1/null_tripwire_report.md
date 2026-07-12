# HYDRA V7 — Phase 1 Null Tripwire

[HYDRA-V7] phase=1 step=25 verdict=GREEN
gate=G1 preuve=reports/v7/phase1/null_tripwire_result.json#ea909645 tests=pending
budget_llm=0.000000/12.00 budget_data=0.000000/0.00 N_trials=pending_retro_estimate burned=1
diff_validation=hydra/validation/v7_null_tripwire.py,scripts/run_v7_null_tripwire.py,tests/ruleset/test_v7_null_tripwire.py CONTRE=This conditional-outcome null freezes the selected signal schedule rather than rerunning feature selection on each synthetic raw path; it is decisive for account-path geometry but not every possible selection-stage artefact.
prochaine_action=auditer_le_verdict_et_exécuter_la_régression_avant_G1

Verdict : **GREEN**.
Objets figés : `1015`.
Pass-rate réel : `0.35607477` sur `25680` épisodes.
Pass-rate null poolé : `0.01764019` sur `77040` épisodes.
NULL_RATIO : `0.04954068241469816`; seuil WORM : `0.8`.

- DAILY_BLOCK_SHUFFLE: pass `0.01865265`, breach `0.77390966`, ratio `0.05238408`.
- VOLATILITY_MATCHED_RANDOM_WALK: pass `0.01538162`, breach `0.74034268`, ratio `0.04319773`.
- YEAR_BLOCK_PERMUTATION: pass `0.01888629`, breach `0.77390966`, ratio `0.05304024`.

Aucune donnée Q4, aucun gap forward et aucun ordre broker n'ont été utilisés.

## CONTRE

This conditional-outcome null freezes the selected signal schedule rather than rerunning feature selection on each synthetic raw path; it is decisive for account-path geometry but not every possible selection-stage artefact.
