# HYDRA V7.1 — Checkpoint de régression intégrée

[HYDRA-V7] phase=4 step=125 verdict=GREEN
gate=V71_INTEGRATED_REGRESSION preuve=reports/v7_1/discovery/v71_development_funnel_result.json#b8767eb9 tests=683/683
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.000000 N_trials=262228 burned=1
diff_validation=hydra/validation/v7_tripwire_evidence.py,hydra/validation/v7_report_schema.py,hydra/validation/v71_hierarchical_multiplicity.py,hydra/validation/v71_event_funnel.py,hydra/validation/v71_mechanism_forensics.py CONTRE=aucun_des_11_effets_walk_forward_positifs_n_atteint_le_minimum_WORM_de_320_evenements_et_ils_peuvent_tous_etre_du_bruit
prochaine_action=deployer_le_controleur_V7_1_puis_prereferencer_une_grammaire_densite_opportunite_sans_achat_data

## Contrôles intégrés

- Régression complète : `683/683`.
- Tests no-lookahead, ruleset, budget, gouvernance et Q4 ciblés : `70/70`.
- Compilation : verte.
- Mission DB, registre et cimetière : `PRAGMA integrity_check = ok`.
- Gouvernance : hash sémantique `05810bc193e51e3c40722163a1ee3ae82fd3a8d7762c48eafa25c3a4cba1102b`, Q4 fermé après un accès atomique déjà consommé.
- Replay déterministe du manifeste D1 : SHA-256 identique `b366bd8f295b0a1110532988b15cc9dde70d450fc8ea9a53b476c16b61a7e15c`.
- Scan de secrets : `868` fichiers suivis, `0` secret non-placeholder.
- Nouvel achat de données V7.1 : `0 USD`.

Justification : clauses 1, 2, 5 et 8 — conserver le verdict sous-puissant, les seuils WORM et l’absence totale d’ordre broker.
La relance est permise par A4 uniquement après cette régression, les contrôles d’intégrité/gouvernance et le scan de secrets verts.

Auto-audit : le risque principal est de confondre onze moyennes walk-forward positives mais sous-puissantes avec onze edges transférables.

## CONTRE

La puissance du validateur est démontrée sur des effets injectés et non sur une nouvelle preuve de marché; la prochaine grammaire peut donc produire un nouveau null propre malgré une meilleure densité d’événements.
