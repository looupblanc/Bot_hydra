# HYDRA V7 — Gate G0

[HYDRA-V7] phase=0 step=25 verdict=GREEN
gate=G0 preuve=reports/v7/phase0_v2/phase0_result.json#8de76b03 tests=565/565
budget_llm=0.000000/8.00 budget_data=0.000000/60.00 N_trials=pending_retro_estimate burned=1
diff_validation=config/rulesets/topstep_150k_v7.json,hydra/account_policy/basket.py,hydra/account_policy/xfa.py,hydra/data/v7_manifest.py,hydra/execution/v7_cost_model.py,hydra/propfirm/combine_episode.py,hydra/propfirm/intraday_mll.py,hydra/propfirm/mll_variants.py,hydra/propfirm/ruleset_v7.py,hydra/propfirm/topstep_150k.py,hydra/propfirm/trading_day.py,hydra/propfirm/xfa_episode.py,hydra/validation/v7_phase0_divergence.py,scripts/build_v7_data_manifest.py,scripts/hydra_mission_doctor.py,scripts/run_v7_phase0.py,tests/ruleset/ CONTRE=R2_reste_non_tranché_et_30987_événements_OHLC_ne_donnent_pas_l'ordre_intrabar_MFE_MAE
prochaine_action=préenregistrer_les_seeds_et_le_pipeline_null_P1_avant_tout_run_contrefactuel

## Verdict

G0 est **VERT** au sens strict du contrat : la simulation de référence est reproductible, R1–R16 sont exécutables, les deux modes MLL sont sélectionnables, le data lake est inventorié avant tout backfill, et aucun ordre ni accès à une nouvelle preuve n'a eu lieu.

Ce verdict ne dit rien sur l'existence d'un edge. Les 55 paniers et 6 politiques XFA restent des artefacts de développement non corrigés pour la multiplicité.

## Preuves

- Contrat actif : `MISSION_CONTRACT.md`, SHA-256 `35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab`.
- Pré-enregistrement G0 v2 : `WORM/bootstrap-phase0-v2-2026-07-12.json`, SHA-256 `bc060975d44439ee571b1d87167ff29d56639ff3948ae1ac1c4eb65b098640a6`.
- Manifest data lake : `data/manifest.json`, SHA-256 fichier `e471cc472a46e68d7164d73ef95c16e995bbded6bad9fcb8537cb89657274b38`, hash canonique `cc8b6da2ea2c235f9871092ac4ddf3a97d4d5dc5c6ba589e30414a337f884371`.
- Résultat divergence : `reports/v7/phase0_v2/phase0_result.json`, SHA-256 `8de76b03a183eb0a5b8fe79609f2c1d01e8afe4599d81a93adf9c1d88fc614f6`.
- Modèle de coûts : `reports/v7/phase0_v2/costs_model.md`, SHA-256 `b9928fbf275bc470ad0c63c463b5a068b6294483f2c7fcd7ddfc842cd9594d73`.
- Registre de preuve : Q4 BURNED, chaîne valide, SHA-256 `04ea729551ec014abe6b93091050e6084b2ce3d02224039951c8aeead23ec537`.

## Résultats chiffrés

- Replay historique exact : 55 paniers + 6 politiques XFA, 0 mismatch.
- Mode `eod_level_rt_breach` : 2 640 épisodes, pass-rate développement 45,1894 %, breach MLL 12,0455 %.
- Mode `intraday_hwm` conservateur : 2 640 épisodes, pass-rate 42,9924 %, breach MLL 21,5530 %.
- Sensibilité : −2,1970 points de pass-rate, +9,5076 points de breach, 251 terminaux modifiés.
- Ambiguïtés d'ordre intrabar MFE/MAE : 30 987.
- Données : 73 artefacts directs, 546 arrays dérivés, 12 produits, 0 fichier non classé, 0 barre forward.
- Tests : 565/565 ; ciblés no-lookahead/Q4/budget : 17/17 ; compileall : OK ; mission DB et deux registry DB : `ok`.
- Doctor bootstrap : `HEALTHY_STOPPED_FOR_V7_BOOTSTRAP`, PID 0, writers 0. Ce statut ne prétend pas que la mission tourne.
- Ordres broker : 0. Dépense Phase 0 data : 0 $.

## CONTRE

Le conflit R2 reste à trancher humainement. Les données OHLC donnent les extrema mais pas leur ordre intrabar ; la variante `intraday_hwm` applique donc une convention conservatrice et ne mesure pas exactement le breach-rate réel. En outre, aucune correction de multiplicité ni calibration null n'a encore été appliquée : tout pass-rate économique hérité reste non probant.

## Décision suivante

La génération demeure gelée. Phase 1 doit maintenant préenregistrer les seeds, produire les trois jeux contrefactuels par produit et rejouer le pipeline strictement identique avant que G1 puisse autoriser la suite.
