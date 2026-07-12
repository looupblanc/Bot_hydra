# HYDRA V7 — Grammaire 0001, manifeste de signaux sans résultats

[HYDRA-V7] phase=4 step=2 verdict=NULL
gate=GRAMMAR_0001_SIGNAL_INTEGRITY preuve=reports/v7/phase4/grammar0001_signal_manifest.json#b0babdf9 tests=6/6
budget_llm=0.000000/solde budget_data=0.000000/60.00 N_trials=246946 burned=1
diff_validation=aucun CONTRE=14_structures_produisent_moins_de_30_signaux_et_9_n_en_produisent_aucun
prochaine_action=figer_le_validateur_et_le_tripwire_complet_avant_tout_calcul_de_PnL

Les `24` structures pré-enregistrées ont produit `250` décisions au total. Le
manifest ne contient ni PnL, ni rendement futur, ni MFE/MAE, ni verdict.

- `9` structures ont zéro signal.
- `14` structures ont moins de `30` signaux et échoueront mécaniquement le gate
  de fréquence déjà WORM si ce décompte reste identique.
- Seules `v7g1_weekend_RTY` (`33`) et `v7g1_weekend_YM` (`31`) atteignent au
  moins `30` signaux.
- Le hash logique du manifeste est
  `4cdee41ac3789fd48e3605f1c04598c383afa1fcff4934bfbbe1f13fc6eea3a5`.

## CONTRE

Le rendement de nouveauté est faible avant même l'économie : la grammaire peut
être sous-puissante plutôt que fausse. Les seuils ne seront toutefois pas
abaissés après ce constat ; le verdict vient du funnel et le prochain pivot
portera sur une nouvelle hypothèse, pas sur un voisin paramétrique.

