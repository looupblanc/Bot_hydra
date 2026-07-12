# HYDRA V7 — Manifest outcome-free grammaire 0002

[HYDRA-V7] phase=4 step=5 verdict=GREEN
gate=GRAMMAR_0002_SIGNAL_FREEZE preuve=reports/v7/phase4/grammar0002_signal_manifest.json#70ebca12 tests=4/4
budget_llm=0.000000/solde budget_data=40.401063/60.00 N_trials=247176 burned=1
diff_validation=aucun CONTRE=14_des_23_structures_restent_sous_30_evenements_et_la_densite_ne_prouve_aucune_esperance
prochaine_action=implementer_le_tribunal_de_validation_sans_modifier_le_generateur

## Portée figée

- 23 structures économiques fixes ;
- 1 142 signaux ;
- 9 structures avec au moins 30 signaux ;
- aucune issue, PnL, cible, MFE ou MAE dans le manifest ;
- reproduction byte-identique au second run ;
- 0 accès Q4, 0 accès gap forward, 0 ordre broker.

## Répartition utile avant tribunal

- réaccélération d'après-midi : CL 63, ES 77, NQ 84, RTY 84, YM 63 ;
- prime overnight : ES 196, NQ 275 ;
- unwind overnight : ES 112, NQ 151 ;
- hebdomadaire CL : 20, donc sous le minimum pré-enregistré ;
- les autres structures restent sous 30 sans adaptation post hoc.

## CONTRE

La densité supérieure à la grammaire 0001 réduit le risque d'un test purement
sous-puissant, mais elle peut seulement multiplier des manifestations d'une
saisonnalité générique. Les coûts, le walk-forward, DSR, BH et les nulls
candidat restent entièrement à exécuter.
