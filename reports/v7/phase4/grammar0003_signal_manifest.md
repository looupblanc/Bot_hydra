# HYDRA V7 — Manifest outcome-free grammaire 0003

[HYDRA-V7] phase=4 step=8 verdict=GREEN
gate=GRAMMAR_0003_SIGNAL_FREEZE preuve=reports/v7/phase4/grammar0003_signal_manifest.json#7e013bfa tests=3/3
budget_llm=0.000000/solde budget_data=40.401063/60.00 N_trials=247366 burned=1
diff_validation=aucun CONTRE=la_densite_multi_journaliere_peut_n_etre_qu_un_biais_directionnel_generique
prochaine_action=relancer_le_tripwire_permanent_puis_executer_le_tribunal_fige

## Portée

- 19 structures fixes ;
- 1 692 signaux ;
- 16 structures avec au moins 30 signaux ;
- 3 structures GC/cross-GC sans signaux exécutables ;
- reproduction byte-identique ;
- aucune issue ou PnL dans le manifest ;
- 0 accès Q4, 0 gap forward, 0 ordre broker.

## CONTRE

Les horizons journaliers augmentent la puissance statistique mais exposent les
candidats à un simple bêta long ou à une autocorrélation de marché. Les trois
nulls pré-enregistrés, DSR/BH et le tripwire doivent les éliminer s'ils ne
portent pas une espérance distincte.
