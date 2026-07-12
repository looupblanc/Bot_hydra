# HYDRA V7 — Validation grammaire 0002

[HYDRA-V7] phase=4 step=6 verdict=NULL
gate=GRAMMAR_0002 preuve=reports/v7/phase4/grammar0002_validation_result.json#fcd3aee1 tests=611/611
budget_llm=0.000000/solde budget_data=40.401063/60.00 N_trials=247176 burned=1
diff_validation=hydra/validation/v7_grammar_0002_validation.py,scripts/run_v7_grammar_0002_validation.py,tests/test_v7_grammar_0002_validation.py CONTRE=les_horloges_de_session_peuvent_rester_des_proxies_non_causaux
prochaine_action=tombstone_grammar_classes_and_preregister_new_hypotheses

## Tripwire permanent

- Verdict : `GREEN`
- NULL_RATIO : `0.049540682415`
- Preuve complète byte-identique au G1 fondateur.

## Funnel

- Structures : `23`
- Signaux : `1142`
- Stage 1 : `7`
- Stage 2 : `7`
- Null suite : `5`
- DSR positifs : `0`
- Rejets BH : `0`
- SIM_EXPLOIT : `0`
- File shadow : `0`

## Meilleurs résultats non promus

- prime overnight NQ : 275 événements, +39 997,50 $ base, +37 935,00 $
  sous coûts ×2, mais le meilleur null vaut 219,51 $/trade contre 141,70 $
  observés à ×1,5 et DSR = -3,485 ;
- prime overnight ES : 196 événements, +21 380,20 $ base, +17 705,20 $
  à ×2, mais null supérieur et DSR = -3,378 ;
- réaccélération ES : 77 événements, +4 469,90 $ à ×2, bat ses nulls,
  mais DSR = -3,662 ;
- unwind ES : 112 événements, +1 574,40 $ à ×2, bat ses nulls, mais
  DSR = -4,340 ;
- aucun résultat n'autorise une fiche candidat WORM ou la consommation du gap.

## CONTRE

Fixed session clocks can still proxy broad seasonality, and the three candidate nulls do not identify a unique causal counterparty. Only a DSR/BH survivor frozen before fresh proof can enter a shadow slot.
