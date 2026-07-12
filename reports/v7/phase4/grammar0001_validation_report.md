# HYDRA V7 — Validation grammaire 0001

[HYDRA-V7] phase=4 step=3 verdict=BLOCKED
gate=GRAMMAR_0001 preuve=reports/v7/phase4/grammar0001_validation_result.json#9aa39f5d tests=593/593
budget_llm=0.000000/solde budget_data=0.000000/60.00 N_trials=246946 burned=1
diff_validation=hydra/validation/v7_grammar_0001_validation.py,scripts/run_v7_grammar_0001_validation.py,tests/test_v7_grammar_0001_validation.py CONTRE=la_grammaire_ne_contient_que_24_structures_et_peut_etre_sous_puissante
prochaine_action=tombstone_grammar_classes_and_preregister_grammar_0002

## Tripwire en tête

- Verdict : `BLOCKED_UNDERPOWERED_GRAMMAR_TRIPWIRE`
- Pass-rate réel : `0.000000`
- Pass-rate null poolé : `0.003472`
- NULL_RATIO : `None`
- Le pass-rate est diagnostique uniquement, jamais une fitness.

## Funnel

- Structures : `24`
- Signaux : `250`
- Survivants Stage 1 : `2`
- Survivants Stage 2 : `2`
- Tués avant walk-forward : `22` (`91.67%`)
- DSR positifs : `0`
- Rejets BH : `0`
- SIM_EXPLOIT : `16`
- File shadow : `0`

## Annotation d'intégrité — champ SIM_EXPLOIT brut

Le champ agrégé brut `SIM_EXPLOIT_count=16` ne constitue pas seize preuves de
disparition d'un edge sous slippage ×2. Il inclut des structures arrêtées avant
calcul d'une espérance, principalement faute de signaux ou d'événements. Selon
R14, le badge `SIM_EXPLOIT` exige qu'une espérance positive mesurable disparaisse
sous coûts ×2. Le décompte contractuellement exploitable est donc :

- `22` morts `INSUFFICIENT_EVENT_COUNT` avant walk-forward ;
- `2` morts `MULTIPLICITY_DEFLATION_FAILURE` après walk-forward ;
- `0` edge positif détruit par le stress ×2 ;
- `0` candidat promu.

Le résultat JSON brut reste intact. Cette annotation empêche seulement que le
cimetière de classes apprenne une fausse cause de mort.

## CONTRE

The grammar encodes only 24 fixed structures and most have low event counts; a blocked or null verdict may reflect insufficient opportunity density as much as absence of the stated mechanisms. Thresholds remain frozen and the next response must be a new economic hypothesis.
