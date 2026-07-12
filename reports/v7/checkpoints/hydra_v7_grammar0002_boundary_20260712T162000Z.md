# HYDRA V7 — Checkpoint frontière grammaire 0002

[HYDRA-V7] phase=4 step=7 verdict=NULL
gate=GRAMMAR_0002 preuve=reports/v7/phase4/grammar0002_validation_result.json#fcd3aee1 tests=611/611
budget_llm=0.000000/solde budget_data=40.401063/60.00 N_trials=247176 burned=1
diff_validation=hydra/validation/v7_grammar_0002_validation.py,scripts/run_v7_grammar_0002_validation.py,tests/test_v7_grammar_0002_validation.py CONTRE=les_meilleurs_PnL_sont_soit_domines_par_leurs_nulls_soit_tres_loin_du_DSR_requis
prochaine_action=tombstone_grammar0002_au_niveau_classe_et_faire_pivoter_les_hypotheses_sans_feedback_parametrique

## Justification contractuelle

La clause 1 impose de rendre le null honnêtement ; les clauses 2 et 3
interdisent de promouvoir les PnL séduisants en abaissant DSR/BH ou en utilisant
le passage Combine comme fitness. La clause 6 n'autorise le pivot qu'au niveau
classe économique × régime × cause de mort.

## Auto-audit

Le moyen le plus probable de se tromper maintenant serait de traiter les deux
primes overnight très profitables en développement comme un edge malgré leurs
nulls supérieurs, puis d'inventer un filtre post hoc pour les sauver.

## Résultat scientifique

- tripwire permanent : GREEN, preuve complète byte-identique ;
- 23 structures, 1 142 signaux ;
- 7 survivent aux coûts ×2 ;
- 5 battent leurs trois contrôles ;
- 0 DSR positif, 0 rejet BH, 0 promotion ;
- 0 vrai SIM_EXPLOIT ;
- 0 accès Q4, 0 gap forward, 0 ordre broker.

## CONTRE

Le DSR utilise un compteur de 247 176 essais, volontairement conservateur, et
peut être trop punitif si la rétro-estimation historique surestime fortement le
nombre de tests réellement indépendants. Ce compteur est toutefois WORM et ne
peut pas être révisé après observation ; seule une preuve fraîche plus longue
peut améliorer la conclusion.
