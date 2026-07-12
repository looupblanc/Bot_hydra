# HYDRA V7 — Audit du tribunal D1 grammaire 0001

[HYDRA-V7] phase=D step=100 verdict=NULL
gate=D1_CANDIDATE_TRIBUNAL preuve=reports/v7/data/d1_candidate_tribunal_result.json#fcdb9477 tests=659/659
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_D1=59.859323/60.00 N_trials_resultat=247684 N_trials_registre_actuel=247892 burned=1
diff_validation=hydra/validation/v7_d1_candidate_tribunal.py,tests/test_v7_d1_candidate_tribunal.py CONTRE=deux_blocs_aout_ne_couvrent_pas_les_autres_saisons_et_le_null_flow_price_initial_etait_degenere
prochaine_action=indexer_les_quatre_classes_D1_0001_puis_executer_le_tripwire_WORM_D1_0002

## Verdict chiffré

- Structures : `8`
- Signaux : `9 095`
- Stage 1 : `1`
- Stage 2 : `0`
- DSR positifs : `0`
- Rejets BH : `0`
- `SIM_EXPLOIT` : `1`
- File shadow : `0`
- Accès Q4/gap : `0/0`
- Ordres broker : `0`

Sept candidats ont une espérance nette négative dès les coûts de base. Le seul
candidat positif à ce niveau, `v7d1g1_absorption_reversal_ES`, passe de
`+3,298765 $/trade` à `-9,201235 $/trade` sous coûts ×1,5 puis
`-21,701235 $/trade` sous coûts ×2. Il est donc tué par le gate de coûts et
badgé `SIM_EXPLOIT` avant walk-forward, DSR ou BH.

## Incidents d'ingénierie fail-closed

Les deux premières exécutions n'ont écrit aucun résultat : le null décalé de
cinq sessions comprimait des chemins événementiels. La réparation conserve le
premier chemin, élimine les collisions et fenêtres de roll invalides, et garde
le seuil WORM de rétention à 80 %. Sur ces blocs courts, la rétention observée
reste comprise entre `61,11 %` et `71,83 %`; le seuil n'a pas été abaissé.

Le contrôle `FLOW_PRICE_DECOUPLING` du résultat 0001 a aussi une rétention nulle
car la source donneuse était modifiée en cascade. Ce défaut est documenté et
corrigé pour les futurs tests, sans réinterpréter ni rejouer 0001. Il ne change
pas son verdict : aucun candidat n'atteint Stage 2, donc aucun null, DSR ou BH ne
pouvait le promouvoir.

## CONTRE

Le scope ne contient que deux blocs d'août et ne permet pas d'inférer que les
quatre mécanismes sont nuls en toute saison. Le verdict porte seulement sur les
formulations exactes 0001, avec leurs horizons et exécutions gelés.
