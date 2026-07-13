# HYDRA V7.2 — Store d'occupation des prix exécutés G12

[HYDRA-V7] phase=4 step=205 verdict=GREEN
gate=V72_G12_DATA_STORE preuve=reports/v7_2/discovery_0012/v72_executed_price_occupancy_store_result.json#fc500a7c tests=5_targeted_plus_two_full_data_replays
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=265347 burned=1
diff_validation=aucun CONTRE=le_store_deterministe_peut_rester_une_transformation_de_la_geometrie_contemporaine
prochaine_action=figer_les_signaux_past_only_dans_un_commit_recherche_separe

- Prints bruts parcourus : `47 822 514`.
- Prints bornés D1 : `33 954 039`.
- Prints ES RTH retenus : `16 129 864`.
- Sortie : `17 200` minutes, `43` sessions, contrats `ESU3/ESU4`.
- Hash parquet : `46dede4ba706eb955ce523f8b7d117a0382a31291b40f9c236209bda47cd2374`.
- Replays : chunks `1 000 000` et `333 333`, hash identique.
- Résultats feature/signal/PnL observés : `non/non/non`.
- Achat data, accès Q4, ordre broker : `0 / 0 / 0`.

## CONTRE

La reproductibilité et la disponibilité à clôture de minute ne prouvent aucun edge. Les modes, revisites et entropies peuvent seulement encoder la volatilité ou la direction contemporaine ; le tripwire de classe et le walk-forward restent décisifs.
