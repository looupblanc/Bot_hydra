# HYDRA V7 — Data lake manifest

[HYDRA-V7] phase=0 step=1 verdict=NULL
gate=BOOTSTRAP_V2 preuve=data/manifest.json#cc8b6da2 tests=pending
budget_llm=0.000000/8.00 budget_data=0.000000/60.00 N_trials=pending_retro_estimate burned=1
diff_validation=hydra/data/v7_manifest.py,scripts/build_v7_data_manifest.py,tests/ruleset/test_v7_data_manifest.py CONTRE=un accès manuel non journalisé ne peut pas être exclu cryptographiquement par le seul inventaire du disque
prochaine_action=vérifier_tous_les_hashes_puis_encoder_R16_sans_ingérer_le_gap

## Cutoffs réels par produit

| Produit | Premier timestamp | Cutoff réel | Début du gap |
|---|---:|---:|---:|
| ES | 2023-01-02T23:00:00Z | 2024-12-31T21:59:00Z | 2024-12-31T22:00:00Z |
| MES | 2023-01-02T23:00:00Z | 2024-12-31T21:59:00Z | 2024-12-31T22:00:00Z |
| NQ | 2023-01-02T23:00:00Z | 2024-12-31T21:59:00Z | 2024-12-31T22:00:00Z |
| MNQ | 2023-01-02T23:00:00Z | 2024-12-31T21:59:00Z | 2024-12-31T22:00:00Z |
| RTY | 2023-01-02T23:00:00Z | 2024-09-30T23:59:00Z | 2024-10-01T00:00:00Z |
| M2K | 2023-01-02T23:00:00Z | 2024-09-30T23:59:00Z | 2024-10-01T00:00:00Z |
| YM | 2023-01-02T23:00:00Z | 2024-09-30T23:59:00Z | 2024-10-01T00:00:00Z |
| MYM | 2023-01-02T23:00:00Z | 2024-09-30T23:59:00Z | 2024-10-01T00:00:00Z |
| GC | 2023-01-02T23:00:00Z | 2024-09-30T23:59:00Z | 2024-10-01T00:00:00Z |
| MGC | 2023-01-02T23:00:00Z | 2024-09-30T23:59:00Z | 2024-10-01T00:00:00Z |
| CL | 2023-01-02T23:00:00Z | 2024-12-31T21:59:00Z | 2024-12-31T22:00:00Z |
| MCL | 2023-01-02T23:00:00Z | 2024-12-31T21:59:00Z | 2024-12-31T22:00:00Z |

Artefacts directs hachés : `73`.
Arrays dérivés réconciliés : `546`.
Fichiers de marché non classés : `0`.
Barres forward présentes : `0`.

Aucune ingestion du gap n'a été exécutée pendant la construction de ce manifest.

## CONTRE

Le manifest prouve l'état du filesystem et des ledgers connus, pas l'impossibilité absolue d'une lecture manuelle non journalisée. Le feed restera donc fail-closed et vérifiera aussi l'antériorité des fiches WORM avant chaque consommation.
