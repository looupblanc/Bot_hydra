# HYDRA V7.2 — Réservation de multiplicité G11

[HYDRA-V7] phase=4 step=198 verdict=GREEN
gate=V72_G11_MULTIPLICITY preuve=reports/v7_2/discovery_0011/v72_g11_multiplicity_reservation.json#e602f786 tests=proof_registry_chain_verified
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=265227 burned=1
diff_validation=aucun CONTRE=une_erreur_de_transcription_du_SHA_long_a_exige_une_annotation_append_only
prochaine_action=figer_les_chemins_de_signaux_puis_executer_la_validation_separee

- Réservation structurelle : `24`.
- Mondes candidat-tripwire : `96`.
- Incrément global : `120` (`265107 → 265227`).
- N_trials campagne après inflation 1,5 : `180`.
- Résultat ou PnL observé avant la réservation : `non`.

## CONTRE

Le premier événement contenait une transcription erronée du SHA Git long. Une annotation append-only immédiate a corrigé uniquement ce champ ; le tag et les deux hashes de contenu WORM étaient corrects. Cette correction est conservée comme anomalie d’audit, sans réécriture du registre.
