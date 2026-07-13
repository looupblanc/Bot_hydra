# HYDRA V7.1 — Déploiement persistant G6

[HYDRA-V7] phase=4 step=167 verdict=GREEN
gate=V71_G6_PERSISTENT_DEPLOYMENT preuve=reports/v7_1/checkpoints/v71_g6_persistent_deployment_evidence_step_0167.json#cf85981b tests=725/725
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263814 burned=1
diff_validation=hydra/validation/v71_trade_size_composition_funnel.py,hydra/validation/v71_trade_size_composition_tripwire.py,hydra/validation/v71_trade_size_composition_power_audit.py,hydra/validation/v71_underpowered_combine_selection.py,hydra/validation/v71_underpowered_combine_diagnostic.py CONTRE=service_sain_mais_zero_candidat_powered_et_zero_pass_Combine
prochaine_action=preregister_independent_confirmation_or_distinct_hypothesis_0007_without_data_purchase

## État persistant

- Service : `active/enabled`, PID `449754`, contrôleur `v7`, un seul processus et un seul writer.
- Mission : même ID, même base, lock PID cohérent, une expérience `RUNNING`, zéro file dupliquée.
- Action : `V71_G6_GREEN_UNDERPOWERED_COMBINE_DIAGNOSTIC_COMPLETE`.
- Preuve : 22 walk-forward positifs tous comptabilisés ; 16 sous-puissants, 4 fragiles, 2 `GEOMETRY_ONLY`.
- Combine diagnostic : 5 candidats, 24 départs, 4 blocs effectifs, 0 passage candidat, 0 passage panier, 0 validation.
- Q4 : 1 transaction historique fermée et `BURNED`; aucune réutilisation.
- Données/ordres : 0 achat nouveau, 0 broker, 0 ordre.

## CONTRE

La continuité systemd prouve seulement la continuité opérationnelle. Elle ne transforme ni le tripwire G6 en preuve fraîche, ni les projections de progrès en passages réels. Aucun candidat ne franchit encore le gate final de puissance à 80 %.
