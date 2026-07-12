# HYDRA V7 — Cimetière actif, bootstrap

[HYDRA-V7] phase=4 step=1 verdict=GREEN
gate=GRAVEYARD_CLASS_FIREWALL preuve=reports/v7/phase4/graveyard_bootstrap.json#57019a86 tests=3/3
budget_llm=0.000000/solde budget_data=0.000000/60.00 N_trials=246706 burned=1
diff_validation=aucun CONTRE=40076_prototypes_non_persistes_sont_agreges_dans_une_classe_residuelle
prochaine_action=preregisterer_des_hypotheses_economiques_V7_a_distance_des_46_signatures_mortes

Le cimetière contient `115 443` objets sous `46` signatures de classe : les
`115 388` prototypes hérités plus les `55` paniers V6 rejetés en Phase 2.
L'interface accessible au générateur expose exclusivement : classe de
mécanisme, régime grossier, cause de mort et effectif. Aucun identifiant de
candidat, seuil, score ou paramètre n'est stocké.

## CONTRE

`40 076` prototypes anciens n'ont pas de ligne individuelle dans le registre.
Ils sont volontairement agrégés sous `UNREGISTERED_HISTORICAL_PROTOTYPES` : cela
évite d'inventer leur provenance et toute fuite paramétrique, mais rend leur
signal de distance peu informatif.
