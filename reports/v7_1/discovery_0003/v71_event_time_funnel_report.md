# HYDRA V7.1 — Event-time Stage 0–2

[HYDRA-V7] phase=4 step=141 verdict=GREEN
gate=V71_G3_STAGE0_STAGE2 preuve=reports/v7_1/discovery_0003/v71_event_time_funnel_result.json#22f9816a tests=128_structures
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=262996 burned=1
diff_validation=hydra/validation/v71_event_time_funnel.py CONTRE=l_horloge_eventuelle_peut_seulement_mesurer_la_liquidite
prochaine_action=classify_event_time_grammar_and_select_next_information_action

- Stage 0 valides/novel: `128`
- Stage 1: `6`
- Walk-forward positifs: `2`
- Walk-forward positifs et >=320 événements: `0`

## CONTRE

Event-time states can be deterministic session-liquidity clocks without directional expectancy; powered positive walk-forward transfer and the permanent tripwire remain mandatory.
