from __future__ import annotations

import hashlib
import json
import os
import signal
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from hydra.data.budget import DatabentoBudgetConfig, cumulative_spend
from hydra.governance.invariants import q4_access_count
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.mission.experiment_queue import ensure_experiment_schema
from hydra.mission.economic_evolution_runtime import (
    CAMPAIGN_CONFIG_RELATIVE_PATH,
    EconomicEvolutionRuntime,
    classify_economic_evolution_action,
    verify_economic_evolution_freeze,
)
from hydra.mission.economic_evolution_successor_runtime import (
    CAMPAIGN_CONFIG_RELATIVE_PATH as SUCCESSOR_CONFIG_RELATIVE_PATH,
    EconomicEvolutionSuccessorRuntime,
    verify_successor_freeze,
)
from hydra.mission.economic_evolution_information_runtime import (
    REVIEW_CONFIG_RELATIVE_PATH as INFORMATION_REVIEW_CONFIG_RELATIVE_PATH,
    EconomicEvolutionInformationRuntime,
    verify_information_review_freeze,
)
from hydra.mission.economic_evolution_validation_runtime import (
    VALIDATION_CONFIG_RELATIVE_PATH as EXPENSIVE_VALIDATION_CONFIG_RELATIVE_PATH,
    EconomicEvolutionValidationRuntime,
    verify_expensive_validation_freeze,
)
from hydra.mission.economic_evolution_failure_runtime import (
    REVIEW_CONFIG_RELATIVE_PATH as FAILURE_REVIEW_CONFIG_RELATIVE_PATH,
    EconomicEvolutionFailureReviewRuntime,
    verify_failure_review_freeze,
)
from hydra.mission.economic_evolution_density_runtime import (
    CAMPAIGN_CONFIG_RELATIVE_PATH as DENSITY_CONFIG_RELATIVE_PATH,
    CAMPAIGN_OUTPUT_RELATIVE_PATH as DENSITY_OUTPUT_RELATIVE_PATH,
    CAMPAIGN_RESULT_NAME as DENSITY_RESULT_NAME,
    EconomicEvolutionDensityRuntime,
    verify_density_freeze,
)
from hydra.mission.economic_evolution_density_terminal_runtime import (
    TERMINAL_VERDICT_RELATIVE_PATH as DENSITY_TERMINAL_VERDICT_RELATIVE_PATH,
    EconomicEvolutionDensityTerminalRuntime,
    load_and_verify_density_terminal_verdict,
)
from hydra.mission.economic_evolution_agreement_runtime import (
    CAMPAIGN_CONFIG_RELATIVE_PATH as AGREEMENT_CONFIG_RELATIVE_PATH,
    CAMPAIGN_OUTPUT_RELATIVE_PATH as AGREEMENT_OUTPUT_RELATIVE_PATH,
    CAMPAIGN_RESULT_NAME as AGREEMENT_RESULT_NAME,
    EconomicEvolutionAgreementRuntime,
    verify_agreement_freeze,
)
from hydra.mission.economic_evolution_agreement_terminal_runtime import (
    TERMINAL_VERDICT_RELATIVE_PATH as AGREEMENT_TERMINAL_VERDICT_RELATIVE_PATH,
    EconomicEvolutionAgreementTerminalRuntime,
    load_and_verify_agreement_terminal_verdict,
)
from hydra.mission.economic_evolution_cross_session_runtime import (
    CAMPAIGN_CONFIG_RELATIVE_PATH as CROSS_SESSION_CONFIG_RELATIVE_PATH,
    CAMPAIGN_OUTPUT_RELATIVE_PATH as CROSS_SESSION_OUTPUT_RELATIVE_PATH,
    CAMPAIGN_RESULT_NAME as CROSS_SESSION_RESULT_NAME,
    EconomicEvolutionCrossSessionRuntime,
    verify_cross_session_freeze,
)
from hydra.mission.economic_evolution_cross_session_terminal_runtime import (
    TERMINAL_VERDICT_RELATIVE_PATH as CROSS_SESSION_TERMINAL_VERDICT_RELATIVE_PATH,
    EconomicEvolutionCrossSessionTerminalRuntime,
    load_and_verify_cross_session_terminal_verdict,
)
from hydra.mission.mission_state import (
    append_event,
    append_jsonl,
    clear_stop,
    connect_state,
    get_kv,
    mission_lock,
    mission_paths,
    set_kv,
    stop_requested,
    write_heartbeat,
)
from hydra.research.economic_evolution_density_campaign import (
    load_and_verify_density_result,
)
from hydra.research.economic_evolution_agreement_campaign import (
    load_and_verify_agreement_result,
)
from hydra.research.economic_evolution_cross_session_campaign import (
    load_and_verify_cross_session_result,
)
from hydra.utils.time import utc_now_iso


CONTRACT_SHA256 = (
    "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
)
CONTROLLER_SCHEMA = "hydra_v7_economic_evolution_controller_v14"
EXPERIMENT_ID = "hydra_v7_1_falsification_20260712_0001"
CONTROLLER_CLAIM_TOKEN = "v7-economic-evolution-single-writer"
CONTROLLER_OWNER = "v7_economic_evolution_controller"
G0_RELATIVE_PATH = Path("reports/v7/phase0_v2/g0_result.json")
G1_RELATIVE_PATH = Path("reports/v7/phase1/g1_result.json")
D1_TRIBUNAL_RELATIVE_PATH = Path(
    "reports/v7/data/d1_candidate_tribunal_result.json"
)
V71_POLICY_RELATIVE_PATH = Path(
    "WORM/v7.1-hierarchical-validation-policy-2026-07-12.json"
)
V71_POWER_RELATIVE_PATH = Path(
    "reports/v7_1/calibration/v71_power_audit_result.json"
)
V71_POWER_EXTENSION_RELATIVE_PATH = Path(
    "reports/v7_1/calibration/v71_power_sample_extension_result.json"
)
V71_SIGNAL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery/v71_signal_manifest.json"
)
V71_FUNNEL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery/v71_development_funnel_result.json"
)
V71_FORENSICS_RELATIVE_PATH = Path(
    "reports/v7_1/forensics/v71_mechanism_forensics_result.json"
)
V71_G2_GRAMMAR_RELATIVE_PATH = Path(
    "WORM/v7.1-opportunity-density-grammar-0002-2026-07-12.json"
)
V71_G2_SIGNAL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0002/v71_opportunity_density_signal_manifest.json"
)
V71_G2_FUNNEL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0002/v71_opportunity_density_funnel_result.json"
)
V71_G2_TRIPWIRE_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0002/v71_opportunity_density_tripwire_result.json"
)
V71_CONFIRMATION_QUEUE_RELATIVE_PATH = Path(
    "WORM/v7.1-independent-confirmation-queue-0001-2026-07-12.json"
)
V71_G3_GRAMMAR_RELATIVE_PATH = Path(
    "WORM/v7.1-event-time-grammar-0003-2026-07-12.json"
)
V71_G3_SIGNAL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0003/v71_event_time_signal_manifest.json"
)
V71_G3_FUNNEL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0003/v71_event_time_funnel_result.json"
)
V71_G3_TRIPWIRE_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0003/v71_event_time_tripwire_result.json"
)
V71_POWER_AWARE_CALIBRATION_RELATIVE_PATH = Path(
    "reports/v7_1/power_aware_0001/"
    "v71_candidate_specific_power_calibration_result.json"
)
V71_POWER_AWARE_AUDIT_RELATIVE_PATH = Path(
    "reports/v7_1/power_aware_0001/v71_power_aware_candidate_audit_result.json"
)
V71_ROLLING_DIAGNOSTIC_RELATIVE_PATH = Path(
    "reports/v7_1/power_aware_0001/v71_event_time_rolling_diagnostic_result.json"
)
V71_G4_GRAMMAR_RELATIVE_PATH = Path(
    "WORM/v7.1-cross-clock-flow-grammar-0004-2026-07-12.json"
)
V71_G4_SIGNAL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0004/v71_cross_clock_flow_signal_manifest.json"
)
V71_G4_FUNNEL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0004/v71_cross_clock_flow_funnel_result.json"
)
V71_G4_TRIPWIRE_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0004/v71_cross_clock_flow_tripwire_result.json"
)
V71_G4_POWER_AUDIT_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0004/v71_cross_clock_flow_power_audit_result.json"
)
V71_CONFIRMATION_QUEUE_V3_RELATIVE_PATH = Path(
    "WORM/v7.1-independent-confirmation-queue-0003-2026-07-12.json"
)
V71_G5_GRAMMAR_RELATIVE_PATH = Path(
    "WORM/v7.1-cross-clock-speed-leadership-grammar-0005-2026-07-13.json"
)
V71_G5_SIGNAL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0005/"
    "v71_cross_clock_speed_leadership_signal_manifest.json"
)
V71_G5_FUNNEL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0005/"
    "v71_cross_clock_speed_leadership_funnel_result.json"
)
V71_G5_TRIPWIRE_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0005/"
    "v71_cross_clock_speed_leadership_tripwire_result.json"
)
V71_G6_GRAMMAR_RELATIVE_PATH = Path(
    "WORM/v7.1-trade-size-composition-grammar-0006-2026-07-13.json"
)
V71_G6_SIGNAL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0006/v71_trade_size_composition_signal_manifest.json"
)
V71_G6_FUNNEL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0006/v71_trade_size_composition_funnel_result.json"
)
V71_G6_TRIPWIRE_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0006/v71_trade_size_composition_tripwire_result.json"
)
V71_G6_POWER_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0006/v71_trade_size_composition_power_audit_result.json"
)
V71_UNDERPOWERED_SELECTION_RELATIVE_PATH = Path(
    "reports/v7_1/combine_research_0001/"
    "v71_underpowered_combine_selection_manifest.json"
)
V71_UNDERPOWERED_DIAGNOSTIC_RELATIVE_PATH = Path(
    "reports/v7_1/combine_research_0001/"
    "v71_underpowered_combine_diagnostic_result.json"
)
V71_G7_GRAMMAR_RELATIVE_PATH = Path(
    "WORM/v7.1-flow-sign-sequence-grammar-0007-2026-07-13.json"
)
V71_G7_SIGNAL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0007/v71_flow_sign_sequence_signal_manifest.json"
)
V71_G7_FUNNEL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0007/v71_flow_sign_sequence_funnel_result.json"
)
V71_G7_TRIPWIRE_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0007/v71_flow_sign_sequence_tripwire_result.json"
)
V71_G8_GRAMMAR_RELATIVE_PATH = Path(
    "WORM/v7.1-intraminute-flow-grammar-0008-2026-07-13.json"
)
V71_G8_FEATURE_MANIFEST_RELATIVE_PATH = Path(
    "data/manifests/v7_d1_intraminute_flow_v1.json"
)
V71_G8_SIGNAL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0008/v71_intraminute_flow_signal_manifest.json"
)
V71_G8_FUNNEL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0008/v71_intraminute_flow_funnel_result.json"
)
V71_G8_TRIPWIRE_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0008/v71_intraminute_flow_tripwire_result.json"
)
V71_G8_POWER_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0008/v71_intraminute_flow_power_audit_result.json"
)
V71_G9_GRAMMAR_RELATIVE_PATH = Path(
    "WORM/v7.1-aggressor-run-topology-grammar-0009-2026-07-13.json"
)
V71_G9_FEATURE_MANIFEST_RELATIVE_PATH = Path(
    "data/manifests/v7_d1_aggressor_run_topology_v1.json"
)
V71_G9_SIGNAL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0009/v71_aggressor_run_topology_signal_manifest.json"
)
V71_G9_FUNNEL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0009/v71_aggressor_run_topology_funnel_result.json"
)
V71_G9_TRIPWIRE_RELATIVE_PATH = Path(
    "reports/v7_1/discovery_0009/v71_aggressor_run_topology_tripwire_result.json"
)
V72_POLICY_RELATIVE_PATH = Path(
    "WORM/v7.2-pareto-crossfit-account-policy-0001-2026-07-13.json"
)
V72_SEMANTICS_RELATIVE_PATH = Path(
    "reports/v7_2/semantics/v72_combine_semantics_audit_result.json"
)
V72_COMPONENT_BANK_WORM_RELATIVE_PATH = Path(
    "WORM/v7.2-component-bank-0001-2026-07-13.json"
)
V72_COMPONENT_BANK_RELATIVE_PATH = Path(
    "reports/v7_2/component_bank/v72_component_bank_result.json"
)
V72_SEARCH_WORM_RELATIVE_PATH = Path(
    "WORM/v7.2-static-basket-search-0001-2026-07-13.json"
)
V72_SEARCH_FREEZE_RELATIVE_PATH = Path(
    "reports/v7_2/crossfit_0001/v72_basket_search_freeze_result.json"
)
V72_CROSS_FIT_RELATIVE_PATH = Path(
    "reports/v7_2/crossfit_0001/v72_static_basket_crossfit_result.json"
)
V72_G10_GRAMMAR_RELATIVE_PATH = Path(
    "WORM/v7.2-flow-impact-relaxation-grammar-0010-2026-07-13.json"
)
V72_G10_VALIDATION_ADDENDUM_RELATIVE_PATH = Path(
    "WORM/v7.2-flow-impact-relaxation-validation-addendum-0010-2026-07-13.json"
)
V72_G10_TRIPWIRE_POLICY_RELATIVE_PATH = Path(
    "WORM/v7.2-flow-impact-relaxation-tripwire-0010-2026-07-13.json"
)
V72_G10_POWER_FREEZE_RELATIVE_PATH = Path(
    "WORM/v7.2-flow-impact-relaxation-power-audit-0010-2026-07-13.json"
)
V72_G10_SIGNAL_RELATIVE_PATH = Path(
    "reports/v7_2/discovery_0010/v72_flow_impact_relaxation_signal_manifest.json"
)
V72_G10_FUNNEL_RELATIVE_PATH = Path(
    "reports/v7_2/discovery_0010/v72_flow_impact_relaxation_funnel_result.json"
)
V72_G10_TRIPWIRE_RELATIVE_PATH = Path(
    "reports/v7_2/discovery_0010/v72_flow_impact_relaxation_tripwire_result.json"
)
V72_G10_RECONCILIATION_RELATIVE_PATH = Path(
    "reports/v7_2/discovery_0010/v72_g10_multiplicity_reconciliation.json"
)
V72_G10_POWER_RELATIVE_PATH = Path(
    "reports/v7_2/discovery_0010/v72_flow_impact_relaxation_power_audit_result.json"
)
V72_G11_GRAMMAR_RELATIVE_PATH = Path(
    "WORM/v7.2-trade-arrival-renewal-grammar-0011-2026-07-13.json"
)
V72_G11_VALIDATION_ADDENDUM_RELATIVE_PATH = Path(
    "WORM/v7.2-trade-arrival-renewal-validation-addendum-0011-2026-07-13.json"
)
V72_G11_TRIPWIRE_POLICY_RELATIVE_PATH = Path(
    "WORM/v7.2-trade-arrival-renewal-tripwire-0011-2026-07-13.json"
)
V72_G11_FEATURE_MANIFEST_RELATIVE_PATH = Path(
    "data/manifests/v7_d1_trade_arrival_renewal_v1.json"
)
V72_G11_SIGNAL_RELATIVE_PATH = Path(
    "reports/v7_2/discovery_0011/v72_trade_arrival_renewal_signal_manifest.json"
)
V72_G11_FUNNEL_RELATIVE_PATH = Path(
    "reports/v7_2/discovery_0011/v72_trade_arrival_renewal_funnel_result.json"
)
V72_G11_TRIPWIRE_RELATIVE_PATH = Path(
    "reports/v7_2/discovery_0011/v72_trade_arrival_renewal_tripwire_result.json"
)
V72_G12_GRAMMAR_RELATIVE_PATH = Path(
    "WORM/v7.2-executed-price-occupancy-grammar-0012-2026-07-13.json"
)
V72_G12_TRIPWIRE_POLICY_RELATIVE_PATH = Path(
    "WORM/v7.2-executed-price-occupancy-tripwire-0012-2026-07-13.json"
)
V72_G12_VALIDATION_ADDENDUM_RELATIVE_PATH = Path(
    "WORM/v7.2-executed-price-occupancy-validation-addendum-0012-2026-07-13.json"
)
V72_G12_FEATURE_MANIFEST_RELATIVE_PATH = Path(
    "data/manifests/v7_d1_executed_price_occupancy_v1.json"
)
V72_G12_SIGNAL_RELATIVE_PATH = Path(
    "reports/v7_2/discovery_0012/v72_executed_price_occupancy_signal_manifest.json"
)
V72_G12_FUNNEL_RELATIVE_PATH = Path(
    "reports/v7_2/discovery_0012/v72_executed_price_occupancy_funnel_result.json"
)
V72_G12_TRIPWIRE_RELATIVE_PATH = Path(
    "reports/v7_2/discovery_0012/v72_executed_price_occupancy_tripwire_result.json"
)
V72_FROZEN_HASHES = {
    str(V72_POLICY_RELATIVE_PATH): "94f4ad89a2ae2ea347f1fce4a9cb4682690652429f34e42e72edf79e03da6677",
    str(V72_SEMANTICS_RELATIVE_PATH): "08200f3009d9c0c474aa665575c0607b918097a172b7f68db1c7f7ed6ac58fbe",
    str(V72_COMPONENT_BANK_WORM_RELATIVE_PATH): "36987e68a670345c890e9d7d2d060263a13f1e94928563f777dfdc572773ba4c",
    str(V72_COMPONENT_BANK_RELATIVE_PATH): "93f02e4d41188d2def0d8532c9c74c1cfeb34ca700a3c56bc14ca12a8aaa3df8",
    str(V72_SEARCH_WORM_RELATIVE_PATH): "9d0fccf04203d75a7d1f0648ed0ad619882f3dad06ed1f55da3f97878e8b1f98",
    str(V72_SEARCH_FREEZE_RELATIVE_PATH): "44d989263581b57c8d5d10e959f82f6089f5b3c1781799cbd6348bf2fdabdb76",
    str(V72_CROSS_FIT_RELATIVE_PATH): "26434b641cb2c908f384e35aa646a49e1f2fcd11b461cd11644ceec0d5840f8a",
}
V72_G10_FROZEN_HASHES = {
    str(V72_G10_GRAMMAR_RELATIVE_PATH): "2513038d857e3599449fbe347bec1d4738ae2adfe9558d5daf4c2c26d322e1cd",
    str(V72_G10_VALIDATION_ADDENDUM_RELATIVE_PATH): "d3f0505f036be27d46c6eaa712dc3f785f1902a71dd402a3ad4c9d591ae16421",
    str(V72_G10_TRIPWIRE_POLICY_RELATIVE_PATH): "77f861b94cc6a190b86508c4adcc15b7e67c5e5af1d22e66e51110714f52741d",
    str(V72_G10_POWER_FREEZE_RELATIVE_PATH): "4cb9220c242d41af1e3a5cf0d077d122eb37566cf53423100e127dc718a91004",
    str(V72_G10_SIGNAL_RELATIVE_PATH): "480b4c3434dadf0f48f46d7434caa0e392b625f59791f391d25be6f5c398ddef",
    str(V72_G10_FUNNEL_RELATIVE_PATH): "ebe44080d1bb953048e768c54de564dcf88789f6f0e4ff9cbd7f8225094edc87",
    str(V72_G10_TRIPWIRE_RELATIVE_PATH): "1e6f26a2e5a8aa4175e67b403b6f1e523f4834407dbc9b27dcf689c1093f9eba",
    str(V72_G10_RECONCILIATION_RELATIVE_PATH): "8978542d5b250c57de7c5f623b18ec9913abe8e070b6a909eec1b404c6c7264a",
    str(V72_G10_POWER_RELATIVE_PATH): "e141def05a8e0b32459308f2b330413ec27df53334c72ad9ba1ab6741d6f809c",
}
V72_G11_FROZEN_HASHES = {
    str(V72_G11_GRAMMAR_RELATIVE_PATH): "d69f021bf4de5b4e5a0fe92d318eba9f00b08c80d99cb3941c43daab4a6b10c2",
    str(V72_G11_VALIDATION_ADDENDUM_RELATIVE_PATH): "6808fd5d97adba4b1b0539687e6db0429aecb9ef44ab52c1e871dd13b29cf39f",
    str(V72_G11_TRIPWIRE_POLICY_RELATIVE_PATH): "1e42cb7fcb70542096b0ddab559ade74bf1e9cb4cfd49150e643a804d41db4dc",
    str(V72_G11_FEATURE_MANIFEST_RELATIVE_PATH): "e31407d772c4540d786efc8016ebb15333ccc06e420d4282a44a31b0658d4c26",
    str(V72_G11_SIGNAL_RELATIVE_PATH): "6225c8067ac813e47f03e5037ce08ba0dfd7392f81421d04f0440c2795260944",
    str(V72_G11_FUNNEL_RELATIVE_PATH): "c1de25fbbd7ca35a96a54c661da686c64cfc2b2b4f46491640e68a7d3a88fff6",
    str(V72_G11_TRIPWIRE_RELATIVE_PATH): "80a4c4a90fbdc4d12d5ba7694683ace5e0df3902b9f529383c8f3a0c2df6ae34",
}
V72_G12_FROZEN_HASHES = {
    str(V72_G12_GRAMMAR_RELATIVE_PATH): "d0fa4eb200f47e1df9d3323c09f9e0c3729802a001b9c946bdf43824846a4c0c",
    str(V72_G12_TRIPWIRE_POLICY_RELATIVE_PATH): "0a40db491ae3cc28fedfe00107b3e3ba417896f4533460e3fabc1c1a1c664786",
    str(V72_G12_VALIDATION_ADDENDUM_RELATIVE_PATH): "badd801bf80dbeba77424e5d73de9c3c1187a9a8d52e38da96127a2de88f2dbd",
    str(V72_G12_FEATURE_MANIFEST_RELATIVE_PATH): "e6c41b6f0b819668798af90aaf7246c07be499badd338cb87bd646f7f95f5408",
    str(V72_G12_SIGNAL_RELATIVE_PATH): "8c6796df76cea34ca87c83e01d5291bc8b5f401d86acf4ce7090891b9a1955d0",
    str(V72_G12_FUNNEL_RELATIVE_PATH): "c250331d1d1cac086694459ec0a667096ceb06923bbb1c68edb1706f67fcd6d3",
    str(V72_G12_TRIPWIRE_RELATIVE_PATH): "8da2418520231d37fd96af3a4a3b60756c6585fd40a19542d78b8f57701c8fbe",
}
V71_FROZEN_HASHES = {
    "MISSION_CONTRACT_AMENDMENT_001_ORDERFLOW.md": "981523c00831fac4dee02aa9bd908be6781ecec63a2a3fa573832206ea173eeb",
    str(V71_POLICY_RELATIVE_PATH): "d745ac9ca51049ccc2f7f1f97d3593cf49231c92a8873737e350e380170f916c",
    "WORM/v7.1-event-mechanism-grammar-0001-2026-07-12.json": "e1c8de955302da2be836bbcebf2bfedc07768b2d9b987ea32258a85a2b0caf8a",
    "WORM/v7.1-powered-promotion-minimum-2026-07-12.json": "3e0211c6a5acea81713431802fc1576da4d5be2a0cc37bf900cd02eabd68c6fa",
    str(V71_G9_GRAMMAR_RELATIVE_PATH): "05ff83f0fbf902381371d3d840ce7393adadfa8e51d6c75e51a76c12a275bce2",
    "WORM/v7.1-aggressor-run-topology-tripwire-0009-2026-07-13.json": "b26460dc7cf277acf68369ea244b24f7bec743d260863283ff3a711093a3318f",
    "WORM/v7.1-aggressor-run-topology-power-audit-0001-2026-07-13.json": "b40a7d359587eeb87eb1e6910158799bf0d02b7a80562c7a26165286db08bcc6",
    str(V71_G2_GRAMMAR_RELATIVE_PATH): "ef44e6e72c42b2ed4b7228f3addbd2f182e3e51bcfb619aa4c0a2102db6d3566",
    "WORM/v7.1-opportunity-density-tripwire-0002-2026-07-12.json": "8e1b7e511f99e1f108a113bb80a69d4985d498ed9d78d2d049e9468a6afdcacf",
    str(V71_CONFIRMATION_QUEUE_RELATIVE_PATH): "23c2925253887a9b86699aac9fa71072fc28848087cb38cc9624bb78751ee0b1",
    "MISSION_CONTRACT_AMENDMENT_002_POWER_AWARE.md": "f41caaa9b4a1ad17c7436f4594ed669c3784321d4afac805dee0b87f79a02caf",
    str(V71_G3_GRAMMAR_RELATIVE_PATH): "df9ffd7c6c87707838f53c30e474d7477bf17532ba29bffc1baa2b2a5bd0903f",
    "WORM/v7.1-event-time-tripwire-0003-2026-07-12.json": "6119d44841456f5a13798cdb4e310de9de6bed388f032b6b3dab2fc00a94229b",
    "WORM/v7.1-power-aware-candidate-freeze-0001-2026-07-12.json": "b66e462989213356106f0cbcd88d31ba4547a61f9900eb1de3e6010cb3d35d83",
    "WORM/v7.1-candidate-specific-power-policy-0001-2026-07-12.json": "39f60b4e402c0a40ccc39b5429e0e2cc2dcc88a80592cd28b05c86abed616673",
    "WORM/v7.1-event-time-executable-diagnostic-0001-2026-07-12.json": "058278f8111dc35d6f19ef484ed4b0674f5bb323dbb2a941ebd9d7971080c944",
    "WORM/v7.1-independent-confirmation-queue-0002-2026-07-12.json": "ce9d10ef013affed665f59aded39d051356ac5ddefbce7273b9612cb4bc7b82b",
    str(V71_G4_GRAMMAR_RELATIVE_PATH): "9341e576b4090f2626079f1678170ad738b523ca10394b89261292a3ee1b2c0e",
    "WORM/v7.1-cross-clock-flow-tripwire-0004-2026-07-12.json": "36504626470927c9ee7883c7eac79c7524dee2342cf30ace5277ce1c3629176a",
    "WORM/v7.1-cross-clock-flow-power-audit-0002-2026-07-12.json": "5fc399bca8a7beab2bcf4972dca99c325f4213f309d28c2427411288b9d92260",
    str(V71_CONFIRMATION_QUEUE_V3_RELATIVE_PATH): "e88641bd49bcf73f1d3007488db9baf09f74b06719255182010622b14c7a9ce3",
    str(V71_G5_GRAMMAR_RELATIVE_PATH): "27a937a112dd4963402f8c12feb69cf9cd347b020ce47396cfffff0e253726c2",
    "WORM/v7.1-cross-clock-speed-leadership-tripwire-0005-2026-07-13.json": "03513d2b8f9dda7bd4208581ea96d9c5ab1f8c6ca21444fc80bbfe95deb67d8d",
    "WORM/v7.1-cross-clock-speed-leadership-verdict-0005-2026-07-13.json": "d72b2f1a5b64a6e33219f6a503c36b79834254d38cb303df36805bcf1f4931d5",
    str(V71_G6_GRAMMAR_RELATIVE_PATH): "3913324e3ab9b707461da4c32a5c4bddfa025af98c5a5f6ba942b7ae0ba7cc29",
    "WORM/v7.1-trade-size-composition-tripwire-0006-2026-07-13.json": "c10f9b671cd6ddb0349de05673c5854e3b37776001224a40b218c0a39b39dea9",
    "WORM/v7.1-trade-size-composition-power-audit-0001-2026-07-13.json": "00c09d4e54d977c6f28df9445019957f40cfa564ea571ecade370064bcd15922",
    "MISSION_CONTRACT_AMENDMENT_003_RESEARCH_STAGE_COMBINE.md": "ffca7c85d685267b81abc45e33b0d74dd12b196acb1b26d54c6ddd3f50f34324",
    "WORM/v7.1-underpowered-combine-research-policy-0001-2026-07-13.json": "33193b3afaf662a7a2b1fe4bcdfb5f9aa2868f6afec55f365e5ea421cd1f3f88",
    "WORM/v7.1-underpowered-combine-cohort-0001-2026-07-13.json": "a2973de8e8ad11607d807b7cea5216db9f860dedff3ade815f34fd360b1c28d5",
    "WORM/v7.1-underpowered-combine-diagnostic-verdict-0001-2026-07-13.json": "7116dba7d9a50e9e109489b55f6fbef32992fd7d96e0c767270d84c127b7fa39",
    str(V71_G7_GRAMMAR_RELATIVE_PATH): "4cb89b0e774f754037fde8a6f86703cda0047eefcd01174e1f65bb8d37fc45ab",
    "WORM/v7.1-flow-sign-sequence-tripwire-0007-2026-07-13.json": "c7806c7ac4c512a05ca468388857419bd87a4a316967366ac8892ca38d25ff7a",
    str(V71_G8_GRAMMAR_RELATIVE_PATH): "36f5d4f8dd2582979d809925782881fb1e159d23ddfbd50dc6a9d348cf5c18dc",
    "WORM/v7.1-intraminute-flow-tripwire-0008-2026-07-13.json": "e4968cc24c5574a42ace695a8ec65f56578d5ca66a8954eac1239efbbfa4a535",
    "WORM/v7.1-intraminute-flow-power-audit-0001-2026-07-13.json": "3f1b8fb8eca73bebf5582071c9c79b75971a5d2d57afda9c006ea9edff9e5104",
}


class V7ControllerIntegrityError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class V7ControllerConfig:
    project_root: str = "."
    state_dir: str = "mission/state"
    sleep_seconds: float = 15.0
    checkpoint_every_steps: int = 25
    persistent: bool = True
    maximum_steps: int | None = None
    no_live_trading: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def classify_v7_action(project_root: str | Path) -> dict[str, Any]:
    root = Path(project_root).resolve()
    if (root / CAMPAIGN_CONFIG_RELATIVE_PATH).is_file():
        predecessor = _classify_v72_action(root)
        return classify_economic_evolution_action(root, predecessor)
    if (root / V72_POLICY_RELATIVE_PATH).is_file():
        return _classify_v72_action(root)
    if (root / V71_POLICY_RELATIVE_PATH).is_file():
        return _classify_v71_action(root)
    tribunal_path = root / D1_TRIBUNAL_RELATIVE_PATH
    if not tribunal_path.is_file():
        return {
            "action_type": "D1_CANDIDATE_TRIBUNAL_PENDING",
            "phase": "D",
            "progressed": False,
            "reason": "The frozen D1 tribunal has no atomic result yet.",
        }
    tribunal = _load_json(tribunal_path)
    verdict = str(tribunal.get("verdict") or "")
    selected = tuple(
        str(value)
        for value in tribunal.get("selected_shadow_queue_candidate_ids") or ()
    )
    if verdict == "GREEN" and selected:
        fiche_root = root / "WORM" / "candidates"
        missing = [
            candidate_id
            for candidate_id in selected
            if not (fiche_root / f"{candidate_id}.json").is_file()
        ]
        if missing:
            return {
                "action_type": "CANDIDATE_FICHE_FREEZE_REQUIRED",
                "phase": "3",
                "progressed": False,
                "candidate_ids": list(selected),
                "missing_candidate_fiches": missing,
                "reason": "WORM fiches must precede any forward-gap ingestion.",
            }
        boundary = root / "mission/state/v7_forward_boundary_manifest.json"
        if not boundary.is_file():
            return {
                "action_type": "FORWARD_BOUNDARY_MANIFEST_REQUIRED",
                "phase": "3",
                "progressed": False,
                "candidate_ids": list(selected),
                "reason": "Candidate fiches exist but the append-only boundary is absent.",
            }
        return {
            "action_type": "FORWARD_FEED_READY",
            "phase": "3",
            "progressed": True,
            "candidate_ids": list(selected),
            "boundary_manifest": str(boundary),
            "reason": "Frozen candidates may enter the post-fiche feed path.",
        }
    if verdict == "NULL" and not selected:
        graveyard = root / "mission/state/graveyard.db"
        source_scope = (
            "HYDRA_V7_GRAMMAR:hydra_v7_d1_microstructure_grammar_0001"
        )
        indexed = False
        if graveyard.is_file():
            conn = sqlite3.connect(f"file:{graveyard}?mode=ro", uri=True)
            try:
                indexed = (
                    conn.execute(
                        "SELECT COUNT(*) FROM class_tombstones WHERE source_scope=?",
                        (source_scope,),
                    ).fetchone()[0]
                    > 0
                )
            finally:
                conn.close()
        if not indexed:
            return {
                "action_type": "D1_CLASS_TOMBSTONE_REQUIRED",
                "phase": "4",
                "progressed": False,
                "reason": "The null D1 classes are not yet indexed in the class-only graveyard.",
            }
        return {
            "action_type": "NEW_HYPOTHESIS_GRAMMAR_REQUIRED",
            "phase": "4",
            "progressed": False,
            "reason": "D1 classes are tombstoned; the next economic hypothesis must be WORM before generation.",
        }
    raise V7ControllerIntegrityError(
        "D1 tribunal has an unsupported or internally inconsistent verdict"
    )


def _classify_v71_action(root: Path) -> dict[str, Any]:
    required = (
        (V71_POWER_RELATIVE_PATH, "V71_POWER_AUDIT_REQUIRED"),
        (V71_POWER_EXTENSION_RELATIVE_PATH, "V71_POWER_EXTENSION_REQUIRED"),
        (V71_SIGNAL_RELATIVE_PATH, "V71_SIGNAL_MANIFEST_REQUIRED"),
        (V71_FUNNEL_RELATIVE_PATH, "V71_DEVELOPMENT_FUNNEL_REQUIRED"),
        (V71_FORENSICS_RELATIVE_PATH, "V71_FORENSICS_REQUIRED"),
    )
    for path, action in required:
        if not (root / path).is_file():
            return {
                "action_type": action,
                "phase": "4",
                "progressed": False,
                "required_path": str(path),
                "reason": "The preregistered V7.1 evidence sequence is incomplete.",
            }
    power = _load_json(root / V71_POWER_RELATIVE_PATH)
    extension = _load_json(root / V71_POWER_EXTENSION_RELATIVE_PATH)
    signal = _load_json(root / V71_SIGNAL_RELATIVE_PATH)
    funnel = _load_json(root / V71_FUNNEL_RELATIVE_PATH)
    forensics = _load_json(root / V71_FORENSICS_RELATIVE_PATH)
    if power.get("verdict") != "RED" or extension.get("verdict") != "GREEN":
        raise V7ControllerIntegrityError("V7.1 power evidence sequence is inconsistent")
    if int(signal.get("candidate_count") or 0) != 256:
        raise V7ControllerIntegrityError("V7.1 signal manifest candidate count drift")
    powered = int(funnel.get("powered_walk_forward_candidate_count") or 0)
    positive = int(funnel.get("walk_forward_positive_count") or 0)
    if powered > 0:
        return {
            "action_type": "V71_STAGE3_COHORT_FREEZE_REQUIRED",
            "phase": "4",
            "progressed": True,
            "powered_candidate_count": powered,
            "walk_forward_positive_count": positive,
            "reason": "Powered walk-forward candidates must be frozen before nulls and DSR/BH.",
        }
    if forensics.get("MINI_MICRO_DIVERGENCE", {}).get("mechanism") != "MECHANISM_CONFIRMED_DEAD":
        raise V7ControllerIntegrityError("V7.1 intra-product artifact status drift")
    if (root / V71_G2_GRAMMAR_RELATIVE_PATH).is_file():
        return _classify_v71_g2_action(root, positive)
    return {
        "action_type": "V71_OPPORTUNITY_DENSITY_GRAMMAR_REQUIRED",
        "phase": "4",
        "progressed": True,
        "walk_forward_positive_count": positive,
        "powered_candidate_count": powered,
        "minimum_powered_events": int(extension["minimum_required_event_count"]),
        "next_experiment_id": "hydra_v7_1_opportunity_density_grammar_0002",
        "next_experiment_state": "PREREGISTRATION_REQUIRED",
        "new_data_purchase_authorized": False,
        "reason": (
            "Eleven distinct formulations are walk-forward positive but below "
            "the frozen 320-event power minimum; expand opportunity coverage "
            "structurally without parameter tuning or new data."
        ),
    }


def _classify_v71_g2_action(root: Path, prior_positive: int) -> dict[str, Any]:
    required = (
        (V71_G2_SIGNAL_RELATIVE_PATH, "V71_G2_SIGNAL_MANIFEST_REQUIRED"),
        (V71_G2_FUNNEL_RELATIVE_PATH, "V71_G2_DEVELOPMENT_FUNNEL_REQUIRED"),
        (V71_G2_TRIPWIRE_RELATIVE_PATH, "V71_G2_TRIPWIRE_REQUIRED"),
        (V71_CONFIRMATION_QUEUE_RELATIVE_PATH, "V71_G2_CONFIRMATION_QUEUE_REQUIRED"),
    )
    for path, action in required:
        if not (root / path).is_file():
            return {
                "action_type": action,
                "phase": "4",
                "progressed": False,
                "required_path": str(path),
                "new_data_purchase_authorized": False,
                "reason": "The preregistered opportunity-density evidence sequence is incomplete.",
            }
    hashes = {
        V71_G2_SIGNAL_RELATIVE_PATH: "c90a2321fc66e114d65dd533d077ec04308ae714369e28b82f5d9e996dd7fa24",
        V71_G2_FUNNEL_RELATIVE_PATH: "2a45c4da55875f90438cd6cb19f1ce79ec8de7d934f7a442e78000364aff5897",
        V71_G2_TRIPWIRE_RELATIVE_PATH: "dddabdad7e828e84bbee974dc47432a1a90b2a1989d26a44d48bf88cef91cbb2",
    }
    drift = [str(path) for path, expected in hashes.items() if _sha256(root / path) != expected]
    if drift:
        raise V7ControllerIntegrityError(
            "V7.1 opportunity-density evidence drift: " + ",".join(drift)
        )
    signal = _load_json(root / V71_G2_SIGNAL_RELATIVE_PATH)
    funnel = _load_json(root / V71_G2_FUNNEL_RELATIVE_PATH)
    tripwire = _load_json(root / V71_G2_TRIPWIRE_RELATIVE_PATH)
    queue = _load_json(root / V71_CONFIRMATION_QUEUE_RELATIVE_PATH)
    if int(signal.get("candidate_count") or 0) != 128:
        raise V7ControllerIntegrityError("V7.1 G2 candidate count drift")
    if int(funnel.get("raw_global_N_trials") or 0) != 262_356:
        raise V7ControllerIntegrityError("V7.1 G2 funnel multiplicity drift")
    if tripwire.get("verdict") not in {
        "GREEN_NULL_ADJUSTED_BASELINE",
        "ARTEFACT_GEOMETRY_ONLY",
        "BLOCKED_UNDERPOWERED",
    }:
        raise V7ControllerIntegrityError("V7.1 G2 tripwire verdict drift")
    if tripwire.get("verdict") != "GREEN_NULL_ADJUSTED_BASELINE":
        return {
            "action_type": "V71_G2_GEOMETRY_OR_POWER_BLOCKED",
            "phase": "4",
            "progressed": True,
            "tripwire_verdict": tripwire.get("verdict"),
            "new_data_purchase_authorized": False,
            "reason": "The opportunity-density grammar cannot advance beyond its permanent tripwire.",
        }
    powered = int(funnel.get("powered_walk_forward_candidate_count") or 0)
    if powered:
        return {
            "action_type": "V71_G2_POWERED_COHORT_FREEZE_REQUIRED",
            "phase": "4",
            "progressed": True,
            "powered_candidate_count": powered,
            "tripwire_verdict": tripwire["verdict"],
            "new_data_purchase_authorized": False,
            "reason": "Powered G2 candidates may proceed to preregistered relevant nulls.",
        }
    candidates = list(queue.get("candidates") or [])
    if len(candidates) != 3 or queue.get("queue_status") != "QUEUED_NO_DATA_PURCHASE_AUTHORIZED_IN_V7_1":
        raise V7ControllerIntegrityError("V7.1 independent confirmation queue drift")
    if (root / V71_G3_GRAMMAR_RELATIVE_PATH).is_file():
        return _classify_v71_power_aware_action(
            root,
            prior_positive=prior_positive,
            g2_positive=int(funnel.get("walk_forward_positive_count") or 0),
        )
    return {
        "action_type": "V71_CONFIRMATION_QUEUE_FROZEN_DISCOVERY_CONTINUES",
        "phase": "4",
        "progressed": True,
        "prior_walk_forward_positive_count": prior_positive,
        "g2_walk_forward_positive_count": int(
            funnel.get("walk_forward_positive_count") or 0
        ),
        "g2_powered_candidate_count": 0,
        "confirmation_candidate_count": len(candidates),
        "confirmation_candidate_ids": [str(row["candidate_id"]) for row in candidates],
        "tripwire_verdict": tripwire["verdict"],
        "tripwire_NULL_RATIO": float(tripwire["NULL_RATIO"]),
        "tripwire_evidence_strength": tripwire["evidence_strength"],
        "next_experiment_id": "hydra_v7_1_distinct_event_time_grammar_0003",
        "next_experiment_state": "PREREGISTRATION_REQUIRED",
        "new_data_purchase_authorized": False,
        "shadow_admission_authorized": False,
        "reason": (
            "Three G2 mechanisms remain underpowered and are frozen for future "
            "independent confirmation; controlled discovery must move to a "
            "distinct event-time class without buying data."
        ),
    }


def _classify_v71_power_aware_action(
    root: Path,
    *,
    prior_positive: int,
    g2_positive: int,
) -> dict[str, Any]:
    required = (
        (V71_G3_SIGNAL_RELATIVE_PATH, "V71_G3_SIGNAL_MANIFEST_REQUIRED"),
        (V71_G3_FUNNEL_RELATIVE_PATH, "V71_G3_DEVELOPMENT_FUNNEL_REQUIRED"),
        (V71_G3_TRIPWIRE_RELATIVE_PATH, "V71_G3_TRIPWIRE_REQUIRED"),
        (
            Path("MISSION_CONTRACT_AMENDMENT_002_POWER_AWARE.md"),
            "V71_POWER_AWARE_AMENDMENT_REQUIRED",
        ),
        (
            Path("WORM/v7.1-power-aware-candidate-freeze-0001-2026-07-12.json"),
            "V71_POWER_AWARE_CANDIDATE_FREEZE_REQUIRED",
        ),
        (
            Path("WORM/v7.1-candidate-specific-power-policy-0001-2026-07-12.json"),
            "V71_CANDIDATE_SPECIFIC_POWER_POLICY_REQUIRED",
        ),
        (
            V71_POWER_AWARE_CALIBRATION_RELATIVE_PATH,
            "V71_CANDIDATE_SPECIFIC_POWER_CALIBRATION_REQUIRED",
        ),
        (V71_POWER_AWARE_AUDIT_RELATIVE_PATH, "V71_POWER_AWARE_AUDIT_REQUIRED"),
        (
            Path("WORM/v7.1-event-time-executable-diagnostic-0001-2026-07-12.json"),
            "V71_EVENT_TIME_EXECUTABLE_FREEZE_REQUIRED",
        ),
        (
            V71_ROLLING_DIAGNOSTIC_RELATIVE_PATH,
            "V71_EVENT_TIME_ROLLING_DIAGNOSTIC_REQUIRED",
        ),
    )
    for path, action in required:
        if not (root / path).is_file():
            return {
                "action_type": action,
                "phase": "4",
                "progressed": False,
                "required_path": str(path),
                "broad_D1_generation_authorized": False,
                "new_data_purchase_authorized": False,
                "reason": (
                    "The principal-authorized power-aware conversion sequence "
                    "must complete before any further broad D1 grammar."
                ),
            }
    hashes = {
        V71_G3_SIGNAL_RELATIVE_PATH: "e515a0ab84600edfd8552c46b3471f77d0ba17ad3b761cf7757d5fdaa89c736d",
        V71_G3_FUNNEL_RELATIVE_PATH: "22f9816aeb2bae8734571dcd84485f0ccbfdb21b4735cbe0ed11356dcbc0358b",
        V71_G3_TRIPWIRE_RELATIVE_PATH: "ae22d7a48eef4ef1804fb81c26453dafc1efdcd138c09c04fd48766cbe1a5b44",
        V71_POWER_AWARE_CALIBRATION_RELATIVE_PATH: "edd3bcdb2ec56bcef2830be7783d74df02041a57b4234b76c1c1803e40b647f5",
        V71_POWER_AWARE_AUDIT_RELATIVE_PATH: "f0eb23117b5703b3d50823365cff7cf9d37c7faeb6ce5628ca7e6c19f04c930b",
        V71_ROLLING_DIAGNOSTIC_RELATIVE_PATH: "0c4203c04e2d0cb598bd6ae485cd884a732287f8d74c4237645ced02f5202bbd",
    }
    drift = [
        str(path)
        for path, expected in hashes.items()
        if _sha256(root / path) != expected
    ]
    if drift:
        raise V7ControllerIntegrityError(
            "V7.1 power-aware evidence drift: " + ",".join(drift)
        )
    signal = _load_json(root / V71_G3_SIGNAL_RELATIVE_PATH)
    funnel = _load_json(root / V71_G3_FUNNEL_RELATIVE_PATH)
    tripwire = _load_json(root / V71_G3_TRIPWIRE_RELATIVE_PATH)
    calibration = _load_json(root / V71_POWER_AWARE_CALIBRATION_RELATIVE_PATH)
    audit = _load_json(root / V71_POWER_AWARE_AUDIT_RELATIVE_PATH)
    rolling = _load_json(root / V71_ROLLING_DIAGNOSTIC_RELATIVE_PATH)
    if int(signal.get("candidate_count") or 0) != 128:
        raise V7ControllerIntegrityError("V7.1 G3 candidate count drift")
    if int(funnel.get("walk_forward_positive_count") or 0) != 2:
        raise V7ControllerIntegrityError("V7.1 G3 walk-forward count drift")
    if tripwire.get("verdict") != "ARTEFACT_GEOMETRY_ONLY":
        raise V7ControllerIntegrityError("V7.1 G3 tripwire verdict drift")
    if calibration.get("verdict") != "GREEN":
        raise V7ControllerIntegrityError("V7.1 power-aware calibration is not GREEN")
    status_counts = dict(audit.get("status_counts") or {})
    if sum(int(value) for value in status_counts.values()) != 16:
        raise V7ControllerIntegrityError("V7.1 power-aware candidate count drift")
    powered = list(audit.get("powered_candidate_ids") or [])
    if rolling.get("episode_power_status") != "INSUFFICIENT_EPISODE_STARTS":
        raise V7ControllerIntegrityError("V7.1 rolling episode power status drift")
    if rolling.get("scientific_status") != "BOUNDED_DIAGNOSTIC_ONLY_NO_PROMOTION":
        raise V7ControllerIntegrityError("V7.1 rolling scientific status drift")
    if powered:
        return {
            "action_type": "V71_POWERED_CANDIDATE_NULLS_DSR_BH_REQUIRED",
            "phase": "4",
            "progressed": True,
            "powered_candidate_ids": [str(value) for value in powered],
            "broad_D1_generation_authorized": False,
            "new_data_purchase_authorized": False,
            "shadow_admission_authorized": False,
            "reason": (
                "Powered walk-forward candidates require relevant nulls and "
                "campaign-level DSR/BH before any shadow decision."
            ),
        }
    if (root / V71_G4_GRAMMAR_RELATIVE_PATH).is_file():
        return _classify_v71_g4_action(
            root,
            prior_positive=prior_positive,
            g2_positive=g2_positive,
            g3_status_counts=status_counts,
        )
    return {
        "action_type": "V71_INDEPENDENT_CONFIRMATION_REQUIRED_LIMITED_DISCOVERY_ONLY",
        "phase": "4",
        "progressed": True,
        "walk_forward_positive_count": prior_positive + g2_positive + 2,
        "power_status_counts": status_counts,
        "powered_candidate_count": 0,
        "principal_named_diagnostic_count": len(
            audit.get("principal_named_bounded_diagnostic_ids") or []
        ),
        "rolling_episode_start_count": int(rolling["episode_start_count"]),
        "rolling_episode_power_status": rolling["episode_power_status"],
        "g3_tripwire_verdict": tripwire["verdict"],
        "g3_NULL_RATIO": float(tripwire["NULL_RATIO"]),
        "broad_D1_generation_authorized": False,
        "limited_structural_discovery_authorized": True,
        "conversion_priority": 0.95,
        "limited_discovery_allocation": 0.05,
        "new_data_purchase_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": "hydra_v7_1_independent_confirmation_planning_0001",
        "next_experiment_state": "FRESH_EVIDENCE_REQUIRED_NO_PURCHASE_IN_CURRENT_PHASE",
        "principal_blocker": (
            "No candidate satisfies the preregistered candidate-specific power "
            "policy; only five 20-day starts exist and G3 pass rates are geometry-contaminated."
        ),
        "reason": (
            "The sixteen frozen candidates are resolved under the calibrated "
            "policy. Independent fresh evidence is required; broad D1 generation "
            "remains paused and only limited distinct discovery may continue."
        ),
    }


def _classify_v71_g4_action(
    root: Path,
    *,
    prior_positive: int,
    g2_positive: int,
    g3_status_counts: Mapping[str, Any],
) -> dict[str, Any]:
    required = (
        (V71_G4_SIGNAL_RELATIVE_PATH, "V71_G4_SIGNAL_MANIFEST_REQUIRED"),
        (V71_G4_FUNNEL_RELATIVE_PATH, "V71_G4_DEVELOPMENT_FUNNEL_REQUIRED"),
        (V71_G4_TRIPWIRE_RELATIVE_PATH, "V71_G4_TRIPWIRE_REQUIRED"),
        (
            Path("WORM/v7.1-cross-clock-flow-power-audit-0002-2026-07-12.json"),
            "V71_G4_POWER_AUDIT_FREEZE_REQUIRED",
        ),
        (V71_G4_POWER_AUDIT_RELATIVE_PATH, "V71_G4_POWER_AUDIT_REQUIRED"),
        (
            V71_CONFIRMATION_QUEUE_V3_RELATIVE_PATH,
            "V71_G4_CONFIRMATION_QUEUE_REQUIRED",
        ),
    )
    for path, action in required:
        if not (root / path).is_file():
            return {
                "action_type": action,
                "phase": "4",
                "progressed": False,
                "required_path": str(path),
                "broad_D1_generation_authorized": False,
                "new_data_purchase_authorized": False,
                "reason": (
                    "The bounded cross-clock conversion sequence must finish "
                    "before another structural discovery action."
                ),
            }
    hashes = {
        V71_G4_SIGNAL_RELATIVE_PATH: "35393b8bee755da085b9214fde3a89975221f1f4ac7915251ed3e8f596f758b8",
        V71_G4_FUNNEL_RELATIVE_PATH: "737e484c7fc51380ccaf588b7dc4bc76f3183530254219a2e621494d025cd828",
        V71_G4_TRIPWIRE_RELATIVE_PATH: "95a9ba4d548fb1fad18a0dae2f11d67bfcf50d4d280f14363c1f51d918a3aaa0",
        V71_G4_POWER_AUDIT_RELATIVE_PATH: "204b79bcc0f75b22351c638469f4be1bc84bfaf636d09c88e1462f2a67c62f67",
    }
    drift = [
        str(path)
        for path, expected in hashes.items()
        if _sha256(root / path) != expected
    ]
    if drift:
        raise V7ControllerIntegrityError(
            "V7.1 cross-clock evidence drift: " + ",".join(drift)
        )
    signal = _load_json(root / V71_G4_SIGNAL_RELATIVE_PATH)
    funnel = _load_json(root / V71_G4_FUNNEL_RELATIVE_PATH)
    tripwire = _load_json(root / V71_G4_TRIPWIRE_RELATIVE_PATH)
    audit = _load_json(root / V71_G4_POWER_AUDIT_RELATIVE_PATH)
    queue = _load_json(root / V71_CONFIRMATION_QUEUE_V3_RELATIVE_PATH)
    if int(signal.get("candidate_count") or 0) != 12:
        raise V7ControllerIntegrityError("V7.1 G4 candidate count drift")
    if int(funnel.get("walk_forward_positive_count") or 0) != 2:
        raise V7ControllerIntegrityError("V7.1 G4 walk-forward count drift")
    if tripwire.get("verdict") != "GREEN_NULL_ADJUSTED_BASELINE":
        raise V7ControllerIntegrityError("V7.1 G4 tripwire verdict drift")
    status_counts = dict(audit.get("status_counts") or {})
    if sum(int(value) for value in status_counts.values()) != 2:
        raise V7ControllerIntegrityError("V7.1 G4 power candidate count drift")
    powered = list(audit.get("powered_candidate_ids") or [])
    if powered:
        return {
            "action_type": "V71_G4_POWERED_ROLLING_COMBINE_REQUIRED",
            "phase": "4",
            "progressed": True,
            "powered_candidate_ids": [str(value) for value in powered],
            "broad_D1_generation_authorized": False,
            "new_data_purchase_authorized": False,
            "shadow_admission_authorized": False,
            "reason": (
                "Powered cross-clock candidates may enter the bounded rolling "
                "Combine diagnostic under the frozen power amendment."
            ),
        }
    if int(queue.get("cumulative_promising_underpowered_count") or 0) != 14:
        raise V7ControllerIntegrityError("V7.1 confirmation queue underpowered count drift")
    if int(queue.get("cumulative_fragile_retired_count") or 0) != 4:
        raise V7ControllerIntegrityError("V7.1 confirmation queue fragile count drift")
    cumulative_status = {
        "PROMISING_UNDERPOWERED": int(
            g3_status_counts.get("PROMISING_UNDERPOWERED", 0)
        )
        + int(status_counts.get("PROMISING_UNDERPOWERED", 0)),
        "WF_POSITIVE_BUT_FRAGILE": int(
            g3_status_counts.get("WF_POSITIVE_BUT_FRAGILE", 0)
        )
        + int(status_counts.get("WF_POSITIVE_BUT_FRAGILE", 0)),
    }
    if (root / V71_G5_GRAMMAR_RELATIVE_PATH).is_file():
        return _classify_v71_g5_action(
            root,
            prior_positive=prior_positive,
            g2_positive=g2_positive,
            cumulative_status=cumulative_status,
            confirmation_queue=queue,
        )
    return {
        "action_type": "V71_G4_INDEPENDENT_CONFIRMATION_REQUIRED_NO_PROMOTION",
        "phase": "4",
        "progressed": True,
        "walk_forward_positive_count": prior_positive + g2_positive + 2 + 2,
        "g4_stage0_valid_count": int(funnel["stage0_valid_novel_count"]),
        "g4_stage1_survivor_count": int(funnel["stage1_pass_count"]),
        "g4_walk_forward_positive_count": int(funnel["walk_forward_positive_count"]),
        "g4_tripwire_verdict": tripwire["verdict"],
        "g4_NULL_RATIO": float(tripwire["NULL_RATIO"]),
        "g4_tripwire_evidence_strength": tripwire["evidence_strength"],
        "g4_power_status_counts": status_counts,
        "cumulative_power_status_counts": cumulative_status,
        "powered_candidate_count": 0,
        "rolling_combine_promotions": 0,
        "confirmation_queue_underpowered_count": int(
            queue["cumulative_promising_underpowered_count"]
        ),
        "confirmation_queue_fragile_retired_count": int(
            queue["cumulative_fragile_retired_count"]
        ),
        "broad_D1_generation_authorized": False,
        "limited_structural_discovery_authorized": True,
        "conversion_priority": 0.95,
        "limited_discovery_allocation": 0.05,
        "new_data_purchase_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": "hydra_v7_1_independent_confirmation_planning_0002",
        "next_experiment_state": "FRESH_EVIDENCE_REQUIRED_NO_PURCHASE_IN_CURRENT_PHASE",
        "principal_blocker": (
            "The strongest new 60-minute cross-clock candidate has a positive "
            "D1 confidence interval but only 33.81% calibrated power versus the "
            "frozen 80% gate; no fresh confirmation window is authorized."
        ),
        "reason": (
            "The bounded grammar 0004 produced two walk-forward positives and a "
            "VERT_NET tripwire, but neither candidate passed the frozen power "
            "policy. The 30-minute version is fragile and the 60-minute version "
            "is queued unchanged for future independent confirmation."
        ),
    }


def _classify_v71_g5_action(
    root: Path,
    *,
    prior_positive: int,
    g2_positive: int,
    cumulative_status: Mapping[str, int],
    confirmation_queue: Mapping[str, Any],
) -> dict[str, Any]:
    required = (
        (V71_G5_SIGNAL_RELATIVE_PATH, "V71_G5_SIGNAL_MANIFEST_REQUIRED"),
        (V71_G5_FUNNEL_RELATIVE_PATH, "V71_G5_DEVELOPMENT_FUNNEL_REQUIRED"),
        (V71_G5_TRIPWIRE_RELATIVE_PATH, "V71_G5_TRIPWIRE_REQUIRED"),
        (
            Path(
                "WORM/v7.1-cross-clock-speed-leadership-verdict-0005-2026-07-13.json"
            ),
            "V71_G5_GEOMETRY_VERDICT_FREEZE_REQUIRED",
        ),
    )
    for path, action in required:
        if not (root / path).is_file():
            return {
                "action_type": action,
                "phase": "4",
                "progressed": False,
                "required_path": str(path),
                "broad_D1_generation_authorized": False,
                "new_data_purchase_authorized": False,
                "reason": (
                    "The bounded speed-leadership sequence must complete before "
                    "another limited structural class is considered."
                ),
            }
    hashes = {
        V71_G5_SIGNAL_RELATIVE_PATH: "fdae549a4542eae64b86208d295794b6ce5e58f4f791ae5a7d262da0ac5b3032",
        V71_G5_FUNNEL_RELATIVE_PATH: "06d9a1f5600bbe51fc516841482e406a26ab2fab49cf6e599e97311cb4a49648",
        V71_G5_TRIPWIRE_RELATIVE_PATH: "ea7755aa5ab60f78298557da422d497d98467457a24a259ff3f3a9919048fc1d",
    }
    drift = [
        str(path)
        for path, expected in hashes.items()
        if _sha256(root / path) != expected
    ]
    if drift:
        raise V7ControllerIntegrityError(
            "V7.1 speed-leadership evidence drift: " + ",".join(drift)
        )
    signal = _load_json(root / V71_G5_SIGNAL_RELATIVE_PATH)
    funnel = _load_json(root / V71_G5_FUNNEL_RELATIVE_PATH)
    tripwire = _load_json(root / V71_G5_TRIPWIRE_RELATIVE_PATH)
    verdict = _load_json(
        root
        / "WORM/v7.1-cross-clock-speed-leadership-verdict-0005-2026-07-13.json"
    )
    if int(signal.get("candidate_count") or 0) != 12:
        raise V7ControllerIntegrityError("V7.1 G5 candidate count drift")
    if int(funnel.get("walk_forward_positive_count") or 0) != 2:
        raise V7ControllerIntegrityError("V7.1 G5 walk-forward count drift")
    if tripwire.get("verdict") != "ARTEFACT_GEOMETRY_ONLY":
        raise V7ControllerIntegrityError("V7.1 G5 tripwire verdict drift")
    if float(tripwire.get("NULL_RATIO") or 0.0) < 0.8:
        raise V7ControllerIntegrityError("V7.1 G5 artefact threshold drift")
    if verdict.get("badge") != "GEOMETRY_ONLY" or int(
        verdict.get("powered_candidate_count") or 0
    ) != 0:
        raise V7ControllerIntegrityError("V7.1 G5 frozen verdict drift")
    graveyard = root / "mission/state/graveyard.db"
    conn = sqlite3.connect(f"file:{graveyard}?mode=ro", uri=True)
    try:
        cemetery_count = int(
            conn.execute(
                "SELECT COALESCE(SUM(candidate_count),0) FROM class_tombstones "
                "WHERE mechanism_class=? AND regime=? AND death_cause=?",
                (
                    "v71g5_cross_clock_speed_leadership",
                    "D1_2023_2024_DATE_MATCHED_BLOCKS",
                    "GEOMETRY_ONLY_NULL_RATIO_GTE_0_8",
                ),
            ).fetchone()[0]
        )
    finally:
        conn.close()
    if cemetery_count != 12:
        raise V7ControllerIntegrityError("V7.1 G5 class tombstone is absent")
    if (root / V71_G6_GRAMMAR_RELATIVE_PATH).is_file():
        return _classify_v71_g6_action(
            root,
            prior_positive=prior_positive,
            g2_positive=g2_positive,
            cumulative_status=cumulative_status,
            confirmation_queue=confirmation_queue,
            g5_cemetery_count=cemetery_count,
        )
    return {
        "action_type": "V71_G5_GEOMETRY_ONLY_PIVOT_TO_NEW_CLASS",
        "phase": "4",
        "progressed": True,
        "walk_forward_positive_count": prior_positive + g2_positive + 2 + 2 + 2,
        "g5_candidate_count": int(signal["candidate_count"]),
        "g5_signal_count": int(signal["signal_count"]),
        "g5_stage1_survivor_count": int(funnel["stage1_pass_count"]),
        "g5_walk_forward_positive_count": int(funnel["walk_forward_positive_count"]),
        "g5_tripwire_verdict": tripwire["verdict"],
        "g5_NULL_RATIO": float(tripwire["NULL_RATIO"]),
        "g5_real_pass_count": tripwire["raw_pass_counts"]["real"],
        "g5_null_pass_count": tripwire["raw_pass_counts"]["null"],
        "g5_power_audit_executed": False,
        "g5_rolling_combine_promotions": 0,
        "g5_cemetery_candidate_count": cemetery_count,
        "cumulative_power_status_counts": dict(cumulative_status),
        "confirmation_queue_underpowered_count": int(
            confirmation_queue["cumulative_promising_underpowered_count"]
        ),
        "confirmation_queue_fragile_retired_count": int(
            confirmation_queue["cumulative_fragile_retired_count"]
        ),
        "geometry_only_candidate_count": 12,
        "powered_candidate_count": 0,
        "broad_D1_generation_authorized": False,
        "limited_structural_discovery_authorized": True,
        "conversion_priority": 0.95,
        "limited_discovery_allocation": 0.05,
        "new_data_purchase_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": "hydra_v7_1_distinct_hypothesis_review_0006",
        "next_experiment_state": "PREREGISTRATION_REQUIRED_NO_DATA_PURCHASE",
        "principal_blocker": (
            "Fourteen candidates still require independent confirmation, while "
            "the latest speed-leadership class is GEOMETRY_ONLY and cannot be salvaged."
        ),
        "reason": (
            "The speed-leadership null passes more often than the real world. "
            "All twelve formulations are class-tombstoned; the perpetual factory "
            "must pivot to a genuinely distinct economic hypothesis without "
            "purchasing data or reusing candidate-level feedback."
        ),
    }


def _classify_v71_g6_action(
    root: Path,
    *,
    prior_positive: int,
    g2_positive: int,
    cumulative_status: Mapping[str, int],
    confirmation_queue: Mapping[str, Any],
    g5_cemetery_count: int,
) -> dict[str, Any]:
    required = (
        (V71_G6_SIGNAL_RELATIVE_PATH, "V71_G6_SIGNAL_MANIFEST_REQUIRED"),
        (V71_G6_FUNNEL_RELATIVE_PATH, "V71_G6_DEVELOPMENT_FUNNEL_REQUIRED"),
        (V71_G6_TRIPWIRE_RELATIVE_PATH, "V71_G6_TRIPWIRE_REQUIRED"),
        (V71_G6_POWER_RELATIVE_PATH, "V71_G6_POWER_AUDIT_REQUIRED"),
        (
            Path("MISSION_CONTRACT_AMENDMENT_003_RESEARCH_STAGE_COMBINE.md"),
            "V71_UNDERPOWERED_COMBINE_AMENDMENT_REQUIRED",
        ),
        (
            Path(
                "WORM/v7.1-underpowered-combine-research-policy-0001-2026-07-13.json"
            ),
            "V71_UNDERPOWERED_COMBINE_POLICY_REQUIRED",
        ),
        (
            Path("WORM/v7.1-underpowered-combine-cohort-0001-2026-07-13.json"),
            "V71_UNDERPOWERED_COMBINE_COHORT_REQUIRED",
        ),
        (V71_UNDERPOWERED_SELECTION_RELATIVE_PATH, "V71_UNDERPOWERED_SELECTION_REQUIRED"),
        (
            V71_UNDERPOWERED_DIAGNOSTIC_RELATIVE_PATH,
            "V71_UNDERPOWERED_COMBINE_DIAGNOSTIC_REQUIRED",
        ),
        (
            Path(
                "WORM/v7.1-underpowered-combine-diagnostic-verdict-0001-2026-07-13.json"
            ),
            "V71_UNDERPOWERED_COMBINE_VERDICT_REQUIRED",
        ),
    )
    for path, action in required:
        if not (root / path).is_file():
            return {
                "action_type": action,
                "phase": "4",
                "progressed": False,
                "required_path": str(path),
                "broad_D1_generation_authorized": False,
                "new_data_purchase_authorized": False,
                "shadow_admission_authorized": False,
                "reason": (
                    "The G6 and underpowered Combine conversion sequence must "
                    "remain atomic before another structural action."
                ),
            }
    hashes = {
        V71_G6_SIGNAL_RELATIVE_PATH: "ea883cbf7ee460b5467f0023f171e67528f696f748c09aa7c95d11a87e7db752",
        V71_G6_FUNNEL_RELATIVE_PATH: "c99dd8aeca6bdcb9f908f6b0b7e39f4d2cf06b8671c95b9e190a276ffef9ec67",
        V71_G6_TRIPWIRE_RELATIVE_PATH: "c3a0a53105ed260acb83c65b42312915bb4ed6f8047f061ca34d3ada81679596",
        V71_G6_POWER_RELATIVE_PATH: "ab7fd3885e23943c4abd532f82902629f1a689e962ca4d8a1d7dc9869a5f32de",
        V71_UNDERPOWERED_SELECTION_RELATIVE_PATH: "6c5c324bbd22bbab4956b9cd310bc98c73ad8e1e48323cb04e08a65b92442dd1",
        V71_UNDERPOWERED_DIAGNOSTIC_RELATIVE_PATH: "6dff583d4aa945f4e2c479b801cf6fa954e0616b8f65d32bada401797ce0c4e0",
    }
    drift = [
        str(path) for path, expected in hashes.items() if _sha256(root / path) != expected
    ]
    if drift:
        raise V7ControllerIntegrityError(
            "V7.1 G6 or Combine diagnostic evidence drift: " + ",".join(drift)
        )
    signal = _load_json(root / V71_G6_SIGNAL_RELATIVE_PATH)
    funnel = _load_json(root / V71_G6_FUNNEL_RELATIVE_PATH)
    tripwire = _load_json(root / V71_G6_TRIPWIRE_RELATIVE_PATH)
    power = _load_json(root / V71_G6_POWER_RELATIVE_PATH)
    selection = _load_json(root / V71_UNDERPOWERED_SELECTION_RELATIVE_PATH)
    diagnostic = _load_json(root / V71_UNDERPOWERED_DIAGNOSTIC_RELATIVE_PATH)
    verdict = _load_json(
        root
        / "WORM/v7.1-underpowered-combine-diagnostic-verdict-0001-2026-07-13.json"
    )
    if int(signal.get("candidate_count") or 0) != 6:
        raise V7ControllerIntegrityError("V7.1 G6 candidate count drift")
    if int(funnel.get("walk_forward_positive_count") or 0) != 2:
        raise V7ControllerIntegrityError("V7.1 G6 walk-forward count drift")
    if tripwire.get("verdict") != "GREEN_NULL_ADJUSTED_BASELINE" or float(
        tripwire.get("NULL_RATIO") or 1.0
    ) >= 0.8:
        raise V7ControllerIntegrityError("V7.1 G6 tripwire is not frozen GREEN")
    if power.get("status_counts") != {"PROMISING_UNDERPOWERED": 2}:
        raise V7ControllerIntegrityError("V7.1 G6 power status drift")
    if power.get("powered_candidate_ids"):
        raise V7ControllerIntegrityError("V7.1 G6 unexpectedly contains powered candidates")
    reconciliation = dict(selection.get("population_reconciliation") or {})
    if int(reconciliation.get("after_G6_accounted_count", 0)) != 22 or int(
        reconciliation.get("unaccounted_count", -1)
    ) != 0:
        raise V7ControllerIntegrityError("V7.1 evidence reconciliation drift")
    if int(selection.get("selected_count") or 0) != 5:
        raise V7ControllerIntegrityError("V7.1 Combine selection count drift")
    if (
        diagnostic.get("scientific_status")
        != "BOUNDED_DIAGNOSTIC_ONLY_NO_PROMOTION"
        or int(diagnostic.get("episode_start_count") or 0) != 24
        or int(diagnostic.get("effective_nonoverlapping_block_count") or 0) != 4
        or int(diagnostic.get("final_power_gate_passed_count", -1)) != 0
        or diagnostic.get("shadow_promotion_authorized") is not False
    ):
        raise V7ControllerIntegrityError("V7.1 Combine diagnostic status drift")
    primary = "eod_level_rt_breach__DLL_disabled"
    candidate_passes = sum(
        int(row["variants"][primary]["pass_count"])
        for row in diagnostic["candidate_results"].values()
    )
    basket_passes = int(diagnostic["basket"]["variants"][primary]["pass_count"])
    if candidate_passes != 0 or basket_passes != 0:
        raise V7ControllerIntegrityError("V7.1 diagnostic verdict pass count drift")
    if verdict.get("verdict") != "NULL_DIAGNOSTIC_NO_PASS_NO_PROMOTION":
        raise V7ControllerIntegrityError("V7.1 diagnostic WORM verdict drift")
    cumulative_after_g6 = {
        "PROMISING_UNDERPOWERED": int(
            cumulative_status.get("PROMISING_UNDERPOWERED", 0)
        )
        + 2,
        "WF_POSITIVE_BUT_FRAGILE": int(
            cumulative_status.get("WF_POSITIVE_BUT_FRAGILE", 0)
        ),
        "GEOMETRY_ONLY_CLASS_TOMBSTONED_NO_POWER_AUDIT": 2,
    }
    g6_action = {
        "action_type": "V71_G6_GREEN_UNDERPOWERED_COMBINE_DIAGNOSTIC_COMPLETE",
        "phase": "4",
        "progressed": True,
        "walk_forward_positive_count": prior_positive + g2_positive + 2 + 2 + 2 + 2,
        "g6_candidate_count": int(signal["candidate_count"]),
        "g6_signal_count": int(signal["signal_count"]),
        "g6_stage1_survivor_count": int(funnel["stage1_pass_count"]),
        "g6_walk_forward_positive_count": int(funnel["walk_forward_positive_count"]),
        "g6_tripwire_verdict": tripwire["verdict"],
        "g6_NULL_RATIO": float(tripwire["NULL_RATIO"]),
        "g6_real_pass_count": tripwire["raw_pass_counts"]["real"],
        "g6_null_pass_count": tripwire["raw_pass_counts"]["null"],
        "g6_power_status_counts": dict(power["status_counts"]),
        "cumulative_power_status_counts": cumulative_after_g6,
        "evidence_reconciliation_accounted_count": 22,
        "evidence_reconciliation_unaccounted_count": 0,
        "underpowered_combine_selected_count": 5,
        "underpowered_combine_status": "PROMISING_UNDERPOWERED_COMBINE_RESEARCH",
        "underpowered_combine_episode_start_count": 24,
        "underpowered_combine_effective_block_count": 4,
        "underpowered_combine_candidate_pass_count": candidate_passes,
        "underpowered_combine_basket_pass_count": basket_passes,
        "underpowered_combine_validated_count": 0,
        "powered_candidate_count": 0,
        "rolling_combine_promotions": 0,
        "g5_cemetery_candidate_count": g5_cemetery_count,
        "confirmation_queue_underpowered_count": int(
            confirmation_queue["cumulative_promising_underpowered_count"]
        )
        + 2,
        "confirmation_queue_fragile_retired_count": int(
            confirmation_queue["cumulative_fragile_retired_count"]
        ),
        "broad_D1_generation_authorized": False,
        "limited_structural_discovery_authorized": True,
        "conversion_priority": 0.95,
        "limited_discovery_allocation": 0.05,
        "new_data_purchase_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": (
            "hydra_v7_1_independent_confirmation_and_distinct_hypothesis_review_0007"
        ),
        "next_experiment_state": "PREREGISTRATION_REQUIRED_NO_DATA_PURCHASE",
        "principal_blocker": (
            "No candidate passes the unchanged 80% power gate; the 24-start "
            "Combine diagnostic has only four effective blocks and zero passes."
        ),
        "reason": (
            "G6 has a VERT_NET tripwire and two nonfragile underpowered candidates. "
            "The authorized research-stage Combine diagnostic resolved five distinct "
            "candidates without promotion: zero candidate or basket passes, all 22 "
            "walk-forward-positive formulations explicitly accounted for, and no data purchase."
        ),
    }
    if (root / V71_G7_GRAMMAR_RELATIVE_PATH).is_file():
        return _classify_v71_g7_action(root, g6_action=g6_action)
    return g6_action


def _classify_v71_g7_action(
    root: Path,
    *,
    g6_action: Mapping[str, Any],
) -> dict[str, Any]:
    required = (
        (V71_G7_SIGNAL_RELATIVE_PATH, "V71_G7_SIGNAL_MANIFEST_REQUIRED"),
        (V71_G7_FUNNEL_RELATIVE_PATH, "V71_G7_DEVELOPMENT_FUNNEL_REQUIRED"),
        (V71_G7_TRIPWIRE_RELATIVE_PATH, "V71_G7_TRIPWIRE_REQUIRED"),
    )
    for path, action in required:
        if not (root / path).is_file():
            return {
                **dict(g6_action),
                "action_type": action,
                "progressed": False,
                "required_path": str(path),
                "reason": (
                    "The preregistered G7 flow-sign sequence must complete its "
                    "atomic Stage 0-2 and permanent tripwire sequence."
                ),
            }
    hashes = {
        V71_G7_SIGNAL_RELATIVE_PATH: "eae86b8260596bb1ba9b8155769dfb16cb528736764ad57c194f5bdf3db48ee6",
        V71_G7_FUNNEL_RELATIVE_PATH: "ec2570bcf75185238751815ffd259ff9e08c2ec9577e30c840ba2eaa188322ba",
        V71_G7_TRIPWIRE_RELATIVE_PATH: "2c670ff6997eb1cfe603b1a747f53de03fc90ddb7f6a210f46325ae5c202ba43",
    }
    drift = [
        str(path)
        for path, expected in hashes.items()
        if _sha256(root / path) != expected
    ]
    if drift:
        raise V7ControllerIntegrityError(
            "V7.1 G7 evidence drift: " + ",".join(drift)
        )
    signal = _load_json(root / V71_G7_SIGNAL_RELATIVE_PATH)
    funnel = _load_json(root / V71_G7_FUNNEL_RELATIVE_PATH)
    tripwire = _load_json(root / V71_G7_TRIPWIRE_RELATIVE_PATH)
    if int(signal.get("candidate_count") or 0) != 6 or int(
        signal.get("signal_count") or 0
    ) != 1_889:
        raise V7ControllerIntegrityError("V7.1 G7 signal manifest drift")
    if int(funnel.get("stage0_valid_novel_count") or 0) != 6:
        raise V7ControllerIntegrityError("V7.1 G7 Stage 0 count drift")
    if int(funnel.get("stage1_pass_count") or 0) != 0 or int(
        funnel.get("walk_forward_positive_count") or 0
    ) != 0:
        raise V7ControllerIntegrityError("V7.1 G7 null funnel drift")
    if tripwire.get("verdict") != "ARTEFACT_GEOMETRY_ONLY" or float(
        tripwire.get("NULL_RATIO") or 0.0
    ) < 0.8:
        raise V7ControllerIntegrityError("V7.1 G7 tripwire verdict drift")
    if tripwire.get("raw_pass_counts") != {
        "real": "5/120",
        "null": "17/360",
    }:
        raise V7ControllerIntegrityError("V7.1 G7 tripwire count drift")
    graveyard = root / "mission/state/graveyard.db"
    conn = sqlite3.connect(f"file:{graveyard}?mode=ro", uri=True)
    try:
        cemetery_count = int(
            conn.execute(
                "SELECT COALESCE(SUM(candidate_count),0) FROM class_tombstones "
                "WHERE mechanism_class=? AND regime=? AND death_cause=?",
                (
                    "v71g7_aggressor_flow_sign_sequences",
                    "D1_2023_2024_DEVELOPMENT_PRICE_NULLS",
                    "GEOMETRY_ONLY_NULL_RATIO_GTE_0_8",
                ),
            ).fetchone()[0]
        )
    finally:
        conn.close()
    if cemetery_count != 6:
        raise V7ControllerIntegrityError("V7.1 G7 class tombstone is absent")
    g7_action = {
        **dict(g6_action),
        "action_type": "V71_G7_GEOMETRY_ONLY_NULL_COMPLETE",
        "g7_candidate_count": 6,
        "g7_signal_count": 1_889,
        "g7_stage0_valid_count": 6,
        "g7_stage1_survivor_count": 0,
        "g7_walk_forward_positive_count": 0,
        "g7_tripwire_verdict": "ARTEFACT_GEOMETRY_ONLY",
        "g7_NULL_RATIO": float(tripwire["NULL_RATIO"]),
        "g7_real_pass_count": tripwire["raw_pass_counts"]["real"],
        "g7_null_pass_count": tripwire["raw_pass_counts"]["null"],
        "g7_power_audit_executed": False,
        "g7_rolling_combine_executed": False,
        "g7_cemetery_candidate_count": cemetery_count,
        "evidence_reconciliation_accounted_count": 22,
        "evidence_reconciliation_unaccounted_count": 0,
        "powered_candidate_count": 0,
        "rolling_combine_promotions": 0,
        "new_data_purchase_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": (
            "hydra_v7_1_independent_confirmation_and_distinct_hypothesis_review_0008"
        ),
        "next_experiment_state": "PREREGISTRATION_REQUIRED_NO_DATA_PURCHASE",
        "principal_blocker": (
            "No candidate passes the unchanged 80% power gate; G7 produced no "
            "Stage-1 survivor and its Combine geometry is no better than null."
        ),
        "reason": (
            "G7 tested six outcome-free ordinal aggressive-flow sequences. All "
            "failed Stage 1 and the permanent tripwire classified the class "
            "GEOMETRY_ONLY (5/120 real versus 17/360 null). The exact class is "
            "tombstoned, no candidate was promoted, all prior 22 WF-positive "
            "formulations remain explicitly reconciled, and no data was purchased."
        ),
    }
    if (root / V71_G8_GRAMMAR_RELATIVE_PATH).is_file():
        return _classify_v71_g8_action(root, g7_action=g7_action)
    return g7_action


def _classify_v71_g8_action(
    root: Path,
    *,
    g7_action: Mapping[str, Any],
) -> dict[str, Any]:
    required = (
        (V71_G8_FEATURE_MANIFEST_RELATIVE_PATH, "V71_G8_FEATURE_MANIFEST_REQUIRED"),
        (V71_G8_SIGNAL_RELATIVE_PATH, "V71_G8_SIGNAL_MANIFEST_REQUIRED"),
        (V71_G8_FUNNEL_RELATIVE_PATH, "V71_G8_DEVELOPMENT_FUNNEL_REQUIRED"),
        (V71_G8_TRIPWIRE_RELATIVE_PATH, "V71_G8_TRIPWIRE_REQUIRED"),
        (V71_G8_POWER_RELATIVE_PATH, "V71_G8_POWER_AUDIT_REQUIRED"),
    )
    for path, action in required:
        if not (root / path).is_file():
            return {
                **dict(g7_action),
                "action_type": action,
                "progressed": False,
                "required_path": str(path),
                "reason": (
                    "The preregistered G8 intraminute-flow sequence must complete "
                    "its feature, Stage 0-2, tripwire and power-audit chain."
                ),
            }
    hashes = {
        V71_G8_FEATURE_MANIFEST_RELATIVE_PATH: "b228dc89ed36d1b47660073dd6e68703eb44c85e6c0e5897a62f3a14168f6ad4",
        V71_G8_SIGNAL_RELATIVE_PATH: "7b5af1090f219055f62180c7bbc03dac4c18af8573452d148f15354d55f95979",
        V71_G8_FUNNEL_RELATIVE_PATH: "cdf426936a6350a997958f4f6bfb326466538881f290d7943bd057d9361dd69b",
        V71_G8_TRIPWIRE_RELATIVE_PATH: "b8e43659e47c0ccf68bd68e95dfea4328035babb0eca3433282bb1a2606000f8",
        V71_G8_POWER_RELATIVE_PATH: "170f56a092f3b33e42f47a9076f562dccd1000db7022957fe4dd59fd498aa1b6",
    }
    drift = [str(path) for path, expected in hashes.items() if _sha256(root / path) != expected]
    if drift:
        raise V7ControllerIntegrityError("V7.1 G8 evidence drift: " + ",".join(drift))
    feature = _load_json(root / V71_G8_FEATURE_MANIFEST_RELATIVE_PATH)
    signal = _load_json(root / V71_G8_SIGNAL_RELATIVE_PATH)
    funnel = _load_json(root / V71_G8_FUNNEL_RELATIVE_PATH)
    tripwire = _load_json(root / V71_G8_TRIPWIRE_RELATIVE_PATH)
    power = _load_json(root / V71_G8_POWER_RELATIVE_PATH)
    if int(feature.get("output", {}).get("row_count") or 0) != 17_200:
        raise V7ControllerIntegrityError("V7.1 G8 feature row count drift")
    if int(signal.get("candidate_count") or 0) != 6 or int(signal.get("signal_count") or 0) != 2_182:
        raise V7ControllerIntegrityError("V7.1 G8 signal manifest drift")
    if int(funnel.get("stage0_valid_novel_count") or 0) != 6 or int(funnel.get("stage1_pass_count") or 0) != 2 or int(funnel.get("walk_forward_positive_count") or 0) != 2:
        raise V7ControllerIntegrityError("V7.1 G8 funnel drift")
    if tripwire.get("verdict") != "GREEN_NULL_ADJUSTED_BASELINE" or tripwire.get("evidence_strength") != "VERT_MINCE" or float(tripwire.get("NULL_RATIO") or 1.0) >= 0.8:
        raise V7ControllerIntegrityError("V7.1 G8 tripwire drift")
    if tripwire.get("raw_pass_counts") != {"real": "10/120", "null": "22/360"}:
        raise V7ControllerIntegrityError("V7.1 G8 tripwire count drift")
    if power.get("status_counts") != {"WF_POSITIVE_BUT_FRAGILE": 2} or power.get("powered_candidate_ids"):
        raise V7ControllerIntegrityError("V7.1 G8 power classification drift")
    cumulative_status = dict(g7_action["cumulative_power_status_counts"])
    cumulative_status["WF_POSITIVE_BUT_FRAGILE"] = int(cumulative_status.get("WF_POSITIVE_BUT_FRAGILE", 0)) + 2
    g8_action = {
        **dict(g7_action),
        "action_type": "V71_G8_GREEN_THIN_FRAGILE_NO_PROMOTION",
        "walk_forward_positive_count": 24,
        "g8_feature_row_count": 17_200,
        "g8_candidate_count": 6,
        "g8_signal_count": 2_182,
        "g8_stage0_valid_count": 6,
        "g8_stage1_survivor_count": 2,
        "g8_walk_forward_positive_count": 2,
        "g8_tripwire_verdict": "GREEN_NULL_ADJUSTED_BASELINE",
        "g8_tripwire_evidence_strength": "VERT_MINCE",
        "g8_NULL_RATIO": float(tripwire["NULL_RATIO"]),
        "g8_real_pass_count": tripwire["raw_pass_counts"]["real"],
        "g8_null_pass_count": tripwire["raw_pass_counts"]["null"],
        "g8_power_status_counts": dict(power["status_counts"]),
        "g8_powered_candidate_count": 0,
        "g8_rolling_combine_executed": False,
        "cumulative_power_status_counts": cumulative_status,
        "confirmation_queue_fragile_retired_count": 6,
        "evidence_reconciliation_accounted_count": 24,
        "evidence_reconciliation_unaccounted_count": 0,
        "powered_candidate_count": 0,
        "rolling_combine_promotions": 0,
        "new_data_purchase_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": "hydra_v7_1_independent_confirmation_and_distinct_hypothesis_review_0009",
        "next_experiment_state": "PREREGISTRATION_REQUIRED_NO_DATA_PURCHASE",
        "principal_blocker": (
            "No candidate passes the unchanged 80% power gate; both G8 positives "
            "lose their edge at 2x costs and after removal of the best event."
        ),
        "reason": (
            "G8 derived 17,200 outcome-free ES intraminute observations from "
            "already purchased D1 prints. Two of six candidates were barely "
            "walk-forward positive and the class tripwire was VERT_MINCE, but "
            "both exact versions are WF_POSITIVE_BUT_FRAGILE. No candidate, "
            "Combine, shadow, holdout or purchase was authorized, and all 24 "
            "walk-forward-positive formulations are explicitly accounted for."
        ),
    }
    if (root / V71_G9_GRAMMAR_RELATIVE_PATH).is_file():
        return _classify_v71_g9_action(root, g8_action=g8_action)
    return g8_action


def _classify_v71_g9_action(
    root: Path,
    *,
    g8_action: Mapping[str, Any],
) -> dict[str, Any]:
    required = (
        (V71_G9_FEATURE_MANIFEST_RELATIVE_PATH, "V71_G9_FEATURE_MANIFEST_REQUIRED"),
        (V71_G9_SIGNAL_RELATIVE_PATH, "V71_G9_SIGNAL_MANIFEST_REQUIRED"),
        (V71_G9_FUNNEL_RELATIVE_PATH, "V71_G9_DEVELOPMENT_FUNNEL_REQUIRED"),
        (V71_G9_TRIPWIRE_RELATIVE_PATH, "V71_G9_TRIPWIRE_REQUIRED"),
    )
    for path, action in required:
        if not (root / path).is_file():
            return {
                **dict(g8_action),
                "action_type": action,
                "progressed": False,
                "required_path": str(path),
                "reason": (
                    "The preregistered G9 aggressor-run topology experiment must "
                    "complete its outcome-free feature, Stage 0-2 and permanent "
                    "tripwire chain."
                ),
            }
    hashes = {
        V71_G9_FEATURE_MANIFEST_RELATIVE_PATH: "b1151bdc493f569eda85d13983ce73df9f92cbfe9f2416fd4ac63183e251127c",
        V71_G9_SIGNAL_RELATIVE_PATH: "f678b6049cb46ed81502f63894082aa5546ce4335054ffe630551cba8b4faaa4",
        V71_G9_FUNNEL_RELATIVE_PATH: "65d6a30ef61c74b1e8dd3dbfc194c062ade5ea34465fac9dbdf122da87fa7623",
        V71_G9_TRIPWIRE_RELATIVE_PATH: "8aeb2e3c1c6f88720cd9d4203d581860c4e682c6f1dc3029a32800fa1810c243",
    }
    drift = [str(path) for path, expected in hashes.items() if _sha256(root / path) != expected]
    if drift:
        raise V7ControllerIntegrityError("V7.1 G9 evidence drift: " + ",".join(drift))
    feature = _load_json(root / V71_G9_FEATURE_MANIFEST_RELATIVE_PATH)
    signal = _load_json(root / V71_G9_SIGNAL_RELATIVE_PATH)
    funnel = _load_json(root / V71_G9_FUNNEL_RELATIVE_PATH)
    tripwire = _load_json(root / V71_G9_TRIPWIRE_RELATIVE_PATH)
    if int(feature.get("output", {}).get("row_count") or 0) != 17_200:
        raise V7ControllerIntegrityError("V7.1 G9 feature row count drift")
    if int(signal.get("candidate_count") or 0) != 4 or int(signal.get("signal_count") or 0) != 1_573:
        raise V7ControllerIntegrityError("V7.1 G9 signal manifest drift")
    if (
        int(funnel.get("stage0_valid_novel_count") or 0) != 4
        or int(funnel.get("stage1_pass_count") or 0) != 0
        or int(funnel.get("walk_forward_positive_count") or 0) != 0
        or funnel.get("classification_counts") != {"FORMULATION_FALSIFIED": 4}
    ):
        raise V7ControllerIntegrityError("V7.1 G9 funnel drift")
    if (
        tripwire.get("verdict") != "BLOCKED_UNDERPOWERED"
        or tripwire.get("NULL_RATIO") is not None
        or tripwire.get("evidence_strength") != "INDETERMINE"
        or tripwire.get("raw_pass_counts") != {"real": "0/80", "null": "9/240"}
    ):
        raise V7ControllerIntegrityError("V7.1 G9 tripwire drift")
    return {
        **dict(g8_action),
        "action_type": "V71_G9_FORMULATIONS_FALSIFIED_TRIPWIRE_UNDERPOWERED",
        "g9_feature_row_count": 17_200,
        "g9_candidate_count": 4,
        "g9_signal_count": 1_573,
        "g9_stage0_valid_count": 4,
        "g9_stage1_survivor_count": 0,
        "g9_walk_forward_positive_count": 0,
        "g9_formulation_falsified_count": 4,
        "g9_tripwire_verdict": "BLOCKED_UNDERPOWERED",
        "g9_tripwire_evidence_strength": "INDETERMINE",
        "g9_NULL_RATIO": None,
        "g9_real_pass_count": "0/80",
        "g9_null_pass_count": "9/240",
        "g9_power_audit_executed": False,
        "g9_rolling_combine_executed": False,
        "evidence_reconciliation_accounted_count": 24,
        "evidence_reconciliation_unaccounted_count": 0,
        "powered_candidate_count": 0,
        "rolling_combine_promotions": 0,
        "new_data_purchase_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": "hydra_v7_1_D1_class_exhaustion_and_independent_evidence_decision_0010",
        "next_experiment_state": "PREREGISTRATION_REQUIRED_NO_DATA_PURCHASE",
        "principal_blocker": (
            "No candidate passes the unchanged 80% power gate; all four G9 "
            "formulations were negative under 1.5x costs and the class tripwire "
            "had zero real passes, so no power or Combine promotion is lawful."
        ),
        "reason": (
            "G9 tested four outcome-free trade-level aggressor-run topology "
            "formulations on 17,200 explicit-contract ES minutes. All failed "
            "Stage 1 under frozen 1.5x costs. The mandatory tripwire observed "
            "0/80 real versus 9/240 null passes and is explicitly underpowered, "
            "so the exact formulations are falsified without declaring the broad "
            "mechanism dead. No power audit, Combine, shadow, holdout or purchase "
            "was authorized."
        ),
    }


def _classify_v72_action(root: Path) -> dict[str, Any]:
    prior = _classify_v71_action(root)
    if prior.get("action_type") != (
        "V71_G9_FORMULATIONS_FALSIFIED_TRIPWIRE_UNDERPOWERED"
    ):
        raise V7ControllerIntegrityError(
            "V7.2 requires the terminal frozen V7.1 G9 decision"
        )
    sequence = (
        (V72_POLICY_RELATIVE_PATH, "V72_POLICY_REQUIRED"),
        (V72_SEMANTICS_RELATIVE_PATH, "V72_COMBINE_SEMANTICS_AUDIT_REQUIRED"),
        (
            V72_COMPONENT_BANK_WORM_RELATIVE_PATH,
            "V72_COMPONENT_BANK_FREEZE_REQUIRED",
        ),
        (V72_COMPONENT_BANK_RELATIVE_PATH, "V72_COMPONENT_BANK_REQUIRED"),
        (V72_SEARCH_WORM_RELATIVE_PATH, "V72_BASKET_SEARCH_FREEZE_REQUIRED"),
        (V72_SEARCH_FREEZE_RELATIVE_PATH, "V72_BASKET_SEARCH_MANIFEST_REQUIRED"),
        (V72_CROSS_FIT_RELATIVE_PATH, "V72_STATIC_BASKET_CROSS_FIT_REQUIRED"),
    )
    for path, action_type in sequence:
        if not (root / path).is_file():
            return {
                **dict(prior),
                "action_type": action_type,
                "phase": "4",
                "progressed": False,
                "required_path": str(path),
                "new_data_purchase_authorized": False,
                "protected_holdout_access_authorized": False,
                "reason": (
                    "The leakage-safe V7.2 account-synthesis sequence is not "
                    "complete; no later stage is authorized."
                ),
            }
    drift = [
        path
        for path, expected in V72_FROZEN_HASHES.items()
        if _sha256(root / path) != expected
    ]
    if drift:
        raise V7ControllerIntegrityError(
            "V7.2 frozen evidence drift: " + ",".join(drift)
        )
    semantics = _load_json(root / V72_SEMANTICS_RELATIVE_PATH)
    component_bank = _load_json(root / V72_COMPONENT_BANK_RELATIVE_PATH)
    search = _load_json(root / V72_SEARCH_FREEZE_RELATIVE_PATH)
    cross_fit = _load_json(root / V72_CROSS_FIT_RELATIVE_PATH)
    if (
        semantics.get("verdict") != "GREEN"
        or not bool(semantics.get("checks", {}).get("R1_no_official_time_limit"))
        or bool(
            semantics.get("v72_observation_layer", {}).get(
                "profitable_survivor_is_terminal_failure"
            )
        )
        or int(semantics.get("new_data_purchase_count") or 0) != 0
        or int(semantics.get("protected_holdout_access_count_delta") or 0) != 0
        or int(semantics.get("outbound_order_count") or 0) != 0
    ):
        raise V7ControllerIntegrityError("V7.2 Combine semantics audit drift")
    if (
        int(component_bank.get("source_walk_forward_positive_count") or 0)
        != 24
        or int(component_bank.get("unaccounted_candidate_count") or 0) != 0
        or int(component_bank.get("primary_component_count") or 0) != 11
        or int(component_bank.get("backup_component_count") or 0) != 4
    ):
        raise V7ControllerIntegrityError("V7.2 component-bank inventory drift")
    if (
        int(search.get("structure_count") or 0) != 1_009
        or int(search.get("multiplicity", {}).get("raw_global_N_trials_after") or 0)
        != 264_911
    ):
        raise V7ControllerIntegrityError("V7.2 basket-search freeze drift")
    expected_status = {"BASKET_RESEARCH_FAILED": 12}
    if (
        cross_fit.get("verdict") != "NULL"
        or int(cross_fit.get("structure_count") or 0) != 1_009
        or int(cross_fit.get("cross_fit_rotation_count") or 0) != 4
        or int(cross_fit.get("held_out_basket_evaluation_count") or 0) != 12
        or int(cross_fit.get("cross_fit_survivor_count") or 0) != 0
        or int(cross_fit.get("promotion_to_48_starts_count") or 0) != 0
        or dict(cross_fit.get("status_counts") or {}) != expected_status
        or int(cross_fit.get("risk_overlay_authorized_count") or 0) != 0
        or int(cross_fit.get("new_data_purchase_count") or 0) != 0
        or int(cross_fit.get("protected_holdout_access_count_delta") or 0) != 0
        or int(cross_fit.get("outbound_order_count") or 0) != 0
        or not bool(cross_fit.get("all_rotation_manifests_frozen_before_held_out_read"))
        or not bool(cross_fit.get("design_and_unseen_metrics_separated"))
    ):
        raise V7ControllerIntegrityError("V7.2 static basket cross-fit drift")
    held_out = list(cross_fit.get("selected_held_out_results") or [])
    operational = list(cross_fit.get("operational_basket_results") or [])
    unseen_pass_count = sum(
        int(row["unseen_metrics"]["BASE"]["pass_count"]) for row in held_out
    )
    unseen_mll_breach_count = sum(
        int(row["unseen_metrics"]["BASE"]["mll_breach_count"])
        for row in held_out
    )
    unseen_normal_positive_count = sum(
        float(row["unseen_metrics"]["BASE"]["net_pnl"]) > 0.0
        for row in held_out
    )
    unseen_stress_positive_count = sum(
        float(row["unseen_metrics"]["STRESS_1_5X"]["net_pnl"]) > 0.0
        for row in held_out
    )
    dominated_count = sum(
        bool(row["dominated_by_best_single_parent"]) for row in operational
    )
    action = {
        **dict(prior),
        "action_type": "V72_STATIC_BASKET_CROSS_FIT_NULL_DISTINCT_MECHANISM_PIVOT",
        "phase": "4",
        "progressed": True,
        "v72_component_bank_primary_count": 11,
        "v72_component_bank_backup_count": 4,
        "v72_economic_cluster_count": 11,
        "v72_static_structure_count": 1_009,
        "v72_design_episode_count": int(cross_fit["design_episode_count"]),
        "v72_cross_fit_rotation_count": 4,
        "v72_held_out_basket_evaluation_count": 12,
        "v72_held_out_pass_count": unseen_pass_count,
        "v72_held_out_mll_breach_count": unseen_mll_breach_count,
        "v72_held_out_normal_positive_count": unseen_normal_positive_count,
        "v72_held_out_stress_positive_count": unseen_stress_positive_count,
        "v72_parent_dominated_count": dominated_count,
        "v72_cross_fit_survivor_count": 0,
        "v72_risk_overlay_executed_count": 0,
        "v72_promotion_to_48_starts_count": 0,
        "v72_scientific_status": "NULL_LEAKAGE_SAFE_CROSS_FIT",
        "raw_global_N_trials": 264_911,
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": "hydra_v7_2_distinct_mechanism_hypothesis_review_0002",
        "next_experiment_state": "WORM_PREREGISTRATION_REQUIRED_NO_DATA_PURCHASE",
        "principal_blocker": (
            "The 12 design-selected baskets produced only one unseen-block pass, "
            "seven unseen MLL breaches, no repeated operational signature across "
            "two rotations, and nine were dominated by a single parent."
        ),
        "reason": (
            "V7.2 completed four leakage-safe leave-one-block-out rotations over "
            "1,009 preregistered static baskets. No exact basket transferred on "
            "two independent blocks, so overlays and 48-start confirmation are "
            "correctly forbidden. Components are retained for future distinct "
            "mechanisms; no data, Q4, shadow or order action is authorized."
        ),
    }
    if (root / V72_G10_GRAMMAR_RELATIVE_PATH).is_file():
        return _classify_v72_g10_action(root, v72_action=action)
    return action


def _classify_v72_g10_action(
    root: Path,
    *,
    v72_action: Mapping[str, Any],
) -> dict[str, Any]:
    required = (
        (V72_G10_VALIDATION_ADDENDUM_RELATIVE_PATH, "V72_G10_VALIDATION_ADDENDUM_REQUIRED"),
        (V72_G10_TRIPWIRE_POLICY_RELATIVE_PATH, "V72_G10_TRIPWIRE_POLICY_REQUIRED"),
        (V72_G10_SIGNAL_RELATIVE_PATH, "V72_G10_SIGNAL_MANIFEST_REQUIRED"),
        (V72_G10_FUNNEL_RELATIVE_PATH, "V72_G10_DEVELOPMENT_FUNNEL_REQUIRED"),
        (V72_G10_TRIPWIRE_RELATIVE_PATH, "V72_G10_TRIPWIRE_REQUIRED"),
        (V72_G10_RECONCILIATION_RELATIVE_PATH, "V72_G10_MULTIPLICITY_RECONCILIATION_REQUIRED"),
        (V72_G10_POWER_FREEZE_RELATIVE_PATH, "V72_G10_POWER_FREEZE_REQUIRED"),
        (V72_G10_POWER_RELATIVE_PATH, "V72_G10_POWER_AUDIT_REQUIRED"),
    )
    for path, action_type in required:
        if not (root / path).is_file():
            return {
                **dict(v72_action),
                "action_type": action_type,
                "progressed": False,
                "required_path": str(path),
                "new_data_purchase_authorized": False,
                "protected_holdout_access_authorized": False,
                "shadow_admission_authorized": False,
                "reason": (
                    "The preregistered G10 conversion chain is incomplete; no "
                    "later inference or promotion is authorized."
                ),
            }
    drift = [
        path
        for path, expected in V72_G10_FROZEN_HASHES.items()
        if _sha256(root / path) != expected
    ]
    if drift:
        raise V7ControllerIntegrityError(
            "V7.2 G10 frozen evidence drift: " + ",".join(drift)
        )
    signal = _load_json(root / V72_G10_SIGNAL_RELATIVE_PATH)
    funnel = _load_json(root / V72_G10_FUNNEL_RELATIVE_PATH)
    tripwire = _load_json(root / V72_G10_TRIPWIRE_RELATIVE_PATH)
    reconciliation = _load_json(root / V72_G10_RECONCILIATION_RELATIVE_PATH)
    power = _load_json(root / V72_G10_POWER_RELATIVE_PATH)
    if (
        int(signal.get("candidate_count") or 0) != 36
        or int(signal.get("signal_count") or 0) != 2_354
        or signal.get("contains_outcomes_or_pnl") is not False
    ):
        raise V7ControllerIntegrityError("V7.2 G10 signal-manifest drift")
    if (
        int(funnel.get("candidate_count") or 0) != 36
        or int(funnel.get("stage0_valid_novel_count") or 0) != 36
        or int(funnel.get("stage1_pass_count") or 0) != 6
        or int(funnel.get("walk_forward_positive_count") or 0) != 5
        or int(funnel.get("SIM_EXPLOIT_survivor_count") or 0) != 4
        or int(funnel.get("new_data_purchase_count") or 0) != 0
        or int(funnel.get("protected_holdout_access_count_delta") or 0) != 0
        or int(funnel.get("outbound_order_count") or 0) != 0
    ):
        raise V7ControllerIntegrityError("V7.2 G10 funnel drift")
    if (
        tripwire.get("verdict") != "GREEN_NULL_ADJUSTED_BASELINE"
        or tripwire.get("evidence_strength") != "VERT_NET"
        or tripwire.get("raw_pass_counts") != {
            "real": "56/720",
            "null": "109/2160",
        }
        or abs(float(tripwire.get("NULL_RATIO") or 0.0) - 0.6488095238095238)
        > 1.0e-15
        or bool(tripwire.get("candidate_promotion_authorized"))
    ):
        raise V7ControllerIntegrityError("V7.2 G10 tripwire drift")
    if (
        reconciliation.get("status")
        != "ACCOUNTED_AFTER_EXECUTION_MULTIPLICITY_OMISSION_DISCLOSED"
        or int(reconciliation.get("accounting", {}).get("delta_trials") or 0)
        != 144
        or int(
            reconciliation.get("accounting", {}).get("global_N_trials_after")
            or 0
        )
        != 265_091
    ):
        raise V7ControllerIntegrityError(
            "V7.2 G10 multiplicity reconciliation drift"
        )
    if (
        int(power.get("candidate_count") or 0) != 4
        or dict(power.get("status_counts") or {})
        != {"PROMISING_UNDERPOWERED": 2, "WF_POSITIVE_BUT_FRAGILE": 2}
        or list(power.get("powered_candidate_ids") or [])
        or bool(power.get("candidate_nulls_executed"))
        or bool(power.get("DSR_BH_executed"))
        or bool(power.get("rolling_combine_executed"))
        or int(power.get("new_data_purchase_count") or 0) != 0
        or int(power.get("protected_holdout_access_count_delta") or 0) != 0
        or int(power.get("outbound_order_count") or 0) != 0
    ):
        raise V7ControllerIntegrityError("V7.2 G10 power-audit drift")
    proof = load_and_verify(root / "mission/state/proof_registry.json")
    current_trials = multiplicity_trial_count(proof)
    if current_trials < 265_107 or burned_window_ids(proof) != ("Q4_2024",):
        raise V7ControllerIntegrityError("V7.2 G10 proof-registry drift")
    g10_action = {
        **dict(v72_action),
        "action_type": "V72_G10_GREEN_TRIPWIRE_UNDERPOWERED_NO_PROMOTION_DISTINCT_MECHANISM_PIVOT",
        "phase": "4",
        "progressed": True,
        "walk_forward_positive_count": 29,
        "evidence_reconciliation_accounted_count": 29,
        "evidence_reconciliation_unaccounted_count": 0,
        "confirmation_queue_underpowered_count": 18,
        "confirmation_queue_fragile_retired_count": 8,
        "g10_candidate_count": 36,
        "g10_signal_count": 2_354,
        "g10_stage0_valid_count": 36,
        "g10_stage1_survivor_count": 6,
        "g10_walk_forward_positive_count": 5,
        "g10_SIM_EXPLOIT_count": 1,
        "g10_SIM_EXPLOIT_survivor_count": 4,
        "g10_tripwire_verdict": "GREEN_NULL_ADJUSTED_BASELINE",
        "g10_tripwire_evidence_strength": "VERT_NET",
        "g10_NULL_RATIO": 0.6488095238095238,
        "g10_real_pass_count": "56/720",
        "g10_null_pass_count": "109/2160",
        "g10_multiplicity_reconciliation_delta": 144,
        "g10_power_status_counts": {
            "PROMISING_UNDERPOWERED": 2,
            "WF_POSITIVE_BUT_FRAGILE": 2,
        },
        "g10_powered_candidate_count": 0,
        "g10_candidate_nulls_executed": False,
        "g10_DSR_BH_executed": False,
        "g10_rolling_combine_executed": False,
        "powered_candidate_count": 0,
        "rolling_combine_promotions": 0,
        "raw_global_N_trials": current_trials,
        "broad_D1_generation_authorized": False,
        "limited_structural_discovery_authorized": True,
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": "hydra_v7_2_distinct_mechanism_hypothesis_review_0003",
        "next_experiment_state": "WORM_PREREGISTRATION_REQUIRED_NO_DATA_PURCHASE",
        "principal_blocker": (
            "G10 produced four cost-2x walk-forward survivors, but none passes "
            "the unchanged 80% candidate-specific power gate; two are fragile "
            "and the two non-fragile candidates remain post-selection underpowered."
        ),
        "reason": (
            "The delayed flow-impact grammar tested 36 frozen structures. Its "
            "permanent tripwire was VERT_NET, but candidate-specific inference "
            "classified two candidates PROMISING_UNDERPOWERED and two "
            "WF_POSITIVE_BUT_FRAGILE. Candidate nulls, DSR/BH, Rolling Combine, "
            "shadow, holdout and data purchase therefore remain unauthorized. "
            "The perpetual factory pivots to another preregistered mechanism."
        ),
    }
    if (root / V72_G11_GRAMMAR_RELATIVE_PATH).is_file():
        return _classify_v72_g11_action(root, g10_action=g10_action)
    return g10_action


def _classify_v72_g11_action(
    root: Path,
    *,
    g10_action: Mapping[str, Any],
) -> dict[str, Any]:
    required = (
        (
            V72_G11_VALIDATION_ADDENDUM_RELATIVE_PATH,
            "V72_G11_VALIDATION_ADDENDUM_REQUIRED",
        ),
        (
            V72_G11_TRIPWIRE_POLICY_RELATIVE_PATH,
            "V72_G11_TRIPWIRE_POLICY_REQUIRED",
        ),
        (
            V72_G11_FEATURE_MANIFEST_RELATIVE_PATH,
            "V72_G11_FEATURE_MANIFEST_REQUIRED",
        ),
        (V72_G11_SIGNAL_RELATIVE_PATH, "V72_G11_SIGNAL_MANIFEST_REQUIRED"),
        (V72_G11_FUNNEL_RELATIVE_PATH, "V72_G11_DEVELOPMENT_FUNNEL_REQUIRED"),
        (V72_G11_TRIPWIRE_RELATIVE_PATH, "V72_G11_TRIPWIRE_REQUIRED"),
    )
    for path, action_type in required:
        if not (root / path).is_file():
            return {
                **dict(g10_action),
                "action_type": action_type,
                "progressed": False,
                "required_path": str(path),
                "new_data_purchase_authorized": False,
                "protected_holdout_access_authorized": False,
                "shadow_admission_authorized": False,
                "reason": (
                    "The preregistered G11 chain is incomplete; no later "
                    "inference, power audit or promotion is authorized."
                ),
            }
    drift = [
        path
        for path, expected in V72_G11_FROZEN_HASHES.items()
        if _sha256(root / path) != expected
    ]
    if drift:
        raise V7ControllerIntegrityError(
            "V7.2 G11 frozen evidence drift: " + ",".join(drift)
        )
    signal = _load_json(root / V72_G11_SIGNAL_RELATIVE_PATH)
    funnel = _load_json(root / V72_G11_FUNNEL_RELATIVE_PATH)
    tripwire = _load_json(root / V72_G11_TRIPWIRE_RELATIVE_PATH)
    if (
        int(signal.get("candidate_count") or 0) != 24
        or int(signal.get("nonempty_candidate_count") or 0) != 24
        or int(signal.get("signal_count") or 0) != 1_076
        or int(signal.get("archive_duplicate_count") or 0) != 0
        or int(signal.get("within_manifest_duplicate_count") or 0) != 0
        or signal.get("contains_outcomes_or_pnl") is not False
    ):
        raise V7ControllerIntegrityError("V7.2 G11 signal-manifest drift")
    if (
        int(funnel.get("candidate_count") or 0) != 24
        or int(funnel.get("stage0_valid_novel_count") or 0) != 24
        or int(funnel.get("insufficient_power_count") or 0) != 15
        or int(funnel.get("stage1_pass_count") or 0) != 2
        or int(funnel.get("walk_forward_positive_count") or 0) != 2
        or int(funnel.get("SIM_EXPLOIT_survivor_count") or 0) != 2
        or bool(funnel.get("candidate_nulls_executed"))
        or bool(funnel.get("DSR_BH_executed"))
        or bool(funnel.get("rolling_combine_executed"))
        or int(funnel.get("new_data_purchase_count") or 0) != 0
        or int(funnel.get("protected_holdout_access_count_delta") or 0) != 0
        or int(funnel.get("outbound_order_count") or 0) != 0
    ):
        raise V7ControllerIntegrityError("V7.2 G11 funnel drift")
    if (
        tripwire.get("verdict") != "ARTEFACT_GEOMETRY_ONLY"
        or tripwire.get("evidence_strength") != "ARTEFACT"
        or tripwire.get("raw_pass_counts")
        != {"real": "6/480", "null": "45/1440"}
        or abs(float(tripwire.get("NULL_RATIO") or 0.0) - 2.5) > 1.0e-15
        or bool(tripwire.get("candidate_promotion_authorized"))
        or int(tripwire.get("new_data_purchase_count") or 0) != 0
        or int(tripwire.get("q4_access_count_delta") or 0) != 0
        or int(tripwire.get("proof_window_burn_delta") or 0) != 0
        or int(tripwire.get("outbound_order_count") or 0) != 0
    ):
        raise V7ControllerIntegrityError("V7.2 G11 tripwire drift")
    proof = load_and_verify(root / "mission/state/proof_registry.json")
    current_trials = multiplicity_trial_count(proof)
    by_id = {str(row["event_id"]): row for row in proof["entries"]}
    reservation = by_id.get(
        "v7_2_trade_arrival_renewal_structural_tripwire_reservation_0011"
    )
    annotation = by_id.get(
        "v7_2_trade_arrival_renewal_reservation_0011_commit_annotation"
    )
    if (
        current_trials < 265_227
        or burned_window_ids(proof) != ("Q4_2024",)
        or reservation is None
        or int(reservation["multiplicity"]["delta_trials"]) != 120
        or int(reservation["multiplicity"]["cumulative_N_trials"]) != 265_227
        or reservation.get("evidence", {}).get("results_seen_before_reservation")
        is not False
        or annotation is None
        or annotation.get("references_event_id")
        != "v7_2_trade_arrival_renewal_structural_tripwire_reservation_0011"
    ):
        raise V7ControllerIntegrityError("V7.2 G11 proof-registry drift")
    graveyard = root / "mission/state/graveyard.db"
    conn = sqlite3.connect(f"file:{graveyard}?mode=ro", uri=True)
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(candidate_count),0),COUNT(*),"
            "MIN(evidence_sha256) FROM class_tombstones "
            "WHERE mechanism_class=? AND regime=? AND death_cause=?",
            (
                "v72g11_intraminute_trade_arrival_renewal",
                "D1_2023_2024_DEVELOPMENT_PRICE_NULLS",
                "GEOMETRY_ONLY_NULL_RATIO_GTE_0_8",
            ),
        ).fetchone()
    finally:
        conn.close()
    if row != (
        24,
        1,
        "80a4c4a90fbdc4d12d5ba7694683ace5e0df3902b9f529383c8f3a0c2df6ae34",
    ):
        raise V7ControllerIntegrityError("V7.2 G11 class tombstone is absent")
    g11_action = {
        **dict(g10_action),
        "action_type": (
            "V72_G11_GEOMETRY_ONLY_CLASS_TOMBSTONED_DISTINCT_MECHANISM_PIVOT"
        ),
        "phase": "4",
        "progressed": True,
        "walk_forward_positive_count": 31,
        "evidence_reconciliation_accounted_count": 31,
        "evidence_reconciliation_unaccounted_count": 0,
        "cumulative_power_status_counts": {
            "GEOMETRY_ONLY_CLASS_TOMBSTONED_NO_POWER_AUDIT": 4,
            "PROMISING_UNDERPOWERED": 18,
            "WF_POSITIVE_BUT_FRAGILE": 8,
            "SIM_EXPLOIT": 1,
        },
        "confirmation_queue_geometry_only_retired_count": 4,
        "g11_candidate_count": 24,
        "g11_signal_count": 1_076,
        "g11_stage0_valid_count": 24,
        "g11_insufficient_power_count": 15,
        "g11_stage1_survivor_count": 2,
        "g11_walk_forward_positive_count": 2,
        "g11_SIM_EXPLOIT_survivor_count": 2,
        "g11_tripwire_verdict": "ARTEFACT_GEOMETRY_ONLY",
        "g11_tripwire_evidence_strength": "ARTEFACT",
        "g11_NULL_RATIO": 2.5,
        "g11_real_pass_count": "6/480",
        "g11_null_pass_count": "45/1440",
        "g11_cemetery_candidate_count": 24,
        "g11_power_audit_executed": False,
        "g11_candidate_nulls_executed": False,
        "g11_DSR_BH_executed": False,
        "g11_rolling_combine_executed": False,
        "powered_candidate_count": 0,
        "rolling_combine_promotions": 0,
        "raw_global_N_trials": current_trials,
        "broad_D1_generation_authorized": False,
        "limited_structural_discovery_authorized": True,
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": (
            "hydra_v7_2_distinct_mechanism_hypothesis_review_0012"
        ),
        "next_experiment_state": "WORM_PREREGISTRATION_REQUIRED_NO_DATA_PURCHASE",
        "principal_blocker": (
            "G11 produced two positive stressed walk-forward formulations, but "
            "the permanent class tripwire passed 45/1440 null account paths "
            "versus 6/480 real paths (NULL_RATIO=2.5)."
        ),
        "reason": (
            "The frozen G11 arrival-renewal grammar tested 24 distinct structures. "
            "Two passed Stage 1, walk-forward and 2x costs, but the permanent "
            "tripwire classified the whole class GEOMETRY_ONLY. All 24 structures "
            "are tombstoned at class level; power audit, candidate nulls, DSR/BH, "
            "Rolling Combine, shadow, holdout, data purchase and orders remain "
            "forbidden. The perpetual factory requires a new WORM mechanism."
        ),
    }
    if (root / V72_G12_GRAMMAR_RELATIVE_PATH).is_file():
        return _classify_v72_g12_action(root, g11_action=g11_action)
    return g11_action


def _classify_v72_g12_action(
    root: Path,
    *,
    g11_action: Mapping[str, Any],
) -> dict[str, Any]:
    required = (
        (
            V72_G12_TRIPWIRE_POLICY_RELATIVE_PATH,
            "V72_G12_TRIPWIRE_POLICY_REQUIRED",
        ),
        (
            V72_G12_VALIDATION_ADDENDUM_RELATIVE_PATH,
            "V72_G12_VALIDATION_ADDENDUM_REQUIRED",
        ),
        (
            V72_G12_FEATURE_MANIFEST_RELATIVE_PATH,
            "V72_G12_FEATURE_MANIFEST_REQUIRED",
        ),
        (V72_G12_SIGNAL_RELATIVE_PATH, "V72_G12_SIGNAL_MANIFEST_REQUIRED"),
        (V72_G12_FUNNEL_RELATIVE_PATH, "V72_G12_DEVELOPMENT_FUNNEL_REQUIRED"),
        (V72_G12_TRIPWIRE_RELATIVE_PATH, "V72_G12_TRIPWIRE_REQUIRED"),
    )
    for path, action_type in required:
        if not (root / path).is_file():
            return {
                **dict(g11_action),
                "action_type": action_type,
                "progressed": False,
                "required_path": str(path),
                "new_data_purchase_authorized": False,
                "protected_holdout_access_authorized": False,
                "shadow_admission_authorized": False,
                "reason": (
                    "The preregistered G12 chain is incomplete; no later "
                    "inference, power audit or promotion is authorized."
                ),
            }
    drift = [
        path
        for path, expected in V72_G12_FROZEN_HASHES.items()
        if _sha256(root / path) != expected
    ]
    if drift:
        raise V7ControllerIntegrityError(
            "V7.2 G12 frozen evidence drift: " + ",".join(drift)
        )
    feature_manifest = _load_json(root / V72_G12_FEATURE_MANIFEST_RELATIVE_PATH)
    signal_manifest = _load_json(root / V72_G12_SIGNAL_RELATIVE_PATH)
    funnel = _load_json(root / V72_G12_FUNNEL_RELATIVE_PATH)
    tripwire = _load_json(root / V72_G12_TRIPWIRE_RELATIVE_PATH)
    if (
        int(feature_manifest.get("output", {}).get("row_count") or 0) != 17_200
        or int(feature_manifest.get("output", {}).get("session_count") or 0)
        != 43
        or sum(
            int(row.get("retained_es_rth_record_count") or 0)
            for row in feature_manifest.get("audits", [])
        )
        != 16_129_864
    ):
        raise V7ControllerIntegrityError("V7.2 G12 feature-manifest drift")
    if (
        int(signal_manifest.get("candidate_count") or 0) != 24
        or int(signal_manifest.get("nonempty_candidate_count") or 0) != 24
        or int(signal_manifest.get("signal_count") or 0) != 2_506
        or int(signal_manifest.get("archive_duplicate_count") or 0) != 0
        or int(signal_manifest.get("within_manifest_duplicate_count") or 0) != 0
        or signal_manifest.get("contains_outcomes_or_pnl") is not False
    ):
        raise V7ControllerIntegrityError("V7.2 G12 signal-manifest drift")
    if (
        int(funnel.get("candidate_count") or 0) != 24
        or int(funnel.get("stage0_valid_novel_count") or 0) != 24
        or int(funnel.get("insufficient_power_count") or 0) != 8
        or int(funnel.get("stage1_pass_count") or 0) != 1
        or int(funnel.get("walk_forward_positive_count") or 0) != 0
        or int(funnel.get("SIM_EXPLOIT_survivor_count") or 0) != 0
        or bool(funnel.get("candidate_nulls_executed"))
        or bool(funnel.get("DSR_BH_executed"))
        or bool(funnel.get("rolling_combine_executed"))
        or int(funnel.get("new_data_purchase_count") or 0) != 0
        or int(funnel.get("protected_holdout_access_count_delta") or 0) != 0
        or int(funnel.get("outbound_order_count") or 0) != 0
    ):
        raise V7ControllerIntegrityError("V7.2 G12 funnel drift")
    if (
        tripwire.get("verdict") != "ARTEFACT_GEOMETRY_ONLY"
        or tripwire.get("evidence_strength") != "ARTEFACT"
        or tripwire.get("raw_pass_counts")
        != {"real": "2/480", "null": "57/1440"}
        or abs(float(tripwire.get("NULL_RATIO") or 0.0) - 9.5) > 1.0e-15
        or int(tripwire.get("real", {}).get("mll_breach_count") or 0) != 341
        or bool(tripwire.get("candidate_promotion_authorized"))
        or int(tripwire.get("new_data_purchase_count") or 0) != 0
        or int(tripwire.get("q4_access_count_delta") or 0) != 0
        or int(tripwire.get("proof_window_burn_delta") or 0) != 0
        or int(tripwire.get("outbound_order_count") or 0) != 0
    ):
        raise V7ControllerIntegrityError("V7.2 G12 tripwire drift")
    proof = load_and_verify(root / "mission/state/proof_registry.json")
    current_trials = multiplicity_trial_count(proof)
    by_id = {str(row["event_id"]): row for row in proof["entries"]}
    reservation = by_id.get(
        "v7_2_executed_price_occupancy_structural_tripwire_reservation_0012"
    )
    if (
        current_trials < 265_347
        or burned_window_ids(proof) != ("Q4_2024",)
        or reservation is None
        or int(reservation["multiplicity"]["delta_trials"]) != 120
        or int(reservation["multiplicity"]["cumulative_N_trials"]) != 265_347
        or reservation.get("evidence", {}).get(
            "feature_results_seen_before_reservation"
        )
        is not False
        or reservation.get("evidence", {}).get(
            "signal_results_seen_before_reservation"
        )
        is not False
        or reservation.get("evidence", {}).get(
            "pnl_results_seen_before_reservation"
        )
        is not False
    ):
        raise V7ControllerIntegrityError("V7.2 G12 proof-registry drift")
    graveyard = root / "mission/state/graveyard.db"
    conn = sqlite3.connect(f"file:{graveyard}?mode=ro", uri=True)
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(candidate_count),0),COUNT(*),"
            "MIN(evidence_sha256) FROM class_tombstones "
            "WHERE mechanism_class=? AND regime=? AND death_cause=?",
            (
                "v72g12_executed_price_occupancy_topology",
                "D1_2023_2024_DEVELOPMENT_PRICE_NULLS",
                "GEOMETRY_ONLY_NULL_RATIO_GTE_0_8",
            ),
        ).fetchone()
    finally:
        conn.close()
    if row != (
        24,
        1,
        "8da2418520231d37fd96af3a4a3b60756c6585fd40a19542d78b8f57701c8fbe",
    ):
        raise V7ControllerIntegrityError("V7.2 G12 class tombstone is absent")
    return {
        **dict(g11_action),
        "action_type": (
            "V72_G12_GEOMETRY_ONLY_CLASS_TOMBSTONED_BOOSTED_DUAL_TRACK_REQUIRED"
        ),
        "phase": "4",
        "progressed": True,
        "g12_candidate_count": 24,
        "g12_signal_count": 2_506,
        "g12_stage0_valid_count": 24,
        "g12_insufficient_power_count": 8,
        "g12_stage1_survivor_count": 1,
        "g12_walk_forward_positive_count": 0,
        "g12_SIM_EXPLOIT_survivor_count": 0,
        "g12_tripwire_verdict": "ARTEFACT_GEOMETRY_ONLY",
        "g12_tripwire_evidence_strength": "ARTEFACT",
        "g12_NULL_RATIO": 9.5,
        "g12_real_pass_count": "2/480",
        "g12_null_pass_count": "57/1440",
        "g12_real_mll_breach_count": 341,
        "g12_cemetery_candidate_count": 24,
        "g12_power_audit_executed": False,
        "g12_candidate_nulls_executed": False,
        "g12_DSR_BH_executed": False,
        "g12_rolling_combine_executed": False,
        "powered_candidate_count": 0,
        "rolling_combine_promotions": 0,
        "raw_global_N_trials": current_trials,
        "broad_D1_generation_authorized": False,
        "limited_structural_discovery_authorized": True,
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": "hydra_v7_boosted_dual_track_tournament_0001",
        "next_experiment_state": "WORM_PREREGISTRATION_REQUIRED_NO_DATA_PURCHASE",
        "principal_blocker": (
            "G12 produced no positive walk-forward formulation and its matched "
            "null account paths passed 57/1440 versus 2/480 real paths "
            "(NULL_RATIO=9.5; real MLL breaches 341/480)."
        ),
        "reason": (
            "The frozen G12 executed-price occupancy grammar tested 24 distinct "
            "structures. One passed Stage 1, none remained positive walk-forward, "
            "and the permanent tripwire classified the class GEOMETRY_ONLY. All "
            "24 exact structures are tombstoned; power audit, DSR/BH, Rolling "
            "Combine, shadow, holdout, data purchase and orders remain forbidden. "
            "The next lawful action is a preregistered multi-family mechanism "
            "tournament run concurrently with account-component conversion."
        ),
    }


class V7FalsificationController:
    def __init__(self, config: V7ControllerConfig) -> None:
        if not config.no_live_trading:
            raise V7ControllerIntegrityError("V7 requires no_live_trading=True")
        if config.checkpoint_every_steps <= 0 or config.sleep_seconds < 0.0:
            raise ValueError("invalid V7 controller cadence")
        self.config = config
        self.root = Path(config.project_root).resolve()
        state_dir = Path(config.state_dir)
        if not state_dir.is_absolute():
            state_dir = self.root / state_dir
        self.paths = mission_paths(str(state_dir))
        self._shutdown = False
        self._economic_runtime = (
            EconomicEvolutionRuntime(self.root, self.paths.state_dir)
            if (self.root / CAMPAIGN_CONFIG_RELATIVE_PATH).is_file()
            else None
        )
        self._economic_successor_runtime = (
            EconomicEvolutionSuccessorRuntime(self.root, self.paths.state_dir)
            if (self.root / SUCCESSOR_CONFIG_RELATIVE_PATH).is_file()
            else None
        )
        self._economic_information_runtime = (
            EconomicEvolutionInformationRuntime(self.root, self.paths.state_dir)
            if (self.root / INFORMATION_REVIEW_CONFIG_RELATIVE_PATH).is_file()
            else None
        )
        self._economic_validation_runtime = (
            EconomicEvolutionValidationRuntime(self.root, self.paths.state_dir)
            if (self.root / EXPENSIVE_VALIDATION_CONFIG_RELATIVE_PATH).is_file()
            else None
        )
        self._economic_failure_review_runtime = (
            EconomicEvolutionFailureReviewRuntime(self.root, self.paths.state_dir)
            if (self.root / FAILURE_REVIEW_CONFIG_RELATIVE_PATH).is_file()
            else None
        )
        self._economic_density_runtime = (
            EconomicEvolutionDensityRuntime(self.root, self.paths.state_dir)
            if (self.root / DENSITY_CONFIG_RELATIVE_PATH).is_file()
            else None
        )
        self._economic_density_terminal_runtime = (
            EconomicEvolutionDensityTerminalRuntime(
                self.root, self.paths.state_dir
            )
            if (self.root / DENSITY_TERMINAL_VERDICT_RELATIVE_PATH).is_file()
            else None
        )
        self._economic_agreement_runtime = (
            EconomicEvolutionAgreementRuntime(self.root, self.paths.state_dir)
            if (self.root / AGREEMENT_CONFIG_RELATIVE_PATH).is_file()
            else None
        )
        self._economic_agreement_terminal_runtime = (
            EconomicEvolutionAgreementTerminalRuntime(
                self.root, self.paths.state_dir
            )
            if (self.root / AGREEMENT_TERMINAL_VERDICT_RELATIVE_PATH).is_file()
            else None
        )
        self._economic_cross_session_runtime = (
            EconomicEvolutionCrossSessionRuntime(
                self.root, self.paths.state_dir
            )
            if (self.root / CROSS_SESSION_CONFIG_RELATIVE_PATH).is_file()
            else None
        )
        self._economic_cross_session_terminal_runtime = (
            EconomicEvolutionCrossSessionTerminalRuntime(
                self.root, self.paths.state_dir
            )
            if (
                self.root / CROSS_SESSION_TERMINAL_VERDICT_RELATIVE_PATH
            ).is_file()
            else None
        )

    def run(self) -> int:
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        with mission_lock(self.paths):
            conn = connect_state(self.paths)
            try:
                self._initialize(conn)
                completed = 0
                while not self._shutdown:
                    if stop_requested(self.paths):
                        self._stop_cleanly(conn, "manual_stop_file")
                        return 0
                    self._step(conn)
                    completed += 1
                    if (
                        self.config.maximum_steps is not None
                        and completed >= self.config.maximum_steps
                    ):
                        self._stop_cleanly(conn, "maximum_steps")
                        return 0
                    if not self.config.persistent:
                        self._stop_cleanly(conn, "non_persistent")
                        return 0
                    if self.config.sleep_seconds:
                        time.sleep(self.config.sleep_seconds)
                self._stop_cleanly(conn, "signal")
                return 0
            except Exception as exc:
                if self._economic_runtime is not None:
                    self._economic_runtime.stop()
                if self._economic_successor_runtime is not None:
                    self._economic_successor_runtime.stop()
                if self._economic_information_runtime is not None:
                    self._economic_information_runtime.stop()
                if self._economic_validation_runtime is not None:
                    self._economic_validation_runtime.stop()
                if self._economic_failure_review_runtime is not None:
                    self._economic_failure_review_runtime.stop()
                if self._economic_density_runtime is not None:
                    self._economic_density_runtime.stop()
                if self._economic_agreement_runtime is not None:
                    self._economic_agreement_runtime.stop()
                if self._economic_cross_session_runtime is not None:
                    self._economic_cross_session_runtime.stop()
                set_kv(conn, "service_state", "V7_INTEGRITY_BLOCKED")
                set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
                set_kv(conn, "current_blocker", f"{type(exc).__name__}:{exc}"[:4000])
                write_heartbeat(
                    self.paths,
                    self._heartbeat(
                        conn,
                        action={
                            "action_type": "V7_INTEGRITY_BLOCKED",
                            "reason": str(exc),
                        },
                    ),
                )
                raise
            finally:
                conn.close()

    def _initialize(self, conn: sqlite3.Connection) -> None:
        self._verify_constitution()
        _verify_database_integrity(conn)
        # A clean restore starts with the v1 mission table.  Reuse the
        # additive, non-destructive lifecycle migration before any V7 query or
        # write so crash recovery does not depend on a legacy controller having
        # touched the database first.
        ensure_experiment_schema(conn)
        legacy_active = conn.execute(
            "SELECT experiment_id FROM experiments WHERE status IN ('QUEUED','RUNNING')"
        ).fetchall()
        if legacy_active and any(str(row[0]) != EXPERIMENT_ID for row in legacy_active):
            raise V7ControllerIntegrityError(
                "legacy queued/running work must not coexist with V7"
            )
        payload = {
            "schema": CONTROLLER_SCHEMA,
            "experiment_id": EXPERIMENT_ID,
            "contract_sha256": CONTRACT_SHA256,
            "source_commit": _git_head(self.root),
            "no_live_trading": True,
            "outbound_order_capability": False,
            "config": self.config.to_dict(),
        }
        now = utc_now_iso()
        conn.execute(
            "INSERT INTO experiments(experiment_id,status,payload,updated_at,"
            "experiment_type,specification_hash,result,priority,attempt_count,"
            "max_attempts,created_at,started_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(experiment_id) DO UPDATE SET status='RUNNING',"
            "payload=excluded.payload,updated_at=excluded.updated_at,"
            "started_at=COALESCE(experiments.started_at,excluded.started_at),"
            "last_error=NULL,claim_token=NULL,claimed_by=NULL,lease_expires_at=NULL",
            (
                EXPERIMENT_ID,
                "RUNNING",
                json.dumps(payload, sort_keys=True),
                now,
                "v7_falsification_perpetual",
                CONTRACT_SHA256,
                None,
                1000.0,
                0,
                1,
                now,
                now,
            ),
        )
        lease_expires_at = self._lease_expires_at()
        conn.execute(
            "UPDATE experiments SET claim_token=?,claimed_by=?,lease_expires_at=? "
            "WHERE experiment_id=?",
            (
                CONTROLLER_CLAIM_TOKEN,
                CONTROLLER_OWNER,
                lease_expires_at,
                EXPERIMENT_ID,
            ),
        )
        conn.commit()
        set_kv(conn, "mission_id", EXPERIMENT_ID)
        set_kv(conn, "mission_contract", CONTROLLER_SCHEMA)
        set_kv(conn, "mission_contract_sha256", CONTRACT_SHA256)
        set_kv(conn, "service_state", "RUNNING_V7_FALSIFICATION")
        set_kv(conn, "last_shutdown", None)
        set_kv(conn, "live_trading_enabled", False)
        set_kv(conn, "broker_order_capability", False)
        set_kv(conn, "governance_passed", True)
        set_kv(conn, "v7_controller_version", CONTROLLER_SCHEMA)
        set_kv(
            conn,
            "current_experiment",
            self._current_experiment(lease_expires_at),
        )
        self._refresh_authoritative_runtime_metrics(conn)
        append_event(conn, "V7_CONTROLLER_INITIALIZED", payload)
        append_jsonl(
            self.paths.decision_ledger,
            {
                "created_at_utc": now,
                "decision_type": "V7_CONTROLLER_INITIALIZED",
                "experiment_id": EXPERIMENT_ID,
                "contract_sha256": CONTRACT_SHA256,
                "outbound_orders": 0,
            },
        )

    def _step(self, conn: sqlite3.Connection) -> None:
        contract_text = self._verify_constitution()
        _verify_database_integrity(conn)
        action = classify_v7_action(self.root)
        if self._economic_runtime is not None:
            action = self._economic_runtime.advance(action)
        if self._economic_successor_runtime is not None:
            action = self._economic_successor_runtime.advance(action)
        if self._economic_information_runtime is not None:
            action = self._economic_information_runtime.advance(action)
        if self._economic_validation_runtime is not None:
            action = self._economic_validation_runtime.advance(action)
        if self._economic_failure_review_runtime is not None:
            action = self._economic_failure_review_runtime.advance(action)
        if self._economic_density_runtime is not None:
            action = self._economic_density_runtime.advance(action)
        if self._economic_density_terminal_runtime is not None:
            action = self._economic_density_terminal_runtime.advance(action)
        if self._economic_agreement_runtime is not None:
            action = self._economic_agreement_runtime.advance(action)
        if self._economic_agreement_terminal_runtime is not None:
            action = self._economic_agreement_terminal_runtime.advance(action)
        if self._economic_cross_session_runtime is not None:
            action = self._economic_cross_session_runtime.advance(action)
        if self._economic_cross_session_terminal_runtime is not None:
            action = self._economic_cross_session_terminal_runtime.advance(action)
        previous = get_kv(conn, "v7_current_action", {})
        step = int(get_kv(conn, "v7_step", 0)) + 1
        progress_at = utc_now_iso()
        lease_expires_at = self._lease_expires_at()
        conn.execute(
            "UPDATE experiments SET updated_at=?,lease_expires_at=? "
            "WHERE experiment_id=? AND status='RUNNING' AND claim_token=?",
            (progress_at, lease_expires_at, EXPERIMENT_ID, CONTROLLER_CLAIM_TOKEN),
        )
        conn.commit()
        set_kv(conn, "v7_step", step)
        set_kv(conn, "v7_current_action", action)
        set_kv(conn, "current_action", action)
        set_kv(conn, "current_phase", f"V7_PHASE_{action['phase']}")
        set_kv(conn, "current_blocker", None)
        set_kv(conn, "service_state", "RUNNING_V7_FALSIFICATION")
        set_kv(conn, "last_progress_at_utc", progress_at)
        set_kv(conn, "progress_sequence", int(get_kv(conn, "progress_sequence", 0)) + 1)
        set_kv(
            conn,
            "current_experiment",
            self._current_experiment(lease_expires_at),
        )
        self._refresh_authoritative_runtime_metrics(conn)
        if _stable_json(previous) != _stable_json(action):
            append_event(
                conn,
                "V7_ACTION_TRANSITION",
                {"step": step, "previous": previous, "current": action},
            )
            append_jsonl(
                self.paths.decision_ledger,
                {
                    "created_at_utc": utc_now_iso(),
                    "decision_type": "V7_ACTION_TRANSITION",
                    "experiment_id": EXPERIMENT_ID,
                    "step": step,
                    "previous": previous,
                    "current": action,
                    "outbound_orders": 0,
                },
            )
        checkpoint = str(get_kv(conn, "v7_latest_checkpoint", ""))
        if step % self.config.checkpoint_every_steps == 0:
            checkpoint = str(
                self._checkpoint(conn, step=step, action=action, contract_text=contract_text)
            )
            set_kv(conn, "v7_latest_checkpoint", checkpoint)
        write_heartbeat(self.paths, self._heartbeat(conn, action=action))

    def _verify_constitution(self) -> str:
        contract = self.root / "MISSION_CONTRACT.md"
        if not contract.is_file():
            raise V7ControllerIntegrityError("MISSION_CONTRACT.md is absent")
        text = contract.read_text(encoding="utf-8")
        if hashlib.sha256(text.encode("utf-8")).hexdigest() != CONTRACT_SHA256:
            raise V7ControllerIntegrityError("MISSION_CONTRACT.md hash drift")
        g0 = _load_json(self.root / G0_RELATIVE_PATH)
        g1 = _load_json(self.root / G1_RELATIVE_PATH)
        if g0.get("verdict") != "GREEN" or g1.get("verdict") != "GREEN":
            raise V7ControllerIntegrityError("G0 and G1 must both be frozen GREEN")
        drift = [
            path
            for path, expected in V71_FROZEN_HASHES.items()
            if _sha256(self.root / path) != expected
        ]
        if drift:
            raise V7ControllerIntegrityError(
                "V7.1 frozen constitutional input drift: " + ",".join(drift)
            )
        proof = load_and_verify(self.root / "mission/state/proof_registry.json")
        if burned_window_ids(proof) != ("Q4_2024",):
            raise V7ControllerIntegrityError("unexpected proof-window state")
        if (self.root / CAMPAIGN_CONFIG_RELATIVE_PATH).is_file():
            verify_economic_evolution_freeze(self.root)
        if (self.root / SUCCESSOR_CONFIG_RELATIVE_PATH).is_file():
            verify_successor_freeze(self.root)
        if (self.root / INFORMATION_REVIEW_CONFIG_RELATIVE_PATH).is_file():
            verify_information_review_freeze(self.root)
        if (self.root / EXPENSIVE_VALIDATION_CONFIG_RELATIVE_PATH).is_file():
            verify_expensive_validation_freeze(self.root)
        if (self.root / FAILURE_REVIEW_CONFIG_RELATIVE_PATH).is_file():
            verify_failure_review_freeze(self.root)
        if (self.root / DENSITY_CONFIG_RELATIVE_PATH).is_file():
            density_config = verify_density_freeze(self.root)
            if (self.root / DENSITY_TERMINAL_VERDICT_RELATIVE_PATH).is_file():
                density_result = load_and_verify_density_result(
                    self.root
                    / DENSITY_OUTPUT_RELATIVE_PATH
                    / DENSITY_RESULT_NAME,
                    density_config,
                )
                load_and_verify_density_terminal_verdict(
                    self.root, result=density_result
                )
        if (self.root / AGREEMENT_CONFIG_RELATIVE_PATH).is_file():
            agreement_config = verify_agreement_freeze(self.root)
            if (
                self.root / AGREEMENT_TERMINAL_VERDICT_RELATIVE_PATH
            ).is_file():
                agreement_result = load_and_verify_agreement_result(
                    self.root
                    / AGREEMENT_OUTPUT_RELATIVE_PATH
                    / AGREEMENT_RESULT_NAME,
                    agreement_config,
                )
                load_and_verify_agreement_terminal_verdict(
                    self.root, result=agreement_result
                )
        if (self.root / CROSS_SESSION_CONFIG_RELATIVE_PATH).is_file():
            cross_session_config = verify_cross_session_freeze(self.root)
            if (
                self.root / CROSS_SESSION_TERMINAL_VERDICT_RELATIVE_PATH
            ).is_file():
                cross_session_result = load_and_verify_cross_session_result(
                    self.root
                    / CROSS_SESSION_OUTPUT_RELATIVE_PATH
                    / CROSS_SESSION_RESULT_NAME,
                    cross_session_config,
                )
                load_and_verify_cross_session_terminal_verdict(
                    self.root, result=cross_session_result
                )
        return text

    def _checkpoint(
        self,
        conn: sqlite3.Connection,
        *,
        step: int,
        action: Mapping[str, Any],
        contract_text: str,
    ) -> Path:
        if not contract_text.startswith("# MISSION HYDRA V7"):
            raise V7ControllerIntegrityError("full contract reread failed")
        proof = load_and_verify(self.root / "mission/state/proof_registry.json")
        path = (
            self.root
            / "reports/v7/checkpoints"
            / f"hydra_v7_persistent_step_{step:06d}.md"
        )
        content = "\n".join(
            [
                f"[HYDRA-V7] phase={action['phase']} step={step} verdict=GREEN",
                f"gate=V7_PERSISTENCE preuve=MISSION_CONTRACT.md#{CONTRACT_SHA256[:8]} tests=deploiement_persistant",
                f"budget_llm=usage_API_non_exposee/solde budget_data=registre_persistant N_trials={_multiplicity(proof)} burned={len(burned_window_ids(proof))}",
                "diff_validation=aucun CONTRE=un_controleur_sain_ne_prouve_pas_un_edge_et_ne_doit_jamais_etre_compte_comme_resultat_scientifique",
                f"prochaine_action={action['action_type']}",
                "",
                "Justification : clauses 1, 5 et 8 — préserver les verdicts, le registre de preuve et zéro ordre broker.",
                "Auto-audit : le risque principal est de confondre continuité opérationnelle et progression scientifique.",
                "",
            ]
        )
        _atomic_text(path, content)
        append_event(
            conn,
            "V7_CONSTITUTIONAL_CHECKPOINT",
            {"step": step, "path": str(path), "sha256": _sha256(path)},
        )
        return path

    def _heartbeat(
        self, conn: sqlite3.Connection, *, action: Mapping[str, Any]
    ) -> dict[str, Any]:
        heartbeat = {
            "controller_version": CONTROLLER_SCHEMA,
            "mission_id": EXPERIMENT_ID,
            "service_state": get_kv(conn, "service_state", "UNKNOWN"),
            "phase": get_kv(conn, "current_phase", "UNKNOWN"),
            "step": int(get_kv(conn, "v7_step", 0)),
            "current_action": dict(action),
            "latest_checkpoint": get_kv(conn, "v7_latest_checkpoint", ""),
            "last_progress_at_utc": get_kv(conn, "last_progress_at_utc", None),
            "current_experiment": get_kv(conn, "current_experiment", {}),
            "q4_access_count": int(get_kv(conn, "q4_access_count", 0)),
            "cumulative_databento_spend_usd": float(
                get_kv(conn, "cumulative_databento_spend_usd", 0.0)
            ),
            "remaining_databento_budget_usd": float(
                get_kv(conn, "remaining_databento_budget_usd", 0.0)
            ),
            "registry_n_trials": int(get_kv(conn, "v7_registry_n_trials", 0)),
            "process_lock": str(self.paths.lock_path),
            "single_writer": True,
            "broker_connections": 0,
            "outbound_orders": 0,
            "automatic_order_capability": False,
        }
        if self._economic_runtime is not None:
            heartbeat["economic_evolution_predecessor"] = (
                self._economic_runtime.snapshot()
            )
        if self._economic_successor_runtime is not None:
            heartbeat["economic_evolution"] = (
                self._economic_successor_runtime.snapshot()
            )
        elif self._economic_runtime is not None:
            heartbeat["economic_evolution"] = self._economic_runtime.snapshot()
        if self._economic_information_runtime is not None:
            heartbeat["economic_evolution_information_review"] = (
                self._economic_information_runtime.snapshot()
            )
        if self._economic_validation_runtime is not None:
            heartbeat["economic_evolution_expensive_validation"] = (
                self._economic_validation_runtime.snapshot()
            )
        if self._economic_failure_review_runtime is not None:
            heartbeat["economic_evolution_failure_review"] = (
                self._economic_failure_review_runtime.snapshot()
            )
        if self._economic_density_runtime is not None:
            heartbeat["economic_evolution_density_diversification"] = (
                self._economic_density_runtime.snapshot()
            )
        if self._economic_density_terminal_runtime is not None:
            heartbeat["economic_evolution_density_terminal"] = (
                self._economic_density_terminal_runtime.snapshot()
            )
        if self._economic_agreement_runtime is not None:
            heartbeat["economic_evolution_directional_agreement"] = (
                self._economic_agreement_runtime.snapshot()
            )
        if self._economic_agreement_terminal_runtime is not None:
            heartbeat["economic_evolution_agreement_terminal"] = (
                self._economic_agreement_terminal_runtime.snapshot()
            )
        if self._economic_cross_session_runtime is not None:
            heartbeat["economic_evolution_cross_session_account"] = (
                self._economic_cross_session_runtime.snapshot()
            )
        if self._economic_cross_session_terminal_runtime is not None:
            heartbeat["economic_evolution_cross_session_terminal"] = (
                self._economic_cross_session_terminal_runtime.snapshot()
            )
        return heartbeat

    def _lease_expires_at(self) -> str:
        seconds = max(90.0, self.config.sleep_seconds * 4.0 + 30.0)
        return (
            datetime.now(timezone.utc) + timedelta(seconds=seconds)
        ).replace(microsecond=0).isoformat()

    def _refresh_authoritative_runtime_metrics(
        self, conn: sqlite3.Connection
    ) -> None:
        budget = DatabentoBudgetConfig()
        _estimated, actual = cumulative_spend(self.root / budget.ledger_path)
        proof = load_and_verify(self.root / "mission/state/proof_registry.json")
        access_count = q4_access_count(
            str(self.root / "reports/data_access/data_access_ledger.jsonl")
        )
        set_kv(conn, "cumulative_databento_spend_usd", float(actual))
        set_kv(
            conn,
            "remaining_databento_budget_usd",
            max(float(budget.hard_cap_usd) - float(actual), 0.0),
        )
        set_kv(conn, "q4_access_count", int(access_count))
        set_kv(conn, "v7_registry_n_trials", _multiplicity(proof))

    @staticmethod
    def _current_experiment(lease_expires_at: str) -> dict[str, Any]:
        return {
            "experiment_id": EXPERIMENT_ID,
            "experiment_type": "v7_falsification_perpetual",
            "status": "RUNNING",
            "claimed_by": CONTROLLER_OWNER,
            "lease_expires_at": lease_expires_at,
        }

    def _stop_cleanly(self, conn: sqlite3.Connection, reason: str) -> None:
        if self._economic_runtime is not None:
            self._economic_runtime.stop()
        if self._economic_successor_runtime is not None:
            self._economic_successor_runtime.stop()
        if self._economic_information_runtime is not None:
            self._economic_information_runtime.stop()
        if self._economic_validation_runtime is not None:
            self._economic_validation_runtime.stop()
        if self._economic_failure_review_runtime is not None:
            self._economic_failure_review_runtime.stop()
        if self._economic_density_runtime is not None:
            self._economic_density_runtime.stop()
        if self._economic_agreement_runtime is not None:
            self._economic_agreement_runtime.stop()
        if self._economic_cross_session_runtime is not None:
            self._economic_cross_session_runtime.stop()
        now = utc_now_iso()
        set_kv(conn, "service_state", "STOPPED_CLEANLY_V7")
        set_kv(conn, "current_phase", "STOPPED_CLEANLY")
        set_kv(conn, "last_shutdown", "clean")
        set_kv(conn, "last_stop_reason", reason)
        set_kv(conn, "current_experiment", {})
        conn.execute(
            "UPDATE experiments SET status='COMPLETED',updated_at=?,completed_at=?,"
            "result=? WHERE experiment_id=?",
            (
                now,
                now,
                json.dumps({"status": "STOPPED_CLEANLY", "reason": reason}),
                EXPERIMENT_ID,
            ),
        )
        conn.commit()
        write_heartbeat(
            self.paths,
            self._heartbeat(
                conn,
                action={"action_type": "STOPPED_CLEANLY", "reason": reason},
            ),
        )

    def _handle_signal(self, _signum: int, _frame: Any) -> None:
        self._shutdown = True


def run_v7_controller(config: V7ControllerConfig) -> int:
    return V7FalsificationController(config).run()


def _verify_database_integrity(conn: sqlite3.Connection) -> None:
    result = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
    if result != "ok":
        raise V7ControllerIntegrityError(f"mission DB integrity failed: {result}")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise V7ControllerIntegrityError(f"required artifact is absent: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise V7ControllerIntegrityError(f"artifact must be an object: {path}")
    return payload


def _multiplicity(proof: Mapping[str, Any]) -> int:
    values = [
        int(entry.get("multiplicity", {}).get("cumulative_N_trials", 0))
        for entry in proof.get("entries", [])
        if isinstance(entry, Mapping)
    ]
    return max(values, default=0)


def _git_head(root: Path) -> str:
    import subprocess

    source_root = root if (root / ".git").exists() else Path(__file__).resolve().parents[2]
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=source_root, text=True
    ).strip()


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


__all__ = [
    "CONTROLLER_SCHEMA",
    "EXPERIMENT_ID",
    "V7ControllerConfig",
    "V7ControllerIntegrityError",
    "V7FalsificationController",
    "classify_v7_action",
    "run_v7_controller",
]
