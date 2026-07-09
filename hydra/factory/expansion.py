from __future__ import annotations

from hydra.backtest.engine import run_backtest
from hydra.data.loader import load_market_data
from hydra.features.market_state import build_market_state
from hydra.propfirm.topstep_rules import evaluate_topstep_style
from hydra.registry.candidates import upsert_candidate
from hydra.validation.correlation import correlation_cluster
from hydra.validation.no_leak import audit_no_lookahead
from hydra.validation.robustness import robustness_score


def evaluate_and_log(conn, candidate, config: dict, synthetic: bool, seed: int, existing_curves: dict) -> str:
    raw = load_market_data(candidate.symbol, candidate.timeframe, synthetic, seed)
    df = build_market_state(raw)
    leak_ok, leak_reason = audit_no_lookahead(df)
    result = run_backtest(candidate, df, seed)
    prop = evaluate_topstep_style(result.equity_curve, config["propfirm"])
    robust = robustness_score(result, seed)
    cluster, correlated = correlation_cluster(candidate.candidate_id, result.equity_curve, existing_curves)
    metrics = result.metrics
    status = "QUALIFIED"
    reason = None
    if not leak_ok:
        status, reason = "REJECTED_NOT_ROBUST", leak_reason
    elif metrics["trade_count"] < config["validation"]["min_trades"]:
        status, reason = "REJECTED_TOO_FEW_TRADES", "below_min_trade_count"
    elif metrics["profit_factor"] < config["validation"]["min_profit_factor"] or metrics["net_profit"] <= 0:
        status, reason = "REJECTED_NO_EDGE", "profit_factor_or_net_profit_below_threshold"
    elif prop["mll_breached"]:
        status, reason = "REJECTED_MLL_BREACH", "trailing_mll_breached"
    elif prop["mll_buffer"] < config["propfirm"]["reject_if_mll_buffer_below"]:
        status, reason = "REJECTED_MLL_BUFFER_TOO_LOW", "mll_buffer_below_absolute_floor"
    elif correlated:
        status, reason = "REJECTED_CORRELATED", "equity_curve_correlation_too_high"
    elif robust < 0.45:
        status, reason = "REJECTED_NOT_ROBUST", "robustness_score_below_threshold"
    elif metrics["max_drawdown"] > config["propfirm"]["max_loss_limit"]:
        status, reason = "REJECTED_TOO_MUCH_DRAWDOWN", "drawdown_above_prop_limit"
    upsert_candidate(conn, candidate, metrics, prop, status, reason, robust, cluster)
    if status == "QUALIFIED":
        existing_curves[candidate.candidate_id] = result.equity_curve
    return status
