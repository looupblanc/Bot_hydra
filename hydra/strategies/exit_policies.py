from __future__ import annotations


EXIT_POLICIES = [
    "fixed_stop_fixed_target",
    "atr_stop_target",
    "volatility_scaled_stop_target",
    "break_even_after_R",
    "trailing_after_R",
    "time_stop",
    "session_close_exit",
    "daily_profit_lock_exit",
    "internal_daily_stop_exit",
    "mll_buffer_protection_exit",
    "partial_profit_take_then_runner",
    "reduce_size_near_mll_exit",
]
