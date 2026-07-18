"""Pump phase detection and health scoring."""

import time
from data.collector import candles_15m
from constants import (
    PUMP_HEALTH_MIN_SCORE,
    PUMP_HEALTH_CAUTION_SCORE,
    PUMP_MAX_AGE_HOURS,
    PUMP_MAX_CORRECTION_PCT,
    PUMP_MAX_BROKEN_LEVELS,
    PUMP_BLEED_MIN_RED_CANDLES,
    PUMP_BLEED_MIN_VOL_TREND,
)


def detect_pump_peak(symbol: str) -> tuple[float, float, float]:
    """
    Returns (pump_high, pump_base, pump_high_time).
    If no pump found — (0.0, 0.0, 0.0).
    """
    c15m = candles_15m.get(symbol, [])
    if len(c15m) < 20:
        return 0.0, 0.0, 0.0

    # Look for peak in last 32 candles (8 hours)
    window = c15m[-32:]
    peak_idx = max(range(len(window)), key=lambda i: window[i]["high"])
    pump_high = window[peak_idx]["high"]
    pump_high_time = window[peak_idx]["open_time"] / 1000  # unix seconds

    # Pump base — minimum before peak
    # peak_idx is relative to window (0..31); convert to absolute index in c15m
    # window = c15m[-32:], so window[0] = c15m[len(c15m)-32]
    abs_peak = len(c15m) - 32 + peak_idx
    # pre-peak slice: up to 50 candles before peak
    pre_peak_start = max(0, abs_peak - 50)
    pre_peak = c15m[pre_peak_start:abs_peak]

    if not pre_peak:
        # fallback: use the candles before the window
        pre_peak = c15m[:-32] if len(c15m) > 32 else []
    if not pre_peak:
        return 0.0, 0.0, 0.0

    pump_base = min(c["low"] for c in pre_peak)

    # Minimum 5% growth required
    if pump_base <= 0 or (pump_high - pump_base) / pump_base < 0.05:
        return 0.0, 0.0, 0.0

    return pump_high, pump_base, pump_high_time


def _is_bleed_structure(symbol: str) -> bool:
    """
    True if last 6 x 15M candles show organised distribution:
    5+ red candles AND rising volume.
    """
    c15m = candles_15m.get(symbol, [])
    if len(c15m) < 6:
        return False

    last = c15m[-6:]
    red_count = sum(1 for c in last if c["close"] < c["open"])
    if red_count < PUMP_BLEED_MIN_RED_CANDLES:
        return False

    vols = [c["volume"] for c in last]
    increasing = sum(1 for i in range(1, len(vols)) if vols[i] > vols[i - 1])
    return increasing >= PUMP_BLEED_MIN_VOL_TREND


def pump_health_score(state, current_price: float) -> int:
    """
    Calculates pump health score 0–100.
    state: SymbolState instance with pump_high, pump_base_price, pump_high_time, broken_since_pump.
    """
    score = 100

    # 1. Freshness penalty
    if state.pump_high_time > 0:
        hours = (time.time() - state.pump_high_time) / 3600
        if hours > PUMP_MAX_AGE_HOURS:
            score -= 40
        elif hours > PUMP_MAX_AGE_HOURS / 2:
            score -= 20

    # 2. Correction depth penalty
    # < 50%  → 0 penalty  (normal correction, levels still ahead)
    # 50–65% → -20 points (deep correction, caution)
    # > 65%  → -40 points (pump almost fully given back)
    pump_body = state.pump_high - state.pump_base_price
    if pump_body > 0 and current_price > 0:
        correction = (state.pump_high - current_price) / pump_body
        if correction > PUMP_MAX_CORRECTION_PCT:   # > 65%
            score -= 40
        elif correction > 0.50:                    # 50–65%
            score -= 20

    # 3. Broken levels penalty
    if state.broken_since_pump >= PUMP_MAX_BROKEN_LEVELS:
        score -= 50
    elif state.broken_since_pump == 1:
        score -= 15

    # 4. Bleed structure penalty
    if _is_bleed_structure(state.symbol):
        score -= 15

    return max(0, min(100, score))


def get_pump_phase(score: int) -> str:
    """Map health score to phase label."""
    if score >= PUMP_HEALTH_CAUTION_SCORE:
        return "active"    # fresh pump, monitor freely
    if score >= PUMP_HEALTH_MIN_SCORE:
        return "caution"   # only strong levels
    if score >= 30:
        return "degraded"  # better skip
    return "dead"           # pump over, stop monitoring


def calc_correction_pct(state) -> float:
    """Helper: correction % of pump body for display."""
    pump_body = state.pump_high - state.pump_base_price
    if pump_body <= 0 or state.pump_high <= 0:
        return 0.0
    c15m = candles_15m.get(state.symbol, [])
    if not c15m:
        return 0.0
    current = c15m[-1]["close"]
    return max(0.0, (state.pump_high - current) / pump_body)
