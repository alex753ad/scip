"""
Pure computational utilities — no circular-import risk.

Все функции здесь принимают данные как аргументы (candles: list[dict])
и не читают глобальное состояние (candles_1m, candles_15m).

Это позволяет импортировать их на уровне модуля из любого файла
(main.py, monitor.py, telegram.py, trigger.py) без circular imports.

trigger.py по-прежнему экспортирует обёртки с той же сигнатурой
(symbol: str → ...) — они просто делегируют сюда.
"""

from __future__ import annotations
from logger import logger


# ---------------------------------------------------------------------------
# ATR
# ---------------------------------------------------------------------------

def calc_atr(candles: list[dict], period: int = 14) -> float:
    """Wilder ATR (with gaps) from raw candle list.

    Args:
        candles: list of dicts with keys high, low, close
        period:  ATR window (default 14)

    Returns 0.0 if not enough data.
    """
    if len(candles) < period + 1:
        return 0.0
    recent = candles[-(period + 1):]
    trs = []
    for i in range(1, len(recent)):
        hl = recent[i]["high"] - recent[i]["low"]
        hc = abs(recent[i]["high"] - recent[i - 1]["close"])
        lc = abs(recent[i]["low"]  - recent[i - 1]["close"])
        trs.append(max(hl, hc, lc))
    return sum(trs) / len(trs)


def calc_atr_pct(candles: list[dict], period: int = 14) -> float:
    """ATR as percentage of current price."""
    atr = calc_atr(candles, period)
    if atr == 0.0 or not candles:
        return 0.0
    current_price = candles[-1]["close"]
    if current_price == 0:
        return 0.0
    return (atr / current_price) * 100


def calc_atr_ratio(candles: list[dict], level: float, period: int = 14) -> float:
    """Distance from current price to level in ATR units."""
    if not candles:
        return 0.0
    atr = calc_atr(candles, period)
    if atr == 0:
        return 0.0
    return round(abs(candles[-1]["close"] - level) / atr, 2)


# ---------------------------------------------------------------------------
# Volume ratio
# ---------------------------------------------------------------------------

def calc_vol_ratio(candles: list[dict]) -> float:
    """Last candle volume / blended 24h+1h MA.

    Mirrors trigger._calc_vol_ratio logic.
    """
    if len(candles) < 2:
        return 1.0
    current_vol = candles[-1]["volume"]
    avg_24h = sum(c["volume"] for c in candles) / len(candles)
    recent_60 = candles[-60:] if len(candles) >= 60 else candles
    avg_1h = sum(c["volume"] for c in recent_60) / len(recent_60) if recent_60 else 1.0
    avg = (avg_24h + avg_1h) / 2
    if avg == 0:
        return 1.0
    return round(current_vol / avg, 2)


def calc_vol_ratio_ma20(candles: list[dict]) -> float:
    """Last candle volume / MA(20). Used in get_vol_ratio_current."""
    if len(candles) < 20:
        return 1.0
    avg_vol_20 = sum(c["volume"] for c in candles[-20:]) / 20
    if avg_vol_20 == 0:
        return 1.0
    return round(candles[-1]["volume"] / avg_vol_20, 2)


# ---------------------------------------------------------------------------
# Approach style
# ---------------------------------------------------------------------------

def detect_approach_style_from_candles(candles: list[dict], n_candles: int = 5) -> str:
    """Classify approach style from raw candle list.

    Returns: 'flash' | 'impulse' | 'bleed' | 'unknown'
    Mirrors trigger.detect_approach_style logic.
    """
    if len(candles) < max(n_candles, 20):
        return "unknown"

    recent = candles[-n_candles:]
    avg_vol_20 = sum(c["volume"] for c in candles[-20:]) / 20

    # flash: one candle with volume >= 2x avg AND body >= 0.5%
    for c in recent:
        if avg_vol_20 > 0 and c["volume"] >= 2 * avg_vol_20:
            body_pct = abs(c["close"] - c["open"]) / c["open"] * 100 if c["open"] > 0 else 0
            if body_pct >= 0.5:
                return "flash"

    # impulse: 3+ green candles then red reversal
    if len(recent) >= 4:
        greens_before = sum(1 for c in recent[:-1] if c["close"] > c["open"])
        last_is_red = recent[-1]["close"] < recent[-1]["open"]
        if greens_before >= 3 and last_is_red:
            return "impulse"

    # bleed: 4+ consecutive red candles with growing volume
    red_streak: list[dict] = []
    for c in recent:
        if c["close"] < c["open"]:
            red_streak.append(c)
        else:
            red_streak = []
    if len(red_streak) >= 4:
        vols = [c["volume"] for c in red_streak]
        if all(vols[i] <= vols[i + 1] for i in range(len(vols) - 1)):
            return "bleed"

    return "unknown"


# ---------------------------------------------------------------------------
# Strength calculation
# ---------------------------------------------------------------------------

def calculate_strength(lvl: dict) -> dict:
    """Calculate strength (1-5) and verdict for a level.

    Pure function — reads only from lvl dict, no global state.
    Mutates and returns the same dict with added fields:
        strength: int (1-5)
        verdict:  'hold' | 'exit' | 'exit_fast'

    Kept in sync with trigger.calculate_strength.
    trigger.calculate_strength now delegates here.
    """
    from constants import (
        STRENGTH_APPROACH_EXIT_THRESHOLD,
        STRENGTH_PUMP_VOLUME_LOW_THRESHOLD,
        STRENGTH_PUMP_VOLUME_HIGH_THRESHOLD,
    )

    level_type        = lvl.get("type", "body_level")
    position          = lvl.get("position", "mid_move")
    approach          = lvl.get("approach", 1)
    vol_ratio         = lvl.get("vol_ratio", 1.0)
    cluster           = lvl.get("cluster", False)
    pump_volume_ratio = lvl.get("pump_volume_ratio", 1.5)
    was_broken        = lvl.get("was_broken", False)
    sweep_reclaimed   = lvl.get("sweep_reclaimed", False)
    max_vol_on_approach = lvl.get("max_vol_on_approach", 0)
    zone_approaches   = lvl.get("zone_approaches", 0)
    engulf_15m        = lvl.get("engulf_15m", False)
    poc_aligned       = lvl.get("poc_aligned", False)
    hourly_open_bonus = lvl.get("hourly_open_bonus", 0)
    round_number_bonus = lvl.get("round_number_bonus", 0)
    candle_count      = lvl.get("candle_count", 0)

    # Base strength by level type
    if level_type == "pump_base":
        strength = 5
    elif level_type == "breakout_level":
        strength = 5
    elif level_type in ("consolidation_base", "body_level", "order_block"):
        strength = 4
    elif level_type in ("consolidation", "wick_level"):
        strength = 3
    else:
        strength = 2

    verdict = "hold"

    # POC alignment — strongest factor
    if poc_aligned:
        strength += 2
        logger.debug("POC aligned bonus", level=lvl.get("level"), bonus=2)

    # Hourly open alignment (meaningful only for select types)
    if hourly_open_bonus >= 2 and level_type in ("pump_base", "order_block", "consolidation_base"):
        strength += 1
        logger.debug("Hourly open bonus", level=lvl.get("level"), bonus=hourly_open_bonus)

    # Round number proximity
    if round_number_bonus >= 2:
        strength += 1
        logger.debug("Round number bonus", level=lvl.get("level"), bonus=round_number_bonus)

    # Candle count bonus
    if 4 <= candle_count <= 15:
        strength += 1
    elif candle_count <= 1:
        strength -= 1

    # Approach count penalty
    if approach >= STRENGTH_APPROACH_EXIT_THRESHOLD:
        strength -= 2
        verdict = "exit"

    # Position bonus
    if position == "origin":
        strength += 1

    # Cluster penalty
    if cluster:
        strength -= 1
        if strength < 4:
            verdict = "exit"

    # Pump volume ratio
    if pump_volume_ratio < STRENGTH_PUMP_VOLUME_LOW_THRESHOLD:
        strength -= 1

    # History penalties
    if was_broken and not sweep_reclaimed and position not in ("in_move", "origin"):
        strength -= 2
        strength = min(strength, 2)
    if max_vol_on_approach > vol_ratio * 2:
        strength -= 1

    # Zone exhaustion
    if zone_approaches == 2:
        strength -= 1
    elif zone_approaches == 3:
        strength -= 2
    elif zone_approaches >= 4:
        strength -= 3
        verdict = "exit"

    # Engulfing pattern
    if engulf_15m and vol_ratio > 2:
        verdict = "exit_fast"

    strength = max(1, min(5, strength))

    lvl["strength"] = strength
    lvl["verdict"]  = verdict

    logger.debug(
        "Strength calculated",
        level=lvl.get("level"),
        type=level_type,
        strength=strength,
        verdict=verdict,
        poc_aligned=poc_aligned,
        candle_count=candle_count,
    )
    return lvl
