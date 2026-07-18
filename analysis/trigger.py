"""Trigger detection and level strength calculation."""

from data.collector import candles_15m, candles_1m
from analysis.level_builder import build_levels, _find_pump_legs
from constants import (
    TRIGGER_GROWTH_THRESHOLD,
    LEVEL_APPROACH_THRESHOLD,
    LEVEL_REAL_CLUSTER_MIN_TOUCHES,
    LEVEL_REAL_CLUSTER_SHIFT_THRESHOLD,
    ATR_PERIOD,
    STRENGTH_PUMP_VOLUME_LOW_THRESHOLD,
    STRENGTH_PUMP_VOLUME_HIGH_THRESHOLD,
    STRENGTH_APPROACH_EXIT_THRESHOLD,
)
from logger import logger
from utils import (
    calc_atr,
    calc_atr_pct,
    calc_atr_ratio,
    calc_vol_ratio,
    calc_vol_ratio_ma20,
    detect_approach_style_from_candles,
    calculate_strength,
)
import statistics
import asyncio


def find_real_level(symbol: str, level: float) -> tuple[float, int]:
    """
    Find real cluster of touches near the given level using 15M candles.
    Counts touches only after the last pump peak to avoid inflated numbers.
    """
    c1m = candles_1m.get(symbol, [])
    c15m = candles_15m.get(symbol, [])
    if len(c1m) < ATR_PERIOD or not c15m:
        return level, 0

    current_price = c1m[-1]["close"]
    atr = calculate_atr(symbol)
    if atr == 0 or current_price == 0:
        return level, 0

    zone_radius = atr * 0.3  # Fixed radius: 0.3 ATR

    # Find pump peak time - count touches only after pump (use recent candles, not all-time)
    recent_c15m = c15m[-50:]  # FIX BUG-13: pump_high взят из [-50:], искать peak_time тоже в нём
    pump_high = max(c["high"] for c in recent_c15m)
    pump_peak_time = next((c["open_time"] for c in recent_c15m if c["high"] >= pump_high * 0.999), None)

    touches = []
    for c in recent_c15m:  # FIX BUG-13: итерировать по тому же срезу, что и pump_peak_time
        # Only count after pump peak
        if pump_peak_time and c["open_time"] < pump_peak_time:
            continue
        # One touch per candle - use the closest extreme to the level
        dist_low = abs(c["low"] - level)
        dist_high = abs(c["high"] - level)
        if dist_low <= zone_radius and dist_low <= dist_high:
            touches.append(c["low"])
        elif dist_high <= zone_radius:
            touches.append(c["high"])

    if len(touches) < LEVEL_REAL_CLUSTER_MIN_TOUCHES:
        return level, 0

    real_level = statistics.median(touches)
    if abs(real_level - level) > zone_radius * LEVEL_REAL_CLUSTER_SHIFT_THRESHOLD:
        from analysis.level_builder import _round_level
        logger.debug("Level adjusted by cluster",
                    symbol=symbol,
                    original=level,
                    adjusted=real_level,
                    touches=len(touches))
        return _round_level(real_level), len(touches)

    return level, len(touches)


def calculate_atr(symbol: str, c1m: list[dict] = None) -> float:
    """Calculate Average True Range for symbol (Wilder, includes gaps)."""
    if c1m is None:
        c1m = candles_1m.get(symbol, [])
    return calc_atr(c1m, ATR_PERIOD)


def calculate_atr_pct(symbol: str) -> float:
    """Calculate ATR as percentage of current price."""
    return calc_atr_pct(candles_1m.get(symbol, []), ATR_PERIOD)


def check_trigger(symbol: str) -> bool:
    """
    Check if correction trigger is activated.
    
    Trigger conditions:
    - Growth >= TRIGGER_GROWTH_THRESHOLD on last 4x15M candles
    - Last 1M candle is red (close < open)
    """
    c15m = candles_15m.get(symbol, [])
    c1m = candles_1m.get(symbol, [])
    if len(c15m) < 4 or len(c1m) < 2:
        return False

    recent_15m = c15m[-4:]
    start_price = recent_15m[0]["open"]
    high_price = max(c["high"] for c in recent_15m)
    if start_price == 0:
        return False
    growth = (high_price - start_price) / start_price
    if growth < TRIGGER_GROWTH_THRESHOLD:
        return False

    last_1m = c1m[-1]
    if last_1m["close"] >= last_1m["open"]:
        return False

    logger.debug("Trigger conditions met", 
                symbol=symbol, 
                growth_pct=round(growth * 100, 2))
    return True


async def get_approaching_levels(symbol: str, use_claude: bool = True) -> list[dict]:
    """
    Get levels that are approaching current price.
    
    Args:
        symbol: Trading symbol
        use_claude: If True, use Claude Haiku for strength calculation
    
    Returns:
        List of level dicts with technical indicators calculated.
    """
    c1m = candles_1m.get(symbol, [])
    if not c1m:
        return []

    current_price = c1m[-1]["close"]
    atr = calculate_atr(symbol)
    if atr == 0:
        return []

    threshold = atr * LEVEL_APPROACH_THRESHOLD
    levels = build_levels(symbol)
    approaching = []

    # LEVEL-06: sort by proximity so closer levels claim candles first
    levels_sorted = sorted(levels, key=lambda l: abs(current_price - l["level"]))
    claimed_times: set = set()
    for lvl in levels_sorted:
        real, _ = find_real_level(symbol, lvl["level"])
        lvl["level"] = real
        distance = abs(current_price - lvl["level"])
        if distance <= threshold:
            # FIX BUG-6: _count_approaches возвращает (count, claimed) — распаковываем tuple
            # Вариант A: стабильный origin уровня (не дрейфует на новых максимумах)
            approach_count, new_claimed = _count_approaches(symbol, lvl["level"], atr,
                                                             exclude_open_times=claimed_times,
                                                             anchor_time=_origin_anchor(symbol, lvl["level"]))
            lvl["touches_count"] = approach_count  # HIGH-2: renamed from "approach" — this is the touches count ML expects
            claimed_times |= new_claimed
            lvl["vol_ratio"] = _calc_vol_ratio(symbol)
            history = get_level_history(symbol, lvl["level"], atr)
            lvl.update(history)
            approaching.append(lvl)

    atr_pct = calculate_atr_pct(symbol)
    zone_radius = atr_pct / 100 * current_price if current_price > 0 else 0

    for lvl in approaching:
        nearby = [
            other for other in levels
            if other["level"] != lvl["level"]
            and abs(other["level"] - lvl["level"]) <= zone_radius
        ]
        zone_approaches = sum(
            _count_approaches(symbol, other["level"], atr,
                              anchor_time=_origin_anchor(symbol, other["level"]))[0]  # FIX BUG-6: [0] = count из tuple
            for other in nearby
        )
        lvl["zone_approaches"] = zone_approaches
        lvl["atr_pct"] = round(atr_pct, 3)

    # Calculate strength with Claude or fallback to Python
    if use_claude and approaching:
        try:
            c15m = candles_15m.get(symbol, [])
            
            # Extract POC from levels
            poc_price = None
            for lvl in levels:
                if lvl.get("poc_aligned"):
                    poc_price = lvl["level"]
                    break
            
            # Import here to avoid circular dependency
            from analysis.claude_strength import calculate_strength_with_claude
            
            # Call async function directly (we're already in async context)
            approaching = await calculate_strength_with_claude(symbol, c15m, approaching, poc_price)
            
            logger.info("Claude strength calculation completed",
                       symbol=symbol,
                       levels_count=len(approaching))
        except Exception as e:
            logger.error("Failed to use Claude, falling back to Python",
                        symbol=symbol,
                        error=str(e))
            # Fallback to Python calculation
            for lvl in approaching:
                calculate_strength(lvl)
    else:
        # Use Python calculation
        for lvl in approaching:
            calculate_strength(lvl)

    # ML scoring: adjust strength and add p_bounce / expected_depth
    try:
        from analysis.ml_score import apply_ml_to_level
        for lvl in approaching:
            apply_ml_to_level(lvl)
    except Exception as e:
        logger.warning("ml_score failed in get_approaching_levels: %s", e)

    logger.debug("Approaching levels found", 
                symbol=symbol, 
                count=len(approaching), 
                threshold=round(threshold, 4))
    return approaching


# ── Approach-counter origin (вариант A: якорь = происхождение уровня) ─────────
# Считаем подходы с момента ФОРМИРОВАНИЯ уровня, а не от последнего ценового пика.
# Origin = open_time пика пампа, определившего уровень. Берётся ОДИН раз при первом
# обнаружении (symbol, level) и мемоизируется, чтобы новые максимумы при гринд-апе
# не сдвигали якорь вперёд (старый баг: _count_approaches обнулял счёт на каждом пике).
# Упрощение: при повторном пампе того же уровня origin не пересчитывается.
_level_origin_time: dict[tuple[str, float], float] = {}


def _origin_anchor(symbol: str, level: float) -> float:
    """Стабильный origin (open_time пика пампа) для подсчёта подходов к уровню.

    Возвращает 0.0, если памп-леги не найдены — тогда _count_approaches считает по
    всему доступному буферу 1m-свечей (без отсечки), что безопасно (буфер ≤300 свечей).
    Мемоизируется по (symbol, level), поэтому не дрейфует на новых максимумах.
    """
    key = (symbol, round(level, 10))
    cached = _level_origin_time.get(key)
    if cached is not None:
        return cached
    origin = 0.0
    try:
        c15m = candles_15m.get(symbol, [])
        legs = _find_pump_legs(c15m)
        if legs:
            peak_idx = max(leg[3] for leg in legs)
            if 0 <= peak_idx < len(c15m):
                origin = float(c15m[peak_idx]["open_time"])
    except Exception as e:
        logger.debug("origin_anchor: pump legs failed, anchor=0", symbol=symbol, error=str(e))
    _level_origin_time[key] = origin
    return origin


def _count_approaches(symbol: str, level: float, atr: float,
                       exclude_open_times: set = None,
                       anchor_time: float = None) -> tuple[int, set]:
    """Count number of times price approached the level after its origin.

    exclude_open_times: set of candle open_times already claimed by a
    neighbouring level (LEVEL-06 fix — prevents overlapping approach zones
    from double-counting the same candles for two close levels).

    anchor_time: стабильный origin уровня (из _origin_anchor) — вариант A. Если задан,
    подходы считаются от него и НЕ обнуляются на новых ценовых максимумах. Если None —
    legacy-поведение (последний пик в окне 100×15m), оставлено для обратной совместимости.

    Returns (count, claimed) tuple.  # FIX BUG-6: было return int + атрибут на функции → race condition в asyncio
    """
    c1m = candles_1m.get(symbol, [])
    c15m = candles_15m.get(symbol, [])
    threshold = atr * LEVEL_APPROACH_THRESHOLD

    if anchor_time is not None:
        # Вариант A: стабильный origin уровня (передан вызывающим из _origin_anchor).
        # anchor_time == 0.0 означает «отсечки нет» → считаем по всему буферу.
        pump_high_time = anchor_time if anchor_time > 0 else None
    else:
        # Legacy: последний пик в окне (дрейфует на новых максимумах — старый баг).
        pump_high_time = None
        if c15m:
            recent_c15m = c15m[-100:]
            pump_high = max(c["high"] for c in recent_c15m)
            # FIX BUG-13: итерируем в обратном порядке чтобы взять ПОСЛЕДНИЙ пик,
            # а не первый — иначе старый памп с тем же high обнуляет все касания
            for c in reversed(recent_c15m):
                if c["high"] >= pump_high * 0.999:
                    pump_high_time = c["open_time"]
                    break

    count = 0
    was_near = False
    claimed: set = set()  # open_times of candles that belong to this approach

    for c in c1m:
        if pump_high_time and c["open_time"] < pump_high_time:
            was_near = False
            continue
        # Skip candles already claimed by a closer level
        if exclude_open_times and c["open_time"] in exclude_open_times:
            was_near = False
            continue
        near = (
            abs(c["low"] - level) <= threshold or
            abs(c["close"] - level) <= threshold
        )
        if near:
            claimed.add(c["open_time"])
        if near and not was_near:
            count += 1
        was_near = near

    # FIX BUG-6: убран _count_approaches._last_claimed — не потокобезопасно в asyncio
    return count, claimed


def get_level_history(symbol: str, level: float, atr: float) -> dict:
    """Get historical behavior of a level.

    was_broken is set only when the candle CLOSES below level AND its low
    is within 3×ATR of the level.  This prevents a neighbouring level's
    confirmed breakout (e.g. 0.2068) from contaminating a lower level
    (e.g. 0.1968) that has never been touched yet.
    """
    c1m = candles_1m.get(symbol, [])
    c15m = candles_15m.get(symbol, [])
    threshold = atr * LEVEL_APPROACH_THRESHOLD
    # Maximum distance from the level at which a candle is considered "related"
    # to this specific level (not a neighbouring one).
    # 3×ATR is wide enough to capture real wicks/sweeps but tight enough to
    # exclude price action that occurred 10-15% above the level.
    broken_zone = atr * 3.0

    # Filter: only consider candles after pump peak
    # BUG-07: use same recent window (last 100 candles ≈ 25h) to avoid picking
    # up a stale pump from weeks ago as the "peak" — identical to _count_approaches.
    pump_high_time = None
    if c15m:
        recent_c15m = c15m[-100:]
        pump_high = max(c["high"] for c in recent_c15m)
        # FIX BUG-13: reversed() — берём ПОСЛЕДНИЙ пик, не первый
        pump_high_time = next((c["open_time"] for c in reversed(recent_c15m) if c["high"] >= pump_high * 0.999), None)

    was_broken = False
    sweep_reclaimed = False
    price_min = None
    max_vol_on_approach = 0.0

    for c in c1m:
        if pump_high_time and c["open_time"] < pump_high_time:
            continue
        if price_min is None or c["low"] < price_min:
            price_min = c["low"]

        near = (
            abs(c["low"] - level) <= threshold or
            abs(c["close"] - level) <= threshold or
            c["low"] < level
        )
        if near:
            if c["volume"] > max_vol_on_approach:
                max_vol_on_approach = c["volume"]

        # Breakout: close below level AND candle originated within broken_zone of level.
        # This ensures that a full breakdown from a HIGHER level (e.g. 0.2068)
        # where candles are 5-10% above THIS level do NOT set was_broken=True here.
        candle_dist_from_level = abs(c["open"] - level)
        if c["close"] < level and candle_dist_from_level <= broken_zone:
            was_broken = True

        if was_broken and c["close"] > level:
            sweep_reclaimed = True

    return {
        "was_broken": was_broken,
        "sweep_reclaimed": sweep_reclaimed,
        "price_min_since_level": price_min if price_min is not None else level,
        "max_vol_on_approach": round(max_vol_on_approach, 2),
    }


def get_breakout_info(symbol: str, level: float) -> dict:
    """Возвращает информацию о пробое уровня."""
    c15m = candles_15m.get(symbol, [])
    if not c15m:
        return {"type": "breakout", "zakol_pct": 0, "rebound_pct": 0}

    zakol_candles = [
        c for c in c15m
        if c["low"] < level and c["high"] >= level
    ]

    if zakol_candles:
        min_low = min(c["low"] for c in zakol_candles)
        zakol_pct = round((level - min_low) / level * 100, 2)

        after_zakol = [c for c in c15m if c["open_time"] >= zakol_candles[-1]["open_time"]]
        closes_above = [c["close"] for c in after_zakol if c["close"] > level]
        rebound_pct = round((max(closes_above) - level) / level * 100, 2) if closes_above else 0

        if rebound_pct > 0:
            return {"type": "zakol", "zakol_pct": zakol_pct, "rebound_pct": rebound_pct}

    return {"type": "breakout", "zakol_pct": 0, "rebound_pct": 0}


def detect_approach_style(symbol: str, n_candles: int = 5) -> str:
    """Classify how price is approaching a level based on recent 1M candles."""
    return detect_approach_style_from_candles(candles_1m.get(symbol, []), n_candles)


def calculate_atr_ratio(symbol: str, level: float) -> float:
    """Distance from current price to level divided by ATR(14) on 1M."""
    return calc_atr_ratio(candles_1m.get(symbol, []), level, ATR_PERIOD)


def get_vol_ratio_current(symbol: str) -> float:
    """Volume of last 1M candle / MA20 volume of 1M candles."""
    return calc_vol_ratio_ma20(candles_1m.get(symbol, []))


def get_btc_change_1m() -> float:
    """BTC % change over the last 1M candle."""
    btc = candles_1m.get("BTCUSDT", [])
    if len(btc) < 2:
        return 0.0
    prev_close = btc[-2]["close"]
    if prev_close == 0:
        return 0.0
    return round((btc[-1]["close"] - prev_close) / prev_close * 100, 4)


_binance_client = None  # shared singleton to avoid creating a new connection per call
_binance_client_lock = None  # asyncio.Lock — created lazily


async def _get_shared_binance_client():
    """Return (and lazily create) a shared AsyncClient singleton."""
    global _binance_client, _binance_client_lock
    if _binance_client_lock is None:
        _binance_client_lock = asyncio.Lock()
    async with _binance_client_lock:
        if _binance_client is None:
            from binance import AsyncClient
            _binance_client = await AsyncClient.create()
    return _binance_client


async def get_funding_rate(symbol: str) -> float | None:
    """Fetch latest funding rate from Binance.
    
    Uses a shared persistent AsyncClient (singleton) to avoid creating
    a new HTTP connection on every call (BUG-03 fix).
    Returns None on error.
    """
    global _binance_client
    try:
        client = await _get_shared_binance_client()
        data = await client.futures_funding_rate(symbol=symbol, limit=1)
        if data:
            return float(data[-1]["fundingRate"])
    except Exception:
        # Connection might be stale — reset singleton so next call reconnects
        logger.debug("Failed to fetch funding rate, resetting client", symbol=symbol)
        _binance_client = None
    return None


def _calc_vol_ratio(symbol: str) -> float:
    return calc_vol_ratio(candles_1m.get(symbol, []))


# calculate_strength is re-exported from utils (imported above)
