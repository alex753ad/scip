"""Simplified level building - find levels, let Claude judge strength."""

from data.collector import candles_15m, candles_1m
from logger import logger
from constants import ATR_PERIOD
import statistics


def _round_level(price: float) -> float:
    """Round price to appropriate precision."""
    if price > 100:
        return round(price, 2)
    elif price >= 1:
        return round(price, 4)
    elif price >= 0.1:
        return round(price, 5)
    elif price >= 0.01:
        return round(price, 6)
    else:
        return round(price, 8)


def _calc_atr(candles: list[dict], period: int) -> float:
    """Calculate ATR (Wilder, includes gaps). Works for any timeframe.
    Note: avoids circular import with trigger.calculate_atr.
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


def _calc_atr_1m(c1m: list[dict]) -> float:
    return _calc_atr(c1m, ATR_PERIOD)


def _calc_atr_15m(c15m: list[dict]) -> float:
    return _calc_atr(c15m, 20)


def _timeframe_bonus(open_time_ms: int) -> int:
    """Bonus if candle opens at significant timeframe (hourly, 4h)."""
    ts = open_time_ms // 1000
    minute = (ts % 3600) // 60
    hour = (ts % 86400) // 3600

    if minute == 0 and hour % 4 == 0:
        return 3  # 4-hour open
    if minute == 0:
        return 2  # Hourly open
    if minute == 30:
        return 1  # Half-hour
    return 0


def _round_number_bonus(price: float) -> int:
    """Bonus if price is near round number."""
    if price <= 0:
        return 0

    if price >= 1000:
        step = 100
    elif price >= 100:
        step = 10
    elif price >= 10:
        step = 1
    elif price >= 1:
        step = 0.1
    elif price >= 0.1:
        step = 0.01
    elif price >= 0.01:
        step = 0.001
    else:
        step = 0.0001

    nearest = round(price / step) * step
    distance_pct = abs(price - nearest) / price * 100

    if distance_pct <= 0.3:
        return 2
    elif distance_pct <= 0.8:
        return 1
    return 0


def _calculate_poc_simple(c15m: list[dict], range_low: float, range_high: float, atr: float) -> float | None:
    """
    Calculate Point of Control (POC) - price level with maximum volume.
    Volume distributed over candle BODY (open-close), not full wick range,
    for more accurate POC placement.
    """
    if not c15m or range_low >= range_high:
        return None

    bin_size = atr * 0.2
    if bin_size == 0:
        return None

    range_candles = [
        c for c in c15m
        if range_low <= c["low"] <= range_high or range_low <= c["high"] <= range_high
    ]

    if not range_candles:
        return None

    volume_by_price = {}

    for candle in range_candles:
        # FIX: distribute volume over body (open-close), not full wick
        body_low  = max(min(candle["open"], candle["close"]), range_low)
        body_high = min(max(candle["open"], candle["close"]), range_high)
        candle_volume = candle["volume"]

        # Doji or very small body — fall back to wick range clipped to range
        if body_high <= body_low:
            body_low  = max(candle["low"],  range_low)
            body_high = min(candle["high"], range_high)

        if body_high <= body_low:
            bin_price = round((body_low - range_low) / bin_size) * bin_size + range_low
            volume_by_price[bin_price] = volume_by_price.get(bin_price, 0) + candle_volume
            continue

        body_range = body_high - body_low
        num_bins = int(body_range / bin_size) + 1

        for i in range(num_bins):
            bin_low  = body_low + i * bin_size
            bin_high = min(bin_low + bin_size, body_high)
            bin_mid  = (bin_low + bin_high) / 2

            if bin_mid > body_high:
                break

            overlap = (bin_high - bin_low) / body_range
            bin_volume = candle_volume * overlap

            volume_by_price[bin_mid] = volume_by_price.get(bin_mid, 0) + bin_volume

    if not volume_by_price:
        return None

    poc_price = max(volume_by_price.items(), key=lambda x: x[1])[0]
    return poc_price


def _find_breakout_level(
    c15m: list[dict],
    pump_start_idx: int,
    pump_high: float,
    atr: float,
) -> tuple[float, int, dict] | None:
    """Find the consolidation ceiling = top of pre-pump range = breakout support.

    This is the level from which price launched the pump. After the pump, it
    becomes the most important support — where price should hold on pullback.

    Logic:
    1. From pump_start_idx, scan forward until we hit the first explosive candle
       (body >= 1.5x ATR) — that's where consolidation ends and pump begins.
    2. The ceiling = highest body_top across all consolidation candles.
    """
    if pump_start_idx <= 0 or atr <= 0:
        return None

    # Find where consolidation ends (first big-body candle = pump launch)
    consolidation_end = pump_start_idx
    for i in range(pump_start_idx, min(pump_start_idx + 100, len(c15m))):
        body = abs(c15m[i]["close"] - c15m[i]["open"])
        if body >= atr * 1.5:
            consolidation_end = i
            break
    else:
        # No explosive candle found — pump was gradual, use full range
        consolidation_end = min(pump_start_idx + 60, len(c15m))

    consol = c15m[pump_start_idx:consolidation_end]
    if len(consol) < 3:
        return None

    # Ceiling = max body_top of consolidation candles (not wicks — body only)
    ceiling = max(max(c["open"], c["close"]) for c in consol)

    # Sanity: ceiling must be below pump_high with some margin
    if ceiling >= pump_high * 0.97:
        return None

    total_vol = sum(c["volume"] for c in consol)

    return ceiling, len(consol), {
        "volume_at_level":    total_vol,
        "hourly_open_bonus":  0,
        "round_number_bonus": _round_number_bonus(ceiling),
    }


def _find_consolidation_zones(c15m: list[dict], support_range_low: float, support_range_high: float, atr: float) -> list[tuple[float, int, dict]]:
    """Find tight consolidation zones where price spent significant time.

    BUG-35/36 fix: cluster_radius and tight-zone threshold are now price-relative
    (0.5% of median price) rather than a fixed ATR multiple.  The global 1M ATR
    that was previously passed here was far too wide for low-price altcoins, causing
    consolidation zones to merge across 2+ ATR distances.  We also compute a local
    15M ATR per window so the tightness check adapts to the volatility of that slice.
    """
    if len(c15m) < 10:
        return []

    zones = []
    window = 8
    step = window // 2  # = 4, avoids heavy overlap (BUG-21 fix)

    for i in range(0, len(c15m) - window, step):
        chunk = c15m[i:i+window]
        chunk_high = max(c["high"] for c in chunk)
        chunk_low  = min(c["low"]  for c in chunk)
        chunk_range = chunk_high - chunk_low

        import statistics as _stats
        price = _stats.median(c["close"] for c in chunk)
        if price <= 0:
            continue

        # BUG-35: price-relative radius — 0.5% of price, bounded by the passed ATR.
        # Prevents over-merging on cheap altcoins where ATR*0.3 > 1% of price.
        radius = min(price * 0.005, atr * 1.5) if atr > 0 else price * 0.005

        # BUG-36: local ATR for the window so the tightness check reflects this
        # slice's volatility rather than the global session ATR.
        if len(chunk) >= 3:
            local_trs = [
                max(chunk[j]["high"] - chunk[j]["low"],
                    abs(chunk[j]["high"] - chunk[j-1]["close"]),
                    abs(chunk[j]["low"]  - chunk[j-1]["close"]))
                for j in range(1, len(chunk))
            ]
            local_atr = sum(local_trs) / len(local_trs)
        else:
            local_atr = atr

        # Consolidation is tight if range < 3× local ATR
        if chunk_range > local_atr * 3:
            continue

        if not (support_range_low <= price <= support_range_high):
            continue

        # Skip if an equivalent level is already queued
        if any(abs(z[0] - price) < radius for z in zones):
            continue

        # Count candles within radius of median close
        count = sum(1 for c in chunk if abs(c["close"] - price) <= radius)
        if count >= 4:
            vol = sum(c["volume"] for c in chunk)
            zones.append((price, count, {"volume_at_level": vol, "type": "consolidation_base"}))

    return zones


def _build_levels_no_pump(
    symbol: str,
    c1m: list[dict],
    c15m: list[dict],
    atr: float,
    atr_15m: float,
    current_price: float,
) -> list[dict]:
    """
    Fallback level builder for symbols without a detectable pump.
    Uses consolidation zones + body levels + 1M near-zone scan.
    Returns up to 7 levels sorted by quality score.
    """
    support_range_low  = current_price * 0.80 if current_price > 0 else 0
    support_range_high = current_price * 1.05 if current_price > 0 else float("inf")
    cluster_radius = max(atr_15m * 0.3, current_price * 0.001) if atr_15m > 0 else max(atr * 0.5, current_price * 0.003)
    # BUG-35: cap at 0.5% of current_price
    if current_price > 0:
        cluster_radius = min(cluster_radius, current_price * 0.005)

    all_levels = []

    # Consolidation zones from 15M
    for price, candle_count, metadata in _find_consolidation_zones(c15m, support_range_low, support_range_high, atr_15m if atr_15m > 0 else atr):
        all_levels.append({
            "level": _round_level(price),
            "type": "consolidation_base",
            "candle_count": candle_count,
            "poc_aligned": False,
            **metadata,
        })

    # Body levels from 15M (no pump_peak_time filter)
    for price, candle_count, metadata in _find_body_levels_simple(
        c15m, support_range_low, support_range_high,
        atr_15m if atr_15m > 0 else atr,
        cluster_radius=cluster_radius,
        pump_peak_time=0,
    ):
        all_levels.append({
            "level": _round_level(price),
            "type": "body_level",
            "candle_count": candle_count,
            "poc_aligned": False,
            **metadata,
        })

    # 1M near-zone levels
    for price, candle_count, metadata in _find_1m_near_zone_levels(c1m, current_price, near_zone_pct=0.20, atr=atr):
        all_levels.append({
            "level": _round_level(price),
            "type": "body_level",
            "candle_count": candle_count,
            "poc_aligned": False,
            **metadata,
        })

    if not all_levels:
        return []

    levels = _deduplicate_simple(all_levels, cluster_radius)
    levels = [lvl for lvl in levels if support_range_low <= lvl["level"] <= support_range_high]
    levels = _assign_positions(levels, support_range_low, support_range_high)
    levels = _mark_clusters(levels)

    # Top-7 by quality score (same as main build_levels)
    def _quality(lvl: dict) -> float:
        score = lvl.get("candle_count", 0) * 10
        score += lvl.get("hourly_open_bonus", 0) * 5
        score += lvl.get("round_number_bonus", 0) * 3
        if lvl["type"] == "consolidation_base":
            score += 2000
        return score

    levels.sort(key=_quality, reverse=True)
    levels = levels[:7]

    logger.debug("Fallback levels built (no pump)", symbol=symbol, count=len(levels),
                prices=[_round_level(l["level"]) for l in levels])
    return levels


def build_levels(symbol: str, c1m_override: list[dict] = None, c15m_override: list[dict] = None) -> list[dict]:
    """
    Build support/resistance levels with SIMPLE logic.

    Philosophy:
    - Find ALL potential levels (pump bases, body levels, wicks)
    - Calculate Volume Profile POC (Point of Control)
    - Attach metadata (volume, timeframe alignment, round numbers)
    - Let trigger.py calculate strength based on metadata
    """
    c1m  = c1m_override  if c1m_override  is not None else candles_1m.get(symbol,  [])
    c15m = c15m_override if c15m_override is not None else candles_15m.get(symbol, [])

    if len(c1m) < 20 or len(c15m) < 5:
        return []

    atr = _calc_atr_1m(c1m)
    if atr == 0:
        return []

    # FIX Bug-3: compute 15M ATR separately — used for pump_base search radius
    atr_15m = _calc_atr_15m(c15m)

    current_price = c1m[-1]["close"] if c1m else 0

    pump_legs = _find_pump_legs(c15m)
    logger.debug("pump_legs result", symbol=symbol, count=len(pump_legs), legs=[(round(l[0],6), round(l[1],6)) for l in pump_legs])
    if not pump_legs:
        logger.debug("No pump legs found — using consolidation fallback", symbol=symbol, c15m_len=len(c15m))
        return _build_levels_no_pump(symbol, c1m, c15m, atr, atr_15m, current_price)

    pump_low  = min(leg[0] for leg in pump_legs)
    pump_high = max(leg[1] for leg in pump_legs)

    # FIX Bug-1 & Bug-2: derive pump_start_idx and pump_peak_time directly from
    # _find_pump_legs results — no more scanning from beginning of c15m
    pump_start_idx = min(leg[2] for leg in pump_legs)   # earliest low index
    pump_peak_idx  = max(leg[3] for leg in pump_legs)   # latest high index
    pump_peak_time = c15m[pump_peak_idx]["open_time"] if pump_peak_idx < len(c15m) else 0

    # Cluster radius based on 15M ATR
    # BUG-35: cap at 0.5% of current_price to prevent over-merging on cheap altcoins
    cluster_radius = max(atr_15m * 0.3, current_price * 0.001) if atr_15m > 0 else max(atr * 0.5, current_price * 0.003)
    if current_price > 0:
        cluster_radius = min(cluster_radius, current_price * 0.005)

    logger.debug("Pump found",
                symbol=symbol,
                low=pump_low,
                high=pump_high,
                legs=len(pump_legs),
                pump_start_idx=pump_start_idx,
                pump_peak_idx=pump_peak_idx,
                move_pct=round((pump_high - pump_low) / pump_low * 100, 2))

    # Expand range to 40% to capture pump bases and POC on vertical moves.
    # Scalping usually looks at 5-10%, but analysis needs full context.
    support_range_low  = current_price * 0.60 if current_price > 0 else pump_low
    support_range_high = current_price * 1.05 if current_price > 0 else pump_high

    # POC: calculated from pump PEAK onward (post-pump consolidation).
    # LEVEL-03: cap at 48 candles (~12h) so we don't pull 2-day noise into the POC.
    # If pump peak is very recent (< 5 candles ago), use at least 5 candles.
    poc_start = max(pump_peak_idx, len(c15m) - 48)
    poc_candles = c15m[poc_start:]
    if len(poc_candles) < 5:
        poc_candles = c15m[pump_peak_idx:]
    last_leg_low  = max(leg[0] for leg in pump_legs)
    # POC range should also be wide enough to catch the consolidation
    poc_range_low = min(last_leg_low, current_price * 0.70)
    poc_price = _calculate_poc_simple(poc_candles, poc_range_low, support_range_high, atr)

    if poc_price:
        logger.debug("POC calculated", symbol=symbol, poc=_round_level(poc_price))

    all_levels = []

    # 1. Pump base levels — one per leg
    seen_bases: set[float] = set()
    for leg_low, leg_high, leg_low_idx, _ in pump_legs:
        if leg_low < support_range_low or leg_low > support_range_high:
            continue
        if any(abs(leg_low - s) <= cluster_radius for s in seen_bases):
            continue
        seen_bases.add(leg_low)
        # FIX Bug-3: pass atr_15m so pump_base search uses correct radius
        pump_base_levels = _find_pump_base_simple(c15m, leg_low, atr, atr_15m)
        for price, candle_count, metadata in pump_base_levels:
            all_levels.append({
                "level": _round_level(price),
                "type": "pump_base",
                "candle_count": candle_count,
                "poc_aligned": False,
                **metadata
            })

    # 1b. Breakout level — consolidation ceiling (the level price launched from)
    # This is the most important post-pump support, often missed by body/wick detection
    for leg in pump_legs:
        leg_start_idx = leg[2]
        breakout = _find_breakout_level(c15m, leg_start_idx, pump_high, atr)
        if breakout:
            price, candle_count, metadata = breakout
            if support_range_low <= price <= support_range_high:
                all_levels.append({
                    "level":        _round_level(price),
                    "type":         "breakout_level",
                    "candle_count": candle_count,
                    "poc_aligned":  False,
                    **metadata
                })

    # 2. Body levels
    body_levels = _find_body_levels_simple(
        c15m, support_range_low, support_range_high, atr, cluster_radius, pump_peak_time
    )
    for price, candle_count, metadata in body_levels:
        all_levels.append({
            "level": _round_level(price),
            "type": "body_level",
            "candle_count": candle_count,
            "poc_aligned": False,
            **metadata
        })

    # 3. Wick levels
    # FIX Bug-2: pass pump_peak_time from legs, not re-searched from start of c15m
    wick_levels = _find_wick_levels_simple(c15m, pump_high, atr, cluster_radius, pump_peak_time)
    for price, candle_count, metadata in wick_levels:
        all_levels.append({
            "level": _round_level(price),
            "type": "wick_level",
            "candle_count": candle_count,
            "poc_aligned": False,
            **metadata
        })

    # 3b. Mid-impulse pauses — short stalls inside pump legs, missed by body/wick detection
    mid_pauses = _find_mid_impulse_pauses(c15m, pump_legs, atr_15m)
    for price, candle_count, metadata in mid_pauses:
        if support_range_low <= price <= support_range_high:
            all_levels.append({
                "level":        _round_level(price),
                "type":         "mid_impulse_pause",
                "candle_count": candle_count,
                "poc_aligned":  False,
                **metadata
            })
    if mid_pauses:
        logger.debug("Mid-impulse pauses found", symbol=symbol, count=len(mid_pauses),
                    prices=[_round_level(p) for p, _, _ in mid_pauses])

    # 1M near-zone scan: catches pauses visible on 1M but swallowed by single 15M candles.
    # 20% zone covers post-pump consolidation range.
    near_zone_levels = _find_1m_near_zone_levels(c1m, current_price, near_zone_pct=0.20, atr=atr)
    for price, candle_count, metadata in near_zone_levels:
        all_levels.append({
            "level":        _round_level(price),
            "type":         "body_level",
            "candle_count": candle_count,
            "poc_aligned":  False,
            **metadata
        })

    # 4. Order block
    # FIX Bug-1: pass pump_start_idx from legs, not re-searched from start of c15m
    order_block = _find_order_block_simple(c15m, pump_low, pump_high, pump_start_idx)
    if order_block:
        price, metadata = order_block
        all_levels.append({
            "level": _round_level(price),
            "type": "order_block",
            "candle_count": 1,
            "poc_aligned": False,
            **metadata
        })

    # 5. Consolidation zones
    consolidation_zones = _find_consolidation_zones(c15m, support_range_low, support_range_high, atr)
    for price, candle_count, metadata in consolidation_zones:
        all_levels.append({
            "level": _round_level(price),
            "type": "consolidation_base",
            "candle_count": candle_count,
            "poc_aligned": False,
            **metadata
        })

    # Adjust cluster radius: for vertical moves (PLAY, BSB), fixed radius might be too large
    # and swallow intermediate levels.
    dynamic_radius = cluster_radius
    if current_price > 0:
        move_pct = (pump_high - pump_low) / pump_low if pump_low > 0 else 0
        if move_pct > 0.5: # If move > 50%, reduce radius to catch intermediate zones
            dynamic_radius = cluster_radius * 0.5
            logger.debug("Vertical move detected, reducing cluster radius", 
                         symbol=symbol, old=cluster_radius, new=dynamic_radius)

    levels = _deduplicate_simple(all_levels, dynamic_radius)

    # Mark POC alignment — allow multiple levels within tight radius
    if poc_price:
        for lvl in levels:
            distance = abs(lvl["level"] - poc_price)
            # Use cluster_radius for strict alignment
            if distance <= cluster_radius:
                lvl["poc_aligned"] = True
                logger.debug("POC aligned to level",
                           symbol=symbol,
                           poc=_round_level(poc_price),
                           level=lvl["level"],
                           distance=round(distance, 6))

        # If no level aligned yet, try snap to closest within 2x radius
        if not any(l.get("poc_aligned") for l in levels):
            closest_level = None
            min_dist = float("inf")
            for lvl in levels:
                dist = abs(lvl["level"] - poc_price)
                if dist < min_dist:
                    min_dist = dist
                    closest_level = lvl
            
            if closest_level and min_dist <= cluster_radius * 2:
                closest_level["poc_aligned"] = True
                logger.debug("POC snapped to closest level",
                           symbol=symbol,
                           poc=_round_level(poc_price),
                           level=closest_level["level"],
                           distance=round(min_dist, 6))
        
        # If still no level aligned and POC is in range, add it as separate level
        if not any(l.get("poc_aligned") for l in levels) and support_range_low <= poc_price <= support_range_high:
            # Use cluster_radius (15M-based) — atr*0.3 (1M) was ~0.00005,
            # too tight to find any candles near the POC price.
            candles_at_poc = [
                c for c in c15m
                if (abs(c["close"] - poc_price) <= cluster_radius or
                    abs(c["open"]  - poc_price) <= cluster_radius or
                    abs(c["low"]   - poc_price) <= cluster_radius or
                    abs(c["high"]  - poc_price) <= cluster_radius)
            ]
            if len(candles_at_poc) >= 2:
                total_volume = sum(c["volume"] for c in candles_at_poc)
                hourly_bonus = max(_timeframe_bonus(c["open_time"]) for c in candles_at_poc)
                round_bonus  = _round_number_bonus(poc_price)
                levels.append({
                    "level":            _round_level(poc_price),
                    "type":             "body_level",
                    "candle_count":     len(candles_at_poc),
                    "poc_aligned":      True,
                    "volume_at_level":  total_volume,
                    "hourly_open_bonus": hourly_bonus,
                    "round_number_bonus": round_bonus,
                })
                logger.debug("POC added as separate level",
                           symbol=symbol, poc=_round_level(poc_price),
                           candles=len(candles_at_poc))
                levels.sort(key=lambda x: x["level"])

    levels = [lvl for lvl in levels if support_range_low <= lvl["level"] <= support_range_high]

    logger.debug("Levels before top-7 filter",
                symbol=symbol,
                count=len(levels),
                prices=[round(l["level"], 6) for l in sorted(levels, key=lambda x: x["level"])])

    # FIX Bug-4: limit pump_base guarantee to 2 closest to current price.
    # With many legs, all pump_bases would fill the top-7, squeezing out
    # body/wick levels that show real post-pump reaction zones.
    pump_bases_by_proximity = sorted(
        [l for l in levels if l["type"] == "pump_base"],
        key=lambda l: abs(current_price - l["level"])
    )
    # Use object identity to mark only the 2 closest pump_bases
    priority_base_ids = {id(pb) for pb in pump_bases_by_proximity[:2]}

    def level_quality(lvl: dict) -> int:
        score = 0
        if lvl.get("poc_aligned"):
            score += 10000
        if id(lvl) in priority_base_ids:
            score += 5000   # only top-2 pump_bases guaranteed
        if lvl.get("type") == "mid_impulse_pause":
            score += 1500   # ensure mid-impulse pauses compete for top-10 slots
        
        # Proximity bonus: levels closer to current price are more relevant
        distance_pct = abs(current_price - lvl["level"]) / current_price if current_price > 0 else 1
        score += int((1.0 - min(distance_pct, 1.0)) * 100)
        
        score += lvl.get("candle_count", 0) * 10
        score += lvl.get("hourly_open_bonus", 0) * 5
        score += lvl.get("round_number_bonus", 0) * 3
        return score

    levels.sort(key=level_quality, reverse=True)
    levels = levels[:10]  # Increased from 7 to 10 to capture more context
    levels.sort(key=lambda x: x["level"])

    levels = _assign_positions(levels, pump_low, pump_high)
    levels = _mark_clusters(levels)

    logger.debug("Levels built",
                symbol=symbol,
                count=len(levels),
                pump_base=sum(1 for l in levels if l["type"] == "pump_base"),
                body=sum(1 for l in levels if l["type"] == "body_level"))

    return levels


def _find_last_pump(c15m: list[dict]) -> tuple[float, float]:
    """Find the last significant pump. Returns overall pump_low and pump_high."""
    legs = _find_pump_legs(c15m)
    if not legs:
        return 0, 0
    return min(leg[0] for leg in legs), max(leg[1] for leg in legs)


def _find_pump_legs(c15m: list[dict]) -> list[tuple[float, float, int, int]]:
    """
    Find all impulse legs within the last significant pump.

    Returns list of (leg_low, leg_high, low_orig_idx, high_orig_idx).
    """
    if len(c15m) < 4:
        return []

    # Search for the most recent pump peak: prefer last 72 candles (~18h),
    # fall back to 200 if no pump found in the recent window.
    window_size = min(200, len(c15m))
    recent_size = min(72, len(c15m))
    window      = c15m[-recent_size:]
    high_price  = max(c["high"] for c in window)
    current_price_val = c15m[-1]["close"] if c15m else 0
    # If recent peak is more than 30% away from current price, it's stale — use wider window
    if current_price_val > 0 and (high_price - current_price_val) / current_price_val > 0.30:
        window      = c15m[-window_size:]
        high_price  = max(c["high"] for c in window)
    high_idx    = None

    for i in range(len(c15m) - 1, max(0, len(c15m) - window_size), -1):
        if c15m[i]["high"] >= high_price * 0.999:
            high_idx = i
            break

    if high_idx is None:
        logger.debug("_find_pump_legs: no high_idx found", symbol="?", window=window_size, high_price=round(high_price,6))
        return []

    pump_start_idx = None
    # LEVEL-02: slow pumps can span > 60 candles — search up to 200 candles before high.
    # Use a two-pass approach: first try strict 5% in 100 candles (fast pump),
    # then fall back to 3% in 200 candles (slow/grinding pump).
    for search_back, min_move in [(100, 0.05), (200, 0.03)]:
        for i in range(max(0, high_idx - search_back), high_idx):
            low_price = c15m[i]["low"]
            if low_price > 0 and (high_price - low_price) / low_price >= min_move:
                pump_start_idx = i
                break
        if pump_start_idx is not None:
            break

    if pump_start_idx is None:
        logger.debug("_find_pump_legs: no pump_start_idx — move < 3% in 200 candles before high",
                    high_price=round(high_price,6), high_idx=high_idx,
                    search_from=max(0, high_idx-200))
        return []

    pump_candles = c15m[pump_start_idx: high_idx + 1]
    if len(pump_candles) < 2:
        return []

    MIN_LEG_PCT      = 0.03
    MIN_REVERSAL_PCT = 0.02

    pivots = [(pump_candles[0]["low"], pump_start_idx, "low")]

    looking_for      = "high"
    running_high     = pump_candles[0]["high"]
    running_high_idx = pump_start_idx
    running_low      = pump_candles[0]["low"]
    running_low_idx  = pump_start_idx

    for i, c in enumerate(pump_candles[1:], 1):
        orig_idx = pump_start_idx + i

        if looking_for == "high":
            if c["high"] > running_high:
                running_high     = c["high"]
                running_high_idx = orig_idx
            if running_high > 0 and (running_high - c["low"]) / running_high >= MIN_REVERSAL_PCT:
                pivots.append((running_high, running_high_idx, "high"))
                running_low     = c["low"]
                running_low_idx = orig_idx
                looking_for     = "low"
        else:
            if c["low"] < running_low:
                running_low     = c["low"]
                running_low_idx = orig_idx
            if running_low > 0 and (c["high"] - running_low) / running_low >= MIN_REVERSAL_PCT:
                pivots.append((running_low, running_low_idx, "low"))
                running_high     = c["high"]
                running_high_idx = orig_idx
                looking_for      = "high"

    if looking_for == "high":
        pivots.append((running_high, running_high_idx, "high"))
    else:
        pivots.append((running_low, running_low_idx, "low"))

    legs = []
    for i in range(len(pivots) - 1):
        p1, p2 = pivots[i], pivots[i + 1]
        if p1[2] == "low" and p2[2] == "high":
            leg_low,  low_orig_idx  = p1[0], p1[1]
            leg_high, high_orig_idx = p2[0], p2[1]
            if leg_low > 0 and (leg_high - leg_low) / leg_low >= MIN_LEG_PCT:
                legs.append((leg_low, leg_high, low_orig_idx, high_orig_idx))

    seen_lows: dict[float, tuple] = {}
    for leg in legs:
        key = round(leg[0], 8)
        if key not in seen_lows or leg[1] > seen_lows[key][1]:
            seen_lows[key] = leg

    legs = [leg for leg in seen_lows.values() if leg[1] > 0 and (leg[1] - leg[0]) / leg[0] >= 0.05]

    legs.sort(key=lambda x: x[0])
    filtered: list[tuple] = []
    for leg in legs:
        if filtered and leg[0] > 0 and (leg[0] - filtered[-1][0]) / filtered[-1][0] < 0.04:
            continue
        filtered.append(leg)

    logger.debug("Pump legs found",
                count=len(filtered),
                legs=[(round(l, 6), round(h, 6)) for l, h, _, _ in filtered])

    return filtered


def _find_pump_base_simple(
    c15m: list[dict],
    pump_low: float,
    atr: float,
    atr_15m: float = 0,       # FIX Bug-3: 15M ATR for correct search radius
) -> list[tuple[float, int, dict]]:
    """Find pump base levels - where the pump started."""

    # FIX Bug-3: use 15M ATR for the search radius.
    # 1M ATR is 3-8x smaller than 15M ATR, causing most 15M pump_base
    # candles to be missed when their low differs from pump_low by > 1M_atr*0.3.
    search_radius = atr_15m * 0.3 if atr_15m > 0 else atr * 1.5

    levels = []

    candles_at_low = [c for c in c15m if abs(c["low"] - pump_low) <= search_radius]

    if candles_at_low:
        total_volume = sum(c["volume"] for c in candles_at_low)
        hourly_bonus = max(_timeframe_bonus(c["open_time"]) for c in candles_at_low)
        round_bonus  = _round_number_bonus(pump_low)
        levels.append((pump_low, len(candles_at_low), {
            "volume_at_level":    total_volume,
            "hourly_open_bonus":  hourly_bonus,
            "round_number_bonus": round_bonus,
        }))

    # Consolidation zone near pump_low (within 10%)
    consol_range   = pump_low * 0.10
    consol_candles = [
        c for c in c15m
        if pump_low <= min(c["open"], c["close"]) <= pump_low + consol_range
    ]

    if len(consol_candles) >= 3:
        median_price = statistics.median([c["close"] for c in consol_candles])

        if abs(median_price - pump_low) > search_radius * 1.5:  # not a duplicate
            candles_at_consol = [c for c in c15m if abs(c["close"] - median_price) <= search_radius]

            if candles_at_consol:
                total_volume = sum(c["volume"] for c in candles_at_consol)
                hourly_bonus = max(_timeframe_bonus(c["open_time"]) for c in candles_at_consol)
                round_bonus  = _round_number_bonus(median_price)
                levels.append((median_price, len(candles_at_consol), {
                    "volume_at_level":    total_volume,
                    "hourly_open_bonus":  hourly_bonus,
                    "round_number_bonus": round_bonus,
                }))

    return levels


def _find_1m_near_zone_levels(
    c1m: list[dict],
    current_price: float,
    near_zone_pct: float,
    atr: float,
) -> list[tuple[float, int, dict]]:
    """
    Find consolidation levels from 1M candles within near_zone_pct of current price.

    Purpose: pump legs on 15M are single large-body candles, hiding 1M pauses.
    Requires >= 2 unique candle touches within cluster radius.
    """
    if not c1m or current_price <= 0 or atr <= 0:
        return []

    zone_low  = current_price * (1 - near_zone_pct)
    zone_high = current_price * (1 + near_zone_pct)
    radius    = max(atr * 0.5, current_price * 0.001)

    # Collect body touch points — include candle if any part of body is in zone
    touch_points: list[tuple[float, dict]] = []
    for c in c1m[-1000:]:
        body_top = max(c["open"], c["close"])
        body_bot = min(c["open"], c["close"])
        if body_top < zone_low or body_bot > zone_high:
            continue
        if zone_low <= body_top <= zone_high:
            touch_points.append((body_top, c))
        if zone_low <= body_bot <= zone_high and body_bot != body_top:
            touch_points.append((body_bot, c))

    if not touch_points:
        return []

    levels: list[tuple[float, int, dict]] = []
    used:   set[int] = set()

    for i, (price_i, _) in enumerate(touch_points):
        if i in used:
            continue

        # Identify all members within radius — don't mark used yet
        members = [
            j for j, (price_j, _) in enumerate(touch_points)
            if j not in used and abs(price_j - price_i) <= radius
        ]

        # Count unique candles in this cluster
        unique_candles = {id(touch_points[k][1]) for k in members}
        if len(unique_candles) < 2:
            used.add(i)
            continue

        # Accept — mark all members as used
        for k in members:
            used.add(k)

        avg_price    = sum(touch_points[k][0] for k in members) / len(members)
        candle_count = len(unique_candles)
        avg_vol      = sum(touch_points[k][1]["volume"] for k in members) / len(members)

        levels.append((avg_price, candle_count, {
            "volume_at_level":    avg_vol,
            "hourly_open_bonus":  0,
            "round_number_bonus": _round_number_bonus(avg_price),
        }))

    return levels


def _find_mid_impulse_pauses(
    c15m: list[dict],
    pump_legs: list[tuple[float, float, int, int]],
    atr_15m: float,
) -> list[tuple[float, int, dict]]:
    """
    Find short pauses/consolidations INSIDE impulse legs that body/wick detection misses.

    These are zones where price briefly stalled mid-move (2-4 candles), visible on the
    chart as a local plateau but swallowed into one large 15M body by standard clustering.

    Logic per leg:
      1. Scan candles between leg_low_idx and leg_high_idx.
      2. Divide the leg into thirds. Skip bottom third (that's the base) and top 15%
         (that's near the high — noise). Work with the middle 55% of the leg range.
      3. Within that price band, find windows of 2-4 candles where:
           - The candle bodies overlap heavily (max_body_top - min_body_bot < atr_15m * 1.5)
           - At least 2 candles have closes within atr_15m * 0.5 of each other
      4. Mark the median body price of such a window as mid_impulse_pause.
      5. Deduplicate pauses closer than atr_15m * 0.4 — keep the one with most candles.
    """
    if not pump_legs or atr_15m <= 0:
        return []

    results: list[tuple[float, int, dict]] = []
    radius = atr_15m * 0.4

    for leg_low, leg_high, low_orig_idx, high_orig_idx in pump_legs:
        leg_range = leg_high - leg_low
        if leg_range <= 0:
            continue

        # Middle band: skip bottom 30% (base) and top 15% (near high)
        band_low  = leg_low  + leg_range * 0.30
        band_high = leg_high - leg_range * 0.15

        if band_high <= band_low:
            continue

        leg_candles = c15m[low_orig_idx: high_orig_idx + 1]
        if len(leg_candles) < 3:
            continue

        # Only candles whose body overlaps the middle band
        mid_candles = []
        for c in leg_candles:
            body_top = max(c["open"], c["close"])
            body_bot = min(c["open"], c["close"])
            if body_top >= band_low and body_bot <= band_high:
                mid_candles.append(c)

        if len(mid_candles) < 2:
            continue

        # Sliding window: look for tight groups
        window_sizes = [4, 3, 2]
        used_indices: set[int] = set()

        for win in window_sizes:
            for i in range(len(mid_candles) - win + 1):
                if any(k in used_indices for k in range(i, i + win)):
                    continue

                chunk = mid_candles[i: i + win]
                body_tops = [max(c["open"], c["close"]) for c in chunk]
                body_bots = [min(c["open"], c["close"]) for c in chunk]
                closes    = [c["close"] for c in chunk]

                # Tight if candle bodies span < 1.5 ATR
                spread = max(body_tops) - min(body_bots)
                if spread > atr_15m * 1.5:
                    continue

                # At least 2 closes within 0.5 ATR of each other
                tight_closes = 0
                for a in range(len(closes)):
                    for b in range(a + 1, len(closes)):
                        if abs(closes[a] - closes[b]) <= atr_15m * 0.5:
                            tight_closes += 1

                if tight_closes < 1:
                    continue

                # Pause price = median of body midpoints
                body_mids = [(t + b) / 2 for t, b in zip(body_tops, body_bots)]
                pause_price = statistics.median(body_mids)

                # Must be within middle band
                if not (band_low <= pause_price <= band_high):
                    continue

                total_vol    = sum(c["volume"] for c in chunk)
                hourly_bonus = max(_timeframe_bonus(c["open_time"]) for c in chunk)
                round_bonus  = _round_number_bonus(pause_price)

                results.append((pause_price, len(chunk), {
                    "volume_at_level":    total_vol,
                    "hourly_open_bonus":  hourly_bonus,
                    "round_number_bonus": round_bonus,
                }))

                for k in range(i, i + win):
                    used_indices.add(k)

    # Deduplicate across legs: keep entry with most candles
    results.sort(key=lambda x: x[0])
    deduped: list[tuple[float, int, dict]] = []
    for price, count, meta in results:
        if deduped and abs(price - deduped[-1][0]) <= radius:
            if count > deduped[-1][1]:
                deduped[-1] = (price, count, meta)
        else:
            deduped.append((price, count, meta))

    return deduped


def _find_body_levels_simple(
    c15m: list[dict],
    range_low: float,
    range_high: float,
    atr: float,
    cluster_radius: float = 0,
    pump_peak_time: int = 0,
) -> list[tuple[float, int, dict]]:
    """Find body levels - 15M candle bodies in the given price range."""
    upper_bound = range_high * 1.05
    avg_vol     = sum(c["volume"] for c in c15m) / len(c15m) if c15m else 1
    radius      = cluster_radius if cluster_radius > 0 else atr * 0.5

    boundaries = []
    for idx, c in enumerate(c15m):
        body_top = max(c["open"], c["close"])
        body_bot = min(c["open"], c["close"])
        if body_bot >= range_low and body_top <= upper_bound:
            tf_bonus   = _timeframe_bonus(c["open_time"])
            # Pre-pump candles get reduced weight to avoid inflating cluster scores
            is_post_pump = pump_peak_time == 0 or c["open_time"] >= pump_peak_time
            vol_weight = (5 if c["volume"] / avg_vol >= 2.0 else 3) if is_post_pump else 1
            boundaries.append((body_top, idx, c["volume"], tf_bonus, vol_weight))
            boundaries.append((body_bot, idx, c["volume"], tf_bonus, vol_weight))

    levels = []
    used   = set()

    for i, (price, candle_idx, volume, tf_bonus, vol_weight) in enumerate(boundaries):
        if i in used:
            continue

        cluster_prices      = [price]
        cluster_candle_idxs = {candle_idx}
        cluster_max_tf_bonus = tf_bonus
        cluster_weight       = vol_weight + tf_bonus

        for j, (other_price, other_idx, other_vol, other_tf, other_wt) in enumerate(boundaries):
            if j == i or j in used:
                continue
            if abs(other_price - price) <= radius:
                cluster_prices.append(other_price)
                cluster_candle_idxs.add(other_idx)
                cluster_max_tf_bonus = max(cluster_max_tf_bonus, other_tf)
                cluster_weight += other_wt + other_tf
                used.add(j)

        avg_price   = sum(cluster_prices) / len(cluster_prices)
        round_bonus = _round_number_bonus(avg_price)
        cluster_weight += round_bonus

        unique_candle_vols = [c15m[idx]["volume"] for idx in cluster_candle_idxs if idx < len(c15m)]
        avg_candle_volume  = sum(unique_candle_vols) / len(unique_candle_vols) if unique_candle_vols else 0

        touch_idxs = {
            idx for idx in cluster_candle_idxs
            if idx < len(c15m) and (
                abs(c15m[idx]["low"]  - avg_price) <= radius or
                abs(c15m[idx]["high"] - avg_price) <= radius
            )
        }

        # Post-pump filter: prefer post-pump touch count, but fall back to
        # total touches for origin/base zones where all candles are pre-pump.
        if pump_peak_time > 0:
            post_pump_touch_idxs = {
                idx for idx in touch_idxs
                if c15m[idx]["open_time"] >= pump_peak_time
            }
            candle_count = len(post_pump_touch_idxs) if post_pump_touch_idxs else len(touch_idxs)
        else:
            candle_count = len(touch_idxs)

        # FIX Bug-5: minimum weight 6, consistent with BODY_CLUSTER_MIN_WEIGHT
        # LEVEL-04: skip cluster if every candle is pre-pump (all pre-pump bodies
        # would pass weight=6 with enough candles, but they inflate strength without
        # post-pump confirmation — must have at least 1 post-pump touch).
        if pump_peak_time > 0:
            has_post_pump = any(
                c15m[idx]["open_time"] >= pump_peak_time
                for idx in cluster_candle_idxs if idx < len(c15m)
            )
            if not has_post_pump:
                used.add(i)
                continue

        if cluster_weight >= 6:
            levels.append((avg_price, candle_count, {
                "volume_at_level":    avg_candle_volume,
                "hourly_open_bonus":  cluster_max_tf_bonus,
                "round_number_bonus": round_bonus,
            }))

        used.add(i)

    return levels


def _find_wick_levels_simple(
    c15m: list[dict],
    pump_high: float,
    atr: float,
    cluster_radius: float = 0,
    pump_peak_time: int = 0,   # FIX Bug-2: receive from build_levels, not re-search
) -> list[tuple[float, int, dict]]:
    """Find wick levels - repeated lows after pump peak."""

    # FIX Bug-2: pump_peak_time is now passed directly from _find_pump_legs
    # so we never scan from the beginning of c15m and pick up an old pump peak.
    if pump_peak_time == 0:
        # Fallback: scan only recent 50 candles, not full history
        for c in c15m[-50:]:
            if c["high"] >= pump_high * 0.999:
                pump_peak_time = c["open_time"]
                break

    if not pump_peak_time:
        return []

    wick_lows = [
        (c["low"], c["volume"], c["open_time"])
        for c in c15m
        if c["open_time"] > pump_peak_time
    ]

    # FIX: use cluster_radius (15M-based) instead of atr*0.3 (1M-based)
    radius = cluster_radius if cluster_radius > 0 else atr * 0.3

    levels = []
    used   = set()

    for i, (price, volume, open_time) in enumerate(wick_lows):
        if i in used:
            continue

        cluster = [(price, volume, open_time)]

        for j, (other_price, other_volume, other_time) in enumerate(wick_lows):
            if j == i or j in used:
                continue
            if abs(other_price - price) <= radius:
                cluster.append((other_price, other_volume, other_time))
                used.add(j)

        if len(cluster) >= 2:
            avg_price    = sum(p for p, v, t in cluster) / len(cluster)
            total_volume = sum(v for p, v, t in cluster)
            tf_bonus     = max(_timeframe_bonus(t) for p, v, t in cluster)
            round_bonus  = _round_number_bonus(avg_price)

            levels.append((avg_price, len(cluster), {
                "volume_at_level":    total_volume,
                "hourly_open_bonus":  tf_bonus,
                "round_number_bonus": round_bonus,
            }))

        used.add(i)

    return levels


def _find_order_block_simple(
    c15m: list[dict],
    pump_low: float,
    pump_high: float,
    pump_start_idx: int = -1,  # FIX Bug-1: receive from _find_pump_legs
) -> tuple[float, dict] | None:
    """Find order block - last bearish candle before pump."""

    # FIX Bug-1: pump_start_idx now comes from _find_pump_legs (low_orig_idx),
    # so we no longer scan c15m from the beginning and pick up stale history.
    if pump_start_idx < 0:
        # Fallback (should not happen in normal flow)
        logger.warning("_find_order_block_simple called without pump_start_idx — using fallback")
        for i, c in enumerate(c15m):
            if c["low"] <= pump_low * 1.001:
                pump_start_idx = i
                break

    if pump_start_idx < 0 or pump_start_idx >= len(c15m):
        return None

    # Look for the last bearish candle in the 5 candles before pump start
    for i in range(pump_start_idx, max(0, pump_start_idx - 5), -1):
        c = c15m[i]
        if c["close"] < c["open"]:  # Bearish
            price        = min(c["open"], c["close"])
            tf_bonus     = _timeframe_bonus(c["open_time"])
            round_bonus  = _round_number_bonus(price)
            return (price, {
                "volume_at_level":    c["volume"],
                "hourly_open_bonus":  tf_bonus,
                "round_number_bonus": round_bonus,
            })

    return None


def _deduplicate_simple(levels: list[dict], radius: float) -> list[dict]:
    """Deduplicate nearby levels - pump_base wins over body_level, else keep more touches.

    LEVEL-05: pump_base and breakout_level are never replaced by a lower-priority
    type even if the lower-priority level has more candle_count.  Equal-priority
    ties are still broken by candle_count, but pump_base never loses to body_level.
    """
    if not levels:
        return []

    TYPE_PRIORITY = {
        "pump_base":         3,
        "breakout_level":    3,   # consolidation ceiling = launch point, same importance as pump_base
        "order_block":       2,
        "consolidation_base": 2,
        "body_level":        1,
        "mid_impulse_pause": 1,
        "consolidation":     1,
        "wick_level":        0,
    }

    sorted_levels = sorted(levels, key=lambda x: x["level"])
    result = [sorted_levels[0]]

    for lvl in sorted_levels[1:]:
        if abs(lvl["level"] - result[-1]["level"]) <= radius:
            prev     = result[-1]
            prev_pri = TYPE_PRIORITY.get(prev["type"], 0)
            curr_pri = TYPE_PRIORITY.get(lvl["type"], 0)
            if curr_pri > prev_pri:
                result[-1] = lvl
            elif curr_pri == prev_pri and lvl["candle_count"] > prev["candle_count"]:
                # LEVEL-05: keep the existing entry if it's pump_base/breakout_level
                # and the challenger is of equal priority but different type.
                KEEP_TYPES = {"pump_base", "breakout_level"}
                if prev["type"] not in KEEP_TYPES:
                    result[-1] = lvl
            # lower priority: keep existing (prev wins)
        else:
            result.append(lvl)

    return result


def _assign_positions(levels: list[dict], pump_low: float, pump_high: float) -> list[dict]:
    """Assign position labels (origin / impulse / mid_move)."""
    pump_range        = pump_high - pump_low
    origin_threshold  = pump_low + pump_range * 0.30
    impulse_threshold = pump_low + pump_range * 0.70

    for lvl in levels:
        if lvl["level"] <= origin_threshold:
            lvl["position"] = "origin"
        elif lvl["level"] <= impulse_threshold:
            lvl["position"] = "impulse"
        else:
            lvl["position"] = "mid_move"

    return levels


_CLUSTER_TYPE_PRIORITY = {
    "pump_base": 3, "breakout_level": 3, "order_block": 2,
    "consolidation_base": 2, "body_level": 1, "mid_impulse_pause": 1,
    "consolidation": 1, "wick_level": 0,
}

def _mark_clusters(levels: list[dict]) -> list[dict]:
    """Mark levels that are clustered together. Only the weaker one is penalised."""
    for i in range(len(levels) - 1):
        diff = abs(levels[i + 1]["level"] - levels[i]["level"])
        avg  = (levels[i + 1]["level"] + levels[i]["level"]) / 2

        if avg > 0 and diff / avg < 0.01:
            pri_i   = _CLUSTER_TYPE_PRIORITY.get(levels[i]["type"], 0)
            pri_j   = _CLUSTER_TYPE_PRIORITY.get(levels[i + 1]["type"], 0)
            if pri_i <= pri_j:
                levels[i]["cluster"] = True
            if pri_j <= pri_i:
                levels[i + 1]["cluster"] = True

    for lvl in levels:
        if "cluster" not in lvl:
            lvl["cluster"] = False

    return levels
