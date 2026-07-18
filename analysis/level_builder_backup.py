"""Level building with improved pump_base detection and Volume Profile support."""

from data.collector import candles_15m, candles_1m
from logger import logger
from constants import ATR_PERIOD
from typing import Optional
import statistics


def _timeframe_bonus(open_time_ms: int) -> int:
    """Возвращает бонус если свеча открывается в начале крупного периода."""
    ts = open_time_ms // 1000
    minute = (ts % 3600) // 60
    hour = (ts % 86400) // 3600

    if minute == 0 and hour % 4 == 0:
        return 3
    if minute == 0:
        return 2
    if minute == 30:
        return 1
    return 0


def _round_number_bonus(price: float) -> int:
    """Возвращает бонус к весу если цена близка к круглому числу."""
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


def _round_level(price: float) -> float:
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


def build_levels(symbol: str, c1m_override: list[dict] = None, c15m_override: list[dict] = None) -> list[dict]:
    """
    Build support/resistance levels with improved pump_base detection.
    
    Improvements:
    - Detects multiple pump zones (not just the last one)
    - Identifies consolidation_base levels
    - Prioritizes Volume Profile POC levels
    - Properly classifies historical pump bases
    """
    c1m = c1m_override if c1m_override is not None else candles_1m.get(symbol, [])
    c15m = c15m_override if c15m_override is not None else candles_15m.get(symbol, [])
    if len(c1m) < 20 or len(c15m) < 5:
        return []

    atr = _calc_atr_1m(c1m)
    if atr == 0:
        return []

    # Find multiple pump zones (up to 5)
    pump_zones = _find_multiple_pump_zones(c15m, max_zones=5)
    logger.debug("Pump zones found", symbol=symbol, count=len(pump_zones))
    
    if not pump_zones:
        return []

    # Get current price for filtering
    current_price = c1m[-1]["close"] if c1m else 0
    
    # Calculate GLOBAL Volume Profile for entire price range
    all_pump_lows = [pz[0] for pz in pump_zones]
    all_pump_highs = [pz[1] for pz in pump_zones]
    global_low = min(all_pump_lows) if all_pump_lows else 0
    global_high = max(all_pump_highs) if all_pump_highs else 0
    
    global_volume_profile = _calculate_volume_profile(c15m, global_low, global_high, atr)
    global_poc = _get_poc_from_profile(global_volume_profile) if global_volume_profile else None
    
    logger.info("Global POC calculated", 
                symbol=symbol, 
                poc=global_poc,
                range_low=global_low,
                range_high=global_high,
                pump_zones_count=len(pump_zones))
    
    all_levels = []
    
    # Add GLOBAL POC as pump_base if it has enough touches
    if global_poc:
        candles_at_global_poc = [
            c for c in c15m 
            if (abs(c["close"] - global_poc) <= atr * 0.3 or 
                abs(c["open"] - global_poc) <= atr * 0.3 or
                abs(c["low"] - global_poc) <= atr * 0.3 or
                abs(c["high"] - global_poc) <= atr * 0.3)
        ]
        
        if len(candles_at_global_poc) >= 3:
            total_volume_poc = sum(c["volume"] for c in candles_at_global_poc)
            hourly_open_bonus = max(_timeframe_bonus(c["open_time"]) for c in candles_at_global_poc)
            round_bonus = _round_number_bonus(global_poc)
            
            all_levels.append({
                "level": _round_level(global_poc),
                "type": "pump_base",
                "candle_count": len(candles_at_global_poc),
                "poc_aligned": True,
                "volume_at_level": total_volume_poc,
                "hourly_open_bonus": hourly_open_bonus,
                "round_number_bonus": round_bonus,
            })
            
            logger.info("Global POC added as pump_base",
                        symbol=symbol,
                        price=_round_level(global_poc),
                        candles=len(candles_at_global_poc),
                        volume=total_volume_poc)
    
    # Process each pump zone
    for pump_zone in pump_zones:
        pump_low, pump_high, pump_start_time, pump_end_time = pump_zone
        
        # Calculate Volume Profile for this pump zone
        pump_candles = [c for c in c15m if pump_start_time <= c["open_time"] <= pump_end_time]
        volume_profile = _calculate_volume_profile(pump_candles, pump_low, pump_high, atr)
        poc_price = _get_poc_from_profile(volume_profile) if volume_profile else None
        
        # Generate pump_base levels from pump_low
        pump_base_levels = _find_pump_base_levels(c15m, pump_low, pump_high, atr, poc_price)
        
        # Generate consolidation_base levels
        consolidation_base_levels = _find_consolidation_base_levels(c15m, pump_low, pump_high, atr, poc_price)
        
        # Generate body_levels (excluding pump_base candidates)
        body_levels = _find_body_levels(c1m, c15m, pump_low, pump_high, atr)
        
        # Generate wick_levels after pump peak
        wick_levels = _find_wick_levels(c1m, c15m, pump_high, atr, pump_end_time)
        
        # Generate order_blocks
        order_blocks = _find_order_blocks(c15m, pump_low, pump_high)
        
        # Combine all levels for this pump zone
        for price, candle_count, metadata in pump_base_levels:
            # CRITICAL: Final safety check - pump_base MUST have >= 3 candles
            if candle_count < 3:
                logger.warning("Filtered weak pump_base", 
                             price=price, 
                             candles=candle_count,
                             reason="less than 3 candles")
                continue
            
            all_levels.append({
                "level": _round_level(price),
                "type": "pump_base",
                "candle_count": candle_count,
                "poc_aligned": metadata.get("poc_aligned", False),
                "volume_at_level": metadata.get("volume_at_level", 0),
                "hourly_open_bonus": metadata.get("hourly_open_bonus", 0),
                "round_number_bonus": metadata.get("round_number_bonus", 0),
            })
        
        for price, candle_count, metadata in consolidation_base_levels:
            # CRITICAL: Check if this consolidation should be upgraded to pump_base
            # Upgrade if: near pump_low OR POC-aligned, AND has >= 3 candles
            is_near_pump_low = abs(price - pump_low) / pump_low < 0.20
            is_poc_aligned = metadata.get("poc_aligned", False)
            
            if (is_near_pump_low or is_poc_aligned) and candle_count >= 3:
                level_type = "pump_base"
                logger.debug("Upgraded consolidation to pump_base",
                           price=price,
                           reason="POC aligned" if is_poc_aligned else "near pump_low",
                           candles=candle_count)
            else:
                level_type = "consolidation_base"
            
            all_levels.append({
                "level": _round_level(price),
                "type": level_type,
                "candle_count": candle_count,
                "poc_aligned": metadata.get("poc_aligned", False),
                "volume_at_level": metadata.get("volume_at_level", 0),
                "hourly_open_bonus": metadata.get("hourly_open_bonus", 0),
                "round_number_bonus": metadata.get("round_number_bonus", 0),
            })
        
        for price, candle_count in body_levels:
            all_levels.append({
                "level": _round_level(price),
                "type": "body_level",
                "candle_count": candle_count,
                "poc_aligned": False,
                "volume_at_level": 0,
                "hourly_open_bonus": 0,
                "round_number_bonus": 0,
            })
        
        for price, candle_count in wick_levels:
            all_levels.append({
                "level": _round_level(price),
                "type": "wick_level",
                "candle_count": candle_count,
                "poc_aligned": False,
                "volume_at_level": 0,
                "hourly_open_bonus": 0,
                "round_number_bonus": 0,
            })
        
        for price, candle_count in order_blocks:
            all_levels.append({
                "level": _round_level(price),
                "type": "order_block",
                "candle_count": candle_count,
                "poc_aligned": False,
                "volume_at_level": 0,
                "hourly_open_bonus": 0,
                "round_number_bonus": 0,
            })

    # Deduplicate across all pump zones with priority
    levels = _deduplicate_with_priority(all_levels, atr * 0.5)
    
    # Filter levels within reasonable range (below current price for support)
    if current_price > 0:
        levels = [lvl for lvl in levels if lvl["level"] <= current_price * 1.05]
    
    # Assign positions based on all pump zones
    all_pump_lows = [pz[0] for pz in pump_zones]
    all_pump_highs = [pz[1] for pz in pump_zones]
    min_pump_low = min(all_pump_lows) if all_pump_lows else 0
    max_pump_high = max(all_pump_highs) if all_pump_highs else 0
    
    levels = _assign_positions(levels, min_pump_low, max_pump_high)
    levels = _mark_clusters(levels)
    
    logger.debug("Final levels built", 
                symbol=symbol, 
                count=len(levels),
                pump_base_count=sum(1 for l in levels if l["type"] == "pump_base"),
                consolidation_base_count=sum(1 for l in levels if l["type"] == "consolidation_base"))
    
    return levels


def _find_multiple_pump_zones(c15m: list[dict], max_zones: int = 5) -> list[tuple[float, float, int, int]]:
    """
    Find multiple pump zones (not just the last one).
    
    Returns:
        List of tuples: (pump_low, pump_high, start_time, end_time)
        Sorted by move magnitude in descending order.
    """
    pump_zones = []
    min_move = 0.05  # 5% minimum
    
    # Scan for all significant pumps
    for i in range(len(c15m) - 1, 0, -1):
        high = c15m[i]["high"]
        high_time = c15m[i]["open_time"]
        
        for j in range(i - 1, max(0, i - 10) - 1, -1):
            low = c15m[j]["low"]
            low_time = c15m[j]["open_time"]
            
            if low == 0:
                continue
                
            move = (high - low) / low
            
            if move > min_move:
                # Check if this zone overlaps with existing zones
                overlaps = False
                for existing_low, existing_high, _, _ in pump_zones:
                    if not (high < existing_low * 0.95 or low > existing_high * 1.05):
                        overlaps = True
                        break
                
                if not overlaps:
                    pump_zones.append((low, high, low_time, high_time))
    
    # Sort by move magnitude
    pump_zones.sort(key=lambda x: (x[1] - x[0]) / x[0], reverse=True)
    
    # Limit to max_zones
    return pump_zones[:max_zones]


def _calculate_volume_profile(candles: list[dict], price_low: float, price_high: float, atr: float) -> dict[float, float]:
    """
    Calculate Volume Profile for a price range.
    
    Returns:
        Dictionary mapping price bins to cumulative volume.
    """
    if not candles or price_low >= price_high:
        return {}
    
    # Create bins of size ATR * 0.2
    bin_size = atr * 0.2
    if bin_size == 0:
        return {}
    
    num_bins = int((price_high - price_low) / bin_size) + 1
    volume_profile = {}
    
    for candle in candles:
        candle_low = candle["low"]
        candle_high = candle["high"]
        candle_volume = candle["volume"]
        
        if candle_high == candle_low:
            # Point candle - assign all volume to one bin
            bin_price = round((candle_low - price_low) / bin_size) * bin_size + price_low
            volume_profile[bin_price] = volume_profile.get(bin_price, 0) + candle_volume
        else:
            # Distribute volume proportionally across bins
            candle_range = candle_high - candle_low
            
            for i in range(num_bins):
                bin_low = price_low + i * bin_size
                bin_high = bin_low + bin_size
                bin_mid = (bin_low + bin_high) / 2
                
                # Calculate overlap between candle and bin
                overlap_low = max(candle_low, bin_low)
                overlap_high = min(candle_high, bin_high)
                
                if overlap_high > overlap_low:
                    overlap_ratio = (overlap_high - overlap_low) / candle_range
                    bin_volume = candle_volume * overlap_ratio
                    volume_profile[bin_mid] = volume_profile.get(bin_mid, 0) + bin_volume
    
    return volume_profile


def _get_poc_from_profile(volume_profile: dict[float, float]) -> Optional[float]:
    """Get Point of Control (price with maximum volume) from Volume Profile."""
    if not volume_profile:
        return None
    
    return max(volume_profile.items(), key=lambda x: x[1])[0]


def _find_pump_base_levels(c15m: list[dict], pump_low: float, pump_high: float, atr: float, poc_price: Optional[float]) -> list[tuple[float, int, dict]]:
    """
    Find pump_base levels from pump_low and nearby consolidations.
    
    CRITICAL: Only levels with >= 3 candles qualify as pump_base.
    POC-aligned levels get priority even if they're not at pump_low.
    
    Returns:
        List of tuples: (price, candle_count, metadata)
        metadata contains: poc_aligned, volume_at_level, hourly_open, round_number_bonus
    """
    levels = []
    
    # CRITICAL: Check if POC itself should be a pump_base FIRST (highest priority)
    if poc_price:
        # Look for candles that touched POC (low, high, open, close)
        candles_at_poc = [
            c for c in c15m 
            if (abs(c["close"] - poc_price) <= atr * 0.3 or 
                abs(c["open"] - poc_price) <= atr * 0.3 or
                abs(c["low"] - poc_price) <= atr * 0.3 or
                abs(c["high"] - poc_price) <= atr * 0.3)
        ]
        
        if len(candles_at_poc) >= 3:  # MINIMUM 3 CANDLES
            total_volume_poc = sum(c["volume"] for c in candles_at_poc)
            hourly_open_bonus = max(_timeframe_bonus(c["open_time"]) for c in candles_at_poc)
            round_bonus = _round_number_bonus(poc_price)
            
            levels.append((poc_price, len(candles_at_poc), {
                "poc_aligned": True,  # By definition
                "volume_at_level": total_volume_poc,
                "hourly_open_bonus": hourly_open_bonus,
                "round_number_bonus": round_bonus,
            }))
            
            logger.debug("POC pump_base added", 
                        price=poc_price, 
                        candles=len(candles_at_poc),
                        volume=total_volume_poc)
    
    # Add pump_low as primary pump_base ONLY if it has enough candles
    candles_at_low = [c for c in c15m if abs(c["low"] - pump_low) <= atr * 0.3]
    
    if len(candles_at_low) >= 3:  # MINIMUM 3 CANDLES
        total_volume = sum(c["volume"] for c in candles_at_low)
        
        poc_aligned = False
        if poc_price and abs(pump_low - poc_price) <= atr * 0.3:
            poc_aligned = True
        
        # Check for hourly open alignment
        hourly_open_bonus = max(_timeframe_bonus(c["open_time"]) for c in candles_at_low)
        round_bonus = _round_number_bonus(pump_low)
        
        # Check if not duplicate of POC
        is_duplicate = any(abs(lvl[0] - pump_low) <= atr * 0.3 for lvl in levels)
        
        if not is_duplicate:
            levels.append((pump_low, len(candles_at_low), {
                "poc_aligned": poc_aligned,
                "volume_at_level": total_volume,
                "hourly_open_bonus": hourly_open_bonus,
                "round_number_bonus": round_bonus,
            }))
    
    # Check for consolidation near pump_low (within 10%)
    consolidation_range = pump_low * 0.10
    consolidation_candles = [
        c for c in c15m 
        if pump_low <= min(c["open"], c["close"]) <= pump_low + consolidation_range
    ]
    
    if len(consolidation_candles) >= 3:
        # Find median close as consolidation level
        median_close = statistics.median([c["close"] for c in consolidation_candles])
        
        if abs(median_close - pump_low) > atr * 0.5:  # Not duplicate of pump_low
            candles_at_consol = [c for c in c15m if abs(c["close"] - median_close) <= atr * 0.3]
            
            if len(candles_at_consol) >= 3:  # MINIMUM 3 CANDLES
                total_volume_consol = sum(c["volume"] for c in candles_at_consol)
                
                poc_aligned_consol = False
                if poc_price and abs(median_close - poc_price) <= atr * 0.3:
                    poc_aligned_consol = True
                
                hourly_open_bonus = max(_timeframe_bonus(c["open_time"]) for c in candles_at_consol)
                round_bonus = _round_number_bonus(median_close)
                
                # Check if not duplicate of existing levels
                is_duplicate = any(abs(lvl[0] - median_close) <= atr * 0.3 for lvl in levels)
                
                if not is_duplicate:
                    levels.append((median_close, len(candles_at_consol), {
                        "poc_aligned": poc_aligned_consol,
                        "volume_at_level": total_volume_consol,
                        "hourly_open_bonus": hourly_open_bonus,
                        "round_number_bonus": round_bonus,
                    }))
    
    return levels


def _find_consolidation_base_levels(c15m: list[dict], pump_low: float, pump_high: float, atr: float, poc_price: Optional[float] = None) -> list[tuple[float, int, dict]]:
    """
    Find consolidation_base levels (horizontal ranges before continuation).
    
    Args:
        poc_price: Optional POC price for alignment checking
    
    Returns:
        List of tuples: (price, candle_count, metadata)
    """
    levels = []
    upper_bound = pump_high * 1.05
    
    zone_candles = [
        c for c in c15m 
        if pump_low <= min(c["open"], c["close"]) and max(c["open"], c["close"]) <= upper_bound
    ]
    
    # Look for 3+ consecutive candles with tight range
    for i in range(len(zone_candles) - 2):
        window = zone_candles[i:i + 3]
        high_w = max(c["high"] for c in window)
        low_w = min(c["low"] for c in window)
        
        if high_w - low_w < atr * 4.0:
            median_close = statistics.median([c["close"] for c in window])
            total_volume = sum(c["volume"] for c in window)
            
            # Check for POC alignment
            poc_aligned = False
            if poc_price and abs(median_close - poc_price) <= atr * 0.3:
                poc_aligned = True
            
            # Check for hourly open and round number bonuses
            hourly_open_bonus = max(_timeframe_bonus(c["open_time"]) for c in window)
            round_bonus = _round_number_bonus(median_close)
            
            levels.append((median_close, len(window), {
                "poc_aligned": poc_aligned,
                "volume_at_level": total_volume,
                "hourly_open_bonus": hourly_open_bonus,
                "round_number_bonus": round_bonus,
            }))
    
    # Deduplicate consolidation levels
    if not levels:
        return []
    
    levels.sort(key=lambda x: x[0])
    deduped = [levels[0]]
    
    for price, count, metadata in levels[1:]:
        if abs(price - deduped[-1][0]) > atr * 0.5:
            deduped.append((price, count, metadata))
        else:
            # Merge with previous
            prev_price, prev_count, prev_metadata = deduped[-1]
            merged_price = (prev_price + price) / 2
            merged_count = prev_count + count
            merged_volume = prev_metadata["volume_at_level"] + metadata["volume_at_level"]
            merged_hourly = max(prev_metadata["hourly_open_bonus"], metadata["hourly_open_bonus"])
            merged_round = max(prev_metadata["round_number_bonus"], metadata["round_number_bonus"])
            
            deduped[-1] = (merged_price, merged_count, {
                "poc_aligned": prev_metadata["poc_aligned"] or metadata["poc_aligned"],
                "volume_at_level": merged_volume,
                "hourly_open_bonus": merged_hourly,
                "round_number_bonus": merged_round,
            })
    
    return deduped


def _find_body_levels(c1m, c15m, pump_low, pump_high, atr):
    weighted_boundaries = []
    upper_bound = pump_high * 1.05

    avg_vol_15m = sum(c["volume"] for c in c15m) / len(c15m) if c15m else 1

    for idx, c in enumerate(c15m):
        body_top = max(c["open"], c["close"])
        body_bot = min(c["open"], c["close"])
        if body_bot >= pump_low and body_top <= upper_bound:
            weight = 5 if c["volume"] / avg_vol_15m >= 2.0 else 3
            tf_bonus = _timeframe_bonus(c["open_time"])
            weight += tf_bonus
            weighted_boundaries.append((body_top, weight, idx))
            weighted_boundaries.append((body_bot, weight, idx))

    # кластеризация по радиусу atr * 0.5
    levels = []
    used = set()
    for i, (price, weight, candle_idx) in enumerate(weighted_boundaries):
        if i in used:
            continue
        cluster_prices = [price]
        cluster_weight = weight
        cluster_candles = {candle_idx}
        for j, (other_price, other_weight, other_idx) in enumerate(weighted_boundaries):
            if j == i or j in used:
                continue
            if abs(other_price - price) <= atr * 0.5:
                cluster_prices.append(other_price)
                cluster_weight += other_weight
                cluster_candles.add(other_idx)
                used.add(j)
        bonus = _round_number_bonus(sum(cluster_prices) / len(cluster_prices))
        cluster_weight += bonus
        if cluster_weight >= 6 and len(cluster_candles) >= 2:
            levels.append((sum(cluster_prices) / len(cluster_prices), len(cluster_candles)))
        used.add(i)

    return levels


def _find_wick_levels(c1m, c15m, pump_high, atr, pump_end_time: int):
    """
    Find wick levels after pump peak.
    
    Args:
        pump_end_time: Timestamp of pump peak (from pump zone)
    """
    wick_lows = []
    for c in c15m:
        if c["open_time"] <= pump_end_time:
            continue
        wick_lows.append(c["low"])

    # Cluster wicks - minimum 4 wicks nearby
    levels = []
    used = set()
    for i, price in enumerate(wick_lows):
        if i in used:
            continue
        cluster = [price]
        for j, other in enumerate(wick_lows):
            if j == i or j in used:
                continue
            if abs(other - price) <= atr * 0.3:
                cluster.append(other)
                used.add(j)
        if len(cluster) >= 4:
            levels.append((sum(cluster) / len(cluster), len(cluster)))
        used.add(i)

    return levels





def _find_order_blocks(c15m: list[dict], pump_low: float, pump_high: float) -> list[tuple[float, int]]:
    """
    Найти Order Block — последнюю красную 15М свечу перед началом пампа.
    Возвращает список (уровень, candle_count=1).
    """
    pump_high_idx = None
    for i, c in enumerate(c15m):
        if c["high"] >= pump_high * 0.999:
            pump_high_idx = i
            break

    if pump_high_idx is None or pump_high_idx < 2:
        return []

    pump_low_idx = None
    for i in range(pump_high_idx - 1, max(0, pump_high_idx - 10) - 1, -1):
        if c15m[i]["low"] <= pump_low * 1.001:
            pump_low_idx = i
            break

    if pump_low_idx is None:
        return []

    for i in range(pump_low_idx, max(0, pump_low_idx - 5) - 1, -1):
        c = c15m[i]
        if c["close"] < c["open"]:
            level = min(c["open"], c["close"])
            return [(level, 1)]

    return []


def _deduplicate_with_priority(levels: list[dict], radius: float) -> list[dict]:
    """
    Deduplicate levels with priority.
    
    Priority order:
    1. POC-aligned pump_base (highest)
    2. pump_base with hourly_open_bonus
    3. pump_base with round_number_bonus
    4. pump_base (general)
    5. consolidation_base
    6. body_level
    7. order_block
    8. wick_level (lowest)
    """
    if not levels:
        return []
    
    def calculate_priority_score(lvl: dict) -> float:
        """Calculate priority score for level."""
        type_priority = {
            "pump_base": 100,
            "consolidation_base": 80,
            "body_level": 60,
            "order_block": 40,
            "wick_level": 20,
        }
        
        score = type_priority.get(lvl["type"], 0)
        
        # POC alignment is CRITICAL
        if lvl.get("poc_aligned"):
            score += 50
        
        # Hourly open bonus
        score += lvl.get("hourly_open_bonus", 0) * 5
        
        # Round number bonus
        score += lvl.get("round_number_bonus", 0) * 3
        
        # Volume bonus
        score += min(lvl.get("volume_at_level", 0) / 1000000, 10)  # Cap at +10
        
        # Candle count bonus
        score += min(lvl.get("candle_count", 0), 10)  # Cap at +10
        
        return score
    
    sorted_levels = sorted(levels, key=lambda x: x["level"])
    result = [sorted_levels[0]]
    
    for lvl in sorted_levels[1:]:
        if abs(lvl["level"] - result[-1]["level"]) <= radius:
            # Merge logic - keep level with higher priority score
            current_score = calculate_priority_score(lvl)
            prev_score = calculate_priority_score(result[-1])
            
            if current_score > prev_score:
                result[-1] = lvl
        else:
            result.append(lvl)
    
    return result


def _calc_atr_1m(c1m: list[dict]) -> float:
    if len(c1m) < 14:
        return 0.0
    recent = c1m[-14:]
    return sum(c["high"] - c["low"] for c in recent) / 14


def _assign_positions(levels: list[dict], pump_low: float, pump_high: float) -> list[dict]:
    origin_threshold = pump_low * 1.20
    for lvl in levels:
        if lvl["level"] <= origin_threshold:
            lvl["position"] = "origin"
        else:
            lvl["position"] = "mid_move"
    return levels


def _mark_clusters(levels: list[dict]) -> list[dict]:
    for i in range(len(levels) - 1):
        diff = abs(levels[i + 1]["level"] - levels[i]["level"])
        avg = (levels[i + 1]["level"] + levels[i]["level"]) / 2
        if avg > 0 and diff / avg < 0.01:
            levels[i]["cluster"] = True
            levels[i + 1]["cluster"] = True

    for lvl in levels:
        if "cluster" not in lvl:
            lvl["cluster"] = False

    return levels
