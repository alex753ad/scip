"""Generate ASCII chart for Claude analysis."""

def generate_ascii_chart(c15m: list[dict], levels: list[dict], poc_price: float = None, width: int = 60, height: int = 20, symbol: str = "") -> str:
    """
    Generate ASCII representation of 15M chart with levels and volume profile.
    
    Args:
        c15m: 15M candles
        levels: Detected levels
        poc_price: Point of Control price
        width: Chart width in characters
        height: Chart height in characters
    
    Returns:
        ASCII chart as string
    """
    if not c15m:
        return "No data"
    
    # Take last N candles
    candles_to_show = min(50, len(c15m))
    recent_candles = c15m[-candles_to_show:]
    
    # Find price range
    all_highs = [c["high"] for c in recent_candles]
    all_lows = [c["low"] for c in recent_candles]
    price_high = max(all_highs)
    price_low = min(all_lows)
    price_range = price_high - price_low
    
    if price_range == 0:
        return "Invalid price range"
    
    # Build chart
    lines = []
    
    # Header
    lines.append(f"{symbol or 'UNKNOWN'} 15M Chart (last {candles_to_show} candles)")
    lines.append(f"Price range: {price_low:.5f} - {price_high:.5f}")
    lines.append("")
    
    # Price axis and candles
    for row in range(height):
        # Calculate price for this row
        price_at_row = price_high - (row / height) * price_range
        
        # Price label
        line = f"{price_at_row:7.5f} |"
        
        # FIX BUG-3: строим массив width ячеек и маппим свечи по col-позиции
        # FIX: используем round+деление на (candles_to_show-1) чтобы последняя свеча
        # попадала в col=width-1, а не в width-2 (при старой формуле крайний столбец всегда пустой)
        row_chars = [" "] * width
        denom = max(candles_to_show - 1, 1)
        for i, candle in enumerate(recent_candles):
            col = int(round(i / denom * (width - 1)))
            if candle["low"] <= price_at_row <= candle["high"]:
                row_chars[col] = "█" if candle["close"] >= candle["open"] else "▓"
        line += "".join(row_chars)
        
        # Mark levels on the right
        level_marker = ""
        if price_range > 0:
            for lvl in levels:
                if abs(lvl["level"] - price_at_row) / price_range < 0.02:  # Within 2% of row
                    poc_mark = "🎯" if lvl.get("poc_aligned") else ""
                    level_marker = f" ← {lvl['level']:.5f} ({lvl['type']}) {poc_mark}"
                    break

            # Mark POC
            if poc_price and abs(poc_price - price_at_row) / price_range < 0.02:
                if not level_marker:
                    level_marker = f" ← POC: {poc_price:.5f} (MAX VOLUME)"
        
        line += level_marker
        lines.append(line)
    
    # Time axis
    lines.append(" " * 8 + "|" + "─" * width)
    
    # Volume Profile
    lines.append("")
    lines.append("Volume Profile:")
    
    # Calculate volume by price bins
    num_bins = 15
    bin_size = price_range / num_bins
    volume_bins = {}
    
    for candle in recent_candles:
        bin_idx = int((candle["close"] - price_low) / bin_size)
        bin_idx = max(0, min(num_bins - 1, bin_idx))
        volume_bins[bin_idx] = volume_bins.get(bin_idx, 0) + candle["volume"]
    
    max_volume = max(volume_bins.values()) if volume_bins else 1
    
    for row in range(height):
        price_at_row = price_high - (row / height) * price_range
        bin_idx = int((price_at_row - price_low) / bin_size)
        bin_idx = max(0, min(num_bins - 1, bin_idx))
        
        volume = volume_bins.get(bin_idx, 0)
        bar_length = int((volume / max_volume) * 20)
        
        bar = "▓" * bar_length
        
        # Mark POC
        poc_mark = ""
        if poc_price and abs(poc_price - price_at_row) / price_range < 0.02:
            poc_mark = " ← POC"
        
        lines.append(f"{price_at_row:7.5f} | {bar}{poc_mark}")
    
    return "\n".join(lines)


def generate_levels_summary(levels: list[dict], poc_price: float = None, avg_volume: float = 0) -> str:
    """
    Generate text summary of detected levels for Claude.
    
    Args:
        levels: Detected levels with metadata
        poc_price: Point of Control price
    
    Returns:
        Text summary
    """
    lines = []
    lines.append("Detected Levels:")
    lines.append("")
    
    for i, lvl in enumerate(levels, 1):
        price = lvl["level"]
        level_type = lvl["type"]
        candle_count = lvl.get("candle_count", 0)
        hourly_bonus = lvl.get("hourly_open_bonus", 0)
        round_bonus = lvl.get("round_number_bonus", 0)
        position = lvl.get("position", "unknown")
        poc_aligned = lvl.get("poc_aligned", False)
        volume = lvl.get("volume_at_level", 0)

        lines.append(f"{i}. Price: {price:.5f}")
        lines.append(f"   Type: {level_type}, Position: {position}")
        lines.append(f"   Touches (candles in history): {candle_count} — use 'Approaches after pump' for post-pump activity")
        
        # Show volume relative to average (not absolute numbers)
        if avg_volume > 0 and volume > 0:
            vol_ratio = volume / avg_volume
            if vol_ratio >= 3.0:
                vol_str = f"very high ({vol_ratio:.1f}x avg)"
            elif vol_ratio >= 1.5:
                vol_str = f"high ({vol_ratio:.1f}x avg)"
            elif vol_ratio >= 0.7:
                vol_str = f"normal ({vol_ratio:.1f}x avg)"
            else:
                vol_str = f"low ({vol_ratio:.1f}x avg)"
        else:
            vol_str = "unknown"
        lines.append(f"   Volume: {vol_str}")

        if hourly_bonus == 3:
            lines.append(f"   Timeframe: 4h open (RARE)")
        elif hourly_bonus == 2:
            lines.append(f"   Timeframe: 1h open")
        elif hourly_bonus == 1:
            lines.append(f"   Timeframe: 30min open")
        else:
            lines.append(f"   Timeframe: no alignment")

        if round_bonus == 2:
            lines.append(f"   Round number: very close")
        elif round_bonus == 1:
            lines.append(f"   Round number: close")
        else:
            lines.append(f"   Round number: not close")

        lines.append(f"   POC aligned: {'YES - MAXIMUM VOLUME' if poc_aligned else 'no'}")
        lines.append("")
    
    if poc_price:
        lines.append(f"POC (Point of Control): {poc_price:.5f}")
        lines.append("POC = price level with MAXIMUM volume = STRONGEST support")
    
    return "\n".join(lines)
