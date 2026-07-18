"""Debug script to analyze GTCUSDT level detection."""

import sys
sys.path.insert(0, 'c:\\scripts\\mark')

import asyncio
from binance.async_client import AsyncClient
from analysis.level_builder import build_levels
from logger import logger


async def fetch_gtc_data():
    """Fetch real GTCUSDT data from Binance."""
    client = await AsyncClient.create()
    
    try:
        # Fetch 15M candles
        klines_15m = await client.get_klines(
            symbol='GTCUSDT',
            interval='15m',
            limit=100
        )
        
        # Fetch 1M candles
        klines_1m = await client.get_klines(
            symbol='GTCUSDT',
            interval='1m',
            limit=300
        )
        
        # Convert to our format
        c15m = []
        for k in klines_15m:
            c15m.append({
                "open_time": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": int(k[6]),
            })
        
        c1m = []
        for k in klines_1m:
            c1m.append({
                "open_time": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": int(k[6]),
            })
        
        return c1m, c15m
        
    finally:
        await client.close_connection()


async def main():
    """Main debug function."""
    print("=" * 60)
    print("GTCUSDT Level Detection Debug")
    print("=" * 60)
    
    # Fetch real data
    print("\nFetching real GTCUSDT data from Binance...")
    c1m, c15m = await fetch_gtc_data()
    
    print(f"Fetched {len(c1m)} 1M candles and {len(c15m)} 15M candles")
    print(f"Current price: {c1m[-1]['close']:.5f}")
    print(f"Price range: {min(c['low'] for c in c15m):.5f} - {max(c['high'] for c in c15m):.5f}")
    
    # Calculate ATR
    atr = sum(c["high"] - c["low"] for c in c1m[-14:]) / 14
    print(f"ATR: {atr:.6f}")
    
    # Build levels
    print("\n" + "=" * 60)
    print("Built Levels")
    print("=" * 60)
    
    levels = build_levels("GTCUSDT", c1m_override=c1m, c15m_override=c15m)
    
    # Show POC if calculated
    poc_levels = [l for l in levels if l.get("poc_aligned")]
    if poc_levels:
        print(f"\n🎯 POC-aligned levels found: {len(poc_levels)}")
        for l in poc_levels:
            print(f"   {l['level']:.5f} - {l['type']}")
    else:
        print(f"\n⚠️  No POC-aligned levels found")
    
    print(f"\nTotal levels: {len(levels)}")
    print(f"Pump_base: {sum(1 for l in levels if l['type'] == 'pump_base')}")
    print(f"Body_level: {sum(1 for l in levels if l['type'] == 'body_level')}")
    print(f"Other: {sum(1 for l in levels if l['type'] not in ['pump_base', 'body_level'])}")
    
    print("\nLevel details:")
    for lvl in levels:
        poc_marker = "🎯" if lvl.get("poc_aligned") else ""
        hourly_marker = f"⏰{lvl.get('hourly_open_bonus', 0)}" if lvl.get('hourly_open_bonus', 0) > 0 else ""
        round_marker = f"🔢{lvl.get('round_number_bonus', 0)}" if lvl.get('round_number_bonus', 0) > 0 else ""
        print(f"  {lvl['level']:.5f} - {lvl['type']:20s} (candles: {lvl.get('candle_count', 0):2d}) {poc_marker} {hourly_marker} {round_marker}")
    
    # Check specific levels
    print("\n" + "=" * 60)
    print("Checking Specific Levels")
    print("=" * 60)
    
    level_13093 = next((l for l in levels if abs(l['level'] - 0.13093) / 0.13093 < 0.01), None)
    level_13672 = next((l for l in levels if abs(l['level'] - 0.13672) / 0.13672 < 0.01), None)
    
    if level_13093:
        print(f"\n✓ Level ~0.13093:")
        print(f"  Actual: {level_13093['level']:.5f}")
        print(f"  Type: {level_13093['type']}")
        print(f"  Candles: {level_13093.get('candle_count', 0)}")
        print(f"  Hourly bonus: {level_13093.get('hourly_open_bonus', 0)}")
        print(f"  Round bonus: {level_13093.get('round_number_bonus', 0)}")
    else:
        print(f"\n✗ Level ~0.13093 NOT FOUND")
    
    if level_13672:
        print(f"\n✓ Level ~0.13672:")
        print(f"  Actual: {level_13672['level']:.5f}")
        print(f"  Type: {level_13672['type']}")
        print(f"  Candles: {level_13672.get('candle_count', 0)}")
        print(f"  Hourly bonus: {level_13672.get('hourly_open_bonus', 0)}")
        print(f"  Round bonus: {level_13672.get('round_number_bonus', 0)}")
    else:
        print(f"\n✗ Level ~0.13672 NOT FOUND")


if __name__ == "__main__":
    asyncio.run(main())
