"""Market screener — separated from main.py to avoid circular imports."""

from constants import (
    SCREENER_MIN_VOLUME_USD,
    SCREENER_MIN_GROWTH_PCT,
    SCREENER_MIN_NATR,
    SCREENER_MIN_15M_VOLUME_USD,
)
from logger import logger


def _format_vol(v: float) -> str:
    if v >= 1_000_000_000:
        return f"{v/1_000_000_000:.1f}B"
    if v >= 1_000_000:
        return f"{v/1_000_000:.0f}M"
    return f"{v/1_000:.0f}K"


async def run_screener() -> list[tuple]:
    """Run market screener and return list of (ticker, chg, natr, vol, symbol).

    Filters:
    1. 24h quoteVolume > SCREENER_MIN_VOLUME_USD ($40M)
    2. 24h priceChangePercent > SCREENER_MIN_GROWTH_PCT (10%)
    3. NATR(5M, 14 bars) > SCREENER_MIN_NATR (2%)
    4. Last 15M candle quoteVolume > SCREENER_MIN_15M_VOLUME_USD ($1M)
    """
    from binance import AsyncClient

    client = await AsyncClient.create()
    rows = []
    try:
        tickers = await client.futures_ticker()

        candidates = []
        for t in tickers:
            sym = t["symbol"]
            if not sym.endswith("USDT"):
                continue
            vol = float(t["quoteVolume"])
            chg = float(t["priceChangePercent"])
            if vol > SCREENER_MIN_VOLUME_USD and chg > SCREENER_MIN_GROWTH_PCT:
                candidates.append((sym, chg, vol))

        for sym, chg, vol in candidates:
            try:
                # Fetch 5M klines for NATR
                klines_5m = await client.futures_klines(symbol=sym, interval="5m", limit=14)
                if len(klines_5m) < 2:
                    continue
                current_price = float(klines_5m[-1][4])
                if current_price == 0:
                    continue
                tr_list = [float(k[2]) - float(k[3]) for k in klines_5m]
                atr = sum(tr_list) / len(tr_list)
                natr = round(atr / current_price * 100, 1)
                if natr <= SCREENER_MIN_NATR:
                    continue

                # Fetch last 15M candle for volume filter
                klines_15m = await client.futures_klines(symbol=sym, interval="15m", limit=3)  # FIX BUG-12: limit=3 чтобы [-2] был доступен
                if not klines_15m:
                    continue
                # Use the last closed 15M candle (index -2 if current is open, else -1)
                # FIX BUG-12: [-1] — незакрытая свеча, объём занижен в 1–14 раз; брать [-2]
                last_15m_vol = float(klines_15m[-2][7])  # последняя закрытая свеча
                if last_15m_vol < SCREENER_MIN_15M_VOLUME_USD:
                    logger.debug(
                        "Screener: 15M vol too low",
                        symbol=sym,
                        vol_15m=last_15m_vol,
                        threshold=SCREENER_MIN_15M_VOLUME_USD,
                    )
                    continue

                ticker = sym.replace("USDT", "")
                rows.append((ticker, chg, natr, vol, sym))

            except Exception as e:
                logger.debug("Screener error", symbol=sym, error=str(e))
    finally:
        await client.close_connection()

    rows.sort(key=lambda x: x[1], reverse=True)
    return rows
