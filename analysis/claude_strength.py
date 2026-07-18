"""Claude-based strength calculation for levels."""

import asyncio
import json
import time
import anthropic
from config import CLAUDE_API_KEY, TELEGRAM_PROXY
from constants import CLAUDE_MODEL, CLAUDE_MAX_TOKENS, CLAUDE_MAX_CONCURRENT_REQUESTS
from analysis.chart_ascii import generate_ascii_chart, generate_levels_summary
from logger import logger

_client: anthropic.AsyncAnthropic | None = None
_semaphore: asyncio.Semaphore | None = None

# Cache for historical base rates from level_outcomes
# { level_type -> p_bounce_pct (float) }, refreshed every 6h
_base_rates_cache: dict = {}
_base_rates_ts: float = 0.0
_BASE_RATES_TTL = 6 * 3600


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        if TELEGRAM_PROXY:
            import httpx
            _client = anthropic.AsyncAnthropic(
                api_key=CLAUDE_API_KEY,
                http_client=httpx.AsyncClient(proxy=TELEGRAM_PROXY),
            )
        else:
            _client = anthropic.AsyncAnthropic(api_key=CLAUDE_API_KEY)
    return _client


def _get_semaphore() -> asyncio.Semaphore:
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(CLAUDE_MAX_CONCURRENT_REQUESTS)
    return _semaphore


def _load_base_rates() -> dict:
    """
    Load p_bounce base rates per level_type from history.db.
    p_bounce = 1 - breakout_rate (partials count as holding).
    Cached for _BASE_RATES_TTL seconds.
    """
    global _base_rates_cache, _base_rates_ts
    if _base_rates_cache and time.time() - _base_rates_ts < _BASE_RATES_TTL:
        return _base_rates_cache

    try:
        import sqlite3
        from data.history import DB_PATH  # same path the rest of the project uses
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT level_type, outcome FROM level_outcomes WHERE outcome IS NOT NULL"
        ).fetchall()
        conn.close()

        from collections import defaultdict
        counts: dict = defaultdict(lambda: {"n": 0, "breakout": 0})
        for level_type, outcome in rows:
            counts[level_type]["n"] += 1
            if outcome == "breakout":
                counts[level_type]["breakout"] += 1

        rates = {}
        for lt, d in counts.items():
            if d["n"] >= 20:  # skip types with too few samples
                rates[lt] = {
                    "p_bounce": round(100 * (d["n"] - d["breakout"]) / d["n"], 1),
                    "n": d["n"],
                }

        _base_rates_cache = rates
        _base_rates_ts = time.time()
        logger.debug("Base rates cache refreshed", types=list(rates.keys()))
    except Exception as e:
        logger.warning("Failed to load base rates from history.db", error=str(e))
        # Return stale cache if available, else empty
        if not _base_rates_cache:
            _base_rates_cache = {}

    return _base_rates_cache


def _load_symbol_profile(symbol: str) -> dict | None:
    """
    Load wick/body/base success rates for this symbol from symbol_profiles.
    Returns None if not enough data.
    """
    try:
        import sqlite3
        from data.history import DB_PATH
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT best_level_type, wick_success_rate, body_success_rate, "
            "base_success_rate, total_signals FROM symbol_profiles WHERE symbol=?",
            (symbol,),
        ).fetchone()
        conn.close()
        if row and row[4] and row[4] >= 10:
            return {
                "best_level_type": row[0],
                "wick_success_rate": row[1],
                "body_success_rate": row[2],
                "base_success_rate": row[3],
                "total_signals": row[4],
            }
    except Exception as e:
        logger.warning("Failed to load symbol profile", symbol=symbol, error=str(e))
    return None


def _build_base_rates_block(base_rates: dict) -> str:
    """Format base rates for the prompt."""
    if not base_rates:
        return "Нет данных (история пуста)."

    # Display order by descending p_bounce
    sorted_types = sorted(base_rates.items(), key=lambda x: -x[1]["p_bounce"])
    lines = []
    for lt, d in sorted_types:
        warn = " ⚠️ часто пробой" if d["p_bounce"] < 60 else ""
        lines.append(f"- {lt}: {d['p_bounce']}% отбоя (n={d['n']}){warn}")
    return "\n".join(lines)


def _build_symbol_stats_block(profile: dict | None, symbol: str) -> str:
    """Format symbol profile for the prompt."""
    if profile is None:
        return "Недостаточно истории по этой монете."

    lines = [f"По {symbol} ({profile['total_signals']} сигналов):"]
    if profile["wick_success_rate"] is not None:
        lines.append(f"- wick_level: успех {profile['wick_success_rate']*100:.0f}%")
    if profile["body_success_rate"] is not None:
        lines.append(f"- body_level: успех {profile['body_success_rate']*100:.0f}%")
    if profile["base_success_rate"] is not None:
        lines.append(f"- pump_base/consolidation: успех {profile['base_success_rate']*100:.0f}%")
    if profile["best_level_type"]:
        lines.append(f"- Лучший тип для монеты: {profile['best_level_type']}")
    return "\n".join(lines)


async def calculate_strength_with_claude(symbol: str, c15m: list[dict], levels: list[dict], poc_price: float = None) -> list[dict]:
    """
    Use Claude Haiku to analyze levels and assign:
      - strength (1-5): ordinal signal for display / auto-monitor selection
      - p_bounce_claude (0-100): Claude's independent probability of bounce,
        used as a second signal alongside ML p_bounce_at_entry.
    """
    if not levels:
        return levels

    # Generate ASCII chart
    chart = generate_ascii_chart(c15m, levels, poc_price, symbol=symbol)

    # Calculate average candle volume for relative comparison
    avg_volume = sum(c["volume"] for c in c15m[-50:]) / min(len(c15m), 50) if c15m else 0

    # Generate levels summary
    summary = generate_levels_summary(levels, poc_price, avg_volume=avg_volume)

    # Load historical context
    base_rates = _load_base_rates()
    symbol_profile = _load_symbol_profile(symbol)
    base_rates_block = _build_base_rates_block(base_rates)
    symbol_stats_block = _build_symbol_stats_block(symbol_profile, symbol)

    # Log what Claude actually sees per level
    logger.info("Claude input levels",
                symbol=symbol,
                levels=[{
                    "price": l["level"],
                    "candle_count": l.get("candle_count", 0),
                    "hourly_open_bonus": l.get("hourly_open_bonus", 0),
                    "round_number_bonus": l.get("round_number_bonus", 0),
                    "poc_aligned": l.get("poc_aligned", False),
                    "position": l.get("position", "?"),
                    "volume_at_level": round(l.get("volume_at_level", 0), 0),
                    "type": l.get("type", "?"),
                } for l in levels])

    prompt = f"""Ты трейдер-аналитик. Оцени каждый уровень поддержки {symbol}: вероятность отбоя и силу.

ЧТО ИМЕННО ОЦЕНИВАЕШЬ: вероятность того, что цена, дойдя до зоны уровня, УДЕРЖИТСЯ
и оттолкнётся вверх (отбой), а не пробьёт зону насквозь (пробой). Это оценка
самого уровня по графику — НЕ прогноз прибыли конкретной сделки. Управление
позицией (вход сеткой, стоп, тейк) — отдельный слой, ты его не видишь и не
учитываешь. Не рассуждай про стопы, размер позиции, точку входа — только про то,
удержит ли ценовая зона отскок.

{chart}

{summary}

Примечание: "Touches (candles in history)" = число 15m-свечей в зоне уровня. Это НЕ количество реальных касаний, а показатель «обжитости» зоны. Чем больше — тем более протестирована зона.

ИСТОРИЧЕСКАЯ P(отбой) ПО ТИПУ УРОВНЯ (базовый приор для оценки):
{base_rates_block}

СТАТИСТИКА ПО ЭТОЙ МОНЕТЕ:
{symbol_stats_block}

КАК ОЦЕНИВАТЬ p_bounce:
1. Возьми исторический приор для типа уровня как отправную точку.
2. Скорректируй вверх при: выравнивание с POC, объём выше среднего (>1.5x), открытие 4h, близко к круглому числу, позиция origin/impulse (первый тест после пампа).
3. Скорректируй вниз при: объём ниже среднего (<0.7x), нет выравнивания по ТФ, wick_level без объёмного подтверждения.
4. wick_level исторически пробивается примерно в половине случаев — давай высокую p_bounce только при явном подтверждении объёмом и POC.
5. POC (помечен "YES - MAXIMUM VOLUME") = уровень с максимальным объёмом = самая сильная поддержка.
6. Оценивай каждый уровень независимо. Не упоминай ML и не выдумывай данные которых нет выше.

ПРАВИЛО СИЛЫ (strength) из p_bounce:
- p_bounce ≥ 95 → strength = 5
- p_bounce 90–94 → strength = 4
- p_bounce 80–89 → strength = 3
- p_bounce 65–79 → strength = 2
- p_bounce < 65 → strength = 1

ПРАВИЛА REASON:
- Максимум 8–10 слов на русском
- Выбери 1–2 главных фактора, которые отличают этот уровень от остальных
- Не перечисляй всё подряд, не повторяй одно и то же у разных уровней

ПРИМЕРЫ ХОРОШИХ ПРИЧИН:
✅ "POC с максимальным объёмом"
✅ "Импульс, объём 19.9x, открытие 4h"
✅ "Wick-уровень, низкий объём — риск пробоя"
✅ "Пампбаза, много свечей в зоне (245)"

Верни ТОЛЬКО JSON (без markdown, без пояснений):
{{
  "levels": [
    {{"price": 0.005605, "p_bounce": 93, "strength": 4, "reason": "Импульс, объём 19.9x, открытие 4h"}},
    {{"price": 0.004606, "p_bounce": 76, "strength": 2, "reason": "Пампбаза, низкий объём"}}
  ]
}}"""

    try:
        logger.debug("Calling Claude Haiku for strength analysis",
                     symbol=symbol,
                     levels_count=len(levels))

        async with _get_semaphore():
            try:
                logger.debug("Requesting Claude Haiku", model=CLAUDE_MODEL)
                response = await _get_client().messages.create(
                    model=CLAUDE_MODEL,
                    max_tokens=CLAUDE_MAX_TOKENS,
                    messages=[{"role": "user", "content": prompt}],
                )
            except Exception as e:
                logger.error("Claude request failed", model=CLAUDE_MODEL, error=str(e))
                raise

        response_text = response.content[0].text.strip()

        # Strip markdown code fences if present
        if response_text.startswith("```"):
            lines = response_text.split("\n")
            json_lines = []
            in_code = False
            for line in lines:
                if line.startswith("```"):
                    in_code = not in_code
                    continue
                if in_code or not line.startswith("```"):
                    json_lines.append(line)
            response_text = "\n".join(json_lines)

        result = json.loads(response_text)

        # Build lookup by price
        claude_levels = {lvl["price"]: lvl for lvl in result.get("levels", [])}

        for lvl in levels:
            price = lvl["level"]

            # Find closest match within 0.5% tolerance
            claude_data = None
            best_dist = None
            for claude_price, data in claude_levels.items():
                if price > 0 and abs(claude_price - price) / price < 0.005:
                    dist = abs(claude_price - price)
                    if best_dist is None or dist < best_dist:
                        best_dist = dist
                        claude_data = data

            if claude_data:
                lvl["strength"] = claude_data["strength"]
                lvl["p_bounce_claude"] = claude_data.get("p_bounce")
                lvl["claude_reason"] = claude_data["reason"]
                lvl["verdict"] = "hold"

                logger.info("Claude strength assigned",
                            symbol=symbol,
                            level=price,
                            strength=claude_data["strength"],
                            p_bounce_claude=claude_data.get("p_bounce"),
                            reason=claude_data["reason"])
            else:
                logger.warning("Claude level not matched",
                               symbol=symbol,
                               level_price=price,
                               claude_prices=list(claude_levels.keys()))
                lvl["strength"] = 3
                lvl["p_bounce_claude"] = None
                lvl["claude_reason"] = "Не проанализирован Claude"
                lvl["verdict"] = "hold"

        return levels

    except Exception as e:
        logger.error("Failed to get Claude strength analysis",
                     symbol=symbol,
                     error=str(e))

        for lvl in levels:
            lvl["strength"] = 3
            lvl["p_bounce_claude"] = None
            lvl["claude_reason"] = f"Ошибка Claude: {str(e)}"
            lvl["verdict"] = "hold"

        return levels
