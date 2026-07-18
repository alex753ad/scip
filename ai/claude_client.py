"""Claude AI client for level analysis (outcome statistics interpreter)."""

import anthropic
import json
import re
from logger import logger
from config import CLAUDE_API_KEY, TELEGRAM_PROXY
from constants import CLAUDE_MODEL, CLAUDE_MAX_TOKENS

# Create client with proxy support if configured
# FIX BUG-7: socks5:// не поддерживается httpx напрямую — добавлена проверка и fallback
if TELEGRAM_PROXY:
    import httpx
    _proxy_url = TELEGRAM_PROXY
    if _proxy_url.startswith("socks5://"):
        try:
            from httpx_socks import AsyncProxyTransport  # pip install httpx-socks
            _transport = AsyncProxyTransport.from_url(_proxy_url)
            _http_client = httpx.AsyncClient(transport=_transport)
        except ImportError:
            logger.warning("httpx-socks not installed, SOCKS5 proxy not supported for Claude — connecting without proxy")
            _http_client = httpx.AsyncClient()
    else:
        _http_client = httpx.AsyncClient(proxy=_proxy_url)
    client = anthropic.AsyncAnthropic(
        api_key=CLAUDE_API_KEY,
        http_client=_http_client,
    )
else:
    client = anthropic.AsyncAnthropic(api_key=CLAUDE_API_KEY)

SYSTEM_PROMPT = [
    {
        "type": "text",
        "text": (
            "Ты — интерпретатор торговой статистики для крипто-бота.\n"
            "Токен In-Play: высокая волатильность, рост 30-200% в сутки.\n"
            "Стратегия: лимитки по Мартингейлу, цель отскок 1-3% от уровня 15М.\n"
            "Отвечай строго JSON. Без markdown. Без пояснений вне JSON."
        ),
        "cache_control": {"type": "ephemeral"},
    }
]


def _parse_json_response(text: str) -> list[dict] | None:
    """Parse JSON response from Claude with fallback regex."""
    text = text.strip()
    match = re.search(r'\[.*\]', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON response", error=str(e), text=text[:200])
            return None
    logger.warning("No JSON array found in response", text=text[:200])
    return None


def _build_user_prompt(
    levels: list[dict],
    outcome_probs: dict | None,
    approach_style: str | None,
    atr_ratio: float | None,
    vol_ratio: float | None,
) -> str:
    """Build user prompt with levels and statistics."""
    input_data = [
        {
            "level": l["level"],
            "strength": l["strength"],
            "verdict": l["verdict"],
            "type": l["type"],
            "position": l.get("position", "mid_move"),
            "approach": l.get("approach", 0),
            "vol_ratio": l.get("vol_ratio", 1.0),
            "pump_volume_ratio": l.get("pump_volume_ratio", 1.5),
            "was_broken": l.get("was_broken", False),
            "sweep_reclaimed": l.get("sweep_reclaimed", False),
            "zone_approaches": l.get("zone_approaches", 0),
        }
        for l in levels
    ]

    logger.info("Claude input_data",
                levels_count=len(input_data),
                data=json.dumps(input_data, ensure_ascii=False))

    parts = [f"Уровни для анализа:\n{json.dumps(input_data, ensure_ascii=False)}"]

    if outcome_probs and outcome_probs.get("sample_size", 0) > 0:
        ss = outcome_probs["sample_size"]
        parts.append(
            f"\nСтатистика исходов по этому токену:\n"
            f"- Не дошла до уровня: {outcome_probs['no_reach'] * 100:.1f}% (из {ss} наблюдений)\n"
            f"- Коснулась но мало забрала: {outcome_probs['partial'] * 100:.1f}%\n"
            f"- Нормальный отскок: {outcome_probs['bounce'] * 100:.1f}%\n"
            f"- Пробой: {outcome_probs['breakout'] * 100:.1f}%\n"
            f"- Средняя глубина входа при отскоке: {outcome_probs['avg_fill_depth']:.2f}%\n"
            f"- Стиль подхода сейчас: {approach_style or 'unknown'}\n"
            f"- Расстояние до уровня в ATR: {atr_ratio or 0}\n"
            f"- Текущий объём к MA20: {vol_ratio or 1.0}x\n"
            f"\nЕсли sample_size < 10 — не опирайся на статистику, укажи это."
        )
    else:
        parts.append(
            "\nСтатистики по токену пока нет. "
            "Опирайся только на тип уровня и контекст подхода."
        )
        if approach_style:
            parts.append(f"Стиль подхода сейчас: {approach_style}")
        if atr_ratio is not None:
            parts.append(f"Расстояние до уровня в ATR: {atr_ratio}")
        if vol_ratio is not None:
            parts.append(f"Текущий объём к MA20: {vol_ratio}x")

    parts.append(
        '\nВерни JSON массив:\n'
        '[{"level": float, "reason": "2-3 предложения на русском", '
        '"grid_advice": "narrow/normal/wide", "confidence": 0.0-1.0}]\n\n'
        'grid_advice: narrow = сузить сетку (<7%), normal = стандарт 7-10%, wide = расширить (>10%)'
    )

    return "\n".join(parts)


async def analyze_levels(
    levels: list[dict],
    outcome_probs: dict = None,
    approach_style: str = None,
    atr_ratio: float = None,
    vol_ratio: float = None,
) -> list[dict]:
    """
    Analyze levels: Claude interprets statistics and returns reason + grid_advice + confidence.
    
    Args:
        levels: List of levels with pre-calculated strength and verdict
        outcome_probs: Result of get_outcome_probs() (optional)
        approach_style: flash / bleed / impulse / unknown (optional)
        atr_ratio: Distance to level in ATR units (optional)
        vol_ratio: Current volume / MA20 (optional)
        
    Returns:
        Same levels with added 'reason', 'grid_advice', 'confidence' fields
    """
    if not levels:
        return []

    try:
        results = await _get_reason(levels, outcome_probs, approach_style, atr_ratio, vol_ratio)
        result_map = {r["level"]: r for r in results}
        
        for lvl in levels:
            matched = result_map.get(lvl["level"], {})
            lvl["reason"] = matched.get("reason", "")
            lvl["grid_advice"] = matched.get("grid_advice", "normal")
            lvl["confidence"] = matched.get("confidence", 0.0)
            
        logger.info("Levels analyzed", count=len(levels), with_reason=len(results))
        return levels
        
    except Exception as e:
        logger.exception("Failed to analyze levels", error=str(e))
        for lvl in levels:
            lvl.setdefault("reason", "")
            lvl.setdefault("grid_advice", "normal")
            lvl.setdefault("confidence", 0.0)
        return levels


async def _get_reason(
    levels: list[dict],
    outcome_probs: dict = None,
    approach_style: str = None,
    atr_ratio: float = None,
    vol_ratio: float = None,
) -> list[dict]:
    """Get reasoning from Claude for each level."""
    user_prompt = _build_user_prompt(levels, outcome_probs, approach_style, atr_ratio, vol_ratio)

    try:
        logger.debug("Requesting Claude analysis", levels_count=len(levels))
        
        response = await client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=CLAUDE_MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        
        text = response.content[0].text
        results = _parse_json_response(text)
        
        if results is None:
            logger.warning("Claude returned invalid JSON")
            return []
            
        logger.debug("Claude analysis completed", results_count=len(results))
        return results
        
    except anthropic.APIError as e:
        logger.error("Claude API error", error=str(e), status_code=getattr(e, 'status_code', None))
        return []
    except Exception as e:
        logger.exception("Unexpected error in Claude request", error=str(e))
        return []
