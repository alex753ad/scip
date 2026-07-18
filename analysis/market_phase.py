"""Market Phase Detection — определение фазы рынка по свечным данным.

Все функции синхронные. Не пишет в БД, не открывает сделок.
Используется из Strategy2SignalFilter.check().
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from data.collector import candles_1m, candles_5m
from constants import (
    PHASE_FLAT_RANGE_MAX_PCT,
    PHASE_FLAT_DIR_EFF_MAX,
    PHASE_PUMP_PRICE_CHANGE_MIN_PCT,
    PHASE_PUMP_ATR_RATIO_MIN,
    PHASE_BLEED_BOUNCE_QUALITY_MAX,
    PHASE_TRADEABLE_BOUNCE_QUALITY_MIN,
    PHASE_RANGE_WINDOW_1M,
    PHASE_PUMP_WINDOW_1M,
    PHASE_BOUNCE_WINDOW_5M,
    PHASE_STRUCT_WINDOW_5M,
)
from logger import logger


class MarketPhase(str, Enum):
    PUMP            = "pump"             # активный памп — торговать стандартно
    FLAT            = "flat"             # консолидация — только от нижней границы
    BLEED           = "bleed"            # медленный слив без коррекций — НЕ торговать
    DROP_TRADEABLE  = "drop_tradeable"   # импульсный откат с коррекциями — торговать
    UNKNOWN         = "unknown"          # недостаточно данных


@dataclass
class PhaseResult:
    phase: MarketPhase
    range_low: float    # нижняя граница ЛОКАЛЬНОГО окна (90м), для детекта фазы
    range_high: float   # верхняя граница ЛОКАЛЬНОГО окна (90м)
    direction_efficiency: float  # 0.0–1.0, насколько направленно движение
    bounce_quality: float        # 0.0–1.0, качество последних отскоков (0 если не считали)
    swing_low: float    # минимум последнего импульса вниз (0.0 если не найден)
    note: str           # короткое пояснение для логов, например "flat:range=2.8%"
    struct_low: float = 0.0   # [P7] структурный 24ч-диапазон для позиционного гейта (0.0 если нет данных)
    struct_high: float = 0.0  # [P7] узкое окно (90м) для гейта занижает позицию у вершины


def compute_range_bounds(symbol: str, window: int = None) -> tuple[float, float]:
    """Вернуть (range_low, range_high) по последним window 1m-свечам."""
    if window is None:
        window = PHASE_RANGE_WINDOW_1M
    c1m = candles_1m.get(symbol, [])[-window:]
    if len(c1m) < 10:
        return (0.0, 0.0)
    range_low  = min(c["low"]  for c in c1m)
    range_high = max(c["high"] for c in c1m)
    return (range_low, range_high)


def compute_struct_bounds_24h(symbol: str) -> tuple[float, float]:
    """[P7] Структурный 24ч-диапазон по 5m-свечам (288×5м=24ч).

    candles_1m держит только до 300 свечей (~5ч), поэтому 24ч берём из 5m-буфера.
    Используется ТОЛЬКО для позиционного гейта в фильтре — узкое 90м-окно (range_low/
    range_high) для детекта фазы оставлено без изменений. Возвращает (0.0, 0.0) при
    нехватке данных — тогда фильтр откатывается на локальное окно.
    """
    c5m = candles_5m.get(symbol, [])[-PHASE_STRUCT_WINDOW_5M:]
    if len(c5m) < 12:   # < 1ч данных — структурный диапазон недостоверен
        return (0.0, 0.0)
    return (min(c["low"] for c in c5m), max(c["high"] for c in c5m))


def find_impulse_swing_low(symbol: str, window: int = 30) -> float:
    """Найти минимальный low последнего импульса вниз в последних window 1m-свечах."""
    c1m = candles_1m.get(symbol, [])[-window:]
    if len(c1m) < 5:
        return 0.0
    min_low = min(c["low"] for c in c1m)
    min_idx = next(i for i, c in enumerate(c1m) if c["low"] == min_low)
    if min_idx == 0:
        return min_low
    # Проверить: до min_idx есть хотя бы 2 красные свечи подряд
    preceding = c1m[:min_idx]
    for i in range(len(preceding) - 1):
        if preceding[i]["close"] < preceding[i]["open"] and preceding[i + 1]["close"] < preceding[i + 1]["open"]:
            return min_low
    return 0.0


def _compute_direction_efficiency(closes: list[float]) -> float:
    """Насколько направленно движение: 1.0 = прямая линия, 0.0 = хаос."""
    if len(closes) < 2:
        return 0.0
    net_change  = abs(closes[-1] - closes[0])
    total_path  = sum(abs(closes[i] - closes[i - 1]) for i in range(1, len(closes)))
    if total_path == 0:
        return 0.0
    return min(1.0, net_change / total_path)


def _compute_bounce_quality(symbol: str, window: int = None) -> float:
    """Качество последних отскоков по 5m-свечам: 1.0 = полный откат, 0.0 = нет отскока."""
    if window is None:
        window = PHASE_BOUNCE_WINDOW_5M
    c5m = candles_5m.get(symbol, [])[-window:]
    if len(c5m) < 6:
        return 0.5  # нейтральное значение при нехватке данных

    # Найти чередующиеся свинг-лоу и свинг-хай
    swing_lows  = []
    swing_highs = []
    for i in range(1, len(c5m) - 1):
        if c5m[i]["low"]  < c5m[i - 1]["low"]  and c5m[i]["low"]  < c5m[i + 1]["low"]:
            swing_lows.append((i, c5m[i]["low"]))
        if c5m[i]["high"] > c5m[i - 1]["high"] and c5m[i]["high"] > c5m[i + 1]["high"]:
            swing_highs.append((i, c5m[i]["high"]))

    # Собрать пары: (swing_low_price, next_swing_high_price, prev_swing_high_price)
    pairs = []
    for sl_idx, sl_price in swing_lows:
        # Предыдущий свинг-хай (до swing_low)
        prev_highs = [(i, p) for i, p in swing_highs if i < sl_idx]
        # Следующий свинг-хай (после swing_low)
        next_highs = [(i, p) for i, p in swing_highs if i > sl_idx]
        if not prev_highs or not next_highs:
            continue
        prev_sh_price = prev_highs[-1][1]
        next_sh_price = next_highs[0][1]
        pairs.append((sl_price, next_sh_price, prev_sh_price))

    if not pairs:
        return 0.5

    qualities = []
    for sl_price, sh_after, sh_before in pairs[-2:]:
        bounce_magnitude = sh_after  - sl_price
        drop_magnitude   = sh_before - sl_price
        if drop_magnitude <= 0:
            continue
        qualities.append(min(1.0, bounce_magnitude / drop_magnitude))

    if not qualities:
        return 0.5
    return sum(qualities) / len(qualities)


def detect_market_phase(
    symbol: str,
    event_atr: float,
    current_price: float,
) -> PhaseResult:
    """Определить фазу рынка по последним свечным данным.

    Returns:
        PhaseResult с фазой и вспомогательными метриками.
    """
    # ── Шаг 0: Получить данные ───────────────────────────────────────────────
    window_1m   = PHASE_RANGE_WINDOW_1M
    pump_window = PHASE_PUMP_WINDOW_1M
    c1m = candles_1m.get(symbol, [])

    if len(c1m) < 20:
        return PhaseResult(
            phase=MarketPhase.UNKNOWN,
            range_low=0.0, range_high=0.0,
            direction_efficiency=0.0,
            bounce_quality=0.0,
            swing_low=0.0,
            note="not_enough_data",
        )

    # ── Шаг 1: Определить диапазон ───────────────────────────────────────────
    recent_1m  = c1m[-window_1m:]
    range_high = max(c["high"] for c in recent_1m)
    range_low  = min(c["low"]  for c in recent_1m)
    mid_price  = (range_high + range_low) / 2
    range_pct  = (range_high - range_low) / mid_price * 100

    # [P7] Структурный 24ч-диапазон (5m) для позиционного гейта — отдельно от
    # узкого 90м-окна детекта фазы. Считаем один раз, кладём во все PhaseResult.
    struct_low, struct_high = compute_struct_bounds_24h(symbol)

    # ── Шаг 2: Direction efficiency по последним 60 1m-свечам ────────────────
    closes_60  = [c["close"] for c in c1m[-60:]]
    dir_eff    = _compute_direction_efficiency(closes_60)
    slope_down = closes_60[-1] < closes_60[0]

    # ── Шаг 3: Pump detection ────────────────────────────────────────────────
    pump_candles = c1m[-pump_window:]
    if len(pump_candles) >= 10:
        price_start      = pump_candles[0]["open"]
        price_end        = pump_candles[-1]["close"]
        if price_start <= 0:
            price_change_pct = 0.0
            atr_ratio        = 1.0
        else:
            price_change_pct = (price_end - price_start) / price_start * 100

            baseline_candles = (
                c1m[-window_1m:-pump_window]
                if len(c1m) >= window_1m
                else c1m[:-pump_window]
            )
            if len(baseline_candles) >= 5:
                avg_candle_range = sum(c["high"] - c["low"] for c in baseline_candles) / len(baseline_candles)
                atr_ratio = event_atr / avg_candle_range if avg_candle_range > 0 else 1.0
            else:
                atr_ratio        = 1.0
                price_change_pct = 0.0
    else:
        price_change_pct = 0.0
        atr_ratio        = 1.0

    if price_change_pct >= PHASE_PUMP_PRICE_CHANGE_MIN_PCT and atr_ratio >= PHASE_PUMP_ATR_RATIO_MIN:
        return PhaseResult(
            phase=MarketPhase.PUMP,
            range_low=range_low, range_high=range_high,
            direction_efficiency=dir_eff,
            bounce_quality=0.0,
            swing_low=0.0,
            struct_low=struct_low, struct_high=struct_high,
            note=f"pump:change={price_change_pct:.1f}%,atr_ratio={atr_ratio:.2f}",
        )

    # ── Шаг 4: Flat detection ─────────────────────────────────────────────────
    if range_pct < PHASE_FLAT_RANGE_MAX_PCT and dir_eff < PHASE_FLAT_DIR_EFF_MAX:
        return PhaseResult(
            phase=MarketPhase.FLAT,
            range_low=range_low, range_high=range_high,
            direction_efficiency=dir_eff,
            bounce_quality=0.0,
            swing_low=0.0,
            struct_low=struct_low, struct_high=struct_high,
            note=f"flat:range={range_pct:.1f}%,dir_eff={dir_eff:.2f}",
        )

    # ── Шаг 5: Bleed vs Tradeable drop (только если slope_down == True) ──────
    if slope_down:
        bq        = _compute_bounce_quality(symbol)
        swing_low = find_impulse_swing_low(symbol)

        if bq < PHASE_BLEED_BOUNCE_QUALITY_MAX:
            return PhaseResult(
                phase=MarketPhase.BLEED,
                range_low=range_low, range_high=range_high,
                direction_efficiency=dir_eff,
                bounce_quality=bq,
                swing_low=swing_low,
                struct_low=struct_low, struct_high=struct_high,
                note=f"bleed:bq={bq:.2f},dir_eff={dir_eff:.2f}",
            )

        if bq >= PHASE_TRADEABLE_BOUNCE_QUALITY_MIN:
            return PhaseResult(
                phase=MarketPhase.DROP_TRADEABLE,
                range_low=range_low, range_high=range_high,
                direction_efficiency=dir_eff,
                bounce_quality=bq,
                swing_low=swing_low,
                struct_low=struct_low, struct_high=struct_high,
                note=f"drop_tradeable:bq={bq:.2f}",
            )

        # Серая зона: PHASE_BLEED_BOUNCE_QUALITY_MAX <= bq < PHASE_TRADEABLE_BOUNCE_QUALITY_MIN
        return PhaseResult(
            phase=MarketPhase.BLEED,
            range_low=range_low, range_high=range_high,
            direction_efficiency=dir_eff,
            bounce_quality=bq,
            swing_low=swing_low,
            struct_low=struct_low, struct_high=struct_high,
            note=f"bleed:bq={bq:.2f},dir_eff={dir_eff:.2f}",
        )

    # ── Шаг 6: Fallback ──────────────────────────────────────────────────────
    return PhaseResult(
        phase=MarketPhase.UNKNOWN,
        range_low=range_low, range_high=range_high,
        direction_efficiency=dir_eff,
        bounce_quality=0.0,
        swing_low=0.0,
        struct_low=struct_low, struct_high=struct_high,
        note=f"unknown:range={range_pct:.1f}%,dir_eff={dir_eff:.2f}",
    )
