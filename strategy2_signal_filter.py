"""Strategy 2 Signal Filter — фильтрация сигналов без бумажной торговли.

Lightweight-класс: никаких записей в БД, никакого наследования от BaseStrategy.
Только проверка условий входа и расчёт параметров сетки.

Используется напрямую из Strategy2Live.on_event().
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass

from data.collector import candles_1m, candles_5m, candles_15m
from analysis.market_phase import (
    detect_market_phase,
    compute_range_bounds,
    find_impulse_swing_low,
    MarketPhase,
    PhaseResult,
)
from constants import (
    S2_MIN_STRENGTH,
    S2_MIN_P_BOUNCE,
    S2_PRESSURE_COOLDOWN_SECONDS,
    S2_GRID_ORDERS,
    S2_POSITION_SIZE_USDT,
    PHASE_FLAT_LEVEL_POSITION_MAX,
    S2_SL_MIN_DIST_ATR,
    S2_SL_RR_MIN,
    S2_SL_FLAT_BUFFER,
    S2_SL_IMPULSE_BUFFER,
    S2_REPLACE_COOLDOWN_SECONDS,
    S2_COOLDOWN_LEVEL_BAND_PCT,
    S2_LEVEL_REENTRY_BLOCK_SECONDS,
    S2_MAX_PRICE_ABOVE_LEVEL_PCT,
    S2_GRID_DEPTH_PCT,
    S2_GRID_TOP_WEIGHT_RATIO,
    S2_GRID_BOTTOM_WEIGHT_RATIO,
    S2_GRID_ANCHOR_PCT_PUMP,
    S2_GRID_ANCHOR_PCT_DEFAULT,
    # G1/G2/G4 (25.06)
    S2_MIN_P_BOUNCE_G2,
    S2_APPROACH_BLOCK,
    S2_APPROACH_CAUTIOUS,
    S2_VOL_FALLING_LOOKBACK,
    S2_APPROACH_SPEED_LOOKBACK,
    S2_VOL_FALLING_RATIO,
    # [H6] body_level — блок входа в активное падение на объёме
    S2_FALL_LOOKBACK,
    S2_FALL_DROP_PCT,
    S2_FALL_VOL_BASELINE,
    S2_FALL_VOL_SPIKE,
    S2_FLIP_ENABLED,
    S2_FLIP_MAX_AGE_HOURS,
    S2_FLIP_MATCH_ATR_MULT,
    S2_FLIP_MATCH_PCT,
    S2_FLIP_MAX_RETEST,
)
from logger import logger
from models import state_manager

# Trailing stop: отступ от пика после TP1
# [TRAILING WIDTH CHANGE] Новый расчёт начат 2026-06-20 10:00 МСК: 0.5% → 1.0%.
# (Эта копия не используется в live-трейлинге — он в strategy2_live.py; держим в синхроне.)
S2_TRAILING_PCT = 0.01   # 1.0% (было 0.005)

# TP2 как множитель ATR от entry
S2_TP2_ATR_MULT = 5.0

# Full-grid TP: при fill=S2_GRID_ORDERS ставим TP на этом % ниже уровня
S2_FULL_GRID_TP_PCT = 0.0015   # 0.15%


@dataclass
class GridParams:
    """Параметры сетки, возвращаемые фильтром при успешной проверке."""
    symbol: str
    level: float
    level_type: str
    level_side: str
    grid_prices: list[float]
    grid_bottom: float
    grid_anchor: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    order_size: float
    grid_sizes: list  # per-order объёмы USDT (верхнее утяжеление)
    atr: float
    strength: int
    p_bounce: float
    approach_style: str
    expected_depth: float
    vol_ratio: float
    ml_delta: int = 0
    p_fast_breakout: float = None
    market_phase: str = "unknown"
    range_low: float = 0.0
    range_high: float = 0.0
    rr: float = 0.0
    # G1/G2/G4 (25.06) — пробрасываются в open_live_trade для анализа
    signal_group: str = "g1"          # 'g1' | 'g2' | 'g4'
    is_flip: int = 0                  # 1 если S/R-флип
    flip_breakout_time: float = None  # epoch сек исходного пробоя
    flip_age_hours: float = None
    retest_number: int = None         # номер ретеста после флипа
    approach_count: int = None        # стабильный счётчик подходов на входе
    cautious_mode: int = 0            # 1 для G2 (трейлер + быстрый выход)
    vol_falling: int = None           # 1 если объём падал при подходе
    approach_speed_pct: float = None  # % изменение цены за S2_APPROACH_SPEED_LOOKBACK свечей до входа
    red_candles_streak: int = None    # кол-во подряд красных 1m-свечей перед входом


def _natr_pct(candles: list, period: int = 14) -> float | None:
    """NATR% = средний True Range за period свечей / текущая цена * 100.

    True Range = max(high-low, |high-prev_close|, |low-prev_close|).
    Абсолютная мера «живости» на данном ТФ. None при нехватке данных.
    """
    if not candles or len(candles) < period + 1:
        return None
    w = candles[-(period + 1):]
    trs = []
    for i in range(1, len(w)):
        h, l, pc = w[i]["high"], w[i]["low"], w[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not trs:
        return None
    atr = sum(trs) / len(trs)
    cur = w[-1]["close"]
    return round(atr / cur * 100, 3) if cur > 0 else None


class Strategy2SignalFilter:
    """Фильтр сигналов для Strategy 2 Limit Grid.

    Не пишет в БД, не открывает сделок — только проверяет условия входа
    и возвращает параметры сетки если вход разрешён.
    """

    def __init__(self) -> None:
        # symbol → timestamp последнего события "pressure"
        self._recent_pressure: dict[str, float] = {}
        # symbol → list[(level, ts)] последних закрытий сделок (band-матч по уровню)
        self._recent_close: dict[str, list[tuple[float, float]]] = {}
        # symbol → list[(level, ts)] последних РАЗМЕЩЕНИЙ сетки (band-матч по уровню)
        self._recent_placed: dict[str, list[tuple[float, float]]] = {}
        # Счётчик причин отказа/пропуска сигналов (для наблюдаемости).
        self._skip_counts: dict[str, int] = {}
        # [HYSTERESIS-LOG] symbol → deque[(ts, phase_value)] последних детектов фазы.
        # Пишется на каждом вычислении фазы в check(); только для логирования метрик
        # стабильности фазы на входе — НЕ гейт. Максимум ~5 мин истории при опросе 5с.
        self._phase_history: dict[str, deque] = {}

    def _liveness_metrics(self, symbol: str) -> dict:
        """[LIVENESS-LOG] Метрики «жив ли памп» на момент входа — только для лога.

        Гейт НЕ строим: бэктест показал, что старые входы (12–24ч) всё ещё в плюсе,
        жёстко резать их нельзя. Копим данные, чтобы позже построить гейт/приоритизацию
        на реальной статистике (fresh ≈ +1.17$/сделку vs stale ≈ +0.42$/сделку).

        Возвращает:
          hours_since_pump_peak — часов с пика за последние 24ч (96×15m)
          vol_decay             — ATR-range последних 4×15m / вокруг пика (0..1+; <0.5 = вяло)
          natr_now_pct          — текущий 15m-range к цене, % (абсолютная «живость», legacy)
          natr_1m / natr_5m / natr_15m — NATR% (True Range, 14 периодов) по таймфреймам.
            Бэктест: на S2 лучше всего разделяет 1m (corr +0.21), 15m хуже (−0.12).
            Логируем все три для калибровки гейта на 100+ live-сделках.
        """
        c15 = candles_15m.get(symbol, [])
        natr_tf = {
            "natr_1m":  _natr_pct(candles_1m.get(symbol, [])),
            "natr_5m":  _natr_pct(candles_5m.get(symbol, [])),
            "natr_15m": _natr_pct(c15),
        }
        if len(c15) < 40:
            return {"hours_since_pump_peak": None, "vol_decay": None, "natr_now_pct": None,
                    **natr_tf}
        w = c15[-96:]
        peak_i = max(range(len(w)), key=lambda i: w[i]["high"])
        peak_t = w[peak_i]["open_time"] / 1000.0
        hours_since = round((time.time() - peak_t) / 3600.0, 1)

        def _rng(cs):
            vals = [(c["high"] - c["low"]) / c["close"] for c in cs if c["close"] > 0]
            return sum(vals) / len(vals) if vals else 0.0

        now_rng = _rng(c15[-4:])
        peak_slice = w[max(0, peak_i - 2): peak_i + 3]
        peak_rng = _rng(peak_slice)
        vol_decay = round(now_rng / peak_rng, 2) if peak_rng > 0 else None
        natr_now = round(now_rng * 100, 2)
        return {"hours_since_pump_peak": hours_since, "vol_decay": vol_decay,
                "natr_now_pct": natr_now, **natr_tf}

    def _record_phase(self, symbol: str, phase_value: str) -> None:
        """Записать текущий детект фазы в историю (для метрик гистерезиса)."""
        dq = self._phase_history.get(symbol)
        if dq is None:
            dq = deque(maxlen=60)  # ~5 мин при опросе раз в 5с
            self._phase_history[symbol] = dq
        dq.append((time.time(), phase_value))

    def _phase_hysteresis_metrics(self, symbol: str, current_phase: str) -> dict:
        """Метрики стабильности фазы на момент входа (только для лога/бэктеста гейта).

        Возвращает:
          phase_streak_checks    — сколько последних подряд детектов == current_phase
          phase_streak_seconds   — сколько секунд фаза непрерывно == current_phase
          bleed_in_last_12       — сколько 'bleed' в последних 12 детектах (~60с)
          seconds_since_bleed    — секунд с последнего 'bleed' (None если не было в истории)
          recent_seq             — компактная строка последних до 12 фаз (b/f/d/p/u)
        """
        dq = self._phase_history.get(symbol)
        if not dq:
            return {"phase_streak_checks": 0, "phase_streak_seconds": 0.0,
                    "bleed_in_last_12": None, "seconds_since_bleed": None, "recent_seq": ""}
        items = list(dq)
        now = time.time()
        # streak подряд с конца, равный current_phase
        streak = 0
        streak_start_ts = now
        for ts, ph in reversed(items):
            if ph == current_phase:
                streak += 1
                streak_start_ts = ts
            else:
                break
        last12 = items[-12:]
        bleed_n = sum(1 for _, ph in last12 if ph == "bleed")
        secs_since_bleed = None
        for ts, ph in reversed(items):
            if ph == "bleed":
                secs_since_bleed = round(now - ts, 1)
                break
        _abbr = {"bleed": "b", "flat": "f", "drop_tradeable": "d", "pump": "p", "unknown": "u"}
        seq = "".join(_abbr.get(ph, "?") for _, ph in last12)
        return {
            "phase_streak_checks": streak,
            "phase_streak_seconds": round(now - streak_start_ts, 1),
            "bleed_in_last_12": bleed_n,
            "seconds_since_bleed": secs_since_bleed,
            "recent_seq": seq,
        }

    def notify_pressure(self, symbol: str) -> None:
        """Вызвать при получении события pressure для символа."""
        self._recent_pressure[symbol] = time.time()

    def _record(self, store: dict, symbol: str, level: float) -> None:
        """Записать (level, now) в per-symbol список и подрезать старьё (>1ч)."""
        now = time.time()
        lst = store.setdefault(symbol, [])
        lst.append((level, now))
        cutoff = now - 3600.0
        store[symbol] = [(lv, ts) for lv, ts in lst if ts >= cutoff]

    def _last_ts_in_band(self, store: dict, symbol: str, level: float) -> float:
        """Самый свежий ts среди недавних уровней symbol в пределах ±band от level.
        Закрывает дыру точного ключа: повтор «рядом» (переокругление/соседний уровень)
        тоже попадает под кулдаун. 0.0 если совпадений нет."""
        band = abs(level) * S2_COOLDOWN_LEVEL_BAND_PCT
        best = 0.0
        for lv, ts in store.get(symbol, ()):  # type: ignore[union-attr]
            if abs(lv - level) <= band and ts > best:
                best = ts
        return best

    def notify_placed(self, symbol: str, level: float) -> None:
        """Вызвать при размещении сетки — запускает cooldown по уровню (band),
        не зависящий от того, как быстро сетка закроется (защита от no-fill спама)."""
        self._record(self._recent_placed, symbol, level)

    def notify_closed(self, symbol: str, level: float) -> None:
        """Вызвать при закрытии сделки — запускает cooldown по уровню (band)."""
        self._record(self._recent_close, symbol, level)

    def get_skip_stats(self) -> dict[str, int]:
        """Копия счётчика причин отказа/пропуска сигналов."""
        return dict(self._skip_counts)

    def reset_skip_stats(self) -> None:
        """Обнулить счётчик (например, при периодическом отчёте)."""
        self._skip_counts.clear()

    # ── G1/G2/G4 helpers (25.06) ─────────────────────────────────────────

    def _compute_vol_falling(self, symbol: str) -> int | None:
        """1 если объём последней 1m-свечи ниже среднего предыдущих, иначе 0.
        None при нехватке данных. Признак для анализа (не жёсткий гейт)."""
        c1m = candles_1m.get(symbol, [])
        look = S2_VOL_FALLING_LOOKBACK
        if len(c1m) < look:
            return None
        vols = [c["volume"] for c in c1m[-look:]]
        prev = vols[:-1]
        if not prev:
            return None
        mean_prev = sum(prev) / len(prev)
        if mean_prev <= 0:
            return None
        return 1 if vols[-1] < mean_prev * S2_VOL_FALLING_RATIO else 0

    def _compute_approach_speed(self, symbol: str) -> float | None:
        """% изменение close за последние S2_APPROACH_SPEED_LOOKBACK 1m-свечей.
        Отрицательное = падение к уровню. None при нехватке данных."""
        c1m = candles_1m.get(symbol, [])
        need = S2_APPROACH_SPEED_LOOKBACK + 1
        if len(c1m) < need:
            return None
        p_old = c1m[-need]["close"]
        p_new = c1m[-1]["close"]
        if p_old <= 0:
            return None
        return round((p_new - p_old) / p_old * 100, 4)

    def _compute_red_candles_streak(self, symbol: str) -> int | None:
        """Количество последних 1m-свечей подряд с close < предыдущего close.
        None при нехватке данных (менее 2 свечей)."""
        c1m = candles_1m.get(symbol, [])
        if len(c1m) < 2:
            return None
        streak = 0
        for i in range(len(c1m) - 1, 0, -1):
            if c1m[i]["close"] < c1m[i - 1]["close"]:
                streak += 1
            else:
                break
        return streak

    def _active_fall_metrics(self, symbol: str) -> tuple[bool, float, float]:
        """[H6] Вернуть (is_fall, drop_pct, spike) для body_level-гейта.

        is_fall=True только при связке: крутой нисходящий импульс за S2_FALL_LOOKBACK
        1m-свечей (drop_pct <= -S2_FALL_DROP_PCT) И всплеск объёма продаж на этом окне
        (spike >= S2_FALL_VOL_SPIKE). Обычная коррекция (объём затухает) сюда не
        попадает. drop_pct/spike возвращаем всегда — чтобы логировать контекст блока.
        При нехватке данных → (False, 0.0, 0.0).
        """
        c1m = candles_1m.get(symbol, [])
        need = S2_FALL_LOOKBACK + S2_FALL_VOL_BASELINE + 1
        if len(c1m) < need:
            return (False, 0.0, 0.0)
        p0 = c1m[-S2_FALL_LOOKBACK - 1]["close"]
        p1 = c1m[-1]["close"]
        if p0 <= 0:
            return (False, 0.0, 0.0)
        drop_pct = (p1 - p0) / p0 * 100.0
        recent = c1m[-S2_FALL_LOOKBACK:]
        base = c1m[-(S2_FALL_LOOKBACK + S2_FALL_VOL_BASELINE):-S2_FALL_LOOKBACK]
        v_recent = sum(c["volume"] for c in recent) / len(recent) if recent else 0.0
        v_base = sum(c["volume"] for c in base) / len(base) if base else 0.0
        spike = (v_recent / v_base) if v_base > 0 else 0.0
        is_fall = (drop_pct <= -S2_FALL_DROP_PCT) and (spike >= S2_FALL_VOL_SPIKE)
        return (is_fall, drop_pct, spike)

    def _compute_approach_count(self, symbol: str, level: float, atr: float,
                                event: dict) -> int:
        """Стабильный счётчик подходов (вариант A) — тот же метод, что _real_touches.
        Fallback: event['approach'], затем 1."""
        try:
            from analysis.trigger import _count_approaches, _origin_anchor
            if atr and atr > 0:
                return _count_approaches(symbol, level, atr,
                                         anchor_time=_origin_anchor(symbol, level))[0]
        except Exception as e:
            logger.debug("S2: approach_count compute failed", symbol=symbol, error=str(e))
        ev = event.get("approach")
        return int(ev) if ev is not None else 1

    async def _detect_flip(self, symbol: str, level: float, atr: float) -> dict:
        """Детект S/R-флипа (G4): уровень был пробит вверх (resistance) ≤24ч назад,
        текущий уровень совпадает по цене (±0.5·ATR или ±0.3%), и это первый ретест.

        Возвращает dict: is_flip, flip_breakout_time (сек), flip_age_hours, retest_number.
        Fail-safe: при любой ошибке/без данных → is_flip=False.
        """
        out = {"is_flip": False, "flip_breakout_time": None,
               "flip_age_hours": None, "retest_number": None}
        if not S2_FLIP_ENABLED:
            return out
        try:
            from data.history import get_recent_breakouts
            from analysis.trigger import _count_approaches
            breakouts = await get_recent_breakouts(symbol, S2_FLIP_MAX_AGE_HOURS, side="resistance")
            if not breakouts:
                return out
            # допуск матча уровня: меньшее из ±0.5·ATR и ±0.3%
            tol_atr = atr * S2_FLIP_MATCH_ATR_MULT if atr and atr > 0 else float("inf")
            tol_pct = level * S2_FLIP_MATCH_PCT
            tol = min(tol_atr, tol_pct)
            for bo in breakouts:  # отсортированы по свежести (DESC)
                if abs(level - bo["level"]) > tol:
                    continue
                breakout_ms = bo.get("breakout_ms") or 0
                # retest_number = число подходов ПОСЛЕ пробоя (якорь = время пробоя)
                retest = 1
                try:
                    if atr and atr > 0 and breakout_ms > 0:
                        retest = _count_approaches(symbol, level, atr, anchor_time=float(breakout_ms))[0]
                except Exception:
                    retest = 1
                is_first = retest <= S2_FLIP_MAX_RETEST
                out.update({
                    "is_flip": bool(is_first),
                    "flip_breakout_time": round(breakout_ms / 1000.0, 3) if breakout_ms else None,
                    "flip_age_hours": round(bo.get("age_hours") or 0.0, 3),
                    "retest_number": retest,
                })
                return out  # первый совпавший (самый свежий) пробой
        except Exception as e:
            logger.debug("S2: flip detect failed", symbol=symbol, error=str(e))
        return out

    async def _log_decision(self, symbol, level, level_type, decision,
                            block_reason=None, signal_group=None, phase=None,
                            approach_count=None, vol_falling=None, is_flip=None,
                            p_bounce=None) -> None:
        """Записать решение фильтра в history.signal_decisions (best-effort)."""
        try:
            from data.history import log_signal_decision
            await log_signal_decision(
                symbol=symbol, level=level, level_type=level_type,
                decision=decision, block_reason=block_reason, signal_group=signal_group,
                phase=phase, approach_count=approach_count, vol_falling=vol_falling,
                is_flip=(1 if is_flip else 0) if is_flip is not None else None,
                p_bounce=p_bounce,
            )
        except Exception:
            pass

    async def check(
        self,
        event: dict,
        current_open_count: int,
        max_open_trades: int,
        has_open_for_symbol: bool,
    ) -> tuple[bool, GridParams | None]:
        """Проверить условия входа.

        Args:
            event: событие proximity из event_bus
            current_open_count: текущее число открытых live-сделок
            max_open_trades: максимально допустимое число сделок
            has_open_for_symbol: уже есть открытая live-сделка по этому символу

        Returns:
            (True, GridParams) — вход разрешён
            (False, None)      — вход заблокирован фильтром
        """
        symbol = event["symbol"]
        strength = event.get("strength", 0)
        p_bounce_raw = event.get("p_bounce", None)
        approach_style = event.get("approach_style", "unknown")

        if p_bounce_raw is None:
            self._skip_counts["p_bounce_missing"] = self._skip_counts.get("p_bounce_missing", 0) + 1
            logger.warning(
                "S2 skip: p_bounce MISSING (ML did not attach value)",
                symbol=symbol,
                level=event.get("level"),
                level_type=event.get("level_type"),
            )
            return False, None
        p_bounce = p_bounce_raw

        # ── Фильтр 0: consolidation_base — торгуем только в 1й/2й подход (25.06) ──
        # Раньше был жёсткий блок ("структурно убыточный режим"). Теперь консолидацию
        # пускаем, но ограниченно: 3+ подход режет гейт G3 ниже, BLEED — Фильтр 9, а
        # во флете Фильтр 10 требует уровень в нижних 30% диапазона (вход от нижней
        # границы). Флипом консолидацию не считаем (см. is_flip ниже) — только G1/G2.

        # ── Фильтр 1: сила уровня ────────────────────────────────────────────
        if strength < S2_MIN_STRENGTH:
            self._skip_counts["strength_low"] = self._skip_counts.get("strength_low", 0) + 1
            logger.debug(
                "S2 skip: strength too low",
                symbol=symbol,
                strength=strength,
                min_required=S2_MIN_STRENGTH,
            )
            return False, None

        # ── G1/G2/G4: подход, флип, группа (25.06) ───────────────────────────
        level = event["level"]
        _atr_gate = event.get("atr", 0.0)
        approach_count = self._compute_approach_count(symbol, level, _atr_gate, event)
        vol_falling = self._compute_vol_falling(symbol)
        approach_speed_pct = self._compute_approach_speed(symbol)
        red_candles_streak = self._compute_red_candles_streak(symbol)
        flip = await self._detect_flip(symbol, level, _atr_gate)
        is_flip = bool(flip["is_flip"])

        # Консолидацию не торгуем как флип — только по номеру подхода (G1/G2).
        # Иначе совпавший по цене флип увёл бы её в g4 в обход правила «1й/2й подход».
        # Обнуляем и метаданные флипа, чтобы is_flip=0 и flip_* в БД были согласованы.
        if event.get("level_type", "") == "consolidation_base":
            is_flip = False
            flip = {"is_flip": False, "flip_breakout_time": None,
                    "flip_age_hours": None, "retest_number": None}

        # Группа + флор p_bounce + cautious-режим:
        #   G4 (флип)           → торгуем, обычный флор, без cautious
        #   G2 (2й/3й подход)   → торгуем осторожно: нижний флор + cautious (трейлер/быстрый выход)
        #                         [H4] 3й подход разрешён: суммарно approach=1-3 → +15.9 USDT.
        #                         Для approach=3 трейлинг запускается только ≥60с после
        #                         последнего заполненного ордера (см. strategy2_live._price_loop).
        #   G1 (1й подход)      → обычный
        if is_flip:
            signal_group, p_bounce_floor, cautious_mode = "g4", S2_MIN_P_BOUNCE, 0
        elif approach_count >= S2_APPROACH_CAUTIOUS:
            signal_group, p_bounce_floor, cautious_mode = "g2", S2_MIN_P_BOUNCE_G2, 1
        else:
            signal_group, p_bounce_floor, cautious_mode = "g1", S2_MIN_P_BOUNCE, 0

        # ── Гейт по подходам: G3 (4+ подхода, не флип) — не торговать ─────────
        # [H4] Порог поднят с 3 до 4: approach=3 теперь торгуется как G2/cautious.
        if approach_count >= S2_APPROACH_BLOCK and not is_flip:
            self._skip_counts["approach_ge_block"] = self._skip_counts.get("approach_ge_block", 0) + 1
            logger.info(
                "S2 skip: approach>=block (G3, level exhausted)",
                symbol=symbol, level=level, approach_count=approach_count,
                block_threshold=S2_APPROACH_BLOCK,
            )
            # P6: уровень "мёртвый" — approach дальше будет только расти, повторно
            # пускать его через фильтр смысла нет. Помечаем для monitor.py, чтобы тот
            # перестал слать proximity-события (которые и приводят сюда каждые 5 сек).
            _state = state_manager.get_state(symbol)
            _task_key = _state.make_task_key(level)
            if not _state.is_level_dead(_task_key):
                _state.mark_level_dead(_task_key)
                logger.info(
                    "S2: level marked dead, monitor will stop sending proximity",
                    symbol=symbol, level=level, approach_count=approach_count,
                )
            await self._log_decision(symbol, level, event.get("level_type"), "block",
                                     block_reason="approach_ge_4", signal_group="g3",
                                     approach_count=approach_count, vol_falling=vol_falling,
                                     is_flip=is_flip, p_bounce=p_bounce)
            return False, None

        # ── Фильтр 2: вероятность отскока (групповой флор) ──────────────────
        # G1/G4 → S2_MIN_P_BOUNCE; G2 → S2_MIN_P_BOUNCE_G2 (ниже, риск держит
        # управление позицией: трейлер + выход по слому). Хард-фильтр touches в
        # ml_score снят, поэтому p_bounce здесь — сырой выход ML.
        if p_bounce < p_bounce_floor:
            _touches = event.get("approach")
            _touches_str = "unknown(no-count)" if _touches is None else _touches
            self._skip_counts["p_bounce_low"] = self._skip_counts.get("p_bounce_low", 0) + 1
            logger.debug(
                "S2 skip: p_bounce below group floor",
                symbol=symbol,
                group=signal_group,
                p_bounce=round(p_bounce, 3),
                floor=p_bounce_floor,
                approach_count=approach_count,
                touches=_touches_str,
            )
            await self._log_decision(symbol, level, event.get("level_type"), "block",
                                     block_reason="p_bounce_low", signal_group=signal_group,
                                     approach_count=approach_count, vol_falling=vol_falling,
                                     is_flip=is_flip, p_bounce=p_bounce)
            return False, None

        # ── Фильтр 3: стиль подхода ──────────────────────────────────────────
        if approach_style == "bleed":
            self._skip_counts["approach_bleed"] = self._skip_counts.get("approach_bleed", 0) + 1
            logger.debug("S2 skip: approach_style=bleed", symbol=symbol)
            return False, None

        # ── Фильтр 4: cooldown после pressure ────────────────────────────────
        last_pressure = self._recent_pressure.get(symbol, 0.0)
        seconds_since_pressure = time.time() - last_pressure
        if seconds_since_pressure < S2_PRESSURE_COOLDOWN_SECONDS:
            self._skip_counts["pressure_cooldown"] = self._skip_counts.get("pressure_cooldown", 0) + 1
            logger.debug(
                "S2 skip: pressure cooldown active",
                symbol=symbol,
                seconds_since_pressure=round(seconds_since_pressure, 1),
                cooldown=S2_PRESSURE_COOLDOWN_SECONDS,
            )
            return False, None

        # ── Фильтр 5: лимит открытых сделок ─────────────────────────────────
        if has_open_for_symbol:
            self._skip_counts["already_open_symbol"] = self._skip_counts.get("already_open_symbol", 0) + 1
            logger.debug("S2 skip: already have open trade for symbol", symbol=symbol)
            return False, None

        if current_open_count >= max_open_trades:
            self._skip_counts["max_open_trades"] = self._skip_counts.get("max_open_trades", 0) + 1
            logger.debug(
                "S2 skip: max open trades reached",
                symbol=symbol,
                open_count=current_open_count,
                max_open=max_open_trades,
            )
            return False, None

        # ── Фильтр 6: cooldown после закрытия по этому уровню (band) ─────────
        level = event["level"]
        seconds_since_close = time.time() - self._last_ts_in_band(self._recent_close, symbol, level)
        if seconds_since_close < 300:
            self._skip_counts["close_cooldown"] = self._skip_counts.get("close_cooldown", 0) + 1
            logger.debug(
                "S2 skip: recent close cooldown",
                symbol=symbol,
                level=level,
                seconds_since_close=round(seconds_since_close, 1),
            )
            return False, None

        # ── Фильтр 6b: cooldown после размещения по этому уровню (band) ──────
        # Защита от no-fill спама: сетка ставится, не наполняется, быстро
        # отменяется и тут же ставится снова. Кулдаун от момента РАЗМЕЩЕНИЯ
        # держит уровень залоченным независимо от пути закрытия. Band-матч
        # ловит и повторы на чуть сдвинутом уровне (переокругление/соседний).
        seconds_since_placed = time.time() - self._last_ts_in_band(self._recent_placed, symbol, level)
        if seconds_since_placed < S2_REPLACE_COOLDOWN_SECONDS:
            self._skip_counts["placement_cooldown"] = self._skip_counts.get("placement_cooldown", 0) + 1
            logger.debug(
                "S2 skip: recent placement cooldown",
                symbol=symbol,
                level=level,
                seconds_since_placed=round(seconds_since_placed, 1),
            )
            return False, None

        # ── Фильтр 6c: жёсткий re-entry блок по уровню-полосе ────────────────
        # ПЕРВИЧНАЯ защита от повторного входа на тот же уровень. Нужна потому что
        # touches-хардфильтр (touches>=2 → p_bounce=0) ненадёжен: _count_approaches
        # сбрасывает счётчик касаний на каждом новом ценовом пике (pump_high_time),
        # так что возврат к уровню после движения вверх читается как «первое касание»
        # с высоким p_bounce → бот заходит снова и снова (кейс LABUSDT). Блок по
        # полосе уровня ловит это независимо от touches: после ЛЮБОЙ активности
        # (размещение или закрытие) на уровне не входим N сек. Первые входы не
        # трогает — на новом уровне прошлой активности в полосе нет.
        last_level_activity = max(
            self._last_ts_in_band(self._recent_close, symbol, level),
            self._last_ts_in_band(self._recent_placed, symbol, level),
        )
        # G2/G4 — НАМЕРЕННЫЕ повторные подходы к уровню: 30-мин re-entry блок к ним
        # не применяем, иначе 2й подход (G2) и ретест флипа (G4) никогда не войдут.
        # Счётчик подходов теперь стабилен (вариант A), поэтому 3+ подхода отсекает
        # гейт G3 выше, а не этот таймер. Для G1 блок сохраняется (анти-спам на свежем
        # уровне; первичная защита от no-fill спама, ради которой он и вводился).
        if last_level_activity > 0.0 and signal_group not in ("g2", "g4"):
            seconds_since_activity = time.time() - last_level_activity
            if seconds_since_activity < S2_LEVEL_REENTRY_BLOCK_SECONDS:
                self._skip_counts["level_reentry_block"] = self._skip_counts.get("level_reentry_block", 0) + 1
                logger.info(
                    "S2 skip: level re-entry block",
                    symbol=symbol,
                    level=level,
                    seconds_since_activity=round(seconds_since_activity, 1),
                )
                return False, None

        # ── Фаза рынка ───────────────────────────────────────────────────────
        # Определить фазу ДО расчёта сетки, чтобы использовать при расчёте SL.
        # event["current_price"] может отсутствовать — fallback через candles_1m.
        _c1m_tmp = candles_1m.get(symbol, [])
        _cur_tmp = _c1m_tmp[-1]["close"] if _c1m_tmp else 0.0
        _atr_tmp = event.get("atr", 0.0)
        phase_result: PhaseResult = detect_market_phase(symbol, _atr_tmp, _cur_tmp)
        phase = phase_result.phase
        self._record_phase(symbol, phase.value)  # [HYSTERESIS-LOG] история для метрик стабильности
        logger.info(
            "S2 market phase",
            symbol=symbol,
            phase=phase.value,
            note=phase_result.note,
        )

        # ── Фильтр 9: блид — не торговать ────────────────────────────────────
        if phase == MarketPhase.BLEED:
            self._skip_counts["phase_bleed"] = self._skip_counts.get("phase_bleed", 0) + 1
            logger.debug("S2 skip: market phase=bleed", symbol=symbol, note=phase_result.note)
            await self._log_decision(symbol, level, event.get("level_type"), "block",
                                     block_reason="phase_bleed", signal_group=signal_group,
                                     phase=phase.value, approach_count=approach_count,
                                     vol_falling=vol_falling, is_flip=is_flip, p_bounce=p_bounce)
            return False, None

        # ── Фильтр 10: флет — уровень должен быть в нижних 30% диапазона ─────
        # Структурные типы (pump_base/breakout_level/wick_level) по позиции НЕ
        # гейтим — как и в Фильтре 10b: pump_base прибылен и в середине диапазона
        # (WR 73%, +4.23). Гейт держим только для body_level и consolidation_base.
        _STRUCTURAL_TYPES = ("pump_base", "breakout_level", "wick_level")
        if phase == MarketPhase.FLAT and event.get("level_type", "") not in _STRUCTURAL_TYPES:
            # [P7] Позицию считаем по структурному 24ч-диапазону, а не по узкому
            # 90м-окну классификатора (оно занижает позицию у вершины). Откат на
            # локальное окно, если 24ч-данных нет.
            _rl = phase_result.struct_low or phase_result.range_low
            _rh = phase_result.struct_high or phase_result.range_high
            _span = _rh - _rl
            if _span > 0:
                _lvl_pos = (event["level"] - _rl) / _span
                if _lvl_pos > PHASE_FLAT_LEVEL_POSITION_MAX:
                    self._skip_counts["flat_level_position"] = self._skip_counts.get("flat_level_position", 0) + 1
                    logger.debug(
                        "S2 skip: flat phase but level not at lower boundary",
                        symbol=symbol,
                        level=event["level"],
                        level_position_pct=round(_lvl_pos * 100, 1),
                        max_allowed_pct=round(PHASE_FLAT_LEVEL_POSITION_MAX * 100, 1),
                        range_low=round(_rl, 8),
                        range_high=round(_rh, 8),
                    )
                    return False, None

        # ── Фильтр 10b: drop_tradeable + body_level — уровень в нижних 30% ────
        # body_level в середине падающего диапазона = нет структурной опоры снизу.
        # pump_base / breakout_level / wick_level — структурные, не трогаем.
        if phase == MarketPhase.DROP_TRADEABLE and event.get("level_type", "") == "body_level":
            # [P7] Структурный 24ч-диапазон вместо узкого 90м-окна (см. Фильтр 10).
            _rl = phase_result.struct_low or phase_result.range_low
            _rh = phase_result.struct_high or phase_result.range_high
            _span = _rh - _rl
            if _span > 0:
                _lvl_pos = (event["level"] - _rl) / _span
                if _lvl_pos > PHASE_FLAT_LEVEL_POSITION_MAX:
                    self._skip_counts["drop_body_level_position"] = self._skip_counts.get("drop_body_level_position", 0) + 1
                    logger.debug(
                        "S2 skip: drop_tradeable body_level not at lower boundary",
                        symbol=symbol,
                        level=event["level"],
                        level_position_pct=round(_lvl_pos * 100, 1),
                        max_allowed_pct=round(PHASE_FLAT_LEVEL_POSITION_MAX * 100, 1),
                        range_low=round(_rl, 8),
                        range_high=round(_rh, 8),
                    )
                    try:
                        from data.history import log_event as _log_event
                        await _log_event(
                            symbol,
                            "skip_drop_body_level",
                            f"level={event['level']} pos={round(_lvl_pos * 100, 1)}% range=[{round(_rl, 6)}..{round(_rh, 6)}]",
                        )
                    except Exception:
                        pass
                    return False, None

        # ── Фильтр 10c [H6]: body_level — не входить в активное падение ────────
        # Подтверждено на VELVET ×2 / HEI / BEAT (~−10.6 за 2 дня): body_level лонг
        # в начале обвала → грид наливается вниз, опоры нет, честный пробой SL.
        # Режем ТОЛЬКО при крутом нисходящем импульсе НА всплеске объёма продаж —
        # обычную коррекцию к уровню (объём затухает) не трогаем. Только body_level;
        # pump_base/структурные не затрагиваем. Лог пишет контекст (drop/spike/цена)
        # для калибровки порогов — наблюдаем эффект на новых данных.
        if event.get("level_type", "") == "body_level":
            _fall, _fall_drop, _fall_spike = self._active_fall_metrics(symbol)
            if _fall:
                _c1m_px = candles_1m.get(symbol, [])
                _cur_px = event.get("current_price") or (_c1m_px[-1]["close"] if _c1m_px else 0.0)
                self._skip_counts["body_level_active_fall"] = self._skip_counts.get("body_level_active_fall", 0) + 1
                logger.info(
                    "S2 skip: body_level in active fall (H6)",
                    symbol=symbol,
                    level=event["level"],
                    current_price=round(_cur_px, 8),
                    drop_pct=round(_fall_drop, 2),
                    vol_spike=round(_fall_spike, 2),
                    drop_thresh=-S2_FALL_DROP_PCT,
                    spike_thresh=S2_FALL_VOL_SPIKE,
                )
                try:
                    from data.history import log_event as _log_event
                    await _log_event(
                        symbol,
                        "skip_body_active_fall",
                        f"level={event['level']} price={round(_cur_px, 8)} "
                        f"drop={round(_fall_drop, 2)}% spike={round(_fall_spike, 2)}x",
                    )
                except Exception:
                    pass
                return False, None

        # ── Расчёт параметров сетки ───────────────────────────────────────────
        atr = event.get("atr", 0.0)
        expected_depth = event.get("expected_depth", 0.0)
        vol_ratio = event.get("vol_ratio", 1.0)
        ml_delta = event.get("ml_delta", 0)
        p_fast_breakout = event.get("p_fast_breakout", None)

        if atr <= 0:
            self._skip_counts["atr_zero"] = self._skip_counts.get("atr_zero", 0) + 1
            logger.debug("S2 skip: atr=0", symbol=symbol)
            return False, None

        grid_width = min(atr * 2.5, level * S2_GRID_DEPTH_PCT)
        step = grid_width / (S2_GRID_ORDERS - 1)
        level_type = event.get("level_type", "")
        # pump_base: откат после импульса часто короткий — первый ордер ближе к цене касания.
        # Остальные типы: стандартный front-run 0.15%.
        grid_anchor_pct = S2_GRID_ANCHOR_PCT_PUMP if level_type == "pump_base" else S2_GRID_ANCHOR_PCT_DEFAULT
        grid_anchor = level * grid_anchor_pct  # первый ордер выше уровня

        c1m = candles_1m.get(symbol, [])
        current_price = c1m[-1]["close"] if c1m else grid_anchor

        # ── Фильтр 7: цена слишком далеко выше УРОВНЯ ───────────────────────
        # Раньше порог был ~grid_anchor*1.005 (~1.1% от уровня) → резал ранние
        # касания, бот входил только на поздних подходах. Теперь разрешаем
        # постановку, пока цена в пределах S2_MAX_PRICE_ABOVE_LEVEL_PCT ОТ УРОВНЯ
        # — сетка ставится заранее и ловит первое касание (5с-wick исполняет
        # лимитки на бирже сам). Анкор не трогаем: филлы всё равно идут у уровня.
        max_price_above = level * (1.0 + S2_MAX_PRICE_ABOVE_LEVEL_PCT)
        if current_price > max_price_above:
            self._skip_counts["price_too_far_above_grid"] = self._skip_counts.get("price_too_far_above_grid", 0) + 1
            logger.debug(
                "S2 skip: price too far above level",
                symbol=symbol,
                current=round(current_price, 8),
                level=round(level, 8),
                diff_pct=round((current_price - level) / level * 100, 3),
                max_pct=round(S2_MAX_PRICE_ABOVE_LEVEL_PCT * 100, 2),
            )
            return False, None

        grid_prices = [grid_anchor - step * i for i in range(S2_GRID_ORDERS)]
        grid_bottom = grid_prices[-1]

        # ── Фильтр 8: цена уже ниже нижнего ордера — вся сетка заполнится ────
        if current_price < grid_bottom:
            self._skip_counts["price_below_grid_bottom"] = self._skip_counts.get("price_below_grid_bottom", 0) + 1
            logger.debug(
                "S2 skip: price already below grid bottom",
                symbol=symbol,
                current=round(current_price, 8),
                grid_bottom=round(grid_bottom, 8),
            )
            return False, None

        # ── TP/SL — логика зависит от фазы рынка ──────────────────────────────
        entry_price_initial = level
        order_size = round(S2_POSITION_SIZE_USDT / S2_GRID_ORDERS, 4)  # средний (для логов/обр. совместимости)
        # Верхнее утяжеление: линейные веса от ratio (верхний ордер) до 1.0 (нижний),
        # нормированные на S2_POSITION_SIZE_USDT. Больше объёма в мелкой зоне отскока.
        _N = S2_GRID_ORDERS
        _ratio_top = S2_GRID_TOP_WEIGHT_RATIO
        _ratio_bot = S2_GRID_BOTTOM_WEIGHT_RATIO
        _weights = [_ratio_top - (_ratio_top - _ratio_bot) * i / (_N - 1) for i in range(_N)]
        _wsum = sum(_weights)
        grid_sizes = [round(S2_POSITION_SIZE_USDT * w / _wsum, 4) for w in _weights]

        if phase == MarketPhase.FLAT:
            # Флет: SL ниже нижней границы диапазона с буфером.
            # Сетка сдвигается вниз так, чтобы grid_bottom == range_low.
            _rl = phase_result.range_low
            if _rl > 0 and grid_bottom > _rl:
                _shift = grid_bottom - _rl
                grid_prices = [round(p - _shift, 8) for p in grid_prices]
                grid_bottom = grid_prices[-1]
                grid_anchor = grid_prices[0]
            stop_loss = (phase_result.range_low if phase_result.range_low > 0 else grid_bottom) - atr * S2_SL_FLAT_BUFFER

        elif phase == MarketPhase.DROP_TRADEABLE:
            # Импульс вниз с отскоками: SL ниже swing_low.
            _swing_low = phase_result.swing_low
            if _swing_low > 0:
                stop_loss = _swing_low - atr * S2_SL_IMPULSE_BUFFER
                # Сетка от текущего уровня до swing_low, но не глубже зоны отскока
                _drop_width = min(level - _swing_low, level * S2_GRID_DEPTH_PCT)
                if _drop_width > 0:
                    _step = _drop_width / (S2_GRID_ORDERS - 1)
                    grid_prices = [round(level - _step * i, 8) for i in range(S2_GRID_ORDERS)]
                    grid_bottom = grid_prices[-1]
                    grid_anchor = grid_prices[0]
                else:
                    stop_loss = grid_bottom - atr * 0.5   # fallback
            else:
                stop_loss = grid_bottom - atr * 0.5   # swing_low не найден → стандарт

        else:
            # PUMP, UNKNOWN: оригинальная формула
            stop_loss = grid_bottom - atr * 0.5

        # Гарантия: SL не ближе S2_SL_MIN_DIST_ATR * atr от entry
        _min_sl_dist = atr * S2_SL_MIN_DIST_ATR
        if (entry_price_initial - stop_loss) < _min_sl_dist:
            stop_loss = entry_price_initial - _min_sl_dist
            logger.info(
                "S2: SL adjusted to minimum distance",
                symbol=symbol,
                min_dist_pct=round(_min_sl_dist / entry_price_initial * 100, 3),
                stop_loss=round(stop_loss, 8),
            )

        # ── Гарантия: SL строго НИЖЕ всей сетки ───────────────────────────────
        # S2_SL_MIN_DIST_ATR (~0.8·ATR) может быть меньше ширины сетки (atr*2.5),
        # тогда предыдущая корректировка ставит SL ВНУТРИ сетки, и нижние ордера
        # оказываются под ним — они физически не исполнятся (цена пробьёт SL
        # раньше, чем дойдёт до них). Сдвигаем SL под нижний ордер сетки.
        # Если из-за расширения SL RR станет ниже порога — сделку корректно
        # отклонит Фильтр 11 ниже (кривую геометрию торговать не нужно).
        if stop_loss >= grid_bottom:
            stop_loss = grid_bottom - atr * 0.5
            logger.info(
                "S2: SL moved below grid_bottom (was inside grid)",
                symbol=symbol,
                grid_bottom=round(grid_bottom, 8),
                stop_loss=round(stop_loss, 8),
            )

        tp1 = entry_price_initial + (entry_price_initial - grid_bottom) * 1.0
        tp2 = entry_price_initial + atr * S2_TP2_ATR_MULT

        # ── Фильтр 11: проверка RR ────────────────────────────────────────────
        _sl_dist  = entry_price_initial - stop_loss
        _tp1_dist = tp1 - entry_price_initial
        _rr = _tp1_dist / _sl_dist if _sl_dist > 0 else 0.0
        if _rr < S2_SL_RR_MIN:
            self._skip_counts["rr_too_low"] = self._skip_counts.get("rr_too_low", 0) + 1
            logger.debug(
                "S2 skip: RR below minimum",
                symbol=symbol,
                rr=round(_rr, 2),
                min_rr=S2_SL_RR_MIN,
                sl_dist_pct=round(_sl_dist / entry_price_initial * 100, 3),
                tp1_dist_pct=round(_tp1_dist / entry_price_initial * 100, 3),
            )
            return False, None

        params = GridParams(
            symbol=symbol,
            level=level,
            level_type=level_type,
            level_side=event.get("level_side", "support"),
            grid_prices=[round(p, 8) for p in grid_prices],
            grid_bottom=round(grid_bottom, 8),
            grid_anchor=round(grid_anchor, 8),
            stop_loss=round(stop_loss, 8),
            take_profit_1=round(tp1, 8),
            take_profit_2=round(tp2, 8),
            order_size=order_size,
            grid_sizes=grid_sizes,
            atr=atr,
            strength=strength,
            p_bounce=p_bounce,
            approach_style=approach_style,
            expected_depth=expected_depth,
            vol_ratio=vol_ratio,
            ml_delta=ml_delta,
            p_fast_breakout=p_fast_breakout,
            market_phase=phase.value,
            range_low=round(phase_result.range_low, 8),
            range_high=round(phase_result.range_high, 8),
            rr=round(_rr, 2),
            signal_group=signal_group,
            is_flip=1 if is_flip else 0,
            flip_breakout_time=flip.get("flip_breakout_time") if is_flip else None,
            flip_age_hours=flip.get("flip_age_hours") if is_flip else None,
            retest_number=flip.get("retest_number") if is_flip else None,
            approach_count=approach_count,
            cautious_mode=cautious_mode,
            vol_falling=vol_falling,
            approach_speed_pct=approach_speed_pct,
            red_candles_streak=red_candles_streak,
        )

        # [P7] Позиция уровня в структурном 24ч-диапазоне — логируем на КАЖДОМ
        # входе (раньше range писался только при skip), чтобы калибровать гейт.
        _slo = phase_result.struct_low or phase_result.range_low
        _shi = phase_result.struct_high or phase_result.range_high
        _pos_struct = (level - _slo) / (_shi - _slo) if (_shi - _slo) > 0 else None
        _pos_local = (
            (level - phase_result.range_low) / (phase_result.range_high - phase_result.range_low)
            if (phase_result.range_high - phase_result.range_low) > 0 else None
        )

        _hyst = self._phase_hysteresis_metrics(symbol, phase.value)  # [HYSTERESIS-LOG]
        _live = self._liveness_metrics(symbol)  # [LIVENESS-LOG]

        logger.info(
            "S2 signal passed all filters",
            symbol=symbol,
            level=level,
            level_type=level_type,
            strength=strength,
            p_bounce=round(p_bounce, 3),
            sl=params.stop_loss,
            tp1=params.take_profit_1,
            tp2=params.take_profit_2,
            rr=params.rr,
            phase=params.market_phase,
            pos_struct_pct=round(_pos_struct * 100, 1) if _pos_struct is not None else None,
            pos_local_pct=round(_pos_local * 100, 1) if _pos_local is not None else None,
            struct_range=[round(_slo, 8), round(_shi, 8)],
            approach_speed_pct=approach_speed_pct,
            red_candles_streak=red_candles_streak,
            phase_streak_checks=_hyst["phase_streak_checks"],
            phase_streak_seconds=_hyst["phase_streak_seconds"],
            bleed_in_last_12=_hyst["bleed_in_last_12"],
            seconds_since_bleed=_hyst["seconds_since_bleed"],
            phase_seq=_hyst["recent_seq"],
            hours_since_pump_peak=_live["hours_since_pump_peak"],
            vol_decay=_live["vol_decay"],
            natr_now_pct=_live["natr_now_pct"],
            natr_1m=_live["natr_1m"],
            natr_5m=_live["natr_5m"],
            natr_15m=_live["natr_15m"],
        )

        self._skip_counts["passed"] = self._skip_counts.get("passed", 0) + 1

        await self._log_decision(symbol, level, level_type, "pass",
                                 signal_group=signal_group, phase=phase.value,
                                 approach_count=approach_count, vol_falling=vol_falling,
                                 is_flip=is_flip, p_bounce=p_bounce)

        return True, params
