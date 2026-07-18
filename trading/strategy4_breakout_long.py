"""Strategy 4: Breakout Long — лонг при пробое уровня сопротивления вверх.

Полностью автономная стратегия: не зависит от monitor.py и других стратегий.
Сама сканирует уровни сопротивления и ведёт собственный мониторинг.

Условия входа:
  - Уровень сопротивления выше цены, в диапазоне atr * S4_RESISTANCE_SCAN_ATR_MAX
  - Цена пробила уровень (body_close > level)
  - Объём пробойной свечи >= S4_MIN_BREAKOUT_VOL_RATIO * avg_20
  - Предыдущая свеча тоже закрылась выше уровня (подтверждение, не zakol)
  - approach (касания снизу) >= S4_MIN_APPROACH_COUNT
  - btc_change_1m >= -S4_BTC_DROP_THRESHOLD (не входить против BTC)
  - нет sweep за последние S4_SWEEP_COOLDOWN_SECONDS сек
  - p_bounce (ML) <= S4_MAX_P_BOUNCE (уровень скорее пробьётся, чем отобьёт)

Управление позицией:
  - SL: entry - atr * S4_SL_ATR_MULT
  - TP1 (50%): entry + atr * S4_TP1_ATR_MULT → стоп в безубыток
  - TP2 (50%): entry + atr * S4_TP2_ATR_MULT
  - Трейлинг после TP1: current - atr * S4_TRAILING_ATR_MULT
  - Bounce-выход: если цена вернулась под уровень
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Optional

from trading.base_strategy import BaseStrategy
from trading.trade_log import open_trade, add_trade_event, get_open_trades
from bot.telegram import send_message, send_close_with_chart
from data.collector import candles_1m, candles_15m, get_delta
from analysis.level_builder import build_levels
from analysis.trigger import calculate_atr, get_btc_change_1m, _count_approaches
from logger import logger

# ── Константы стратегии ───────────────────────────────────────────────────────
# Все параметры здесь — легко менять для калибровки

# Вход
S4_MIN_BREAKOUT_VOL_RATIO      = 2.4    # минимальный объём пробойной свечи (×avg)
S4_MIN_APPROACH_COUNT          = 1      # минимум касаний снизу (resistance слабее support)
S4_MAX_P_BOUNCE                = 0.65   # ML: если вероятность отбоя > порога — пропустить
S4_BTC_DROP_THRESHOLD          = 0.002  # -0.2% btc_change_1m = не входить
S4_SWEEP_COOLDOWN_SECONDS      = 120    # пауза после sweep
S4_RESISTANCE_SCAN_ATR_MAX     = 5.0   # сканировать уровни не дальше N*ATR выше цены
S4_RESISTANCE_SCAN_ATR_MIN     = 0.3   # не брать уровни вплотную к цене
S4_MIN_LEVEL_STRENGTH          = 2      # минимальная сила уровня (0–5)
S4_ALLOWED_LEVEL_TYPES = {
    "breakout_level", "consolidation_base",
    "body_level", "wick_level", "order_block",
}

# Выход
S4_SL_ATR_MULT                 = 1.5   # стоп-лосс = entry - atr * mult
S4_TP1_ATR_MULT                = 1.5   # TP1 = entry + atr * mult
S4_TP2_ATR_MULT                = 3.0   # TP2 = entry + atr * mult
S4_TRAILING_ATR_MULT           = 1.5   # трейлинг стоп после TP1
S4_MIN_TRADE_DURATION_MINUTES  = 1.0   # не закрывать раньше (избегаем шум)
S4_BOUNCE_BACK_THRESHOLD       = 0.998 # close < level * threshold → breakout_failed

# Сканирование
S4_SCAN_INTERVAL_SECONDS       = 5     # как часто сканировать уровни
S4_CONFIRM_CANDLES             = 2     # сколько свечей подряд выше уровня = подтверждение

# Логирование — какие поля писать в entry_context для статистики
# (не влияют на логику входа, только на качество данных для калибровки)
S4_LOG_EXTRA_CONTEXT           = True


class Strategy4BreakoutLong(BaseStrategy):
    strategy_id   = 4
    strategy_name = "breakout_long"

    def __init__(self) -> None:
        super().__init__()
        # sweep cooldown: symbol → timestamp последнего sweep
        self._recent_sweep: dict[str, float] = {}
        # активные resistance-мониторы: symbol → set уровней
        self._monitored_levels: dict[str, set[float]] = {}
        # уже подтверждённые пробои (symbol, level) → чтобы не открывать дважды
        self._breakout_fired: set[tuple[str, float]] = set()
        # фоновый сканер запущен
        self._scanner_task: Optional[asyncio.Task] = None

    # ── Публичный интерфейс ───────────────────────────────────────────────────

    async def on_event(self, event: dict) -> None:
        """Обрабатываем только sweep — для sweep-cooldown."""
        if event.get("event_type") == "sweep":
            sym = event.get("symbol")
            if sym:
                self._recent_sweep[sym] = time.time()

    def start_scanner(self) -> None:
        """Запустить фоновый сканер уровней. Вызвать один раз из strategy_runner."""
        if self._scanner_task is None or self._scanner_task.done():
            self._scanner_task = asyncio.create_task(
                self._scan_loop(),
                name="s4_resistance_scanner",
            )

    # ── Сканер уровней ────────────────────────────────────────────────────────

    async def _scan_loop(self) -> None:
        """
        Каждые S4_SCAN_INTERVAL_SECONDS секунд:
        1. Берём все символы с открытыми свечами из candles_1m.
        2. Ищем resistance-уровни над ценой.
        3. Проверяем пробой.
        """
        while True:
            await asyncio.sleep(S4_SCAN_INTERVAL_SECONDS)
            try:
                symbols = list(candles_1m.keys())
                for symbol in symbols:
                    try:
                        await self._scan_symbol(symbol)
                    except Exception as e:
                        logger.debug("S4 scan error", symbol=symbol, error=str(e))
            except Exception as e:
                logger.error("S4 scan_loop error", error=str(e))

    async def _scan_symbol(self, symbol: str) -> None:
        c1m = candles_1m.get(symbol, [])
        if len(c1m) < 3:
            return

        current_price = c1m[-1]["close"]
        if current_price <= 0:
            return

        atr = calculate_atr(symbol)
        if atr <= 0:
            return

        levels = self._find_resistance_levels(symbol, current_price, atr)
        if not levels:
            return

        for lvl in levels:
            await self._check_level_breakout(symbol, c1m, lvl, atr, current_price)

    def _find_resistance_levels(
        self, symbol: str, current_price: float, atr: float
    ) -> list[dict]:
        """Найти уровни сопротивления выше цены в диапазоне ATR."""
        upper_bound = current_price + atr * S4_RESISTANCE_SCAN_ATR_MAX
        lower_bound = current_price + atr * S4_RESISTANCE_SCAN_ATR_MIN

        try:
            all_levels = build_levels(symbol)
        except Exception:
            return []

        result = []
        for lvl in all_levels:
            price = lvl.get("level", 0.0)
            if price <= lower_bound or price > upper_bound:
                continue
            level_type = lvl.get("type", "body_level")
            if level_type not in S4_ALLOWED_LEVEL_TYPES:
                continue
            strength = lvl.get("strength", 0)
            if strength < S4_MIN_LEVEL_STRENGTH:
                continue
            result.append({
                "level":    price,
                "type":     level_type,
                "strength": strength,
                "p_bounce": lvl.get("p_bounce", 0.5),
                "expected_depth": lvl.get("expected_depth", 0.0),
            })

        # Ближайшие первыми
        result.sort(key=lambda x: x["level"] - current_price)
        return result

    async def _check_level_breakout(
        self,
        symbol:        str,
        c1m:           list[dict],
        lvl:           dict,
        atr:           float,
        current_price: float,
    ) -> None:
        level      = lvl["level"]
        level_type = lvl["type"]
        level_key  = (symbol, round(level, 10))

        # Уже стреляли по этому уровню — пропустить
        if level_key in self._breakout_fired:
            return

        last  = c1m[-1]
        prev  = c1m[-2]

        body_close = last["close"]
        prev_close = prev["close"]

        # Пробой: текущая И предыдущая свеча закрылись выше уровня
        if body_close <= level or prev_close <= level:
            return

        # Объём пробойной свечи
        avg_vol = sum(c["volume"] for c in c1m[-20:]) / max(len(c1m[-20:]), 1)
        breakout_vol_ratio = last["volume"] / avg_vol if avg_vol > 0 else 1.0

        if breakout_vol_ratio < S4_MIN_BREAKOUT_VOL_RATIO:
            logger.debug(
                "S4 skip: vol_ratio=%.2f < %.2f",
                breakout_vol_ratio, S4_MIN_BREAKOUT_VOL_RATIO,
                symbol=symbol, level=level,
            )
            return

        # Касания снизу
        approach_count = _count_approaches(symbol, level, atr)[0]
        if approach_count < S4_MIN_APPROACH_COUNT:
            logger.debug(
                "S4 skip: approach=%d < %d",
                approach_count, S4_MIN_APPROACH_COUNT,
                symbol=symbol, level=level,
            )
            return

        # ML p_bounce
        p_bounce = lvl.get("p_bounce", 0.5)
        if p_bounce > S4_MAX_P_BOUNCE:
            logger.debug(
                "S4 skip: p_bounce=%.3f > %.3f",
                p_bounce, S4_MAX_P_BOUNCE,
                symbol=symbol, level=level,
            )
            return

        # BTC фильтр
        btc_change = get_btc_change_1m()
        if btc_change is not None and btc_change < -S4_BTC_DROP_THRESHOLD:
            logger.debug(
                "S4 skip: BTC drop btc_change=%.4f", btc_change,
                symbol=symbol,
            )
            return

        # Sweep cooldown
        last_sweep = self._recent_sweep.get(symbol, 0.0)
        if time.time() - last_sweep < S4_SWEEP_COOLDOWN_SECONDS:
            logger.debug("S4 skip: sweep cooldown", symbol=symbol)
            return

        # Проверка что можно открыть сделку
        if not await self._can_open_trade(symbol):
            return

        # Всё ок — отмечаем уровень как использованный и открываем
        self._breakout_fired.add(level_key)
        await self._open_trade(
            symbol, lvl, atr, current_price, breakout_vol_ratio,
            approach_count, btc_change, avg_vol,
        )

    # ── Открытие сделки ───────────────────────────────────────────────────────

    async def _open_trade(
        self,
        symbol:             str,
        lvl:                dict,
        atr:                float,
        entry_price:        float,
        breakout_vol_ratio: float,
        approach_count:     int,
        btc_change:         Optional[float],
        avg_vol:            float,
    ) -> None:
        level      = lvl["level"]
        level_type = lvl["type"]
        strength   = lvl["strength"]
        p_bounce   = lvl.get("p_bounce", 0.5)

        stop_loss    = entry_price - atr * S4_SL_ATR_MULT
        take_profit_1 = entry_price + atr * S4_TP1_ATR_MULT
        take_profit_2 = entry_price + atr * S4_TP2_ATR_MULT

        trade_id = str(uuid.uuid4())
        trade = {
            "trade_id":               trade_id,
            "strategy_id":            self.strategy_id,
            "strategy_name":          self.strategy_name,
            "symbol":                 symbol,
            "level":                  level,
            "level_type":             level_type,
            "level_side":             "resistance",
            "entry_signal":           "breakout",
            "strength_at_entry":      strength,
            "p_bounce_at_entry":      p_bounce,
            "expected_depth_at_entry": lvl.get("expected_depth", 0.0),
            "approach_style":         "unknown",  # заполнится в entry_context
            "vol_ratio_at_entry":     round(breakout_vol_ratio, 2),
            "atr_at_entry":           atr,
            "entry_price":            entry_price,
            "entry_time":             time.time(),
            "position_size":          self.POSITION_SIZE_USDT,
            "direction":              "long",
            "grid_orders_json":       None,
            "grid_fill_count":        None,
        }

        await open_trade(trade)

        params_note = json.dumps({
            "stop_loss":              round(stop_loss, 10),
            "take_profit_1":          round(take_profit_1, 10),
            "take_profit_2":          round(take_profit_2, 10),
            "tp1_hit":                False,
            "stop_moved_to_breakeven": False,
            "breakout_vol_ratio":     round(breakout_vol_ratio, 2),
        })
        await add_trade_event(trade_id, "params_set", entry_price, params_note)

        # ── Entry context — максимум полей для статистики ─────────────────────
        if S4_LOG_EXTRA_CONTEXT:
            await self._log_entry_context(
                trade_id, symbol, level, atr, entry_price,
                breakout_vol_ratio, approach_count, btc_change, avg_vol, p_bounce,
            )

        await self._send_open_message(trade, stop_loss, take_profit_1, take_profit_2)

        logger.info(
            "S4 trade opened",
            trade_id=trade_id,
            symbol=symbol,
            level=round(level, 10),
            entry=entry_price,
            sl=round(stop_loss, 10),
            tp1=round(take_profit_1, 10),
            tp2=round(take_profit_2, 10),
            vol_ratio=round(breakout_vol_ratio, 2),
            approach=approach_count,
            p_bounce=round(p_bounce, 3),
            atr=round(atr, 10),
        )

    async def _log_entry_context(
        self,
        trade_id:           str,
        symbol:             str,
        level:              float,
        atr:                float,
        entry_price:        float,
        breakout_vol_ratio: float,
        approach_count:     int,
        btc_change:         Optional[float],
        avg_vol:            float,
        p_bounce:           float,
    ) -> None:
        """Логировать расширенный контекст входа для последующей калибровки."""
        try:
            c1m = candles_1m.get(symbol, [])

            # Дельта
            try:
                delta_data = get_delta(symbol)
                delta_at_entry = round(delta_data.get("delta", 0.0), 4)
                buy_vol  = round(delta_data.get("buy_vol", 0.0), 4)
                sell_vol = round(delta_data.get("sell_vol", 0.0), 4)
            except Exception:
                delta_at_entry = buy_vol = sell_vol = 0.0

            # Параметры пробойной свечи
            candle_range       = 0.0
            candle_body_ratio  = 0.0
            candle_upper_wick  = 0.0
            candle_lower_wick  = 0.0
            if c1m:
                last = c1m[-1]
                candle_range = last["high"] - last["low"]
                if candle_range > 0:
                    body = abs(last["close"] - last["open"])
                    candle_body_ratio = round(body / candle_range, 4)
                    upper_wick = last["high"] - max(last["close"], last["open"])
                    lower_wick = min(last["close"], last["open"]) - last["low"]
                    candle_upper_wick = round(upper_wick / candle_range, 4)
                    candle_lower_wick = round(lower_wick / candle_range, 4)

            # Насколько далеко вошли выше уровня (проскальзывание)
            entry_overshoot_pct = round((entry_price - level) / level * 100, 4) if level > 0 else 0.0

            # Расстояние от уровня до SL/TP в % и ATR
            sl_dist_pct  = round((entry_price - (entry_price - atr * S4_SL_ATR_MULT)) / entry_price * 100, 4)
            tp1_dist_pct = round((atr * S4_TP1_ATR_MULT) / entry_price * 100, 4)
            tp2_dist_pct = round((atr * S4_TP2_ATR_MULT) / entry_price * 100, 4)

            # Объём предыдущих свечей (тренд объёма перед пробоем)
            vol_3_candles = [
                round(c1m[-i]["volume"] / avg_vol, 2) if avg_vol > 0 else 1.0
                for i in range(1, min(4, len(c1m)))
            ]

            # 15m контекст
            c15m = candles_15m.get(symbol, [])
            atr_15m_ratio = 0.0
            trend_candles_15m = 0
            if c15m and len(c15m) >= 5:
                highs_15m = [c["close"] for c in c15m[-5:]]
                trend_candles_15m = sum(
                    1 for i in range(1, len(highs_15m))
                    if highs_15m[i] > highs_15m[i - 1]
                )
                tr_15m = [c["high"] - c["low"] for c in c15m[-14:]]
                atr_15m = sum(tr_15m) / len(tr_15m) if tr_15m else 0.0
                atr_15m_ratio = round(atr_15m / entry_price * 100, 4) if entry_price > 0 else 0.0

            # Попытка получить approach_style
            try:
                from analysis.trigger import detect_approach_style as _das
                approach_style = _das(symbol)
            except Exception:
                approach_style = "unknown"

            # Обновить поле в trade dict (approach_style)
            try:
                await add_trade_event(trade_id, "approach_style_update", entry_price,
                                      json.dumps({"approach_style": approach_style}))
            except Exception:
                pass

            ctx = {
                # Уровень и пробой
                "approach_count":        approach_count,
                "p_bounce":              round(p_bounce, 4),
                "breakout_vol_ratio":    round(breakout_vol_ratio, 2),
                "entry_overshoot_pct":   entry_overshoot_pct,
                "approach_style":        approach_style,

                # Свеча пробоя
                "candle_body_ratio":     candle_body_ratio,
                "candle_upper_wick":     candle_upper_wick,
                "candle_lower_wick":     candle_lower_wick,
                "candle_range_pct":      round(candle_range / entry_price * 100, 4) if entry_price > 0 else 0.0,

                # Объём
                "vol_ratio_candles":     vol_3_candles,  # [текущая, -1, -2]

                # Дельта
                "delta_at_entry":        delta_at_entry,
                "buy_vol":               buy_vol,
                "sell_vol":              sell_vol,

                # BTC
                "btc_change_1m":         btc_change,

                # Расстояния TP/SL
                "sl_dist_pct":           sl_dist_pct,
                "tp1_dist_pct":          tp1_dist_pct,
                "tp2_dist_pct":          tp2_dist_pct,
                "rr_tp1":                round(tp1_dist_pct / sl_dist_pct, 2) if sl_dist_pct > 0 else 0.0,
                "rr_tp2":                round(tp2_dist_pct / sl_dist_pct, 2) if sl_dist_pct > 0 else 0.0,

                # 15m контекст
                "atr_15m_ratio":         atr_15m_ratio,
                "trend_candles_15m":     trend_candles_15m,
            }

            await add_trade_event(trade_id, "entry_context", entry_price, json.dumps(ctx))

        except Exception as e:
            logger.warning("S4 entry_context logging failed", trade_id=trade_id, error=str(e))

    # ── Управление позицией ───────────────────────────────────────────────────

    async def _check_exit(self, trade: dict, current_price: float) -> None:
        trade_id    = trade["trade_id"]
        entry_price = trade["entry_price"]

        age_minutes = (time.time() - trade["entry_time"]) / 60
        if age_minutes < S4_MIN_TRADE_DURATION_MINUTES:
            return

        params = self._extract_params(trade)
        if params is None:
            logger.warning(
                "S4 _check_exit: params not found",
                trade_id=trade_id,
                events_preview=(trade.get("events_json") or "")[:200],
            )
            return

        stop_loss     = params["stop_loss"]
        take_profit_1 = params["take_profit_1"]
        take_profit_2 = params["take_profit_2"]
        tp1_hit       = params.get("tp1_hit", False)
        stop_moved    = params.get("stop_moved_to_breakeven", False)
        level         = trade.get("level", entry_price)

        effective_stop = entry_price if stop_moved else stop_loss

        # TP2
        if current_price >= take_profit_2:
            avg_exit = (take_profit_1 + take_profit_2) / 2 if tp1_hit else take_profit_2
            await self._close_and_track(trade_id, trade["symbol"], avg_exit, "take_profit_2")
            await self._send_close_message(trade, avg_exit, "take_profit_2")
            return

        # TP1
        if not tp1_hit and current_price >= take_profit_1:
            params["tp1_hit"]                = True
            params["stop_moved_to_breakeven"] = True
            await add_trade_event(
                trade_id, "tp1_hit", current_price,
                json.dumps({"partial_exit_price": current_price, "partial_exit_pct": 50})
            )
            await add_trade_event(trade_id, "params_updated", current_price, json.dumps(params))
            logger.info("S4 TP1 hit, stop → breakeven", trade_id=trade_id)
            return

        # Трейлинг после TP1
        if tp1_hit:
            atr = trade.get("atr_at_entry") or 0.0
            if atr > 0:
                new_trailing = current_price - atr * S4_TRAILING_ATR_MULT
                if new_trailing > effective_stop:
                    params["stop_loss"]               = round(new_trailing, 10)
                    params["stop_moved_to_breakeven"]  = True
                    await add_trade_event(
                        trade_id, "trailing_stop_updated", current_price,
                        json.dumps({"new_stop": round(new_trailing, 10), "atr": atr})
                    )
                    await add_trade_event(trade_id, "params_updated", current_price, json.dumps(params))

        # Bounce back — пробой не подтвердился (цена вернулась под уровень)
        if current_price < level * S4_BOUNCE_BACK_THRESHOLD and not tp1_hit:
            await self._close_and_track(trade_id, trade["symbol"], current_price, "breakout_failed_bounce")
            await self._send_close_message(trade, current_price, "breakout_failed_bounce")
            logger.info("S4 closed: breakout_failed_bounce", trade_id=trade_id, level=level)
            return

        # Stop loss
        if current_price <= effective_stop:
            if tp1_hit:
                avg_exit = (take_profit_1 + entry_price) / 2
                await self._close_and_track(trade_id, trade["symbol"], avg_exit, "stop_loss")
                await self._send_close_message(trade, avg_exit, "stop_loss")
            else:
                await self._close_and_track(trade_id, trade["symbol"], current_price, "stop_loss")
                await self._send_close_message(trade, current_price, "stop_loss")

    # ── Вспомогательные ───────────────────────────────────────────────────────

    def _extract_params(self, trade: dict) -> Optional[dict]:
        try:
            events = json.loads(trade.get("events_json") or "[]")
        except Exception:
            return None
        for ev in reversed(events):
            if ev.get("type") in ("params_updated", "params_set"):
                try:
                    return json.loads(ev["note"])
                except Exception:
                    return None
        return None

    # ── Telegram ──────────────────────────────────────────────────────────────

    async def _send_open_message(
        self, trade: dict, stop_loss: float, tp1: float, tp2: float
    ) -> None:
        ep  = trade["entry_price"]
        sl_pct  = self._format_pct((stop_loss - ep) / ep * 100)
        tp1_pct = self._format_pct((tp1 - ep) / ep * 100)
        tp2_pct = self._format_pct((tp2 - ep) / ep * 100)

        vol_ratio  = trade.get("vol_ratio_at_entry", 0.0)
        p_bounce   = trade.get("p_bounce_at_entry", 0.0)
        strength   = trade.get("strength_at_entry", 0)

        # Достать approach_count из entry_context
        approach = 0
        try:
            events = json.loads(trade.get("events_json") or "[]")
            for ev in reversed(events):
                if ev.get("type") == "entry_context":
                    ctx = json.loads(ev.get("note", "{}"))
                    approach = ctx.get("approach_count", 0)
                    break
        except Exception:
            pass

        text = (
            f"🟢 [S4 Breakout Long] {trade['symbol']} LONG\n"
            f"   Пробой resistance: {trade['level']} ({trade['level_type']})\n"
            f"   Объём: ×{vol_ratio:.1f} | Касаний: {approach} | p_bounce: {p_bounce:.2f} | strength: {strength}\n"
            f"   Вход: {ep}\n"
            f"   SL: {round(stop_loss, 8)} ({sl_pct})"
            f" | TP1: {round(tp1, 8)} ({tp1_pct})"
            f" | TP2: {round(tp2, 8)} ({tp2_pct})\n"
            f"   Позиция: {int(self.POSITION_SIZE_USDT)} USDT"
        )
        try:
            await send_message(text)
        except Exception as e:
            logger.error("S4 send_open_message failed", error=str(e))

    async def _send_close_message(self, trade: dict, exit_price: float, reason: str) -> None:
        ep      = trade["entry_price"]
        pnl_pct  = (exit_price - ep) / ep * 100
        pnl_usdt = self.POSITION_SIZE_USDT * pnl_pct / 100
        icon     = "✅" if pnl_pct >= 0 else "🔴"

        max_fav   = trade.get("max_favorable_pct") or 0.0
        max_adv   = trade.get("max_adverse_pct")   or 0.0
        max_profit_usdt = self.POSITION_SIZE_USDT * max_fav / 100
        max_loss_usdt   = self.POSITION_SIZE_USDT * max_adv / 100

        text = (
            f"{icon} [S4 Breakout Long] {trade['symbol']} закрыт\n"
            f"   Причина: {reason}\n"
            f"   Вход: {ep} → Выход: {exit_price}\n"
            f"   PnL: {self._format_pct(pnl_pct)} ({self._format_pct(pnl_usdt, sign=True)} USDT)"
            f" | Время: {self._format_duration(trade['entry_time'])}\n"
            f"   📈 Max profit: +{max_fav:.2f}% (+{max_profit_usdt:.2f} USDT)\n"
            f"   📉 Max drawdown: -{max_adv:.2f}% (-{max_loss_usdt:.2f} USDT)"
        )
        try:
            await send_close_with_chart(
                text, trade["symbol"],
                entry_price=trade["entry_price"],
                exit_price=exit_price,
                level=trade.get("level"),
            )
        except Exception as e:
            logger.error("S4 send_close_message failed", error=str(e))
