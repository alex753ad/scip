"""Strategy 3: Breakout Momentum — short при подтверждённом пробое поддержки."""

from __future__ import annotations

import json
import time
import uuid

from trading.base_strategy import BaseStrategy
from trading.trade_log import open_trade, add_trade_event, get_open_trades
from bot.telegram import send_message, send_close_with_chart
from constants import (
    S3_MIN_BREAKOUT_VOL_RATIO,
    S3_MIN_BREAKOUT_VOL_RATIO_STRONG,
    S3_SWEEP_COOLDOWN_SECONDS,
    S3_TP1_ATR_MULT,
    S3_TP2_ATR_MULT,
    S3_SL_ATR_MULT,
    S3_MIN_TRADE_DURATION_MINUTES,
    S3_MIN_STRENGTH,       # новый: минимальный strength для входа (рекомендация: 4)
    S3_MAX_NATR_5M,        # новый: максимальный NATR 5м для входа (рекомендация: 0.03)
)
from data.collector import candles_1m, candles_5m, get_delta
from logger import logger

# Trailing stop до TP1 (аналогично S1, адаптировано для short).
# Активируется когда цена прошла вниз >= S3_TRAILING_ACTIVATE_PCT от входа.
# Стоп = текущий минимум + S3_TRAILING_OFFSET_PCT (для short: стоп выше минимума).
S3_TRAILING_ACTIVATE_PCT = 0.005   # 0.5% favorable move для активации
S3_TRAILING_OFFSET_PCT   = 0.003   # стоп = low_peak * (1 + offset)


class Strategy3Breakout(BaseStrategy):
    strategy_id = 3
    strategy_name = "breakout"

    def __init__(self) -> None:
        super().__init__()  # FIX BUG-1: создаёт _tracker_tasks, иначе AttributeError при _close_and_track
        # symbol → timestamp последнего события "sweep"
        self._recent_sweep: dict[str, float] = {}

    # ── Вход ──────────────────────────────────────────────────────────

    async def on_event(self, event: dict) -> None:
        event_type = event.get("event_type")

        # FIX BUG-2: два блока sweep объединены — второй (_handle_sweep_warning) никогда не достигался
        if event_type == "sweep":
            self._recent_sweep[event["symbol"]] = time.time()
            await self._handle_sweep_warning(event)  # проверяет открытые сделки внутри
            return

        if event_type == "breakout":
            await self._try_open(event)
            return

        # Bounce по уровню — признак ложного пробоя, закрыть short
        if event_type == "bounce":
            await self._handle_bounce(event)

    async def _try_open(self, event: dict) -> None:
        symbol = event["symbol"]
        breakout_vol_ratio = event.get("breakout_vol_ratio", 0.0)
        level_side = event.get("level_side", "")
        level_type = event.get("level_type", "")

        if breakout_vol_ratio < S3_MIN_BREAKOUT_VOL_RATIO_STRONG:
            return
        if level_side != "support":
            return

        # Задача 1: фильтр по strength — входить только при strength >= S3_MIN_STRENGTH (рекомендация: 4)
        strength = event.get("strength", 0)
        if strength < S3_MIN_STRENGTH:
            logger.debug("S3 skip: strength below threshold", symbol=symbol, strength=strength, threshold=S3_MIN_STRENGTH)
            return

        # Задача 2+5: исключить pump_base и consolidation_base полностью
        if level_type == "pump_base":
            logger.debug("S3 skip: pump_base level excluded since 09.06", symbol=symbol)
            return
        if level_type == "consolidation_base":
            logger.debug("S3 skip: consolidation_base level excluded", symbol=symbol)
            return

        # Не входить в short если BTC растёт в эту минуту (контртренд).
        btc_change = event.get("btc_change_1m")
        if btc_change is not None and btc_change > 0.002:  # BTC +0.2% за минуту
            logger.debug(
                "S3 skip: BTC counter-trend on breakout",
                symbol=symbol, btc_change=btc_change,
            )
            return

        # Не торговать если был sweep незадолго до пробоя (ложный пробой)
        last_sweep = self._recent_sweep.get(symbol, 0.0)
        if time.time() - last_sweep < S3_SWEEP_COOLDOWN_SECONDS:
            return

        # Задача 4: вычислить NATR 5м (Normalized ATR = ATR/close * 100)
        # Логируется всегда; используется как фильтр входа при S3_MAX_NATR_5M > 0.
        natr_5m = self._calc_natr_5m(symbol)

        if S3_MAX_NATR_5M > 0 and natr_5m is not None and natr_5m > S3_MAX_NATR_5M:
            logger.debug(
                "S3 skip: NATR_5m above threshold",
                symbol=symbol, natr_5m=round(natr_5m, 5), threshold=S3_MAX_NATR_5M,
            )
            return

        if not await self._can_open_trade(symbol):
            return

        entry_price = event["current_price"]
        level = event["level"]
        atr = event.get("atr", 0.0)

        # FIX-SL: SL считается от entry_price, а не от level.
        # Раньше: level + 0.5*ATR → SL был в ~0.5% от level, но entry на 2-7% ниже,
        # итого реальный риск ~3-8% от entry вместо задуманных ~0.5%.
        stop_loss = entry_price + atr * S3_SL_ATR_MULT
        take_profit_1 = entry_price - atr * S3_TP1_ATR_MULT
        take_profit_2 = entry_price - atr * S3_TP2_ATR_MULT

        is_strong_breakout = breakout_vol_ratio >= S3_MIN_BREAKOUT_VOL_RATIO_STRONG

        trade_id = str(uuid.uuid4())
        trade = {
            "trade_id": trade_id,
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "symbol": symbol,
            "level": level,
            "level_type": level_type,
            "level_side": level_side,
            "entry_signal": "breakout",
            "strength_at_entry": strength,
            "p_bounce_at_entry": event.get("p_bounce", 0.0),
            "expected_depth_at_entry": event.get("expected_depth", 0.0),
            "approach_style": event.get("approach_style", "unknown"),
            "vol_ratio_at_entry": breakout_vol_ratio,
            "atr_at_entry": atr,
            "entry_price": entry_price,
            "entry_time": time.time(),
            "position_size": self.POSITION_SIZE_USDT,
            "direction": "short",
            "grid_orders_json": None,
            "grid_fill_count": None,
        }

        await open_trade(trade)

        params_note = json.dumps({
            "stop_loss": round(stop_loss, 8),
            "take_profit_1": round(take_profit_1, 8),
            "take_profit_2": round(take_profit_2, 8),
            "tp1_hit": False,
            "stop_moved_to_breakeven": False,
            # Задача 3: trailing-поля (аналог S1, адаптирован для short)
            "trailing_active": False,
            "trailing_low": None,     # минимум цены с момента активации (для short: тянем вниз)
            "trailing_stop": None,
            "breakout_vol_ratio": breakout_vol_ratio,
            "is_strong_breakout": is_strong_breakout,
        })
        await add_trade_event(trade_id, "params_set", entry_price, params_note)

        # Записываем контекст входа для последующего анализа ложных пробоев.
        # delta_at_entry: агрессия продавца в момент пробоя (отрицательная = продавцы доминировали).
        # candle_body_ratio: отношение тела к диапазону последней 1М свечи (0 = закол, 1 = чистое тело).
        # natr_5m_at_entry: нормализованный ATR на 5М (задача 4) — логируется на всех сделках.
        try:
            delta_data = get_delta(symbol)
            delta_at_entry = round(delta_data.get("delta", 0.0), 4)

            c1m = candles_1m.get(symbol, [])
            if c1m:
                last_c = c1m[-1]
                candle_range = last_c["high"] - last_c["low"]
                if candle_range > 0:
                    candle_body_ratio = round(abs(last_c["close"] - last_c["open"]) / candle_range, 4)
                else:
                    candle_body_ratio = 0.0
            else:
                candle_body_ratio = 0.0

            trades_count = delta_data.get("trades", 0)
            trades_per_min = trades_count * 2  # буфер 30 сек → пересчёт в минуту

            await add_trade_event(
                trade_id, "entry_context", entry_price,
                json.dumps({
                    "delta_at_entry": delta_at_entry,
                    "candle_body_ratio": candle_body_ratio,
                    "trades_per_min": trades_per_min,
                    "natr_5m_at_entry": round(natr_5m, 6) if natr_5m is not None else None,
                })
            )
        except Exception as e:
            logger.warning("S3 entry_context logging failed", trade_id=trade_id, error=str(e))

        await self._send_open_message(trade, stop_loss, take_profit_1, take_profit_2, natr_5m=natr_5m)

        logger.info(
            "S3 trade opened",
            trade_id=trade_id,
            symbol=symbol,
            entry=entry_price,
            sl=round(stop_loss, 8),
            tp1=round(take_profit_1, 8),
            tp2=round(take_profit_2, 8),
            vol_ratio=breakout_vol_ratio,
            natr_5m=round(natr_5m, 6) if natr_5m is not None else None,
        )

    # ── Сопровождение ─────────────────────────────────────────────────

    async def _check_exit(self, trade: dict, current_price: float) -> None:
        trade_id = trade["trade_id"]
        entry_price = trade["entry_price"]

        # Защита: events_json может быть None если БД вернула NULL
        if trade.get("events_json") is None:
            trade["events_json"] = "[]"

        age_minutes = (time.time() - trade["entry_time"]) / 60
        if age_minutes < S3_MIN_TRADE_DURATION_MINUTES:
            return

        params = self._extract_params(trade)
        if params is None:
            logger.warning(
                "S3 _check_exit: params not found in events_json",
                trade_id=trade_id,
                events_json_preview=(trade.get("events_json") or "")[:200],
            )
            return

        stop_loss = params["stop_loss"]
        take_profit_1 = params["take_profit_1"]
        take_profit_2 = params["take_profit_2"]
        tp1_hit = params.get("tp1_hit", False)
        stop_moved = params.get("stop_moved_to_breakeven", False)
        trailing_active = params.get("trailing_active", False)
        trailing_low = params.get("trailing_low")
        trailing_stop = params.get("trailing_stop")

        # Short: стоп выше цены входа; после TP1 — на уровне безубытка
        effective_stop = entry_price if stop_moved else stop_loss

        # TP2 — цена ушла достаточно вниз
        if current_price <= take_profit_2:
            avg_exit = (take_profit_1 + take_profit_2) / 2 if tp1_hit else take_profit_2
            await self._close_and_track(trade_id, trade["symbol"], avg_exit, "take_profit_2")
            await self._send_close_message(trade, avg_exit, "take_profit_2")
            return

        # TP1 — частичная фиксация, стоп → безубыток, trailing снимается
        if not tp1_hit and current_price <= take_profit_1:
            params["tp1_hit"] = True
            params["stop_moved_to_breakeven"] = True
            params["trailing_active"] = False  # trailing снимается, стоп уходит в breakeven
            params["trailing_low"] = None
            params["trailing_stop"] = None
            await add_trade_event(
                trade_id, "tp1_hit", current_price,
                json.dumps({"partial_exit_price": current_price, "partial_exit_pct": 50})
            )
            await add_trade_event(trade_id, "params_updated", current_price, json.dumps(params))
            logger.info("S3 TP1 hit, stop moved to breakeven", trade_id=trade_id)
            return

        # ── Trailing stop до TP1 (аналог S1, адаптирован для short) ──────────
        # Для short: пик движения = минимум цены (low); стоп = low_peak * (1 + offset)
        if not tp1_hit:
            _c5m = candles_5m.get(trade["symbol"], [])
            _last_low = min((c["low"] for c in _c5m[-2:]), default=current_price)
            fav_pct = (entry_price - _last_low) / entry_price  # >0 если цена упала

            if trailing_active and trailing_low is not None:
                # Обновить пик если low ушёл ниже
                if _last_low < trailing_low:
                    trailing_low = _last_low
                    trailing_stop = round(trailing_low * (1.0 + S3_TRAILING_OFFSET_PCT), 8)
                    params["trailing_low"] = trailing_low
                    params["trailing_stop"] = trailing_stop
                    await add_trade_event(
                        trade_id, "params_updated", current_price, json.dumps(params)
                    )
                    logger.debug(
                        "S3 trailing low updated",
                        trade_id=trade_id, low=trailing_low, trailing_stop=trailing_stop,
                    )

                # Срабатывание trailing stop: цена вернулась выше стопа
                if trailing_stop is not None and current_price >= trailing_stop:
                    await self._close_and_track(trade_id, trade["symbol"], current_price, "trailing_stop")
                    await self._send_close_message(trade, current_price, "trailing_stop")
                    return

            elif fav_pct >= S3_TRAILING_ACTIVATE_PCT:
                # Активировать trailing
                trailing_low = _last_low
                trailing_stop = round(trailing_low * (1.0 + S3_TRAILING_OFFSET_PCT), 8)
                params["trailing_active"] = True
                params["trailing_low"] = trailing_low
                params["trailing_stop"] = trailing_stop
                await add_trade_event(
                    trade_id, "params_updated", current_price, json.dumps(params)
                )
                logger.info(
                    "S3 trailing activated",
                    trade_id=trade_id, low=trailing_low, trailing_stop=trailing_stop,
                )
                return

        # Stop loss (для short: цена ушла вверх выше стопа)
        if current_price >= effective_stop:
            if tp1_hit:
                avg_exit = (take_profit_1 + entry_price) / 2
                await self._close_and_track(trade_id, trade["symbol"], avg_exit, "stop_loss")
                await self._send_close_message(trade, avg_exit, "stop_loss")
            else:
                await self._close_and_track(trade_id, trade["symbol"], current_price, "stop_loss")
                await self._send_close_message(trade, current_price, "stop_loss")

    async def _handle_bounce(self, event: dict) -> None:
        """Bounce по тому же уровню = пробой не подтвердился, закрыть short."""
        trades = await get_open_trades(self.strategy_id)
        for trade in trades:
            if trade["symbol"] != event["symbol"]:
                continue
            if abs(trade["level"] - event["level"]) / max(trade["level"], 1) > 0.005:
                continue
            current_price = event["current_price"]
            await self._close_and_track(trade["trade_id"], trade["symbol"], current_price, "breakout_failed_bounce")
            await self._send_close_message(trade, current_price, "breakout_failed_bounce")
            logger.info(
                "S3 trade closed — breakout failed (bounce)",
                trade_id=trade["trade_id"],
            )

    async def _handle_sweep_warning(self, event: dict) -> None:
        """Sweep после открытия — записать предупреждение, не закрывать."""
        trades = await get_open_trades(self.strategy_id)
        for trade in trades:
            if trade["symbol"] != event["symbol"]:
                continue
            await add_trade_event(
                trade["trade_id"], "sweep_warning", event["current_price"],
                f"sweep detected after entry, vol_ratio={event.get('sweep_vol_ratio', 0)}"
            )
            logger.info("S3 sweep warning recorded", trade_id=trade["trade_id"])

    # ── Вспомогательные ───────────────────────────────────────────────

    def _extract_params(self, trade: dict) -> dict | None:
        try:
            events = json.loads(trade.get("events_json") or "[]")
        except Exception:
            return None
        for ev in reversed(events):
            if ev["type"] in ("params_updated", "params_set"):
                try:
                    return json.loads(ev["note"])
                except Exception:
                    return None
        return None

    def _calc_natr_5m(self, symbol: str) -> float | None:
        """NATR 5М = ATR(14) / close * 100. Использует candles_5m из data.collector.
        Возвращает None если данных недостаточно (< 15 свечей).
        Логировать на всех сделках независимо от фильтра.
        """
        candles = candles_5m.get(symbol, [])
        if len(candles) < 15:
            return None
        # ATR(14): среднее true range по последним 14 периодам
        trs = []
        for i in range(-14, 0):
            c = candles[i]
            prev_close = candles[i - 1]["close"]
            tr = max(
                c["high"] - c["low"],
                abs(c["high"] - prev_close),
                abs(c["low"] - prev_close),
            )
            trs.append(tr)
        atr14 = sum(trs) / len(trs)
        close = candles[-1]["close"]
        if close <= 0:
            return None
        return atr14 / close * 100

    # ── Telegram ──────────────────────────────────────────────────────

    async def _send_open_message(
        self, trade: dict, stop_loss: float, tp1: float, tp2: float, natr_5m: float | None = None
    ) -> None:
        ep = trade["entry_price"]
        # Short: SL выше входа (+), TP ниже входа (-)
        sl_pct = self._format_pct((stop_loss - ep) / ep * 100)
        tp1_pct = self._format_pct((tp1 - ep) / ep * 100)
        tp2_pct = self._format_pct((tp2 - ep) / ep * 100)
        params = self._extract_params(trade) or {}
        vol_ratio = params.get("breakout_vol_ratio", trade.get("vol_ratio_at_entry", 0.0))
        natr_str = f" | NATR5m={natr_5m:.4f}%" if natr_5m is not None else ""
        text = (
            f"🔴 [S3 Breakout] {trade['symbol']} SHORT\n"
            f"   Пробой уровня: {trade['level']} ({trade['level_type']})"
            f" | Объём: ×{vol_ratio:.1f}\n"
            f"   Вход: {ep} | strength={trade['strength_at_entry']}{natr_str}\n"
            f"   SL: {round(stop_loss, 8)} ({sl_pct})"
            f" | TP1: {round(tp1, 8)} ({tp1_pct})"
            f" | TP2: {round(tp2, 8)} ({tp2_pct})\n"
            f"   Позиция: {int(self.POSITION_SIZE_USDT)} USDT"
        )
        try:
            await send_message(text)
        except Exception as e:
            logger.error("S3 send_open_message failed", error=str(e))

    async def _send_close_message(self, trade: dict, exit_price: float, reason: str) -> None:
        ep = trade["entry_price"]
        # Short: прибыль если цена упала
        pnl_pct = (ep - exit_price) / ep * 100
        pnl_usdt = self.POSITION_SIZE_USDT * pnl_pct / 100
        icon = "✅" if pnl_pct >= 0 else "🔴"
        max_fav  = trade.get("max_favorable_pct") or 0.0
        max_adv  = trade.get("max_adverse_pct") or 0.0
        max_profit_usdt = self.POSITION_SIZE_USDT * max_fav / 100
        max_loss_usdt   = self.POSITION_SIZE_USDT * max_adv / 100
        text = (
            f"{icon} [S3 Breakout] {trade['symbol']} закрыт\n"
            f"   Причина: {reason}\n"
            f"   Вход: {ep} → Выход: {exit_price}\n"
            f"   PnL: {self._format_pct(pnl_pct)} ({self._format_pct(pnl_usdt, sign=True)} USDT)"
            f" | Время: {self._format_duration(trade['entry_time'])}\n"
            f"   📈 Max profit: +{max_fav:.2f}% (+{max_profit_usdt:.2f} USDT)\n"
            f"   📉 Max drawdown: -{max_adv:.2f}% (-{max_loss_usdt:.2f} USDT)"
        )
        try:
            await send_close_with_chart(text, trade["symbol"],
                entry_price=trade["entry_price"], exit_price=exit_price, level=trade.get("level"))
        except Exception as e:
            logger.error("S3 send_close_message failed", error=str(e))
