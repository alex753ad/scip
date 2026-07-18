"""Strategy 2: Limit Grid — 5 лимитных ордеров в зоне уровня."""

from __future__ import annotations

import json
import time
import uuid

import aiosqlite

from trading.base_strategy import BaseStrategy
from trading.trade_log import open_trade, add_trade_event, get_open_trades, DB_PATH
from bot.telegram import send_message, send_close_with_chart
from constants import (
    S2_MIN_STRENGTH,
    S2_MIN_P_BOUNCE,
    S2_PRESSURE_COOLDOWN_SECONDS,
    S2_GRID_ORDERS,
    S2_POSITION_SIZE_USDT,
)
from data.collector import candles_1m
from trading.event_bus import publish as _publish
from logger import logger

# Trailing stop: отступ от пика после TP1
S2_TRAILING_PCT = 0.005   # 0.5%

# TP2 как множитель ATR от entry (явный, не через grid_bottom)
S2_TP2_ATR_MULT = 5.0

# Full-grid TP: при fill=10 ставим TP на этом % ниже уровня (возврат к уровню снизу)
S2_FULL_GRID_TP_PCT = 0.0015   # 0.15%


class Strategy2LimitGrid(BaseStrategy):
    strategy_id = 2
    strategy_name = "limit_grid"

    def __init__(self) -> None:
        super().__init__()  # FIX BUG-1: создаёт _tracker_tasks, иначе AttributeError при _close_and_track
        # symbol → timestamp последнего события "pressure"
        self._recent_pressure: dict[str, float] = {}
        # "symbol:level" 2192 timestamp 043f043e0441043b04350434043d04350433043e 04370430043a0440044b04420438044f 044104340435043b043a0438 043f043e 044d0442043e043c0443 04430440043e0432043d044e
        self._recent_close: dict[str, float] = {}

    # ── Вход ──────────────────────────────────────────────────────────

    async def on_event(self, event: dict) -> None:
        event_type = event.get("event_type")

        if event_type == "pressure":
            self._recent_pressure[event["symbol"]] = time.time()
            return

        if event_type == "proximity":
            await self._try_open(event)
            return

        if event_type == "breakout":
            await self._handle_breakout(event)
            return

    async def _try_open(self, event: dict) -> None:
        symbol = event["symbol"]
        strength = event.get("strength", 0)
        p_bounce = event.get("p_bounce", 0.0)
        approach_style = event.get("approach_style", "unknown")

        if strength < S2_MIN_STRENGTH:
            logger.info(
                "S2 skip: strength too low",
                symbol=symbol,
                strength=strength,
                min_required=S2_MIN_STRENGTH,
            )
            return
        if p_bounce < S2_MIN_P_BOUNCE:
            logger.info(
                "S2 skip: p_bounce too low",
                symbol=symbol,
                p_bounce=round(p_bounce, 3),
                min_required=S2_MIN_P_BOUNCE,
            )
            return
        if approach_style == "bleed":
            logger.info(
                "S2 skip: approach_style=bleed",
                symbol=symbol,
            )
            return

        # Нет давления за последние N секунд
        last_pressure = self._recent_pressure.get(symbol, 0.0)
        seconds_since_pressure = time.time() - last_pressure
        if seconds_since_pressure < S2_PRESSURE_COOLDOWN_SECONDS:
            logger.info(
                "S2 skip: pressure cooldown active",
                symbol=symbol,
                seconds_since_pressure=round(seconds_since_pressure, 1),
                cooldown=S2_PRESSURE_COOLDOWN_SECONDS,
            )
            return

        if not await self._can_open_trade(symbol):
            open_count = await self._open_trades_count()
            has_symbol = await self._has_open_trade_for_symbol(symbol)
            logger.info(
                "S2 skip: cannot open trade",
                symbol=symbol,
                has_open_for_symbol=has_symbol,
                open_count=open_count,
                max_open=self.MAX_OPEN_TRADES,
            )
            return

        level = event["level"]
        # Кулдаун после закрытия сделки по этому уровню — не открывать повторно сразу
        _close_key = f"{symbol}:{level}"
        seconds_since_close = time.time() - self._recent_close.get(_close_key, 0.0)
        if seconds_since_close < 300:
            logger.info(
                "S2 skip: recent close cooldown",
                symbol=symbol,
                level=level,
                seconds_since_close=round(seconds_since_close, 1),
            )
            return
        atr = event.get("atr", 0.0)
        expected_depth = event.get("expected_depth", 0.0)

        grid_width = atr * 2.5
        step = grid_width / (S2_GRID_ORDERS - 1)
        grid_anchor = level * 1.0015  # первый ордер на 0.15% выше уровня (front-run)

        _c1m_now = candles_1m.get(symbol, [])
        _current_price_now = _c1m_now[-1]["close"] if _c1m_now else grid_anchor
        grid_prices = [grid_anchor - step * i for i in range(S2_GRID_ORDERS)]
        grid_bottom = grid_prices[-1]

        # Не открывать если цена выше grid_anchor более чем на 0.5% (уровень уже выше рынка)
        if _current_price_now > grid_anchor * 1.005:
            logger.info(
                "S2 skip: price too far above grid",
                symbol=symbol,
                current=round(_current_price_now, 8),
                anchor=round(grid_anchor, 8),
                diff_pct=round((_current_price_now - grid_anchor) / grid_anchor * 100, 3),
            )
            return
        # Не открывать если цена уже ниже нижнего ордера — вся сетка мгновенно заполнится
        if _current_price_now < grid_bottom:
            logger.info(
                "S2 skip: price already below grid bottom",
                symbol=symbol,
                current=round(_current_price_now, 8),
                grid_bottom=round(grid_bottom, 8),
            )
            return
        order_size = round(S2_POSITION_SIZE_USDT / S2_GRID_ORDERS, 4)

        grid_orders = [
            {
                "index": i + 1,
                "price": round(p, 8),
                "size": order_size,
                "filled": False,
                "fill_time": None,
                "cancelled": False,
            }
            for i, p in enumerate(grid_prices)
        ]

        bottom_price = grid_prices[-1]
        stop_loss = bottom_price - atr * 0.5

        entry_price_initial = level  # до первого fill = верхний ордер

        # TP1: entry + (entry - grid_bottom) × 1
        # TP2: entry + ATR × S2_TP2_ATR_MULT (явный множитель, независим от числа fills)
        tp1 = entry_price_initial + (entry_price_initial - bottom_price) * 1.0
        tp2 = entry_price_initial + atr * S2_TP2_ATR_MULT

        trade_id = str(uuid.uuid4())
        trade = {
            "trade_id": trade_id,
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "symbol": symbol,
            "level": level,
            "level_type": event.get("level_type", ""),
            "level_side": event.get("level_side", "support"),
            "entry_signal": "proximity",
            "strength_at_entry": strength,
            "p_bounce_at_entry": p_bounce,
            "expected_depth_at_entry": expected_depth,
            "approach_style": approach_style,
            "vol_ratio_at_entry": event.get("vol_ratio", 1.0),
            "atr_at_entry": atr,
            "entry_price": entry_price_initial,
            "entry_time": time.time(),
            "position_size": S2_POSITION_SIZE_USDT,
            "direction": "long",
            "grid_orders_json": json.dumps(grid_orders),
            "grid_fill_count": 0,
        }

        await open_trade(trade)

        # Уведомить live о новой сетке
        await _publish({
            "event_type":     "s2_grid_opened",
            "paper_trade_id": trade_id,
            "symbol":         symbol,
            "level":          level,
            "level_type":     event.get("level_type", ""),
            "grid_prices":    [round(p, 8) for p in grid_prices],
            "stop_loss":      round(stop_loss, 8),
            "take_profit_1":  round(tp1, 8),
            "take_profit_2":  round(tp2, 8),
        })

        params_note = json.dumps({
            "stop_loss": round(stop_loss, 8),
            "take_profit_1": round(tp1, 8),
            "take_profit_2": round(tp2, 8),
            "grid_bottom": round(bottom_price, 8),
            "atr": atr,
            "tp1_hit": False,
            "stop_moved_to_breakeven": False,
            # trailing — инициализируем пустыми, заполнятся при tp1_hit
            "trailing_active": False,
            "trailing_peak": None,
            "trailing_stop": None,
            # full-grid TP — заполнится при fill=10
            "full_grid_tp": None,
        })
        await add_trade_event(trade_id, "params_set", entry_price_initial, params_note)

        await self._send_open_message(trade, grid_orders, stop_loss, tp1, tp2)

        logger.info(
            "S2 grid opened",
            trade_id=trade_id,
            symbol=symbol,
            level=level,
            grid_count=S2_GRID_ORDERS,
            sl=round(stop_loss, 8),
            tp1=round(tp1, 8),
            tp2=round(tp2, 8),
        )

    # ── Сопровождение ─────────────────────────────────────────────────

    async def _check_exit(self, trade: dict, current_price: float) -> None:
        trade_id = trade["trade_id"]

        # Проверить заполнение ордеров в любом случае — включая первый fill при fill_count==0.
        await self._process_grid_fills(trade, current_price)

        # Таймаут без единого fill — проверяем после попытки заполнить
        if trade["grid_fill_count"] == 0:
            if time.time() - trade["entry_time"] > 1200:  # 20 минут без fill
                await self._close_and_track(trade_id, trade["symbol"], trade["entry_price"], "timeout_no_fill")
                await self._send_close_message(trade, trade["entry_price"], "timeout_no_fill")
            return

        # Перечитать trade из БД после возможного обновления в _process_grid_fills
        updated = await self._reload_trade(trade_id)
        if updated is None or updated["status"] != "open":
            return

        params = self._extract_params_full(updated)
        if not params:
            return

        stop_loss = params["stop_loss"]
        take_profit_1 = params["take_profit_1"]
        take_profit_2 = params["take_profit_2"]
        tp1_hit = params.get("tp1_hit", False)
        trailing_active = params.get("trailing_active", False)
        trailing_peak = params.get("trailing_peak")
        trailing_stop = params.get("trailing_stop")
        full_grid_tp = params.get("full_grid_tp")
        entry_price = updated["entry_price"]

        fill_count = updated.get("grid_fill_count") or 0
        filled_usdt = self._filled_usdt(fill_count)

        # High/Low последних 2 свечей 1М для захвата быстрых движений между тиками.
        _c1m = candles_1m.get(updated["symbol"], [])
        _last_high = max((c["high"] for c in _c1m[-2:]), default=current_price)
        _last_low = min((c["low"] for c in _c1m[-2:]), default=current_price)

        # ── Предложение 2: full-grid TP (fill == S2_GRID_ORDERS) ──────────
        # При заполнении всех ордеров сетки уровень пробит насквозь.
        # Ждём возврата к уровню снизу: TP = level × (1 − S2_FULL_GRID_TP_PCT).
        # Эта ветка работает только до tp1_hit — после TP1 переходим на trailing логику.
        if full_grid_tp is not None and not tp1_hit:
            if _last_high >= full_grid_tp:
                await self._close_and_notify(updated, full_grid_tp, "full_grid_tp", filled_usdt)
                await self._send_close_message(updated, full_grid_tp, "full_grid_tp")
                return
            # Стоп по-прежнему актуален — проверим ниже
            if current_price <= stop_loss:
                await self._close_and_notify(updated, current_price, "stop_loss", filled_usdt)
                await self._send_close_message(updated, current_price, "stop_loss")
            return

        # ── TP2 — только если trailing ещё не активирован ───────────────────
        if not tp1_hit and _last_high >= take_profit_2:
            avg_exit = take_profit_2
            await self._close_and_notify(updated, avg_exit, "take_profit_2", filled_usdt)
            await self._send_close_message(updated, avg_exit, "take_profit_2")
            return

        # ── Предложение 1: TP1 → активировать trailing немедленно ─────────
        if not tp1_hit and _last_high >= take_profit_1:
            # Инициализируем trailing от текущего high (пика в момент TP1)
            peak = _last_high
            t_stop = round(peak * (1.0 - S2_TRAILING_PCT), 8)

            params["tp1_hit"] = True
            params["stop_moved_to_breakeven"] = True
            params["trailing_active"] = True
            params["trailing_peak"] = peak
            params["trailing_stop"] = t_stop

            await add_trade_event(
                trade_id, "tp1_hit", current_price,
                json.dumps({
                    "partial_exit_price": _last_high,
                    "partial_exit_pct": 50,
                    "trailing_peak": peak,
                    "trailing_stop": t_stop,
                })
            )
            await add_trade_event(trade_id, "params_updated", current_price, json.dumps(params))
            logger.info(
                "S2 TP1 hit — trailing activated",
                trade_id=trade_id,
                peak=peak,
                trailing_stop=t_stop,
            )
            return

        # ── Trailing stop: обновлять пик и стоп каждый тик после tp1_hit ──
        if trailing_active and trailing_peak is not None:
            new_peak = trailing_peak
            if _last_high > trailing_peak:
                new_peak = _last_high
                new_t_stop = round(new_peak * (1.0 - S2_TRAILING_PCT), 8)
                params["trailing_peak"] = new_peak
                params["trailing_stop"] = new_t_stop
                await add_trade_event(
                    trade_id, "params_updated", current_price,
                    json.dumps(params)
                )
                logger.debug(
                    "S2 trailing peak updated",
                    trade_id=trade_id,
                    new_peak=new_peak,
                    new_trailing_stop=new_t_stop,
                )
                trailing_stop = new_t_stop

            # Проверка trailing stop по _last_low — захватывает sweep между тиками
            if trailing_stop is not None and _last_low <= trailing_stop:
                # Выходим по trailing: среднее между TP1 и trailing_stop (половина позиции уже "зафиксирована")
                avg_exit = (take_profit_1 + trailing_stop) / 2
                await self._close_and_notify(updated, avg_exit, "trailing_stop", filled_usdt)
                await self._send_close_message(updated, avg_exit, "trailing_stop")
                return

        # ── Обычный стоп (до TP1) ─────────────────────────────────────────
        if not tp1_hit:
            if current_price <= stop_loss:
                await self._close_and_notify(updated, current_price, "stop_loss", filled_usdt)
                await self._send_close_message(updated, current_price, "stop_loss")

    async def _process_grid_fills(self, trade: dict, current_price: float) -> None:
        """Исполнить ордера сетки, до цены которых дошёл рынок.

        Проверяем не current_price, а low последних 2 свечей 1М — это решает
        проблему sweep: быстрое движение вниз с возвратом укладывается в 1–3 сек
        и не попадает в поллинг event bus (~5 сек), но всегда отражается в low свечи.
        fill_price = order["price"] — лимитный ордер исполняется по своей цене.
        """
        trade_id = trade["trade_id"]
        symbol = trade["symbol"]
        try:
            grid_orders = json.loads(trade["grid_orders_json"] or "[]")
        except Exception:
            return

        # Low последних 2 закрытых свечей 1М как прокси реального минимума цены.
        _c1m = candles_1m.get(symbol, [])
        _last_low = min((c["low"] for c in _c1m[-2:]), default=current_price)

        changed = False
        for order in grid_orders:
            if order["filled"] or order.get("cancelled"):
                continue
            if _last_low <= order["price"]:
                order["filled"] = True
                order["fill_time"] = time.time()
                changed = True

                await add_trade_event(
                    trade_id, "order_filled", order["price"],
                    json.dumps({"order_index": order["index"], "price": order["price"]})
                )

                fill_count = sum(1 for o in grid_orders if o["filled"])
                weighted_entry = (
                    sum(o["price"] for o in grid_orders if o["filled"]) / fill_count
                )

                # Обновить grid в БД
                await self._update_grid_in_db(
                    trade_id, grid_orders, fill_count, weighted_entry, trade
                )

        if changed:
            fill_count = sum(1 for o in grid_orders if o["filled"])
            weighted_entry = sum(o["price"] for o in grid_orders if o["filled"]) / fill_count

            # FIX BUG-10: перечитываем trade из БД — исходный dict не содержит params_updated
            # событий, добавленных в этой же сессии.
            fresh_trade = await self._reload_trade(trade_id)
            if fresh_trade:
                await self._recalculate_params(trade_id, weighted_entry, fill_count, fresh_trade)

    async def _update_grid_in_db(
        self,
        trade_id: str,
        grid_orders: list,
        fill_count: int,
        weighted_entry: float,
        trade: dict,
    ) -> None:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """UPDATE trades
                   SET grid_orders_json = ?, grid_fill_count = ?, entry_price = ?, updated_at = ?
                   WHERE trade_id = ?""",
                (
                    json.dumps(grid_orders),
                    fill_count,
                    round(weighted_entry, 8),
                    time.time(),
                    trade_id,
                ),
            )
            await db.commit()

    async def _recalculate_params(
        self,
        trade_id: str,
        weighted_entry: float,
        fill_count: int,
        trade: dict,
    ) -> None:
        """Пересчитать TP/SL после изменения средневзвешенного entry_price.

        При fill_count == S2_GRID_ORDERS (полный grid) устанавливаем full_grid_tp:
        TP = level × (1 - S2_FULL_GRID_TP_PCT), только если он выше weighted_entry.
        Это даёт шанс закрыться при возврате цены к уровню снизу.
        TP2 теперь задаётся как entry + ATR × S2_TP2_ATR_MULT (явный, не через grid_bottom).
        """
        params = self._extract_params(trade)
        if params is None:
            return

        # Preserve trailing/state fields from full event history in case
        # _extract_params picked up a stale params_updated without them.
        existing_params = self._extract_params_full(trade)
        for key in ("trailing_active", "trailing_peak", "trailing_stop", "tp1_hit", "stop_moved_to_breakeven"):
            if not params.get(key):
                val = existing_params.get(key)
                if val is not None:
                    params[key] = val

        grid_bottom = params.get("grid_bottom", weighted_entry)
        atr = params.get("atr", trade.get("atr_at_entry", 0.0))
        level = trade.get("level", weighted_entry)

        stop_loss = grid_bottom - atr * 0.5
        # При полном заполнении сетки уровень пробит насквозь — ужесточаем стоп
        if fill_count >= S2_GRID_ORDERS:
            stop_loss = weighted_entry - atr * 0.2
        tp1 = weighted_entry + (weighted_entry - grid_bottom) * 1.0
        # TP2 = entry + ATR × 5 (явный, стабильный при любом числе fills)
        tp2 = weighted_entry + atr * S2_TP2_ATR_MULT

        # Предложение 2: при заполнении всего grid — полный-grid TP
        full_grid_tp = None
        if fill_count >= S2_GRID_ORDERS:
            candidate = round(level * (1.0 - S2_FULL_GRID_TP_PCT), 8)
            # Выставляем только если выше breakeven (иначе фиксировали бы убыток)
            full_grid_tp = candidate if candidate > weighted_entry else round(weighted_entry * 1.001, 8)
            logger.info(
                "S2 full grid reached — full_grid_tp set",
                trade_id=trade_id,
                fill_count=fill_count,
                level=level,
                weighted_entry=round(weighted_entry, 8),
                full_grid_tp=full_grid_tp,
            )
            await send_message(
                f"⚠️ [S2 Grid] {trade.get('symbol', '')} — сетка заполнена полностью ({fill_count}/{S2_GRID_ORDERS})\n"
                f"   Ср. вход: {round(weighted_entry, 8)} | Full-grid TP: {full_grid_tp}\n"
                f"   Уровень пробит насквозь. Ждём возврат к уровню."
            )

        params.update({
            "stop_loss": round(stop_loss, 8),
            "take_profit_1": round(tp1, 8),
            "take_profit_2": round(tp2, 8),
            "full_grid_tp": full_grid_tp,
        })
        await add_trade_event(trade_id, "params_updated", weighted_entry, json.dumps(params))

    async def _handle_breakout(self, event: dict) -> None:
        trades = await get_open_trades(self.strategy_id)
        for trade in trades:
            if trade["symbol"] != event["symbol"]:
                continue
            if abs(trade["level"] - event["level"]) / max(trade["level"], 1) > 0.005:
                continue

            trade_id = trade["trade_id"]
            current_price = event["current_price"]
            fill_count = trade.get("grid_fill_count") or 0

            # Отменить неисполненные ордера
            try:
                grid_orders = json.loads(trade["grid_orders_json"] or "[]")
                for o in grid_orders:
                    if not o["filled"]:
                        o["cancelled"] = True
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE trades SET grid_orders_json = ?, updated_at = ? WHERE trade_id = ?",
                        (json.dumps(grid_orders), time.time(), trade_id),
                    )
                    await db.commit()
            except Exception as e:
                logger.error("S2 cancel grid orders failed", error=str(e))

            if fill_count == 0:
                # Позиции не было — закрываем с нулевым PnL без вызова close_trade.
                async with aiosqlite.connect(DB_PATH) as _db:
                    await _db.execute(
                        """UPDATE trades
                           SET exit_price = ?, exit_time = ?, exit_reason = ?,
                               pnl_pct = 0.0, pnl_usdt = 0.0, duration_minutes = ?,
                               status = 'closed', updated_at = ?
                           WHERE trade_id = ?""",
                        (
                            trade["entry_price"],
                            time.time(),
                            "cancelled_no_fill",
                            round((time.time() - trade["entry_time"]) / 60, 2),
                            time.time(),
                            trade_id,
                        ),
                    )
                    await _db.commit()
                await self._send_close_message(trade, trade["entry_price"], "cancelled_no_fill")
                logger.info("S2 grid cancelled (no fills), pnl=0", trade_id=trade_id)
            else:
                # Есть реальная позиция — проверяем, активен ли trailing
                params = self._extract_params(trade)
                tp1_hit = params.get("tp1_hit", False) if params else False
                trailing_stop = params.get("trailing_stop") if params else None
                take_profit_1 = params.get("take_profit_1", trade["entry_price"]) if params else trade["entry_price"]

                if tp1_hit and trailing_stop is not None:
                    # После TP1: среднее между TP1 и текущим trailing_stop
                    avg_exit = (take_profit_1 + max(current_price, trailing_stop)) / 2
                    exit_reason = "breakout_after_tp1"
                else:
                    avg_exit = current_price
                    exit_reason = "breakout_confirmed"

                await self._close_and_notify(trade, avg_exit, exit_reason, self._filled_usdt(fill_count))
                updated_trade = await self._reload_trade_closed(trade_id) or trade
                await self._send_close_message(updated_trade, avg_exit, exit_reason)

            logger.info("S2 grid closed on breakout", trade_id=trade_id, fill_count=fill_count)

    def _filled_usdt(self, fill_count: int) -> float:
        """Реальный размер позиции в USDT по числу исполненных ордеров."""
        return S2_POSITION_SIZE_USDT * fill_count / S2_GRID_ORDERS

    # ── Таймаут ───────────────────────────────────────────────────────

    async def _check_timeout(self) -> None:
        """
        Переопределяем базовый _check_timeout чтобы при fill_count=0
        закрывать с pnl=0, а не считать фантомный PnL от entry_price=level.
        Сделки с fill_count>0 обрабатываются как обычно через базовый метод.
        """
        from data.collector import candles_1m
        from trading.trade_log import update_trade_extremes
        import time as _time

        trades = await get_open_trades(self.strategy_id)
        now = _time.time()
        for trade in trades:
            age_minutes = (now - trade["entry_time"]) / 60
            if age_minutes < self.TRADE_TIMEOUT_MINUTES:
                continue

            fill_count = trade.get("grid_fill_count") or 0
            trade_id = trade["trade_id"]
            symbol = trade["symbol"]

            if fill_count == 0:
                # Позиции не было — закрываем с нулевым PnL
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        """UPDATE trades
                           SET exit_price = ?, exit_time = ?, exit_reason = ?,
                               pnl_pct = 0.0, pnl_usdt = 0.0, duration_minutes = ?,
                               status = 'closed', updated_at = ?
                           WHERE trade_id = ?""",
                        (
                            trade["entry_price"],
                            now,
                            "timeout_no_fill",
                            round(age_minutes, 2),
                            now,
                            trade_id,
                        ),
                    )
                    await db.commit()
                await self._send_close_message(trade, trade["entry_price"], "timeout_no_fill")
                logger.info("S2 timeout_no_fill (zero PnL)", trade_id=trade_id, symbol=symbol)
            else:
                # Страховка: перечитать fill_count из БД перед закрытием
                fresh = await self._reload_trade(trade_id)
                if fresh and (fresh.get("grid_fill_count") or 0) == 0:
                    # fill_count обнулился — закрыть с pnl=0
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute(
                            """UPDATE trades
                               SET exit_price = ?, exit_time = ?, exit_reason = ?,
                                   pnl_pct = 0.0, pnl_usdt = 0.0, duration_minutes = ?,
                                   status = 'closed', updated_at = ?
                               WHERE trade_id = ?""",
                            (trade["entry_price"], now, "timeout_no_fill",
                             round(age_minutes, 2), now, trade_id),
                        )
                        await db.commit()
                    await self._send_close_message(trade, trade["entry_price"], "timeout_no_fill")
                    logger.info("S2 timeout_no_fill guard (zero PnL)", trade_id=trade_id, symbol=symbol)
                    continue
                # Есть реальная позиция — стандартная логика
                c1m = candles_1m.get(symbol, [])
                current_price = c1m[-1]["close"] if c1m else trade["entry_price"]
                try:
                    await update_trade_extremes(
                        trade_id, current_price, trade["entry_price"], trade["direction"]
                    )
                    ep = trade["entry_price"]
                    if ep > 0:
                        fav = (current_price - ep) / ep * 100
                        adv = (ep - current_price) / ep * 100
                        trade["max_favorable_pct"] = max(trade.get("max_favorable_pct") or 0.0, fav)
                        trade["max_adverse_pct"]   = max(trade.get("max_adverse_pct") or 0.0, adv)
                    await self._close_and_notify(trade, current_price, "timeout", self._filled_usdt(fill_count))
                    await self._send_close_message(trade, current_price, "timeout")
                    logger.info(
                        "S2 timeout with fills",
                        trade_id=trade_id, symbol=symbol,
                        fill_count=fill_count, age_minutes=round(age_minutes, 1),
                    )
                except Exception as e:
                    logger.error("S2 _check_timeout error", trade_id=trade_id, error=str(e))

    # ── Вспомогательные ───────────────────────────────────────────────

    async def _close_and_notify(
        self,
        trade: dict,
        exit_price: float,
        exit_reason: str,
        filled_usdt: float | None = None,
    ) -> None:
        """Закрыть paper trade и уведомить live о закрытии."""
        trade_id = trade["trade_id"]
        symbol = trade["symbol"]
        await self._close_and_track(trade_id, symbol, exit_price, exit_reason, filled_usdt)
        await _publish({
            "event_type":     "s2_grid_closed",
            "paper_trade_id": trade_id,
            "symbol":         symbol,
            "exit_reason":    exit_reason,
            "exit_price":     exit_price,
        })

    async def _close_and_track(self, trade_id, symbol, exit_price, exit_reason, filled_size=None):
        # Фиксируем кулдаун по уровню перед закрытием (trade ещё open, level доступен)
        try:
            from trading.trade_log import get_open_trades as _got
            _trades = await _got(self.strategy_id)
            _level = next((t['level'] for t in _trades if t['trade_id'] == trade_id), None)
            if _level is not None:
                self._recent_close[f'{symbol}:{_level}'] = time.time()
        except Exception:
            pass
        await super()._close_and_track(trade_id, symbol, exit_price, exit_reason, filled_size)

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

    def _extract_params_full(self, trade: dict) -> dict:
        """Scan ALL params events to collect the most recent value of each field.

        Used in _recalculate_params to preserve trailing/state fields that may
        be absent from the very last params_updated (written before tp1_hit).
        """
        try:
            events = json.loads(trade.get("events_json") or "[]")
        except Exception:
            return {}
        merged: dict = {}
        for ev in events:
            if ev["type"] in ("params_updated", "params_set"):
                try:
                    merged.update(json.loads(ev["note"]))
                except Exception:
                    pass
        return merged

    async def _reload_trade(self, trade_id: str) -> dict | None:
        trades = await get_open_trades(self.strategy_id)
        for t in trades:
            if t["trade_id"] == trade_id:
                return t
        return None

    async def _reload_trade_closed(self, trade_id: str) -> dict | None:
        """Перечитать трейд из БД по trade_id независимо от статуса (нужно после close_trade)."""
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT * FROM trades WHERE trade_id = ?", (trade_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    return dict(row) if row else None
        except Exception as e:
            logger.error("_reload_trade_closed failed", trade_id=trade_id, error=str(e))
            return None

    # ── Telegram ──────────────────────────────────────────────────────

    async def _send_open_message(
        self,
        trade: dict,
        grid_orders: list,
        stop_loss: float,
        tp1: float,
        tp2: float,
    ) -> None:
        order_size = round(S2_POSITION_SIZE_USDT / S2_GRID_ORDERS, 2)
        prices_str = "  ".join(f"#{o['index']}: {o['price']}" for o in grid_orders)
        text = (
            f"🔵 [S2 Grid] {trade['symbol']} LONG — сетка выставлена\n"
            f"   Уровень: {trade['level']} ({trade['level_type']}, strength={trade['strength_at_entry']})"
            f" | p_bounce={trade['p_bounce_at_entry']:.2f}\n"
            f"   Ордера ({S2_GRID_ORDERS}×{order_size} USDT):\n"
            f"     {prices_str}\n"
            f"   SL: {round(stop_loss, 8)} | TP1: {round(tp1, 8)} | TP2: {round(tp2, 8)} (entry+{S2_TP2_ATR_MULT}×ATR)"
        )
        try:
            await send_message(text)
        except Exception as e:
            logger.error("S2 send_open_message failed", error=str(e))

    async def _send_close_message(self, trade: dict, exit_price: float, reason: str) -> None:
        ep = trade["entry_price"]
        fill_count = trade.get("grid_fill_count") or 0
        pnl_pct = (exit_price - ep) / ep * 100 if ep > 0 else 0.0
        filled_size = S2_POSITION_SIZE_USDT * fill_count / S2_GRID_ORDERS
        pnl_usdt = filled_size * pnl_pct / 100
        icon = "✅" if pnl_pct >= 0 else "🔴"
        max_fav  = trade.get("max_favorable_pct") or 0.0
        max_adv  = trade.get("max_adverse_pct") or 0.0
        max_profit_usdt = filled_size * max_fav / 100
        max_loss_usdt   = filled_size * max_adv / 100

        # Время с первого fill до закрытия
        first_fill_duration = ""
        try:
            grid_orders = json.loads(trade.get("grid_orders_json") or "[]")
            fill_times = [o["fill_time"] for o in grid_orders if o.get("filled") and o.get("fill_time")]
            if fill_times:
                first_fill_duration = " | С 1-го fill: " + self._format_duration(min(fill_times))
        except Exception:
            pass

        # Добавляем пометку для trailing/full_grid выходов
        reason_label = {
            "trailing_stop": "trailing stop 🎯",
            "full_grid_tp": "full-grid TP (возврат к уровню) 🎯",
            "breakout_after_tp1": "breakout после TP1",
        }.get(reason, reason)

        text = (
            f"{icon} [S2 Grid] {trade['symbol']} закрыт\n"
            f"   Заполнено ордеров: {fill_count}/{S2_GRID_ORDERS}"
            f" | Ср. вход: {ep} → Выход: {exit_price}\n"
            f"   Причина: {reason_label}\n"
            f"   PnL: {self._format_pct(pnl_pct)} ({self._format_pct(pnl_usdt, sign=True)} USDT)"
            f" | Время: {self._format_duration(trade['entry_time'])}{first_fill_duration}\n"
            f"   📈 Max profit: +{max_fav:.2f}% (+{max_profit_usdt:.2f} USDT)\n"
            f"   📉 Max drawdown: -{max_adv:.2f}% (-{max_loss_usdt:.2f} USDT)"
        )
        try:
            await send_close_with_chart(text, trade["symbol"],
                entry_price=trade["entry_price"], exit_price=exit_price, level=trade.get("level"))
        except Exception as e:
            logger.error("S2 send_close_message failed", error=str(e))
