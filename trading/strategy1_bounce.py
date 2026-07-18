"""Strategy 1: Bounce — вход по подтверждённому отбою (bounce / sweep)."""

from __future__ import annotations

import json
import time
import uuid

from trading.base_strategy import BaseStrategy
from trading.trade_log import open_trade, add_trade_event, get_open_trades
from constants import S1_MIN_STRENGTH, S1_MIN_P_BOUNCE, S1_MAX_VOL_RATIO, S1_SL_PCT, S1_TP_PCT
from logger import logger


class Strategy1Bounce(BaseStrategy):
    strategy_id = 1
    strategy_name = "bounce"

    # ── Вход ──────────────────────────────────────────────────────────

    async def on_event(self, event: dict) -> None:
        event_type = event.get("event_type")

        # Реагируем на bounce и sweep как сигналы входа
        if event_type in ("bounce", "sweep"):
            await self._try_open(event)
            return

        # Breakout по открытой сделке — экстренный выход
        if event_type == "breakout":
            await self._handle_breakout(event)

    async def _try_open(self, event: dict) -> None:
        symbol = event["symbol"]
        strength = event.get("strength", 0)
        p_bounce = event.get("p_bounce", 0.0)
        approach_style = event.get("approach_style", "unknown")

        if strength < S1_MIN_STRENGTH:
            return
        if p_bounce < S1_MIN_P_BOUNCE:
            return
        if approach_style == "bleed":
            return
        # pump_base уровни в bearish режиме пробиваются насквозь — исключить полностью
        level_type = event.get("level_type", "")
        if level_type == "pump_base":
            logger.debug("S1 skip: pump_base level", symbol=symbol, level_type=level_type)
            return
        # Не входить если объём на касании выше порога — S1 ловит тихие отбои.
        # Данные history.db: vol 0.8–3.0× даёт одинаковый bounce rate ~41%.
        # Высокий vol (>1.2×) — уже активное движение, для S2/S3, не S1.
        vol_ratio = event.get("vol_ratio", 1.0)
        if vol_ratio > S1_MAX_VOL_RATIO:
            logger.debug(
                "S1 skip: vol_ratio above threshold (noisy touch)",
                symbol=symbol, vol_ratio=vol_ratio, threshold=S1_MAX_VOL_RATIO,
            )
            return
        # S1-fix фильтр входа: только consolidation_base или strength==4.
        # pump_base уже отсечён выше. По backtest только эта выборка даёт
        # положительное матожидание (худший случай +5.6%, n=58).
        if not (strength == 4 or level_type == "consolidation_base"):
            logger.debug(
                "S1 skip: вне прибыльной выборки",
                symbol=symbol, level_type=level_type, strength=strength,
            )
            return
        if not await self._can_open_trade(symbol):
            return

        entry_price = event["current_price"]
        atr = event.get("atr", 0.0)
        expected_depth = event.get("expected_depth", 0.0)

        # S1-fix: фиксированный симметричный брекет от цены входа.
        # Никакого ATR/depth/RR — только проценты. Трейлинг удалён.
        stop_loss   = entry_price * (1.0 - S1_SL_PCT)
        take_profit = entry_price * (1.0 + S1_TP_PCT)

        trade_id = str(uuid.uuid4())
        trade = {
            "trade_id": trade_id,
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "symbol": symbol,
            "level": event["level"],
            "level_type": event.get("level_type", ""),
            "level_side": event.get("level_side", "support"),
            "entry_signal": event["event_type"],
            "strength_at_entry": strength,
            "p_bounce_at_entry": p_bounce,
            "expected_depth_at_entry": expected_depth,
            "approach_style": approach_style,
            "vol_ratio_at_entry": event.get("vol_ratio", 1.0),
            "atr_at_entry": atr,
            "entry_price": entry_price,
            "entry_time": time.time(),
            "position_size": self.POSITION_SIZE_USDT,
            "direction": "long",
            "grid_orders_json": None,
            "grid_fill_count": None,
            # Параметры выхода хранятся в extra-полях events_json
        }

        await open_trade(trade)

        # Сохранить параметры выхода как первое событие
        params_note = json.dumps({
            "stop_loss": round(stop_loss, 8),
            "take_profit": round(take_profit, 8),
        })
        await add_trade_event(trade_id, "params_set", entry_price, params_note)

        trade["entry_price"] = entry_price  # для сообщения
        await self._send_open_message(trade, stop_loss, take_profit)

        logger.info(
            "S1 trade opened",
            trade_id=trade_id,
            symbol=symbol,
            entry=entry_price,
            sl=round(stop_loss, 8),
            tp=round(take_profit, 8),
        )

    # ── Сопровождение ─────────────────────────────────────────────────

    async def _check_exit(self, trade: dict, current_price: float) -> None:
        trade_id = trade["trade_id"]

        params = self._extract_params(trade)
        if params is None:
            return
        stop_loss   = params["stop_loss"]
        take_profit = params["take_profit"]

        # S1-fix: единый брекет на всю позицию. Только current_price (live).
        # НЕ использовать high/low свечей — это был источник бага трейлинга.
        if current_price >= take_profit:
            await self._close_and_track(trade_id, trade["symbol"], take_profit, "take_profit")
            await self._send_close_message(trade, take_profit, "take_profit")
            return
        if current_price <= stop_loss:
            await self._close_and_track(trade_id, trade["symbol"], stop_loss, "stop_loss")
            await self._send_close_message(trade, stop_loss, "stop_loss")
            return

    async def _handle_breakout(self, event: dict) -> None:
        """Закрыть сделку при подтверждённом пробое того же уровня."""
        trades = await get_open_trades(self.strategy_id)
        for trade in trades:
            if trade["symbol"] != event["symbol"]:
                continue
            if abs(trade["level"] - event["level"]) / max(trade["level"], 1) > 0.005:
                continue
            current_price = event["current_price"]
            await self._close_and_track(trade["trade_id"], trade["symbol"], current_price, "breakout_confirmed")
            await self._send_close_message(trade, current_price, "breakout_confirmed")
            logger.info("S1 trade closed on breakout", trade_id=trade["trade_id"])

    # ── Вспомогательные ───────────────────────────────────────────────

    def _extract_params(self, trade: dict) -> dict | None:
        """Достать последний params_set / params_updated из events_json."""
        try:
            events = json.loads(trade.get("events_json") or "[]")
        except Exception:
            return None
        # Берём последний params_updated или params_set
        for ev in reversed(events):
            if ev["type"] in ("params_updated", "params_set"):
                try:
                    return json.loads(ev["note"])
                except Exception:
                    return None
        return None

    # ── Telegram ──────────────────────────────────────────────────────

    async def _send_open_message(
        self, trade: dict, stop_loss: float, take_profit: float
    ) -> None:
        ep = trade["entry_price"]
        sl_pct = self._format_pct((stop_loss - ep) / ep * 100)
        tp_pct = self._format_pct((take_profit - ep) / ep * 100)
        text = (
            f"📈 [S1 Bounce] {trade['symbol']} LONG\n"
            f"   Уровень: {trade['level']} ({trade['level_type']}, strength={trade['strength_at_entry']})\n"
            f"   Вход: {ep} | p_bounce={trade['p_bounce_at_entry']:.2f} | style={trade['approach_style']}\n"
            f"   SL: {round(stop_loss, 8)} ({sl_pct}) | TP: {round(take_profit, 8)} ({tp_pct})\n"
            f"   Позиция: {int(self.POSITION_SIZE_USDT)} USDT"
        )
        # Уведомления в Telegram отключены — сделка пишется в trades.db (open_trade).
        logger.debug("S1 open message (telegram disabled)", text=text)

    async def _send_close_message(self, trade: dict, exit_price: float, reason: str) -> None:
        ep = trade["entry_price"]
        pnl_pct = (exit_price - ep) / ep * 100
        pnl_usdt = self.POSITION_SIZE_USDT * pnl_pct / 100
        icon = "✅" if pnl_pct >= 0 else "🔴"
        max_fav  = trade.get("max_favorable_pct") or 0.0
        max_adv  = trade.get("max_adverse_pct") or 0.0
        max_profit_usdt = self.POSITION_SIZE_USDT * max_fav / 100
        max_loss_usdt   = self.POSITION_SIZE_USDT * max_adv / 100
        text = (
            f"{icon} [S1 Bounce] {trade['symbol']} закрыт\n"
            f"   Причина: {reason}\n"
            f"   Вход: {ep} → Выход: {exit_price}\n"
            f"   PnL: {self._format_pct(pnl_pct)} ({self._format_pct(pnl_usdt, sign=True)} USDT)"
            f" | Время: {self._format_duration(trade['entry_time'])}\n"
            f"   📈 Max profit: +{max_fav:.2f}% (+{max_profit_usdt:.2f} USDT)\n"
            f"   📉 Max drawdown: -{max_adv:.2f}% (-{max_loss_usdt:.2f} USDT)"
        )
        # Уведомления в Telegram отключены — сделка закрывается в trades.db (close_trade).
        logger.debug("S1 close message (telegram disabled)", text=text)
