"""strategy5_live.py — РЕАЛЬНОЕ исполнение S5 на отдельном Bybit sub-аккаунте.

Подкласс Strategy5Continuation: детектор сигнала и сканер наследуются без
дублирования. Переопределены только запуск, отбор и открытие/сопровождение —
на реальные ордера через bybit_client_s5 (изолированный ключ sub-аккаунта).

Работает ПАРАЛЛЕЛЬНО с paper-S5:
  • paper-инстанс пишет идеальный лог в s5_signals.db (основание сигналов);
  • этот live-инстанс торгует реально и пишет в s5_live_trades.db.
Сравнение paper↔live даёт цену исполнения (slippage, тайминг).

ВАЖНО: базовый _price_loop читает paper trades.db и НЕ видит live-сделки S5,
поэтому здесь свой _manage_loop + reconcile с биржей.

Включается флагом окружения S5_LIVE=true в strategy_runner. Без ключей
BYBIT_S5_* модуль bybit_client_s5 поднимет ошибку — S5-live не стартует, S2 цел.

Поток одной сделки (без сетки — один вход, как в бэктесте):
  вход market → stop-market SL на бирже → TP1 (50% market reduce_only) →
  SL в безубыток → трейлинг → TP2 (остаток market). SL на бирже срабатывает сам;
  reconcile ловит это по get_position==0 и сводит PnL по executions.
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from decimal import Decimal, ROUND_DOWN
from typing import Optional

from trading.strategy5_continuation import Strategy5Continuation, S5_FIRE_COOLDOWN_SEC
from trading import bybit_client_s5 as api
from trading import s5_live_log as s5db
from data.collector import candles_1m
from logger import logger

# ── Константы live ────────────────────────────────────────────────────────────
S5_LIVE_LEVERAGE          = 3
S5_LIVE_POSITION_USDT     = 100.0   # меньше paper ($200): форвард с ограниченным риском
S5_LIVE_MANAGE_INTERVAL   = 5       # сек между проверками сопровождения
S5_LIVE_MAX_OPEN          = 3       # одновременных live-позиций S5
S5_LIVE_TP1_FRACTION      = 0.5
S5_LIVE_TRAIL_R           = 1.0
S5_LIVE_FEE               = 0.00055


class Strategy5Live(Strategy5Continuation):
    strategy_name = "continuation_live"

    def __init__(self) -> None:
        super().__init__()
        self._manage_task: Optional[asyncio.Task] = None
        self._instr_cache: dict[str, dict] = {}

    # ── Запуск: сканер (наследован) + свой цикл сопровождения ──────────────────

    def start_scanner(self) -> None:
        super().start_scanner()  # запускает _scan_loop (детектор)
        if self._manage_task is None or self._manage_task.done():
            self._manage_task = asyncio.create_task(
                self._manage_loop(), name="s5_live_manage")

    # ── Отбор: live-лимиты + отсутствие позиции на бирже ──────────────────────

    async def _can_open_trade(self, symbol: str) -> bool:
        open_live = await s5db.get_open_trades()
        if any(t["symbol"] == symbol for t in open_live):
            return False
        if len(open_live) >= S5_LIVE_MAX_OPEN:
            return False
        # Безопасность: на sub-аккаунте не должно быть позиции по символу
        try:
            pos = await api.get_position(symbol)
            if pos and pos.get("size", 0) > 0:
                return False
        except Exception:
            return False  # не смогли проверить → не входим
        return True

    # ── Сканер-хук: НЕ пишем в s5_signals (это делает paper-инстанс) ──────────

    async def _scan_symbol(self, symbol: str) -> None:
        last = self._last_fire.get(symbol, 0.0)
        if time.time() - last < S5_FIRE_COOLDOWN_SEC:
            return
        if not await self._can_open_trade(symbol):
            return
        sig = self._detect_signal(symbol)
        if sig is None:
            return
        self._last_fire[symbol] = time.time()
        await self._open_live(symbol, sig)

    # ── Инструмент / qty ──────────────────────────────────────────────────────

    async def _instrument(self, symbol: str) -> Optional[dict]:
        if symbol in self._instr_cache:
            return self._instr_cache[symbol]
        raw = await api.get_instrument_info(symbol)
        if not raw:
            return None
        lot = raw.get("lotSizeFilter", {})
        pf = raw.get("priceFilter", {})
        try:
            parsed = {
                "qty_step": float(lot.get("qtyStep", "0.001")),
                "min_qty": float(lot.get("minOrderQty", "0.001")),
                "min_notional": float(lot.get("minNotionalValue", "5") or "5"),
                "tick": float(pf.get("tickSize", "0.0001")),
            }
        except (ValueError, TypeError):
            return None
        self._instr_cache[symbol] = parsed
        return parsed

    @staticmethod
    def _snap(value: float, step: float) -> float:
        if step <= 0:
            return value
        return float((Decimal(str(value)) / Decimal(str(step))).to_integral_value(
            rounding=ROUND_DOWN) * Decimal(str(step)))

    def _calc_qty(self, price: float, instr: dict) -> float:
        raw = S5_LIVE_POSITION_USDT / price if price > 0 else 0.0
        qty = self._snap(raw, instr["qty_step"])
        if qty < instr["min_qty"] or qty * price < instr["min_notional"]:
            return 0.0
        return qty

    # ── Открытие реальной сделки ──────────────────────────────────────────────

    async def _open_live(self, symbol: str, sig: dict) -> None:
        trade_id = str(uuid.uuid4())
        instr = await self._instrument(symbol)
        if instr is None:
            logger.warning("S5Live: no instrument info, skip", symbol=symbol)
            return
        entry_ref = sig["entry"]
        qty = self._calc_qty(entry_ref, instr)
        if qty <= 0:
            logger.warning("S5Live: qty=0 (dear/cheap coin), skip", symbol=symbol, price=entry_ref)
            return

        try:
            await api.set_leverage(symbol, S5_LIVE_LEVERAGE)
            entry_order = await api.place_market_order(
                symbol=symbol, side="Buy", qty=qty,
                order_link_id=f"s5e_{trade_id[:8]}")
        except Exception as e:
            logger.error("S5Live: entry order failed", symbol=symbol, error=str(e))
            return

        # Реальные цена/размер входа с биржи
        await asyncio.sleep(0.5)
        pos = await api.get_position(symbol)
        entry_price = pos["avg_price"] if pos and pos.get("avg_price") else entry_ref
        filled_qty = pos["size"] if pos and pos.get("size") else qty

        # Пересчёт SL/целей от риска на РЕАЛЬНОМ входе (геометрия из сигнала)
        rr = entry_price - sig["sl"]
        if rr <= 0:
            logger.error("S5Live: rr<=0 after fill, closing", symbol=symbol,
                         entry=entry_price, sl=sig["sl"])
            try:
                await api.place_market_order(symbol=symbol, side="Sell", qty=filled_qty,
                                             reduce_only=True)
            except Exception:
                pass
            return
        sl = sig["sl"]
        tp1 = entry_price + rr * 1.0
        tp2 = entry_price + rr * 2.0

        sl_trigger = self._snap(sl, instr["tick"])
        sl_order_id = None
        try:
            sl_order = await api.place_stop_market_order(
                symbol=symbol, side="Sell", qty=filled_qty,
                trigger_price=sl_trigger, order_link_id=f"s5sl_{trade_id[:8]}")
            sl_order_id = sl_order.get("orderId")
        except Exception as e:
            # Не смогли поставить SL — аварийно закрываем вход (нельзя держать без защиты)
            logger.error("S5Live: SL placement failed — closing entry", symbol=symbol, error=str(e))
            try:
                await api.place_market_order(symbol=symbol, side="Sell", qty=filled_qty,
                                             reduce_only=True)
            except Exception:
                pass
            return

        await s5db.open_trade({
            "trade_id": trade_id, "symbol": symbol, "entry_time": time.time(),
            "entry_price": entry_price, "qty": filled_qty,
            "position_size_usdt": S5_LIVE_POSITION_USDT,
            "stop_loss": sl, "take_profit_1": tp1, "take_profit_2": tp2, "rr": rr,
            "entry_order_id": entry_order.get("orderId"), "sl_order_id": sl_order_id,
            "basis": sig.get("basis"),
        })
        logger.info("S5Live OPEN", trade_id=trade_id, symbol=symbol, entry=entry_price,
                    qty=filled_qty, sl=sl, tp1=tp1, tp2=tp2, basis=sig.get("basis"))

    # ── Цикл сопровождения + reconcile ────────────────────────────────────────

    async def _manage_loop(self) -> None:
        while True:
            await asyncio.sleep(S5_LIVE_MANAGE_INTERVAL)
            try:
                open_trades = await s5db.get_open_trades()
                for t in open_trades:
                    try:
                        await self._manage_one(t)
                    except Exception as e:
                        logger.error("S5Live manage error", trade_id=t.get("trade_id"),
                                     symbol=t.get("symbol"), error=str(e))
            except Exception as e:
                logger.error("S5Live manage_loop error", error=str(e))

    async def _manage_one(self, t: dict) -> None:
        symbol = t["symbol"]; trade_id = t["trade_id"]
        c = candles_1m.get(symbol, [])
        price = c[-1]["close"] if c else None

        # Reconcile: позиция закрыта на бирже (сработал stop-market SL) → свести PnL
        pos = await api.get_position(symbol)
        exch_qty = pos["size"] if pos else 0.0
        if exch_qty <= 0:
            await self._finalize_closed(t, reason="sl_exchange" if not t["tp1_hit"]
                                        else "trail_or_sl_exchange")
            return
        if price is None:
            return

        entry = t["entry_price"]; rr = t["rr"] or 0.0
        tp1 = t["take_profit_1"]; tp2 = t["take_profit_2"]
        tp1_hit = bool(t["tp1_hit"])

        # TP2 — закрыть остаток рынком
        if price >= tp2:
            try:
                await api.place_market_order(symbol=symbol, side="Sell", qty=exch_qty,
                                             reduce_only=True, order_link_id=f"s5tp2_{trade_id[:8]}")
            except Exception as e:
                logger.error("S5Live TP2 close failed", trade_id=trade_id, error=str(e))
                return
            await s5db.add_event(trade_id, "tp2_close", json.dumps({"price": price}))
            await asyncio.sleep(0.8)
            await self._finalize_closed(t, reason="take_profit_2")
            return

        # TP1 — продать 50%, перенести SL в безубыток
        if not tp1_hit and price >= tp1:
            instr = await self._instrument(symbol) or {"qty_step": 0.0}
            half = self._snap(exch_qty * S5_LIVE_TP1_FRACTION, instr["qty_step"])
            if half > 0:
                try:
                    await api.place_market_order(symbol=symbol, side="Sell", qty=half,
                                                 reduce_only=True, order_link_id=f"s5tp1_{trade_id[:8]}")
                except Exception as e:
                    logger.error("S5Live TP1 partial failed", trade_id=trade_id, error=str(e))
                    return
                # Перевесить биржевой SL на остаток по цене входа (безубыток)
                await self._replace_sl(t, remaining_after=exch_qty - half, new_sl=entry)
                await s5db.update_trade(trade_id, tp1_hit=1, stop_moved_to_breakeven=1,
                                        stop_loss=entry)
                await s5db.add_event(trade_id, "tp1_partial",
                                     json.dumps({"price": price, "sold_qty": half}))
                logger.info("S5Live TP1 hit, SL→BE", trade_id=trade_id, symbol=symbol)
            return

        # Трейлинг после TP1: поднять биржевой SL
        if tp1_hit and rr > 0:
            new_sl = price - rr * S5_LIVE_TRAIL_R
            if new_sl > t["stop_loss"]:
                await self._replace_sl(t, remaining_after=exch_qty, new_sl=new_sl)
                await s5db.update_trade(trade_id, stop_loss=round(new_sl, 10))
                await s5db.add_event(trade_id, "trail", json.dumps({"new_sl": round(new_sl, 10)}))

    async def _replace_sl(self, t: dict, remaining_after: float, new_sl: float) -> None:
        """Отменить старый биржевой SL и поставить новый stop-market на остаток."""
        symbol = t["symbol"]; trade_id = t["trade_id"]
        instr = await self._instrument(symbol) or {"tick": 0.0}
        trig = self._snap(new_sl, instr["tick"]) if instr.get("tick") else new_sl
        try:
            await api.cancel_all_orders(symbol)  # снимает старый stop-market
            if remaining_after > 0:
                res = await api.place_stop_market_order(
                    symbol=symbol, side="Sell", qty=remaining_after,
                    trigger_price=trig, order_link_id=f"s5sl_{trade_id[:8]}_{int(time.time())}")
                await s5db.update_trade(trade_id, sl_order_id=res.get("orderId"))
        except Exception as e:
            logger.error("S5Live: replace SL failed — position may be unprotected",
                         trade_id=trade_id, symbol=symbol, error=str(e))

    async def _finalize_closed(self, t: dict, reason: str) -> None:
        """Свести реализованный PnL по executions и закрыть запись."""
        symbol = t["symbol"]; trade_id = t["trade_id"]
        start_ms = int(t["entry_time"] * 1000) - 2000
        try:
            execs = await api.get_executions_by_symbol(symbol, start_ms=start_ms, limit=100)
        except Exception:
            execs = []
        buy_qty = buy_val = sell_qty = sell_val = fee = 0.0
        for e in execs:
            try:
                q = float(e.get("execQty", 0)); px = float(e.get("execPrice", 0))
                fee += float(e.get("execFee", 0) or 0)
                if e.get("side") == "Buy":
                    buy_qty += q; buy_val += q * px
                else:
                    sell_qty += q; sell_val += q * px
            except (ValueError, TypeError):
                continue
        if buy_qty <= 0 or sell_qty <= 0:
            # нет данных — оценим по последней цене
            c = candles_1m.get(symbol, [])
            px = c[-1]["close"] if c else t["entry_price"]
            pnl = (px - t["entry_price"]) / t["entry_price"] * S5_LIVE_POSITION_USDT
            await s5db.close_trade(trade_id, px, reason, pnl, qty_mismatch=True)
            logger.warning("S5Live: no executions, PnL estimated", trade_id=trade_id)
            return
        avg_entry = buy_val / buy_qty
        avg_exit = sell_val / sell_qty
        matched = min(buy_qty, sell_qty)
        pnl = matched * (avg_exit - avg_entry) - fee
        mismatch = abs(buy_qty - sell_qty) / buy_qty > 0.02
        await s5db.close_trade(trade_id, round(avg_exit, 8), reason, pnl, qty_mismatch=mismatch)
        logger.info("S5Live CLOSED", trade_id=trade_id, symbol=symbol, reason=reason,
                    pnl=round(pnl, 4), qty_mismatch=mismatch)
