"""Strategy 2 Live — прямое исполнение на Bybit Demo без paper trading.

Принцип: Strategy2Live сама слушает event_bus, фильтрует сигналы через
Strategy2SignalFilter и напрямую выставляет ордера на Bybit Demo.

Поток:
  event_bus → on_event("proximity")  → SignalFilter.check() → выставить ордера
  event_bus → on_event("pressure")   → SignalFilter.notify_pressure()
  event_bus → on_event("breakout")   → закрыть live позицию по символу
  каждые 5 сек                       → _price_loop → _check_tp_trailing (TP/trailing)
  каждые 60 сек                      → reconcile_positions → сверить с биржей

TP/Trailing проверяется каждые 5 сек (_price_loop), как в бумажной стратегии.
SL управляется биржей (SL-ордера Bybit) + страховочная проверка в reconcile.
Включение/отключение — через флаг S2_LIVE_ENABLED (устанавливается из Telegram).
Никаких импортов из paper trading (trade_log, base_strategy).
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Optional

from trading.bybit_client import (
    cancel_all_orders,
    cancel_order,
    get_best_bid,
    get_executions,
    get_executions_by_symbol,
    get_instrument_info,
    get_open_orders_for_symbol,
    get_order_history,
    get_orderbook,
    get_open_interest_history,
    get_tickers,
    get_position,
    place_limit_order,
    place_market_order,
    place_stop_limit_order,
    place_stop_market_order,
    set_leverage,
)
from trading.live_trade_log import (
    add_live_event,
    close_live_trade,
    get_open_live_trades,
    init_live_trades_db,
    log_live_error,
    open_live_trade,
    update_live_trade,
)
from trading.strategy2_signal_filter import Strategy2SignalFilter, GridParams
from constants import (
    S2_GRID_ORDERS,
    S2_POSITION_SIZE_USDT,
    S2_NO_FILL_GRACE_SECONDS,
    S2_TRAILING_START_SECONDS,
    S2_MGMT_SLOM_EXIT,
    S2_MGMT_CONFIRM_CANDLES,
)
from bot.telegram import send_message, send_close_with_chart, send_photo_with_caption
from logger import logger

LEVERAGE = 10
MAX_OPEN_TRADES = 3

# ── TP/Trailing константы (идентичны бумажной стратегии) ─────────────────────
S2_TRAILING_PCT    = 0.005   # 0.5% — фоллбэк-отступ trailing stop (если ATR недоступен) и пол ширины
S2_TRAILING_ATR_MULT = 1.0   # отступ trailing = ATR₁ₘ × N (диапазон настройки 0.8–1.2); фикс 0.5% был слишком узким для альтов
S2_TP2_ATR_MULT    = 5.0     # TP2 = entry + ATR × 5
S2_FULL_GRID_TP_PCT = 0.0015 # 0.15% — TP при полном заполнении сетки

# [H4] Для approach=3 (G2/cautious) трейлинг запускается не ранее чем через N секунд
# после ПОСЛЕДНЕГО заполненного ордера сетки (не от entry_time).
# Даёт сетке время добрать ордера на 3-м подходе прежде чем включать управление.
S2_APPROACH3_TRAILING_DELAY_SECONDS = 60

# [H1] Причины закрытия, требующие НЕМЕДЛЕННОГО рынка (без лимит-then-market).
# Всё остальное (breakout_confirmed, full_grid_tp, trailing_stop) идёт через лимитки.
_SL_CLOSE_REASONS = {
    "stop_loss",
    "reconcile_sl_breach",
    "reconcile_sl_breached_market",
    "full_grid_sl_price_breach",
    "full_grid_sl_update_failed_market",
    "recovery_sl_breached",
}

# ── Глобальный флаг включения ─────────────────────────────────────────────────
S2_LIVE_ENABLED: bool = True


def set_live_enabled(enabled: bool) -> None:
    global S2_LIVE_ENABLED
    S2_LIVE_ENABLED = enabled
    logger.info("S2 live trading", enabled=enabled)


def is_live_enabled() -> bool:
    return S2_LIVE_ENABLED


# ── [L2] Метрики стакана на момент сигнала ──────────────────────────────────
_OB_BUCKETS = (("0-0.5", 0.5), ("0.5-1", 1.0), ("1-2", 2.0), ("2-5", 5.0))


def _ob_bucket(dist_pct: float) -> Optional[str]:
    """Имя бакета по дистанции от mid (в %), либо None если дальше 5%."""
    for name, hi in _OB_BUCKETS:
        if dist_pct < hi:
            return name
    return "2-5" if dist_pct <= 5.0 else None


def _compute_ob_metrics(ob: dict, grid_bottom: float, level: float) -> dict:
    """6 скаляров + бакетный профиль из одного снимка /v5/market/orderbook.

    Все объёмы — в USDT (price*size). Опорная цена для процентных метрик — mid;
    для ob_spread_pct — bid1/ask1 напрямую; для ob_bid_vol_grid — абсолютный
    коридор [grid_bottom, level] (mid не нужен). Возвращает {} при пустом стакане.
    """
    bids = [(float(p), float(s)) for p, s in ob.get("b", [])]
    asks = [(float(p), float(s)) for p, s in ob.get("a", [])]
    if not bids or not asks:
        return {}

    bid1, ask1 = bids[0][0], asks[0][0]
    mid = (bid1 + ask1) / 2
    spread_pct = (ask1 - bid1) / bid1 * 100 if bid1 > 0 else None

    bid_vol_top = sum(p * s for p, s in bids)
    ask_vol_top = sum(p * s for p, s in asks)
    denom = bid_vol_top + ask_vol_top
    imbalance = bid_vol_top / denom if denom > 0 else None

    bid_vol_grid = sum(p * s for p, s in bids if grid_bottom <= p <= level)
    bid_vol_1pct = sum(p * s for p, s in bids if p >= mid * 0.99) if mid > 0 else 0.0

    bid_buckets = {name: 0.0 for name, _ in _OB_BUCKETS}
    ask_buckets = {name: 0.0 for name, _ in _OB_BUCKETS}
    if mid > 0:
        for p, s in bids:
            b = _ob_bucket((mid - p) / mid * 100)
            if b:
                bid_buckets[b] += p * s
        for p, s in asks:
            b = _ob_bucket((p - mid) / mid * 100)
            if b:
                ask_buckets[b] += p * s

    raw = {
        "bid": {k: round(v, 2) for k, v in bid_buckets.items()},
        "ask": {k: round(v, 2) for k, v in ask_buckets.items()},
    }
    return {
        "ob_spread_pct":   round(spread_pct, 4) if spread_pct is not None else None,
        "ob_bid_vol_grid": round(bid_vol_grid, 2),
        "ob_bid_vol_topN": round(bid_vol_top, 2),
        "ob_ask_vol_topN": round(ask_vol_top, 2),
        "ob_imbalance":    round(imbalance, 4) if imbalance is not None else None,
        "ob_bid_vol_1pct": round(bid_vol_1pct, 2),
        "ob_raw_json":     json.dumps(raw, separators=(",", ":")),
    }


def _compute_oi_funding_metrics(ticker: Optional[dict], oi_hist: Optional[list]) -> dict:
    """3 скаляра из тикера + истории OI. funding_rate/oi_value — точечные «сейчас»
    из тикера (oi_value в USD, openInterestValue — сравнимо между символами).
    oi_change_1h_pct — % изменения OI за ~1ч из истории (в контрактах: % инвариантен
    к единицам, а изменение числа контрактов — прямая мера новых позиций). None при
    отсутствии/некорректности данных.
    """
    out = {
        "funding_rate_at_entry": None,
        "oi_value_at_entry": None,
        "oi_change_1h_pct": None,
    }
    if ticker:
        fr = ticker.get("fundingRate")
        if fr not in (None, ""):
            try:
                out["funding_rate_at_entry"] = float(fr)
            except (ValueError, TypeError):
                pass
        oiv = ticker.get("openInterestValue")
        if oiv not in (None, ""):
            try:
                out["oi_value_at_entry"] = round(float(oiv), 2)
            except (ValueError, TypeError):
                pass
    if oi_hist:
        try:
            pts = sorted(
                (
                    (int(r["timestamp"]), float(r["openInterest"]))
                    for r in oi_hist
                    if r.get("openInterest") not in (None, "") and r.get("timestamp")
                ),
                key=lambda x: x[0],
            )
            if len(pts) >= 2 and pts[0][1] > 0:
                oi_old, oi_now = pts[0][1], pts[-1][1]
                out["oi_change_1h_pct"] = round((oi_now - oi_old) / oi_old * 100, 4)
        except (ValueError, TypeError, KeyError):
            pass
    return out


class Strategy2Live:
    """Live-исполнение Strategy 2 Limit Grid на Bybit Demo.

    Слушает event_bus напрямую через on_event():
      - "proximity"  → проверить фильтр → выставить ордера на бирже
      - "pressure"   → уведомить фильтр
      - "breakout"   → закрыть открытую live позицию по символу
    """

    def __init__(self) -> None:
        self._instrument_cache: dict[str, dict] = {}
        self._signal_filter = Strategy2SignalFilter()
        self._price_loop_started: bool = False
        self._closing_trades: set[str] = set()  # guard against concurrent market_close
        # symbol -> timestamp until which new grid attempts are blocked
        self._bybit_not_live_until: dict[str, float] = {}   # 24 h, retCode=110074
        self._stop_cooldown_until: dict[str, float] = {}    # 2 h after stop

    # ── Инициализация ─────────────────────────────────────────────────────────

    async def initialize(self) -> None:
        await init_live_trades_db()
        if not self._price_loop_started:
            self._price_loop_started = True
            asyncio.create_task(self._price_loop(), name="s2live_price_loop")
        from trading.live_price_tracker import resume_post_exit_trackers
        await resume_post_exit_trackers()
        logger.info("Strategy2Live initialized")

    async def recover_on_startup(self) -> None:
        """При старте сверить открытые сделки в БД с биржей."""
        open_trades = await get_open_live_trades()
        if not open_trades:
            logger.info("Strategy2Live: no open trades to recover")
            return

        logger.info("Strategy2Live: recovering open trades", count=len(open_trades))

        for trade in open_trades:
            trade_id = trade["trade_id"]
            symbol = trade["symbol"]
            fill_count = trade.get("grid_fill_count") or 0

            try:
                position = await get_position(symbol)
            except Exception as e:
                logger.error("S2Live recovery: get_position failed",
                             trade_id=trade_id, symbol=symbol, error=str(e))
                await log_live_error(trade_id, "recover_on_startup/get_position", str(e))
                await send_message(
                    f"⚠️ [S2 Live] Старт: не удалось проверить позицию {symbol}\n"
                    f"   Сделка {trade_id[:8]} оставлена открытой — проверь вручную!"
                )
                continue

            if position is not None and float(position.get("size", 0)) > 0:
                real_qty = float(position["size"])
                real_avg_price: Optional[float] = None
                try:
                    raw = position.get("avgPrice")
                    if raw:
                        real_avg_price = float(raw)
                except (ValueError, TypeError):
                    pass

                update_fields: dict = {"bybit_position_qty": real_qty}
                if real_avg_price and real_avg_price > 0:
                    update_fields["entry_price"] = round(real_avg_price, 8)

                await update_live_trade(trade_id, **update_fields)
                await add_live_event(trade_id, "recovered",
                                     f"qty={real_qty} avg_price={real_avg_price}")

                # ── Выставить SL если его нет ─────────────────────────────────
                existing_sl = trade.get("bybit_sl_order_id")
                stop_loss = trade.get("stop_loss")
                if not existing_sl and stop_loss and real_qty > 0:
                    # Получить текущую цену чтобы проверить: триггер должен быть ниже рынка
                    from data.collector import candles_1m as _c1m
                    _c = _c1m.get(symbol, [])
                    current_price_now = float(_c[-1]["close"]) if _c else None

                    if current_price_now is not None and stop_loss >= current_price_now:
                        # SL уже пробит — немедленное рыночное закрытие
                        logger.warning(
                            "S2Live recovery: SL already breached, forcing market close",
                            trade_id=trade_id, symbol=symbol,
                            stop_loss=stop_loss, current_price=current_price_now,
                        )
                        sl_info = f"⚠️ SL пробит ({stop_loss} >= {current_price_now}) — принудительное закрытие"
                        await send_message(
                            f"🚨 [S2 Live] {symbol} — SL {stop_loss} уже пробит"
                            f" (текущая {current_price_now})\n"
                            f"   Принудительное рыночное закрытие позиции!"
                        )
                        await self._market_close(trade, "recovery_sl_breached")
                        continue
                    else:
                        try:
                            instrument = await self._get_instrument(symbol)
                            sl_trigger = self._round_price(stop_loss, instrument)
                            sl_result = await place_stop_market_order(
                                symbol=symbol,
                                side="Sell",
                                qty=real_qty,
                                trigger_price=sl_trigger,
                                order_link_id=f"s2sl_{trade_id[:8]}",
                            )
                            sl_order_id = sl_result.get("orderId")
                            await update_live_trade(trade_id, bybit_sl_order_id=sl_order_id)
                            await add_live_event(trade_id, "sl_placed_on_recovery", json.dumps({
                                "sl_order_id": sl_order_id,
                                "trigger": sl_trigger,
                                "qty": real_qty,
                            }))
                            logger.info("S2Live recovery: SL placed", trade_id=trade_id,
                                        symbol=symbol, sl_order_id=sl_order_id, trigger=sl_trigger)
                            sl_info = f"SL выставлен: {sl_trigger} (ордер #{sl_order_id[:8] if sl_order_id else '?'})"
                        except Exception as e:
                            logger.error("S2Live recovery: failed to place SL",
                                         trade_id=trade_id, symbol=symbol, error=str(e))
                            await log_live_error(trade_id, "recover_on_startup/place_sl", str(e))
                            sl_info = f"⚠️ SL НЕ выставлен: {e}"
                elif existing_sl:
                    sl_info = f"SL уже есть: #{existing_sl[:8]}"
                else:
                    sl_info = "SL не выставлен (нет stop_loss в БД)"

                await send_message(
                    f"🔄 [S2 Live] Старт: позиция {symbol} найдена на бирже\n"
                    f"   Qty: {real_qty} | Вход: {real_avg_price} | Fills: {fill_count}/{S2_GRID_ORDERS}\n"
                    f"   {sl_info}"
                )
                logger.info("S2Live recovery: position alive, synced",
                            trade_id=trade_id, symbol=symbol,
                            real_qty=real_qty, real_avg_price=real_avg_price)

            else:
                if fill_count == 0:
                    await close_live_trade(trade_id, None, "recovered_no_position", 0.0)
                    await send_message(
                        f"⚪ [S2 Live] Старт: {symbol} — позиции нет, fills=0\n"
                        f"   Сделка закрыта с PnL=0"
                    )
                else:
                    real_exit_price: Optional[float] = None
                    try:
                        history = await get_order_history(symbol, limit=50)
                        sl_order_id = trade.get("bybit_sl_order_id")
                        entry_time_ms = int(trade.get("entry_time", 0) * 1000)

                        if sl_order_id and sl_order_id in history:
                            raw = history[sl_order_id].get("avgPrice")
                            if raw:
                                real_exit_price = float(raw)

                        if real_exit_price is None:
                            for order in history.values():
                                if (order.get("side") == "Sell"
                                        and order.get("orderStatus") == "Filled"
                                        and int(order.get("updatedTime", 0)) > entry_time_ms):
                                    raw = order.get("avgPrice")
                                    if raw:
                                        real_exit_price = float(raw)
                                        break
                    except Exception as e:
                        logger.warning("S2Live recovery: could not fetch exit price",
                                       trade_id=trade_id, error=str(e))

                    entry_price = trade.get("entry_price") or 0
                    real_qty_closed = trade.get("bybit_position_qty") or 0
                    if real_exit_price and entry_price > 0 and real_qty_closed > 0:
                        pnl_usdt = real_qty_closed * (real_exit_price - entry_price)
                        exit_reason = "recovered_sl_or_manual"
                    else:
                        real_exit_price = real_exit_price or entry_price
                        pnl_usdt = 0.0
                        exit_reason = "recovered_unknown_exit"

                    await close_live_trade(trade_id, real_exit_price, exit_reason,
                                           round(pnl_usdt, 4))
                    icon = "✅" if pnl_usdt >= 0 else "🔴"
                    await send_message(
                        f"{icon} [S2 Live] Старт: {symbol} — позиции нет на бирже\n"
                        f"   Закрылась пока бот был выключен\n"
                        f"   Выход: {real_exit_price} | PnL: {pnl_usdt:+.2f} USDT"
                    )
                    logger.info("S2Live recovery: position gone, trade closed",
                                trade_id=trade_id, symbol=symbol,
                                exit_reason=exit_reason)

    # ── Ценовой цикл (каждые 5 сек) ─────────────────────────────────────────

    async def _price_loop(self) -> None:
        """Каждые 5 сек проверяет TP/trailing для всех открытых live-сделок.

        Аналог _price_loop из strategy_runner для бумажных стратегий.
        Работает независимо от reconcile — реагирует на быстрые ценовые движения
        без ожидания 60-секундного цикла. Важно для скальп-стратегии.
        """
        while True:
            await asyncio.sleep(5)
            if not S2_LIVE_ENABLED:
                continue
            try:
                trades = await get_open_live_trades()
                if not trades:
                    continue
                for trade in trades:
                    fill_count = trade.get("grid_fill_count") or 0

                    # ── Слой-3: фиксация первого филла + ранний слом-выход ──────
                    # Проверяется С МОМЕНТА ФИЛЛА (до возрастного гейта trailing),
                    # т.к. слом — ранний сигнал в первых свечах после касания.
                    if fill_count > 0:
                        if not trade.get("first_fill_time"):
                            try:
                                _now_ff = time.time()
                                await update_live_trade(trade["trade_id"], first_fill_time=_now_ff)
                                trade["first_fill_time"] = _now_ff
                                await add_live_event(trade["trade_id"], "first_fill",
                                                     json.dumps({"fill_count": fill_count}))
                            except Exception as e:
                                logger.error("S2Live: first_fill_time write error",
                                             trade_id=trade["trade_id"], error=str(e))
                        try:
                            if await self._check_slom_exit(trade):
                                continue
                        except Exception as e:
                            logger.error("S2Live price_loop: slom exit error",
                                         trade_id=trade["trade_id"], error=str(e))

                    trade_age = time.time() - trade.get("entry_time", 0)
                    if trade_age < S2_TRAILING_START_SECONDS:
                        continue
                    if fill_count == 0:
                        continue

                    # [H4] Для approach=3 (G2/cautious) — трейлинг только через
                    # S2_APPROACH3_TRAILING_DELAY_SECONDS секунд после ПОСЛЕДНЕГО
                    # заполненного ордера. Даёт сетке дотянуть ордера до закрытия,
                    # не включая управление раньше времени на истощённом уровне.
                    approach_at_entry = trade.get("approach_count_at_entry")
                    if (approach_at_entry is not None
                            and approach_at_entry >= 3
                            and trade.get("cautious_mode") == 1):
                        try:
                            grid_orders = json.loads(trade.get("grid_orders_json") or "[]")
                            filled_times = [
                                o["fill_time"] for o in grid_orders
                                if o.get("filled") and o.get("fill_time")
                            ]
                            if filled_times:
                                last_fill_ts = max(filled_times)
                                if (time.time() - last_fill_ts) < S2_APPROACH3_TRAILING_DELAY_SECONDS:
                                    continue
                        except Exception as _e:
                            logger.debug("S2Live price_loop: approach3 last_fill check error",
                                         trade_id=trade["trade_id"], error=str(_e))
                    try:
                        await self._check_tp_trailing(trade)
                    except Exception as e:
                        logger.error(
                            "S2Live price_loop: _check_tp_trailing error",
                            trade_id=trade["trade_id"],
                            symbol=trade["symbol"],
                            error=str(e),
                        )
            except Exception as e:
                logger.error("S2Live price_loop error", error=str(e))

    # ── Главный вход — события от event_bus ──────────────────────────────────

    async def on_event(self, event: dict) -> None:
        """Вызывается из strategy_runner для каждого события."""
        if not S2_LIVE_ENABLED:
            return

        event_type = event.get("event_type")

        if event_type == "pressure":
            self._signal_filter.notify_pressure(event["symbol"])
            return

        if event_type == "proximity":
            await self._try_open(event)
            return

        if event_type == "breakout":
            await self._handle_breakout(event)
            return

    # ── Открытие: фильтр → ордера на бирже ───────────────────────────────────

    async def _try_open(self, event: dict) -> None:
        """Проверить фильтры и открыть сетку на Bybit Demo если прошло."""
        symbol = event["symbol"]

        # Кулдаун: контракт не торгуется на Bybit (24 ч)
        not_live_until = self._bybit_not_live_until.get(symbol, 0)
        if time.time() < not_live_until:
            logger.debug("S2Live: symbol blocked (not live on Bybit)", symbol=symbol,
                         unblocked_in_sec=int(not_live_until - time.time()))
            return

        # Кулдаун: 2 ч после стопа
        stop_until = self._stop_cooldown_until.get(symbol, 0)
        if time.time() < stop_until:
            logger.debug("S2Live: symbol in stop cooldown", symbol=symbol,
                         unblocked_in_sec=int(stop_until - time.time()))
            return

        # Текущее состояние live-сделок для лимит-проверки
        open_trades = await get_open_live_trades()
        current_open_count = len(open_trades)
        has_open_for_symbol = any(t["symbol"] == symbol for t in open_trades)

        passed, params = await self._signal_filter.check(
            event=event,
            current_open_count=current_open_count,
            max_open_trades=MAX_OPEN_TRADES,
            has_open_for_symbol=has_open_for_symbol,
        )

        if not passed or params is None:
            return

        await self._open_grid(params)

    async def _rollback_orphaned_grid(self, symbol: str, grid_orders_placed: list) -> None:
        """Откат биржевой стороны грида, когда запись сделки в БД не удалась.
        Без записи в БД позиция не отслеживается и не получает SL — поэтому
        её нужно немедленно ликвидировать: отменить висящие лимит-ордера и,
        если часть грида уже залилась, закрыть позицию рыночным reduce_only.
        Все шаги обёрнуты в try/except — откат должен быть максимально живучим."""
        # 1. Отменить все открытые лимит-ордера сетки по символу.
        try:
            await cancel_all_orders(symbol)
            logger.info("S2Live rollback: cancel_all_orders ok", symbol=symbol)
        except Exception as e:
            logger.error("S2Live rollback: cancel_all_orders failed",
                         symbol=symbol, error=str(e))

        # 2. Если позиция уже набралась (часть ног залилась до сбоя) — закрыть рыночным.
        try:
            pos = await get_position(symbol)
            size = float(pos.get("size", 0)) if pos else 0.0
            if size > 0:
                await place_market_order(
                    symbol=symbol, side="Sell", qty=size, reduce_only=True
                )
                logger.info("S2Live rollback: position closed market",
                            symbol=symbol, qty=size)
            else:
                logger.info("S2Live rollback: no position to close", symbol=symbol)
        except Exception as e:
            logger.error("S2Live rollback: position close failed — ТРЕБУЕТСЯ "
                         "РУЧНАЯ ПРОВЕРКА позиции на бирже",
                         symbol=symbol, error=str(e))

    async def _open_grid(self, params: "GridParams") -> None:
        """Выставить лимитные ордера сетки на Bybit Demo."""
        symbol = params.symbol
        trade_id = str(uuid.uuid4())

        # Параметры инструмента
        try:
            instrument = await self._get_instrument(symbol)
        except Exception as e:
            logger.error("S2Live: failed to get instrument info", symbol=symbol, error=str(e))
            if symbol not in self._bybit_not_live_until:
                await send_message(f"⚠️ [S2 Live] {symbol} — нет параметров инструмента: {e}")
            self._bybit_not_live_until[symbol] = time.time() + 24 * 3600
            return

        # [P4] Плечо: min(LEVERAGE, maxLeverage биржи) — инструмент уже загружен.
        effective_leverage = min(LEVERAGE, instrument.get("max_leverage", LEVERAGE))
        try:
            await set_leverage(symbol, effective_leverage)
            if effective_leverage < LEVERAGE:
                logger.info("S2Live: leverage clamped to symbol max",
                            symbol=symbol, requested=LEVERAGE, effective=effective_leverage)
        except Exception as e:
            logger.error("S2Live: failed to set leverage", symbol=symbol, error=str(e))
            await send_message(
                f"⚠️ [S2 Live] {symbol} — не удалось выставить плечо x{effective_leverage}: {e}"
            )
            return

        # [L2 + OI/funding] Единый снимок рыночного контекста на сигнале — три
        # запроса ПАРАЛЛЕЛЬНО (orderbook + tickers + OI-history), ДО постановки
        # наших ордеров. Параллель, чтобы не растить латентность грида (≈1 RTT, не 3).
        # Любой сбой не блокирует вход: соответствующие метрики останутся None.
        ob_metrics: dict = {}
        oi_metrics: dict = {}
        try:
            ob_res, ticker_res, oi_res = await asyncio.gather(
                get_orderbook(symbol, limit=50),
                get_tickers(symbol),
                get_open_interest_history(symbol, interval_time="5min", limit=13),
                return_exceptions=True,
            )
            if isinstance(ob_res, dict) and ob_res:
                ob_metrics = _compute_ob_metrics(ob_res, params.grid_bottom, params.level)
            ticker = ticker_res if isinstance(ticker_res, dict) else None
            oi_hist = oi_res if isinstance(oi_res, list) else None
            oi_metrics = _compute_oi_funding_metrics(ticker, oi_hist)
        except Exception as e:
            logger.warning("S2Live: market snapshot failed", symbol=symbol, error=str(e))

        # Разместить лимитные ордера
        grid_orders_placed: list[dict] = []
        failed_count = 0

        for i, price in enumerate(params.grid_prices):
            price_rounded = self._round_price(price, instrument)
            # Верхнее утяжеление: размер берём из grid_sizes[i]; fallback на order_size.
            _gs = getattr(params, "grid_sizes", None)
            size_usdt = _gs[i] if _gs and i < len(_gs) else params.order_size
            qty = self._calc_qty(size_usdt, price, instrument)
            if qty <= 0:
                logger.error("S2Live: qty=0 for grid order", symbol=symbol, price=price)
                failed_count += 1
                continue

            order_link_id = f"s2live_{trade_id[:8]}_{i}"
            try:
                result = await place_limit_order(
                    symbol=symbol,
                    side="Buy",
                    qty=qty,
                    price=price_rounded,
                    order_link_id=order_link_id,
                )
                grid_orders_placed.append({
                    "index": i + 1,
                    "price": price_rounded,
                    "qty": qty,
                    "order_id": result["orderId"],
                    "filled": False,
                    "fill_time": None,
                    "cancelled": False,
                })
            except Exception as e:
                err = str(e)
                logger.error("S2Live: failed to place limit order", symbol=symbol,
                             index=i + 1, price=price_rounded, error=err)
                failed_count += 1
                if "retCode=110074" in err:
                    # Контракт не торгуется на Bybit — остальные ордера упадут так же.
                    self._bybit_not_live_until[symbol] = time.time() + 24 * 3600
                    logger.warning("S2Live: symbol not live on Bybit, cooldown 24h set",
                                   symbol=symbol)
                    break

        if not grid_orders_placed:
            await send_message(
                f"❌ [S2 Live] {symbol} — все {S2_GRID_ORDERS} ордеров не прошли, сетка не открыта"
            )
            return

        # Записать в БД live trades.
        # КРИТИЧНО: ордера уже на бирже. Если запись в БД упадёт (например,
        # рассинхрон схемы), сделка не попадёт в трекинг и позиция останется
        # без SL — «осиротеет». Поэтому при любом сбое записи немедленно
        # откатываем биржевую сторону: отменяем висящие лимит-ордера и, если
        # что-то уже залилось, закрываем позицию рыночным ордером.
        try:
            await open_live_trade({
                "trade_id": trade_id,
                "symbol": symbol,
                "level": params.level,
                "level_type": params.level_type,
                "entry_price": params.level,
                "entry_time": time.time(),
                "position_size_usdt": S2_POSITION_SIZE_USDT,
                "direction": "long",
                "grid_orders": grid_orders_placed,
                "bybit_order_ids": [o["order_id"] for o in grid_orders_placed],
                "bybit_sl_order_id": None,
                "strength_at_entry": params.strength,
                "p_bounce_at_entry": params.p_bounce,
                "expected_depth_at_entry": params.expected_depth,
                "ml_delta_at_entry": params.ml_delta,
                "p_fast_breakout_at_entry": params.p_fast_breakout,
                "vol_ratio_at_entry": params.vol_ratio,
                "stop_loss": params.stop_loss,
                "take_profit_1": params.take_profit_1,
                "take_profit_2": params.take_profit_2,
                # G1/G2/G4 (25.06) — для анализа
                "signal_group": params.signal_group,
                "is_flip": params.is_flip,
                "flip_breakout_time": params.flip_breakout_time,
                "flip_age_hours": params.flip_age_hours,
                "retest_number": params.retest_number,
                "approach_count_at_entry": params.approach_count,
                "cautious_mode": params.cautious_mode,
                "vol_falling": params.vol_falling,
                "approach_speed_pct": params.approach_speed_pct,
                "red_candles_streak": params.red_candles_streak,
                # [L2] метрики стакана на момент сигнала (None при сбое снимка)
                **ob_metrics,
                # [OI/funding] метрики позиционирования на момент сигнала (None при сбое)
                **oi_metrics,
            })
        except Exception as e:
            logger.error("S2Live: open_live_trade failed — откатываю ордера, "
                         "чтобы не оставить позицию без SL/трекинга",
                         symbol=symbol, trade_id=trade_id, error=str(e))
            await self._rollback_orphaned_grid(symbol, grid_orders_placed)
            await send_message(
                f"🛑 [S2 Live] {symbol} — сбой записи сделки в БД ({e}). "
                f"Грид отменён и позиция (если была) закрыта, чтобы не висеть без SL."
            )
            return
        self._signal_filter.notify_placed(symbol, params.level)
        await add_live_event(trade_id, "grid_placed", json.dumps({
            "orders": len(grid_orders_placed),
            "failed": failed_count,
            "strength": params.strength,
            "p_bounce": round(params.p_bounce, 3),
            "atr": params.atr,
        }))

        # Сохранить начальные параметры TP/SL в event-лог (аналог params_set в бумажной)
        await add_live_event(trade_id, "params_set", json.dumps({
            "stop_loss":      params.stop_loss,
            "take_profit_1":  params.take_profit_1,
            "take_profit_2":  params.take_profit_2,
            "grid_bottom":    params.grid_bottom,
            "atr":            params.atr,
            "tp1_hit":        False,
            "trailing_active": False,
            "trailing_peak":  None,
            "trailing_stop":  None,
            "full_grid_tp":   None,
            "stop_moved_to_breakeven": False,
        }))

        # ── Выставить SL-ордер на бирже ──────────────────────────────────────
        # Суммарный qty всей сетки — SL на всю потенциальную позицию.
        # trigger_price = stop_loss, order_price = stop_loss - 1 tick (гарантия исполнения).
        # Bybit требует triggerDirection=2 (Falling) — триггер ДОЛЖЕН быть ниже текущей цены.
        # При открытии сетки цена выше grid_anchor, поэтому stop_loss всегда ниже — проверка
        # добавлена для защиты от аномальных ситуаций.
        total_grid_qty = sum(o["qty"] for o in grid_orders_placed)
        sl_order_id: Optional[str] = None
        try:
            from data.collector import candles_1m as _c1m_sl
            _c_sl = _c1m_sl.get(symbol, [])
            _current_sl = float(_c_sl[-1]["close"]) if _c_sl else None
            if _current_sl is not None and params.stop_loss >= _current_sl:
                raise RuntimeError(
                    f"stop_loss {params.stop_loss} >= current_price {_current_sl}: "
                    f"триггер не может быть выше рынка для Sell stop-limit"
                )
            sl_trigger = self._round_price(params.stop_loss, instrument)
            sl_result = await place_stop_market_order(
                symbol=symbol,
                side="Sell",
                qty=total_grid_qty,
                trigger_price=sl_trigger,
                order_link_id=f"s2sl_{trade_id[:8]}",
            )
            sl_order_id = sl_result.get("orderId")
            await update_live_trade(trade_id, bybit_sl_order_id=sl_order_id)
            await add_live_event(trade_id, "sl_placed", json.dumps({
                "sl_order_id": sl_order_id,
                "trigger": sl_trigger,
                "order_price": sl_trigger,  # market — нет отдельной order_price
                "qty": total_grid_qty,
            }))
            logger.info("S2Live: SL order placed", trade_id=trade_id, symbol=symbol,
                        sl_order_id=sl_order_id, trigger=sl_trigger, qty=total_grid_qty)
        except Exception as e:
            logger.error("S2Live: failed to place SL order", symbol=symbol,
                         trade_id=trade_id, error=str(e))
            await log_live_error(trade_id, "open_grid/place_sl", str(e))
            await send_message(
                f"⚠️ [S2 Live] {symbol} — сетка открыта, но SL-ордер НЕ выставлен!\n"
                f"   Ошибка: {e}\n"
                f"   Установи SL вручную на уровне {params.stop_loss}"
            )

        prices_str = "  ".join(f"#{o['index']}: {o['price']}" for o in grid_orders_placed)
        failed_str = f"\n   ⚠️ Не размещено: {failed_count}" if failed_count else ""
        sl_str = f"{params.stop_loss} (ордер #{sl_order_id[:8] if sl_order_id else 'НЕТ'})"
        _grid_text = (
            f"🟢 [S2 Live] {symbol} — сетка на Bybit Demo\n"
            f"   Уровень: {params.level} ({params.level_type}, strength={params.strength})"
            f" | p_bounce={params.p_bounce:.2f}\n"
            f"   Ордера ({len(grid_orders_placed)}/{S2_GRID_ORDERS}):\n"
            f"     {prices_str}{failed_str}\n"
            f"   SL: {sl_str} | TP1: {params.take_profit_1} | TP2: {params.take_profit_2}\n"
            f"   Плечо: x{LEVERAGE} | Размер: {S2_POSITION_SIZE_USDT} USDT"
        )
        # Возможная TVH — ближайший к текущей цене (последний размещённый) ордер сетки.
        _possible_entry_price = grid_orders_placed[-1]["price"] if grid_orders_placed else None
        await send_close_with_chart(
            _grid_text, symbol,
            entry_price=_possible_entry_price,
            level=params.level,
            entry_time=time.time(),
            exit_time=time.time(),
        )
        logger.info("S2Live grid opened", trade_id=trade_id, symbol=symbol,
                    orders=len(grid_orders_placed))

        # [P5] Верхние ноги сетки, выставленные выше рынка, заливаются мгновенно,
        # а _sync_grid_fills крутится только в _price_loop (раз в 5с) — до первого
        # прохода grid_fill_count=0 и бот секунды управляет фантомной позицией.
        # Синхронизируем филлы сразу после постановки.
        try:
            fresh_trade = await self._reload_trade(trade_id)
            if fresh_trade:
                await self._sync_grid_fills(fresh_trade)
        except Exception as e:
            logger.warning("S2Live: immediate sync_grid_fills failed",
                           trade_id=trade_id, symbol=symbol, error=str(e))

    # ── Breakout: закрыть live позицию по символу ─────────────────────────────

    async def _handle_breakout(self, event: dict) -> None:
        """При пробое уровня — закрыть все live позиции по этому символу."""
        symbol = event["symbol"]
        level = event.get("level")

        open_trades = await get_open_live_trades()
        for trade in open_trades:
            if trade["symbol"] != symbol:
                continue
            # Проверяем что breakout относится к нашему уровню (5% tolerance)
            if level and trade.get("level"):
                # [FIX HIGH-4] знаменатель = trade["level"] (не max(...,1)):
                # для sub-1 цен max вернул бы 1 и превратил 0.5%-проверку в абсолютную ±0.005.
                trade_level = trade["level"]
                if trade_level > 0 and abs(trade_level - level) / trade_level > 0.005:
                    continue

            logger.info("S2Live: breakout detected, closing position",
                        trade_id=trade["trade_id"], symbol=symbol)
            await self._market_close(trade, "breakout_confirmed")

    # ── Рыночное закрытие ─────────────────────────────────────────────────────

    async def _market_close(self, trade: dict, reason: str) -> None:
        """Закрыть позицию рыночным ордером. Все данные — только с биржи."""
        trade_id = trade["trade_id"]
        symbol = trade["symbol"]

        # Guard: prevent concurrent/repeated close attempts for the same trade
        if trade_id in self._closing_trades:
            logger.debug("S2Live: _market_close skipped — already in progress",
                         trade_id=trade_id, reason=reason)
            return
        self._closing_trades.add(trade_id)
        try:
            await self._do_market_close(trade, reason, trade_id, symbol)
        finally:
            self._closing_trades.discard(trade_id)

    async def _limit_then_market_sell(
        self, symbol: str, trade_id: str, reason: str
    ) -> Optional[str]:
        """[H1] Закрыть лонг лимит-then-market (для не-SL причин).

        3 попытки лимиткой по лучшему биду, по 2с каждая; перед каждой перечитывает
        остаток позиции и бид. Неисполненную лимитку отменяет. Остаток после 3 попыток
        добивает рынком — закрытие гарантировано. Возвращает order_id последнего
        размещённого ордера (рыночного остатка, либо последней лимитки если позиция
        закрылась лимитками) — он идёт в _fetch_real_exit_price как fallback; основной
        PnL всё равно сводится из всех исполнений (P1).
        """
        instrument = await self._get_instrument(symbol)
        last_order_id: Optional[str] = None

        for attempt in range(3):
            pos = await get_position(symbol)
            rem = float(pos["size"]) if pos and pos.get("size") else 0.0
            if rem <= 0:
                return last_order_id  # закрыто лимитками

            bid = await get_best_bid(symbol)
            if bid is None:
                logger.warning("S2Live[H1]: no bid, switching to market",
                               trade_id=trade_id, symbol=symbol, attempt=attempt + 1)
                break

            price = self._round_price(bid, instrument)
            try:
                res = await place_limit_order(
                    symbol=symbol, side="Sell", qty=rem, price=price,
                    reduce_only=True,
                    order_link_id=f"s2cl_{trade_id[:8]}_{attempt}",
                )
                oid = res.get("orderId")
                last_order_id = oid
                logger.info("S2Live[H1]: limit close placed", trade_id=trade_id, symbol=symbol,
                            attempt=attempt + 1, price=price, qty=rem, reason=reason)
            except Exception as e:
                logger.warning("S2Live[H1]: limit close place failed, switching to market",
                               trade_id=trade_id, symbol=symbol, attempt=attempt + 1, error=str(e))
                break

            await asyncio.sleep(2)
            # Отменить неисполненный (или частично исполненный) остаток лимитки
            try:
                await cancel_order(symbol, oid)
            except Exception:
                pass

        # Остаток — рынком (гарантия закрытия)
        pos = await get_position(symbol)
        rem = float(pos["size"]) if pos and pos.get("size") else 0.0
        if rem <= 0:
            logger.info("S2Live[H1]: closed fully by limit orders",
                        trade_id=trade_id, symbol=symbol, reason=reason)
            return last_order_id
        result = await place_market_order(symbol=symbol, side="Sell", qty=rem, reduce_only=True)
        mkt_id = result.get("orderId")
        logger.info("S2Live[H1]: market close of remainder",
                    trade_id=trade_id, symbol=symbol,
                    qty=rem, reason=reason, order_id=mkt_id,
                    note="limit orders covered partial fill, market closed rest")
        return mkt_id

    async def _do_market_close(self, trade: dict, reason: str, trade_id: str, symbol: str) -> None:
        # Отменить все открытые ордера
        try:
            await cancel_all_orders(symbol)
        except Exception as e:
            logger.error("S2Live: cancel_all_orders failed", trade_id=trade_id, error=str(e))
            await log_live_error(trade_id, f"market_close({reason})/cancel_all", str(e))

        # Получить реальную позицию с биржи
        position = None
        try:
            position = await get_position(symbol)
        except Exception as e:
            logger.error("S2Live: get_position failed before market close",
                         trade_id=trade_id, error=str(e))
            await log_live_error(trade_id, f"market_close({reason})/get_position", str(e))
            await send_message(
                f"🚨 [S2 Live] {symbol} — не удалось получить позицию с биржи!\n"
                f"   Причина: {reason} | ПРОВЕРЬ ПОЗИЦИЮ ВРУЧНУЮ!"
            )
            return

        if position is None or float(position.get("size", 0)) <= 0:
            # Позиция уже закрыта биржей (вероятно SL до нашего опроса).
            # Восстанавливаем реальную сделку из истории исполнений — иначе PnL теряется.
            # wait_for_sell=True с 5 попытками: SL мог сработать буквально только что,
            # execution ещё не попал в /v5/execution/list.
            recon = await self._realized_pnl_from_executions(
                trade, wait_for_sell=True, sell_retries=5, sell_retry_delay=1.0
            )
            if recon:
                await update_live_trade(trade_id, entry_price=recon["entry_price"],
                                        bybit_position_qty=recon["qty"],
                                        grid_fill_count=recon["fill_count"])
                await close_live_trade(trade_id, recon["exit_price"], reason, recon["pnl_usdt"])
                if "stop" in reason or "sl" in reason:
                    self._stop_cooldown_until[symbol] = time.time() + 2 * 3600
                if trade.get("level"):
                    self._signal_filter.notify_closed(symbol, trade["level"])
                icon = "✅" if recon["pnl_usdt"] >= 0 else "🔴"
                logger.info("S2Live: reconstructed closed-on-exchange from executions",
                            trade_id=trade_id, symbol=symbol, reason=reason, **recon)
                await send_message(
                    f"{icon} [S2 Live] {symbol} закрыт на бирже (восстановлено из исполнений)\n"
                    f"   Причина: {reason} | {recon['entry_price']} → {recon['exit_price']}\n"
                    f"   PnL: {recon['pnl_usdt']:+.2f} USDT | Fills: {recon['fill_count']}/{S2_GRID_ORDERS}"
                )
                return
            logger.warning("S2Live: no position on exchange during market_close",
                           trade_id=trade_id, symbol=symbol, reason=reason)
            await close_live_trade(trade_id, None, f"{reason}_no_position", None)
            # Уведомить фильтр о закрытии
            if trade.get("level"):
                self._signal_filter.notify_closed(symbol, trade["level"])
            await send_message(
                f"⚪ [S2 Live] {symbol} — позиция закрыта, исполнений не найдено\n"
                f"   Причина попытки: {reason}"
            )
            return

        real_qty = float(position["size"])
        real_avg_price: Optional[float] = None
        try:
            raw_avg = position.get("avgPrice")
            if raw_avg:
                real_avg_price = float(raw_avg)
        except (ValueError, TypeError):
            pass

        # [H1] Исполнение продажи. SL-причины — сразу рынок (срочно). Остальные
        # (breakout / TP / trailing) — лимит-then-market: 3 попытки лимиткой по
        # лучшему биду (2с каждая), остаток добиваем рынком. P1-реконсайл ниже
        # сведёт PnL и цену выхода из всех исполнений (лимитные филлы + рынок).
        market_order_id: Optional[str] = None
        try:
            if reason in _SL_CLOSE_REASONS:
                result = await place_market_order(symbol=symbol, side="Sell",
                                                  qty=real_qty, reduce_only=True)
                market_order_id = result.get("orderId")
                logger.info("S2Live: market close executed (SL, urgent)", trade_id=trade_id,
                            symbol=symbol, qty=real_qty, reason=reason, order_id=market_order_id)
            else:
                market_order_id = await self._limit_then_market_sell(
                    symbol, trade_id, reason
                )
        except Exception as e:
            logger.error("S2Live: close execution FAILED",
                         trade_id=trade_id, symbol=symbol, qty=real_qty, error=str(e))
            await log_live_error(trade_id, f"market_close({reason})/execute", str(e))
            await send_message(
                f"🚨 [S2 Live] {symbol} — ОШИБКА закрытия!\n"
                f"   Qty: {real_qty} | Причина: {reason} | Ошибка: {e}\n"
                f"   ЗАКРОЙ ПОЗИЦИЮ ВРУЧНУЮ!"
            )
            return

        # [FIX CANCEL-RACE] Детерминированная подчистка остатка. Грид-ордер мог
        # залиться между get_position и продажей (лог: cancel «too late to cancel»
        # 110001) — тогда reduce_only закрыл лишь увиденный объём, а долив повис бы
        # до 60-сек reconcile. Повторно отменяем всё, ждём отражения фила, дочищаем
        # рынком. Делается ДО _realized_pnl_from_executions, чтобы Sell подчистки
        # попал в сведение PnL.
        try:
            await cancel_all_orders(symbol)
            await asyncio.sleep(1.0)
            leftover_pos = await get_position(symbol)
            leftover = float(leftover_pos["size"]) if leftover_pos and leftover_pos.get("size") else 0.0
            if leftover > 0:
                sweep = await place_market_order(symbol=symbol, side="Sell",
                                                 qty=leftover, reduce_only=True)
                logger.warning(
                    "S2Live: swept leftover position after close (cancel/fill race)",
                    trade_id=trade_id, symbol=symbol, leftover_qty=leftover,
                    reason=reason, order_id=sweep.get("orderId"),
                )
                await add_live_event(trade_id, "leftover_swept",
                                     json.dumps({"qty": leftover, "reason": reason}))
                await asyncio.sleep(1.0)  # дать исполнению подчистки попасть в /execution/list
        except Exception as e:
            logger.error("S2Live: leftover sweep failed — reconcile will catch remainder",
                         trade_id=trade_id, symbol=symbol, error=str(e))
            await log_live_error(trade_id, f"market_close({reason})/leftover_sweep", str(e))

        # Реальная цена исполнения с биржи.
        # avg_price_fallback передаётся для совместимости сигнатуры, но внутри НЕ используется
        # (это entry price, а не exit — использование давало BEATUSDT exit=entry).
        real_exit_price: Optional[float] = await self._fetch_real_exit_price(
            symbol, market_order_id, trade_id, avg_price_fallback=real_avg_price
        )

        if real_exit_price is None:
            # _fetch_real_exit_price не нашла цену — последний шанс: попробовать
            # _realized_pnl_from_executions (get_executions_by_symbol с ретраями).
            # Сценарий: avgPrice ордера в order_history уже есть, а executions ещё не
            # появились — fetch нашла бы её через history, но executions-путь не ждал.
            # Здесь даём 12 попыток × 1с перед тем как сдаться.
            recon_fallback = await self._realized_pnl_from_executions(
                trade, wait_for_sell=True, sell_retries=12, sell_retry_delay=1.0
            )
            if recon_fallback:
                await update_live_trade(trade_id, entry_price=recon_fallback["entry_price"],
                                        bybit_position_qty=recon_fallback["qty"],
                                        grid_fill_count=recon_fallback["fill_count"])
                await close_live_trade(trade_id, recon_fallback["exit_price"],
                                       reason, recon_fallback["pnl_usdt"])
                if "stop_loss" in reason or "sl" in reason:
                    self._stop_cooldown_until[symbol] = time.time() + 2 * 3600
                if trade.get("level"):
                    self._signal_filter.notify_closed(symbol, trade["level"])
                icon = "✅" if recon_fallback["pnl_usdt"] >= 0 else "🔴"
                logger.info("S2Live: no_exit_price recovered via executions fallback",
                            trade_id=trade_id, symbol=symbol, reason=reason, **recon_fallback)
                await send_close_with_chart(
                    f"{icon} [S2 Live] {symbol} закрыт (цена восстановлена из исполнений)\n"
                    f"   Причина: {reason} | {recon_fallback['entry_price']} → {recon_fallback['exit_price']}\n"
                    f"   PnL: {recon_fallback['pnl_usdt']:+.2f} USDT | Fills: {recon_fallback['fill_count']}/{S2_GRID_ORDERS}",
                    symbol=symbol,
                    entry_price=recon_fallback["entry_price"],
                    exit_price=recon_fallback["exit_price"],
                    level=trade.get("level"),
                    entry_time=trade.get("entry_time"),
                    exit_time=time.time(),
                )
                return
            await log_live_error(
                trade_id, f"market_close({reason})/no_exit_price",
                f"order_id={market_order_id} — биржа не вернула avgPrice"
            )
            await send_message(
                f"🚨 [S2 Live] {symbol} — закрыт, но цена исполнения НЕ ПОЛУЧЕНА\n"
                f"   Причина: {reason} | OrderId: {market_order_id}\n"
                f"   Запиши цену выхода вручную в БД!"
            )
            await close_live_trade(trade_id, None, f"{reason}_no_exit_price", None)
            # Всё равно уведомить фильтр
            if trade.get("level"):
                self._signal_filter.notify_closed(symbol, trade["level"])
            return

        # PnL по реальным ценам биржи.
        # [P1] Реализованный PnL и цену выхода сводим из ВСЕЙ истории исполнений,
        # а не из остаточного qty: если биржевой SL закрыл основной объём за секунду
        # до нашего get_position, тот вернёт лишь остаток, и расчёт
        # real_qty × (exit − entry) потеряет убыток по основной части позиции.
        # wait_for_sell=True: execution появляется на бирже с задержкой ~1-3с после
        # закрытия ордера. Без ожидания sell_qty=0 → fallback → неправильный PnL.
        # 12 попыток × 1с = максимум 12 сек ожидания, достаточно для любого ликвидного рынка.
        fill_count = trade.get("grid_fill_count") or 0
        entry_for_pnl: Optional[float] = real_avg_price  # дефолт; перезапишется из recon если доступен
        pnl_usdt: Optional[float] = None
        recon = await self._realized_pnl_from_executions(
            trade, wait_for_sell=True, sell_retries=12, sell_retry_delay=1.0
        )
        if recon:
            entry_for_pnl = recon["entry_price"]
            real_exit_price = recon["exit_price"]
            real_qty = recon["qty"]
            pnl_usdt = recon["pnl_usdt"]
            fill_count = recon["fill_count"]
            await update_live_trade(trade_id, entry_price=recon["entry_price"],
                                    bybit_position_qty=recon["qty"],
                                    grid_fill_count=recon["fill_count"])
        else:
            # Fallback: история исполнений недоступна — считаем по остатку
            # (entry_for_pnl уже = real_avg_price из дефолта выше).
            logger.warning("S2Live: executions unavailable on close, PnL may be understated",
                           trade_id=trade_id, symbol=symbol, reason=reason)
            if entry_for_pnl and entry_for_pnl > 0:
                pnl_usdt = real_qty * (real_exit_price - entry_for_pnl)
            # Исполнения недоступны ⇒ recon не учёл частичный Sell TP1. Добираем
            # его PnL из событий, иначе нога потеряется (на основном recon-пути НЕ нужно).
            partial = self._sum_partial_tp1_pnl(trade)
            if partial:
                pnl_usdt = (pnl_usdt or 0.0) + partial
                logger.info("S2Live: added partial_tp1 leg to fallback PnL",
                            trade_id=trade_id, symbol=symbol, partial_pnl=partial)
            if real_avg_price and abs(real_avg_price - (trade.get("entry_price") or 0)) > 1e-8:
                await update_live_trade(trade_id, entry_price=round(real_avg_price, 8),
                                        bybit_position_qty=real_qty)

        await close_live_trade(trade_id, real_exit_price, reason, pnl_usdt)

        # Кулдаун 2 ч после стопа
        if "stop_loss" in reason or "sl" in reason:
            self._stop_cooldown_until[symbol] = time.time() + 2 * 3600
            logger.info("S2Live: stop cooldown 2h set", symbol=symbol, reason=reason)

        # Уведомить фильтр — cooldown по уровню
        if trade.get("level"):
            self._signal_filter.notify_closed(symbol, trade["level"])

        icon = "✅" if (pnl_usdt is not None and pnl_usdt >= 0) else "🔴"
        entry_str = round(entry_for_pnl, 8) if entry_for_pnl else "нет данных"
        pnl_str = f"{pnl_usdt:+.2f} USDT" if pnl_usdt is not None else "нет данных"
        close_text = (
            f"{icon} [S2 Live] {symbol} закрыт\n"
            f"   Причина: {reason} | Выход (биржа): {real_exit_price}\n"
            f"   Вход (биржа): {entry_str} | Qty: {real_qty}\n"
            f"   PnL: {pnl_str} | Fills: {fill_count}/{S2_GRID_ORDERS}"
        )
        await send_close_with_chart(
            close_text,
            symbol=symbol,
            entry_price=entry_for_pnl,
            exit_price=real_exit_price,
            level=trade.get("level"),
            entry_time=trade.get("entry_time"),
            exit_time=time.time(),
        )

    # ── Периодическая сверка с биржей ─────────────────────────────────────────

    async def reconcile_positions(self) -> None:
        """Каждые 60 сек: сверить open live-сделки с реальным состоянием на бирже."""
        if not S2_LIVE_ENABLED:
            return

        trades = await get_open_live_trades()
        if not trades:
            return

        for trade in trades:
            try:
                await self._reconcile_one(trade)
            except Exception as e:
                logger.error("S2Live reconcile: unexpected error",
                             trade_id=trade["trade_id"], symbol=trade["symbol"], error=str(e))
                await log_live_error(trade["trade_id"], "reconcile_positions", str(e))

    async def _reconcile_one(self, trade: dict) -> None:
        trade_id = trade["trade_id"]
        symbol = trade["symbol"]
        fill_count = trade.get("grid_fill_count") or 0

        # Guard: не лезть в сделку, которую прямо сейчас закрывает _market_close
        # (price_loop / full_grid_tp / trailing) — иначе гонка и двойное закрытие.
        if trade_id in self._closing_trades:
            logger.debug("S2Live reconcile: skipped — market_close in progress",
                         trade_id=trade_id, symbol=symbol)
            return

        trade_age = time.time() - trade.get("entry_time", 0)
        if trade_age < S2_NO_FILL_GRACE_SECONDS:
            return

        position = None
        try:
            position = await get_position(symbol)
        except Exception as e:
            logger.warning("S2Live reconcile: get_position failed, skipping",
                           trade_id=trade_id, symbol=symbol, error=str(e))
            return

        await self._sync_grid_fills(trade)
        trade = await self._reload_trade(trade_id)
        if trade is None:
            return
        fill_count = trade.get("grid_fill_count") or 0

        exchange_qty = float(position["size"]) if position else 0.0

        if exchange_qty == 0:
            if fill_count == 0:
                # Быстрый круг залив→SL мог закрыться между опросами (grace 600с).
                # Сначала спросить историю исполнений, и только потом считать «no-fill».
                recon = await self._realized_pnl_from_executions(trade)
                if recon:
                    await update_live_trade(trade_id, entry_price=recon["entry_price"],
                                            bybit_position_qty=recon["qty"],
                                            grid_fill_count=recon["fill_count"])
                    try:
                        await cancel_all_orders(symbol)
                    except Exception:
                        pass
                    await close_live_trade(trade_id, recon["exit_price"],
                                           "reconcile_closed_on_exchange", recon["pnl_usdt"])
                    if trade.get("level"):
                        self._signal_filter.notify_closed(symbol, trade["level"])
                    icon = "✅" if recon["pnl_usdt"] >= 0 else "🔴"
                    logger.info("S2Live reconcile: filled+closed reconstructed from executions",
                                trade_id=trade_id, symbol=symbol, **recon)
                    await send_close_with_chart(
                        f"{icon} [S2 Live] {symbol} — восстановлено из истории биржи\n"
                        f"   {recon['entry_price']} → {recon['exit_price']} | "
                        f"PnL {recon['pnl_usdt']:+.2f} | Fills {recon['fill_count']}/{S2_GRID_ORDERS}",
                        symbol,
                        entry_price=recon["entry_price"],
                        exit_price=recon["exit_price"],
                        level=trade.get("level"),
                        entry_time=trade.get("entry_time"),
                        exit_time=time.time(),
                    )
                    return
                try:
                    await cancel_all_orders(symbol)
                except Exception as e:
                    logger.warning("S2Live reconcile: cancel_all failed",
                                   trade_id=trade_id, error=str(e))
                await close_live_trade(trade_id, None, "reconcile_no_fill", 0.0)
                if trade.get("level"):
                    self._signal_filter.notify_closed(symbol, trade["level"])
                _no_fill_text = (
                    f"⚪ [S2 Live] reconcile: {symbol} — позиции нет, fills=0\n"
                    f"   Ордера отменены, сделка закрыта с PnL=0"
                )
                await send_close_with_chart(
                    _no_fill_text, symbol,
                    level=trade.get("level"),
                    entry_time=trade.get("entry_time"),
                    exit_time=time.time(),
                )
                logger.info("S2Live reconcile: no position, no fills — closed",
                            trade_id=trade_id, symbol=symbol)
                return

            # Guard на время обработки реального закрытия — чтобы price_loop
            # (_check_tp_trailing → _market_close) не мог параллельно закрыть
            # ту же сделку вторым событием.
            if trade_id in self._closing_trades:
                return
            self._closing_trades.add(trade_id)
            try:
                # Один пустой ответ биржи (qty=0) может быть временным сбоем/
                # задержкой API, а не реальным закрытием — перепроверяем перед
                # тем как считать сделку закрытой.
                await asyncio.sleep(2)
                try:
                    position_confirm = await get_position(symbol)
                except Exception as e:
                    logger.warning("S2Live reconcile: confirm get_position failed, skipping",
                                   trade_id=trade_id, symbol=symbol, error=str(e))
                    return
                if position_confirm and float(position_confirm.get("size", 0)) > 0:
                    logger.info(
                        "S2Live reconcile: position re-appeared on confirm read — false close avoided",
                        trade_id=trade_id, symbol=symbol)
                    return

                real_exit_price: Optional[float] = None
                reconcile_exit_reason = "reconcile_closed_on_exchange"
                try:
                    history = await get_order_history(symbol, limit=50)
                    entry_time_ms = int(trade.get("entry_time", 0) * 1000)
                    sl_order_id = trade.get("bybit_sl_order_id")

                    # ── Явная проверка статуса биржевого SL-ордера ───────────────
                    # Позволяет отличить «биржевой SL сработал» от «бот закрыл сам
                    # (trailing/TP), но не записал avgPrice из-за задержки биржи».
                    if sl_order_id and sl_order_id in history:
                        sl_order = history[sl_order_id]
                        sl_status = sl_order.get("orderStatus")
                        if sl_status == "Filled":
                            # Причина известна точно — SL биржи сработал.
                            # Цену берём из ордера если готова, иначе найдём в общем поиске ниже.
                            reconcile_exit_reason = "reconcile_exchange_sl_filled"
                            raw = sl_order.get("avgPrice")
                            if raw:
                                real_exit_price = float(raw)
                        elif sl_status in ("Cancelled", "Rejected"):
                            # SL-ордер не сработал — закрытие было по другой причине
                            # (скорее всего собственный бот: trailing/TP/breakout)
                            reconcile_exit_reason = "reconcile_bot_close_no_price"

                    if real_exit_price is None:
                        for order in history.values():
                            if (order.get("side") == "Sell"
                                    and order.get("orderStatus") == "Filled"
                                    and int(order.get("updatedTime", 0)) > entry_time_ms):
                                raw = order.get("avgPrice")
                                if raw:
                                    real_exit_price = float(raw)
                                    break
                except Exception as e:
                    logger.warning("S2Live reconcile: could not fetch exit price",
                                   trade_id=trade_id, error=str(e))

                db_entry = trade.get("entry_price")
                real_qty = trade.get("bybit_position_qty")
                pnl_usdt: Optional[float] = None

                # Попытаться получить реальную среднюю цену входа с биржи
                # (актуально если reconcile не успел синхронизировать entry_price до закрытия)
                reconcile_avg_price: Optional[float] = None
                try:
                    closed_position = await get_position(symbol)
                    if closed_position:
                        raw_avg = closed_position.get("avgPrice")
                        if raw_avg:
                            reconcile_avg_price = float(raw_avg)
                except Exception:
                    pass
                effective_entry = reconcile_avg_price or db_entry

                # [P1] Реализованный PnL сводим из ВСЕЙ истории исполнений
                # (SL-добивка + остаток), а не по одному выходу и остаточному qty.
                # Причину закрытия (SL биржи / бот) и кулдаун определяет логика выше —
                # её не трогаем, заменяем только величину PnL.
                recon = await self._realized_pnl_from_executions(trade)
                if recon:
                    pnl_usdt = recon["pnl_usdt"]
                    if real_exit_price is None:
                        real_exit_price = recon["exit_price"]
                elif real_exit_price and effective_entry and real_qty:
                    pnl_usdt = real_qty * (real_exit_price - effective_entry)
                # Синхронизировать entry_price в БД если биржа вернула актуальное значение
                if reconcile_avg_price and db_entry and abs(reconcile_avg_price - db_entry) > 1e-8:
                    await update_live_trade(trade_id, entry_price=round(reconcile_avg_price, 8))

                await close_live_trade(trade_id, real_exit_price,
                                       reconcile_exit_reason, pnl_usdt)
                if "sl" in reconcile_exit_reason:
                    self._stop_cooldown_until[symbol] = time.time() + 2 * 3600
                    logger.info("S2Live: stop cooldown 2h set (reconcile)",
                                symbol=symbol, reason=reconcile_exit_reason)
                if trade.get("level"):
                    self._signal_filter.notify_closed(symbol, trade["level"])

                exit_str = str(real_exit_price) if real_exit_price else "не найдена"
                pnl_str = f"{pnl_usdt:+.2f} USDT" if pnl_usdt is not None else "неизвестен"
                reason_hint = {
                    "reconcile_exchange_sl_filled": "SL биржи сработал",
                    "reconcile_bot_close_no_price": "бот закрыл (цена не записана вовремя)",
                    "reconcile_closed_on_exchange": "неизвестная причина",
                }.get(reconcile_exit_reason, reconcile_exit_reason)
                await send_close_with_chart(
                    f"⚠️ [S2 Live] reconcile: {symbol} — позиция закрыта на бирже\n"
                    f"   Причина: {reason_hint}\n"
                    f"   Выход: {exit_str} | PnL: {pnl_str}",
                    symbol,
                    entry_price=effective_entry,
                    exit_price=real_exit_price,
                    level=trade.get("level"),
                    entry_time=trade.get("entry_time"),
                    exit_time=time.time(),
                )
                logger.info("S2Live reconcile: position closed on exchange",
                            trade_id=trade_id, symbol=symbol,
                            exit_reason=reconcile_exit_reason,
                            real_exit_price=real_exit_price, pnl_usdt=pnl_usdt)
                return
            finally:
                self._closing_trades.discard(trade_id)

        # Позиция жива — синхронизировать qty и entry.
        # Guard: на время sync qty/entry и перестановки SL-ордера — чтобы
        # _market_close (price_loop) не закрыл сделку ровно в этот момент
        # (гонка с cancel_all_orders/market-ордером на бирже).
        #
        # ВАЖНО: между чтением `position` (выше, до await _sync_grid_fills/
        # _reload_trade) и этой точкой _market_close мог уже полностью
        # отработать (add → закрытие → discard) — guard ниже его не поймает,
        # т.к. к этому моменту он уже снят. Поэтому здесь повторно проверяем
        # позицию на бирже, а не доверяем устаревшему снэпшоту.
        if trade_id in self._closing_trades:
            return
        self._closing_trades.add(trade_id)
        try:
            try:
                position = await get_position(symbol)
            except Exception as e:
                logger.warning("S2Live reconcile: re-check get_position failed, skipping",
                               trade_id=trade_id, symbol=symbol, error=str(e))
                return
            exchange_qty = float(position["size"]) if position else 0.0
            if exchange_qty == 0:
                logger.info(
                    "S2Live reconcile: position closed during reconcile (race) — skip sync",
                    trade_id=trade_id, symbol=symbol)
                return
            real_avg_price: Optional[float] = None
            try:
                raw_avg = position.get("avgPrice") if position else None
                if raw_avg:
                    real_avg_price = float(raw_avg)
            except (ValueError, TypeError):
                pass

            update_fields: dict = {}
            db_qty = trade.get("bybit_position_qty") or 0.0
            if db_qty == 0 or abs(exchange_qty - db_qty) / exchange_qty > 0.01:
                update_fields["bybit_position_qty"] = exchange_qty

            db_entry = trade.get("entry_price") or 0.0
            if real_avg_price and real_avg_price > 0:
                if db_entry == 0 or abs(real_avg_price - db_entry) / real_avg_price > 0.0001:
                    update_fields["entry_price"] = round(real_avg_price, 8)

            if update_fields:
                await update_live_trade(trade_id, **update_fields)
                await add_live_event(trade_id, "reconciled", json.dumps(update_fields))
                logger.info("S2Live reconcile: synced", trade_id=trade_id,
                            symbol=symbol, updates=update_fields)

            # ── Проверить SL-ордер на бирже: выставить если отсутствует ─────────
            existing_sl = trade.get("bybit_sl_order_id")
            stop_loss = trade.get("stop_loss")
            if stop_loss and fill_count > 0:
                sl_missing = not existing_sl
                if existing_sl:
                    # Проверить жив ли SL-ордер (мог быть отменён вручную)
                    try:
                        realtime = await get_open_orders_for_symbol(symbol)
                        history_sl = await get_order_history(symbol, limit=20)
                        sl_status = realtime.get(existing_sl) or (
                            history_sl.get(existing_sl, {}).get("orderStatus")
                        )
                        if sl_status in ("Cancelled", "Rejected", None):
                            sl_missing = True
                            logger.warning("S2Live reconcile: SL order gone, will re-place",
                                           trade_id=trade_id, symbol=symbol,
                                           sl_order_id=existing_sl, status=sl_status)
                    except Exception as e:
                        logger.warning("S2Live reconcile: could not check SL order status",
                                       trade_id=trade_id, error=str(e))

                if sl_missing:
                    try:
                        # Проверить что триггер ниже текущей цены (требование Bybit triggerDirection=2)
                        from data.collector import candles_1m as _c1m_r
                        _c_r = _c1m_r.get(symbol, [])
                        _cur_r = float(_c_r[-1]["close"]) if _c_r else None
                        if _cur_r is not None and stop_loss >= _cur_r:
                            # SL уже пробит — stop-limit выставить нельзя, закрыть рынком
                            logger.warning(
                                "S2Live reconcile: SL breached, cannot place stop-limit, forcing market close",
                                trade_id=trade_id, symbol=symbol,
                                stop_loss=stop_loss, current_price=_cur_r,
                            )
                            await send_message(
                                f"🚨 [S2 Live] {symbol} — SL {stop_loss} пробит"
                                f" (текущая {_cur_r})\n"
                                f"   Stop-limit невозможен — принудительное рыночное закрытие!"
                            )
                            # Освобождаем guard перед вызовом _market_close — он сам
                            # корректно его захватит (иначе он бы пропустил закрытие).
                            self._closing_trades.discard(trade_id)
                            await self._market_close(trade, "reconcile_sl_breached_market")
                            return
                        instrument = await self._get_instrument(symbol)
                        sl_trigger = self._round_price(stop_loss, instrument)
                        sl_result = await place_stop_market_order(
                            symbol=symbol,
                            side="Sell",
                            qty=exchange_qty,
                            trigger_price=sl_trigger,
                            order_link_id=f"s2sl_{trade_id[:8]}_{int(time.time())}",
                        )
                        new_sl_id = sl_result.get("orderId")
                        await update_live_trade(trade_id, bybit_sl_order_id=new_sl_id)
                        await add_live_event(trade_id, "sl_placed_on_reconcile", json.dumps({
                            "sl_order_id": new_sl_id,
                            "trigger": sl_trigger,
                            "qty": exchange_qty,
                        }))
                        logger.info("S2Live reconcile: SL re-placed", trade_id=trade_id,
                                    symbol=symbol, sl_order_id=new_sl_id, trigger=sl_trigger)
                        await send_message(
                            f"⚠️ [S2 Live] {symbol} — SL-ордер отсутствовал, выставлен заново\n"
                            f"   Триггер: {sl_trigger} | Qty: {exchange_qty}"
                        )
                    except Exception as e:
                        logger.error("S2Live reconcile: failed to re-place SL",
                                     trade_id=trade_id, symbol=symbol, error=str(e))
                        await log_live_error(trade_id, "reconcile/re_place_sl", str(e))
                        await send_message(
                            f"🚨 [S2 Live] {symbol} — не удалось выставить SL-ордер!\n"
                            f"   Ошибка: {e} | Установи SL вручную: {stop_loss}"
                        )
        finally:
            self._closing_trades.discard(trade_id)

        # ── Проверить TP / trailing stop выполняется в _price_loop (каждые 5 сек) ──
        # В reconcile оставляем только страховочную проверку SL на случай если
        # биржевой SL-ордер не сработал (Bybit глюк, отмена вручную и т.п.)
        # Перечитываем — price_loop мог закрыть сделку между итерациями reconcile.
        if await self._reload_trade(trade_id) is None:
            return

        # ── Страховочный SL: цена против stop_loss ───────────────────────────
        from data.collector import candles_1m
        c1m = candles_1m.get(symbol, [])
        current_price = c1m[-1]["close"] if c1m else None

        if current_price and stop_loss and fill_count > 0:
            if current_price <= stop_loss:
                logger.warning(
                    "S2Live reconcile: price below stop_loss, forcing market close",
                    trade_id=trade_id, symbol=symbol,
                    current_price=current_price, stop_loss=stop_loss,
                )
                await send_message(
                    f"🚨 [S2 Live] {symbol} — цена {current_price} ниже SL {stop_loss}\n"
                    f"   SL-ордер не сработал — принудительное рыночное закрытие!"
                )
                await self._market_close(trade, "reconcile_sl_breach")

    # ── Синхронизация fills с биржи ───────────────────────────────────────────

    async def _sync_grid_fills(self, trade: dict) -> None:
        """Проверить реальные fills ордеров на бирже и обновить в БД."""
        trade_id = trade["trade_id"]
        symbol = trade["symbol"]

        try:
            grid_orders = json.loads(trade.get("grid_orders_json") or "[]")
        except Exception:
            return

        pending = [o for o in grid_orders
                   if not o.get("filled") and not o.get("cancelled")]
        if not pending:
            return

        try:
            realtime_statuses = await get_open_orders_for_symbol(symbol)
        except Exception as e:
            logger.error("S2Live: get_open_orders_for_symbol failed",
                         trade_id=trade_id, error=str(e))
            return

        history_map: dict = {}
        try:
            history_map = await get_order_history(symbol)
        except Exception as e:
            logger.warning("S2Live: get_order_history failed in sync_fills",
                           trade_id=trade_id, error=str(e))

        changed = False
        for order in pending:
            order_id = order.get("order_id")
            if not order_id:
                continue

            status = realtime_statuses.get(order_id)
            if status is None and order_id in history_map:
                status = history_map[order_id].get("orderStatus")

            if status == "Filled":
                real_price: Optional[float] = None
                if order_id in history_map:
                    raw = history_map[order_id].get("avgPrice")
                    if raw:
                        try:
                            real_price = float(raw)
                        except (ValueError, TypeError):
                            pass

                order["filled"] = True
                order["fill_time"] = time.time()
                order["fill_price"] = real_price if real_price else order["price"]
                changed = True
                logger.info("S2Live: grid order filled", trade_id=trade_id,
                            symbol=symbol, index=order["index"],
                            fill_price=order["fill_price"])

            elif status == "Cancelled":
                order["cancelled"] = True
                changed = True

        if not changed:
            return

        filled_orders = [o for o in grid_orders if o.get("filled")]
        fill_count = len(filled_orders)

        if fill_count == 0:
            await update_live_trade(
                trade_id,
                grid_orders_json=json.dumps(grid_orders),
                grid_fill_count=0,
            )
            return

        total_qty = sum(o["qty"] for o in filled_orders)
        if total_qty > 0:
            weighted_entry = (
                sum(o.get("fill_price", o["price"]) * o["qty"] for o in filled_orders)
                / total_qty
            )
        else:
            weighted_entry = (
                sum(o.get("fill_price", o["price"]) for o in filled_orders) / fill_count
            )

        await update_live_trade(
            trade_id,
            grid_orders_json=json.dumps(grid_orders),
            grid_fill_count=fill_count,
            entry_price=round(weighted_entry, 8),
            bybit_position_qty=round(total_qty, 8),
        )
        await add_live_event(trade_id, "grid_fill", json.dumps({
            "fill_count": fill_count,
            "weighted_entry": round(weighted_entry, 8),
            "total_qty": round(total_qty, 8),
        }))

        # Пересчитать TP/SL по новому weighted_entry
        fresh_trade = await self._reload_trade(trade_id)
        if fresh_trade:
            try:
                await self._recalculate_params(fresh_trade)
            except Exception as e:
                logger.warning("S2Live: _recalculate_params failed after fill",
                               trade_id=trade_id, error=str(e))

    # ── Получение реальной цены исполнения ────────────────────────────────────

    async def _fetch_real_exit_price(
        self,
        symbol: str,
        order_id: Optional[str],
        trade_id: str,
        retries: int = 7,
        delay: float = 0.5,
        avg_price_fallback: Optional[float] = None,
    ) -> Optional[float]:
        """Получить цену исполнения рыночного ордера с биржи.

        [FIX HIGH-1] 7 попыток с нарастающей паузой (0.5→1→2→3…) вместо 3×0.5 с.
        Цепочка fallback после исчерпания попыток:
          1. get_executions_by_symbol (Sell, последние 5 мин) — реальная цена выхода
          2. mark-price из последней свечи — приближение
        avg_price_fallback (avgPrice позиции ДО закрытия = entry price) НЕ используется:
        именно он давал exit=entry для BEATUSDT.
        """
        if not order_id:
            return None

        for attempt in range(retries):
            # Нарастающая пауза: 0.5, 1, 2, 3, 3, 3, 3 сек
            pause = delay * min(2 ** attempt, 6)
            await asyncio.sleep(pause)
            try:
                executions = await get_executions(symbol, order_id)
                if executions:
                    total_qty = sum(float(e.get("execQty", 0)) for e in executions)
                    total_value = sum(
                        float(e.get("execQty", 0)) * float(e.get("execPrice", 0))
                        for e in executions
                    )
                    if total_qty > 0:
                        return round(total_value / total_qty, 8)
            except Exception as e:
                logger.warning("_fetch_real_exit_price: executions failed",
                               trade_id=trade_id, attempt=attempt, error=str(e))

            try:
                history = await get_order_history(symbol, limit=10)
                entry = history.get(order_id)
                if entry:
                    raw = entry.get("avgPrice")
                    if raw:
                        price = float(raw)
                        if price > 0:
                            return round(price, 8)
            except Exception as e:
                logger.warning("_fetch_real_exit_price: order history failed",
                               trade_id=trade_id, attempt=attempt, error=str(e))

        logger.error("_fetch_real_exit_price: all attempts failed — using fallback",
                     trade_id=trade_id, order_id=order_id)

        # Fallback 1: get_executions_by_symbol — Sell-исполнения за последние 5 минут.
        # avg_price_fallback (avgPrice позиции ДО закрытия) намеренно НЕ используется:
        # это entry price, а не exit — именно из-за него BEATUSDT записал exit=entry.
        try:
            start_ms = int((time.time() - 300) * 1000)  # последние 5 минут
            execs = await get_executions_by_symbol(symbol, start_ms=start_ms, limit=50)
            sell_execs = [e for e in execs if e.get("side") == "Sell"]
            if sell_execs:
                total_qty = sum(float(e.get("execQty", 0)) for e in sell_execs)
                total_val = sum(
                    float(e.get("execQty", 0)) * float(e.get("execPrice", 0))
                    for e in sell_execs
                )
                if total_qty > 0:
                    price = round(total_val / total_qty, 8)
                    logger.warning("_fetch_real_exit_price: fallback to get_executions_by_symbol",
                                   trade_id=trade_id, price=price, sell_execs=len(sell_execs))
                    return price
        except Exception as e:
            logger.error("_fetch_real_exit_price: get_executions_by_symbol fallback failed",
                         trade_id=trade_id, error=str(e))

        # Fallback 2: mark-price из последней свечи
        try:
            from data.collector import candles_1m as _c1m_fb
            _c = _c1m_fb.get(symbol, [])
            if _c:
                mark = float(_c[-1]["close"])
                if mark > 0:
                    logger.warning("_fetch_real_exit_price: fallback to last candle close",
                                   trade_id=trade_id, price=mark)
                    return round(mark, 8)
        except Exception as e:
            logger.error("_fetch_real_exit_price: mark-price fallback failed",
                         trade_id=trade_id, error=str(e))

        return None

    # ── Восстановление сделки из истории исполнений ───────────────────────────

    async def _realized_pnl_from_executions(
        self,
        trade: dict,
        wait_for_sell: bool = False,
        sell_retries: int = 12,
        sell_retry_delay: float = 1.0,
    ) -> Optional[dict]:
        """Восстановить закрытую сделку из истории исполнений биржи (/v5/execution/list).

        Источник истины, не зависит от того, успел ли бот увидеть позицию.
        Возвращает {entry_price, exit_price, qty, pnl_usdt, fill_count} если есть
        исполнения входа И выхода; None — если вход реально не залился (true no-fill)
        или позиция ещё открыта (есть вход, нет выхода).
        Один открытый трейд на символ (бот блокирует дубль), поэтому окно по времени
        однозначно принадлежит этой сделке.

        wait_for_sell=True: если Buy-исполнения уже есть, а Sell ещё нет — повторять
        запросы к бирже до sell_retries раз с паузой sell_retry_delay сек.
        Используется сразу после закрытия позиции, пока execution ещё не появился
        в /v5/execution/list. Без этого бот падает в fallback с неправильным PnL.
        """
        symbol = trade["symbol"]
        trade_id = trade.get("trade_id")
        start_ms = int((trade.get("entry_time", 0) - 5) * 1000)  # -5с буфер

        try:
            grid_ids = set(json.loads(trade.get("bybit_order_ids_json") or "[]"))
        except Exception:
            grid_ids = set()

        def _parse(execs: list) -> tuple:
            buy_qty = buy_val = sell_qty = sell_val = fee = 0.0
            filled: set = set()
            for e in execs:
                try:
                    q = float(e.get("execQty", 0)); p = float(e.get("execPrice", 0))
                    f = float(e.get("execFee", 0) or 0)
                except (ValueError, TypeError):
                    continue
                if q <= 0 or p <= 0:
                    continue
                fee += f
                if e.get("side") == "Buy":
                    buy_qty += q; buy_val += q * p
                    if e.get("orderId") in grid_ids:
                        filled.add(e.get("orderId"))
                elif e.get("side") == "Sell":
                    sell_qty += q; sell_val += q * p
            return buy_qty, buy_val, sell_qty, sell_val, fee, filled

        max_attempts = sell_retries if wait_for_sell else 1
        for attempt in range(max_attempts):
            try:
                execs = await get_executions_by_symbol(symbol, start_ms=start_ms, limit=100)
            except Exception as e:
                logger.warning("S2Live: get_executions_by_symbol failed",
                               trade_id=trade_id, attempt=attempt, error=str(e))
                if attempt < max_attempts - 1:
                    await asyncio.sleep(sell_retry_delay)
                    continue
                return None

            if not execs:
                if wait_for_sell and attempt < max_attempts - 1:
                    await asyncio.sleep(sell_retry_delay)
                    continue
                return None

            buy_qty, buy_val, sell_qty, sell_val, fee, filled_grid_ids = _parse(execs)

            if buy_qty <= 0:
                return None  # вход не залился — настоящий no-fill

            if sell_qty <= 0:
                if wait_for_sell and attempt < max_attempts - 1:
                    logger.debug(
                        "S2Live: sell execution not yet available, retrying",
                        trade_id=trade_id, attempt=attempt + 1, max=max_attempts,
                    )
                    await asyncio.sleep(sell_retry_delay)
                    continue
                return None  # позиция ещё открыта — не закрываем здесь

            # Оба плеча есть — считаем PnL
            break
        else:
            # Все попытки исчерпаны, sell так и не появился
            logger.warning(
                "S2Live: sell executions not found after retries",
                trade_id=trade_id, retries=max_attempts,
            )
            return None

        # [FIX BUG-PHANTOM] Реализованный PnL считаем ТОЛЬКО по сведённому объёму
        # (matched_qty = min(buy_qty, sell_qty)) против средней цены входа.
        # Раньше было pnl = sell_val - buy_val - fee: если в /execution/list попала
        # лишь одна нога частичного выхода (buy_qty=322, sell_qty=161), незакрытые
        # покупки записывались как чистый убыток (-84$ OUSDT, -58$ HMSTR). Теперь
        # незалитая часть в PnL не попадает — её добьёт reduce_only SL, а следующий
        # reconcile сведёт остаток отдельной записью.
        avg_entry = buy_val / buy_qty
        avg_exit  = sell_val / sell_qty
        matched_qty = min(buy_qty, sell_qty)
        pnl = matched_qty * (avg_exit - avg_entry) - fee

        qty_mismatch = abs(buy_qty - sell_qty) / buy_qty > 0.02
        if qty_mismatch:
            logger.warning(
                "S2Live: buy/sell qty mismatch on close — PnL booked on matched qty only, "
                "remainder left for reconcile",
                trade_id=trade_id, symbol=symbol,
                buy_qty=round(buy_qty, 8), sell_qty=round(sell_qty, 8),
                matched_qty=round(matched_qty, 8),
            )

        return {
            "entry_price": round(avg_entry, 8),
            "exit_price": round(avg_exit, 8),
            "qty": round(matched_qty, 8),
            "pnl_usdt": round(pnl, 6),
            "fill_count": len(filled_grid_ids) or 1,
            "qty_mismatch": qty_mismatch,
        }

    # ── Инструмент ────────────────────────────────────────────────────────────

    async def _get_instrument(self, symbol: str) -> dict:
        if symbol in self._instrument_cache:
            return self._instrument_cache[symbol]
        info = await get_instrument_info(symbol)
        if info is None:
            raise RuntimeError(f"instrument info not found for {symbol}")
        lot_filter = info.get("lotSizeFilter", {})
        price_filter = info.get("priceFilter", {})
        leverage_filter = info.get("leverageFilter", {})
        qty_step = float(lot_filter.get("qtyStep", "0.001"))
        min_qty = float(lot_filter.get("minOrderQty", "0.001"))
        # [FIX MIN-NOTIONAL] Минимальная стоимость ордера (qty*price). Bybit отдаёт
        # minNotionalValue в lotSizeFilter (обычно "5"); фолбэк 5 USDT. Используется
        # в _calc_qty, чтобы не слать ордер, который биржа отклонит с retCode=110094.
        try:
            min_notional = float(lot_filter.get("minNotionalValue", "5"))
        except (ValueError, TypeError):
            min_notional = 5.0
        tick_size = price_filter.get("tickSize", "0.01")
        price_scale = (
            len(tick_size.rstrip("0").split(".")[-1]) if "." in tick_size else 0
        )
        # [P4] maxLeverage из биржи — клампим LEVERAGE к нему.
        try:
            max_lev = int(float(leverage_filter.get("maxLeverage", LEVERAGE)))
        except (ValueError, TypeError):
            max_lev = LEVERAGE
        parsed = {"qty_step": qty_step, "min_qty": min_qty, "price_scale": price_scale,
                  "max_leverage": max_lev, "min_notional": min_notional}
        self._instrument_cache[symbol] = parsed
        return parsed

    def _calc_qty(self, usdt: float, price: float, instrument: dict) -> float:
        if price <= 0:
            return 0.0
        from decimal import Decimal, ROUND_DOWN
        step = instrument["qty_step"]
        raw = usdt / price
        step_d = Decimal(str(step))
        qty_d = Decimal(str(raw))
        snapped = (qty_d / step_d).to_integral_value(rounding=ROUND_DOWN) * step_d
        qty = float(snapped)
        if qty < instrument["min_qty"]:
            return 0.0
        # [FIX MIN-NOTIONAL] Отклонить ордер ниже минимальной стоимости биржи
        # (qty*price < min_notional) — иначе биржа вернёт retCode=110094 (VELVET-кейс).
        if qty * price < instrument.get("min_notional", 5.0):
            return 0.0
        return qty

    def _snap_qty(self, qty: float, instrument: dict) -> float:
        """Округлить готовый qty ВНИЗ к шагу лота (для частичного закрытия)."""
        from decimal import Decimal, ROUND_DOWN
        step_d = Decimal(str(instrument["qty_step"]))
        snapped = (Decimal(str(qty)) / step_d).to_integral_value(rounding=ROUND_DOWN) * step_d
        return float(snapped)

    def _round_price(self, price: float, instrument: dict) -> float:
        return round(price, instrument.get("price_scale", 2))

    # ── Вспомогательные ───────────────────────────────────────────────────────

    # ── Параметры TP/SL из event-лога ────────────────────────────────────────

    def _extract_params(self, trade: dict) -> dict | None:
        """Вернуть параметры из ПОСЛЕДНЕГО события params_set / params_updated.

        Используется в _check_tp_trailing — берём только последний snapshot,
        чтобы не смешивать поля разных эпох (до/после trailing-активации).
        Возвращает None если подходящих событий нет.
        """
        try:
            events = json.loads(trade.get("events_json") or "[]")
        except Exception:
            return None
        for ev in reversed(events):
            if ev.get("type") in ("params_set", "params_updated"):
                try:
                    return json.loads(ev.get("note") or "{}")
                except Exception:
                    return None
        return None

    def _extract_params_full(self, trade: dict) -> dict:
        """Собрать наиболее актуальное значение каждого поля из ВСЕХ событий params_*.

        Используется в _recalculate_params — позволяет подтянуть trailing-поля
        (trailing_active, trailing_peak, trailing_stop, tp1_hit) которые могут
        отсутствовать в самом последнем params_updated если он был записан
        до активации trailing.
        """
        try:
            events = json.loads(trade.get("events_json") or "[]")
        except Exception:
            return {}
        merged: dict = {}
        for ev in events:
            if ev.get("type") in ("params_set", "params_updated"):
                try:
                    merged.update(json.loads(ev.get("note") or "{}"))
                except Exception:
                    pass
        return merged

    async def _recalculate_params(self, trade: dict) -> None:
        """Пересчитать TP/SL после изменения weighted_entry (новый fill).

        Логика идентична бумажной Strategy2LimitGrid._recalculate_params:
          - TP1 = entry + (entry - grid_bottom)
          - TP2 = entry + ATR × S2_TP2_ATR_MULT
          - SL  = grid_bottom - ATR × 0.5  (при полной сетке: entry - ATR × 0.2)
          - full_grid_tp при fill == S2_GRID_ORDERS
        Состояние trailing сохраняется из полной истории событий (_extract_params_full).
        """
        trade_id = trade["trade_id"]
        symbol = trade["symbol"]
        fill_count = trade.get("grid_fill_count") or 0
        if fill_count == 0:
            return

        # Используем full-merge чтобы подтянуть trailing-поля из всей истории
        existing = self._extract_params_full(trade)
        if not existing:
            return

        # Если trailing уже активен — не трогаем TP1/TP2/trailing (управляются в _check_tp_trailing)
        if existing.get("trailing_active"):
            return

        weighted_entry = trade.get("entry_price") or 0.0
        if weighted_entry <= 0:
            return

        grid_bottom = existing.get("grid_bottom", weighted_entry)
        # Защита: fallback на trade-поле если atr пропал из params (не должно, но на всякий случай)
        atr = existing.get("atr") or trade.get("atr_at_entry") or 0.0
        level = trade.get("level", weighted_entry)

        # [FIX CRITICAL-1] При полной сетке цена уже у grid_bottom, поэтому
        # sl = weighted_entry - atr*0.2 оказывается ВЫШЕ текущей цены → немедленный стоп.
        # Держим SL ниже grid_bottom одинаково для частичной и полной сетки.
        # Выход при полной сетке делается через full_grid_tp / трейлинг.
        sl = grid_bottom - atr * 0.5

        # [FIX TP-RUNAWAY] TP1/TP2 считаем от УРОВНЯ, а не от weighted_entry.
        # Раньше tp1 = weighted_entry + (weighted_entry - grid_bottom): после доливок
        # weighted_entry растёт → цель убегает вверх от цены, отскок до исходного TP1
        # не засчитывается → сделка доезжает до SL (OGN -8$, паттерн full_grid).
        # Теперь цель фиксирована на том же значении, что и при постановке сетки.
        tp1 = level + (level - grid_bottom)
        tp2 = level + atr * S2_TP2_ATR_MULT

        # [FIX FULL-GRID-TP] Режим full_grid_tp отключён: при полной сетке выход идёт
        # штатным путём TP1(50%)+трейлинг/TP2/SL, как для частичной. Раньше full_grid_tp
        # заставлял «ждать возврата к уровню» и снова смещал цель — убыточно.
        full_grid_tp = None

        params = dict(existing)
        params["stop_loss"]     = round(sl, 8)
        params["take_profit_1"] = round(tp1, 8)
        params["take_profit_2"] = round(tp2, 8)
        params["full_grid_tp"]  = full_grid_tp
        # Восстановить trailing-поля из full-merge если в последнем snapshot их не было
        for key in ("tp1_hit", "trailing_active", "trailing_peak", "trailing_stop",
                    "stop_moved_to_breakeven"):
            if not params.get(key):
                val = existing.get(key)
                if val is not None:
                    params[key] = val
        params.setdefault("tp1_hit", False)
        params.setdefault("trailing_active", False)
        params.setdefault("trailing_peak", None)
        params.setdefault("trailing_stop", None)

        await update_live_trade(trade_id,
                                stop_loss=round(sl, 8),
                                take_profit_1=round(tp1, 8),
                                take_profit_2=round(tp2, 8))
        await add_live_event(trade_id, "params_updated", json.dumps(params))

        # ── При полном grid — переставить биржевой SL на новый (ужесточённый) уровень ──
        # Старый SL: grid_bottom - ATR*0.5 (ниже). Новый: entry - ATR*0.2 (выше, ближе).
        # ВАЖНО: retCode=110093 — цена уже ниже нового триггера в момент постановки.
        # Алгоритм: сначала ставим новый SL, только после успеха отменяем старый.
        # Если цена уже на уровне или ниже нового триггера — сразу market-close.
        if fill_count >= S2_GRID_ORDERS:
            sl_order_id = trade.get("bybit_sl_order_id")
            exchange_qty = trade.get("bybit_position_qty") or 0.0
            # [FIX N2-FULLGRID] При S2_GRID_ORDERS=2 эта ветка срабатывает на каждой
            # полностью заполненной сделке, хотя SL-формула теперь одинакова для
            # частичной и полной сетки (grid_bottom - 0.5*ATR) — перестановка стала
            # холостым churn (cancel+create = окно без SL + гонки). Переставляем
            # ТОЛЬКО если новый триггер реально отличается от прежнего на >=1 тик.
            try:
                _instr_chk = await self._get_instrument(symbol)
                _tick_chk = 10 ** (-_instr_chk.get("price_scale", 2))
                _prev_sl = existing.get("stop_loss") or 0.0
                if _prev_sl and abs(round(sl, 8) - _prev_sl) < _tick_chk:
                    return  # SL не изменился — биржевой ордер уже стоит правильно
            except Exception:
                pass
            try:
                from trading.bybit_client import cancel_order as _cancel_order
                from data.collector import candles_1m as _c1m_fg
                instrument = await self._get_instrument(symbol)
                sl_trigger = self._round_price(round(sl, 8), instrument)

                # Проверить текущую цену: если уже на уровне или ниже нового SL-триггера —
                # stop-market биржа отклонит (triggerDirection не выполнено). Сразу закрываем рынком.
                _c_fg = _c1m_fg.get(symbol, [])
                _cur_fg = float(_c_fg[-1]["close"]) if _c_fg else None
                if _cur_fg is not None and _cur_fg <= sl_trigger:
                    logger.warning(
                        "S2Live: full grid SL update skipped — price already at/below new trigger, "
                        "closing market immediately",
                        trade_id=trade_id, symbol=symbol,
                        current_price=_cur_fg, sl_trigger=sl_trigger,
                    )
                    await send_message(
                        f"🚨 [S2 Live] {symbol} — полная сетка: цена {_cur_fg} уже ≤ нового SL "
                        f"{sl_trigger}\n   Принудительное рыночное закрытие позиции!"
                    )
                    await self._market_close(trade, "full_grid_sl_price_breach")
                    return

                if exchange_qty > 0:
                    # Сначала ставим новый SL-МАРКЕТ (старый ещё жив — нет окна без защиты)
                    new_sl_result = await place_stop_market_order(
                        symbol=symbol,
                        side="Sell",
                        qty=exchange_qty,
                        trigger_price=sl_trigger,
                        order_link_id=f"s2sl_{trade_id[:8]}_{int(time.time())}",
                    )
                    new_sl_id = new_sl_result.get("orderId")

                    # Только после успешной постановки нового SL отменяем старый
                    if sl_order_id:
                        try:
                            await _cancel_order(symbol, sl_order_id)
                        except Exception as _ce:
                            logger.warning("S2Live: could not cancel old SL after placing new",
                                           trade_id=trade_id, old_sl=sl_order_id, error=str(_ce))

                    await update_live_trade(trade_id, bybit_sl_order_id=new_sl_id)
                    await add_live_event(trade_id, "sl_updated_full_grid", json.dumps({
                        "sl_order_id": new_sl_id,
                        "trigger": sl_trigger,
                        "qty": exchange_qty,
                    }))
                    logger.info("S2Live: full grid SL updated on exchange",
                                trade_id=trade_id, symbol=symbol,
                                sl_trigger=sl_trigger, qty=exchange_qty)
                    await send_message(
                        f"⚠️ [S2 Live] {symbol} — сетка заполнена полностью ({fill_count}/{S2_GRID_ORDERS})\n"
                        f"   Ср. вход: {round(weighted_entry, 8)} | Full-grid TP: {full_grid_tp}\n"
                        f"   Новый SL: {sl_trigger} | Ждём возврат к уровню."
                    )
            except Exception as e:
                logger.warning("S2Live: failed to update SL on exchange after full grid",
                               trade_id=trade_id, symbol=symbol, error=str(e))
                await log_live_error(trade_id, "recalculate_params/update_sl_full_grid", str(e))
                # Fallback: если перестановка SL не удалась — немедленно проверить цену
                # и закрыть рынком, не дожидаясь следующего reconcile-цикла.
                try:
                    from data.collector import candles_1m as _c1m_fb
                    _c_fb = _c1m_fb.get(symbol, [])
                    _cur_fb = float(_c_fb[-1]["close"]) if _c_fb else None
                    if _cur_fb is not None and _cur_fb <= sl:
                        logger.warning(
                            "S2Live: SL update failed and price below SL — market close fallback",
                            trade_id=trade_id, symbol=symbol,
                            current_price=_cur_fb, sl=sl,
                        )
                        await send_message(
                            f"🚨 [S2 Live] {symbol} — не удалось переставить SL после полной сетки"
                            f" и цена {_cur_fb} ≤ SL {sl}\n   Экстренное рыночное закрытие!"
                        )
                        await self._market_close(trade, "full_grid_sl_update_failed_market")
                        return
                except Exception as _fb_e:
                    logger.error("S2Live: fallback market close also failed",
                                 trade_id=trade_id, error=str(_fb_e))

        logger.info("S2Live: params recalculated", trade_id=trade_id,
                    sl=round(sl, 8), tp1=round(tp1, 8), tp2=round(tp2, 8),
                    full_grid_tp=full_grid_tp, fill_count=fill_count)

    def _detect_slom(self, symbol: str) -> bool:
        """Слом: последняя ЗАКРЫТАЯ 1m-свеча закрылась ниже low предыдущей закрытой.

        Реализует правило «3я свеча перекрыла 2ю вниз (не импульс, а слом)».
        c1m[-1] — бегущая свеча, поэтому берём две последние ЗАКРЫТЫЕ: c1m[-2] («3я»)
        и c1m[-3] («2я»). Закрытые свечи, чтобы не реагировать на внутрибарный хвост.
        """
        from data.collector import candles_1m
        c1m = candles_1m.get(symbol, [])
        if len(c1m) < 3:
            return False
        prev_closed = c1m[-3]   # «2я»
        last_closed = c1m[-2]   # «3я» (последняя закрытая)
        return last_closed["close"] < prev_closed["low"]

    async def _check_slom_exit(self, trade: dict) -> bool:
        """Слой-3 (управление позицией): ранний выход по слому в окне первых
        S2_MGMT_CONFIRM_CANDLES свечей после первого филла.

        Вход остаётся на касании (сетка как есть). Это НЕ входной гейт, а
        пост-филл управление: если в первые свечи случился слом (отскок не
        подтвердился) — выходим, не дожидаясь SL. Возвращает True если закрыли.
        """
        if not S2_MGMT_SLOM_EXIT:
            return False
        # Слом-выход только для G2 (cautious) и G4 (флип): у G1 ранний откат нормален,
        # риск ложного срабатывания выше, чем польза.
        if not (trade.get("cautious_mode") == 1 or trade.get("signal_group") == "g4"):
            return False
        if (trade.get("grid_fill_count") or 0) == 0:
            return False
        ff = trade.get("first_fill_time")
        if not ff:
            return False
        # окно подтверждения: только первые N свечей после филла
        if (time.time() - ff) > S2_MGMT_CONFIRM_CANDLES * 60.0:
            return False
        # после TP1/в трейлинге управление идёт штатно — не вмешиваемся
        params = self._extract_params(trade)
        if params and params.get("tp1_hit"):
            return False

        symbol = trade["symbol"]
        if not self._detect_slom(symbol):
            return False

        trade_id = trade["trade_id"]
        logger.info("S2Live: slom early-exit (layer-3)",
                    trade_id=trade_id, symbol=symbol,
                    group=trade.get("signal_group"),
                    cautious=trade.get("cautious_mode"),
                    age_since_fill_s=round(time.time() - ff, 1))
        try:
            await update_live_trade(trade_id, mgmt_exit_trigger="slom_3rd_candle")
        except Exception as e:
            logger.warning("S2Live: mgmt_exit_trigger write failed",
                           trade_id=trade_id, error=str(e))
        try:
            await add_live_event(trade_id, "slom_exit",
                                 json.dumps({"age_since_fill_s": round(time.time() - ff, 1)}))
        except Exception:
            pass
        # отменить биржевой SL перед рыночным закрытием (иначе двойное закрытие)
        sl_id = trade.get("bybit_sl_order_id")
        if sl_id:
            try:
                from trading.bybit_client import cancel_order as _cancel
                await _cancel(symbol, sl_id)
            except Exception as e:
                logger.warning("S2Live: could not cancel SL before slom close",
                               trade_id=trade_id, error=str(e))
        await self._market_close(trade, "slom_3rd_candle")
        return True

    @staticmethod
    def _trailing_stop_price(peak: float, atr: float) -> float:
        """trailing_stop = peak − max(ATR×mult, 0.5%×peak).

        Отступ ATR-зависимый (шире у волатильных альтов), но не уже прежних 0.5%
        и не схлопывается в ноль при нулевом/отсутствующем ATR (фоллбэк на 0.5%).
        """
        pct_off = peak * S2_TRAILING_PCT
        atr_off = (atr * S2_TRAILING_ATR_MULT) if (atr and atr > 0) else 0.0
        return round(peak - max(atr_off, pct_off), 8)

    @staticmethod
    def _sum_partial_tp1_pnl(trade: dict) -> float:
        """Сумма PnL частичных выходов TP1 из events_json (informational pnl_leg).

        Используется ТОЛЬКО в фоллбэке закрытия, когда история исполнений биржи
        недоступна и PnL считается по остатку — тогда частичная нога потерялась бы.
        На основном пути recon из исполнений уже включает частичный Sell — там НЕ звать.
        """
        try:
            events = json.loads(trade.get("events_json") or "[]")
        except Exception:
            return 0.0
        total = 0.0
        for ev in events:
            if ev.get("type") != "partial_tp1":
                continue
            try:
                leg = json.loads(ev.get("note") or "{}").get("pnl_leg")
                if leg is not None:
                    total += float(leg)
            except Exception:
                continue
        return round(total, 6)

    async def _partial_close_tp1(self, trade: dict, ref_price: float) -> None:
        """На TP1 закрыть 50% позиции рыночным reduce_only, остаток вести трейлингом.

        Сделку НЕ финализирует. Пишет informational-событие partial_tp1
        {price, qty, pnl_leg}. Финальный PnL из исполнений сам учтёт этот Sell
        (см. _realized_pnl_from_executions), поэтому pnl_leg — только для анализа
        и фоллбэка. Биржевой SL (reduce_only) закроет остаток — осиротения нет.
        Если лот не делится (нога < min_qty) — частичный выход пропускается,
        трейлинг ведётся на всю позицию.
        """
        symbol = trade["symbol"]
        trade_id = trade["trade_id"]
        try:
            instrument = await self._get_instrument(symbol)
            pos = await get_position(symbol)
        except Exception as e:
            logger.warning("S2Live: partial TP1 skipped — get_position/instrument failed",
                           trade_id=trade_id, symbol=symbol, error=str(e))
            return
        total = float(pos["size"]) if pos and pos.get("size") else 0.0
        if total <= 0:
            return
        half = self._snap_qty(total * 0.5, instrument)
        remainder = self._snap_qty(total - half, instrument)
        if half < instrument["min_qty"] or remainder < instrument["min_qty"]:
            logger.info("S2Live: partial TP1 skipped — leg below min_qty, trailing full",
                        trade_id=trade_id, symbol=symbol, total=total,
                        min_qty=instrument["min_qty"])
            return
        try:
            res = await place_market_order(symbol=symbol, side="Sell",
                                           qty=half, reduce_only=True)
        except Exception as e:
            logger.warning("S2Live: partial TP1 sell FAILED, trailing full position",
                           trade_id=trade_id, symbol=symbol, qty=half, error=str(e))
            await log_live_error(trade_id, "partial_tp1/place_market_order", str(e))
            return
        entry = trade.get("entry_price") or 0.0
        pnl_leg = round(half * (ref_price - entry), 6) if entry > 0 else None
        await add_live_event(trade_id, "partial_tp1", json.dumps({
            "price": ref_price, "qty": half, "pnl_leg": pnl_leg,
        }))
        await update_live_trade(trade_id, bybit_position_qty=remainder)
        logger.info("S2Live: partial TP1 — 50% closed, trailing remainder",
                    trade_id=trade_id, symbol=symbol, closed_qty=half,
                    remainder=remainder, order_id=res.get("orderId"))

    async def _check_tp_trailing(self, trade: dict) -> None:
        """Проверить TP1/TP2/full_grid_tp/trailing_stop по текущей цене.

        Вызывается из _price_loop каждые 5 сек пока позиция жива.
        Логика 1-в-1 с бумажной стратегией:
          1. full_grid_tp  — при полной сетке ждём возврата к уровню;
                             если не hit — проверяем SL (как в бумажной)
          2. TP2           — быстрый выход при ATR×5 без трейлинга
          3. TP1           — активировать trailing (не закрывать сразу)
          4. trailing stop — обновлять пик, закрыть при откате 0.5%
          5. обычный SL    — страховка если биржевой SL-ордер не сработал
        """
        from data.collector import candles_1m
        symbol = trade["symbol"]
        trade_id = trade["trade_id"]
        fill_count = trade.get("grid_fill_count") or 0

        if fill_count == 0:
            return

        c1m = candles_1m.get(symbol, [])
        if not c1m:
            return

        current_price = c1m[-1]["close"]
        last_high = max((c["high"] for c in c1m[-2:]), default=current_price)
        last_low  = min((c["low"]  for c in c1m[-2:]), default=current_price)

        # Используем last-only snapshot — не смешиваем поля разных эпох
        params = self._extract_params(trade)
        if not params:
            return

        tp1_hit         = params.get("tp1_hit", False)
        trailing_active = params.get("trailing_active", False)
        trailing_peak   = params.get("trailing_peak")
        trailing_stop   = params.get("trailing_stop")
        full_grid_tp    = params.get("full_grid_tp")
        take_profit_1   = params.get("take_profit_1") or trade.get("take_profit_1")
        take_profit_2   = params.get("take_profit_2") or trade.get("take_profit_2")
        stop_loss       = params.get("stop_loss") or trade.get("stop_loss")

        # ── 1. full_grid_tp ───────────────────────────────────────────────────
        if full_grid_tp is not None and not tp1_hit:
            if last_high >= full_grid_tp:
                logger.info("S2Live: full_grid_tp hit", trade_id=trade_id,
                            symbol=symbol, price=full_grid_tp)
                await self._market_close(trade, "full_grid_tp")
                return
            # Пока не hit — проверяем SL (как в бумажной стратегии)
            if stop_loss and current_price <= stop_loss:
                logger.info("S2Live: stop_loss hit (full_grid mode)", trade_id=trade_id,
                            symbol=symbol, price=current_price, stop_loss=stop_loss)
                await self._market_close(trade, "stop_loss")
            return

        # ── 2. TP2 ────────────────────────────────────────────────────────────
        if not tp1_hit and take_profit_2 and last_high >= take_profit_2:
            logger.info("S2Live: TP2 hit", trade_id=trade_id,
                        symbol=symbol, tp2=take_profit_2)
            await self._market_close(trade, "take_profit_2")
            return

        # ── 3. TP1 → активировать trailing ───────────────────────────────────
        if not tp1_hit and take_profit_1 and last_high >= take_profit_1:
            # Частично зафиксировать 50% на TP1, остаток — трейлингом.
            await self._partial_close_tp1(trade, take_profit_1)
            # [FIX WICK-PEAK] Стартовый пик = уровень TP1 (где реально зафиксировали),
            # а не last_high: фитиль (напр. 0.5406 на OUSDT при цене 0.5368) задавал
            # peak выше рынка → trailing_stop сразу оказывался над ценой → мгновенный
            # выход в минус. Берём max(tp1, current) — реальную, а не фитильную отметку.
            peak   = max(take_profit_1, current_price)
            atr    = params.get("atr") or trade.get("atr_at_entry") or 0.0
            t_stop = self._trailing_stop_price(peak, atr)
            params["tp1_hit"]                 = True
            params["trailing_active"]         = True
            params["trailing_peak"]           = peak
            params["trailing_stop"]           = t_stop
            params["stop_moved_to_breakeven"] = True
            await add_live_event(trade_id, "tp1_hit", json.dumps({
                "price": take_profit_1,
                "trailing_peak": peak,
                "trailing_stop": t_stop,
            }))
            await add_live_event(trade_id, "params_updated", json.dumps(params))
            logger.info("S2Live: TP1 hit — trailing activated",
                        trade_id=trade_id, symbol=symbol,
                        peak=peak, trailing_stop=t_stop)
            return

        # ── 4. Trailing stop ──────────────────────────────────────────────────
        if trailing_active and trailing_peak is not None:
            # Peak обновляем по current_price (close свечи), не по last_high (фитилю).
            # Фитиль = мимолётный выброс; close = цена, где рынок реально устоял.
            # Иначе фитиль задаёт peak, следующий же close < стоп → мгновенный выход.
            if current_price > trailing_peak:
                new_peak   = current_price
                atr        = params.get("atr") or trade.get("atr_at_entry") or 0.0
                new_t_stop = self._trailing_stop_price(new_peak, atr)
                params["trailing_peak"]  = new_peak
                params["trailing_stop"]  = new_t_stop
                trailing_stop = new_t_stop
                await add_live_event(trade_id, "params_updated", json.dumps(params))
                logger.debug("S2Live: trailing peak updated",
                             trade_id=trade_id, new_peak=new_peak, t_stop=new_t_stop)

            # Триггерим стоп по ТЕКУЩЕЙ цене (close), а не по внутрисвечному
            # last_low: раньше та же свеча, чей high задал peak, тут же выбивала
            # стоп своим low (диапазон альта >0.5% ⇒ откат TP1→стоп за 4-5 сек).
            if trailing_stop is not None and current_price <= trailing_stop:
                logger.info("S2Live: trailing stop hit",
                            trade_id=trade_id, symbol=symbol,
                            trailing_stop=trailing_stop, current_price=current_price)
                # Отменить биржевой SL перед рыночным закрытием (иначе двойное закрытие)
                sl_id = trade.get("bybit_sl_order_id")
                if sl_id:
                    try:
                        from trading.bybit_client import cancel_order as _cancel
                        await _cancel(symbol, sl_id)
                    except Exception as e:
                        logger.warning("S2Live: could not cancel SL before trailing close",
                                       trade_id=trade_id, error=str(e))
                await self._market_close(trade, "trailing_stop")
                return

        # ── 5. Обычный SL (до TP1, страховка) ────────────────────────────────
        if not tp1_hit and stop_loss and current_price <= stop_loss:
            logger.info("S2Live: stop_loss hit", trade_id=trade_id,
                        symbol=symbol, price=current_price, stop_loss=stop_loss)
            await self._market_close(trade, "stop_loss")


    async def _reload_trade(self, trade_id: str) -> Optional[dict]:
        trades = await get_open_live_trades()
        for t in trades:
            if t["trade_id"] == trade_id:
                return t
        return None
