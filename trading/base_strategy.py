"""Base class for all trading strategies (paper trading)."""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod

from data.collector import candles_1m
from bot.telegram import send_message, send_close_with_chart
from trading.trade_log import (
    get_open_trades,
    close_trade,
    update_trade_extremes,
    add_trade_event,
)
from constants import (
    STRATEGY_POSITION_SIZE_USDT,
    STRATEGY_MAX_OPEN_TRADES,
    STRATEGY_TRADE_TIMEOUT_MINUTES,
)
from logger import logger


class BaseStrategy(ABC):
    strategy_id: int       # задаётся в подклассе как атрибут класса
    strategy_name: str     # задаётся в подклассе как атрибут класса

    POSITION_SIZE_USDT: float = STRATEGY_POSITION_SIZE_USDT
    MAX_OPEN_TRADES: int = STRATEGY_MAX_OPEN_TRADES
    TRADE_TIMEOUT_MINUTES: float = STRATEGY_TRADE_TIMEOUT_MINUTES

    def __init__(self) -> None:
        self._tracker_tasks: set[asyncio.Task] = set()

    # ── Публичный интерфейс ───────────────────────────────────────────

    @abstractmethod
    async def on_event(self, event: dict) -> None:
        """Точка входа — вызывается из strategy_runner для каждого события."""

    async def _send_open_message(self, trade: dict, *args, **kwargs) -> None:
        """Telegram-сообщение об открытии сделки.
        Каждая стратегия переопределяет со своими параметрами (stop_loss, tp1, tp2 и т.д.)
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement _send_open_message")

    @abstractmethod
    async def _send_close_message(self, trade: dict, exit_price: float, reason: str) -> None:
        """Telegram-сообщение о закрытии сделки."""

    # ── Общая логика: таймаут ─────────────────────────────────────────

    async def _check_timeout(self) -> None:
        """
        Закрыть все открытые сделки этой стратегии, которые превысили
        TRADE_TIMEOUT_MINUTES. Вызывается каждые 60 сек из strategy_runner.
        """
        trades = await get_open_trades(self.strategy_id)
        now = time.time()
        for trade in trades:
            age_minutes = (now - trade["entry_time"]) / 60
            if age_minutes < self.TRADE_TIMEOUT_MINUTES:
                continue

            symbol = trade["symbol"]
            c1m = candles_1m.get(symbol, [])
            current_price = (
                c1m[-1]["close"] if c1m else trade["entry_price"]
            )

            try:
                # FIX-EXTREMES: принудительно обновить экстремумы перед закрытием.
                # _check_timeout закрывает сделку минуя _update_open_trades,
                # поэтому если _price_loop пропустил тики (рестарт, пустые свечи),
                # max_favorable/max_adverse остаются нулями.
                await update_trade_extremes(
                    trade["trade_id"],
                    current_price,
                    trade["entry_price"],
                    trade["direction"],
                )
                ep = trade["entry_price"]
                if ep > 0:
                    if trade["direction"] == "long":
                        fav = (current_price - ep) / ep * 100
                        adv = (ep - current_price) / ep * 100
                    else:
                        fav = (ep - current_price) / ep * 100
                        adv = (current_price - ep) / ep * 100
                    trade["max_favorable_pct"] = max(trade.get("max_favorable_pct") or 0.0, fav)
                    trade["max_adverse_pct"]   = max(trade.get("max_adverse_pct") or 0.0, adv)
                await self._close_and_track(trade["trade_id"], symbol, current_price, "timeout")
                await self._send_close_message(trade, current_price, "timeout")
                logger.info(
                    "Trade closed by timeout",
                    strategy=self.strategy_name,
                    trade_id=trade["trade_id"],
                    symbol=symbol,
                    age_minutes=round(age_minutes, 1),
                )
            except Exception as e:
                logger.error(
                    "Error closing timed-out trade",
                    strategy=self.strategy_name,
                    trade_id=trade["trade_id"],
                    error=str(e),
                )

    # ── Общая логика: обновление экстремумов и проверка TP/SL ─────────

    async def _update_open_trades(self, symbol: str, current_price: float) -> None:
        """
        Для всех открытых сделок этой стратегии по symbol:
        1. Обновить max_favorable_pct / max_adverse_pct.
        2. Проверить достижение stop_loss и take_profit.

        Логика TP/SL специфична для стратегии — делегируется _check_exit().
        """
        trades = await get_open_trades(self.strategy_id)
        for trade in trades:
            if trade["symbol"] != symbol:
                continue
            try:
                await update_trade_extremes(
                    trade["trade_id"],
                    current_price,
                    trade["entry_price"],
                    trade["direction"],
                )
                # Обновить dict в памяти чтобы _send_close_message видел актуальные extremes
                ep = trade["entry_price"]
                if ep > 0:
                    if trade["direction"] == "long":
                        fav = (current_price - ep) / ep * 100
                        adv = (ep - current_price) / ep * 100
                    else:
                        fav = (ep - current_price) / ep * 100
                        adv = (current_price - ep) / ep * 100
                    trade["max_favorable_pct"] = max(trade.get("max_favorable_pct") or 0.0, fav)
                    trade["max_adverse_pct"]   = max(trade.get("max_adverse_pct") or 0.0, adv)
                await self._check_exit(trade, current_price)
            except Exception as e:
                logger.error(
                    "Error updating open trade",
                    strategy=self.strategy_name,
                    trade_id=trade["trade_id"],
                    error=str(e),
                )

    @abstractmethod
    async def _check_exit(self, trade: dict, current_price: float) -> None:
        """
        Проверить условия выхода (SL / TP1 / TP2) для одной открытой сделки.
        Вызывается из _update_open_trades для каждой сделки символа.

        trade — полная строка из БД (все поля).
        current_price — цена последней 1М свечи.

        При срабатывании выхода:
          1. await close_trade(trade["trade_id"], exit_price, exit_reason)
          2. await self._send_close_message(trade, exit_price, exit_reason)
        """

    # ── Трекинг цены после закрытия ──────────────────────────────────

    async def _close_and_track(
        self,
        trade_id: str,
        symbol: str,
        exit_price: float,
        exit_reason: str,
        filled_size: float | None = None,
    ) -> None:
        """
        Закрыть сделку и запустить 30-минутный трекинг цены после закрытия.

        Использовать вместо прямого вызова close_trade() во всех стратегиях.
        Порядок строго: сначала close_trade, потом запуск трекера.
        exit_time фиксируется здесь — сразу после close_trade, до await asyncio.sleep.

        filled_size — реальный размер позиции в USDT (передаётся S2 для
        корректного pnl_usdt при частичном заполнении сетки).
        """
        await close_trade(trade_id, exit_price, exit_reason, filled_size)
        exit_time = time.time()
        self._start_post_exit_tracker(trade_id, symbol, exit_time)

    def _start_post_exit_tracker(
        self,
        trade_id: str,
        symbol: str,
        exit_time: float,
    ) -> None:
        """Запустить фоновую задачу трекинга цены после закрытия сделки."""
        from trading.price_tracker import track_post_exit_price
        task = asyncio.create_task(
            track_post_exit_price(trade_id, symbol, exit_time),
            name=f"post_exit_tracker::{trade_id}",
        )
        # Сохранить ссылку на задачу — без этого GC может её уничтожить до завершения
        self._tracker_tasks.add(task)
        task.add_done_callback(self._tracker_tasks.discard)

    # ── Вспомогательные методы ────────────────────────────────────────

    async def _send_close_with_chart(
        self,
        text: str,
        trade: dict,
        exit_price: float,
    ) -> None:
        """
        Отправить уведомление о закрытии сделки с графиком 1М.
        Вызывать из _send_close_message вместо send_message.
        Если генерация графика падает — автоматически fallback на текст.
        """
        await send_close_with_chart(
            text=text,
            symbol=trade["symbol"],
            entry_price=trade.get("entry_price"),
            exit_price=exit_price,
            level=trade.get("level"),
        )

    async def _has_open_trade_for_symbol(self, symbol: str) -> bool:
        """True если по symbol уже есть открытая сделка этой стратегии."""
        trades = await get_open_trades(self.strategy_id)
        return any(t["symbol"] == symbol for t in trades)

    async def _open_trades_count(self) -> int:
        """Количество открытых сделок этой стратегии."""
        trades = await get_open_trades(self.strategy_id)
        return len(trades)

    async def _can_open_trade(self, symbol: str) -> bool:
        """
        True если можно открыть новую сделку:
        - нет открытой сделки по этому символу
        - не превышен MAX_OPEN_TRADES
        """
        if await self._has_open_trade_for_symbol(symbol):
            return False
        if await self._open_trades_count() >= self.MAX_OPEN_TRADES:
            return False
        return True

    @staticmethod
    def _format_pct(value: float, sign: bool = True) -> str:
        """Форматировать процент для Telegram: +1.23% или -0.45%."""
        prefix = "+" if sign and value >= 0 else ""
        return f"{prefix}{value:.2f}%"

    @staticmethod
    def _format_duration(entry_time: float) -> str:
        """Время с момента входа в виде '47 мин' или '2ч 13мин'."""
        minutes = int((time.time() - entry_time) / 60)
        if minutes < 60:
            return f"{minutes} мин"
        hours, mins = divmod(minutes, 60)
        return f"{hours}ч {mins}мин"
