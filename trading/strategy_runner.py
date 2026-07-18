"""Strategy runner — подписывается на event bus и прогоняет события через все стратегии."""

from __future__ import annotations

import asyncio
import os

from trading.event_bus import subscribe
from trading.strategy1_bounce import Strategy1Bounce
from trading.strategy3_breakout import Strategy3Breakout
from trading.strategy4_breakout_long import Strategy4BreakoutLong
from trading.strategy5_continuation import Strategy5Continuation
from trading.strategy2_live import Strategy2Live
from trading.trade_log import init_trades_db, get_open_trades
from trading.price_tracker import resume_post_exit_trackers
from data.collector import candles_1m
from logger import logger


async def run_strategies() -> None:
    """
    Точка входа — запускается в asyncio.gather() из main.py.

    1. Инициализирует trades.db (paper) и live_trades.db (S2Live).
    2. Создаёт экземпляры стратегий S1, S3, S4 (paper) и Strategy2Live (live Bybit).
    3. Запускает фоновый _timeout_checker (раз в 60 сек) — таймауты paper + reconcile S2Live.
    4. Запускает фоновый _price_loop (раз в 5 сек) — только paper стратегии S1/S3/S4.
    5. Главный цикл: ждёт событие из event bus → on_event → _update_open_trades (paper).
    """
    await init_trades_db()
    logger.info("trades.db initialized")
    await resume_post_exit_trackers()

    # S3 (breakout-шорт) отключён: подтверждённый отрицательный EV на paper
    # (-53$, winrate 0.39 при R≈1:1). Вернуть — раскомментировать Strategy3Breakout ниже.
    strategies = [Strategy1Bounce(), Strategy4BreakoutLong(),
                  Strategy5Continuation()]  # , Strategy3Breakout()
    strategies[1].start_scanner()  # S4 resistance scanner
    strategies[2].start_scanner()  # S5 continuation (paper) scanner

    # S5-LIVE параллельно с paper: реальные ордера на отдельном sub-аккаунте.
    # Включается S5_LIVE=true + ключи BYBIT_S5_API_KEY/SECRET в окружении.
    # Не в списке strategies (не идёт через paper _price_loop) — у него свой
    # _manage_loop и своя s5_live_trades.db. S2 не затрагивается.
    s5_live = None
    if os.getenv("S5_LIVE", "false").lower() == "true":
        try:
            from trading.strategy5_live import Strategy5Live
            s5_live = Strategy5Live()
            s5_live.start_scanner()  # запускает детектор + _manage_loop
            logger.info("S5-LIVE запущена (sub-account, s5_live_trades.db)")
        except Exception as e:
            logger.error("S5-LIVE не стартовала (проверь BYBIT_S5_* ключи)", error=str(e))
            s5_live = None

    s2_live = Strategy2Live()
    await s2_live.initialize()
    await s2_live.recover_on_startup()

    asyncio.create_task(_timeout_checker(strategies, s2_live))
    asyncio.create_task(_price_loop(strategies))

    while True:
        event = await subscribe()
        symbol        = event.get("symbol")
        current_price = event.get("current_price")

        for strategy in strategies:
            try:
                await strategy.on_event(event)
            except Exception as e:
                logger.error(
                    "strategy on_event error",
                    strategy_id=strategy.strategy_id,
                    event_type=event.get("event_type"),
                    error=str(e),
                )

        try:
            await s2_live.on_event(event)
        except Exception as e:
            logger.error("s2_live on_event error", event_type=event.get("event_type"), error=str(e))

        if symbol and current_price:
            for strategy in strategies:
                try:
                    await strategy._update_open_trades(symbol, current_price)
                except Exception as e:
                    logger.error(
                        "strategy _update_open_trades error",
                        strategy_id=strategy.strategy_id,
                        symbol=symbol,
                        error=str(e),
                    )


async def _price_loop(strategies: list) -> None:
    """
    Каждые 5 сек берёт текущую цену из candles_1m для всех символов
    с открытыми сделками и вызывает _update_open_trades.
    Работает независимо от event bus — стоп/ТП срабатывают даже если
    monitor.py уже завершил наблюдение за уровнем.
    """
    while True:
        await asyncio.sleep(5)
        try:
            open_trades = await get_open_trades()
            symbols = {t["symbol"] for t in open_trades}
            for symbol in symbols:
                c1m = candles_1m.get(symbol)
                if not c1m:
                    continue
                current_price = c1m[-1]["close"]
                for strategy in strategies:
                    try:
                        await strategy._update_open_trades(symbol, current_price)
                    except Exception as e:
                        logger.error(
                            "price_loop _update_open_trades error",
                            strategy_id=strategy.strategy_id,
                            symbol=symbol,
                            error=str(e),
                        )

        except Exception as e:
            logger.error("price_loop error", error=str(e))


async def _timeout_checker(strategies: list, s2_live: Strategy2Live) -> None:
    """Каждые 60 сек проверяет таймауты по всем стратегиям."""
    while True:
        await asyncio.sleep(60)
        for strategy in strategies:
            try:
                await strategy._check_timeout()
            except Exception as e:
                logger.error(
                    "strategy _check_timeout error",
                    strategy_id=strategy.strategy_id,
                    error=str(e),
                )
        try:
            await s2_live.reconcile_positions()
        except Exception as e:
            logger.error("s2_live reconcile_positions error", error=str(e))
