"""Трекинг движения цены в течение 30 минут после закрытия сделки."""

from __future__ import annotations

import asyncio
import time

import aiosqlite

from data.collector import candles_1m
from trading.trade_log import DB_PATH
from logger import logger

# Длительность трекинга в секундах
TRACKING_DURATION_SECONDS = 1800  # 30 минут

# Интервал опроса в секундах
TRACKING_POLL_INTERVAL = 5

def _close_time_seconds(ts: float) -> float:
    """close_time → секунды независимо от единиц коллектора (мс эпоха ~1.78e12,
    с ~1.78e9). Защита от тихой поломки фильтра при смене формата свечей."""
    return ts / 1000.0 if ts > 1e11 else ts



async def track_post_exit_price(trade_id: str, symbol: str, exit_time: float) -> None:
    """
    Отслеживать high/low цены символа в течение TRACKING_DURATION_SECONDS секунд
    после exit_time и записать результат в таблицу trades.

    Запускается как отдельная asyncio-задача сразу после close_trade().
    Завершается самостоятельно по истечении 30 минут или при отмене задачи.

    Алгоритм:
    1. Каждые TRACKING_POLL_INTERVAL секунд читать candles_1m[symbol].
    2. Из свечей 1М отбирать только те, у которых close_time >= exit_time * 1000
       (close_time свечи хранится в миллисекундах, exit_time передаётся в секундах).
    3. Из отобранных свечей брать max(high) и min(low).
    4. Если candles_1m[symbol] пуст или нет подходящих свечей — пропустить итерацию,
       не записывать ничего, подождать следующий интервал.
    5. По окончании 30 минут записать финальные значения в БД и завершиться.

    Параметры:
        trade_id  — UUID строки в таблице trades (поле trade_id, не числовой id)
        symbol    — тикер, например "BTCUSDT"
        exit_time — unix timestamp закрытия сделки (float, секунды), равен time.time()
                    в момент вызова close_trade()
    """
    deadline = exit_time + TRACKING_DURATION_SECONDS
    running_high: float | None = None
    running_low: float | None = None

    logger.debug(
        "post_exit_tracker started",
        trade_id=trade_id,
        symbol=symbol,
        exit_time=exit_time,
        deadline=deadline,
    )

    try:
        while True:
            now = time.time()

            candles = candles_1m.get(symbol, [])

            # close_time в свечах — миллисекунды; exit_time — секунды
            # close_time нормализуем к секундам — не зависим от мс/с в коллекторе
            relevant = [c for c in candles if _close_time_seconds(c["close_time"]) >= exit_time]

            if relevant:
                period_high = max(c["high"] for c in relevant)
                period_low  = min(c["low"]  for c in relevant)

                if running_high is None or period_high > running_high:
                    running_high = period_high
                if running_low is None or period_low < running_low:
                    running_low = period_low

            if now >= deadline:
                break

            remaining = deadline - now
            sleep_for = min(TRACKING_POLL_INTERVAL, remaining)
            await asyncio.sleep(sleep_for)

    except asyncio.CancelledError:
        # Задача отменена (бот остановлен) — записать что накопилось
        logger.debug("post_exit_tracker cancelled", trade_id=trade_id)

    finally:
        # Записать результат в БД в любом случае: и по истечении 30 минут, и при отмене
        if running_high is not None and running_low is not None:
            await _save_post_exit_range(trade_id, running_high, running_low)
        else:
            logger.debug(
                "post_exit_tracker: no candle data to save",
                trade_id=trade_id,
                symbol=symbol,
            )


async def resume_post_exit_trackers() -> None:
    """
    Вызывать при старте бота (например из run_strategies() в strategy_runner.py):

        from trading.price_tracker import resume_post_exit_trackers
        await resume_post_exit_trackers()

    Находит закрытые сделки без post_exit данных у которых 30-минутное окно
    ещё не истекло, и перезапускает трекер для каждой.
    При рестарте бота _tracker_tasks теряются из памяти — эта функция их восстанавливает.
    """
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """SELECT trade_id, symbol, exit_time
                   FROM trades
                   WHERE status = 'closed'
                     AND post_exit_high_30m IS NULL
                     AND exit_time IS NOT NULL
                     AND exit_time >= ?""",
                (time.time() - TRACKING_DURATION_SECONDS,),
            ) as cur:
                rows = await cur.fetchall()
    except Exception as e:
        logger.error("resume_post_exit_trackers: db query failed: %s", e)
        return

    if not rows:
        return

    logger.info("resume_post_exit_trackers: resuming %d trackers after restart", len(rows))

    for row in rows:
        if row["trade_id"] in _resume_task_ids:
            continue
        _resume_task_ids.add(row["trade_id"])
        task = asyncio.create_task(
            track_post_exit_price(row["trade_id"], row["symbol"], row["exit_time"]),
            name=f"post_exit_tracker::{row['trade_id']}",
        )
        # asyncio держит ссылку на задачи в event loop пока они живы —
        # здесь этого достаточно, задачи короткоживущие (макс 30 мин).
        # Для надёжности добавляем в модульный set:
        _resume_tasks.add(task)
        task.add_done_callback(_resume_tasks.discard)
        logger.debug(
            "resume_post_exit_trackers: resumed trade_id=%s symbol=%s",
            row["trade_id"], row["symbol"],
        )


# Хранилище задач восстановления — защита от GC и дедупликация по trade_id
_resume_task_ids: set[str] = set()
_resume_tasks: set[asyncio.Task] = set()


async def _save_post_exit_range(
    trade_id: str,
    price_high: float,
    price_low: float,
) -> None:
    """Записать post_exit_high_30m, post_exit_low_30m, post_exit_tracked_until в trades."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                UPDATE trades
                SET post_exit_high_30m      = ?,
                    post_exit_low_30m       = ?,
                    post_exit_tracked_until = ?,
                    updated_at              = ?
                WHERE trade_id = ?
                """,
                (
                    round(price_high, 8),
                    round(price_low,  8),
                    round(time.time(), 3),
                    time.time(),
                    trade_id,
                ),
            )
            await db.commit()
        logger.info(
            "post_exit_tracker saved",
            trade_id=trade_id,
            high=round(price_high, 8),
            low=round(price_low,   8),
        )
    except Exception as e:
        logger.error(
            "post_exit_tracker _save_post_exit_range failed",
            trade_id=trade_id,
            error=str(e),
        )
