"""SQLite storage for paper trading — trades.db."""

from __future__ import annotations

import json
import os
import time
from typing import Optional

import aiosqlite

# Railway-aware путь — рядом с history.db
try:
    from config import RAILWAY_VOLUME_MOUNT_PATH
    DB_PATH = os.path.join(RAILWAY_VOLUME_MOUNT_PATH, "trades.db")
except Exception:
    DB_PATH = os.path.join(os.path.dirname(__file__), "..", "trades.db")

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS trades (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id             INTEGER NOT NULL,
    strategy_name           TEXT NOT NULL,
    trade_id                TEXT NOT NULL UNIQUE,
    symbol                  TEXT NOT NULL,
    level                   REAL NOT NULL,
    level_type              TEXT,
    level_side              TEXT,
    entry_signal            TEXT NOT NULL,
    strength_at_entry       INTEGER,
    p_bounce_at_entry       REAL,
    expected_depth_at_entry REAL,
    approach_style          TEXT,
    vol_ratio_at_entry      REAL,
    atr_at_entry            REAL,
    entry_price             REAL NOT NULL,
    entry_time              REAL NOT NULL,
    position_size           REAL NOT NULL,
    direction               TEXT NOT NULL,
    grid_orders_json        TEXT,
    grid_fill_count         INTEGER,
    events_json             TEXT DEFAULT '[]',
    max_favorable_pct       REAL,
    max_adverse_pct         REAL,
    exit_price              REAL,
    exit_time               REAL,
    exit_reason             TEXT,
    pnl_pct                 REAL,
    pnl_usdt                REAL,
    duration_minutes        REAL,
    status                  TEXT NOT NULL DEFAULT 'open',
    created_at              REAL NOT NULL,
    updated_at              REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_symbol     ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_strategy   ON trades(strategy_id);
CREATE INDEX IF NOT EXISTS idx_trades_status     ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time);
"""

# Порядок колонок совпадает с CREATE TABLE (без id — AUTOINCREMENT)
_INSERT_COLS = (
    "strategy_id", "strategy_name", "trade_id",
    "symbol", "level", "level_type", "level_side", "entry_signal",
    "strength_at_entry", "p_bounce_at_entry", "expected_depth_at_entry",
    "approach_style", "vol_ratio_at_entry", "atr_at_entry",
    "entry_price", "entry_time", "position_size", "direction",
    "grid_orders_json", "grid_fill_count",
    "events_json",
    "max_favorable_pct", "max_adverse_pct",
    "status", "created_at", "updated_at",
)


async def init_trades_db() -> None:
    """Создать таблицу и индексы если не существуют."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(_CREATE_TABLE)
        await db.commit()
        for col_sql in [
            "ALTER TABLE trades ADD COLUMN post_exit_high_30m REAL",
            "ALTER TABLE trades ADD COLUMN post_exit_low_30m REAL",
            "ALTER TABLE trades ADD COLUMN post_exit_tracked_until REAL",
        ]:
            try:
                await db.execute(col_sql)
                await db.commit()
            except Exception:
                pass  # колонка уже существует


async def open_trade(trade: dict) -> str:
    """
    Записать новую открытую сделку.
    trade — словарь с полями из _INSERT_COLS (кроме events_json, max_*, status, created_at, updated_at —
    они выставляются здесь автоматически).
    Возвращает trade_id.
    """
    now = time.time()
    row = {
        **trade,
        "events_json": "[]",
        "max_favorable_pct": 0.0,
        "max_adverse_pct": 0.0,
        "status": "open",
        "created_at": now,
        "updated_at": now,
    }
    placeholders = ", ".join("?" for _ in _INSERT_COLS)
    cols = ", ".join(_INSERT_COLS)
    values = tuple(row.get(c) for c in _INSERT_COLS)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"INSERT INTO trades ({cols}) VALUES ({placeholders})",
            values,
        )
        await db.commit()

    return trade["trade_id"]


async def add_trade_event(
    trade_id: str,
    event_type: str,
    price: float,
    note: str = "",
) -> None:
    """
    Добавить запись в events_json сделки.
    Каждая запись: {"time": float, "type": str, "price": float, "note": str}
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT events_json FROM trades WHERE trade_id = ?", (trade_id,)
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return

        try:
            events = json.loads(row[0] or "[]")
        except Exception:
            events = []

        events.append({
            "time": time.time(),
            "type": event_type,
            "price": price,
            "note": note,
        })

        await db.execute(
            "UPDATE trades SET events_json = ?, updated_at = ? WHERE trade_id = ?",
            (json.dumps(events), time.time(), trade_id),
        )
        await db.commit()


async def update_trade_extremes(
    trade_id: str,
    current_price: float,
    entry_price: float,
    direction: str,
) -> None:
    """
    Обновить max_favorable_pct и max_adverse_pct если новые значения больше.
    direction="long":  favorable = вверх, adverse = вниз
    direction="short": favorable = вниз,  adverse = вверх
    """
    if entry_price <= 0:
        return

    if direction == "long":
        favorable = (current_price - entry_price) / entry_price * 100
        adverse   = (entry_price - current_price) / entry_price * 100
    else:
        favorable = (entry_price - current_price) / entry_price * 100
        adverse   = (current_price - entry_price) / entry_price * 100

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT max_favorable_pct, max_adverse_pct FROM trades WHERE trade_id = ?",
            (trade_id,),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return

        new_favorable = max(row[0] or 0.0, favorable)
        new_adverse   = max(row[1] or 0.0, adverse)

        await db.execute(
            """UPDATE trades
               SET max_favorable_pct = ?, max_adverse_pct = ?, updated_at = ?
               WHERE trade_id = ?""",
            (new_favorable, new_adverse, time.time(), trade_id),
        )
        await db.commit()


async def close_trade(
    trade_id: str,
    exit_price: float,
    exit_reason: str,
    filled_size: Optional[float] = None,
) -> None:
    """
    Закрыть сделку: вычислить pnl, duration и записать в БД.
    Для short: прибыль если цена упала ниже entry_price.

    filled_size — реальный размер позиции в USDT (для S2, где исполнено
    только fill_count из S2_GRID_ORDERS ордеров). Если None, используется
    полный position_size из БД.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT entry_price, entry_time, position_size, direction FROM trades WHERE trade_id = ?",
            (trade_id,),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return

        entry_price, entry_time, position_size, direction = row

        if direction == "long":
            pnl_pct = (exit_price - entry_price) / entry_price * 100
        else:
            pnl_pct = (entry_price - exit_price) / entry_price * 100

        effective_size  = filled_size if filled_size is not None else position_size
        pnl_usdt        = effective_size * pnl_pct / 100
        duration_minutes = (time.time() - entry_time) / 60
        now              = time.time()

        await db.execute(
            """UPDATE trades
               SET exit_price = ?, exit_time = ?, exit_reason = ?,
                   pnl_pct = ?, pnl_usdt = ?, duration_minutes = ?,
                   status = 'closed', updated_at = ?
               WHERE trade_id = ?""",
            (
                exit_price, now, exit_reason,
                round(pnl_pct, 6), round(pnl_usdt, 4), round(duration_minutes, 2),
                now, trade_id,
            ),
        )
        await db.commit()


async def get_open_trades(strategy_id: Optional[int] = None) -> list[dict]:
    """Вернуть все открытые сделки, опционально фильтруя по strategy_id."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if strategy_id is not None:
            async with db.execute(
                "SELECT * FROM trades WHERE status = 'open' AND strategy_id = ? ORDER BY entry_time",
                (strategy_id,),
            ) as cur:
                rows = await cur.fetchall()
        else:
            async with db.execute(
                "SELECT * FROM trades WHERE status = 'open' ORDER BY entry_time",
            ) as cur:
                rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def get_trade_stats(strategy_id: int) -> dict:
    """
    Агрегированная статистика по закрытым сделкам стратегии.
    Возвращает dict с полями: total, wins, losses, win_rate,
    avg_pnl_pct, avg_win_pct, avg_loss_pct, total_pnl_usdt,
    avg_duration_minutes, max_drawdown_pct.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT pnl_pct, pnl_usdt, duration_minutes, max_adverse_pct, grid_fill_count
               FROM trades
               WHERE status = 'closed' AND strategy_id = ?""",
            (strategy_id,),
        ) as cur:
            rows = await cur.fetchall()

    if not rows:
        return {
            "total": 0, "total_with_fills": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
            "avg_pnl_pct": 0.0, "avg_win_pct": 0.0, "avg_loss_pct": 0.0,
            "total_pnl_usdt": 0.0, "avg_duration_minutes": 0.0,
            "max_drawdown_pct": 0.0,
        }

    total      = len(rows)
    # Сделки с реальными fills (grid_fill_count может быть None для S1/S3)
    filled     = [r for r in rows if (r[4] or 0) > 0 or r[4] is None]
    total_with_fills = len(filled)

    # PnL считаем только по реальным сделкам
    wins       = [r[0] for r in filled if (r[0] or 0) > 0]
    losses     = [r[0] for r in filled if (r[0] or 0) <= 0]
    all_pnl    = [r[0] or 0.0 for r in filled]
    all_usdt   = [r[1] or 0.0 for r in filled]
    all_dur    = [r[2] or 0.0 for r in filled]
    all_adverse = [r[3] or 0.0 for r in filled]

    base = total_with_fills if total_with_fills > 0 else 1

    return {
        "total":               total,
        "total_with_fills":    total_with_fills,
        "wins":                len(wins),
        "losses":              len(losses),
        "win_rate":            round(len(wins) / base * 100, 1),
        "avg_pnl_pct":         round(sum(all_pnl) / base, 3),
        "avg_win_pct":         round(sum(wins) / len(wins), 3) if wins else 0.0,
        "avg_loss_pct":        round(sum(losses) / len(losses), 3) if losses else 0.0,
        "total_pnl_usdt":      round(sum(all_usdt), 4),
        "avg_duration_minutes": round(sum(all_dur) / base, 1),
        "max_drawdown_pct":    round(max(all_adverse), 3) if all_adverse else 0.0,
    }
