"""SQLite storage for live Bybit trades — live_trades/live_trades_<date>.db, по дням (UTC).

Каждая сделка живёт в файле дня её ОТКРЫТИЯ (entry_time), даже если закрывается на
следующий день. Маршрутизация trade_id → файл дня идёт через JSON-реестр
(live_trades_open_days.json, лежит рядом с папкой live_trades/) — без сканирования
прошлых файлов в обычной работе. Реестр содержит ТОЛЬКО открытые сделки: запись
удаляется при закрытии сделки, и если для даты больше не осталось открытых сделок —
её файл "финализируется" (WAL checkpoint) и больше не трогается.

Если реестр пуст/повреждён (например, файл вручную удалён) — resolve_db_path_for_trade
делает fallback-сканирование последних LOOKBACK_DAYS дней.

Схема таблицы расширена полями Bybit: bybit_order_ids, bybit_sl_order_id,
bybit_position_qty, реальный fill по биржевым данным.
"""

from __future__ import annotations

import asyncio
import glob
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiosqlite

try:
    from config import RAILWAY_VOLUME_MOUNT_PATH
    _BASE_DIR: str = RAILWAY_VOLUME_MOUNT_PATH  # "" на Aeza, "/mnt/..." на Railway
except Exception:
    _BASE_DIR = ""

# На Aeza (_BASE_DIR == "") — относительные пути: live_trades/ рядом с CWD,
# как оригинальный live_trades.db лежал в CWD.
# На Railway — абсолютный путь внутри volume.
DB_DIR = os.path.join(_BASE_DIR, "live_trades") if _BASE_DIR else "live_trades"
_REGISTRY_PATH = (os.path.join(_BASE_DIR, "live_trades_open_days.json")
                  if _BASE_DIR else "live_trades_open_days.json")

# Совместимость: другие модули, импортирующие DB_PATH, получат None и должны
# быть обновлены на resolve_db_path_for_trade() / db_paths_for_recent_days().
DB_PATH: Optional[str] = None

LOOKBACK_DAYS = 7  # fallback-сканирование, только если реестр пуст/повреждён

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS live_trades (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id                TEXT NOT NULL UNIQUE,
    paper_trade_id          TEXT,
    symbol                  TEXT NOT NULL,
    level                   REAL NOT NULL,
    level_type              TEXT,
    entry_price             REAL,
    entry_time              REAL NOT NULL,
    position_size_usdt      REAL NOT NULL,
    direction               TEXT NOT NULL DEFAULT 'long',

    -- Bybit ордера сетки
    grid_orders_json        TEXT DEFAULT '[]',
    grid_fill_count         INTEGER DEFAULT 0,

    -- Bybit IDs
    bybit_order_ids_json    TEXT DEFAULT '[]',
    bybit_sl_order_id       TEXT,
    bybit_position_qty      REAL DEFAULT 0,

    -- Параметры сигнала на момент входа
    strength_at_entry       INTEGER,
    p_bounce_at_entry       REAL,
    expected_depth_at_entry REAL,
    ml_delta_at_entry       INTEGER,
    p_fast_breakout_at_entry REAL,
    vol_ratio_at_entry      REAL,

    -- [L2] стакан на момент сигнала (форвард-запись, см. analysis L2)
    ob_spread_pct           REAL,
    ob_bid_vol_grid         REAL,
    ob_bid_vol_topn         REAL,
    ob_ask_vol_topn         REAL,
    ob_imbalance            REAL,
    ob_bid_vol_1pct         REAL,
    ob_raw_json             TEXT,

    -- [OI/funding] позиционирование на момент сигнала (форвард-запись)
    funding_rate_at_entry   REAL,
    oi_value_at_entry       REAL,
    oi_change_1h_pct        REAL,

    -- Группа сигнала и метаданные флипа (анализ G1/G2/G4)
    signal_group            TEXT,       -- 'g1' | 'g2' | 'g4'
    is_flip                 INTEGER DEFAULT 0,
    flip_breakout_time      REAL,       -- ts исходного breakout (источник флипа)
    flip_age_hours          REAL,
    retest_number           INTEGER,    -- номер ретеста после флипа (торгуем 1й)
    approach_count_at_entry INTEGER,    -- из trigger._count_approaches на входе
    cautious_mode           INTEGER DEFAULT 0,  -- 1 для G2 (трейлер + быстрый выход)
    vol_falling             INTEGER,    -- 1 если объём падал при подходе
    mgmt_exit_trigger       TEXT,       -- причина выхода по управлению; ставится на выходе
    first_fill_time         REAL,       -- время первого филла сетки

    -- SL/TP параметры
    stop_loss               REAL,
    take_profit_1           REAL,
    take_profit_2           REAL,

    -- Выход
    exit_price              REAL,
    exit_time               REAL,
    exit_reason              TEXT,
    pnl_usdt                REAL,
    duration_minutes        REAL,
    status                  TEXT NOT NULL DEFAULT 'open',

    -- post-exit трекинг (30 мин после закрытия)
    post_exit_high_30m      REAL,
    post_exit_low_30m       REAL,
    post_exit_tracked_until REAL,

    -- Служебные
    events_json             TEXT DEFAULT '[]',
    error_log_json          TEXT DEFAULT '[]',
    created_at              REAL NOT NULL,
    updated_at              REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_lt_symbol ON live_trades(symbol);
CREATE INDEX IF NOT EXISTS idx_lt_status ON live_trades(status);
"""

_registry_lock = asyncio.Lock()
_registry_cache: Optional[dict] = None


# ───────────────────────── даты / пути ─────────────────────────

def _utc_date_str(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def _db_path_for_date(date_str: str) -> str:
    return os.path.join(DB_DIR, f"live_trades_{date_str}.db")


# ───────────────────────── реестр открытых сделок ─────────────────────────

def _load_registry() -> dict:
    if not os.path.exists(_REGISTRY_PATH):
        return {}
    try:
        with open(_REGISTRY_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_registry(reg: dict) -> None:
    try:
        if _BASE_DIR:  # на Aeza _BASE_DIR == "", CWD всегда существует — makedirs не нужен
            os.makedirs(_BASE_DIR, exist_ok=True)
        tmp = _REGISTRY_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(reg, f, indent=2)
        os.replace(tmp, _REGISTRY_PATH)
    except Exception:
        pass  # реестр — best-effort; при сбое сработает fallback-сканирование


def _get_registry() -> dict:
    global _registry_cache
    if _registry_cache is None:
        _registry_cache = _load_registry()
    return _registry_cache


async def _set_trade_date(trade_id: str, date_str: str) -> None:
    async with _registry_lock:
        reg = _get_registry()
        reg[trade_id] = date_str
        _save_registry(reg)


async def _pop_trade_date(trade_id: str) -> Optional[str]:
    async with _registry_lock:
        reg = _get_registry()
        date_str = reg.pop(trade_id, None)
        if date_str is not None:
            _save_registry(reg)
        return date_str


async def _finalize_day_if_empty(date_str: str) -> None:
    """Если для даты больше нет открытых сделок — checkpoint WAL. Файл дня
    после этого больше никем не пишется (новые сделки уже в файле сегодня)."""
    reg = _get_registry()
    if date_str in reg.values():
        return
    path = _db_path_for_date(date_str)
    if not os.path.exists(path):
        return
    try:
        async with aiosqlite.connect(path) as db:
            await db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        pass


# ───────────────────────── инициализация / резолвинг пути ─────────────────────────

# Колонки, которые могли быть добавлены в _CREATE_TABLE позже создания
# уже существующих посуточных файлов. CREATE TABLE IF NOT EXISTS их НЕ добавляет
# в существующий файл, поэтому при деплое новых полей в середине дня INSERT падает,
# сделка не записывается, а позиция на бирже остаётся без SL/трекинга.
# Этот список и тело _migrate_columns() закрывают разрыв (idempotent ALTER).
_MIGRATE_COLUMNS = [
    ("approach_speed_pct", "REAL"),
    ("red_candles_streak", "INTEGER"),
    ("ob_spread_pct", "REAL"),
    ("ob_bid_vol_grid", "REAL"),
    ("ob_bid_vol_topn", "REAL"),
    ("ob_ask_vol_topn", "REAL"),
    ("ob_imbalance", "REAL"),
    ("ob_bid_vol_1pct", "REAL"),
    ("ob_raw_json", "TEXT"),
    ("funding_rate_at_entry", "REAL"),
    ("oi_value_at_entry", "REAL"),
    ("oi_change_1h_pct", "REAL"),
]


async def _migrate_columns(db) -> None:
    """Добавить недостающие колонки в существующую таблицу live_trades.
    Idempotent: колонка добавляется только если её ещё нет."""
    cur = await db.execute("PRAGMA table_info(live_trades)")
    existing = {row[1] for row in await cur.fetchall()}
    for name, coltype in _MIGRATE_COLUMNS:
        if name not in existing:
            await db.execute(f"ALTER TABLE live_trades ADD COLUMN {name} {coltype}")


async def _ensure_db(date_str: str) -> str:
    os.makedirs(DB_DIR, exist_ok=True)
    path = _db_path_for_date(date_str)
    async with aiosqlite.connect(path) as db:
        await db.executescript(_CREATE_TABLE)
        await _migrate_columns(db)
        await db.commit()
    return path


async def init_live_trades_db() -> None:
    """Инициализировать файл текущего (UTC) дня. Файлы других дней создаются
    лениво при первой записи (open_live_trade)."""
    await _ensure_db(_utc_date_str(time.time()))


async def resolve_db_path_for_trade(trade_id: str) -> Optional[str]:
    """Найти путь к файлу дня, где лежит trade_id. Сначала реестр (O(1)),
    при промахе — fallback-сканирование последних LOOKBACK_DAYS дней."""
    reg = _get_registry()
    date_str = reg.get(trade_id)
    if date_str:
        return _db_path_for_date(date_str)

    today = datetime.now(timezone.utc).date()
    for back in range(LOOKBACK_DAYS):
        d = (today - timedelta(days=back)).strftime("%Y-%m-%d")
        path = _db_path_for_date(d)
        if not os.path.exists(path):
            continue
        try:
            async with aiosqlite.connect(path) as db:
                async with db.execute(
                    "SELECT 1 FROM live_trades WHERE trade_id = ?", (trade_id,)
                ) as cur:
                    row = await cur.fetchone()
            if row:
                await _set_trade_date(trade_id, d)
                return path
        except Exception:
            continue
    return None


def db_paths_for_recent_days(n_days: int = 2) -> list[str]:
    """Существующие файлы за последние n_days (включая сегодня), UTC.
    Используется модулями, которым нужны недавно ЗАКРЫТЫЕ сделки
    (live_price_tracker — окно поиска короче, чем LOOKBACK_DAYS)."""
    today = datetime.now(timezone.utc).date()
    paths = []
    for back in range(n_days):
        d = (today - timedelta(days=back)).strftime("%Y-%m-%d")
        path = _db_path_for_date(d)
        if os.path.exists(path):
            paths.append(path)
    return paths


# ───────────────────────── CRUD ─────────────────────────

async def open_live_trade(trade: dict) -> str:
    """Записать новую live-сделку в файл дня entry_time. Возвращает trade_id."""
    now = time.time()
    trade_id = trade["trade_id"]
    entry_time = trade.get("entry_time", now)
    date_str = _utc_date_str(entry_time)
    path = await _ensure_db(date_str)

    async with aiosqlite.connect(path) as db:
        await db.execute(
            """INSERT INTO live_trades
               (trade_id, paper_trade_id, symbol, level, level_type,
                entry_price, entry_time, position_size_usdt, direction,
                grid_orders_json, grid_fill_count,
                bybit_order_ids_json, bybit_sl_order_id, bybit_position_qty,
                strength_at_entry, p_bounce_at_entry, expected_depth_at_entry,
                ml_delta_at_entry, p_fast_breakout_at_entry, vol_ratio_at_entry,
                ob_spread_pct, ob_bid_vol_grid, ob_bid_vol_topn, ob_ask_vol_topn,
                ob_imbalance, ob_bid_vol_1pct, ob_raw_json,
                funding_rate_at_entry, oi_value_at_entry, oi_change_1h_pct,
                signal_group, is_flip, flip_breakout_time, flip_age_hours,
                retest_number, approach_count_at_entry, cautious_mode, vol_falling,
                stop_loss, take_profit_1, take_profit_2,
                status, events_json, error_log_json, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                trade_id,
                trade.get("paper_trade_id"),
                trade["symbol"],
                trade["level"],
                trade.get("level_type"),
                trade.get("entry_price"),
                entry_time,
                trade["position_size_usdt"],
                trade.get("direction", "long"),
                json.dumps(trade.get("grid_orders", [])),
                0,
                json.dumps(trade.get("bybit_order_ids", [])),
                trade.get("bybit_sl_order_id"),
                0.0,
                trade.get("strength_at_entry"),
                trade.get("p_bounce_at_entry"),
                trade.get("expected_depth_at_entry"),
                trade.get("ml_delta_at_entry"),
                trade.get("p_fast_breakout_at_entry"),
                trade.get("vol_ratio_at_entry"),
                trade.get("ob_spread_pct"),
                trade.get("ob_bid_vol_grid"),
                trade.get("ob_bid_vol_topN"),
                trade.get("ob_ask_vol_topN"),
                trade.get("ob_imbalance"),
                trade.get("ob_bid_vol_1pct"),
                trade.get("ob_raw_json"),
                trade.get("funding_rate_at_entry"),
                trade.get("oi_value_at_entry"),
                trade.get("oi_change_1h_pct"),
                trade.get("signal_group"),
                trade.get("is_flip"),
                trade.get("flip_breakout_time"),
                trade.get("flip_age_hours"),
                trade.get("retest_number"),
                trade.get("approach_count_at_entry"),
                trade.get("cautious_mode"),
                trade.get("vol_falling"),
                trade.get("stop_loss"),
                trade.get("take_profit_1"),
                trade.get("take_profit_2"),
                "open",
                "[]",
                "[]",
                now,
                now,
            ),
        )
        await db.commit()

    await _set_trade_date(trade_id, date_str)
    return trade_id


async def add_live_event(trade_id: str, event_type: str, note: str = "") -> None:
    path = await resolve_db_path_for_trade(trade_id)
    if path is None:
        return
    async with aiosqlite.connect(path) as db:
        async with db.execute(
            "SELECT events_json FROM live_trades WHERE trade_id = ?", (trade_id,)
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return
        try:
            events = json.loads(row[0] or "[]")
        except Exception:
            events = []
        events.append({"time": time.time(), "type": event_type, "note": note})
        await db.execute(
            "UPDATE live_trades SET events_json = ?, updated_at = ? WHERE trade_id = ?",
            (json.dumps(events), time.time(), trade_id),
        )
        await db.commit()


async def log_live_error(trade_id: str, context: str, error: str) -> None:
    """Добавить запись об ошибке в error_log_json сделки."""
    path = await resolve_db_path_for_trade(trade_id)
    if path is None:
        return
    async with aiosqlite.connect(path) as db:
        async with db.execute(
            "SELECT error_log_json FROM live_trades WHERE trade_id = ?", (trade_id,)
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return
        try:
            errors = json.loads(row[0] or "[]")
        except Exception:
            errors = []
        errors.append({"time": time.time(), "context": context, "error": error})
        await db.execute(
            "UPDATE live_trades SET error_log_json = ?, updated_at = ? WHERE trade_id = ?",
            (json.dumps(errors), time.time(), trade_id),
        )
        await db.commit()


async def update_live_trade(trade_id: str, **fields) -> None:
    """Обновить произвольные поля сделки."""
    if not fields:
        return
    path = await resolve_db_path_for_trade(trade_id)
    if path is None:
        return
    fields["updated_at"] = time.time()
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [trade_id]
    async with aiosqlite.connect(path) as db:
        await db.execute(
            f"UPDATE live_trades SET {set_clause} WHERE trade_id = ?", values
        )
        await db.commit()


async def close_live_trade(
    trade_id: str,
    exit_price: float,
    exit_reason: str,
    pnl_usdt: float,
) -> None:
    path = await resolve_db_path_for_trade(trade_id)
    if path is None:
        return

    now = time.time()
    async with aiosqlite.connect(path) as db:
        async with db.execute(
            "SELECT entry_time, symbol FROM live_trades WHERE trade_id = ?", (trade_id,)
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return
        duration = (now - row[0]) / 60
        symbol = row[1]
        await db.execute(
            """UPDATE live_trades
               SET exit_price = ?, exit_time = ?, exit_reason = ?,
                   pnl_usdt = ?, duration_minutes = ?,
                   status = 'closed', updated_at = ?
               WHERE trade_id = ?""",
            (exit_price, now, exit_reason, round(pnl_usdt, 4) if pnl_usdt is not None else None,
             round(duration, 2), now, trade_id),
        )
        await db.commit()

    # Сделка закрыта — убираем из реестра открытых; если для её дня открытых
    # сделок больше не осталось, файл дня финализируем (WAL checkpoint).
    date_str = await _pop_trade_date(trade_id)
    if date_str:
        await _finalize_day_if_empty(date_str)

    # Запускаем post-exit трекинг сразу при закрытии (порт price_tracker.py на live_trades).
    # Отложенный импорт — избегает circular import.
    try:
        import asyncio as _asyncio
        from trading.live_price_tracker import track_post_exit_price as _track
        _asyncio.create_task(
            _track(trade_id, symbol, now, db_path=path),
            name=f"live_post_exit_tracker::{trade_id}",
        )
    except Exception:
        pass  # трекинг — best-effort, не должен ронять close_live_trade


async def get_open_live_trades() -> list[dict]:
    """Открытые сделки из всех релевантных дневных файлов. Источник правды —
    реестр (даты, на которые он указывает); сегодня/вчера добавляются всегда
    для самозалечивания на случай гонки при записи реестра."""
    reg = _get_registry()
    dates = set(reg.values())
    dates.add(_utc_date_str(time.time()))
    dates.add(_utc_date_str(time.time() - 86400))

    results: list[dict] = []
    for date_str in sorted(dates):
        path = _db_path_for_date(date_str)
        if not os.path.exists(path):
            continue
        async with aiosqlite.connect(path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM live_trades WHERE status = 'open' ORDER BY entry_time"
            ) as cur:
                rows = await cur.fetchall()
        for r in rows:
            d = dict(r)
            results.append(d)
            if d["trade_id"] not in reg:
                await _set_trade_date(d["trade_id"], date_str)

    results.sort(key=lambda d: d["entry_time"])
    return results


async def get_live_trade_stats() -> dict:
    rows: list[tuple] = []
    for path in sorted(glob.glob(os.path.join(DB_DIR, "live_trades_*.db"))):
        try:
            async with aiosqlite.connect(path) as db:
                async with db.execute(
                    "SELECT pnl_usdt, duration_minutes FROM live_trades WHERE status = 'closed'"
                ) as cur:
                    rows.extend(await cur.fetchall())
        except Exception:
            continue
    if not rows:
        return {"total": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
                "total_pnl_usdt": 0.0, "avg_duration_minutes": 0.0}
    total = len(rows)
    wins = [r[0] for r in rows if (r[0] or 0) > 0]
    losses = [r[0] for r in rows if (r[0] or 0) <= 0]
    return {
        "total": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / total * 100, 1),
        "total_pnl_usdt": round(sum(r[0] or 0 for r in rows), 4),
        "avg_duration_minutes": round(sum(r[1] or 0 for r in rows) / total, 1),
    }
