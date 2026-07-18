import aiosqlite
import os
from logger import logger

# On Railway, use /data volume for persistence. Locally use project root.
_DATA_DIR = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(_DATA_DIR, "history.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS level_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    level REAL NOT NULL,
    level_type TEXT,
    strength_claude INTEGER,
    approach_type TEXT,
    vol_ratio_on_approach REAL,
    touches_count INTEGER,
    result TEXT,
    duration_minutes REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS symbol_profiles (
    symbol TEXT PRIMARY KEY,
    best_level_type TEXT,
    wick_success_rate REAL,
    body_success_rate REAL,
    base_success_rate REAL,
    total_signals INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS symbol_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    event_type TEXT NOT NULL,
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Решения фильтра S2 по каждому сигналу (pass/block) — для анализа G1/G2/G4 (25.06).
-- Пишутся в т.ч. для ЗАБЛОКИРОВАННЫХ сигналов, которые не стали сделками,
-- поэтому отдельная таблица, а не live_trades.
CREATE TABLE IF NOT EXISTS signal_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    level REAL,
    level_type TEXT,
    decision TEXT,          -- 'pass' | 'block'
    block_reason TEXT,      -- напр. 'approach_ge_3' | 'phase_bleed' | 'p_bounce_low'
    signal_group TEXT,      -- 'g1' | 'g2' | 'g4' (если определена)
    phase TEXT,
    approach_count INTEGER,
    vol_falling INTEGER,
    is_flip INTEGER,
    p_bounce REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# New columns added to level_outcomes
_NEW_COLUMNS = [
    ("approach_style", "TEXT"),
    ("vol_ratio_at_touch", "REAL"),
    ("atr_ratio", "REAL"),
    ("fill_depth_pct", "REAL"),
    ("outcome", "TEXT"),
    ("btc_change_1m", "REAL"),
    ("funding_rate", "REAL"),
    ("monitoring_age_minutes", "REAL"),
    ("trades_per_min_1m", "REAL"),   # avg trades/min за последнюю 1М свечу при касании
    ("trades_per_min_5m", "REAL"),   # avg trades/min за последние 5 свечей при касании
    ("trades_per_min_15m", "REAL"),  # avg trades/min за последние 15 свечей при касании
    ("trades_increasing", "INTEGER"),  # 1 если trades_per_min_1m > trades_per_min_5m > trades_per_min_15m
    ("ml_delta", "INTEGER"),         # поправка ML к strength: +1 / 0 / -1 / -2
    ("p_fast_breakout", "REAL"),     # вероятность быстрого пробоя (clf2), None если clf2 не загружен
    ("p_bounce_at_entry", "REAL"),       # p_bounce из ML на входе (для перекалибровки порогов)
    ("expected_depth_at_entry", "REAL"), # ожидаемый прокол % из ML на входе
    ("vol_falling", "INTEGER"),          # 1 если объём падал при подходе (G-логика, 25.06)
    ("is_flip", "INTEGER"),              # 1 если уровень — S/R-флип (G4)
    ("flip_age_hours", "REAL"),          # возраст исходного пробоя на момент ретеста
    # BUG-4: duration_minutes declared REAL in schema; SQLite stores it correctly regardless
    # of legacy INTEGER declaration — no ALTER needed, new writes will be REAL seconds.
]


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        # Add new columns if they don't exist
        cursor = await db.execute("PRAGMA table_info(level_outcomes)")
        existing = {row[1] for row in await cursor.fetchall()}
        for col_name, col_type in _NEW_COLUMNS:
            if col_name not in existing:
                await db.execute(
                    f"ALTER TABLE level_outcomes ADD COLUMN {col_name} {col_type}"
                )
        await db.commit()


async def log_event(symbol: str, event_type: str, details: str = "") -> None:
    """
    Log a symbol lifecycle event.
    
    event_type examples:
      added_screener   — монета добавлена автоскринером
      added_manual     — монета добавлена вручную
      levels_built     — уровни построены (details: JSON список уровней)
      monitoring_start — мониторинг уровня запущен (details: level, strength)
      bounce           — отскок от уровня (details: level, fill_depth)
      breakout         — пробой уровня (details: level)
      removed          — монета удалена из списка (details: причина)
    """
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO symbol_events (symbol, event_type, details) VALUES (?, ?, ?)",
                (symbol, event_type, details),
            )
            await db.commit()
    except Exception:
        logger.exception("Failed to log event", symbol=symbol, event_type=event_type)


async def log_signal_decision(
    symbol: str,
    level: float = None,
    level_type: str = None,
    decision: str = None,        # 'pass' | 'block'
    block_reason: str = None,    # причина блока, если decision='block'
    signal_group: str = None,    # 'g1' | 'g2' | 'g4'
    phase: str = None,
    approach_count: int = None,
    vol_falling: int = None,
    is_flip: int = None,
    p_bounce: float = None,
) -> None:
    """Записать решение фильтра S2 (pass/block) в signal_decisions для анализа.

    Пишется для КАЖДОГО оценённого сигнала, включая заблокированные (которые не
    стали сделками) — иначе причины блоков теряются на рестарте (раньше жили
    только в логах и in-memory _skip_counts).
    """
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT INTO signal_decisions
                   (symbol, level, level_type, decision, block_reason, signal_group,
                    phase, approach_count, vol_falling, is_flip, p_bounce)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (symbol, level, level_type, decision, block_reason, signal_group,
                 phase, approach_count, vol_falling, is_flip, p_bounce),
            )
            await db.commit()
    except Exception:
        logger.exception("Failed to log signal decision", )


async def get_recent_breakouts(
    symbol: str,
    max_age_hours: float,
    side: str = "resistance",
) -> list[dict]:
    """Недавние пробои уровня для детекта S/R-флипа (G4).

    Возвращает записи level_outcomes с outcome='breakout' за последние max_age_hours
    по указанной стороне (approach_type хранит level_side). Для флипа resistance→support
    берём side='resistance' (уровень был сопротивлением и пробит вверх).

    Поля каждой записи: level, age_hours, breakout_ms (epoch мс пробоя, UTC).
    created_at в БД — TEXT CURRENT_TIMESTAMP (UTC), поэтому julianday/strftime в UTC.
    """
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT level,
                          (julianday('now') - julianday(created_at)) * 24.0 AS age_hours,
                          CAST(strftime('%s', created_at) AS INTEGER) * 1000 AS breakout_ms
                   FROM level_outcomes
                   WHERE symbol = ?
                     AND outcome = 'breakout'
                     AND approach_type = ?
                     AND (julianday('now') - julianday(created_at)) * 24.0 <= ?
                   ORDER BY created_at DESC""",
                (symbol, side, max_age_hours),
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]
    except Exception:
        logger.exception("Failed to get recent breakouts")
        return []


async def get_symbol_history(symbol: str, limit: int = 50) -> list[dict]:
    """Get recent events for a symbol."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT event_type, details, created_at
                   FROM symbol_events WHERE symbol = ?
                   ORDER BY id DESC LIMIT ?""",
                (symbol, limit),
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]
    except Exception:
        logger.exception("Failed to get symbol history")
        return []


async def save_level_outcome(
    symbol: str,
    level: float,
    level_type: str,
    strength: int,
    approach_type: str,
    vol_ratio: float,
    touches: int,
    result: str,
    duration: int,
    # new profile fields
    outcome: str = None,
    approach_style: str = None,
    vol_ratio_at_touch: float = None,
    atr_ratio: float = None,
    fill_depth_pct: float = None,
    btc_change_1m: float = None,
    funding_rate: float = None,
    monitoring_age_minutes: float = None,
    trades_per_min_1m: float = None,
    trades_per_min_5m: float = None,
    trades_per_min_15m: float = None,
    trades_increasing: int = None,
    ml_delta: int = None,
    p_fast_breakout: float = None,
    p_bounce_at_entry: float = None,
    expected_depth_at_entry: float = None,
    vol_falling: int = None,
    is_flip: int = None,
    flip_age_hours: float = None,
) -> None:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT INTO level_outcomes
                (symbol, level, level_type, strength_claude, approach_type,
                 vol_ratio_on_approach, touches_count, result, duration_minutes,
                 outcome, approach_style, vol_ratio_at_touch, atr_ratio,
                 fill_depth_pct, btc_change_1m, funding_rate, monitoring_age_minutes,
                 trades_per_min_1m, trades_per_min_5m, trades_per_min_15m, trades_increasing,
                 ml_delta, p_fast_breakout, p_bounce_at_entry, expected_depth_at_entry,
                 vol_falling, is_flip, flip_age_hours)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (symbol, level, level_type, strength, approach_type,
                 vol_ratio, touches, result, duration,
                 outcome, approach_style, vol_ratio_at_touch, atr_ratio,
                 fill_depth_pct, btc_change_1m, funding_rate, monitoring_age_minutes,
                 trades_per_min_1m, trades_per_min_5m, trades_per_min_15m, trades_increasing,
                 ml_delta, p_fast_breakout, p_bounce_at_entry, expected_depth_at_entry,
                 vol_falling, is_flip, flip_age_hours),
            )
            await db.commit()
    except Exception:
        logger.exception("Failed to save level outcome")


async def get_symbol_profile(symbol: str) -> dict:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM symbol_profiles WHERE symbol = ?", (symbol,)
            )
            row = await cursor.fetchone()
            if row:
                return dict(row)
    except Exception:
        logger.exception("Failed to get symbol profile")
    return {}


async def update_symbol_profile(symbol: str) -> None:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                """SELECT level_type, outcome FROM level_outcomes
                WHERE symbol = ? AND outcome IS NOT NULL
                ORDER BY id DESC LIMIT 50""",
                (symbol,),
            )
            rows = await cursor.fetchall()

            if not rows:
                return

            total = len(rows)
            type_counts = {"pump_base": [0, 0], "body_level": [0, 0], "wick_level": [0, 0]}

            for level_type, outcome in rows:
                if level_type in type_counts:
                    type_counts[level_type][1] += 1
                    if outcome == "bounce":
                        type_counts[level_type][0] += 1

            base_rate = type_counts["pump_base"][0] / type_counts["pump_base"][1] if type_counts["pump_base"][1] > 0 else 0
            body_rate = type_counts["body_level"][0] / type_counts["body_level"][1] if type_counts["body_level"][1] > 0 else 0
            wick_rate = type_counts["wick_level"][0] / type_counts["wick_level"][1] if type_counts["wick_level"][1] > 0 else 0

            best_type = max(type_counts.items(), key=lambda x: x[1][0] / x[1][1] if x[1][1] > 0 else 0)[0]

            await db.execute(
                """INSERT OR REPLACE INTO symbol_profiles
                (symbol, best_level_type, wick_success_rate, body_success_rate,
                 base_success_rate, total_signals, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (symbol, best_type, wick_rate, body_rate, base_rate, total),
            )
            await db.commit()
    except Exception:
        logger.exception("Failed to update symbol profile")


async def get_outcome_probs(
    symbol: str,
    level_type: str,
    approach_style: str = None,
    n: int = 30,
) -> dict:
    """Get outcome probability distribution from history."""
    empty = {
        "no_reach": 0.0, "partial": 0.0, "bounce": 0.0, "breakout": 0.0,
        "sample_size": 0, "style_filtered": False, "avg_fill_depth": 0.0,
    }
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            # Try with approach_style filter first
            style_filtered = False
            if approach_style:
                cursor = await db.execute(
                    """SELECT outcome, fill_depth_pct FROM level_outcomes
                    WHERE symbol = ? AND level_type = ? AND approach_style = ?
                      AND outcome IS NOT NULL
                    ORDER BY id DESC LIMIT ?""",
                    (symbol, level_type, approach_style, n),
                )
                rows = await cursor.fetchall()
                if len(rows) >= 5:
                    style_filtered = True

            if not style_filtered:
                cursor = await db.execute(
                    """SELECT outcome, fill_depth_pct FROM level_outcomes
                    WHERE symbol = ? AND level_type = ? AND outcome IS NOT NULL
                    ORDER BY id DESC LIMIT ?""",
                    (symbol, level_type, n),
                )
                rows = await cursor.fetchall()

            if not rows:
                return empty

            total = len(rows)
            counts = {"no_reach": 0, "partial": 0, "bounce": 0, "breakout": 0}
            fill_depths = []

            _PARTIAL = {"partial", "partial_shallow", "partial_mid", "partial_deep"}
            for outcome, fill_depth in rows:
                normalized = "partial" if outcome in _PARTIAL else outcome
                if normalized in counts:
                    counts[normalized] += 1
                if outcome == "bounce" and fill_depth is not None:
                    fill_depths.append(fill_depth)

            return {
                "no_reach": round(counts["no_reach"] / total, 3),
                "partial": round(counts["partial"] / total, 3),
                "bounce": round(counts["bounce"] / total, 3),
                "breakout": round(counts["breakout"] / total, 3),
                "sample_size": total,
                "style_filtered": style_filtered,
                "avg_fill_depth": round(sum(fill_depths) / len(fill_depths), 3) if fill_depths else 0.0,
            }
    except Exception:
        logger.exception("Failed to get outcome probs")
        return empty
