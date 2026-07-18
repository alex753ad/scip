"""Сохранение свечей коллектора на диск — candles/candles_<date>.db, по дням (UTC).

Дата файла берётся из open_time САМОЙ свечи (UTC), поэтому переход через полночь
не требует никакой особой логики: свеча 23:58 пишется в файл сегодняшнего дня,
00:02 — в файл следующего, автоматически, без пересечения с задачами recovery
(в отличие от live_trades, тут нет понятия "открытая сделка" — каждая свеча
самодостаточна и не нужно её потом находить по id).

Схема свечи — ровно как в collector._parse_kline:
  {open_time, open, high, low, close, volume, close_time, trades}  # *_time в мс

Идемпотентно: PK (symbol, timeframe, open_time) + INSERT OR REPLACE.
snapshot_all() пишет только ЗАКРЫТЫЕ свечи (последняя в буфере — бегущая, пропускается)
и каждую закрытую — один раз (трекинг last_saved в памяти).

Интеграция в collector.start_collector (без изменений, как раньше):
  from data.candle_store import init_db, snapshot_all
  init_db()                                   # один раз при старте
  ... в while True, перед await asyncio.sleep(5):
  snapshot_all(candles_1m, candles_5m, candles_15m)
"""
import csv
import glob
import os
import sqlite3
from datetime import datetime, timezone

DB_DIR = "candles"          # папка с файлами candles_YYYY-MM-DD.db (положи рядом с history.db)
_MAX_OPEN_CONNS = 2         # держим открытыми только сегодня + вчера, остальные коннекты закрываем
_conns: dict[str, sqlite3.Connection] = {}     # date_str -> connection
_last_saved: dict[tuple[str, str], int] = {}   # (symbol, tf) -> max open_time уже сохранённый

_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS candles (
        symbol      TEXT NOT NULL,
        timeframe   TEXT NOT NULL,
        open_time   INTEGER NOT NULL,
        open        REAL, high REAL, low REAL, close REAL,
        volume      REAL, close_time INTEGER, trades INTEGER,
        PRIMARY KEY (symbol, timeframe, open_time)
    )
"""


def init_db(path: str | None = None) -> None:
    """path (опционально) переопределяет DB_DIR — папку с дневными файлами
    (раньше переопределял путь к единственному файлу candles.db)."""
    global DB_DIR
    if path:
        DB_DIR = path
    os.makedirs(DB_DIR, exist_ok=True)


def _date_str_from_open_time_ms(open_time_ms: int) -> str:
    return datetime.fromtimestamp(open_time_ms / 1000, timezone.utc).strftime("%Y-%m-%d")


def _db_path_for_date(date_str: str) -> str:
    return os.path.join(DB_DIR, f"candles_{date_str}.db")


def _get_conn(date_str: str) -> sqlite3.Connection:
    conn = _conns.get(date_str)
    if conn is not None:
        return conn

    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(_db_path_for_date(date_str))
    conn.execute("PRAGMA journal_mode=WAL;")   # чтобы анализ читал, пока коллектор пишет
    conn.execute(_CREATE_TABLE)
    conn.commit()
    _conns[date_str] = conn

    # Держим открытыми только последние _MAX_OPEN_CONNS дат, остальные закрываем.
    # date_str (только что открытый) никогда не закрываем здесь же — иначе
    # вернули бы вызывающему уже закрытый conn (баг: "Cannot operate on a closed database").
    if len(_conns) > _MAX_OPEN_CONNS:
        candidates = [d for d in sorted(_conns) if d != date_str]
        for old_date in candidates[:len(_conns) - _MAX_OPEN_CONNS]:
            try:
                _conns[old_date].execute("PRAGMA wal_checkpoint(TRUNCATE)")
                _conns[old_date].close()
            except Exception:
                pass
            del _conns[old_date]

    return conn


def save(symbol: str, timeframe: str, candles) -> int:
    """Сохранить свечу (dict) или список свечей. Каждая попадает в файл своей даты
    (определяется по open_time свечи). Возвращает число записанных строк."""
    if isinstance(candles, dict):
        candles = [candles]

    by_date: dict[str, list] = {}
    for c in candles:
        d = _date_str_from_open_time_ms(c["open_time"])
        by_date.setdefault(d, []).append(c)

    total = 0
    for date_str, group in by_date.items():
        conn = _get_conn(date_str)
        rows = [(symbol, timeframe, c["open_time"], c["open"], c["high"], c["low"],
                 c["close"], c["volume"], c["close_time"], c["trades"]) for c in group]
        conn.executemany(
            "INSERT OR REPLACE INTO candles VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
        conn.commit()
        total += len(rows)
    return total


def snapshot_all(c1: dict, c5: dict, c15: dict) -> int:
    """Сбросить только новые закрытые свечи из in-memory буферов. Вызывать каждый цикл."""
    total = 0
    for tf, buf in (("1m", c1), ("5m", c5), ("15m", c15)):
        for symbol, lst in buf.items():
            if len(lst) < 2:
                continue
            closed = lst[:-1]                       # последняя — бегущая, не трогаем
            last = _last_saved.get((symbol, tf), -1)
            fresh = [c for c in closed if c["open_time"] > last]
            if fresh:
                total += save(symbol, tf, fresh)
                _last_saved[(symbol, tf)] = fresh[-1]["open_time"]
    return total


def _existing_dates() -> list[str]:
    """Даты всех существующих файлов candles_*.db, по возрастанию."""
    dates = []
    for path in glob.glob(os.path.join(DB_DIR, "candles_*.db")):
        name = os.path.basename(path)
        dates.append(name[len("candles_"):-len(".db")])
    return sorted(dates)


def export(symbol: str, timeframe: str, out_csv: str,
           start_utc: str | None = None, end_utc: str | None = None) -> int:
    """Выгрузить сохранённые свечи в CSV (по всем дневным файлам, попавшим в диапазон).
    start/end — UTC 'YYYY-MM-DD HH:MM' (опц.)."""

    def ms(s):
        return int(datetime.strptime(s, "%Y-%m-%d %H:%M")
                   .replace(tzinfo=timezone.utc).timestamp() * 1000)

    start_ms = ms(start_utc) if start_utc else None
    end_ms = ms(end_utc) if end_utc else None

    q = ("SELECT open_time,open,high,low,close,volume,close_time,trades "
         "FROM candles WHERE symbol=? AND timeframe=?")
    p = [symbol, timeframe]
    if start_ms is not None:
        q += " AND open_time>=?"; p.append(start_ms)
    if end_ms is not None:
        q += " AND open_time<?";  p.append(end_ms)
    q += " ORDER BY open_time"

    all_rows = []
    for date_str in _existing_dates():
        # дешёвый пропуск файлов точно вне диапазона по самой дате файла
        if start_utc and date_str < start_utc[:10]:
            continue
        if end_utc and date_str > end_utc[:10]:
            continue
        conn = sqlite3.connect(_db_path_for_date(date_str))
        try:
            all_rows.extend(conn.execute(q, p).fetchall())
        finally:
            conn.close()

    all_rows.sort(key=lambda r: r[0])

    n = 0
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_utc", "open", "high", "low", "close", "volume", "close_time_utc", "trades"])
        for ot, o, h, l, cl, v, ct, tr in all_rows:
            iso = datetime.fromtimestamp(ot / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            isc = datetime.fromtimestamp(ct / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            w.writerow([iso, o, h, l, cl, v, isc, tr]); n += 1
    return n


def stats() -> None:
    """Печать статистики по всем дневным файлам (агрегация по symbol/timeframe)."""
    agg: dict[tuple[str, str], list] = {}  # (symbol, tf) -> [count, min_ot, max_ot]
    for date_str in _existing_dates():
        conn = sqlite3.connect(_db_path_for_date(date_str))
        try:
            for sym, tf, n, a, b in conn.execute(
                "SELECT symbol,timeframe,COUNT(*),MIN(open_time),MAX(open_time) "
                "FROM candles GROUP BY symbol,timeframe"
            ):
                key = (sym, tf)
                if key not in agg:
                    agg[key] = [0, a, b]
                agg[key][0] += n
                agg[key][1] = min(agg[key][1], a)
                agg[key][2] = max(agg[key][2], b)
        finally:
            conn.close()

    for (sym, tf), (n, a, b) in sorted(agg.items()):
        fa = datetime.fromtimestamp(a / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
        fb = datetime.fromtimestamp(b / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
        print(f"{sym:12} {tf:4} {n:6}  {fa} .. {fb} UTC")


if __name__ == "__main__":
    import sys
    init_db()
    if len(sys.argv) >= 2 and sys.argv[1] == "stats":
        stats()
    elif len(sys.argv) >= 5 and sys.argv[1] == "export":
        # export SYMBOL TF OUT.csv [START] [END]
        n = export(*sys.argv[2:5], *(sys.argv[5:7] if len(sys.argv) > 5 else []))
        print(f"{sys.argv[4]}: {n} свечей")
    else:
        print("usage: python candle_store.py stats | export SYMBOL TF OUT.csv [START_UTC] [END_UTC]")
