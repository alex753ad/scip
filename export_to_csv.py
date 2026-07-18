"""Export trades and signals from SQLite to CSV files."""

import csv
import sqlite3
import os
import sys
from datetime import datetime

TRADES_DB  = "trades.db"
HISTORY_DB = "history.db"
OUT_TRADES  = "trades_export.csv"
OUT_SIGNALS = "signals_export.csv"


def ts(val):
    if val is None:
        return ""
    try:
        return datetime.fromtimestamp(float(val)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(val)


def export_trades(db_path, out_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM trades ORDER BY entry_time"
    ).fetchall()
    conn.close()

    if not rows:
        print("trades: нет данных")
        return

    # Exclude raw JSON and internal fields
    skip = {"grid_orders_json", "events_json", "trade_id"}
    cols = [c for c in rows[0].keys() if c not in skip]

    # Add human-readable datetime columns
    time_cols = {"entry_time", "exit_time", "created_at", "updated_at", "post_exit_tracked_until"}

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        header = []
        for c in cols:
            header.append(c)
            if c in time_cols:
                header.append(c + "_dt")
        writer.writerow(header)

        for row in rows:
            line = []
            for c in cols:
                val = row[c]
                line.append(val)
                if c in time_cols:
                    line.append(ts(val))
            writer.writerow(line)

    print(f"trades: {len(rows)} строк → {out_path}")


def export_signals(db_path, out_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Try symbol_events first, fallback to events
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    if "symbol_events" in tables:
        table = "symbol_events"
    elif "events" in tables:
        table = "events"
    else:
        print(f"signals: таблица событий не найдена. Таблицы: {tables}")
        conn.close()
        return

    rows = conn.execute(f"SELECT * FROM {table} ORDER BY created_at").fetchall()
    conn.close()

    if not rows:
        print("signals: нет данных")
        return

    cols = rows[0].keys()
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        header = list(cols)
        if "created_at" in cols:
            header.append("created_at_dt")
        writer.writerow(header)

        for row in rows:
            line = list(row)
            if "created_at" in cols:
                line.append(ts(row["created_at"]))
            writer.writerow(line)

    print(f"signals: {len(rows)} строк → {out_path}")


if __name__ == "__main__":
    # Allow passing custom paths as arguments
    trades_db  = sys.argv[1] if len(sys.argv) > 1 else TRADES_DB
    history_db = sys.argv[2] if len(sys.argv) > 2 else HISTORY_DB

    if os.path.exists(trades_db):
        export_trades(trades_db, OUT_TRADES)
    else:
        print(f"trades.db не найден: {trades_db}")

    if os.path.exists(history_db):
        export_signals(history_db, OUT_SIGNALS)
    else:
        print(f"history.db не найден: {history_db}")
