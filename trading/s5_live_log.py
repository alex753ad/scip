"""s5_live_log.py — учёт РЕАЛЬНЫХ сделок S5 в отдельной s5_live_trades.db.

Изолирован от S2 (live_trades.db) и от paper-лога S5 (s5_signals.db). Хранит
жизненный цикл live-сделки S5: вход, биржевые id ордеров, TP1-частичное закрытие,
трейлинг, выход, реализованный PnL по executions.

Все операции синхронные внутри, вызываются через asyncio.to_thread из стратегии.
Схема создаётся автоматически при первом обращении (_ensure).
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import time

DB_PATH = "s5_live_trades.db"


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.execute("PRAGMA journal_mode=WAL")
    return con


def _ensure(con: sqlite3.Connection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS s5_live_trades (
            trade_id TEXT PRIMARY KEY,
            symbol TEXT,
            status TEXT DEFAULT 'open',
            entry_time REAL, entry_dt TEXT,
            entry_price REAL, qty REAL, position_size_usdt REAL,
            stop_loss REAL, take_profit_1 REAL, take_profit_2 REAL, rr REAL,
            entry_order_id TEXT, sl_order_id TEXT,
            tp1_hit INTEGER DEFAULT 0, stop_moved_to_breakeven INTEGER DEFAULT 0,
            exit_time REAL, exit_dt TEXT, exit_price REAL, exit_reason TEXT,
            realized_pnl_usdt REAL, qty_mismatch INTEGER DEFAULT 0,
            basis TEXT, events_json TEXT DEFAULT '[]'
        )""")
    con.commit()


def _fmt(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts))


# ── sync ядра ────────────────────────────────────────────────────────────────

def _open_sync(t: dict) -> None:
    con = _connect()
    try:
        _ensure(con)
        con.execute("""
            INSERT OR REPLACE INTO s5_live_trades
            (trade_id, symbol, status, entry_time, entry_dt, entry_price, qty,
             position_size_usdt, stop_loss, take_profit_1, take_profit_2, rr,
             entry_order_id, sl_order_id, basis, events_json)
            VALUES (?,?,'open',?,?,?,?,?,?,?,?,?,?,?,?, '[]')""", (
            t["trade_id"], t["symbol"], t["entry_time"], _fmt(t["entry_time"]),
            t["entry_price"], t["qty"], t["position_size_usdt"],
            t["stop_loss"], t["take_profit_1"], t["take_profit_2"], t["rr"],
            t.get("entry_order_id"), t.get("sl_order_id"), t.get("basis"),
        ))
        con.commit()
    finally:
        con.close()


def _event_sync(trade_id: str, etype: str, note: str) -> None:
    con = _connect()
    try:
        _ensure(con)
        row = con.execute("SELECT events_json FROM s5_live_trades WHERE trade_id=?",
                          (trade_id,)).fetchone()
        evs = json.loads(row[0]) if row and row[0] else []
        evs.append({"ts": time.time(), "type": etype, "note": note})
        con.execute("UPDATE s5_live_trades SET events_json=? WHERE trade_id=?",
                    (json.dumps(evs), trade_id))
        con.commit()
    finally:
        con.close()


def _update_sync(trade_id: str, fields: dict) -> None:
    con = _connect()
    try:
        _ensure(con)
        cols = ", ".join(f"{k}=?" for k in fields)
        con.execute(f"UPDATE s5_live_trades SET {cols} WHERE trade_id=?",
                    (*fields.values(), trade_id))
        con.commit()
    finally:
        con.close()


def _close_sync(trade_id: str, exit_price: float, reason: str,
                realized_pnl: float, qty_mismatch: bool) -> None:
    con = _connect()
    try:
        _ensure(con)
        now = time.time()
        con.execute("""
            UPDATE s5_live_trades SET status='closed', exit_time=?, exit_dt=?,
                exit_price=?, exit_reason=?, realized_pnl_usdt=?, qty_mismatch=?
            WHERE trade_id=?""", (
            now, _fmt(now), exit_price, reason, round(realized_pnl, 6),
            1 if qty_mismatch else 0, trade_id))
        con.commit()
    finally:
        con.close()


def _get_open_sync() -> list[dict]:
    con = _connect()
    try:
        _ensure(con)
        con.row_factory = sqlite3.Row
        rows = con.execute("SELECT * FROM s5_live_trades WHERE status='open'").fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def _get_stats_sync() -> dict:
    con = _connect()
    try:
        _ensure(con)
        rows = con.execute(
            "SELECT realized_pnl_usdt FROM s5_live_trades WHERE status='closed'"
        ).fetchall()
        pnls = [r[0] for r in rows if r[0] is not None]
        n = len(pnls)
        wins = sum(1 for p in pnls if p > 0)
        open_n = con.execute(
            "SELECT COUNT(*) FROM s5_live_trades WHERE status='open'").fetchone()[0]
        return {
            "total": n,
            "wins": wins,
            "losses": n - wins,
            "win_rate": (wins / n * 100) if n else 0.0,
            "total_pnl_usdt": round(sum(pnls), 4),
            "open": open_n,
        }
    finally:
        con.close()


# ── async обёртки ────────────────────────────────────────────────────────────

async def open_trade(t: dict) -> None:
    try: await asyncio.to_thread(_open_sync, t)
    except Exception: pass

async def add_event(trade_id: str, etype: str, note: str) -> None:
    try: await asyncio.to_thread(_event_sync, trade_id, etype, note)
    except Exception: pass

async def update_trade(trade_id: str, **fields) -> None:
    try: await asyncio.to_thread(_update_sync, trade_id, fields)
    except Exception: pass

async def close_trade(trade_id: str, exit_price: float, reason: str,
                      realized_pnl: float, qty_mismatch: bool = False) -> None:
    try: await asyncio.to_thread(_close_sync, trade_id, exit_price, reason,
                                 realized_pnl, qty_mismatch)
    except Exception: pass

async def get_open_trades() -> list[dict]:
    try: return await asyncio.to_thread(_get_open_sync)
    except Exception: return []

async def get_stats() -> dict:
    try: return await asyncio.to_thread(_get_stats_sync)
    except Exception:
        return {"total": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
                "total_pnl_usdt": 0.0, "open": 0}
