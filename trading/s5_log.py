"""s5_log.py — выделенная БД стратегии S5 (continuation).

Отдельный файл s5_signals.db с двумя таблицами:
  • signals — КАЖДЫЙ сигнал S5 с полным основанием входа (почему вошли):
              рост пампа, свежесть, глубина отката, объём триггера, фаза рынка,
              затухание волатильности, дистанция до EMA, дельта ордерфлоу, +
              человекочитаемое поле basis. opened=1 если сигнал стал сделкой.
  • trades  — жизненный цикл сделки: вход, цели, статус, выход, PnL.

Назначение — самодостаточный файл для еженедельной отправки на разбор.
Авторитетный PnL всё равно дублируется в общей trades.db (strategy_id=5);
здесь pnl_usdt считается по факту выхода S5 (TP1 50% + остаток) для удобства.

Все операции best-effort и вынесены в поток (asyncio.to_thread) — не блокируют
event loop и никогда не роняют торговлю.
"""
from __future__ import annotations

import asyncio
import sqlite3
import time

DB_PATH = "s5_signals.db"


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.execute("PRAGMA journal_mode=WAL")
    return con


def _ensure(con: sqlite3.Connection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL, dt TEXT, symbol TEXT,
            entry REAL, sl REAL, tp1 REAL, tp2 REAL, rr REAL,
            growth_pct REAL, hours_since_peak REAL, retr_pct REAL,
            retr_candles INTEGER, trig_vol_ratio REAL,
            market_phase TEXT, vol_decay REAL, natr_now_pct REAL,
            ema_fast REAL, ema_slow REAL, ema_dist_pct REAL,
            delta_at_entry REAL, buy_vol REAL, sell_vol REAL,
            basis TEXT, opened INTEGER DEFAULT 0, trade_id TEXT
        )""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id TEXT PRIMARY KEY,
            signal_id INTEGER, symbol TEXT,
            entry_time REAL, entry_dt TEXT, entry_price REAL,
            sl REAL, tp1 REAL, tp2 REAL, rr REAL,
            status TEXT DEFAULT 'open',
            exit_time REAL, exit_price REAL, exit_reason TEXT,
            tp1_hit INTEGER DEFAULT 0,
            pnl_pct REAL, pnl_usdt REAL,
            basis TEXT
        )""")
    con.commit()


def _fmt(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts))


# ── синхронные ядра (выполняются в потоке) ────────────────────────────────────

def _log_signal_sync(sig: dict) -> int:
    con = _connect()
    try:
        _ensure(con)
        cur = con.execute("""
            INSERT INTO signals (ts, dt, symbol, entry, sl, tp1, tp2, rr,
                growth_pct, hours_since_peak, retr_pct, retr_candles, trig_vol_ratio,
                market_phase, vol_decay, natr_now_pct, ema_fast, ema_slow, ema_dist_pct,
                delta_at_entry, buy_vol, sell_vol, basis, opened, trade_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            sig.get("ts"), _fmt(sig.get("ts", time.time())), sig.get("symbol"),
            sig.get("entry"), sig.get("sl"), sig.get("tp1"), sig.get("tp2"), sig.get("rr"),
            sig.get("growth_pct"), sig.get("hours_since_peak"), sig.get("retr_pct"),
            sig.get("retr_candles"), sig.get("trig_vol_ratio"),
            sig.get("market_phase"), sig.get("vol_decay"), sig.get("natr_now_pct"),
            sig.get("ema_fast"), sig.get("ema_slow"), sig.get("ema_dist_pct"),
            sig.get("delta_at_entry"), sig.get("buy_vol"), sig.get("sell_vol"),
            sig.get("basis"), 1 if sig.get("trade_id") else 0, sig.get("trade_id"),
        ))
        con.commit()
        return cur.lastrowid
    finally:
        con.close()


def _open_trade_sync(t: dict) -> None:
    con = _connect()
    try:
        _ensure(con)
        con.execute("""
            INSERT OR REPLACE INTO trades (trade_id, signal_id, symbol, entry_time,
                entry_dt, entry_price, sl, tp1, tp2, rr, status, basis)
            VALUES (?,?,?,?,?,?,?,?,?,?, 'open', ?)""", (
            t.get("trade_id"), t.get("signal_id"), t.get("symbol"), t.get("entry_time"),
            _fmt(t.get("entry_time", time.time())), t.get("entry_price"),
            t.get("sl"), t.get("tp1"), t.get("tp2"), t.get("rr"), t.get("basis"),
        ))
        con.commit()
    finally:
        con.close()


def _close_trade_sync(trade_id: str, exit_price: float, reason: str,
                      tp1_hit: bool, pnl_pct: float, pnl_usdt: float) -> None:
    con = _connect()
    try:
        _ensure(con)
        con.execute("""
            UPDATE trades SET status='closed', exit_time=?, exit_price=?, exit_reason=?,
                tp1_hit=?, pnl_pct=?, pnl_usdt=? WHERE trade_id=?""", (
            time.time(), exit_price, reason, 1 if tp1_hit else 0,
            round(pnl_pct, 4), round(pnl_usdt, 4), trade_id,
        ))
        con.commit()
    finally:
        con.close()


# ── async-обёртки (best-effort, не роняют торговлю) ──────────────────────────

async def log_signal(sig: dict) -> int:
    try:
        return await asyncio.to_thread(_log_signal_sync, sig)
    except Exception:
        return 0


async def open_trade(t: dict) -> None:
    try:
        await asyncio.to_thread(_open_trade_sync, t)
    except Exception:
        pass


async def close_trade(trade_id: str, exit_price: float, reason: str,
                      tp1_hit: bool, pnl_pct: float, pnl_usdt: float) -> None:
    try:
        await asyncio.to_thread(_close_trade_sync, trade_id, exit_price, reason,
                                tp1_hit, pnl_pct, pnl_usdt)
    except Exception:
        pass
