"""verify_s5.py — проверка, что стратегия S5 (continuation) работает корректно.

Две независимые проверки:

  A) FORWARD  — читает реальные закрытые сделки S5 из trades.db и считает
                статистику (PnL, winrate + доверительный интервал, сравнение с
                ожиданием бэктеста: winrate≈0.58, avg≈+0.60$/сделку).

  B) SHADOW   — заново прогоняет ДЕТЕКТОР S5 по свечам candles_*.db и печатает,
                какие сигналы он ДОЛЖЕН был выдать. Сверяешь список с реальными
                входами S5 в trades.db: совпадают → сканер работает; расходятся →
                проблема в живой подаче свечей/таймингах.

Запуск:
    python verify_s5.py                 # обе проверки
    python verify_s5.py --trades PATH   # путь к trades.db (или папке дневных БД)
    python verify_s5.py --candles DIR   # каталог с candles_*.db для shadow

Здоровье сканера (быстрые сигналы):
    • 0 сделок за сутки при наличии пампов → сканер не стреляет (импорт/тайминг).
    • десятки сделок в час → фильтр объёма/отката сломан (переторговка).
    • winrate заметно < 0.5 при R≈1:1 → отрицательный EV, разбирать вход.
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os
import sqlite3

import pandas as pd


# ── A. FORWARD: статистика реальных сделок S5 ────────────────────────────────

def _load_trades(path: str) -> pd.DataFrame:
    files = []
    if os.path.isdir(path):
        files = sorted(glob.glob(os.path.join(path, "*.db")))
    else:
        files = [path]
    frames = []
    for f in files:
        try:
            con = sqlite3.connect(f)
            frames.append(pd.read_sql("select * from trades", con))
            con.close()
        except Exception:
            pass
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _wilson_ci(wins: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """95% доверительный интервал для доли (Wilson) — честен на малом n."""
    if n == 0:
        return (0.0, 0.0)
    p = wins / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - margin), min(1.0, centre + margin))


def _pnl(row) -> float:
    if pd.notna(row.get("pnl_usdt")):
        return float(row["pnl_usdt"])
    ep, xp = row.get("entry_price"), row.get("exit_price")
    size = row.get("position_size") or 200.0
    if pd.notna(ep) and pd.notna(xp) and ep:
        return (float(xp) - float(ep)) / float(ep) * float(size)
    return float("nan")


def forward_check(trades_path: str) -> None:
    df = _load_trades(trades_path)
    print("=" * 64)
    print("A) FORWARD — реальные сделки S5 из trades.db")
    print("=" * 64)
    if df.empty:
        print("trades.db не найдена/пуста по пути:", trades_path)
        return
    s5 = df[df.strategy_id == 5].copy()
    print(f"всего записей S5: {len(s5)} | открытых: {(s5.status!='closed').sum()}")
    c = s5[s5.status == "closed"].copy()
    if c.empty:
        print("\n⚠ Закрытых сделок S5 ещё нет.")
        print("  Если бот работает >суток и были пампы — сканер, вероятно, НЕ стреляет:")
        print("  проверь лог на 'S5 trade opened' и что strategy_runner запустил start_scanner().")
        return
    c["pnl"] = c.apply(_pnl, axis=1)
    c = c.dropna(subset=["pnl"])
    n = len(c); wins = int((c.pnl > 0).sum()); wr = wins / n
    lo, hi = _wilson_ci(wins, n)
    total = c.pnl.sum(); avg = c.pnl.mean()
    print(f"\nзакрытых сделок:  {n}")
    print(f"итог PnL:         {total:+.2f}$")
    print(f"avg / сделку:     {avg:+.3f}$   (ожидание бэктеста ≈ +0.60$)")
    print(f"winrate:          {wr:.2f}   95% ДИ [{lo:.2f}, {hi:.2f}]   (ожидание ≈ 0.58)")
    if wins:
        print(f"avg win:          {c[c.pnl>0].pnl.mean():+.2f}$")
    if n - wins:
        print(f"avg loss:         {c[c.pnl<=0].pnl.mean():+.2f}$")
    print("\nпо exit_reason:")
    print(c.groupby("exit_reason").agg(n=("pnl", "size"), pnl=("pnl", "sum")).round(2).to_string())

    # контекст входа из params_set (growth/hours_since_peak/retr/trig_vol_ratio)
    ctx = []
    for _, r in c.iterrows():
        try:
            evs = json.loads(r.get("events_json") or "[]")
            p = next((json.loads(e["note"]) for e in evs if e.get("type") == "params_set"), {})
        except Exception:
            p = {}
        ctx.append(p)
    cx = pd.DataFrame(ctx)
    if "hours_since_peak" in cx:
        c2 = c.reset_index(drop=True).join(cx)
        print("\nPnL по свежести пампа (hours_since_peak):")
        print(c2.groupby(pd.cut(c2.hours_since_peak, [0, 2, 4, 6, 24]))
              .agg(n=("pnl", "size"), pnl=("pnl", "sum"),
                   wr=("pnl", lambda x: (x > 0).mean())).round(2).to_string())

    # вердикт
    print("\nВЕРДИКТ:")
    if n < 30:
        print(f"  ⏳ n={n} < 30 — рано судить. Нужно ≥30–50 сделок для сигнала, ≥100 для уверенности.")
    elif lo > 0.5 and avg > 0:
        print(f"  ✅ Нижняя граница winrate {lo:.2f} > 0.5 и avg>0 — эдж подтверждается форвардом.")
    elif hi < 0.5 or total < 0:
        print(f"  ❌ Верх ДИ {hi:.2f} < 0.5 или PnL<0 — эдж не подтверждается, разбирать вход.")
    else:
        print(f"  ⚠ ДИ [{lo:.2f},{hi:.2f}] пересекает 0.5 — пока неотличимо от нуля, копить дальше.")


# ── B. SHADOW: что S5-детектор ДОЛЖЕН был выдать по свечам ────────────────────

# зеркало констант S5 (держать синхронно с strategy5_continuation.py)
LB, PUMP, FMIN, FMAX = 240, 8.0, 1.5, 6.0
RMIN, RMAX, RPMIN, RPMAX = 2, 6, 0.4, 4.0
EF, ES, LT, UT, TVM = 9, 21, 0.997, 1.01, 1.2
SLBUF, TP1R, TP2R, TRLR = 0.0015, 1.0, 2.0, 1.0
FEE, POS, TIMEOUT, COOLDOWN = 0.00055, 200.0, 90, 1800


def _ema_last(vals, n):
    k = 2 / (n + 1); e = vals[0]
    for v in vals[1:]:
        e = v * k + e * (1 - k)
    return e


def _load_candles(cdir, tf):
    d = {}
    for f in sorted(glob.glob(os.path.join(cdir, "candles_*.db"))):
        con = sqlite3.connect(f)
        df = pd.read_sql(f"select symbol,open_time,open,high,low,close,volume "
                         f"from candles where timeframe='{tf}'", con); con.close()
        for s, g in df.groupby("symbol"):
            d.setdefault(s, []).append(g)
    return {s: pd.concat(g).drop_duplicates("open_time").sort_values("open_time")
            .reset_index(drop=True) for s, g in d.items()}


def shadow_check(cdir: str) -> None:
    print("\n" + "=" * 64)
    print("B) SHADOW — что S5-детектор должен был выдать по свечам")
    print("=" * 64)
    C1 = _load_candles(cdir, "1m"); C15 = _load_candles(cdir, "15m")
    if not C1:
        print("нет candles_*.db в", cdir); return

    def hsp(sym, ts):
        df = C15.get(sym)
        if df is None or len(df) < 40: return None
        w = df[df.open_time <= ts].tail(96)
        if len(w) < 40: return None
        pk = w.high.idxmax()
        return (ts / 1000 - w.loc[pk, "open_time"] / 1000) / 3600

    rows = []
    for sym, df in C1.items():
        o, h, l, cl, v, t = (df.open.values, df.high.values, df.low.values,
                             df.close.values, df.volume.values, df.open_time.values)
        n = len(df); last_fire = -1e18; trig = LB + 2
        while trig < n:
            i = trig - 1
            if i < LB or t[trig] - last_fire < COOLDOWN * 1000: trig += 1; continue
            base = cl[i - LB]
            if base <= 0 or (cl[i] - base) / base * 100 < PUMP: trig += 1; continue
            hh = hsp(sym, int(t[i]))
            if hh is None or not (FMIN <= hh <= FMAX): trig += 1; continue
            red = 0; j = i
            while j > 0 and red <= RMAX:
                if cl[j] <= o[j] or h[j] <= h[j - 1]: red += 1; j -= 1
                else: break
            if not (RMIN <= red <= RMAX): trig += 1; continue
            swing = h[max(0, i - (RMAX + 2)):i + 1].max()
            if swing <= 0 or not (RPMIN <= (swing - l[i]) / swing * 100 <= RPMAX): trig += 1; continue
            cl60 = list(cl[max(0, trig - 59):trig + 1])
            if not (_ema_last(cl60, ES) * LT <= cl[i] <= _ema_last(cl60, EF) * UT): trig += 1; continue
            if v[i] > v[max(0, i - 30):i].mean(): trig += 1; continue
            if not (cl[trig] > h[i] and v[trig] > v[i] * TVM): trig += 1; continue
            entry = cl[trig]; sl = l[max(0, i - red):i + 1].min() * (1 - SLBUF); rr = entry - sl
            if rr <= 0: trig += 1; continue
            ts_s = int(t[trig] / 1000)
            rows.append({"symbol": sym, "unix": ts_s,
                         "time": pd.to_datetime(ts_s, unit="s"),
                         "entry": round(entry, 8), "hsp": round(hh, 1)})
            last_fire = t[trig]; trig += TIMEOUT
    S = pd.DataFrame(rows)
    print(f"ожидаемых сигналов S5 по свечам: {len(S)}")
    if len(S):
        print(S.sort_values("time")[["time", "symbol", "entry", "hsp"]].to_string(index=False))
        print("\nСверка: каждый такой сигнал должен иметь запись S5 в trades.db")
        print("с близкими symbol/entry_price/entry_time. Есть в shadow, нет в trades →")
        print("сканер пропустил (импорт, тайминг свечей, cooldown, MAX_OPEN_TRADES).")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", default="trades.db")
    ap.add_argument("--candles", default=".")
    ap.add_argument("--skip-shadow", action="store_true")
    a = ap.parse_args()
    forward_check(a.trades)
    if not a.skip_shadow:
        shadow_check(a.candles)
