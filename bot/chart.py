"""Генерация графика 1M свечей для уведомлений о закрытии сделки."""

from __future__ import annotations

import io
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd


def generate_close_chart(
    symbol: str,
    candles: list[dict],          # список {"open","high","low","close","volume","time"}
    entry_price: Optional[float] = None,
    exit_price: Optional[float] = None,
    level: Optional[float] = None,
    entry_time: Optional[float] = None,  # unix timestamp (сек) входа в позицию
    exit_time: Optional[float] = None,   # unix timestamp (сек) выхода из позиции
    n_candles: int = 120,         # сколько свечей показывать (120 × 1m = 2 часа)
    save_path: Optional[str] = None,  # если передан — сохранить PNG по этому пути
) -> bytes:
    """
    Вернуть PNG-байты графика в стиле образца:
    - чёрный фон
    - свечи (зелёные/красные)
    - объём внизу (те же цвета)
    - скользящая средняя 20 (пунктир жёлтый)
    - Volume Profile справа (горизонтальные бары)
    - POC линия + подпись
    - уровень грида (level) — горизонтальная линия
    - вход — стрелка под свечой входа, выход — стрелка над свечой выхода
    """
    if not candles or len(candles) < 2:
        return b""

    # --- TEMP DEBUG DUMP: снять реальные входные данные одного вызова на диск ---
    try:
        import json, os, time as _t
        _dbg_dir = "chart_debug"
        os.makedirs(_dbg_dir, exist_ok=True)
        _dbg_path = os.path.join(_dbg_dir, f"{symbol}_{int(_t.time())}.json")
        with open(_dbg_path, "w") as _f:
            json.dump({
                "symbol": symbol,
                "n_candles_arg": n_candles,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "level": level,
                "entry_time": entry_time,
                "exit_time": exit_time,
                "len_candles": len(candles),
                "candles_first": candles[0],
                "candles_last": candles[-1],
                "candles": candles,
            }, _f, default=str)
    except Exception:
        pass
    # --- END TEMP DEBUG DUMP ---

    # Normalize "time" field: collector uses "open_time", chart expects "time"
    for c in candles:
        if "time" not in c and "open_time" in c:
            c["time"] = c["open_time"]

    # Окно показа: n_candles минут ДО exit_time (а не до конца списка candles,
    # т.к. список может содержать свечи новее момента закрытия сделки).
    # Если exit_time не передан — берём окно от конца списка, как раньше.
    if exit_time is not None:
        exit_ts = exit_time * 1000 if exit_time < 1e12 else exit_time
        data = [c for c in candles if (c.get("time", 0) * 1000 if c.get("time", 0) < 1e12 else c.get("time", 0)) <= exit_ts]
        if not data:
            data = candles
        data = data[-n_candles:] if len(data) > n_candles else data
    else:
        data = candles[-n_candles:] if len(candles) > n_candles else candles

    # Расширить окно назад, если свеча входа (entry_time) старше текущего окна.
    if entry_time is not None:
        entry_ts = entry_time * 1000 if entry_time < 1e12 else entry_time
        pool = [c for c in candles if c.get("time", 0) <= (data[-1]["time"] if data else 0)] if exit_time is not None else candles
        while len(data) < len(pool):
            t0 = data[0].get("time", 0)
            t0_ms = t0 * 1000 if t0 < 1e12 else t0
            if t0_ms <= entry_ts:
                break
            data = pool[-(len(data) + 1):]

    import logging as _logging
    _log = _logging.getLogger(__name__)
    t_first = data[0].get("time", 0)
    t_last = data[-1].get("time", 0)
    _log.info(f"chart slice: total={len(candles)} data={len(data)} t_first={t_first} t_last={t_last} entry_time={entry_time} exit_time={exit_time}")

    # --- DataFrame ---
    df = pd.DataFrame(data)
    # time может быть timestamp в мс или с
    if df["time"].iloc[0] > 1e12:
        df["time"] = pd.to_datetime(df["time"], unit="ms")
    else:
        df["time"] = pd.to_datetime(df["time"], unit="s")
    df = df.set_index("time")
    df = df[["open", "high", "low", "close", "volume"]].astype(float)

    opens  = df["open"].values
    highs  = df["high"].values
    lows   = df["low"].values
    closes = df["close"].values
    vols   = df["volume"].values
    n      = len(df)
    x      = np.arange(n)

    # --- Volume Profile ---
    price_min = lows.min()
    price_max = highs.max()
    n_bins    = 40
    bins      = np.linspace(price_min, price_max, n_bins + 1)
    vp        = np.zeros(n_bins)
    for i in range(n):
        lo, hi, vol = lows[i], highs[i], vols[i]
        for b in range(n_bins):
            overlap = min(hi, bins[b + 1]) - max(lo, bins[b])
            if overlap > 0 and (hi - lo) > 0:
                vp[b] += vol * overlap / (hi - lo)

    poc_bin  = int(np.argmax(vp))
    poc_price = (bins[poc_bin] + bins[poc_bin + 1]) / 2

    # --- Layout ---
    fig = plt.figure(figsize=(13, 7), facecolor="#0d0d0d")
    # 2 rows: candles (70%) + volume (30%)
    # 2 cols: chart (88%) + volume profile (12%)
    gs = fig.add_gridspec(
        2, 2,
        height_ratios=[3, 1],
        width_ratios=[10, 1.4],
        hspace=0.04, wspace=0.02,
        left=0.06, right=0.97, top=0.93, bottom=0.06,
    )
    ax_c  = fig.add_subplot(gs[0, 0])  # candles
    ax_v  = fig.add_subplot(gs[1, 0], sharex=ax_c)  # volume
    ax_vp = fig.add_subplot(gs[0, 1], sharey=ax_c)  # volume profile

    for ax in (ax_c, ax_v, ax_vp):
        ax.set_facecolor("#0d0d0d")
        ax.tick_params(colors="#888888", labelsize=7)
        for spine in ax.spines.values():
            spine.set_edgecolor("#333333")

    # --- Candles ---
    bull_col = "#40e0c0"   # cyan-green как на образце
    bear_col = "#e05060"   # красный

    for i in range(n):
        is_bull = closes[i] >= opens[i]
        col     = bull_col if is_bull else bear_col
        body_lo = min(opens[i], closes[i])
        body_hi = max(opens[i], closes[i])
        body_h  = max(body_hi - body_lo, (price_max - price_min) * 0.001)
        ax_c.add_patch(mpatches.Rectangle(
            (i - 0.35, body_lo), 0.7, body_h,
            facecolor=col, edgecolor=col, linewidth=0,
        ))
        ax_c.plot([i, i], [lows[i], highs[i]], color=col, linewidth=0.8)

    # --- MA20 ---
    ma_len = 20
    if n >= ma_len:
        ma = pd.Series(closes).rolling(ma_len).mean().values
        ax_c.plot(x, ma, color="#f5a623", linewidth=1.0, linestyle="--", alpha=0.85, zorder=3)

    # --- Горизонтальная линия уровня грида ---
    def _hline(ax, price, color, label, style="-"):
        ax.axhline(price, color=color, linewidth=0.8, linestyle=style, alpha=0.75)
        ax.text(n - 1, price, f" {label} {price:.6g}",
                color=color, fontsize=6.5, va="center", ha="right", zorder=5)

    if level is not None:
        _hline(ax_c, level, "#ffe066", "LVL", style=":")

    # --- Стрелки входа/выхода на конкретных свечах ---
    def _nearest_idx(ts: Optional[float]) -> Optional[int]:
        if ts is None:
            return None
        try:
            ts_ms = int(round((ts * 1000 if ts < 1e12 else ts)))
            # Сравниваем через int64 наносекунды, чтобы избежать ValueError
            # "Cannot losslessly convert units" при разной точности datetime64.
            target_ns = ts_ms * 1_000_000
            index_ns = df.index.values.astype("int64")
            pos = int(np.searchsorted(index_ns, target_ns))
            pos = min(max(pos, 0), n - 1)
            if pos > 0 and abs(index_ns[pos - 1] - target_ns) < abs(index_ns[pos] - target_ns):
                pos -= 1
            return pos
        except Exception:
            return None

    price_span = price_max - price_min
    arrow_gap = price_span * 0.025

    entry_idx = _nearest_idx(entry_time)
    exit_idx = _nearest_idx(exit_time)
    close_together = (
        entry_idx is not None and exit_idx is not None and abs(entry_idx - exit_idx) <= max(3, n // 20)
    )

    try:
        if entry_idx is not None and entry_price is not None:
            label_x = max(entry_idx - (n * 0.03 if close_together else 0), n * 0.04)
            y_tip = lows[entry_idx] - arrow_gap * 0.3
            y_tail = y_tip - arrow_gap * 1.3
            ax_c.annotate(
                "", xy=(entry_idx, y_tip), xytext=(entry_idx, y_tail),
                arrowprops=dict(arrowstyle="-|>", color="#40c0e0", lw=1.6, mutation_scale=14),
                zorder=6,
            )
            ax_c.text(label_x, y_tail - arrow_gap * 0.3, f"ENTRY {entry_price:.6g}",
                       color="#40c0e0", fontsize=6.5, va="top",
                       ha="right" if close_together else "center", zorder=6)
    except Exception:
        pass

    try:
        if exit_idx is not None and exit_price is not None:
            label_x = min(exit_idx + (n * 0.03 if close_together else 0), n * 0.96)
            col_exit = "#40e080" if exit_price >= (entry_price or exit_price) else "#e05060"
            y_tip = highs[exit_idx] + arrow_gap * 0.3
            y_tail = y_tip + arrow_gap * 1.3
            ax_c.annotate(
                "", xy=(exit_idx, y_tip), xytext=(exit_idx, y_tail),
                arrowprops=dict(arrowstyle="-|>", color=col_exit, lw=1.6, mutation_scale=14),
                zorder=6,
            )
            ax_c.text(label_x, y_tail + arrow_gap * 0.3, f"EXIT {exit_price:.6g}",
                       color=col_exit, fontsize=6.5, va="bottom",
                       ha="left" if close_together else "center", zorder=6)
    except Exception:
        pass

    # --- POC ---
    ax_c.axhline(poc_price, color="#44cc66", linewidth=0.7, linestyle=":", alpha=0.8)

    # --- Volume ---
    vol_colors = [bull_col if closes[i] >= opens[i] else bear_col for i in range(n)]
    ax_v.bar(x, vols, color=vol_colors, width=0.8, alpha=0.85)
    ax_v.set_ylim(0, vols.max() * 1.1)

    # --- Volume Profile bars ---
    bar_h = (bins[1] - bins[0]) * 0.85
    max_vp = vp.max() if vp.max() > 0 else 1
    for b in range(n_bins):
        bar_w = vp[b] / max_vp
        mid   = (bins[b] + bins[b + 1]) / 2
        color = "#44cc66" if b == poc_bin else "#c04040"
        ax_vp.add_patch(mpatches.Rectangle(
            (0, mid - bar_h / 2), bar_w, bar_h,
            facecolor=color, alpha=0.75, linewidth=0,
        ))
    ax_vp.set_xlim(0, 1.05)
    ax_vp.text(0.05, poc_price, f"POC {poc_price:.6g}",
               color="#44cc66", fontsize=6.5, va="center", ha="left",
               transform=ax_vp.get_yaxis_transform())
    ax_vp.set_xticks([])
    ax_vp.yaxis.set_visible(False)

    # --- X-axis ticks ---
    tick_step = max(1, n // 8)
    tick_indices = list(range(0, n, tick_step))
    ax_v.set_xticks(x[tick_indices])
    ax_v.set_xticklabels(
        [df.index[i].strftime("%H:%M") for i in tick_indices],
        color="#888888", fontsize=7,
    )
    plt.setp(ax_c.get_xticklabels(), visible=False)

    # --- Y-axis ---
    ax_c.yaxis.tick_right()
    ax_c.yaxis.set_label_position("right")
    ax_v.yaxis.tick_right()
    ax_v.set_ylabel("Vol", color="#666666", fontsize=7, labelpad=2)
    ax_v.yaxis.set_label_position("right")

    # --- Title ---
    fig.text(0.07, 0.955, f"{symbol}  1M", color="#dddddd",
             fontsize=11, fontweight="bold", va="top")

    # --- Grid ---
    ax_c.grid(axis="y", color="#222222", linewidth=0.5, linestyle="-")
    ax_v.grid(axis="y", color="#222222", linewidth=0.5, linestyle="-")

    ax_c.set_xlim(-0.8, n + 0.2)
    ax_c.margins(y=0.12)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, facecolor="#0d0d0d")
    plt.close(fig)
    buf.seek(0)
    png_bytes = buf.read()

    if save_path:
        try:
            import os
            os.makedirs(os.path.dirname(save_path), exist_ok=True) if os.path.dirname(save_path) else None
            with open(save_path, "wb") as f:
                f.write(png_bytes)
        except Exception:
            pass  # не роняем генерацию если сохранение не удалось

    return png_bytes
