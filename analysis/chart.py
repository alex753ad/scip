import io
from datetime import datetime, timezone
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from data.collector import candles_15m
from logger import logger


def generate_chart(symbol: str, levels: list[dict], broken_levels: list[dict] = None, c15m_override: list[dict] = None) -> bytes | None:
    c15m = c15m_override if c15m_override is not None else candles_15m.get(symbol)
    if not c15m or len(c15m) < 5:
        return None

    # Дедупликация по open_time
    seen = set()
    unique = []
    for c in c15m:
        if c["open_time"] not in seen:
            seen.add(c["open_time"])
            unique.append(c)
    candles = unique[-50:]
    opens = np.array([c["open"] for c in candles])
    highs = np.array([c["high"] for c in candles])
    lows = np.array([c["low"] for c in candles])
    closes = np.array([c["close"] for c in candles])
    volumes = np.array([c["volume"] for c in candles])
    n = len(candles)
    x = np.arange(n)

    # VWAP
    typical = (highs + lows + closes) / 3
    cum_tv = np.cumsum(typical * volumes)
    cum_v = np.cumsum(volumes)
    vwap = cum_tv / np.where(cum_v == 0, 1, cum_v)

    # Volume Profile — 100 zones
    price_min, price_max = float(lows.min()), float(highs.max())
    num_zones = 100
    zone_edges = np.linspace(price_min, price_max, num_zones + 1)
    zone_vol = np.zeros(num_zones)
    zone_buy = np.zeros(num_zones)
    zone_sell = np.zeros(num_zones)

    for i in range(n):
        lo_idx = np.searchsorted(zone_edges, lows[i], side="right") - 1
        hi_idx = np.searchsorted(zone_edges, highs[i], side="right") - 1
        lo_idx = max(0, min(lo_idx, num_zones - 1))
        hi_idx = max(0, min(hi_idx, num_zones - 1))
        span = hi_idx - lo_idx + 1
        vol_per_zone = volumes[i] / span
        is_buy = closes[i] > opens[i]
        for j in range(lo_idx, hi_idx + 1):
            zone_vol[j] += vol_per_zone
            if is_buy:
                zone_buy[j] += vol_per_zone
            else:
                zone_sell[j] += vol_per_zone

    zone_mids = (zone_edges[:-1] + zone_edges[1:]) / 2
    zone_h = zone_edges[1] - zone_edges[0]
    poc_idx = int(np.argmax(zone_vol))
    poc_price = float(zone_mids[poc_idx])

    # --- Plot ---
    bg = "#0d0d0d"
    fig = plt.figure(figsize=(14, 8), facecolor=bg, dpi=100)

    # GridSpec 2x2: свечи + VP сверху, объём + пусто снизу
    gs = fig.add_gridspec(2, 2, height_ratios=[4, 1], width_ratios=[85, 15],
                          hspace=0.05, wspace=0.02)
    ax = fig.add_subplot(gs[0, 0])
    ax_vp = fig.add_subplot(gs[0, 1], sharey=ax)
    ax_vol = fig.add_subplot(gs[1, 0], sharex=ax)

    ax.set_facecolor(bg)
    ax_vp.set_facecolor(bg)
    ax_vol.set_facecolor(bg)

    # Candles
    body_w = 0.6
    for i in range(n):
        color = "#26a69a" if closes[i] >= opens[i] else "#ef5350"
        y_body = min(opens[i], closes[i])
        h_body = abs(closes[i] - opens[i])
        if h_body == 0:
            h_body = (highs[i] - lows[i]) * 0.01 or price_max * 0.0001
        ax.add_patch(Rectangle(
            (x[i] - body_w / 2, y_body), body_w, h_body,
            facecolor=color, edgecolor=color, linewidth=0.5,
        ))
        ax.plot([x[i], x[i]], [lows[i], highs[i]], color=color, linewidth=0.7)

    # VWAP
    ax.plot(x, vwap, color="#ff9800", linestyle="--", linewidth=1.2, label="VWAP")

    # Support levels (только strength >= 4, в пределах ylim)
    visible_levels = []
    for lvl in levels:
        lv = lvl.get("level", 0)
        if lvl.get("strength", 0) < 4:
            continue
        if lv < price_min or lv > price_max:
            continue
        visible_levels.append(lv)

    sorted_levels = sorted(visible_levels)
    last_labeled = None
    for lv in sorted_levels:
        ax.axhline(y=lv, color="#26a69a", linewidth=0.8, linestyle="-", alpha=0.7)
        if last_labeled is None or abs(lv - last_labeled) / lv > 0.005:
            # FIX BUG-16: x=0.5 при xlim[-1..n+1] рисовало метки на первых свечах; n-1 = правый край
            ax.text(n - 1, lv, f" {lv}", fontsize=7, color="#26a69a", va="bottom", ha="right")
            last_labeled = lv

    # Broken levels (пробитые уровни)
    if broken_levels:
        broken_sorted = sorted(broken_levels, key=lambda l: l["level"])
        last_broken_labeled = None
        for lvl in broken_sorted:
            lv = lvl["level"]
            if lv < price_min or lv > price_max:
                continue
            color = "#00ff88" if lvl.get("breakout_type") == "zakol" else "#ff4444"
            ax.axhline(y=lv, color=color, linewidth=0.8, linestyle="--", alpha=0.7)
            if last_broken_labeled is None or abs(lv - last_broken_labeled) / lv > 0.005:
                # FIX BUG-16: x=0.5 рисовало метки на первых свечах; n-1 = правый край
                ax.text(n - 1, lv, f" {lv}", fontsize=7, color=color, va="bottom", ha="right")
                last_broken_labeled = lv

    # POC
    ax.axhline(y=poc_price, color="#ffeb3b", linewidth=0.8, linestyle=":", alpha=0.6)
    ax_vp.text(0.05, poc_price, f"POC {poc_price:.6g}", fontsize=7, color="#ffeb3b", va="center")

    ax.set_xlim(-1, n + 1)
    # ylim строго по свечам + 0.1%
    margin_lo = price_min * 0.001
    margin_hi = price_max * 0.001
    y_lo = price_min - margin_lo
    y_hi = price_max + margin_hi
    ax.set_ylim(y_lo, y_hi)

    ax.tick_params(colors="#666", labelsize=8)
    ax.spines[:].set_visible(False)
    ax.set_title(f"{symbol}  15M", color="#ccc", fontsize=11, loc="left", pad=8)

    # Volume Profile bars
    max_vol = zone_vol.max() if zone_vol.max() > 0 else 1
    for i in range(num_zones):
        w = zone_vol[i] / max_vol
        color = "#26a69a" if zone_buy[i] >= zone_sell[i] else "#ef5350"
        alpha = 0.5 if i != poc_idx else 0.9
        ax_vp.barh(zone_mids[i], w, height=zone_h * 0.8, color=color, alpha=alpha)

    ax_vp.set_xlim(0, 1.05)
    ax_vp.tick_params(left=False, labelleft=False, bottom=False, labelbottom=False)
    ax_vp.spines[:].set_visible(False)

    # Скрыть пустую ячейку [1,1]
    ax_empty = fig.add_subplot(gs[1, 1])
    ax_empty.set_facecolor(bg)
    ax_empty.axis("off")

    # Volume bars (в долларах)
    vol_usd = volumes * closes
    vol_colors = ["#26a69a" if closes[i] >= opens[i] else "#ef5350" for i in range(n)]
    ax_vol.bar(x, vol_usd, width=body_w, color=vol_colors, alpha=0.7)
    ax_vol.set_xlim(-1, n + 1)
    ax_vol.set_ylim(0, np.percentile(vol_usd, 95) * 1.1)
    ax_vol.ticklabel_format(axis="y", style="plain")
    ax_vol.yaxis.set_major_formatter(plt.FuncFormatter(
        lambda v, _: f"${v/1e6:.0f}M" if v >= 1e6 else f"${v/1e3:.0f}K" if v >= 1e3 else f"${v:.0f}"
    ))
    ax_vol.spines[:].set_visible(False)

    # Ось X — время HH:MM каждые 5 свечей (на основном графике)
    tick_idx = list(range(0, n, 5))
    tick_labels = []
    for i in tick_idx:
        ts = candles[i].get("open_time", 0)
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        tick_labels.append(dt.strftime("%H:%M"))
    ax.set_xticks(tick_idx)
    ax.set_xticklabels(tick_labels)
    ax.tick_params(axis="x", colors="#666", labelsize=8, labelbottom=True)
    ax_vol.tick_params(colors="#666", labelsize=8, labelbottom=False)

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=bg, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()
