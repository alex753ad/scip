"""
ML Model Health Check
Reads trades.db and reports MAE, calibration, and WinRate by p_bounce buckets.

Usage:
    python ml_health_check.py [--db path/to/trades.db] [--min-trades 10]

Metrics:
    1. Calibration — WinRate per p_bounce bucket vs predicted probability
    2. MAE — mean absolute error between p_bounce and actual outcome (1=profit, 0=loss)
    3. Brier Score — overall probabilistic accuracy
    4. Direction accuracy — does higher p_bounce → better PnL?
    5. expected_depth MAE — how far off is depth prediction vs actual max_favorable_pct
"""

from __future__ import annotations

import argparse
import os  # FIX BUG-19: нужен для os.path.exists
import sqlite3
from collections import defaultdict


# ── Config ────────────────────────────────────────────────────────────────────

BUCKETS = [
    (0.00, 0.50, "0.00–0.50 (bearish)"),
    (0.50, 0.70, "0.50–0.70 (weak)   "),
    (0.70, 0.85, "0.70–0.85 (moderate)"),
    (0.85, 0.95, "0.85–0.95 (strong) "),
    (0.95, 1.01, "0.95–1.00 (high)   "),
]

# Сделки без реального PnL — не считаем как win/loss
SKIP_EXIT_REASONS = {"cancelled_no_fill", "timeout_no_fill"}


# ── Load ──────────────────────────────────────────────────────────────────────

def load_trades(db_path: str) -> list[dict]:
    # FIX BUG-19: sqlite3.connect создаёт пустую БД если файла нет → OperationalError на SELECT
    if not os.path.exists(db_path):
        print(f"  ❌ БД не найдена: {db_path}")
        return []
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT
                p_bounce_at_entry,
                expected_depth_at_entry,
                pnl_pct,
                pnl_usdt,
                strategy_name,
                exit_reason,
                strength_at_entry,
                max_favorable_pct,
                direction,
                level_type
            FROM trades
            WHERE status = 'closed'
        """)
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except sqlite3.OperationalError as e:
        print(f"  ❌ Ошибка чтения БД: {e}")
        return []


# ── Helpers ───────────────────────────────────────────────────────────────────

def is_win(trade: dict) -> bool | None:
    """True=profit, False=loss, None=skip (no fill)."""
    if trade["exit_reason"] in SKIP_EXIT_REASONS:
        return None
    return (trade["pnl_pct"] or 0) > 0


def bucket_label(p: float) -> str | None:
    for lo, hi, label in BUCKETS:
        if lo <= p < hi:
            return label
    return None


def fmt_pct(v: float) -> str:
    return f"{v:+.1f}%" if v is not None else "  n/a"


def fmt_bar(winrate: float, width: int = 20) -> str:
    filled = round(winrate * width)
    return "[" + "█" * filled + "░" * (width - filled) + "]"


# ── Report ────────────────────────────────────────────────────────────────────

def run(db_path: str, min_trades: int) -> None:
    trades = load_trades(db_path)

    # Split: with p_bounce and without
    with_pb  = [t for t in trades if (t["p_bounce_at_entry"] or 0) > 0]
    without  = [t for t in trades if (t["p_bounce_at_entry"] or 0) == 0]
    tradeable = [t for t in with_pb if t["exit_reason"] not in SKIP_EXIT_REASONS]

    print("=" * 60)
    print("  ML MODEL HEALTH REPORT")
    print("=" * 60)
    print(f"  Всего закрытых сделок : {len(trades)}")
    print(f"  С p_bounce > 0        : {len(with_pb)}")
    print(f"  Без p_bounce (=0)     : {len(without)}  ← ML не считал")
    print(f"  Реальных входов       : {len(tradeable)}  ← для метрик")
    print()

    if len(tradeable) < min_trades:
        print(f"  ⚠️  Слишком мало данных ({len(tradeable)} < {min_trades}). Результаты ненадёжны.")
        print()

    # ── 1. MAE & Brier Score ─────────────────────────────────────────

    mae_sum = 0.0
    brier_sum = 0.0
    n = 0
    for t in tradeable:
        p = t["p_bounce_at_entry"]
        actual = 1.0 if is_win(t) else 0.0
        mae_sum   += abs(p - actual)
        brier_sum += (p - actual) ** 2
        n += 1

    mae    = mae_sum   / n if n else None
    brier  = brier_sum / n if n else None

    print("── 1. Общая точность модели ─────────────────────────────────")
    if mae is not None:
        health = "✅ OK" if mae < 0.35 else ("⚠️  слабо" if mae < 0.45 else "❌ плохо")
        print(f"  MAE    : {mae:.4f}  {health}")
        print(f"           (0 = идеал, 0.5 = монетка, >0.5 = хуже случайного)")
        brier_health = "✅ OK" if brier < 0.25 else ("⚠️  слабо" if brier < 0.35 else "❌ плохо")
        print(f"  Brier  : {brier:.4f}  {brier_health}")
        print(f"           (0 = идеал, 0.25 = монетка)")
    print()

    # ── 2. Calibration — WinRate по корзинам ────────────────────────

    print("── 2. Калибровка: WinRate по корзинам p_bounce ──────────────")
    print(f"  {'Корзина':<22} {'n':>4}  {'WinRate':>8}  {'Avg PnL':>8}  {'Ожидание':>10}  Бар")
    print("  " + "-" * 58)

    bucket_data: dict[str, list] = defaultdict(list)
    for t in tradeable:
        p = t["p_bounce_at_entry"]
        lbl = bucket_label(p)
        if lbl:
            w = is_win(t)
            bucket_data[lbl].append({
                "win": w,
                "pnl": t["pnl_pct"] or 0,
                "p": p,
            })

    calibration_ok = True
    prev_wr = None
    for lo, hi, label in BUCKETS:
        items = bucket_data[label]
        if not items:
            print(f"  {label}  {'—':>4}  {'—':>8}  {'—':>8}  {'—':>10}")
            continue

        wins    = sum(1 for x in items if x["win"])
        total   = len(items)
        wr      = wins / total
        avg_pnl = sum(x["pnl"] for x in items) / total
        mid_p   = (lo + hi) / 2

        # Монотонность: WinRate должен расти с ростом p_bounce
        mono = ""
        if prev_wr is not None and total >= 3:
            if wr < prev_wr - 0.05:
                mono = " ⚠️"
                calibration_ok = False
        if total >= 3:
            prev_wr = wr

        bar = fmt_bar(wr)
        print(f"  {label}  {total:>4}  {wr:>7.1%}  {avg_pnl:>+7.2f}%  p_mid={mid_p:.2f}  {bar}{mono}")

    print()
    if calibration_ok:
        print("  ✅ Монотонность: WinRate растёт с p_bounce — модель откалибрована")
    else:
        print("  ⚠️  Нарушение монотонности — модель плохо откалибрована")
    print()

    # ── 3. Expected depth MAE ────────────────────────────────────────

    depth_trades = [
        t for t in tradeable
        if (t["expected_depth_at_entry"] or 0) > 0
        and (t["max_favorable_pct"] or 0) > 0
    ]
    print("── 3. Точность предсказания глубины (expected_depth) ────────")
    if depth_trades:
        depth_errors = [
            abs((t["expected_depth_at_entry"] or 0) - (t["max_favorable_pct"] or 0))
            for t in depth_trades
        ]
        depth_mae = sum(depth_errors) / len(depth_errors)
        avg_predicted = sum(t["expected_depth_at_entry"] or 0 for t in depth_trades) / len(depth_trades)
        avg_actual    = sum(t["max_favorable_pct"] or 0    for t in depth_trades) / len(depth_trades)
        depth_health  = "✅ OK" if depth_mae < 1.5 else ("⚠️  слабо" if depth_mae < 3.0 else "❌ плохо")
        print(f"  n              : {len(depth_trades)}")
        print(f"  Avg predicted  : {avg_predicted:.2f}%")
        print(f"  Avg actual     : {avg_actual:.2f}%")
        print(f"  MAE depth      : {depth_mae:.4f}%  {depth_health}")
    else:
        print("  Нет данных для оценки")
    print()

    # ── 4. По стратегиям ─────────────────────────────────────────────

    print("── 4. MAE по стратегиям ─────────────────────────────────────")
    strat_data: dict[str, list] = defaultdict(list)
    for t in tradeable:
        p = t["p_bounce_at_entry"]
        actual = 1.0 if is_win(t) else 0.0
        strat_data[t["strategy_name"]].append(abs(p - actual))

    for strat, errors in sorted(strat_data.items()):
        mae_s = sum(errors) / len(errors)
        health = "✅" if mae_s < 0.35 else ("⚠️" if mae_s < 0.45 else "❌")
        print(f"  {strat:<15} n={len(errors):>3}  MAE={mae_s:.4f}  {health}")
    print()

    # ── 5. Вердикт ───────────────────────────────────────────────────

    print("── 5. Вердикт ───────────────────────────────────────────────")
    if n < 30:
        print("  ⚠️  Выборка мала (<30 сделок) — переобучать модель рано.")
        print("      Нужно минимум 200 записей в history.db с новыми фильтрами.")
    elif mae is not None and mae < 0.35 and calibration_ok:
        print("  ✅ Модель работает корректно.")
    elif mae is not None and mae >= 0.45:
        print("  ❌ Модель не справляется. Запустить: python train_ml.py --db history.db --force")
    else:
        print("  ⚠️  Модель слабая. Рекомендуется переобучение после накопления данных.")
        print("      Запустить: python train_ml.py --db history.db --force")

    # Проверка: не застряли ли пороги (все p_bounce в узком диапазоне?)
    if with_pb:
        p_vals = [t["p_bounce_at_entry"] for t in with_pb]
        p_min, p_max = min(p_vals), max(p_vals)
        p_range = p_max - p_min
        if p_range < 0.1:
            print(f"\n  ❌ Пороги заморожены: p_bounce диапазон {p_min:.3f}–{p_max:.3f} (разброс {p_range:.3f})")
            print("      Модель выдаёт почти одинаковые значения → ML не влияет на решения.")
            print("      Проверить thresholds.json: THRESHOLD_HIGH и THRESHOLD_LOW слишком близко.")
        elif p_range < 0.2:
            print(f"\n  ⚠️  Узкий диапазон p_bounce: {p_min:.3f}–{p_max:.3f} (разброс {p_range:.3f})")
            print("      Пороги могут быть некорректными. Проверить thresholds.json.")

    print()
    print("=" * 60)


# ── Entry ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ML Model Health Check")
    parser.add_argument("--db", default="trades.db", help="Path to trades.db")
    parser.add_argument("--min-trades", type=int, default=10, help="Min trades to trust results")
    args = parser.parse_args()
    run(args.db, args.min_trades)
