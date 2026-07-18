#!/usr/bin/env python3
"""Выгрузка 1m (или иных) klines с Binance USDT-M Futures в CSV.

Источник тот же, что у коллектора (Binance Futures REST), цифры совпадут.
Только stdlib, без зависимостей.

Пример:
  python klines_to_csv.py CLOUSDT "2026-06-21 22:00" "2026-06-22 06:00"
  python klines_to_csv.py CLOUSDT "2026-06-21 22:00" "2026-06-22 06:00" -i 1m -o clo.csv
Время — в UTC.
"""
import sys, csv, json, argparse, urllib.request
from datetime import datetime, timezone

BASE = "https://fapi.binance.com/fapi/v1/klines"  # USDT-M фьючерсы; для спота: api.binance.com/api/v3/klines

def to_ms(s):
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp() * 1000)

def fetch(symbol, interval, start_ms, end_ms):
    rows, cur = [], start_ms
    while cur < end_ms:
        url = f"{BASE}?symbol={symbol}&interval={interval}&startTime={cur}&endTime={end_ms}&limit=1500"
        with urllib.request.urlopen(url, timeout=30) as r:
            batch = json.load(r)
        if not batch:
            break
        rows.extend(batch)
        last_open = batch[-1][0]
        if len(batch) < 1500:
            break
        cur = last_open + 1  # сдвиг за последнюю свечу
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol")
    ap.add_argument("start", help='UTC "YYYY-MM-DD HH:MM"')
    ap.add_argument("end",   help='UTC "YYYY-MM-DD HH:MM"')
    ap.add_argument("-i", "--interval", default="1m")
    ap.add_argument("-o", "--out", default=None)
    a = ap.parse_args()

    out = a.out or f"{a.symbol}_{a.interval}.csv"
    rows = fetch(a.symbol, a.interval, to_ms(a.start), to_ms(a.end))

    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_utc", "open", "high", "low", "close", "volume", "close_time_utc", "trades"])
        for k in rows:
            ot = datetime.fromtimestamp(k[0] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            ct = datetime.fromtimestamp(k[6] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            w.writerow([ot, k[1], k[2], k[3], k[4], k[5], ct, k[8]])
    print(f"{out}: {len(rows)} свечей  ({a.start} → {a.end} UTC)")

if __name__ == "__main__":
    main()
