#!/usr/bin/env python3
"""Backfill live_trades.db PnL from Bybit /v5/execution/list.

USAGE:
  python backfill_live_pnl.py --api-key KEY --api-secret SECRET
  python backfill_live_pnl.py --api-key KEY --api-secret SECRET --apply
  python backfill_live_pnl.py --api-key KEY --api-secret SECRET --since 2026-06-21
"""
from __future__ import annotations
import argparse, asyncio, datetime as dt, hashlib, hmac, json, os, shutil, sqlite3, sys, time
try:
    import aiohttp
except ImportError:
    print('pip install aiohttp'); sys.exit(1)

BYBIT_DEMO  = 'https://api-demo.bybit.com'
RECV_WINDOW = '5000'
LEAK_REASONS = ('%_no_position', '%_no_exit_price', 'reconcile_no_fill')


def _qs(params):
    return '&'.join(f'{k}={v}' for k, v in sorted(params.items()))


def _headers(params, api_key, api_secret):
    ts = str(int(time.time() * 1000))
    sig = hmac.new(
        api_secret.encode(),
        (ts + api_key + RECV_WINDOW + _qs(params)).encode(),
        hashlib.sha256
    ).hexdigest()
    return {
        'X-BAPI-API-KEY':     api_key,
        'X-BAPI-SIGN':        sig,
        'X-BAPI-SIGN-TYPE':   '2',
        'X-BAPI-TIMESTAMP':   ts,
        'X-BAPI-RECV-WINDOW': RECV_WINDOW,
        'Content-Type':       'application/json',
    }


async def fetch_executions(symbol, start_ms, api_key, api_secret):
    params = {'category': 'linear', 'symbol': symbol,
              'startTime': str(int(start_ms)), 'limit': '100'}
    url = BYBIT_DEMO + '/v5/execution/list?' + _qs(params)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=_headers(params, api_key, api_secret),
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                text = await resp.text()
        try:
            data = json.loads(text)
        except Exception:
            print(f'  Bybit non-JSON response ({symbol}): {text[:120]}', file=sys.stderr)
            return []
        rc = data.get('retCode')
        if rc != 0:
            print(f'  Bybit retCode={rc} ({symbol}): {data.get("retMsg", "")}', file=sys.stderr)
            return []
        return data.get('result', {}).get('list', [])
    except Exception as e:
        print(f'  fetch_executions({symbol}): {e}', file=sys.stderr)
        return []
def safe_grid_ids(raw):
    if not raw:
        return set()
    raw = raw.strip()
    if not raw or raw in ('null', '[]'):
        return set()
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return set(str(x) for x in parsed if x)
    except Exception:
        pass
    try:
        import ast
        parsed = ast.literal_eval(raw)
        if isinstance(parsed, list):
            return set(str(x) for x in parsed if x)
    except Exception:
        pass
    return set()


def reconstruct(execs, grid_ids, end_ts):
    if not execs:
        return {'cls': 'NO_DATA'}
    buy_qty = buy_val = sell_qty = sell_val = fee = 0.0
    filled_ids = set()
    for e in execs:
        try:
            q  = float(e.get('execQty',   0))
            pr = float(e.get('execPrice', 0))
            f  = float(e.get('execFee',   0) or 0)
            ets = float(e['execTime']) / 1000.0 if e.get('execTime') else None
        except (ValueError, TypeError):
            continue
        if q <= 0 or pr <= 0:
            continue
        if end_ts and ets and ets >= end_ts:
            continue
        fee += f
        side = e.get('side', '')
        if side == 'Buy':
            buy_qty += q; buy_val += q * pr
            oid = e.get('orderId')
            if oid and oid in grid_ids:
                filled_ids.add(oid)
        elif side == 'Sell':
            sell_qty += q; sell_val += q * pr
    if buy_qty <= 0: return {'cls': 'NO_FILL'}
    if sell_qty <= 0: return {'cls': 'OPEN?'}
    pnl = sell_val - buy_val - fee
    return {'cls': 'RECOVER',
            'entry_price': round(buy_val  / buy_qty,  8),
            'exit_price':  round(sell_val / sell_qty, 8),
            'qty':         round(buy_qty, 8),
            'pnl_usdt':    round(pnl, 6),
            'fill_count':  len(filled_ids) or 1}


def load_rows(db, since_ts):
    con = sqlite3.connect(db); con.row_factory = sqlite3.Row
    cond = ' OR '.join(['exit_reason LIKE ?'] * len(LEAK_REASONS)) + ' OR pnl_usdt IS NULL'
    params = list(LEAK_REASONS)
    sql = f'SELECT * FROM live_trades WHERE ({cond})'
    if since_ts: sql += ' AND entry_time >= ?'; params.append(since_ts)
    rows = list(con.execute(sql + ' ORDER BY entry_time', params))
    con.close(); return rows


def next_end_ts(db, symbol, after):
    con = sqlite3.connect(db)
    r = con.execute('SELECT MIN(entry_time) FROM live_trades WHERE symbol=? AND entry_time>?', (symbol, after)).fetchone()
    con.close()
    return r[0] if r and r[0] else None


def write_updates(db, updates):
    backup = f'{db}.bak_{int(time.time())}'
    shutil.copy2(db, backup)
    con = sqlite3.connect(db); now = time.time()
    for tid, flds in updates:
        flds['updated_at'] = now
        sets = ', '.join(f'{k} = ?' for k in flds)
        con.execute(f'UPDATE live_trades SET {sets} WHERE trade_id = ?', list(flds.values()) + [tid])
    con.commit(); con.close()
    return backup


async def run(args):
    if not os.path.exists(args.db):
        print(f'NOT FOUND: {args.db}'); sys.exit(1)
    mock = not (args.api_key and args.api_secret)
    if mock:
        print('No API keys - MOCK mode (no exchange calls).')
        print('Add --api-key KEY --api-secret SECRET to query Bybit.')
    since_ts = None
    if args.since:
        since_ts = dt.datetime.strptime(args.since, '%Y-%m-%d').replace(tzinfo=dt.timezone.utc).timestamp()
    rows = load_rows(args.db, since_ts)
    label = ('mock' if mock else 'API') + ', ' + ('DRY-RUN' if not args.apply else 'WILL WRITE')
    print(f'Candidates: {len(rows)} ({label})')
    if not rows: return
    def U(ts): return dt.datetime.utcfromtimestamp(ts).strftime('%m-%d %H:%M')
    updates, counts, total_pnl = [], {'RECOVER':0,'NO_FILL':0,'OPEN?':0,'NO_DATA':0}, 0.0
    print(f"{'symbol':<12}{'entry':>11}  {'old reason':<34} {'cls':<9} {'pnl':>9}  note")
    print('-' * 95)
    for r in rows:
        symbol, entry_time = r['symbol'], r['entry_time']
        grid_ids = safe_grid_ids(r['bybit_order_ids_json'])
        end_ts   = next_end_ts(args.db, symbol, entry_time)
        if mock:
            res = {'cls': 'NO_DATA'}
        else:
            execs = await fetch_executions(symbol, int((entry_time - 5) * 1000), args.api_key, args.api_secret)
            res   = reconstruct(execs, grid_ids, end_ts)
        cls = res['cls']; counts[cls] += 1
        if cls == 'RECOVER':
            total_pnl += res['pnl_usdt']
            updates.append((r['trade_id'], {
                'entry_price': res['entry_price'], 'exit_price': res['exit_price'],
                'bybit_position_qty': res['qty'], 'grid_fill_count': res['fill_count'],
                'pnl_usdt': res['pnl_usdt'], 'exit_reason': 'backfill_executions'}))
            note = f"{res['entry_price']} -> {res['exit_price']}  fills={res['fill_count']}"
            pnl_s = f"{res['pnl_usdt']:+.2f}"
        elif cls == 'NO_FILL':
            if r['pnl_usdt'] is None:
                updates.append((r['trade_id'], {'pnl_usdt': 0.0, 'exit_reason': 'backfill_no_fill'}))
            note, pnl_s = 'true no-fill', '0.00'
        elif cls == 'OPEN?':
            note, pnl_s = 'entry yes, exit no - check manually', ''
        else:
            note, pnl_s = 'no executions', ''
        old = str(r['exit_reason'] or '')[:33]
        print(f'{symbol:<12}{U(entry_time):>11}  {old:<34} {cls:<9} {pnl_s:>9}  {note}')
        if not mock: await asyncio.sleep(args.sleep)
    print('-' * 95)
    print(f'SUMMARY:')
    for k, v in counts.items(): print(f'  {k:<9}: {v}')
    print(f'  Recovered PnL: {round(total_pnl, 2)} USDT')
    print(f'  Rows to write: {len(updates)}')
    if mock or not args.apply:
        print('MOCK mode.' if mock else 'DRY-RUN: add --apply to write.')
        return
    backup = write_updates(args.db, updates)
    print(f'  Backup : {backup}')
    print(f'  Written: {len(updates)} rows. Done.')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db',         default='live_trades.db')
    p.add_argument('--api-key',    default=None, dest='api_key')
    p.add_argument('--api-secret', default=None, dest='api_secret')
    p.add_argument('--apply',      action='store_true')
    p.add_argument('--since',      default=None, help='YYYY-MM-DD')
    p.add_argument('--sleep',      type=float, default=0.5)
    asyncio.run(run(p.parse_args()))


if __name__ == '__main__':
    main()
