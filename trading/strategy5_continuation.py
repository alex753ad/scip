"""Strategy 5: Continuation — лонг на возобновлении импульса ВНУТРИ живого пампа.

Автономная сканерная стратегия (каркас как у S4), но триггер другой: не пробой
заранее заданного уровня, а паттерн «живой памп → короткий откат → возобновление
на объёме». Закрывает слепую зону S2, который просыпается уже на мёртвом дне пампа.

Гипотеза подтверждена псевдо-бэктестом на неделе (59 монет): при фильтре объёма
на возобновлении — +38$/нед, winrate 0.58, avg +0.46$/сделку, 83 сигнала. Без
фильтра объёма — около нуля. Ключ: объём триггер-свечи = участники ещё в стакане.

PAPER-only: пишет в trades.db через open_trade, реальных ордеров не ставит.

Условия входа (LONG):
  1. Памп живой: рост за ~240×1m свечей >= S5_PUMP_MIN_PCT.
  2. Свежесть: пик за 24ч (15m) в окне [S5_FRESH_MIN_H, S5_FRESH_MAX_H] часов.
  3. Откат: S5_RETRACE_MIN..MAX не-растущих 1m-свечей подряд, глубина от локального
     свинг-хая в [S5_RETRACE_MIN_PCT, S5_RETRACE_MAX_PCT]%.
  4. Цена отката у EMA(S5_EMA_FAST..S5_EMA_SLOW).
  5. Объём отката затухает: vol(откат) < средний vol пампа.
  6. Триггер: последняя закрытая свеча close > high свечи-конца-отката И
     vol(триггер) > vol(откат) * S5_TRIG_VOL_MULT  ← подтверждение спроса.

Управление позицией (как S4):
  SL = low отката * (1 - S5_SL_BUF). Rr = entry - SL.
  TP1 (50%) = entry + Rr*S5_TP1_R → стоп в безубыток.
  TP2 (50%) = entry + Rr*S5_TP2_R. Трейлинг после TP1 = current - Rr*S5_TRAIL_R.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Optional

from trading.base_strategy import BaseStrategy
from trading.trade_log import open_trade, add_trade_event
from trading import s5_log
from data.collector import candles_1m, candles_5m, candles_15m
from logger import logger

try:
    from data.collector import get_delta as _get_delta
except Exception:  # pragma: no cover
    _get_delta = None
try:
    from analysis.market_phase import detect_market_phase as _detect_phase
except Exception:  # pragma: no cover
    _detect_phase = None

# ── Константы (в модуле, как у S4 — не трогаем контракт constants.py) ─────────
# Вход
S5_PUMP_LOOKBACK_1M   = 240    # окно оценки роста пампа (~4ч)
S5_PUMP_MIN_PCT       = 8.0    # % рост за окно, чтобы считать пампом
S5_FRESH_MIN_H        = 1.5    # моложе — хаос вершины (в бэктесте минус)
S5_FRESH_MAX_H        = 6.0    # старше — памп выдыхается
S5_RETRACE_MIN        = 2      # 1m-свечей отката (мин)
S5_RETRACE_MAX        = 6      # 1m-свечей отката (макс)
S5_RETRACE_MIN_PCT    = 0.4    # глубина отката от свинг-хая, мин %
S5_RETRACE_MAX_PCT    = 4.0    # макс % (глубже = уже разворот, не откат)
S5_EMA_FAST           = 9
S5_EMA_SLOW           = 21
S5_EMA_LOWER_TOL      = 0.997  # цена не ниже EMA_slow*tol
S5_EMA_UPPER_TOL      = 1.01   # и не выше EMA_fast*tol
S5_TRIG_VOL_MULT      = 1.2    # объём триггер-свечи / объём конца отката — КЛЮЧ

# Выход (R-мультипликаторы от риска Rr = entry - SL)
S5_SL_BUF             = 0.0015
S5_TP1_R              = 1.0
S5_TP2_R              = 2.0
S5_TRAIL_R            = 1.0
S5_MIN_TRADE_MIN      = 1.0    # не закрывать раньше (шум)

# Сканирование
S5_SCAN_INTERVAL_SECONDS = 5
S5_FIRE_COOLDOWN_SEC     = 7200  # [FIX COOLDOWN] 2ч вместо 30 мин. При 30 мин бот
# перезаходил в XPIN×4/BUS×4/SENT×3 за день — серии убытков −6.38/−3.4/−2.2$.
# 2ч убирают «долбёж» в нисходящий тренд; свежих пампов за 2ч обычно ≥1 новый символ.


def _natr_pct(candles: list, period: int = 14):
    """NATR% (True Range, period свечей) / текущая цена * 100. None при нехватке."""
    if not candles or len(candles) < period + 1:
        return None
    w = candles[-(period + 1):]
    trs = []
    for i in range(1, len(w)):
        h, l, pc = w[i]["high"], w[i]["low"], w[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not trs:
        return None
    atr = sum(trs) / len(trs)
    cur = w[-1]["close"]
    return round(atr / cur * 100, 3) if cur > 0 else None


def _ema_last(vals: list[float], n: int) -> float:
    """EMA(n) последнего значения по списку close."""
    if not vals:
        return 0.0
    k = 2.0 / (n + 1)
    e = vals[0]
    for v in vals[1:]:
        e = v * k + e * (1 - k)
    return e


class Strategy5Continuation(BaseStrategy):
    strategy_id   = 5
    strategy_name = "continuation"

    def __init__(self) -> None:
        super().__init__()
        # symbol → timestamp последнего входа (антиспам)
        self._last_fire: dict[str, float] = {}
        self._scanner_task: Optional[asyncio.Task] = None

    # ── Публичный интерфейс ───────────────────────────────────────────────────

    async def on_event(self, event: dict) -> None:
        """S5 событий не использует — работает от собственного сканера."""
        return

    def start_scanner(self) -> None:
        """Запустить фоновый сканер. Вызвать один раз из strategy_runner."""
        if self._scanner_task is None or self._scanner_task.done():
            self._scanner_task = asyncio.create_task(
                self._scan_loop(), name="s5_continuation_scanner",
            )

    # ── Сканер ────────────────────────────────────────────────────────────────

    async def _scan_loop(self) -> None:
        while True:
            await asyncio.sleep(S5_SCAN_INTERVAL_SECONDS)
            try:
                for symbol in list(candles_1m.keys()):
                    try:
                        await self._scan_symbol(symbol)
                    except Exception as e:
                        logger.debug("S5 scan error", symbol=symbol, error=str(e))
            except Exception as e:
                logger.error("S5 scan_loop error", error=str(e))

    def _hours_since_pump_peak(self, symbol: str) -> Optional[float]:
        c15 = candles_15m.get(symbol, [])
        if len(c15) < 40:
            return None
        w = c15[-96:]
        peak_i = max(range(len(w)), key=lambda i: w[i]["high"])
        return (time.time() - w[peak_i]["open_time"] / 1000.0) / 3600.0

    def _detect_signal(self, symbol: str) -> Optional[dict]:
        """Вернуть dict с параметрами входа, если паттерн continuation есть сейчас."""
        c1m = candles_1m.get(symbol, [])
        if len(c1m) < S5_PUMP_LOOKBACK_1M + 10:
            return None

        closes = [c["close"] for c in c1m]
        highs  = [c["high"]  for c in c1m]
        lows   = [c["low"]   for c in c1m]
        opens  = [c["open"]  for c in c1m]
        vols   = [c["volume"] for c in c1m]

        # trigger = последняя закрытая свеча; retr_end = свеча перед ней
        trig_i = len(c1m) - 1
        i = trig_i - 1  # конец отката
        if i < S5_PUMP_LOOKBACK_1M:
            return None

        # 1. Памп живой
        base = closes[i - S5_PUMP_LOOKBACK_1M]
        if base <= 0:
            return None
        growth = (closes[i] - base) / base * 100.0
        if growth < S5_PUMP_MIN_PCT:
            return None

        # 2. Свежесть
        hsp = self._hours_since_pump_peak(symbol)
        if hsp is None or not (S5_FRESH_MIN_H <= hsp <= S5_FRESH_MAX_H):
            return None

        # 3. Откат: 2..6 не-растущих свечей подряд, заканчивая на i
        red = 0
        j = i
        while j > 0 and red <= S5_RETRACE_MAX:
            if closes[j] <= opens[j] or highs[j] <= highs[j - 1]:
                red += 1
                j -= 1
            else:
                break
        if not (S5_RETRACE_MIN <= red <= S5_RETRACE_MAX):
            return None

        # Глубина отката от локального свинг-хая
        lookback = S5_RETRACE_MAX + 2
        swing_hi = max(highs[max(0, i - lookback): i + 1])
        if swing_hi <= 0:
            return None
        retr_pct = (swing_hi - lows[i]) / swing_hi * 100.0
        if not (S5_RETRACE_MIN_PCT <= retr_pct <= S5_RETRACE_MAX_PCT):
            return None

        # 4. Цена отката у EMA
        ef = _ema_last(closes[-60:], S5_EMA_FAST)
        es = _ema_last(closes[-60:], S5_EMA_SLOW)
        if not (es * S5_EMA_LOWER_TOL <= closes[i] <= ef * S5_EMA_UPPER_TOL):
            return None

        # 5. Объём отката затухает
        pump_vol = sum(vols[max(0, i - 30):i]) / max(1, len(vols[max(0, i - 30):i]))
        if vols[i] > pump_vol:
            return None

        # 6. Триггер: close выше хая конца отката + подтверждение объёмом
        if not (closes[trig_i] > highs[i] and vols[trig_i] > vols[i] * S5_TRIG_VOL_MULT):
            return None

        # SL под лоу отката, риск, цели в R
        retr_low = min(lows[max(0, i - red): i + 1])
        sl = retr_low * (1.0 - S5_SL_BUF)
        entry = closes[trig_i]
        rr = entry - sl
        if rr <= 0:
            return None

        # ── Расширенный контекст входа (основание) для калибровки ──────────────
        # Затухание волатильности и NATR из 15m
        vol_decay = natr_now = None
        c15 = candles_15m.get(symbol, [])
        if len(c15) >= 40:
            w15 = c15[-96:]
            pk = max(range(len(w15)), key=lambda k: w15[k]["high"])

            def _rng(cs):
                xs = [(c["high"] - c["low"]) / c["close"] for c in cs if c["close"] > 0]
                return sum(xs) / len(xs) if xs else 0.0
            now_rng = _rng(c15[-4:])
            peak_rng = _rng(w15[max(0, pk - 2): pk + 3])
            vol_decay = round(now_rng / peak_rng, 2) if peak_rng > 0 else None
            natr_now = round(now_rng * 100, 2)

        natr_1m = _natr_pct(c1m)
        natr_5m = _natr_pct(candles_5m.get(symbol, []))
        natr_15m = _natr_pct(c15)

        # Фаза рынка (детектор S2) — на своём ATR 1m
        market_phase = None
        if _detect_phase is not None:
            trs = [highs[k] - lows[k] for k in range(max(1, len(c1m) - 14), len(c1m))]
            atr1m = sum(trs) / len(trs) if trs else 0.0
            try:
                market_phase = _detect_phase(symbol, atr1m, entry).phase.value
            except Exception:
                market_phase = None

        # Дельта ордерфлоу: приоритет — aggTrades буфер (если символ под монитором),
        # fallback — разность объёма бычьих/медвежьих 1m-свечей за последние 5 минут.
        # [FIX DELTA] aggTrades пуст для символов вне активного монитора → используем прокси.
        delta = buy_v = sell_v = None
        try:
            if _get_delta is not None:
                dd = _get_delta(symbol)
                if dd.get("trades", 0) > 0:
                    # реальная дельта из стрима
                    delta = round(dd["delta"], 4)
                    buy_v = round(dd["buy_vol"], 4)
                    sell_v = round(dd["sell_vol"], 4)
            if delta is None:
                # прокси: объём зелёных (close>open) vs красных 1m за 5 свечей
                last5 = c1m[-5:]
                bv = sum(c["volume"] for c in last5 if c["close"] > c["open"])
                sv = sum(c["volume"] for c in last5 if c["close"] <= c["open"])
                if bv + sv > 0:
                    buy_v = round(bv, 4)
                    sell_v = round(sv, 4)
                    delta = round(bv - sv, 4)
        except Exception:
            pass

        ema_dist_pct = round((closes[i] - es) / es * 100, 3) if es > 0 else None
        trig_vr = round(vols[trig_i] / vols[i], 2) if vols[i] > 0 else 0.0
        basis = (f"pump+{growth:.1f}% {hsp:.1f}h retr{retr_pct:.1f}%/{red}c "
                 f"trigvol{trig_vr:.1f}x phase={market_phase} vdecay={vol_decay}")

        return {
            "ts": time.time(),
            "entry": entry,
            "sl": sl,
            "tp1": entry + rr * S5_TP1_R,
            "tp2": entry + rr * S5_TP2_R,
            "rr": rr,
            "growth": round(growth, 1),
            "growth_pct": round(growth, 1),
            "hours_since_peak": round(hsp, 1),
            "retr_pct": round(retr_pct, 2),
            "retr_candles": red,
            "trig_vol_ratio": trig_vr,
            "swing_hi": swing_hi,
            "market_phase": market_phase,
            "vol_decay": vol_decay,
            "natr_now_pct": natr_now,
            "natr_1m": natr_1m,
            "natr_5m": natr_5m,
            "natr_15m": natr_15m,
            "ema_fast": round(ef, 10),
            "ema_slow": round(es, 10),
            "ema_dist_pct": ema_dist_pct,
            "delta_at_entry": delta,
            "buy_vol": buy_v,
            "sell_vol": sell_v,
            "basis": basis,
        }

    # Фазы, при которых continuation не работает: S5 ловит продолжение пампа,
    # а bleed/drop_tradeable = организованный слив. Данные 33 сделок:
    # bleed wr=0.25 (-1.66$), drop_tradeable wr=0.33 (-7.92$) → блокируем.
    S5_BLOCKED_PHASES = {"bleed", "drop_tradeable"}

    async def _scan_symbol(self, symbol: str) -> None:
        # антиспам по символу
        last = self._last_fire.get(symbol, 0.0)
        if time.time() - last < S5_FIRE_COOLDOWN_SEC:
            return
        if not await self._can_open_trade(symbol):
            return

        # [FIX PHASE GATE] Проверяем фазу ДО детектора сигнала — дёшево и быстро.
        # Фаза считается на свежих 1m-свечах, до тяжёлой логики _detect_signal.
        phase_val = None
        if _detect_phase is not None:
            c1m_tmp = candles_1m.get(symbol, [])
            if len(c1m_tmp) >= 20:
                try:
                    atr_tmp = sum(c["high"] - c["low"] for c in c1m_tmp[-14:]) / 14
                    phase_val = _detect_phase(symbol, atr_tmp, c1m_tmp[-1]["close"]).phase.value
                except Exception:
                    phase_val = None

        if phase_val in self.S5_BLOCKED_PHASES:
            logger.debug(
                "S5 skip: blocked phase",
                symbol=symbol, phase=phase_val,
                reason="bleed/drop_tradeable → wr<0.35 на живых данных",
            )
            return

        sig = self._detect_signal(symbol)
        if sig is None:
            return
        self._last_fire[symbol] = time.time()
        # Записать сигнал с основанием в выделенную S5-БД (best-effort)
        trade_id = str(uuid.uuid4())
        sig["symbol"] = symbol
        sig["trade_id"] = trade_id
        signal_id = await s5_log.log_signal(sig)
        await self._open_trade(symbol, sig, trade_id, signal_id)

    # ── Открытие сделки (paper) ───────────────────────────────────────────────

    async def _open_trade(self, symbol: str, sig: dict, trade_id: str,
                          signal_id: int) -> None:
        entry = sig["entry"]
        trade = {
            "trade_id":                trade_id,
            "strategy_id":             self.strategy_id,
            "strategy_name":           self.strategy_name,
            "symbol":                  symbol,
            "level":                   round(sig["swing_hi"], 10),
            "level_type":              "pump_continuation",
            "level_side":              "resistance",
            "entry_signal":            "continuation",
            "strength_at_entry":       0,
            "p_bounce_at_entry":       0.0,
            "expected_depth_at_entry": 0.0,
            "approach_style":          "impulse",
            "vol_ratio_at_entry":      sig["trig_vol_ratio"],
            "atr_at_entry":            sig["rr"],   # риск-единица (для трейлинга)
            "entry_price":             entry,
            "entry_time":              time.time(),
            "position_size":           self.POSITION_SIZE_USDT,
            "direction":               "long",
            "grid_orders_json":        None,
            "grid_fill_count":         None,
        }
        await open_trade(trade)

        params_note = json.dumps({
            "stop_loss":               round(sig["sl"], 10),
            "take_profit_1":           round(sig["tp1"], 10),
            "take_profit_2":           round(sig["tp2"], 10),
            "rr":                      round(sig["rr"], 10),
            "tp1_hit":                 False,
            "stop_moved_to_breakeven": False,
            # контекст для калибровки
            "growth_pct":              sig["growth"],
            "hours_since_peak":        sig["hours_since_peak"],
            "retr_pct":                sig["retr_pct"],
            "trig_vol_ratio":          sig["trig_vol_ratio"],
        })
        await add_trade_event(trade_id, "params_set", entry, params_note)

        # Зеркалим сделку в выделенную S5-БД (сделки + сигналы + основание)
        await s5_log.open_trade({
            "trade_id": trade_id, "signal_id": signal_id, "symbol": symbol,
            "entry_time": trade["entry_time"], "entry_price": entry,
            "sl": sig["sl"], "tp1": sig["tp1"], "tp2": sig["tp2"], "rr": sig["rr"],
            "basis": sig.get("basis"),
        })

        await self._send_open_message(trade, sig["sl"], sig["tp1"], sig["tp2"])
        logger.info(
            "S5 trade opened", trade_id=trade_id, symbol=symbol, entry=entry,
            sl=round(sig["sl"], 10), tp1=round(sig["tp1"], 10), tp2=round(sig["tp2"], 10),
            growth=sig["growth"], hours_since_peak=sig["hours_since_peak"],
            trig_vol_ratio=sig["trig_vol_ratio"], market_phase=sig.get("market_phase"),
            natr_1m=sig.get("natr_1m"), natr_15m=sig.get("natr_15m"),
            basis=sig.get("basis"),
        )

    # ── Сопровождение (TP1 half → BE → trailing → TP2 / SL), как S4 ───────────

    async def _check_exit(self, trade: dict, current_price: float) -> None:
        trade_id    = trade["trade_id"]
        entry_price = trade["entry_price"]

        if (time.time() - trade["entry_time"]) / 60 < S5_MIN_TRADE_MIN:
            return

        params = self._extract_params(trade)
        if params is None:
            return

        stop_loss     = params["stop_loss"]
        take_profit_1 = params["take_profit_1"]
        take_profit_2 = params["take_profit_2"]
        tp1_hit       = params.get("tp1_hit", False)
        stop_moved    = params.get("stop_moved_to_breakeven", False)
        rr            = params.get("rr") or trade.get("atr_at_entry") or 0.0

        effective_stop = params.get("stop_loss", stop_loss) if stop_moved else stop_loss

        # TP2
        if current_price >= take_profit_2:
            avg_exit = (take_profit_1 + take_profit_2) / 2 if tp1_hit else take_profit_2
            await self._close_and_track(trade_id, trade["symbol"], avg_exit, "take_profit_2")
            await self._send_close_message(trade, avg_exit, "take_profit_2")
            return

        # TP1 → 50% + стоп НИЖЕ входа на 20% риска (не точный безубыток).
        # Точный BE entry слишком жёсткий: фитиль до точки входа на волатильных альтах
        # случается в 60%+ случаев → 13/22 сделок закрылись около нуля вместо TP2.
        # Буфер 0.2R даёт цене дышать, avg/сделку вырастает.
        BE_BUFFER = 0.2
        if not tp1_hit and current_price >= take_profit_1:
            new_stop = entry_price - rr * BE_BUFFER  # ниже входа, не точно на входе
            params["tp1_hit"] = True
            params["stop_moved_to_breakeven"] = True
            params["stop_loss"] = round(new_stop, 10)
            await add_trade_event(trade_id, "tp1_hit", current_price,
                                  json.dumps({"partial_exit_price": current_price,
                                              "partial_exit_pct": 50,
                                              "new_stop": round(new_stop, 10)}))
            await add_trade_event(trade_id, "params_updated", current_price, json.dumps(params))
            logger.info("S5 TP1 hit, stop → breakeven", trade_id=trade_id)
            return

        # Трейлинг после TP1
        if tp1_hit and rr > 0:
            new_trailing = current_price - rr * S5_TRAIL_R
            if new_trailing > effective_stop:
                params["stop_loss"] = round(new_trailing, 10)
                params["stop_moved_to_breakeven"] = True
                await add_trade_event(trade_id, "params_updated", current_price, json.dumps(params))

        # Stop loss
        if current_price <= effective_stop:
            if tp1_hit:
                avg_exit = (take_profit_1 + entry_price) / 2
                await self._close_and_track(trade_id, trade["symbol"], avg_exit, "stop_loss")
                await self._send_close_message(trade, avg_exit, "stop_loss")
            else:
                await self._close_and_track(trade_id, trade["symbol"], current_price, "stop_loss")
                await self._send_close_message(trade, current_price, "stop_loss")

    # ── Вспомогательные ───────────────────────────────────────────────────────

    def _extract_params(self, trade: dict) -> Optional[dict]:
        try:
            events = json.loads(trade.get("events_json") or "[]")
        except Exception:
            return None
        for ev in reversed(events):
            if ev.get("type") in ("params_updated", "params_set"):
                try:
                    return json.loads(ev["note"])
                except Exception:
                    return None
        return None

    # ── Telegram отключён (эксперимент) — только лог ──────────────────────────

    async def _send_open_message(self, trade: dict, stop_loss: float,
                                 take_profit_1: float, take_profit_2: float) -> None:
        logger.debug("S5 open (telegram disabled)", symbol=trade["symbol"],
                     entry=trade["entry_price"])

    async def _send_close_message(self, trade: dict, exit_price: float, reason: str) -> None:
        ep = trade["entry_price"]
        params = self._extract_params(trade) or {}
        tp1_hit = bool(params.get("tp1_hit"))
        tp1 = params.get("take_profit_1") or exit_price
        if tp1_hit:
            pnl_pct = ((tp1 - ep) / ep * 0.5 + (exit_price - ep) / ep * 0.5) * 100
        else:
            pnl_pct = (exit_price - ep) / ep * 100
        pnl_usdt = self.POSITION_SIZE_USDT * pnl_pct / 100
        await s5_log.close_trade(trade["trade_id"], exit_price, reason,
                                 tp1_hit, pnl_pct, pnl_usdt)
        logger.info(
            "S5 trade closed", symbol=trade["symbol"], reason=reason,
            pnl_pct=round(pnl_pct, 2), pnl_usdt=round(pnl_usdt, 3),
            tp1_hit=tp1_hit, trade_id=trade["trade_id"],
        )
