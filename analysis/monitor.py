"""Level monitoring with breakout/rebound detection."""

import asyncio
import time
from data.collector import candles_15m, candles_1m, start_delta_tracking, stop_delta_tracking, get_delta, _stream_agg_trades
from bot.telegram import send_message
from constants import (
    VOLUME_BREAKOUT_RATIO,
    VOLUME_SPIKE_RATIO,
    VOLUME_SPIKE_RESET_RATIO,
    DISTANCE_RESET_ATR_MULTIPLIER,
    DISTANCE_PARTIAL_RESET_ATR_MULTIPLIER,
    WEAK_BREAKOUT_COOLDOWN_SECONDS,
    COLLECTOR_UPDATE_INTERVAL_SECONDS,
    PRESSURE_MIN_DIRECTIONAL_CANDLES,
    PRESSURE_ZONE_MIN_DISTANCE_PCT,
    PRESSURE_ZONE_MAX_DISTANCE_PCT,
    PRESSURE_VOLUME_MIN_RATIO,
    LEVEL_BROKEN_MIN_CANDLES,
    PUMP_MAX_BROKEN_LEVELS,
    PROXIMITY_ALERT_DISTANCE_PCT,
)
from logger import logger
from utils import calc_atr, detect_approach_style_from_candles
from models import state_manager

# Global cooldown tracking for pressure alerts
_pressure_alert_sent: dict[str, float] = {}  # key: "SYMBOL_LEVEL" -> timestamp
PRESSURE_ALERT_COOLDOWN = 3600  # 1 hour cooldown for pressure alerts
_PRESSURE_ALERT_TTL = PRESSURE_ALERT_COOLDOWN * 2  # evict entries older than 2× cooldown

# Global dedup for rebound messages — referenced inside start_monitor
_rebound_last_sent: dict[str, float] = {}  # key: "symbol" -> timestamp

# Global dedup for level_broken alerts — prevents spam when multiple monitor instances run for same symbol
_level_broken_sent: dict[str, float] = {}  # key: "SYMBOL_LEVEL" -> timestamp
LEVEL_BROKEN_ALERT_COOLDOWN = 300  # 5 min cooldown

# Global cooldown for volume_spike alerts — prevents spam on sustained high-volume candles
_volume_spike_sent: dict[str, float] = {}  # key: "SYMBOL_LEVEL" -> timestamp
VOLUME_SPIKE_ALERT_COOLDOWN = 300  # 5 min cooldown per symbol+level


def _evict_stale_pressure_alerts() -> None:
    """Remove entries from _pressure_alert_sent that are older than TTL.
    
    Call periodically (e.g. before each lookup) to prevent unbounded growth
    when the screener continuously adds new symbols (BUG-19 fix).
    """
    now = time.time()
    stale = [k for k, ts in _pressure_alert_sent.items() if now - ts > _PRESSURE_ALERT_TTL]
    for k in stale:
        del _pressure_alert_sent[k]


async def _handle_sweep(symbol: str, level: float, level_side: str, c1m: list[dict]):
    reclaim_vol = c1m[-1]["volume"]
    avg_vol = sum(c["volume"] for c in c1m[-20:]) / min(len(c1m), 20)
    ratio = round(reclaim_vol / avg_vol, 1) if avg_vol > 0 else 1.0
    # Уведомления в Telegram отключены при любом объёме (BUG-fix: ранее слали при ratio<2.0).
    logger.debug("sweep detected (telegram disabled)", symbol=symbol, level=level, ratio=ratio)


def _calc_trade_activity(symbol: str) -> dict:
    """
    Считает trades/min за 1м, 5м, 15м на основе поля trades в свечах.
    Вызывается в момент касания уровня.
    Returns: trades_per_min_1m, trades_per_min_5m, trades_per_min_15m, trades_increasing
    """
    c1m = candles_1m.get(symbol, [])
    if not c1m or "trades" not in c1m[-1]:
        return {"trades_per_min_1m": None, "trades_per_min_5m": None,
                "trades_per_min_15m": None, "trades_increasing": None}

    t1m = float(c1m[-1]["trades"])
    t5m = round(sum(c["trades"] for c in c1m[-5:]) / min(len(c1m), 5), 1) if len(c1m) >= 1 else None
    t15m = round(sum(c["trades"] for c in c1m[-15:]) / min(len(c1m), 15), 1) if len(c1m) >= 1 else None

    increasing = int(t1m > t5m > t15m) if (t5m is not None and t15m is not None) else None

    return {
        "trades_per_min_1m": t1m,
        "trades_per_min_5m": t5m,
        "trades_per_min_15m": t15m,
        "trades_increasing": increasing,
    }


async def start_monitor(
    symbol: str,
    level: float,
    level_side: str,
    stop_event: asyncio.Event = None,
    # profile fields for outcome saving
    approach_style: str = None,
    atr_ratio: float = None,
    vol_ratio: float = None,
    level_type: str = "body_level",
    strength: int = 0,
    p_bounce: float = 0.0,
    expected_depth: float = 0.0,
    ml_delta: int = 0,
    p_fast_breakout: float = None,
    approach: int = 0,
    on_bounce=None,  # async callable(symbol, level) — вызывается немедленно при bounce
) -> str | None:
    """Monitor a level until body of 1M candle breaks it.
    level_side: 'support' or 'resistance'
    Returns 'breakout' if level was broken with volume, None otherwise.
    """
    # FIX zombie-monitor (1/2): не-ASCII тикеры ('龙虾USDT', '币安人生USDT' и т.п.) —
    # это отображаемые имена из discovery, не торгуемые в live на Bybit. Они не должны
    # порождать активные мониторы (один такой символ давал ~31% всего skip-шума и
    # висел active с неинициализированной моделью — латентный риск, если разблокируется).
    if not symbol.isascii():
        logger.warning("monitor: skip non-ascii / non-tradable symbol",
                       symbol=symbol, level=level, level_type=level_type)
        return None

    # FIX zombie-monitor (2/2): монитор с неположительным p_bounce НИКОГДА не пройдёт
    # фильтр входа (на фильтре p_bounce<=0 → hard-block), но при этом крутится вечно и
    # эмитит skip-событие каждый цикл. Такой уровень либо неинициализирован (ML не
    # отскорил → p_bounce=0, expected_depth=0), либо жёстко заблокирован по touches>=2 —
    # в обоих случаях торговать его нельзя, держать активным бессмысленно и рискованно.
    if p_bounce is None or p_bounce <= 0.0:
        logger.info("monitor: skip level with non-positive p_bounce (won't pass entry filter)",
                    symbol=symbol, level=level, level_type=level_type,
                    p_bounce=p_bounce, expected_depth=expected_depth)
        return None

    atr = calc_atr(candles_1m.get(symbol, []))
    # P6: task_key для проверки "мёртвости" уровня (см. proximity-блок ниже)
    _p6_task_key = state_manager.get_state(symbol).make_task_key(level)

    touched = False
    approach_warned = False
    proximity_sent = False
    weak_breakout_sent = False
    weak_breakout_time = 0.0
    rebound_sent = False
    volume_spike_notified = False
    sweep_sent = False
    engulf_sent = False
    level_broken_sent = False
    classify_sent = False  # prevent duplicate _classify_and_log_level_event calls
    _outcome_saved = [False]  # mutable flag: True if monitor.py already wrote level_outcomes
    iteration = 0
    delta_stream_task = None
    delta_signal_sent = False
    touch_c1m_idx = 0  # index in c1m when touch happened
    touch_classify_at = 0  # c1m index when to classify (touch_idx + 5)

    # Track min price during monitoring for fill_depth_pct
    min_price_during = None
    max_price_during = None
    touch_start_time: float = 0.0
    vol_ratio_captured: float = vol_ratio if vol_ratio is not None else 1.0  # vol_ratio captured at moment of first touch
    _monitoring_start_time = time.time()
    _trade_activity: dict = {}  # trade activity snapshot at moment of touch

    def _make_result(reason, _touched=False):
        """Build result dict with outcome info."""
        fdp = 0.0
        if level > 0:
            if level_side == "support" and min_price_during is not None:
                fdp = (level - min_price_during) / level * 100
            elif level_side == "resistance" and max_price_during is not None:
                fdp = (max_price_during - level) / level * 100
            fdp = max(fdp, 0.0)

        if reason == "breakout":
            outcome = "breakout"
        elif not _touched:
            outcome = "no_reach"
        elif fdp >= 2.0:
            outcome = "partial_deep"
        elif fdp >= 1.0:
            outcome = "partial_mid"
        elif fdp >= 0.1:
            outcome = "partial_shallow"
        else:
            outcome = "bounce"

        return {
            "reason": reason,
            "outcome": outcome,
            "fill_depth_pct": round(fdp, 4),
            "approach_style": approach_style,
            "atr_ratio": atr_ratio,
            # FIX BUG-VOL: раньше при vol_ratio_captured==1.0 брался vol_ratio (объём
            # подхода к уровню), что давало неверный признак для ML. Теперь всегда
            # используется vol_ratio_captured — после фиксов выше он корректен.
            "vol_ratio_at_touch": vol_ratio_captured,
            "outcome_saved": _outcome_saved[0],
        }

    _monitor_result = None

    def _make_event(event_type: str, current_price: float, **extra) -> dict:
        """Build event dict for the event bus from local monitor context."""
        avg_vol_ctx = sum(
            c["volume"] for c in candles_1m.get(symbol, [])[-20:]
        ) / max(len(candles_1m.get(symbol, [])[-20:]), 1)
        last_vol = candles_1m.get(symbol, [{}])[-1].get("volume", 0)
        vr = round(last_vol / avg_vol_ctx, 2) if avg_vol_ctx > 0 else 1.0
        try:
            from analysis.trigger import get_btc_change_1m as _btc_chg
            _btc_change_1m = _btc_chg()
        except Exception:
            _btc_change_1m = None
        return {
            "event_type": event_type,
            "symbol": symbol,
            "level": level,
            "level_side": level_side,
            "level_type": level_type,
            "strength": strength,
            "p_bounce": p_bounce,
            "expected_depth": expected_depth,
            "ml_delta": ml_delta,
            "p_fast_breakout": p_fast_breakout,
            "approach_style": approach_style or "unknown",
            "vol_ratio": vr,
            "atr": atr,
            "current_price": current_price,
            "timestamp": time.time(),
            "monitoring_start_time": _monitoring_start_time,
            "approach": approach if approach is not None else _real_touches(symbol, level),
            "btc_change_1m": _btc_change_1m,
            **extra,
        }

    # FIX BUG-11: try/finally гарантирует отмену delta_stream_task при любом выходе
    try:
        while True:
            if stop_event and stop_event.is_set():
                _monitor_result = _make_result(None, touched)
                break

            iteration += 1
            if iteration % 60 == 0:
                atr = calc_atr(c1m)

            c1m = candles_1m.get(symbol, [])
            if c1m:
                last = c1m[-1]
                body_close = last["close"]
                body_open = last["open"]
                body_bottom = min(body_close, body_open)
                body_top = max(body_close, body_open)

                # Track extremes for fill_depth_pct
                if min_price_during is None or last["low"] < min_price_during:
                    min_price_during = last["low"]
                if max_price_during is None or last["high"] > max_price_during:
                    max_price_during = last["high"]

                if level_side == "support" and body_close < level:
                    avg_vol = sum(c["volume"] for c in c1m[-20:]) / min(len(c1m), 20)
                    breakout_vol_ratio = last["volume"] / avg_vol if avg_vol > 0 else 1.0
                    # Confirm breakout: need 2 consecutive closes below level to avoid zakol
                    prev_close_below = len(c1m) >= 2 and c1m[-2]["close"] < level
                    if breakout_vol_ratio >= VOLUME_BREAKOUT_RATIO and prev_close_below:
                        await send_message(
                            f"💥 {symbol} пробой {level} с объёмом ×{breakout_vol_ratio:.1f} — настоящий, выход"
                        )
                        # Refresh approach_style and p_bounce at moment of breakout.
                        # If monitor started with approach_style="unknown" (e.g. startup/phase1),
                        # ML scored p_bounce on unknown style → too low. Re-score now with
                        # current style so strategy3 gets accurate p_bounce in the event.
                        try:
                            from analysis.trigger import detect_approach_style
                            from analysis.ml_score import ml_score as _ml_score
                            _current_style = detect_approach_style(symbol) or approach_style or "unknown"
                            if _current_style != approach_style:
                                _lvl_tmp = {
                                    "type": level_type,
                                    "strength": strength,
                                    "vol_ratio": vol_ratio_captured,
                                    "atr_ratio": atr_ratio or 2.0,
                                    "approach_style": _current_style,
                                    "monitoring_age_minutes": time.time() - _monitoring_start_time,  # MEDIUM-5: seconds, ml_score converts to minutes
                                }
                                _ml = _ml_score(_lvl_tmp)
                                approach_style = _current_style
                                p_bounce = _ml["p_bounce"]
                                expected_depth = _ml["expected_depth"]
                                logger.debug(
                                    "monitor: breakout p_bounce refreshed style=%s p_bounce=%.3f",
                                    _current_style, p_bounce,
                                )
                        except Exception as _ml_e:
                            logger.debug("monitor: breakout p_bounce refresh failed: %s", _ml_e)
                        try:
                            from trading.event_bus import publish as _eb_publish
                            await _eb_publish(_make_event(
                                "breakout", body_close,
                                breakout_vol_ratio=round(breakout_vol_ratio, 2),
                            ))
                        except Exception as _eb_e:
                            logger.debug("event_bus publish error (breakout support): %s", _eb_e)
                        # ── Pump Phase: count broken level ────────────────
                        try:
                            from models import state_manager as _sm
                            _st = _sm.get_state(symbol)
                            _st.broken_since_pump += 1
                            from analysis.pump_phase import pump_health_score, get_pump_phase, calc_correction_pct
                            if _st.broken_since_pump >= PUMP_MAX_BROKEN_LEVELS:
                                _st.pump_phase = "dead"
                                _corr = calc_correction_pct(_st)
                                await send_message(
                                    f"🚫 {symbol} — памп завершён\n"
                                    f"   Пробито уровней без отскока: {_st.broken_since_pump}\n"
                                    f"   Коррекция от пика: {_corr:.0%}\n"
                                    f"   Мониторинг остановлен. Жду новый памп."
                                )
                            else:
                                _st.pump_health = pump_health_score(_st, body_close)
                                _st.pump_phase = get_pump_phase(_st.pump_health)
                        except Exception as _pp_e:
                            logger.debug("pump_phase update error (breakout support): %s", _pp_e)
                        # ─────────────────────────────────────────────────
                        # FIX BUG-VOL: при breakout без предварительного касания
                        # vol_ratio_captured = 1.0 (default). Перезаписываем объёмом
                        # пробойной свечи, чтобы vol_ratio_at_touch в history.db
                        # содержал реальный объём, а не заглушку → корректное обучение ML.
                        if not touched:
                            vol_ratio_captured = round(breakout_vol_ratio, 2)
                            # FIX-STYLE: при breakout без touch стиль тоже определяем здесь
                            from analysis.trigger import detect_approach_style as _das
                            approach_style = _das(symbol)
                            # FIX: capture trade activity at breakout moment (was empty {} for no-touch breakouts)
                            _trade_activity = _calc_trade_activity(symbol)
                            _outcome_saved[0] = True  # FIX BUG-C1: предотвращает дубль в _monitored
                        _monitor_result = _make_result("breakout", touched)
                        break
                    elif breakout_vol_ratio >= VOLUME_BREAKOUT_RATIO and not prev_close_below:
                        # High volume but only 1 candle below — possible zakol, wait for confirmation
                        now = time.time()
                        if not weak_breakout_sent or (now - weak_breakout_time) > WEAK_BREAKOUT_COOLDOWN_SECONDS:
                            await send_message(
                                f"⚠️ {symbol} закол {level} с объёмом ×{breakout_vol_ratio:.1f} — ждём подтверждения"
                            )
                            try:
                                from trading.event_bus import publish as _eb_publish
                                await _eb_publish(_make_event(
                                    "weak_breakout", body_close,
                                    breakout_vol_ratio=round(breakout_vol_ratio, 2),
                                ))
                            except Exception as _eb_e:
                                logger.debug("event_bus publish error (weak_breakout support): %s", _eb_e)
                            weak_breakout_sent = True
                            weak_breakout_time = now
                    else:
                        now = time.time()
                        if not weak_breakout_sent or (now - weak_breakout_time) > WEAK_BREAKOUT_COOLDOWN_SECONDS:
                            await send_message(
                                f"⚠️ {symbol} пробой {level} на слабом объёме (×{breakout_vol_ratio:.1f}) — возможен sweep, наблюдаем"
                            )
                            try:
                                from trading.event_bus import publish as _eb_publish
                                await _eb_publish(_make_event(
                                    "weak_breakout", body_close,
                                    breakout_vol_ratio=round(breakout_vol_ratio, 2),
                                ))
                            except Exception as _eb_e:
                                logger.debug("event_bus publish error (weak_breakout support low vol): %s", _eb_e)
                            weak_breakout_sent = True
                            weak_breakout_time = now
                elif level_side == "support" and body_close >= level:
                    pass  # don't reset — prevents spam on repeated dips

                if level_side == "resistance" and body_close > level:
                    avg_vol = sum(c["volume"] for c in c1m[-20:]) / min(len(c1m), 20)
                    breakout_vol_ratio = last["volume"] / avg_vol if avg_vol > 0 else 1.0
                    prev_close_above = len(c1m) >= 2 and c1m[-2]["close"] > level
                    if breakout_vol_ratio >= VOLUME_BREAKOUT_RATIO and prev_close_above:
                        await send_message(
                            f"💥 {symbol} пробой {level} с объёмом ×{breakout_vol_ratio:.1f} — настоящий, выход"
                        )
                        try:
                            from trading.event_bus import publish as _eb_publish
                            await _eb_publish(_make_event(
                                "breakout", body_close,
                                breakout_vol_ratio=round(breakout_vol_ratio, 2),
                            ))
                        except Exception as _eb_e:
                            logger.debug("event_bus publish error (breakout resistance): %s", _eb_e)
                        # ── Pump Phase: count broken level ────────────────
                        try:
                            from models import state_manager as _sm
                            _st = _sm.get_state(symbol)
                            _st.broken_since_pump += 1
                            from analysis.pump_phase import pump_health_score, get_pump_phase, calc_correction_pct
                            if _st.broken_since_pump >= PUMP_MAX_BROKEN_LEVELS:
                                _st.pump_phase = "dead"
                                _corr = calc_correction_pct(_st)
                                await send_message(
                                    f"🚫 {symbol} — памп завершён\n"
                                    f"   Пробито уровней без отскока: {_st.broken_since_pump}\n"
                                    f"   Коррекция от пика: {_corr:.0%}\n"
                                    f"   Мониторинг остановлен. Жду новый памп."
                                )
                            else:
                                _st.pump_health = pump_health_score(_st, body_close)
                                _st.pump_phase = get_pump_phase(_st.pump_health)
                        except Exception as _pp_e:
                            logger.debug("pump_phase update error (breakout resistance): %s", _pp_e)
                        # ─────────────────────────────────────────────────
                        # FIX BUG-VOL: аналогично support — при breakout без касания
                        # сохраняем объём пробойной свечи в vol_ratio_captured.
                        if not touched:
                            vol_ratio_captured = round(breakout_vol_ratio, 2)
                            # FIX-STYLE: при breakout без touch стиль тоже определяем здесь
                            from analysis.trigger import detect_approach_style as _das
                            approach_style = _das(symbol)
                        _monitor_result = _make_result("breakout", touched)
                        break
                    elif breakout_vol_ratio >= VOLUME_BREAKOUT_RATIO and not prev_close_above:
                        now = time.time()
                        if not weak_breakout_sent or (now - weak_breakout_time) > WEAK_BREAKOUT_COOLDOWN_SECONDS:
                            await send_message(
                                f"⚠️ {symbol} закол {level} с объёмом ×{breakout_vol_ratio:.1f} — ждём подтверждения"
                            )
                            try:
                                from trading.event_bus import publish as _eb_publish
                                await _eb_publish(_make_event(
                                    "weak_breakout", body_close,
                                    breakout_vol_ratio=round(breakout_vol_ratio, 2),
                                ))
                            except Exception as _eb_e:
                                logger.debug("event_bus publish error (weak_breakout resistance): %s", _eb_e)
                            weak_breakout_sent = True
                            weak_breakout_time = now
                    else:
                        now = time.time()
                        if not weak_breakout_sent or (now - weak_breakout_time) > WEAK_BREAKOUT_COOLDOWN_SECONDS:
                            await send_message(
                                f"⚠️ {symbol} пробой {level} на слабом объёме (×{breakout_vol_ratio:.1f}) — возможен sweep, наблюдаем"
                            )
                            try:
                                from trading.event_bus import publish as _eb_publish
                                await _eb_publish(_make_event(
                                    "weak_breakout", body_close,
                                    breakout_vol_ratio=round(breakout_vol_ratio, 2),
                                ))
                            except Exception as _eb_e:
                                logger.debug("event_bus publish error (weak_breakout resistance low vol): %s", _eb_e)
                            weak_breakout_sent = True
                            weak_breakout_time = now
                elif level_side == "resistance" and body_close <= level:
                    pass  # don't reset — prevents spam on repeated pops

                if level_side == "support" and last["low"] <= level * 1.002:
                    if not touched:
                        # Start delta tracking on first touch
                        start_delta_tracking(symbol)
                        if delta_stream_task is None or delta_stream_task.done():
                            delta_stream_task = asyncio.create_task(_stream_agg_trades(symbol))
                        touch_c1m_idx = len(c1m) - 1
                        touch_classify_at = touch_c1m_idx + 5  # classify after 5 x 1M candles
                        touch_start_time = time.time()
                        avg_vol = sum(c["volume"] for c in c1m[-20:]) / max(len(c1m[-20:]), 1)
                        vol_ratio_captured = round(last["volume"] / avg_vol, 2) if avg_vol > 0 else 1.0
                        _trade_activity = _calc_trade_activity(symbol)
                        # FIX-STYLE: переопределяем стиль в момент касания, а не при старте монитора
                        from analysis.trigger import detect_approach_style as _das
                        approach_style = _das(symbol)
                        logger.debug("Delta tracking started on touch", symbol=symbol, level=level)
                    touched = True
                if level_side == "resistance" and last["high"] >= level * 0.998:
                    if not touched:
                        start_delta_tracking(symbol)
                        if delta_stream_task is None or delta_stream_task.done():
                            delta_stream_task = asyncio.create_task(_stream_agg_trades(symbol))
                        touch_c1m_idx = len(c1m) - 1
                        touch_classify_at = touch_c1m_idx + 5
                        touch_start_time = time.time()
                        avg_vol = sum(c["volume"] for c in c1m[-20:]) / max(len(c1m[-20:]), 1)
                        vol_ratio_captured = round(last["volume"] / avg_vol, 2) if avg_vol > 0 else 1.0
                        _trade_activity = _calc_trade_activity(symbol)
                        # FIX-STYLE: переопределяем стиль в момент касания, а не при старте монитора
                        from analysis.trigger import detect_approach_style as _das
                        approach_style = _das(symbol)
                    touched = True

                # Classify touch event after 5 x 1M candles
                if touched and touch_classify_at > 0 and len(c1m) >= touch_classify_at and not rebound_sent:
                    if not classify_sent:
                        asyncio.create_task(_classify_and_log_level_event(
                            symbol, level, c1m, candles_15m.get(symbol, []),
                            min_price_during or level, touch_c1m_idx,
                            level_type=level_type, strength=strength,
                            approach_style=approach_style, atr_ratio=atr_ratio,
                            touch_start_time=touch_start_time,
                            monitoring_start_time=_monitoring_start_time,
                            vol_ratio_at_touch=vol_ratio_captured,
                            outcome_saved_flag=_outcome_saved,
                            trade_activity=_trade_activity,
                            level_side=level_side,
                            ml_delta=ml_delta,
                            p_fast_breakout=p_fast_breakout,
                            p_bounce=p_bounce,
                            expected_depth=expected_depth,
                        ))
                        classify_sent = True
                    touch_classify_at = 0  # reset so we don't classify again

                # Delta signal: buy pressure absorbing sells at support
                if touched and not delta_signal_sent:
                    d = get_delta(symbol, window_seconds=30)
                    if d["trades"] >= 10:  # enough data
                        if level_side == "support" and d["delta"] > 0 and d["buy_vol"] > d["sell_vol"] * 1.5:
                            await send_message(
                                f"⚡ {symbol} дельта разворот у {level}\n"
                                f"   Buy {d['buy_vol']:.1f} vs Sell {d['sell_vol']:.1f} за 30с\n"
                                f"   Покупатели поглощают продажи — вход"
                            )
                            delta_signal_sent = True
                            logger.info("Delta reversal signal sent", symbol=symbol, level=level,
                                       buy=d["buy_vol"], sell=d["sell_vol"])
                        elif level_side == "resistance" and d["delta"] < 0 and d["sell_vol"] > d["buy_vol"] * 1.5:
                            await send_message(
                                f"⚡ {symbol} дельта разворот у {level}\n"
                                f"   Sell {d['sell_vol']:.1f} vs Buy {d['buy_vol']:.1f} за 30с\n"
                                f"   Продавцы давят — шорт"
                            )
                            delta_signal_sent = True

                if level_side == "support" and touched and body_close > body_open and body_close > level:
                    avg_vol = sum(c["volume"] for c in c1m[-20:]) / min(len(c1m), 20)
                    if not rebound_sent:
                        if not classify_sent:
                            asyncio.create_task(_classify_and_log_level_event(
                                symbol, level, c1m, candles_15m.get(symbol, []),
                                min_price_during or level, touch_c1m_idx,
                                level_type=level_type, strength=strength,
                                approach_style=approach_style, atr_ratio=atr_ratio,
                                touch_start_time=touch_start_time,
                            monitoring_start_time=_monitoring_start_time,
                                vol_ratio_at_touch=vol_ratio_captured,
                                outcome_saved_flag=_outcome_saved,
                                trade_activity=_trade_activity,
                                level_side=level_side,
                                ml_delta=ml_delta, p_fast_breakout=p_fast_breakout,
                                p_bounce=p_bounce, expected_depth=expected_depth,
                            ))
                        else:
                            # classify already ran (5-candle timer) but bounce happened later — log it now
                            asyncio.create_task(_log_bounce_outcome(
                                symbol, level, min_price_during or level,
                                level_type=level_type, strength=strength,
                                approach_style=approach_style, atr_ratio=atr_ratio,
                                touch_start_time=touch_start_time,
                            monitoring_start_time=_monitoring_start_time,
                                vol_ratio_at_touch=vol_ratio_captured,
                                outcome_saved_flag=_outcome_saved,
                                trade_activity=_trade_activity,
                                level_side=level_side,
                                ml_delta=ml_delta, p_fast_breakout=p_fast_breakout,
                                p_bounce=p_bounce, expected_depth=expected_depth,
                            ))
                        classify_sent = True
                        rebound_sent = True
                        touched = False
                        delta_signal_sent = False
                        stop_delta_tracking(symbol)
                        if last["volume"] > avg_vol:
                            _now = time.time()
                            if _now - _rebound_last_sent.get(symbol, 0) > 60:
                                _rebound_last_sent[symbol] = _now
                                # Уведомление в Telegram отключено.
                                logger.debug("rebound confirmed (telegram disabled)",
                                             symbol=symbol, level=level, side="support")
                        try:
                            from trading.event_bus import publish as _eb_publish
                            await _eb_publish(_make_event("bounce", body_close))
                        except Exception as _eb_e:
                            logger.debug("event_bus publish error (bounce support): %s", _eb_e)
                        # ── Немедленно запустить resistance-монитор выше ──────
                        if on_bounce is not None:
                            try:
                                asyncio.create_task(on_bounce(symbol, level))
                            except Exception as _ob_e:
                                logger.debug("on_bounce callback error: %s", _ob_e)
                        # ─────────────────────────────────────────────────────
                        # ── Pump Phase: reset broken counter on confirmed bounce ──
                        try:
                            from models import state_manager as _sm
                            _st = _sm.get_state(symbol)
                            _st.broken_since_pump = 0
                            _st.last_bounce_time = time.time()
                            from analysis.pump_phase import pump_health_score, get_pump_phase
                            _st.pump_health = pump_health_score(_st, body_close)
                            _st.pump_phase = get_pump_phase(_st.pump_health)
                        except Exception as _pp_e:
                            logger.debug("pump_phase reset error (bounce support): %s", _pp_e)
                        # ────────────────────────────────────────────────────────

                if level_side == "resistance" and touched and body_close < body_open and body_close < level:
                    avg_vol = sum(c["volume"] for c in c1m[-20:]) / min(len(c1m), 20)
                    if not rebound_sent:
                        if not classify_sent:
                            asyncio.create_task(_classify_and_log_level_event(
                                symbol, level, c1m, candles_15m.get(symbol, []),
                                max_price_during or level, touch_c1m_idx,
                                level_type=level_type, strength=strength,
                                approach_style=approach_style, atr_ratio=atr_ratio,
                                touch_start_time=touch_start_time,
                            monitoring_start_time=_monitoring_start_time,
                                vol_ratio_at_touch=vol_ratio_captured,
                                outcome_saved_flag=_outcome_saved,
                                trade_activity=_trade_activity,
                                level_side=level_side,
                                ml_delta=ml_delta, p_fast_breakout=p_fast_breakout,
                                p_bounce=p_bounce, expected_depth=expected_depth,
                            ))
                        else:
                            asyncio.create_task(_log_bounce_outcome(
                                symbol, level, max_price_during or level,
                                level_type=level_type, strength=strength,
                                approach_style=approach_style, atr_ratio=atr_ratio,
                                touch_start_time=touch_start_time,
                            monitoring_start_time=_monitoring_start_time,
                                vol_ratio_at_touch=vol_ratio_captured,
                                outcome_saved_flag=_outcome_saved,
                                trade_activity=_trade_activity,
                                level_side=level_side,
                                ml_delta=ml_delta, p_fast_breakout=p_fast_breakout,
                                p_bounce=p_bounce, expected_depth=expected_depth,
                            ))
                        classify_sent = True
                        rebound_sent = True
                        touched = False
                        delta_signal_sent = False
                        stop_delta_tracking(symbol)
                        if last["volume"] > avg_vol:
                            _now = time.time()
                            if _now - _rebound_last_sent.get(symbol, 0) > 60:
                                _rebound_last_sent[symbol] = _now
                                # Уведомление в Telegram отключено.
                                logger.debug("rebound confirmed (telegram disabled)",
                                             symbol=symbol, level=level, side="resistance")
                        try:
                            from trading.event_bus import publish as _eb_publish
                            await _eb_publish(_make_event("bounce", body_close))
                        except Exception as _eb_e:
                            logger.debug("event_bus publish error (bounce resistance): %s", _eb_e)
                        # ── Pump Phase: reset broken counter on confirmed bounce ──
                        try:
                            from models import state_manager as _sm
                            _st = _sm.get_state(symbol)
                            _st.broken_since_pump = 0
                            _st.last_bounce_time = time.time()
                            from analysis.pump_phase import pump_health_score, get_pump_phase
                            _st.pump_health = pump_health_score(_st, body_close)
                            _st.pump_phase = get_pump_phase(_st.pump_health)
                        except Exception as _pp_e:
                            logger.debug("pump_phase reset error (bounce resistance): %s", _pp_e)
                        # ────────────────────────────────────────────────────────

                current_price = last["close"]
                if atr > 0:
                    distance = abs(current_price - level)
                    if distance > atr * DISTANCE_RESET_ATR_MULTIPLIER:
                        # If touched but price moved away without confirmed rebound — classify
                        if touched and not rebound_sent and not classify_sent:
                            asyncio.create_task(_classify_and_log_level_event(
                                symbol, level, c1m, candles_15m.get(symbol, []),
                                min_price_during or level, touch_c1m_idx,
                                level_type=level_type, strength=strength,
                                approach_style=approach_style, atr_ratio=atr_ratio,
                                touch_start_time=touch_start_time,
                            monitoring_start_time=_monitoring_start_time,
                                vol_ratio_at_touch=vol_ratio_captured,
                                outcome_saved_flag=_outcome_saved,
                                trade_activity=_trade_activity,
                                level_side=level_side,
                                ml_delta=ml_delta, p_fast_breakout=p_fast_breakout,
                                p_bounce=p_bounce, expected_depth=expected_depth,
                            ))
                            classify_sent = True
                        # Check near_miss: came within 0.5% but never touched
                        elif not touched and min_price_during is not None and not classify_sent:
                            dist_pct = (level - min_price_during) / level * 100 if level > min_price_during else 0
                            if 0 < dist_pct <= 0.5:
                                asyncio.create_task(_classify_and_log_level_event(
                                    symbol, level, c1m, candles_15m.get(symbol, []),
                                    min_price_during, touch_c1m_idx,
                                    level_type=level_type, strength=strength,
                                    approach_style=approach_style, atr_ratio=atr_ratio,
                                    touch_start_time=touch_start_time,
                            monitoring_start_time=_monitoring_start_time,
                                    vol_ratio_at_touch=vol_ratio_captured,
                                    outcome_saved_flag=_outcome_saved,
                                    trade_activity=_trade_activity,
                                    level_side=level_side,
                                    ml_delta=ml_delta, p_fast_breakout=p_fast_breakout,
                                    p_bounce=p_bounce, expected_depth=expected_depth,
                                ))
                                classify_sent = True
                        rebound_sent = False
                        approach_warned = False
                        proximity_sent = False
                        touched = False
                        classify_sent = False  # reset for next touch
                        _outcome_saved[0] = False  # reset: next touch is a new event
                        engulf_sent = False
                        level_broken_sent = False
                        delta_signal_sent = False
                        stop_delta_tracking(symbol)
                    elif distance > atr * DISTANCE_PARTIAL_RESET_ATR_MULTIPLIER:
                        touched = False
                        rebound_sent = False
                        sweep_sent = False  # FIX BUG-C4: иначе повторный sweep игнорируется
                        classify_sent = False
                        proximity_sent = False  # сброс: при следующем подходе proximity выйдет снова
                        _outcome_saved[0] = False  # reset: next touch is a new event
                        touch_start_time = 0.0
                        vol_ratio_captured = 1.0
                        min_price_during = None
                        max_price_during = None

                avg_vol_20 = sum(c["volume"] for c in c1m[-20:]) / min(len(c1m), 20)
                if volume_spike_notified and avg_vol_20 > 0 and last["volume"] / avg_vol_20 < VOLUME_SPIKE_RESET_RATIO:
                    volume_spike_notified = False

                # Proximity: публикуем событие когда цена приближается к уровню на PROXIMITY_ALERT_DISTANCE_PCT,
                # ещё до касания — чтобы S2 успел выставить сетку заблаговременно.
                # P6: если фильтр уже пометил уровень мёртвым (approach>=S2_APPROACH_BLOCK),
                # событие не публикуем — иначе цикл approach<->reset гонит proximity
                # каждые 5 сек и впустую дёргает filter.check() (лог-спам, лишний CPU).
                if not touched and not proximity_sent and atr > 0:
                    _dist_pct = abs(current_price - level) / level
                    if _dist_pct <= PROXIMITY_ALERT_DISTANCE_PCT:
                        proximity_sent = True
                        if state_manager.get_state(symbol).is_level_dead(_p6_task_key):
                            logger.debug("monitor: proximity suppressed (level dead, G3)",
                                         symbol=symbol, level=level)
                        else:
                            try:
                                from trading.event_bus import publish as _eb_publish
                                await _eb_publish(_make_event("proximity", current_price))
                            except Exception as _eb_e:
                                logger.debug("event_bus publish error (proximity): %s", _eb_e)

                is_sweep = _check_sweep_reclaim(c1m, level, level_side)
                if is_sweep and not sweep_sent:
                    await _handle_sweep(symbol, level, level_side, c1m)
                    sweep_sent = True
                    # Don't reset sweep_sent - it should only be sent once per monitoring session
                    try:
                        from trading.event_bus import publish as _eb_publish
                        _reclaim_vol = c1m[-1]["volume"]
                        _avg_vol_sw = sum(c["volume"] for c in c1m[-20:]) / max(len(c1m[-20:]), 1)
                        _sweep_vr = round(_reclaim_vol / _avg_vol_sw, 2) if _avg_vol_sw > 0 else 1.0
                        await _eb_publish(_make_event(
                            "sweep", c1m[-1]["close"],
                            sweep_vol_ratio=_sweep_vr,
                        ))
                    except Exception as _eb_e:
                        logger.debug("event_bus publish error (sweep): %s", _eb_e)

            alert, alert_type = _check_complications(symbol, level, level_side, approach_warned, volume_spike_notified, engulf_sent, level_broken_sent, weak_breakout_sent)
            if alert:
                # Set flag BEFORE sending message to prevent race condition
                if alert_type == "pressure":
                    approach_warned = True
                elif alert_type == "volume_spike":
                    volume_spike_notified = True
                elif alert_type == "engulf":
                    engulf_sent = True
                elif alert_type == "level_broken":
                    level_broken_sent = True
                
                # Now send the message
                await send_message(alert)

                # Publish to event bus
                if alert_type == "pressure":
                    try:
                        from trading.event_bus import publish as _eb_publish
                        _cp_alert = candles_1m.get(symbol, [{}])[-1].get("close", 0.0)
                        await _eb_publish(_make_event("pressure", _cp_alert))
                    except Exception as _eb_e:
                        logger.debug("event_bus publish error (pressure): %s", _eb_e)
                elif alert_type == "volume_spike":
                    try:
                        from trading.event_bus import publish as _eb_publish
                        _cp_vs = candles_1m.get(symbol, [{}])[-1].get("close", 0.0)
                        _c1m_vs = candles_1m.get(symbol, [])
                        _avg_vs = sum(c["volume"] for c in _c1m_vs[-60:]) / max(len(_c1m_vs[-60:]), 1) if _c1m_vs else 1
                        _spike_r = int(_c1m_vs[-1]["volume"] / _avg_vs) if _c1m_vs and _avg_vs > 0 else 1
                        await _eb_publish(_make_event("volume_spike", _cp_vs, spike_ratio=_spike_r))
                    except Exception as _eb_e:
                        logger.debug("event_bus publish error (volume_spike): %s", _eb_e)

            await asyncio.sleep(COLLECTOR_UPDATE_INTERVAL_SECONDS)

    finally:
        # FIX BUG-11: cleanup выполняется всегда — при break, stop_event и исключении
        stop_delta_tracking(symbol)
        if delta_stream_task and not delta_stream_task.done():
            delta_stream_task.cancel()

    return _monitor_result


# Dedup guard: prevent multiple classify calls for the same touch event
_classify_last_sent: dict[str, float] = {}  # key: "symbol:level" -> timestamp


def _get_market_context(symbol: str, touch_start_time: float, monitoring_start_time: float = 0.0) -> dict:
    """
    Вычислить btc_change_1m и monitoring_age_minutes в момент сохранения.
    funding_rate — async, получается отдельно через _get_market_context_async.
    Вызывается из _log_bounce_outcome и _classify_and_log_level_event.

    monitoring_age_minutes = время от старта монитора до касания уровня, в СЕКУНДАХ
    (имя поля сохранено как есть из-за схемы БД; MEDIUM-5: единицы унифицированы
    с main.py:_monitored (duration, секунды) и ml_score.py (делит /60 при инференсе)).
    touch_start_time — момент касания, monitoring_start_time — старт монитора.
    """
    from analysis.trigger import get_btc_change_1m as _get_btc
    btc_change_1m = None
    try:
        btc_change_1m = _get_btc()
    except Exception:
        pass

    monitoring_age_minutes = None
    if monitoring_start_time > 0 and touch_start_time > 0:
        # MEDIUM-5: store seconds (was minutes via /60) — unify units across
        # all writers of this field; ml_score.py and train_ml.py both expect seconds.
        monitoring_age_minutes = round(touch_start_time - monitoring_start_time, 2)
        monitoring_age_minutes = max(0.0, monitoring_age_minutes)

    return {
        "btc_change_1m": btc_change_1m,
        "monitoring_age_minutes": monitoring_age_minutes,
    }


async def _get_funding_rate(symbol: str) -> float | None:
    """Получить funding_rate через тот же путь что main.py."""
    from analysis.trigger import get_funding_rate as _get_fr
    try:
        return await _get_fr(symbol)
    except Exception:
        return None


def _real_touches(symbol: str, level: float) -> int:
    """Реальное число касаний уровня на момент записи исхода (для history.db).

    Раньше в save_level_outcome жёстко писалась 1 → повторные заходы в обучающих
    данных выглядели как первое касание и ломали калибровку (часть «неудачных
    отбоёв» на деле были предсказуемо плохими повторами touches>=2). Считаем
    честно тем же методом, что и фильтр входа — _count_approaches от пика пампа.
    Best-effort: при любой ошибке/нехватке данных возвращаем 1 (как было)."""
    try:
        from analysis.trigger import _count_approaches, _origin_anchor
        _atr = calc_atr(candles_1m.get(symbol, []))
        if _atr and _atr > 0:
            # Вариант A: тот же стабильный origin-якорь, что и фильтр входа —
            # чтобы записанный touches совпадал с approach_count_at_entry.
            return _count_approaches(symbol, level, _atr,
                                     anchor_time=_origin_anchor(symbol, level))[0]
    except Exception:
        pass
    return 1


async def _log_bounce_outcome(
    symbol: str,
    level: float,
    min_price: float,
    level_type: str = "body_level",
    strength: int = 0,
    approach_style: str = None,
    atr_ratio: float = None,
    touch_start_time: float = 0.0,
    monitoring_start_time: float = 0.0,
    vol_ratio_at_touch: float = 1.0,
    outcome_saved_flag: list = None,
    trade_activity: dict = None,
    level_side: str = "support",
    ml_delta: int = 0,
    p_fast_breakout: float = None,
    p_bounce: float = 0.0,
    expected_depth: float = 0.0,
):
    """Log a confirmed bounce directly to level_outcomes (used when classify already ran)."""
    if outcome_saved_flag is not None and outcome_saved_flag[0]:
        logger.debug("outcome already saved for this touch, skipping duplicate write")
        return
    import time as _time
    from data.history import log_event, save_level_outcome

    fill_depth_pct = (level - min_price) / level * 100 if min_price < level else 0.0
    now = _time.time()
    duration_minutes = round((now - touch_start_time), 1) if touch_start_time > 0 else 0.0  # BUG-4 fix: store seconds as REAL, not int minutes

    ctx = _get_market_context(symbol, touch_start_time, monitoring_start_time)
    funding = await _get_funding_rate(symbol)
    if fill_depth_pct >= 1.0:
        event_type = "zakol_deep"
    elif fill_depth_pct >= 0.1:
        event_type = "zakol"
    else:
        event_type = "bounce"
    await log_event(symbol, event_type,
                    f"level={level} depth={fill_depth_pct:.2f}% (late confirm)")
    if fill_depth_pct >= 2.0:
        outcome = "partial_deep"
    elif fill_depth_pct >= 1.0:
        outcome = "partial_mid"
    elif fill_depth_pct >= 0.1:
        outcome = "partial_shallow"
    else:
        outcome = "bounce"
    ta = trade_activity or {}
    await save_level_outcome(
        symbol=symbol,
        level=level,
        level_type=level_type,
        strength=strength,
        approach_type=level_side,
        vol_ratio=vol_ratio_at_touch,
        touches=_real_touches(symbol, level),
        result="отбой",
        duration=duration_minutes,
        outcome=outcome,
        approach_style=approach_style,
        vol_ratio_at_touch=vol_ratio_at_touch,
        atr_ratio=atr_ratio,
        fill_depth_pct=round(fill_depth_pct, 4),
        btc_change_1m=ctx["btc_change_1m"],
        funding_rate=funding,
        monitoring_age_minutes=ctx["monitoring_age_minutes"],
        trades_per_min_1m=ta.get("trades_per_min_1m"),
        trades_per_min_5m=ta.get("trades_per_min_5m"),
        trades_per_min_15m=ta.get("trades_per_min_15m"),
        trades_increasing=ta.get("trades_increasing"),
        ml_delta=ml_delta,
        p_fast_breakout=p_fast_breakout,
        p_bounce_at_entry=p_bounce,
        expected_depth_at_entry=expected_depth,
    )
    if outcome_saved_flag is not None:
        outcome_saved_flag[0] = True


async def _classify_and_log_level_event(
    symbol: str,
    level: float,
    c1m: list[dict],
    c15m: list[dict],
    min_price: float,
    touch_time_idx: int,  # index in c1m when touch happened
    level_type: str = "body_level",
    strength: int = 0,
    approach_style: str = None,
    atr_ratio: float = None,
    touch_start_time: float = 0.0,
    monitoring_start_time: float = 0.0,
    vol_ratio_at_touch: float = 1.0,
    outcome_saved_flag: list = None,
    trade_activity: dict = None,
    level_side: str = "support",
    ml_delta: int = 0,
    p_fast_breakout: float = None,
    p_bounce: float = 0.0,
    expected_depth: float = 0.0,
):
    """
    Classify what happened at the level and log to history + send message.

    Categories:
    - near_miss: price came within 0.5% but didn't touch
    - bounce: touched and returned above on 1M
    - zakol: pierced but returned above within 1M
    - zakol_deep: pierced >1%, check retest within 5 x 1M
    - breakout: 15M candle closed below OR price moved to next level
    """
    import time as _time
    dedup_key = f"{symbol}:{level}"
    now = _time.time()
    if now - _classify_last_sent.get(dedup_key, 0) < 300:  # 300s cooldown per level (BUG-1 fix: was 60s, caused ~47 writes/session)
        return
    _classify_last_sent[dedup_key] = now
    if outcome_saved_flag is not None and outcome_saved_flag[0]:
        logger.debug("outcome already saved for this touch, skipping duplicate write")
        return
    from data.history import log_event, save_level_outcome

    if not c1m or level == 0:
        return

    current_price = c1m[-1]["close"]
    fill_depth_pct = (level - min_price) / level * 100 if min_price < level else 0.0

    duration_minutes = round((now - touch_start_time), 1) if touch_start_time > 0 else 0.0  # BUG-4 fix: seconds as REAL

    # Get candles after touch
    post_touch = c1m[touch_time_idx:touch_time_idx + 20] if touch_time_idx < len(c1m) else []

    # --- NEAR MISS ---
    if fill_depth_pct < 0.1:  # didn't actually touch
        dist_pct = (current_price - level) / level * 100
        if 0 < dist_pct <= 0.5:
            details = f"level={level} min_price={min_price:.6f} dist={dist_pct:.2f}%"
            await log_event(symbol, "near_miss", details)
        return

    # --- ZAKOL or BOUNCE ---
    returned_above = any(c["close"] > level for c in post_touch[:5])

    outcome = None
    event_type = None
    details = None

    if returned_above:
        # Price pierced level but came back — zakol
        if fill_depth_pct >= 1.0:
            retest_candles = post_touch[1:6]
            retest = any(
                abs(c["low"] - level) / level * 100 <= 0.3
                for c in retest_candles
            )
            if retest:
                event_type = "zakol_deep_retest"
                details = f"level={level} depth={fill_depth_pct:.2f}% retest=yes"
            else:
                event_type = "zakol_deep"
                details = f"level={level} depth={fill_depth_pct:.2f}% retest=no"
        else:
            event_type = "zakol"
            details = f"level={level} depth={fill_depth_pct:.2f}%"
        # outcome по глубине — одинаковая логика с _make_result
        if fill_depth_pct >= 2.0:
            outcome = "partial_deep"
        elif fill_depth_pct >= 1.0:
            outcome = "partial_mid"
        elif fill_depth_pct >= 0.1:
            outcome = "partial_shallow"
        else:
            outcome = "bounce"
    else:
        # Price went below level and didn't return in 5 candles — not a bounce
        # Детализируем partial по глубине, как в _make_result, чтобы outcome
        # был консистентен независимо от пути сохранения.
        event_type = "no_return"
        if fill_depth_pct >= 2.0:
            outcome = "partial_deep"
        elif fill_depth_pct >= 1.0:
            outcome = "partial_mid"
        elif fill_depth_pct >= 0.1:
            outcome = "partial_shallow"
        else:
            outcome = "partial_shallow"
        details = f"level={level} fill_depth={fill_depth_pct:.2f}%"

    ctx = _get_market_context(symbol, touch_start_time, monitoring_start_time)
    funding = await _get_funding_rate(symbol)
    await log_event(symbol, event_type, details)

    # Also write to level_outcomes so ML has the data
    ta = trade_activity or {}
    await save_level_outcome(
        symbol=symbol,
        level=level,
        level_type=level_type,
        strength=strength,
        approach_type=level_side,
        vol_ratio=vol_ratio_at_touch,
        touches=_real_touches(symbol, level),
        result="отбой",
        duration=duration_minutes,
        outcome=outcome,
        approach_style=approach_style,
        vol_ratio_at_touch=vol_ratio_at_touch,
        atr_ratio=atr_ratio,
        fill_depth_pct=round(fill_depth_pct, 4),
        btc_change_1m=ctx["btc_change_1m"],
        funding_rate=funding,
        monitoring_age_minutes=ctx["monitoring_age_minutes"],
        trades_per_min_1m=ta.get("trades_per_min_1m"),
        trades_per_min_5m=ta.get("trades_per_min_5m"),
        trades_per_min_15m=ta.get("trades_per_min_15m"),
        trades_increasing=ta.get("trades_increasing"),
        ml_delta=ml_delta,
        p_fast_breakout=p_fast_breakout,
        p_bounce_at_entry=p_bounce,
        expected_depth_at_entry=expected_depth,
    )
    if outcome_saved_flag is not None:
        outcome_saved_flag[0] = True


def _check_complications(symbol: str, level: float, level_side: str, approach_warned: bool = False, volume_spike_notified: bool = False, engulf_sent: bool = False, level_broken_sent: bool = False, weak_breakout_active: bool = False) -> tuple[str | None, str | None]:
    """Check for various complication patterns during monitoring."""
    c1m = candles_1m.get(symbol, [])
    c15m = candles_15m.get(symbol, [])

    if len(c1m) < 10 or len(c15m) < 2:
        return None, None

    if not approach_warned and not weak_breakout_active:
        # Evict stale entries to prevent unbounded memory growth (BUG-19)
        _evict_stale_pressure_alerts()
        pressure_key = f"{symbol}_{level}"
        now = time.time()
        last_sent = _pressure_alert_sent.get(pressure_key, 0)

        if (now - last_sent) > PRESSURE_ALERT_COOLDOWN:
            vol_trend = _check_volume_trend_approach(symbol, level, level_side)
            if vol_trend:
                _pressure_alert_sent[pressure_key] = now
                return vol_trend, "pressure"

    if not level_broken_sent and not weak_breakout_active:
        broken = _check_level_broken(c1m, level)
        if broken:
            key = f"{symbol}_{level}"
            now = time.time()
            if now - _level_broken_sent.get(key, 0) > LEVEL_BROKEN_ALERT_COOLDOWN:
                _level_broken_sent[key] = now

    # FIX BUG-M2: _check_volume_spike определена, но никогда не вызывалась
    if not volume_spike_notified:
        spike_ratio = _check_volume_spike(c1m)
        if spike_ratio is not None:
            key = f"{symbol}_{level}"
            now = time.time()
            if now - _volume_spike_sent.get(key, 0) > VOLUME_SPIKE_ALERT_COOLDOWN:
                _volume_spike_sent[key] = now

    return None, None


def _check_volume_spike(c1m: list[dict]) -> int | None:
    if len(c1m) < 10:
        return None

    current = c1m[-1]
    if current["close"] >= current["open"]:
        return None

    avg_vol = sum(c["volume"] for c in c1m[-60:]) / len(c1m[-60:]) if len(c1m) >= 60 else sum(c["volume"] for c in c1m) / len(c1m)

    if avg_vol == 0:
        return None

    ratio = current["volume"] / avg_vol
    if ratio >= VOLUME_SPIKE_RATIO:
        return int(ratio)
    return None


def _check_engulfing(c15m: list[dict]) -> bool:
    if len(c15m) < 2:
        return False

    prev = c15m[-2]
    curr = c15m[-1]

    prev_is_green = prev["close"] > prev["open"]
    curr_is_red = curr["close"] < curr["open"]

    if not prev_is_green or not curr_is_red:
        return False

    return curr["open"] >= prev["close"] and curr["close"] <= prev["open"]


def _check_level_broken(c1m: list[dict], level: float) -> bool:
    """Check if level was broken by multiple candles."""
    recent = c1m[-LEVEL_BROKEN_MIN_CANDLES:]
    if not all(c["close"] < level for c in recent):
        return False
    avg_vol = sum(c["volume"] for c in c1m[-20:]) / min(len(c1m), 20)
    return recent[-1]["volume"] > avg_vol


def _check_sweep_reclaim(c1m: list[dict], level: float, level_side: str = "support") -> bool:
    if len(c1m) < 3:
        return False

    prev = c1m[-2]
    curr = c1m[-1]

    if level_side == "support":
        swept = prev["low"] < level and prev["close"] < level
        reclaimed = curr["close"] > level and curr["volume"] > prev["volume"]
    else:
        swept = prev["high"] > level and prev["close"] > level
        reclaimed = curr["close"] < level and curr["volume"] > prev["volume"]

    return swept and reclaimed


def _check_volume_trend_approach(symbol: str, level: float, level_side: str = "support") -> str | None:
    """Check for growing volume pressure as price approaches level."""
    c1m = candles_1m.get(symbol, [])
    c15m = candles_15m.get(symbol, [])
    if len(c1m) < 5 or not c15m:
        return None

    current_price = c1m[-1]["close"]
    if level == 0:
        return None
    distance_pct = abs(current_price - level) / level * 100
    if not (PRESSURE_ZONE_MIN_DISTANCE_PCT * 100 <= distance_pct <= PRESSURE_ZONE_MAX_DISTANCE_PCT * 100):
        return None

    recent = c1m[-5:]
    if level_side == "support":
        directional_candles = [c for c in recent if c["close"] < c["open"]]
    else:
        directional_candles = [c for c in recent if c["close"] > c["open"]]
    if len(directional_candles) < PRESSURE_MIN_DIRECTIONAL_CANDLES:
        return None

    volumes = [c["volume"] for c in directional_candles]

    # Check that volumes are strictly growing across all directional candles.
    # len(volumes) >= PRESSURE_MIN_DIRECTIONAL_CANDLES (3) is already guaranteed above.
    growing = all(volumes[i] < volumes[i + 1] for i in range(len(volumes) - 1))
    if not growing:
        return None

    avg_vol = sum(c["volume"] for c in c1m[-20:]) / min(len(c1m), 20)
    last_vol_ratio = round(volumes[-1] / avg_vol, 1) if avg_vol > 0 else 1.0
    
    # Only alert if volume ratio is significant (> 1.5x)
    if last_vol_ratio < 1.5:
        return None

    last_15m = c15m[-1]
    avg_15m = sum(c["volume"] for c in c15m[-20:]) / min(len(c15m), 20)
    if level_side == "support":
        confirmed_15m = last_15m["close"] < last_15m["open"] and last_15m["volume"] > avg_15m * PRESSURE_VOLUME_MIN_RATIO
    else:
        confirmed_15m = last_15m["close"] > last_15m["open"] and last_15m["volume"] > avg_15m * PRESSURE_VOLUME_MIN_RATIO

    if confirmed_15m:
        return (
            f"🔴 {symbol} продавец наращивает давление на подходе к {level}\n"
            f"   Объём 1М растёт ×{last_vol_ratio}, подтверждено 15М\n"
            f"   → вход рискованный, возможен пробой"
        )
    else:
        return None