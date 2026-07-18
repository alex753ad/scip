"""Main orchestrator for trading bot."""

import asyncio
import time
import json
import os
import subprocess
import sys
from logger import logger
from constants import (
    TRIGGER_COOLDOWN_SECONDS,
    SCREENER_DELAY_SECONDS,
    SCREENER_MIN_VOLUME_USD,
    SCREENER_MIN_GROWTH_PCT,
    SCREENER_MIN_NATR,
    SCREENER_AUTO_INTERVAL_SECONDS,
    CLAUDE_MAX_CONCURRENT_REQUESTS,
    COLLECTOR_UPDATE_INTERVAL_SECONDS,
    PROXIMITY_ALERT_DISTANCE_PCT,
    PROXIMITY_ALERT_COOLDOWN_SECONDS,
    PUMP_HEALTH_MIN_SCORE,
    PUMP_HEALTH_CAUTION_SCORE,
    MONITOR_HEALTH_INTERVAL_SECONDS,
    MONITOR_MIN_NATR_5M,
    MONITOR_MIN_1M_TRADES,
    MONITOR_NATR_FAIL_DURATION_SECONDS,
    MONITOR_TRADES_FAIL_DURATION_SECONDS,
)
from models import state_manager
from data.collector import start_collector, candles_1m
from analysis.trigger import (
    check_trigger, get_approaching_levels,
    detect_approach_style, calculate_atr_ratio, get_vol_ratio_current,
    get_btc_change_1m, get_funding_rate,
)
from utils import calculate_strength, calc_atr as _calc_atr_util
from analysis.monitor import start_monitor
from bot.telegram import send_message, start_bot, bot_ready
from config import token_registry, blacklist, validate_config, TRIGGER_TIMES_FILE, ACTIVE_MONITORS_FILE, BYBIT_API_KEY, BYBIT_API_SECRET
from data.history import init_db, save_level_outcome, update_symbol_profile, get_outcome_probs, log_event


# Global semaphore for Claude API rate limiting
claude_semaphore = asyncio.Semaphore(CLAUDE_MAX_CONCURRENT_REQUESTS)


def save_active_monitors():
    """Save currently active monitors and all known levels to file for restart recovery."""
    try:
        from bot.telegram import _last_analysis_cache
    except Exception:
        _last_analysis_cache = {}

    monitors = []
    for state in state_manager._states.values():
        sym = state.symbol
        c1m_data = candles_1m.get(sym, [])
        cur = c1m_data[-1]["close"] if c1m_data else 0

        active_levels = set()
        for task_key in state.tasks:
            parsed = state.parse_task_key(task_key)
            if parsed is not None:
                active_levels.add(parsed[1])

        cached_levels = _last_analysis_cache.get(sym, [])
        saved_any = False
        for lvl_entry in cached_levels:
            lvl = lvl_entry["level"]
            side = "resistance" if cur > 0 and lvl > cur else "support"
            monitors.append({
                "symbol":         sym,
                "level":          lvl,
                "level_side":     side,
                "active":         lvl in active_levels,
                "strength":       lvl_entry.get("strength", 0),
                "type":           lvl_entry.get("type", "body_level"),
                "p_bounce":       lvl_entry.get("p_bounce", 0.0),
                "expected_depth": lvl_entry.get("expected_depth", 0.0),
            })
            saved_any = True

        # Fallback: кэша нет — сохранить хотя бы активные мониторы
        if not saved_any:
            for task_key in state.tasks:
                parsed = state.parse_task_key(task_key)
                if parsed is not None:
                    lvl = parsed[1]
                    side = "resistance" if cur > 0 and lvl > cur else "support"
                    monitors.append({
                        "symbol":         sym,
                        "level":          lvl,
                        "level_side":     side,
                        "active":         True,
                        "strength":       state.level_strengths.get(task_key, 0),
                        "type":           "body_level",
                        "p_bounce":       0.0,
                        "expected_depth": 0.0,
                    })
    try:
        with open(ACTIVE_MONITORS_FILE, "w") as f:
            json.dump(monitors, f, indent=2)
    except Exception as e:
        logger.error("Failed to save active monitors", error=str(e))


def load_active_monitors() -> list[dict]:
    """Load saved monitors from file."""
    if os.path.exists(ACTIVE_MONITORS_FILE):
        try:
            with open(ACTIVE_MONITORS_FILE) as f:
                data = json.load(f)
                logger.info("Loaded active monitors", count=len(data))
                return data
        except Exception as e:
            logger.error("Failed to load active monitors", error=str(e))
    return []


def load_trigger_times() -> dict[str, float]:
    """Load trigger cooldown timestamps from file."""
    if os.path.exists(TRIGGER_TIMES_FILE):
        try:
            with open(TRIGGER_TIMES_FILE) as f:
                data = json.load(f)
                logger.info("Loaded trigger times", count=len(data))
                return data
        except Exception as e:
            logger.error("Failed to load trigger times", error=str(e))
            return {}
    return {}


def save_trigger_times(data: dict[str, float]):
    """Save trigger cooldown timestamps to file."""
    try:
        with open(TRIGGER_TIMES_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.exception("Failed to save trigger times", error=str(e))


from analysis.screener import run_screener as _run_screener, _format_vol


async def _send_startup_screener():
    """Send market screener on bot startup."""
    await asyncio.sleep(SCREENER_DELAY_SECONDS)
    try:
        from datetime import datetime, timezone
        rows = await _run_screener()
        if not rows:
            logger.info("No symbols passed screener filter")
            return

        now_str = datetime.now(timezone.utc).strftime("%d.%m %H:%M")
        lines = [f"📊 Рынок  {now_str} UTC\n"]
        lines.append(f"{'TICKER':<10} {'CHG%':>6}  {'NATR':>4}  {'VOL':>6}")
        lines.append("─" * 34)
        for ticker, chg, natr, vol, _ in rows:
            lines.append(f"{ticker:<10} {chg:>+5.1f}  {natr:>4.1f}  {_format_vol(vol):>6}")

        await send_message("```\n" + "\n".join(lines) + "\n```")
        logger.info("Startup screener sent", symbols_count=len(rows))
    except Exception as e:
        logger.exception("Error in startup screener", error=str(e))


async def _auto_screener_loop():
    """Every 30 minutes scan market, auto-add new symbols, build levels and start monitoring."""
    from datetime import datetime, timezone
    await asyncio.sleep(SCREENER_AUTO_INTERVAL_SECONDS)  # skip first run

    while True:
        known_symbols = set(token_registry.get_all())
        try:
            rows = await _run_screener()
            if not rows:
                await asyncio.sleep(SCREENER_AUTO_INTERVAL_SECONDS)
                continue

            now_str = datetime.now(timezone.utc).strftime("%d.%m %H:%M")
            new_symbols = []

            for ticker, chg, natr, vol, sym in rows:
                if blacklist.contains(sym):
                    continue
                if sym not in known_symbols:
                    token_registry.add(sym)
                    known_symbols.add(sym)
                    new_symbols.append((ticker, chg, natr, vol, sym))
                    logger.info("Auto-added new symbol from screener", symbol=sym)

            # For each new symbol: fetch data, build levels, start monitoring
            if new_symbols:
                from binance import AsyncClient
                from data.collector import _parse_kline, candles_1m, candles_15m
                from analysis.level_builder import build_levels
                from analysis.trigger import calculate_atr, get_level_history, _count_approaches
                from analysis.claude_strength import calculate_strength_with_claude
                import json as _json

                client = None
                try:
                    client = await AsyncClient.create()
                    for ticker, chg, natr, vol, sym in new_symbols:
                        try:
                            raw_15m = await client.futures_klines(symbol=sym, interval="15m", limit=500)
                            raw_1m = await client.futures_klines(symbol=sym, interval="1m", limit=300)
                            candles_15m[sym] = [_parse_kline(k) for k in raw_15m]
                            candles_1m[sym] = [_parse_kline(k) for k in raw_1m]

                            ext_c1m = candles_1m[sym]
                            ext_c15m = candles_15m[sym]
                            all_levels = build_levels(sym, c1m_override=ext_c1m, c15m_override=ext_c15m)

                            await log_event(sym, "added_screener", f"chg={chg:+.1f}% natr={natr:.1f}%")

                            if not all_levels:
                                await send_message(f"🆕 {sym} добавлен | {chg:+.1f}% | NATR {natr:.1f}%\n   Уровни не найдены, мониторинг не запущен")
                                continue

                            current_price = ext_c1m[-1]["close"]
                            atr = calculate_atr(sym)
                            range_limit = current_price * 0.20

                            supports = [
                                lvl for lvl in all_levels
                                if lvl["level"] < current_price
                                and (current_price - lvl["level"]) <= range_limit
                                and (current_price - lvl["level"]) >= atr * 1.5
                            ]

                            if not supports:
                                await send_message(f"🆕 {sym} добавлен | {chg:+.1f}% | NATR {natr:.1f}%\n   Нет уровней в диапазоне 20%, мониторинг не запущен")
                                continue

                            for lvl in supports:
                                lvl["symbol"] = sym
                                lvl["approach"] = _count_approaches(sym, lvl["level"], atr)[0] if atr > 0 else 0  # FIX BUG-6: tuple[0]=count
                                if atr > 0:
                                    lvl.update(get_level_history(sym, lvl["level"], atr))
                                calculate_strength(lvl)
                                lvl["python_strength"] = lvl["strength"]

                            poc_price = next((l["level"] for l in supports if l.get("poc_aligned")), None)
                            supports = await calculate_strength_with_claude(sym, ext_c15m, supports, poc_price)

                            for lvl in supports:
                                py = lvl.get("python_strength", lvl["strength"])
                                if lvl.get("approach", 0) >= 2 or (lvl.get("was_broken") and not lvl.get("sweep_reclaimed")):
                                    lvl["strength"] = min(lvl["strength"], py)

                            try:  # FIX BUG-15: убран мёртвый if-блок (strength==0 после calculate_strength невозможен)
                                from analysis.ml_score import apply_ml_to_level
                                _style_screener = detect_approach_style(sym)
                                for lvl in supports:
                                    lvl["approach_style"] = _style_screener
                                    lvl["monitoring_age_minutes"] = 0.0  # FIX BUG-M8: новый символ, age = 0
                                    apply_ml_to_level(lvl)
                            except Exception as _e:
                                logger.warning("ml_score failed in screener: %s", _e)

                            strong = [l for l in supports if l["strength"] >= 3]
                            if not strong:
                                await send_message(f"🆕 {sym} добавлен | {chg:+.1f}% | NATR {natr:.1f}%\n   Нет уровней с силой >= 3, мониторинг не запущен")
                                continue

                            # Monitor only the nearest strong level
                            nearest = min(strong, key=lambda l: abs(current_price - l["level"]))
                            sym_state = state_manager.get_state(sym)
                            task_key = sym_state.make_task_key(nearest["level"])
                            if task_key not in sym_state.tasks:
                                task = asyncio.create_task(
                                    _monitored(sym, nearest["level"], "support",
                                              level_type=nearest["type"],
                                              strength=nearest["strength"],
                                              p_bounce=nearest.get("p_bounce", 0.0),
                                              expected_depth=nearest.get("expected_depth", 0.0),
                                              approach=nearest.get("approach", 0))
                                )
                                sym_state.add_task(nearest["level"], task, strength=nearest.get("strength", 0))
                                sym_state.phase = "phase2"

                                stars = "⭐️" * nearest["strength"]
                                reason = nearest.get("claude_reason", "")
                                msg = (f"🆕 {sym} добавлен | {chg:+.1f}% | NATR {natr:.1f}%\n"
                                       f"   {stars} {nearest['level']} — {nearest['type']}\n")
                                if reason:
                                    msg += f"   💭 {reason}\n"
                                msg += "👁 Мониторинг запущен"
                                await send_message(msg)

                                levels_info = [{"level": l["level"], "type": l["type"], "strength": l["strength"]} for l in strong]
                                await log_event(sym, "levels_built", _json.dumps(levels_info))
                                await log_event(sym, "monitoring_start",
                                               f"level={nearest['level']} strength={nearest['strength']} type={nearest['type']}")
                                logger.info("Auto monitoring started", symbol=sym, level=nearest["level"])

                        except Exception as e:
                            logger.exception("Error setting up new symbol", symbol=sym, error=str(e))
                finally:
                    if client is not None:
                        await client.close_connection()

            logger.info("Auto screener completed", total=len(rows), new=len(new_symbols))

        except Exception as e:
            logger.exception("Error in auto screener loop", error=str(e))

        await asyncio.sleep(SCREENER_AUTO_INTERVAL_SECONDS)



# Global set of symbols currently being processed in phase1
_building_levels: set[str] = set()

# Cooldown for "proximity" events published to the strategy event_bus (S1/S2/S3).
# Independent from PROXIMITY_ALERT_COOLDOWN_SECONDS (Telegram alert, ~once per session) —
# strategies need repeated proximity events as price keeps approaching the level.
PROXIMITY_EVENT_COOLDOWN_SECONDS = 15
_proximity_event_sent: dict[str, float] = {}  # key: task_key -> timestamp of last event_bus publish

# Stores the level that was replaced by a closer one during _run_phase1.
# After breakout of the new level, this level becomes the next monitoring target.
_previous_levels: dict[str, float] = {}


async def _trigger_loop():
    """Main loop checking for correction triggers."""
    trigger_times = load_trigger_times()

    while True:
        try:
            tokens = token_registry.get_all()
            for symbol in tokens:
                state = state_manager.get_state(symbol)

                # Skip if already building levels
                if state.phase == "phase1" or symbol in _building_levels:
                    continue

                # Skip blacklisted symbols
                if blacklist.contains(symbol):
                    continue

                # Check cooldown
                last = trigger_times.get(symbol, 0)
                if time.time() - last < TRIGGER_COOLDOWN_SECONDS:
                    continue

                # Check trigger condition
                if check_trigger(symbol):
                    _building_levels.add(symbol)
                    trigger_times[symbol] = time.time()
                    save_trigger_times(trigger_times)
                    logger.info("Trigger activated", symbol=symbol)
                    await _run_phase1(symbol)
                    
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("Error in trigger loop", error=str(e))
            try:
                await send_message("⚠️ Ошибка в trigger loop, см. логи")
            except Exception:
                pass
        
        await asyncio.sleep(COLLECTOR_UPDATE_INTERVAL_SECONDS)



async def _run_phase1(symbol: str):
    """Phase 1: Build levels, start monitoring. Claude NOT used here - only on manual analyze/check."""
    state = state_manager.get_state(symbol)
    was_in_phase2 = state.phase == "phase2"
    # Don't change phase to "phase1" if already monitoring — avoids race condition
    # where a finishing monitor resets phase to "idle" mid-build
    if not was_in_phase2:
        state.phase = "phase1"

    _building_levels.add(symbol)
    try:
        # FIX BUG-M6: сброс кэша в начале phase1 — чтобы при исключении
        # _proximity_loop не итерировал по устаревшим уровням
        from bot.telegram import _last_analysis_cache
        _last_analysis_cache.pop(symbol, None)

        from analysis.level_builder import build_levels
        from analysis.trigger import calculate_atr, get_level_history, _count_approaches
        from analysis.pump_phase import detect_pump_peak, pump_health_score, get_pump_phase, calc_correction_pct

        c1m = candles_1m.get(symbol, [])
        current_price = c1m[-1]["close"] if c1m else 0
        if not current_price:
            state.phase = "phase2" if state.has_active_tasks() else "idle"
            return

        # ── Pump Phase: record pump data and check health ─────────────
        pump_high, pump_base, pump_high_time = detect_pump_peak(symbol)
        if pump_high > 0:
            state.pump_high = pump_high
            state.pump_base_price = pump_base
            state.pump_high_time = pump_high_time
            # New trigger = new pump → reset broken-levels counter
            state.broken_since_pump = 0

        health = pump_health_score(state, current_price)
        state.pump_health = health
        state.pump_phase = get_pump_phase(health)

        if health < PUMP_HEALTH_MIN_SCORE:
            corr = calc_correction_pct(state)
            logger.info("Pump degraded, skipping monitoring",
                        symbol=symbol, health=health,
                        broken=state.broken_since_pump, correction=f"{corr:.0%}")
            await send_message(
                f"⚠️ {symbol} памп деградировал (score={health}/100)\n"
                f"   Пробито уровней: {state.broken_since_pump} | "
                f"Коррекция: {corr:.0%}\n"
                f"   Мониторинг пропущен."
            )
            state.phase = "phase2" if state.has_active_tasks() else "idle"
            return

        # In caution mode only monitor strength >= 4 levels
        min_strength = 4 if health < PUMP_HEALTH_CAUTION_SCORE else 3
        # ─────────────────────────────────────────────────────────────

        atr = calculate_atr(symbol)
        range_limit = current_price * 0.20

        all_levels = build_levels(symbol)
        levels = [
            lvl for lvl in all_levels
            if lvl["level"] < current_price
            and (current_price - lvl["level"]) <= range_limit
            and (atr == 0 or (current_price - lvl["level"]) >= atr * 1.5)
        ]

        for lvl in levels:
            lvl["symbol"] = symbol
            lvl["level_side"] = "support"
            lvl["approach"] = _count_approaches(symbol, lvl["level"], atr)[0] if atr > 0 else 0  # FIX BUG-6: tuple[0]=count
            if atr > 0:
                lvl.update(get_level_history(symbol, lvl["level"], atr))
            calculate_strength(lvl)

        try:  # FIX BUG-15: убран мёртвый if-блок (strength==0 после calculate_strength невозможен)
            from analysis.ml_score import apply_ml_to_level
            _style_phase = detect_approach_style(symbol)
            for lvl in levels:
                lvl["approach_style"] = _style_phase
                apply_ml_to_level(lvl)
        except Exception as _e:
            logger.warning("ml_score failed in phase loop: %s", _e)

        # --- Stop weak monitors (strength < 3) — всегда, не только в phase2 ---
        if current_price > 0 and state.tasks:
            weak_levels = []
            for task_key in list(state.tasks.keys()):
                task_strength = state.level_strengths.get(task_key, 3)
                if task_strength < 3:
                    stop_ev = state.stop_flags.get(task_key)
                    if stop_ev:
                        stop_ev.set()
                    task = state.tasks.get(task_key)
                    if task:
                        task.cancel()
                    parsed_wk = state.parse_task_key(task_key)
                    state.remove_task(task_key)
                    if parsed_wk:
                        weak_levels.append(parsed_wk[1])
                    logger.info("Weak monitor stopped (strength < 3)",
                               symbol=symbol, task_key=task_key, strength=task_strength)
            if weak_levels:
                levels_str = ", ".join(str(l) for l in sorted(weak_levels))
                # Уведомление в Telegram отключено.
                logger.debug("weak monitors stopped (telegram disabled)",
                             symbol=symbol, levels=levels_str)

        # --- Stop stale monitors (levels now outside -20% range) ---
        if was_in_phase2 and current_price > 0:
            range_low = current_price * 0.80
            stale_levels = []
            for task_key in list(state.tasks.keys()):
                parsed = state.parse_task_key(task_key)
                if parsed is None:
                    continue
                monitored_level = parsed[1]
                if monitored_level < range_low:
                    stop_ev = state.stop_flags.get(task_key)
                    if stop_ev:
                        stop_ev.set()
                    task = state.tasks.get(task_key)
                    if task:
                        task.cancel()
                    state.remove_task(task_key)
                    stale_levels.append(monitored_level)
                    logger.info("Stale monitor stopped (out of range)",
                               symbol=symbol, level=monitored_level,
                               current_price=current_price)
            if stale_levels:
                levels_str = ", ".join(str(l) for l in sorted(stale_levels))
                # Уведомление в Telegram отключено.
                logger.debug("stale monitors stopped (telegram disabled)",
                             symbol=symbol, levels=levels_str)
            # Clear analyzed cache so new pump levels aren't blocked by old entries
            state.clear_analyzed_levels()

        if not levels:
            logger.debug("No approaching levels found", symbol=symbol)
            state.phase = "phase2" if state.has_active_tasks() else "idle"
            return

        # Filter out already analyzed levels
        new_levels = [
            lvl for lvl in levels
            if not state.is_level_analyzed(lvl["level"])
        ]

        if not new_levels:
            logger.debug("All levels already analyzed", symbol=symbol)
            state.phase = "phase2" if state.has_active_tasks() else "idle"
            return

        # Filter by strength >= min_strength (3 normally, 4 in caution mode)
        strong = [lvl for lvl in new_levels if lvl["strength"] >= min_strength]

        # Mark all as analyzed
        for lvl in new_levels:
            state.mark_level_analyzed(lvl["level"])

        if not strong:
            logger.info("No strong levels found", symbol=symbol, total_levels=len(new_levels))
            state.phase = "phase2" if state.has_active_tasks() else "idle"
            return

        # Send notifications and start monitoring — only the NEAREST strong level
        # Others will be picked up after breakout via _start_next_level_after_breakout
        strong_sorted = sorted(strong, key=lambda l: abs(current_price - l["level"]))
        nearest = strong_sorted[0]

        level_side = nearest.get("level_side", "support")
        task_key = state.make_task_key(nearest["level"])

        # Check if there's already a monitored level
        current_monitored_level = None
        current_task_key = None
        for tk in list(state.tasks.keys()):
            parsed = state.parse_task_key(tk)
            if parsed is not None:
                current_monitored_level = parsed[1]
                current_task_key = tk
                break

        if current_task_key is not None:
            new_dist = abs(current_price - nearest["level"])
            old_dist = abs(current_price - current_monitored_level)
            if new_dist >= old_dist:
                # New level is not closer — keep old monitor
                logger.debug("Nearest level already monitored with closer level",
                             symbol=symbol, current=current_monitored_level)
                state.phase = "phase2"
                return
            # New level IS closer — cancel old, save it, start new
            _previous_levels[symbol] = current_monitored_level
            stop_ev = state.stop_flags.get(current_task_key)
            if stop_ev:
                stop_ev.set()
            old_task = state.tasks.get(current_task_key)
            if old_task:
                old_task.cancel()
            state.remove_task(current_task_key)
            logger.info("Replaced old monitor with closer level",
                        symbol=symbol, old=current_monitored_level, new=nearest["level"])
            await send_message(
                f"🔄 {symbol} найден уровень ближе к цене\n"
                f"   Старый: {current_monitored_level} → Новый: {nearest['level']}\n"
                f"   Старый будет следующим после пробоя"
            )
        elif task_key in state.tasks:
            state.phase = "phase2"
            return

        # Проверка: уровень не должен быть уже пробит к моменту запуска монитора.
        # Берём последние 30 свечей 1М и смотрим были ли закрытия ниже уровня.
        # Если цена уже ниже уровня ИЛИ уровень пробивался телом свечи >= 2 раз — пропускаем.
        _level_val = nearest["level"]
        _broken_before_start = False
        if current_price < _level_val:
            # Цена уже ниже уровня — мониторить бессмысленно
            _broken_before_start = True
            logger.info("Level already below current price, skipping monitor",
                        symbol=symbol, level=_level_val, current_price=current_price)
        else:
            _recent_1m = c1m[-30:] if len(c1m) >= 30 else c1m
            _close_below = sum(1 for c in _recent_1m if c["close"] < _level_val)
            if _close_below >= 2:
                _broken_before_start = True
                logger.info("Level broken in recent candles, skipping monitor",
                            symbol=symbol, level=_level_val, close_below=_close_below)

        if _broken_before_start:
            state.phase = "phase2" if state.has_active_tasks() else "idle"
            return

        zone_approaches = nearest.get("zone_approaches", 0)
        atr_pct = nearest.get("atr_pct", 0)
        stars = "⭐️" * nearest["strength"] if nearest["strength"] > 0 else "☆"
        dist_pct = (current_price - nearest["level"]) / current_price * 100

        # Пересчитываем ML с реальными значениями прямо перед отправкой
        try:  # FIX BUG-15: убран мёртвый if-блок (strength==0 после calculate_strength невозможен)
            real_atr_ratio = calculate_atr_ratio(symbol, nearest["level"])
            real_vol_ratio = get_vol_ratio_current(symbol)
            nearest["atr_ratio"] = real_atr_ratio
            nearest["vol_ratio"] = real_vol_ratio
            nearest["approach_style"] = detect_approach_style(symbol)
            nearest["monitoring_age_minutes"] = 0.0  # FIX BUG-M8: уровень только что найден
            from analysis.ml_score import apply_ml_to_level
            apply_ml_to_level(nearest)
        except Exception as _e:
            logger.warning("ml_score pre-message recalc failed: %s", _e)

        if was_in_phase2:
            text = (
                f"📋 {symbol} новый уровень после пампа\n"
                f"   {stars} {nearest['level']} — {nearest.get('type', '')} ({dist_pct:.1f}%)\n"
            )
        else:
            text = (
                f"📋 {symbol} коррекция началась\n"
                f"   {stars} {nearest['level']} — {nearest.get('type', '')} ({dist_pct:.1f}%)\n"
            )
        if zone_approaches >= 1:
            text += f"   ⚠️ Зона тестировалась {zone_approaches} раз(а)\n"
        p_b = nearest.get("p_bounce")
        e_d = nearest.get("expected_depth")
        if p_b is not None:
            depth_str = f" | прокол ~{e_d:.1f}%" if e_d is not None else ""
            text += f"   🤖 P(отбой): {p_b:.0%}{depth_str}\n"
        # 2.1: метка подтверждённого bounce при vol_ratio_at_touch 2–4x (90.9% bounce в истории)
        _vol_touch = nearest.get("vol_ratio_at_touch") or nearest.get("vol_ratio", 0)
        if _vol_touch and 2.0 <= _vol_touch <= 4.0:
            text += f"   🔒 Подтверждённый bounce (vol×{_vol_touch:.1f})\n"
        # Pump health line
        text += f"   💊 Pump health: {state.pump_health}/100 ({state.pump_phase})\n"
        text += f"\n   Жду цену на {nearest['level']}..."

        await send_message(text)
        logger.info("Level monitoring started",
                   symbol=symbol, level=nearest['level'], strength=nearest['strength'])

        task = asyncio.create_task(
            _monitored(symbol, nearest["level"], level_side,
                       level_type=nearest["type"],
                       strength=nearest["strength"],
                       p_bounce=nearest.get("p_bounce", 0.0),
                       expected_depth=nearest.get("expected_depth", 0.0),
                       approach=nearest.get("approach", 0))
        )
        state.add_task(nearest["level"], task, strength=nearest.get("strength", 0))
        state.phase = "phase2"

        # Update analysis cache so breakout chain can find next levels
        from bot.telegram import _last_analysis_cache
        _last_analysis_cache[symbol] = [
            {
                "level": l["level"],
                "strength": l["strength"],
                "type": l["type"],
                "p_bounce": l.get("p_bounce", 0.0),
                "expected_depth": l.get("expected_depth", 0.0),
            }
            for l in sorted(levels, key=lambda x: x["level"])
        ]

    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.exception("Error in phase1", symbol=symbol, error=str(e))
        state.phase = "phase2" if was_in_phase2 else "idle"
        try:
            await send_message(f"⚠️ Ошибка в phase1({symbol}), см. логи")
        except Exception:
            pass
    finally:
        _building_levels.discard(symbol)



async def _monitored(symbol: str, level: float, level_side: str,
                     level_type: str = "body_level",
                     strength: int = 0,
                     approach_style: str = None, atr_ratio: float = None,
                     vol_ratio: float = None,
                     p_bounce: float = 0.0,
                     expected_depth: float = 0.0,
                     approach: int = 0):
    """Monitor a level until breakout or manual stop."""
    state = state_manager.get_state(symbol)
    task_key = state.make_task_key(level)
    stop_event = state.stop_flags.get(task_key)

    start_time = time.time()
    reason = None  # ensure defined for finally block
    outcome = None
    fill_depth_pct = 0.0
    # Compute approach metrics now (at monitoring start) if not provided by caller
    if approach_style is None:
        approach_style = detect_approach_style(symbol)
    if atr_ratio is None:
        atr_ratio = calculate_atr_ratio(symbol, level)
    m_approach_style = approach_style
    m_atr_ratio = atr_ratio
    m_vol_ratio = vol_ratio

    # Save monitors state on start
    save_active_monitors()

    try:
        # on_bounce: при отбое от support немедленно стартуем resistance-монитор,
        # не дожидаясь завершения текущего монитора.
        _on_bounce = _start_resistance_after_bounce if level_side == "support" else None

        monitor_result = await start_monitor(
            symbol, level, level_side, stop_event,
            approach_style=approach_style,
            atr_ratio=atr_ratio,
            vol_ratio=vol_ratio,
            level_type=level_type,
            strength=strength,
            p_bounce=p_bounce,
            expected_depth=expected_depth,
            approach=approach,
            on_bounce=_on_bounce,
        )
        duration = round(time.time() - start_time, 1)  # BUG-4 fix: seconds as REAL, not int minutes

        if isinstance(monitor_result, dict):
            reason = monitor_result.get("reason")
            outcome = monitor_result.get("outcome")
            fill_depth_pct = monitor_result.get("fill_depth_pct", 0.0)
            m_approach_style = monitor_result.get("approach_style")
            m_atr_ratio = monitor_result.get("atr_ratio")
            m_vol_ratio = monitor_result.get("vol_ratio_at_touch")
            outcome_already_saved = monitor_result.get("outcome_saved", False)
        else:
            reason = monitor_result
            outcome = "breakout" if reason == "breakout" else None
            outcome_already_saved = False

        result = "пробой" if reason == "breakout" else "отбой"

        # Clear analyzed cache on breakout so level can be re-evaluated
        if reason == "breakout":
            state.analyzed_levels.discard(f"{symbol}:{level}")

        btc_change = get_btc_change_1m()
        funding = await get_funding_rate(symbol)

        try:
            from analysis.trigger import _calc_vol_ratio, _count_approaches, calculate_atr
            atr = calculate_atr(symbol)
            vol_ratio_old = _calc_vol_ratio(symbol)
            touches = _count_approaches(symbol, level, atr)[0] if atr > 0 else 1  # FIX BUG-6: tuple[0]=count
            if outcome_already_saved:
                logger.debug("Outcome already saved by monitor, skipping duplicate",
                             symbol=symbol, level=level, outcome=outcome)
            else:
                await save_level_outcome(
                    symbol=symbol, level=level, level_type=level_type,
                    strength=strength, approach_type=level_side,
                    vol_ratio=vol_ratio_old, touches=touches,
                    result=result, duration=duration,
                    outcome=outcome,
                    approach_style=m_approach_style,
                    vol_ratio_at_touch=m_vol_ratio,
                    atr_ratio=m_atr_ratio,
                    fill_depth_pct=fill_depth_pct,
                    btc_change_1m=btc_change,
                    funding_rate=funding,
                    monitoring_age_minutes=duration,  # BUG-4/MEDIUM-5: seconds REAL — same unit as monitor.py writers
                )
            await update_symbol_profile(symbol)
            logger.info("Level outcome saved",
                       symbol=symbol, level=level, result=result,
                       outcome=outcome, duration=duration)
            if outcome == "bounce":
                await log_event(symbol, "bounce", f"level={level} fill_depth={fill_depth_pct:.2f}% duration={duration}m")
            elif outcome == "breakout":
                await log_event(symbol, "breakout", f"level={level} duration={duration}m")
        except Exception as e:
            logger.exception("Failed to save history", task_key=task_key, error=str(e))

    except asyncio.CancelledError:
        logger.info("Monitor cancelled", symbol=symbol, level=level)
    except Exception as e:
        logger.exception("Error in monitor", symbol=symbol, level=level, error=str(e))
    finally:
        state.remove_task(task_key)

        # Route to next monitor based on direction and outcome
        if token_registry.contains(symbol):
            try:
                if reason == "breakout" and level_side == "support":
                    # Support broken → next support below
                    await _start_next_level_after_breakout(symbol, level)
                elif reason == "breakout" and level_side == "resistance":
                    # Resistance broken → next resistance above
                    await _start_resistance_after_bounce(symbol, level)
                elif outcome == "bounce" and level_side == "support":
                    # Bounced up from support → monitor resistance above
                    await _start_resistance_after_bounce(symbol, level)
                elif outcome == "bounce" and level_side == "resistance":
                    # Rejected at resistance → back to support below
                    await _start_next_level_after_breakout(symbol, level)
            except Exception as e:
                logger.exception("Error routing next monitor", symbol=symbol,
                                 level=level, reason=reason, level_side=level_side)

        # Update phase
        if not state.has_active_tasks():
            state.phase = "idle"

        # Save monitors state after change
        save_active_monitors()



async def _start_next_level_after_breakout(symbol: str, broken_level: float):
    """After a breakout, find and start monitoring the next level below.

    Priority:
    1. Levels from _last_analysis_cache (shown to user via /analyze)
    2. Rebuild levels from candle data
    If nothing found — check screener and possibly remove symbol.
    """
    from data.collector import candles_1m as _c1m, candles_15m as _c15m
    from analysis.trigger import calculate_atr, get_level_history, _count_approaches
    from bot.telegram import _last_analysis_cache

    state = state_manager.get_state(symbol)
    ext_c1m = _c1m.get(symbol, [])
    current_price = ext_c1m[-1]["close"] if ext_c1m else 0
    atr = calculate_atr(symbol)

    def _in_range(lvl_price: float) -> bool:
        if current_price <= 0:
            return False
        dist = current_price - lvl_price
        return (
            lvl_price < broken_level
            and 0 < dist <= current_price * 0.20
            and (atr == 0 or dist >= atr * 1.5)
        )

    next_started = False

    # --- Priority 0: level saved when it was replaced by a closer one ---
    prev_level = _previous_levels.pop(symbol, None)
    if prev_level and _in_range(prev_level):
        task_key = state.make_task_key(prev_level)
        if task_key not in state.tasks:
            # FIX BUG-C5: берём параметры из кэша, иначе strength=0, p_bounce=0.0
            cached_all = _last_analysis_cache.get(symbol, [])
            _prev_cached = next((l for l in cached_all if l["level"] == prev_level), {})
            task = asyncio.create_task(
                _monitored(symbol, prev_level, "support",
                           level_type=_prev_cached.get("type", "body_level"),
                           strength=_prev_cached.get("strength", 0),
                           p_bounce=_prev_cached.get("p_bounce", 0.0),
                           expected_depth=_prev_cached.get("expected_depth", 0.0))
            )
            state.add_task(prev_level, task)
            state.phase = "phase2"
            await send_message(
                f"📋 {symbol} возврат к предыдущему уровню\n"
                f"   {prev_level}\n"
                f"👁 Мониторинг запущен"
            )
            await log_event(symbol, "monitoring_start",
                           f"level={prev_level} (previous, after breakout of {broken_level})")
            next_started = True
            logger.info("Previous level started after breakout", symbol=symbol, level=prev_level)

    # --- Priority 1: cached levels from last /analyze ---
    cached = _last_analysis_cache.get(symbol, [])
    candidates = [l for l in cached if _in_range(l["level"]) and l.get("strength", 0) >= 3]

    if candidates:
        nearest = min(candidates, key=lambda l: abs(current_price - l["level"]))
        task_key = state.make_task_key(nearest["level"])
        if task_key not in state.tasks:
            # BUG-05: recalculate strength with fresh data — cache may be hours old
            nearest["approach"] = _count_approaches(symbol, nearest["level"], atr)[0] if atr > 0 else 0  # FIX BUG-6: tuple[0]=count
            if atr > 0:
                from analysis.trigger import get_level_history
                nearest.update(get_level_history(symbol, nearest["level"], atr))
            calculate_strength(nearest)
            try:
                from analysis.ml_score import apply_ml_to_level
                nearest["approach_style"] = detect_approach_style(symbol)
                nearest["monitoring_age_minutes"] = 0.0  # FIX BUG-M8: новый монитор, age = 0
                apply_ml_to_level(nearest)
            except Exception as _e:
                logger.warning("ml_score failed in cache recalc: %s", _e)
            # Re-check strength after recalculation — level may have degraded
            if nearest.get("strength", 0) < 2:
                logger.info("Cached level degraded after recalc, skipping",
                            symbol=symbol, level=nearest["level"],
                            strength=nearest.get("strength"))
            else:
                task = asyncio.create_task(
                    _monitored(symbol, nearest["level"], "support",
                               level_type=nearest["type"],
                               strength=nearest["strength"],
                               p_bounce=nearest.get("p_bounce", 0.0),
                               expected_depth=nearest.get("expected_depth", 0.0))
                )
                state.add_task(nearest["level"], task, strength=nearest.get("strength", 0))
                state.phase = "phase2"
                stars = "⭐️" * nearest["strength"]
                await send_message(
                    f"📋 {symbol} следующий уровень\n"
                    f"   {stars} {nearest['level']} — {nearest['type']}\n"
                    f"👁 Мониторинг запущен"
                )
                await log_event(symbol, "monitoring_start",
                               f"level={nearest['level']} strength={nearest['strength']} (after breakout of {broken_level})")
                next_started = True
                logger.info("Next level from cache started", symbol=symbol, level=nearest["level"])

    # --- Priority 2: rebuild from candles ---
    if not next_started and ext_c1m:
        from analysis.level_builder import build_levels
        ext_c15m = _c15m.get(symbol, [])
        all_levels = build_levels(symbol, c1m_override=ext_c1m, c15m_override=ext_c15m)

        rebuild_candidates = [lvl for lvl in all_levels if _in_range(lvl["level"])]
        for lvl in rebuild_candidates:
            lvl["symbol"] = symbol
            lvl["approach"] = _count_approaches(symbol, lvl["level"], atr)[0] if atr > 0 else 0  # FIX BUG-6: tuple[0]=count
            if atr > 0:
                lvl.update(get_level_history(symbol, lvl["level"], atr))
            calculate_strength(lvl)

        try:  # FIX BUG-15: убран мёртвый if-блок (strength==0 после calculate_strength невозможен)
            from analysis.ml_score import apply_ml_to_level
            _style_rebuild = detect_approach_style(symbol)
            for lvl in rebuild_candidates:
                lvl["approach_style"] = _style_rebuild
                lvl["monitoring_age_minutes"] = 0.0  # FIX BUG-M8: свежий rebuild, age = 0
                apply_ml_to_level(lvl)
        except Exception as _e:
            logger.warning("ml_score failed in rebuild: %s", _e)

        strong = [l for l in rebuild_candidates if l["strength"] >= 3]
        if strong:
            nearest = min(strong, key=lambda l: abs(current_price - l["level"]))
            task_key = state.make_task_key(nearest["level"])
            if task_key not in state.tasks:
                task = asyncio.create_task(
                    _monitored(symbol, nearest["level"], "support",
                               level_type=nearest["type"],
                               strength=nearest["strength"],
                               p_bounce=nearest.get("p_bounce", 0.0),
                               expected_depth=nearest.get("expected_depth", 0.0))
                )
                state.add_task(nearest["level"], task)
                state.phase = "phase2"
                stars = "⭐️" * nearest["strength"]
                await send_message(
                    f"📋 {symbol} следующий уровень\n"
                    f"   {stars} {nearest['level']} — {nearest['type']}\n"
                    f"👁 Мониторинг запущен"
                )
                await log_event(symbol, "monitoring_start",
                               f"level={nearest['level']} strength={nearest['strength']} (rebuilt, after breakout of {broken_level})")
                next_started = True
                logger.info("Next level rebuilt and started", symbol=symbol, level=nearest["level"])

    # --- No level found ---
    if not next_started:
        try:
            from bot.telegram import _last_analysis_cache
            # Don't remove if there are known levels below in cache
            cached = _last_analysis_cache.get(symbol, [])
            has_cached_below = any(
                l["level"] < broken_level
                and (current_price - l["level"]) <= current_price * 0.20
                for l in cached
            )
            if has_cached_below:
                await log_event(symbol, "breakout",
                               f"level={broken_level} — no next level started but cache has candidates")
                logger.info("No next level started but cache has candidates, keeping symbol",
                           symbol=symbol)
                return

            rows = await _run_screener()
            screener_symbols = {sym for _, _, _, _, sym in rows}
            if symbol not in screener_symbols:
                token_registry.remove(symbol)
                await log_event(symbol, "removed",
                               f"breakout at level={broken_level} + dropped from screener")
                await send_message(
                    f"🗑 {symbol} удалён из списка\n"
                    f"   Все уровни пробиты и монета выпала из скринера"
                )
            else:
                await log_event(symbol, "breakout",
                               f"level={broken_level} — no next level, still in screener")
        except Exception as e:
            logger.error("Failed to check screener after breakout", symbol=symbol, error=str(e))


def _find_resistance_above(symbol: str, current_price: float, atr: float) -> dict | None:
    """Find nearest resistance level above current price.

    Sources (in priority order):
    1. Cached levels from last /analyze that are above current price
    2. 1M wick highs above current price (clustered)
    """
    from data.collector import candles_1m as _c1m
    from bot.telegram import _last_analysis_cache

    zone_high = current_price * 1.20
    radius    = max(atr * 2, current_price * 0.005)
    candidates: list[dict] = []

    # 1. Cache (levels shown by /analyze — may include POC above price)
    for lvl in _last_analysis_cache.get(symbol, []):
        price = lvl["level"]
        if current_price < price <= zone_high:
            candidates.append({
                "level":    price,
                "type":     lvl.get("type", "body_level"),
                "strength": lvl.get("strength", 3),
            })

    # 2. 1M wick highs
    c1m = _c1m.get(symbol, [])
    if c1m:
        highs = [c["high"] for c in c1m[-300:] if current_price < c["high"] <= zone_high]
        used: set[int] = set()
        for i, h in enumerate(highs):
            if i in used:
                continue
            cluster = [h]
            for j, h2 in enumerate(highs):
                if j != i and j not in used and abs(h2 - h) <= radius:
                    cluster.append(h2)
                    used.add(j)
            used.add(i)
            if len(cluster) >= 2:
                avg = sum(cluster) / len(cluster)
                candidates.append({"level": avg, "type": "wick_level", "strength": 3})

    if not candidates:
        return None
    return min(candidates, key=lambda c: c["level"] - current_price)


async def _start_resistance_after_bounce(symbol: str, support_level: float) -> None:
    """After a bounce from support (or breakout of resistance), monitor the nearest resistance above."""
    from data.collector import candles_1m as _c1m
    from analysis.trigger import calculate_atr

    if not token_registry.contains(symbol):
        return

    state = state_manager.get_state(symbol)
    c1m   = _c1m.get(symbol, [])
    if not c1m:
        return

    current_price = c1m[-1]["close"]
    atr           = calculate_atr(symbol)
    resistance    = _find_resistance_above(symbol, current_price, atr)

    if not resistance:
        logger.info("No resistance found after bounce", symbol=symbol, support=support_level)
        return

    task_key = state.make_task_key(resistance["level"])
    if task_key in state.tasks:
        return

    from analysis.trigger import _count_approaches
    res_approach = _count_approaches(symbol, resistance["level"], atr)[0] if atr > 0 else 0

    task = asyncio.create_task(
        _monitored(symbol, resistance["level"], "resistance",
                   level_type=resistance["type"],
                   strength=resistance.get("strength", 3),
                   approach=res_approach)
    )
    state.add_task(resistance["level"], task)
    state.phase = "phase2"

    stars = "⭐️" * resistance.get("strength", 3)
    if resistance.get("strength", 3) >= 3:
        await send_message(
            f"🔺 {symbol} сопротивление после отскока\n"
            f"   {stars} {resistance['level']} — {resistance['type']}\n"
            f"👁 Мониторинг запущен"
        )
    else:
        logger.debug("weak resistance after bounce (telegram disabled)",
                     symbol=symbol, level=resistance["level"],
                     strength=resistance.get("strength", 3))
    await log_event(symbol, "monitoring_start",
                    f"level={resistance['level']} side=resistance (after bounce from {support_level})")
    logger.info("Resistance monitor started", symbol=symbol,
                resistance=resistance["level"], from_support=support_level)


def cancel_tasks_for_symbol(symbol: str):
    """Cancel all monitoring tasks for a symbol."""
    state = state_manager.get_state(symbol)
    state.cancel_all_tasks()
    logger.info("All tasks cancelled", symbol=symbol)


def clear_analysis_cache(symbol: str):
    """Clear analyzed levels cache for a symbol."""
    state = state_manager.get_state(symbol)
    state.clear_analyzed_levels()
    logger.info("Analysis cache cleared", symbol=symbol)


async def _stale_monitor_loop() -> None:
    """Every 5 minutes: cancel monitors that drifted >10% from price and rebuild levels.

    Handles the case where price pumped far away from an old support level but
    no new trigger fired, so _run_phase1 was never called to find a closer level.
    """
    STALE_PCT   = 10.0   # cancel if level is more than 10% from current price
    INTERVAL    = 300    # check every 5 minutes

    await asyncio.sleep(90)  # let startup settle first
    while True:
        try:
            stale_symbols: set[str] = set()
            cancelled_tasks: list[asyncio.Task] = []
            all_tasks = state_manager.get_all_active_tasks()

            for task_key, task in list(all_tasks.items()):
                from models import SymbolState
                parsed = SymbolState.parse_task_key(task_key)
                if parsed is None:
                    continue
                symbol, level = parsed

                c1m = candles_1m.get(symbol, [])
                if not c1m or level == 0:
                    continue

                current_price = c1m[-1]["close"]
                if current_price == 0:
                    continue

                distance_pct = abs(current_price - level) / current_price * 100
                if distance_pct <= STALE_PCT:
                    continue

                # Cancel stale monitor
                state   = state_manager.get_state(symbol)
                stop_ev = state.stop_flags.get(task_key)
                if stop_ev:
                    stop_ev.set()
                if not task.done():
                    task.cancel()
                    cancelled_tasks.append(task)
                state.remove_task(task_key)
                stale_symbols.add(symbol)
                logger.info("Stale monitor cancelled — rebuilding",
                            symbol=symbol, level=level,
                            current=current_price, distance_pct=round(distance_pct, 1))

            # Wait for all cancelled tasks to fully exit their finally blocks
            # before starting _run_phase1, to avoid two monitors on the same symbol.
            if cancelled_tasks:
                await asyncio.gather(*cancelled_tasks, return_exceptions=True)

            # Trigger level rebuild for each affected symbol
            for symbol in stale_symbols:
                if symbol not in _building_levels and token_registry.contains(symbol):
                    _building_levels.add(symbol)  # FIX BUG-C2: до create_task, иначе _trigger_loop успевает войти
                    await asyncio.sleep(0.3)
                    asyncio.create_task(_run_phase1(symbol))

        except Exception:
            logger.exception("Error in stale monitor loop")

        await asyncio.sleep(INTERVAL)


async def _proximity_loop():
    """Loop checking for proximity alerts."""
    while True:
        try:
            all_tasks = state_manager.get_all_active_tasks()
            
            for task_key, task in list(all_tasks.items()):
                # Parse task_key: "SYMBOL::LEVEL"
                from models import SymbolState as _SS
                _parsed = _SS.parse_task_key(task_key)
                if _parsed is None:
                    continue
                symbol, level = _parsed

                state = state_manager.get_state(symbol)
                
                # Check if task is stopped
                stop_event = state.stop_flags.get(task_key)
                if stop_event and stop_event.is_set():
                    continue
                
                # Get current price
                c1m = candles_1m.get(symbol, [])
                if not c1m:
                    continue
                    
                current_price = c1m[-1]["close"]
                if level == 0:
                    continue
                    
                distance_pct = abs(current_price - level) / current_price * 100

                now = time.time()
                last_sent = state.proximity_notified.get(task_key, 0)

                # Only alert if price is approaching from above (for support)
                # and hasn't already bounced (price still near or below level)
                approaching = current_price > level  # price above support = approaching
                cooldown_ok = (now - last_sent) > PROXIMITY_ALERT_COOLDOWN_SECONDS
                
                # Check if we're in proximity zone
                # FIX BUG-9: PROXIMITY_ALERT_DISTANCE_PCT=0.02 (доля), distance_pct в %; убрано *100 — используем долю напрямую
                distance_fraction = abs(current_price - level) / current_price
                in_proximity_zone = distance_fraction <= PROXIMITY_ALERT_DISTANCE_PCT

                # Only send alert if:
                # 1. In proximity zone
                # 2. Approaching from above
                # 3. Pump is not dead (pump_phase guard)
                _pump_phase_ok = state.pump_phase not in ("dead",)

                # ── Telegram alert: ~раз за сессию мониторинга (24ч cooldown) ──
                if in_proximity_zone and approaching and cooldown_ok and _pump_phase_ok:
                    await send_message(
                        f"🎯 {symbol} цена в {distance_pct:.2f}% от уровня {level} — готовь ордер"
                    )
                    state.proximity_notified[task_key] = now
                    logger.info("Proximity alert sent", 
                               symbol=symbol, 
                               level=level, 
                               distance_pct=distance_pct)

                # ── Event bus: отдельный короткий cooldown — стратегиям (S1/S2/S3)
                # нужно несколько шансов поймать цену в своей зоне входа, а не один
                # на всю сессию (FIX: S2 не успевал выставить сетку, т.к. единственное
                # proximity-событие приходило ещё до входа цены в узкую зону грида).
                last_event_sent = _proximity_event_sent.get(task_key, 0)
                event_cooldown_ok = (now - last_event_sent) > PROXIMITY_EVENT_COOLDOWN_SECONDS
                if in_proximity_zone and approaching and event_cooldown_ok and _pump_phase_ok:
                    _proximity_event_sent[task_key] = now
                    # Publish proximity event to strategy event bus
                    try:
                        from trading.event_bus import publish as _eb_publish
                        from analysis.trigger import calculate_atr as _calc_atr_prox
                        from bot.telegram import _last_analysis_cache as _lac_prox
                        _atr_prox = _calc_atr_prox(symbol)
                        _cached_prox = {
                            lvl["level"]: lvl
                            for lvl in _lac_prox.get(symbol, [])
                        }
                        _lvl_info = min(
                            _cached_prox.values(),
                            key=lambda x: abs(x["level"] - level),
                            default={},
                        ) if _cached_prox else {}
                        _avg_vol_prox = sum(c["volume"] for c in c1m[-20:]) / max(len(c1m[-20:]), 1)
                        _vr_prox = round(c1m[-1]["volume"] / _avg_vol_prox, 2) if _avg_vol_prox > 0 else 1.0
                        await _eb_publish({
                            "event_type": "proximity",
                            "symbol": symbol,
                            "level": level,
                            "level_side": "support" if current_price > level else "resistance",
                            "level_type": _lvl_info.get("type", "body_level"),
                            "strength": state.level_strengths.get(task_key, _lvl_info.get("strength", 0)),
                            "p_bounce": _lvl_info.get("p_bounce", 0.0),
                            "expected_depth": _lvl_info.get("expected_depth", 0.0),
                            "approach_style": detect_approach_style(symbol),
                            "vol_ratio": _vr_prox,
                            "atr": _atr_prox,
                            "current_price": current_price,
                            "timestamp": now,
                        })
                    except Exception as _eb_e:
                        logger.debug("event_bus publish error (proximity): %s", _eb_e)

                # Reset cooldown if price moved far away (> 5% from level)
                # This allows re-alerting if price comes back after leaving
                if distance_pct > 5.0 and task_key in state.proximity_notified:
                    # Only reset if enough time passed (at least 1 hour)
                    if (now - last_sent) > 3600:
                        del state.proximity_notified[task_key]
                        logger.debug("Proximity cooldown reset - price moved away",
                                   symbol=symbol,
                                   level=level,
                                   distance_pct=distance_pct)

                # Drop event_bus cooldown entry once price is far from the level —
                # next approach should be free to publish proximity again immediately.
                if distance_pct > 5.0:
                    _proximity_event_sent.pop(task_key, None)

            # Track touches on weak (unmonitored) levels from cache
            from bot.telegram import _last_analysis_cache
            for symbol, cached_levels in list(_last_analysis_cache.items()):
                c1m = candles_1m.get(symbol, [])
                if len(c1m) < 5:
                    continue
                current_price = c1m[-1]["close"]
                sym_state = state_manager.get_state(symbol)
                monitored = {
                    p[1] for k in sym_state.tasks
                    if (p := sym_state.parse_task_key(k)) is not None
                }

                for lvl_info in cached_levels:
                    lvl_price = lvl_info["level"]
                    strength = lvl_info.get("strength", 0)

                    if lvl_price in monitored:
                        continue  # already fully monitored
                    if strength >= 3:
                        continue  # strong levels are monitored separately
                    if lvl_price >= current_price:
                        continue  # only support levels below price

                    touch_idx_key = f"touch_idx_{symbol}_{lvl_price}"
                    min_price_key = f"min_price_{symbol}_{lvl_price}"
                    resolved_key  = f"resolved_{symbol}_{lvl_price}"
                    wts = sym_state.weak_touch_state

                    from analysis.trigger import calculate_atr
                    atr = calculate_atr(symbol)
                    touch_zone = atr * 0.5 if atr > 0 else lvl_price * 0.005

                    price_touched = current_price <= lvl_price + touch_zone

                    if price_touched:
                        if touch_idx_key not in wts:
                            # First touch — record candle index and min price
                            wts[touch_idx_key] = len(c1m) - 1
                            wts[min_price_key] = c1m[-1]["low"]
                        else:
                            # Update min price during touch
                            wts[min_price_key] = min(wts.get(min_price_key, lvl_price), c1m[-1]["low"])
                    else:
                        # Price moved away — resolve if we had a touch
                        touch_idx = wts.get(touch_idx_key)
                        if touch_idx is not None and resolved_key not in wts:
                            min_price = wts.get(min_price_key, lvl_price)
                            fill_depth = (lvl_price - min_price) / lvl_price * 100 if min_price < lvl_price else 0.0

                            # Determine outcome: bounce or breakout
                            post_touch = c1m[int(touch_idx):]
                            returned_above = any(c["close"] > lvl_price for c in post_touch[-10:])
                            stayed_below = all(c["close"] < lvl_price for c in post_touch[-5:]) if len(post_touch) >= 5 else False

                            if returned_above:
                                event = "zakol" if fill_depth >= 0.3 else "bounce"
                                await log_event(symbol, event,
                                               f"level={lvl_price} strength={strength} depth={fill_depth:.2f}% (weak, unmonitored)")
                                logger.info("Weak level touch resolved",
                                           symbol=symbol, level=lvl_price,
                                           event=event, fill_depth=fill_depth)
                            elif stayed_below:
                                await log_event(symbol, "breakout",
                                               f"level={lvl_price} strength={strength} (weak, unmonitored)")
                                logger.info("Weak level broken",
                                           symbol=symbol, level=lvl_price)

                            # Mark resolved, clean up touch state
                            wts[resolved_key] = time.time()
                            wts.pop(touch_idx_key, None)
                            wts.pop(min_price_key, None)

                        # Reset resolve flag after price moves far away (> 2 ATR)
                        if resolved_key in wts:
                            dist = current_price - lvl_price
                            if atr > 0 and dist > atr * 2:
                                wts.pop(resolved_key, None)
                    
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("Error in proximity loop", error=str(e))
            
        await asyncio.sleep(COLLECTOR_UPDATE_INTERVAL_SECONDS)



async def shutdown():
    """Graceful shutdown of all bot components."""
    logger.info("Shutting down gracefully...")
    
    # Save tasks before cancelling, then wait for them
    all_tasks = state_manager.get_all_active_tasks()
    state_manager.cancel_all_tasks()
    if all_tasks:
        await asyncio.gather(*all_tasks.values(), return_exceptions=True)
    
    try:
        await send_message("🛑 Бот остановлен")
    except Exception:
        pass
        
    logger.info("Shutdown complete")


async def _startup_monitoring():
    """On startup, build levels and start monitoring for all symbols in list."""
    # Wait for collector to load candle data
    await asyncio.sleep(30)

    tokens = token_registry.get_all()
    if not tokens:
        return

    logger.info("Starting monitoring for existing symbols", count=len(tokens))

    from binance import AsyncClient
    from data.collector import _parse_kline, candles_1m, candles_15m
    from analysis.level_builder import build_levels
    from analysis.trigger import calculate_atr, get_level_history, _count_approaches
    import json as _json

    client = await AsyncClient.create()
    try:
        # --- Step 1: Restore previously active monitors from file ---
        saved_monitors = load_active_monitors()
        restored_symbols: set[str] = set()

        if saved_monitors:
            logger.info("Restoring saved monitors", count=len(saved_monitors))

            from collections import defaultdict
            by_symbol: dict[str, list[tuple]] = defaultdict(list)
            for entry in saved_monitors:
                sym   = entry.get("symbol")
                level = entry.get("level")
                if sym and level and sym in tokens:
                    meta = {
                        "active":         entry.get("active", False),
                        "strength":       entry.get("strength", 0),
                        "type":           entry.get("type", "body_level"),
                        "p_bounce":       entry.get("p_bounce", 0.0),
                        "expected_depth": entry.get("expected_depth", 0.0),
                    }
                    by_symbol[sym].append((float(level), entry.get("level_side", "support"), meta))

            for sym, level_entries in by_symbol.items():
                try:
                    if sym not in candles_1m or not candles_1m[sym]:
                        raw_15m = await client.futures_klines(symbol=sym, interval="15m", limit=500)
                        raw_1m  = await client.futures_klines(symbol=sym, interval="1m",  limit=300)
                        candles_15m[sym] = [_parse_kline(k) for k in raw_15m]
                        candles_1m[sym]  = [_parse_kline(k) for k in raw_1m]

                    c1m = candles_1m.get(sym, [])
                    current_price = c1m[-1]["close"] if c1m else 0

                    # Восстанавливаем кэш всех уровней из файла
                    from bot.telegram import _last_analysis_cache as _lac_restore
                    _lac_restore[sym] = [
                        {
                            "level":          lvl,
                            "strength":       meta.get("strength", 0),
                            "type":           meta.get("type", "body_level"),
                            "p_bounce":       meta.get("p_bounce", 0.0),
                            "expected_depth": meta.get("expected_depth", 0.0),
                        }
                        for lvl, side, meta in sorted(level_entries, key=lambda e: e[0])
                    ]

                    # Монитор: активный если есть, иначе ближайший
                    active_entries = [(lvl, side, meta) for lvl, side, meta in level_entries if meta.get("active")]
                    candidates = active_entries if active_entries else level_entries
                    nearest_lvl, saved_side, nearest_meta = (
                        min(candidates, key=lambda e: abs(current_price - e[0]))
                        if current_price > 0 else candidates[0]
                    )

                    strength               = nearest_meta.get("strength", 0)
                    p_bounce_restore       = nearest_meta.get("p_bounce", 0.0)
                    expected_depth_restore = nearest_meta.get("expected_depth", 0.0)
                    level_type_restore     = nearest_meta.get("type", "body_level")

                    sym_state = state_manager.get_state(sym)
                    task_key  = sym_state.make_task_key(nearest_lvl)
                    if task_key not in sym_state.tasks:
                        task = asyncio.create_task(_monitored(sym, nearest_lvl, saved_side,
                            level_type=level_type_restore,
                            strength=strength,
                            p_bounce=p_bounce_restore,
                            expected_depth=expected_depth_restore))
                        sym_state.add_task(nearest_lvl, task, strength=strength)
                        sym_state.phase = "phase2"

                        restored_symbols.add(sym)
                        all_lvls = [e[0] for e in level_entries]
                        if len(all_lvls) > 1:
                            logger.info("Restored nearest of multiple saved monitors",
                                        symbol=sym, chosen=nearest_lvl,
                                        discarded=[l for l in all_lvls if l != nearest_lvl])
                        else:
                            logger.info("Monitor restored", symbol=sym, level=nearest_lvl)
                        await log_event(sym, "monitoring_start",
                            f"level={nearest_lvl} strength={strength} (restored after restart)")
                except Exception as e:
                    logger.exception("Failed to restore monitor", symbol=sym, error=str(e))

            if restored_symbols:
                await send_message(
                    f"♻️ Восстановлены мониторинги после рестарта:\n" +
                    "\n".join(f"   {s}" for s in sorted(restored_symbols))
                )

        # --- Step 2: For symbols without restored monitors — build fresh ---
        for symbol in tokens:
            if symbol in restored_symbols:
                continue  # already restored
            sym_state = state_manager.get_state(symbol)
            if sym_state.has_active_tasks():
                continue  # already monitoring
            try:
                raw_15m = await client.futures_klines(symbol=symbol, interval="15m", limit=500)
                raw_1m = await client.futures_klines(symbol=symbol, interval="1m", limit=300)
                candles_15m[symbol] = [_parse_kline(k) for k in raw_15m]
                candles_1m[symbol] = [_parse_kline(k) for k in raw_1m]

                ext_c1m = candles_1m[symbol]
                ext_c15m = candles_15m[symbol]
                all_levels = build_levels(symbol, c1m_override=ext_c1m, c15m_override=ext_c15m)

                if not all_levels:
                    logger.debug("No levels on startup", symbol=symbol)
                    continue

                current_price = ext_c1m[-1]["close"]
                atr = calculate_atr(symbol)
                range_limit = current_price * 0.20

                # Initialise pump phase so _run_phase1 / _proximity_loop have correct state
                try:
                    from analysis.pump_phase import detect_pump_peak, pump_health_score, get_pump_phase
                    _ph, _pb, _pht = detect_pump_peak(symbol)
                    if _ph > 0:
                        sym_state.pump_high = _ph
                        sym_state.pump_base_price = _pb
                        sym_state.pump_high_time = _pht
                    _health = pump_health_score(sym_state, current_price)
                    sym_state.pump_health = _health
                    sym_state.pump_phase = get_pump_phase(_health)
                    if _health < PUMP_HEALTH_MIN_SCORE:
                        logger.info("Pump degraded on startup, skipping",
                                    symbol=symbol, health=_health)
                        continue
                except Exception as _pe:
                    logger.warning("pump_health init failed on startup: %s", _pe)

                supports = [
                    lvl for lvl in all_levels
                    if lvl["level"] < current_price
                    and (current_price - lvl["level"]) <= range_limit
                    and (current_price - lvl["level"]) >= atr * 1.5
                ]

                if not supports:
                    continue

                for lvl in supports:
                    lvl["symbol"] = symbol
                    lvl["approach"] = _count_approaches(symbol, lvl["level"], atr)[0] if atr > 0 else 0  # FIX BUG-6: tuple[0]=count
                    if atr > 0:
                        lvl.update(get_level_history(symbol, lvl["level"], atr))
                    calculate_strength(lvl)
                    lvl["python_strength"] = lvl["strength"]

                try:  # FIX BUG-15: убран мёртвый if-блок (strength==0 после calculate_strength невозможен)
                    from analysis.ml_score import apply_ml_to_level
                    _style_startup = detect_approach_style(symbol)
                    for lvl in supports:
                        lvl["approach_style"] = _style_startup
                        lvl["monitoring_age_minutes"] = 0.0  # FIX BUG-M8: старт, age = 0
                        apply_ml_to_level(lvl)
                except Exception as _e:
                    logger.warning("ml_score failed in startup: %s", _e)

                # Startup: use Python only, no Claude (save tokens)
                strong = [l for l in supports if l["strength"] >= 3]
                if not strong:
                    logger.debug("No strong levels on startup", symbol=symbol)
                    continue

                # Monitor only the nearest level — others picked up after breakout
                nearest = min(strong, key=lambda l: abs(current_price - l["level"]))

                # Log levels built
                levels_info = [{"level": l["level"], "type": l["type"], "strength": l["strength"]} for l in strong]
                await log_event(symbol, "levels_built", _json.dumps(levels_info))

                sym_state = state_manager.get_state(symbol)
                task_key = sym_state.make_task_key(nearest["level"])
                if task_key not in sym_state.tasks:
                    stars = "⭐️" * nearest["strength"]
                    dist_pct = (current_price - nearest["level"]) / current_price * 100
                    p_b = nearest.get("p_bounce")
                    startup_text = (
                        f"📋 {symbol} мониторинг при старте\n"
                        f"   {stars} {nearest['level']} — {nearest.get('type', '')} ({dist_pct:.1f}%)\n"
                    )
                    if p_b is not None:
                        e_d = nearest.get("expected_depth")
                        depth_str = f" | прокол ~{e_d:.1f}%" if e_d is not None else ""
                        startup_text += f"   🤖 P(отбой): {p_b:.0%}{depth_str}\n"
                    startup_text += f"   Жду цену на {nearest['level']}..."
                    await send_message(startup_text)

                    task = asyncio.create_task(
                        _monitored(symbol, nearest["level"], "support",
                                  level_type=nearest["type"],
                                  strength=nearest["strength"],
                                  p_bounce=nearest.get("p_bounce", 0.0),
                                  expected_depth=nearest.get("expected_depth", 0.0))
                    )
                    sym_state.add_task(nearest["level"], task, strength=nearest.get("strength", 0))
                    sym_state.phase = "phase2"

                    # Заполняем кэш чтобы _proximity_loop брал корректные strength/p_bounce
                    from bot.telegram import _last_analysis_cache as _lac_startup
                    _lac_startup[symbol] = [
                        {
                            "level": l["level"],
                            "strength": l["strength"],
                            "type": l["type"],
                            "p_bounce": l.get("p_bounce", 0.0),
                            "expected_depth": l.get("expected_depth", 0.0),
                        }
                        for l in sorted(supports, key=lambda x: x["level"])
                    ]

                    await log_event(symbol, "monitoring_start",
                                   f"level={nearest['level']} strength={nearest['strength']} type={nearest['type']} (startup)")
                    logger.info("Startup monitoring started", symbol=symbol, level=nearest["level"])

            except Exception as e:
                logger.exception("Error starting monitoring for symbol on startup",
                               symbol=symbol, error=str(e))
    finally:
        await client.close_connection()



# Время первого сбоя по каждому условию: symbol -> timestamp
_natr_fail_since: dict[str, float] = {}
_trades_fail_since: dict[str, float] = {}


async def _monitor_health_loop():
    """Every minute: remove symbols that lost activity (low NATR or low trade count).

    Checks for each monitored symbol:
    - NATR(5m, 14 bars) >= MONITOR_MIN_NATR_5M (0.8%) — удаляет после 60 мин непрерывного сбоя
    - Last closed 1m candle trades >= MONITOR_MIN_1M_TRADES (200) — удаляет после 30 мин непрерывного сбоя

    On failure: stops all monitors, removes from token_registry.
    Re-entry only via screener (all original criteria apply).
    """
    from data.collector import candles_5m, candles_1m as _c1m

    await asyncio.sleep(120)  # дать старту устояться
    while True:
        try:
            now = time.time()
            for symbol in list(token_registry.get_all()):
                if blacklist.contains(symbol):
                    continue

                # ── 1. NATR(5m, 14 bars) ──────────────────────────────
                c5m = candles_5m.get(symbol, [])
                if len(c5m) < 2:
                    continue  # данных нет — не трогаем
                bars = c5m[-14:] if len(c5m) >= 14 else c5m
                current_price = float(c5m[-1]["close"])
                if current_price == 0:
                    continue
                atr_5m = sum(float(k["high"]) - float(k["low"]) for k in bars) / len(bars)
                natr_5m = atr_5m / current_price * 100

                # ── 2. Trades в последней закрытой 1m свече ───────────
                c1m = _c1m.get(symbol, [])
                # [-2] — последняя закрытая; [-1] может быть открытой
                if len(c1m) < 2:
                    continue
                last_closed_trades = c1m[-2]["trades"]

                # ── 3. Обновление таймеров сбоев ─────────────────────
                natr_ok = natr_5m >= MONITOR_MIN_NATR_5M
                trades_ok = last_closed_trades >= MONITOR_MIN_1M_TRADES

                if natr_ok:
                    _natr_fail_since.pop(symbol, None)
                else:
                    _natr_fail_since.setdefault(symbol, now)

                if trades_ok:
                    _trades_fail_since.pop(symbol, None)
                else:
                    _trades_fail_since.setdefault(symbol, now)

                # ── 4. Проверка: достаточно ли долго условие не выполняется ──
                natr_fail_long = (
                    not natr_ok
                    and now - _natr_fail_since[symbol] >= MONITOR_NATR_FAIL_DURATION_SECONDS
                )
                trades_fail_long = (
                    not trades_ok
                    and now - _trades_fail_since[symbol] >= MONITOR_TRADES_FAIL_DURATION_SECONDS
                )

                if not natr_fail_long and not trades_fail_long:
                    continue

                # ── 5. Удаление ───────────────────────────────────────
                reasons = []
                if natr_fail_long:
                    reasons.append(f"NATR(5m)={natr_5m:.2f}% < {MONITOR_MIN_NATR_5M}% на протяжении часа")
                if trades_fail_long:
                    reasons.append(f"сделок(1m)={last_closed_trades} < {MONITOR_MIN_1M_TRADES} на протяжении 30 мин")

                _natr_fail_since.pop(symbol, None)
                _trades_fail_since.pop(symbol, None)

                # Останавливаем все мониторы символа
                state = state_manager.get_state(symbol)
                for task_key in list(state.tasks.keys()):
                    stop_ev = state.stop_flags.get(task_key)
                    if stop_ev:
                        stop_ev.set()
                    task = state.tasks.get(task_key)
                    if task and not task.done():
                        task.cancel()
                    state.remove_task(task_key)
                state.phase = "idle"

                token_registry.remove(symbol)
                await log_event(symbol, "removed", "monitor_health: " + "; ".join(reasons))
                # Уведомление в Telegram отключено.
                logger.info("Symbol removed by monitor health check",
                            symbol=symbol, natr_5m=round(natr_5m, 2),
                            trades_1m=last_closed_trades)

        except Exception:
            logger.exception("Error in monitor health loop")

        await asyncio.sleep(MONITOR_HEALTH_INTERVAL_SECONDS)


async def _ml_retrain_loop():
    """Periodically check if ML models need retraining and reload after train_ml.py finishes."""
    await asyncio.sleep(300)  # wait 5 min after startup
    while True:
        try:
            from train_ml import should_retrain
            from data.history import DB_PATH as _db_path
            if should_retrain(_db_path):
                logger.info("ML retrain triggered")
                proc = await asyncio.create_subprocess_exec(
                    sys.executable, "train_ml.py", "--db", _db_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode == 0:
                    from analysis.ml_score import reload_models
                    await reload_models()
                    logger.info("ML retrain complete, models reloaded")
                else:
                    logger.error("train_ml.py failed",
                                 returncode=proc.returncode,
                                 stderr=stderr.decode()[:500])
        except Exception:
            logger.exception("Error in ML retrain loop")
        await asyncio.sleep(3600)  # check every hour



async def _check_bybit() -> bool:
    """Проверить подключение к Bybit Demo при старте. Возвращает False если ключи не заданы или подпись неверна."""
    await bot_ready.wait()
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        logger.warning("Bybit API keys not set — S2 Live trading disabled")
        return False
    try:
        from trading.bybit_client import _get
        resp = await _get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        if resp.get("retCode") != 0:
            logger.error(
                "Bybit Demo auth failed — S2 Live disabled",
                retCode=resp.get("retCode"),
                retMsg=resp.get("retMsg"),
            )
            await send_message(
                f"⚠️ Bybit Demo: ошибка авторизации (retCode={resp.get('retCode')}). "
                f"S2 Live торговля отключена."
            )
            return False
        coins = resp["result"]["list"][0].get("coin", [])
        usdt = next((c for c in coins if c["coin"] == "USDT"), None)
        balance = float(usdt.get("walletBalance", 0)) if usdt else 0.0
        logger.info("Bybit Demo connected", balance_usdt=round(balance, 2))
        await send_message(f"✅ Bybit Demo подключён | Баланс: {balance:.2f} USDT")
        return True
    except Exception as e:
        logger.error("Bybit Demo connection error — S2 Live disabled", error=str(e))
        await send_message(f"⚠️ Bybit Demo: ошибка подключения ({e}). S2 Live торговля отключена.")
        return False


from constants import (
    PROXIMITY_ARM_MODE, PROXIMITY_ARM_SCAN_INTERVAL,
    PROXIMITY_ARM_DIST_ATR_MIN, PROXIMITY_ARM_DIST_ATR_MAX,
    PROXIMITY_ARM_SHADOW_MIN_STRENGTH, PROXIMITY_ARM_MIN_STRENGTH,
    PROXIMITY_ARM_LEVEL_TYPES, PROXIMITY_ARM_COOLDOWN_SECONDS,
    PROXIMITY_ARM_MAX_CONCURRENT,
)

# ═════════════════════════════════════════════════════════════════════════════
# Proximity-arm (Шаг 1: shadow) — доармить СПЯЩИЕ известные монеты по приближению
# к сильной поддержке, В ОБХОД check_trigger. check_trigger требует рост ≥3% в
# ПОСЛЕДНЕМ часе и потому пропускает поздние откаты к pump_base (памп был раньше).
# Отдельный путь; в _trigger_loop / _stale_monitor_loop не вмешивается.
# Режим по умолчанию "shadow": только логирует would-arm, ничего не армит.
# ═════════════════════════════════════════════════════════════════════════════
_proximity_arm_times: dict[str, float] = {}   # symbol -> ts последнего проксимити-арминга


def _proximity_arm_candidate(symbol: str) -> dict | None:
    """Дешёвый Python-only пре-чек (без Claude, без сайд-эффектов).

    Возвращает ближайшую сильную поддержку НИЖЕ цены в полосе
    [DIST_ATR_MIN..DIST_ATR_MAX]·ATR как dict(level,type,strength,dist_atr), либо None.
    """
    from analysis.level_builder import build_levels
    from analysis.trigger import calculate_atr

    c1m = candles_1m.get(symbol, [])
    if len(c1m) < 20:
        return None
    current_price = c1m[-1]["close"]
    if current_price <= 0:
        return None
    atr = calculate_atr(symbol)
    if atr <= 0:
        return None

    lo = PROXIMITY_ARM_DIST_ATR_MIN * atr
    hi = PROXIMITY_ARM_DIST_ATR_MAX * atr

    best = None
    best_dist = None
    for lvl in build_levels(symbol):
        lv = lvl.get("level", 0.0)
        if lv <= 0 or lv >= current_price:
            continue
        dist = current_price - lv
        if dist < lo or dist > hi:
            continue
        calculate_strength(lvl)              # Python-сила, без Claude
        st = lvl.get("strength", 0)
        if st < PROXIMITY_ARM_SHADOW_MIN_STRENGTH:
            continue
        if best is None or dist < best_dist:
            best = {"level": lv, "type": lvl.get("type", "?"),
                    "strength": st, "dist_atr": round(dist / atr, 2)}
            best_dist = dist
    return best


async def _proximity_arm_loop():
    """Шаг 1 (shadow): скан спящих известных монет и лог would-arm.

    active-ветка (Шаг 3) применяет строгие фильтры и делегирует в _run_phase1
    ровно как _stale_monitor_loop — общий скоринг/guard/мониторинг не дублируются.
    """
    if PROXIMITY_ARM_MODE == "off":
        return
    await asyncio.sleep(120)   # дать старту устаканиться
    logger.info("proximity-arm loop started", mode=PROXIMITY_ARM_MODE)

    while True:
        try:
            for symbol in token_registry.get_all():
                state = state_manager.get_state(symbol)

                # только спящие: не строится, не мониторит, не в blacklist, ascii
                if symbol in _building_levels or state.phase == "phase1":
                    continue
                if state.has_active_tasks():
                    continue
                if blacklist.contains(symbol) or not symbol.isascii():
                    continue
                if time.time() - _proximity_arm_times.get(symbol, 0) < PROXIMITY_ARM_COOLDOWN_SECONDS:
                    continue

                cand = _proximity_arm_candidate(symbol)
                if not cand:
                    continue

                # ── SHADOW: только лог, дедуп раз в кулдаун, никакого арминга ──
                if PROXIMITY_ARM_MODE == "shadow":
                    _proximity_arm_times[symbol] = time.time()
                    logger.info("proximity-arm would-arm (shadow)",
                                symbol=symbol, level=cand["level"], type=cand["type"],
                                strength=cand["strength"], dist_atr=cand["dist_atr"])
                    continue

                # ── ACTIVE: строгие фильтры + делегирование в _run_phase1 ──────
                if cand["strength"] < PROXIMITY_ARM_MIN_STRENGTH:
                    continue
                if cand["type"] not in PROXIMITY_ARM_LEVEL_TYPES:
                    continue
                busy = sum(1 for s in token_registry.get_all()
                           if state_manager.get_state(s).phase == "phase1")
                if busy >= PROXIMITY_ARM_MAX_CONCURRENT:
                    break   # система занята сборкой — доберём на следующем скане

                _proximity_arm_times[symbol] = time.time()
                _building_levels.add(symbol)             # до create_task — как в _stale_monitor_loop
                logger.info("proximity-arm activated",
                            symbol=symbol, level=cand["level"], type=cand["type"],
                            strength=cand["strength"], dist_atr=cand["dist_atr"])
                asyncio.create_task(_run_phase1(symbol))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("Error in proximity-arm loop", error=str(e))

        await asyncio.sleep(PROXIMITY_ARM_SCAN_INTERVAL)


async def main():
    """Main entry point."""
    # Validate configuration
    if not validate_config():
        logger.error("Configuration validation failed")
        return

    # Initialize database
    await init_db()
    logger.info("Database initialized")

    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    try:
        import signal
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(shutdown()))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(shutdown()))
    except (NotImplementedError, AttributeError):
        # Windows doesn't support add_signal_handler
        logger.warning("Signal handlers not available on this platform")

    logger.info("Starting trading bot...")

    from trading.strategy_runner import run_strategies
    from web_server import start_web_server

    # Start all components
    await asyncio.gather(
        start_collector(),
        start_bot(),
        _check_bybit(),
        _trigger_loop(),
        _proximity_loop(),
        _auto_screener_loop(),
        _startup_monitoring(),
        _stale_monitor_loop(),
        _proximity_arm_loop(),
        _monitor_health_loop(),
        _ml_retrain_loop(),
        run_strategies(),
        start_web_server(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down")
