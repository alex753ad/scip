from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import asyncio

from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_PROXY, token_registry, blacklist
from logger import logger

# Bot will be initialized in start_bot() function
bot = None
bot_ready = asyncio.Event()
dp = Dispatcher()
router = Router()
dp.include_router(router)

# Cache of last analysis results per symbol: {symbol: [{"level": float, "strength": int, "type": str}, ...]}
_last_analysis_cache: dict[str, list[dict]] = {}


def _authorized(message: Message) -> bool:
    return message.chat.id == TELEGRAM_CHAT_ID


def _authorized_cb(callback: CallbackQuery) -> bool:
    return callback.message.chat.id == TELEGRAM_CHAT_ID


def normalize_symbol(raw: str) -> str:
    symbol = raw.upper().strip()
    if not symbol.endswith("USDT"):
        symbol = symbol + "USDT"
    return symbol


def get_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить"), KeyboardButton(text="➖ Убрать")],
            [KeyboardButton(text="📋 Список"), KeyboardButton(text="👁 Мониторинги")],
            [KeyboardButton(text="🔍 Проверить уровень")],
            [KeyboardButton(text="📊 Анализ"), KeyboardButton(text="📊 Рынок")],
            [KeyboardButton(text="📊 S2 статистика"), KeyboardButton(text="📈 S5 статистика")],
        ],
        resize_keyboard=True,
        persistent=True,
    )




@router.message(F.text == "📈 S5 статистика")
async def btn_s5_stats(message: Message):
    if not _authorized(message):
        return
    from trading import s5_live_log
    from data.collector import candles_1m
    try:
        stats = await s5_live_log.get_stats()
        open_trades = await s5_live_log.get_open_trades()
    except Exception as e:
        await message.answer(f"Ошибка получения S5-статистики: {e}",
                             reply_markup=get_main_keyboard())
        return

    open_lines = []
    for t in open_trades:
        c1m = candles_1m.get(t["symbol"], [])
        price = c1m[-1]["close"] if c1m else 0
        ep = t.get("entry_price") or 0
        pnl = (price - ep) / ep * 100 if ep > 0 else 0
        tp1 = "✓TP1" if t.get("tp1_hit") else ""
        open_lines.append(f"  {t['symbol']} | PnL: {pnl:+.2f}% {tp1}")
    open_str = "\n".join(open_lines) if open_lines else "  нет"

    text = (
        f"📈 S5 Live (continuation, sub-аккаунт)\n\n"
        f"Открытые позиции:\n{open_str}\n\n"
        f"Закрытые сделки: {stats['total']}\n"
        f"  Win rate: {stats['win_rate']:.0f}% ({stats['wins']}W / {stats['losses']}L)\n"
        f"  PnL итого: {stats['total_pnl_usdt']:+.2f} USDT"
    )
    await message.answer(text, reply_markup=get_main_keyboard())


class CheckLevel(StatesGroup):
    waiting_for_symbol = State()
    waiting_for_level = State()


class AddSymbol(StatesGroup):
    waiting_for_input = State()


class StopMonitor(StatesGroup):
    waiting_for_choice = State()


class AnalyzeSymbol(StatesGroup):
    waiting_for_choice = State()


def _tokens_inline_keyboard(prefix: str) -> InlineKeyboardMarkup | None:
    tokens = token_registry.get_all()
    if not tokens:
        return None
    buttons = []
    row = []
    for t in tokens:
        short = t.replace("USDT", "")
        row.append(InlineKeyboardButton(text=short, callback_data=f"{prefix}:{t}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.message(F.text == "➕ Добавить")
async def btn_add_symbol(message: Message, state: FSMContext):
    if not _authorized(message):
        return
    await message.answer("Введи символ монеты (например BTCUSDT):", reply_markup=get_main_keyboard())
    await state.set_state(AddSymbol.waiting_for_input)


@router.message(AddSymbol.waiting_for_input)
async def btn_add_symbol_input(message: Message, state: FSMContext):
    if not _authorized(message):
        return
    await state.clear()
    symbol = normalize_symbol(message.text.strip())
    if token_registry.contains(symbol):
        await message.answer(f"{symbol} уже в списке", reply_markup=get_main_keyboard())
        return
    token_registry.add(symbol)
    from data.history import log_event
    await log_event(symbol, "added_manual")
    await message.answer(f"✅ {symbol} добавлен — загружаю данные...", reply_markup=get_main_keyboard())
    try:
        from binance import AsyncClient
        from data.collector import _parse_kline, candles_1m, candles_15m
        client = await AsyncClient.create()
        try:
            raw_15m = await client.futures_klines(symbol=symbol, interval="15m", limit=500)
            raw_1m  = await client.futures_klines(symbol=symbol, interval="1m",  limit=300)
            candles_15m[symbol] = [_parse_kline(k) for k in raw_15m]
            candles_1m[symbol]  = [_parse_kline(k) for k in raw_1m]
        finally:
            await client.close_connection()
        await message.answer(f"✅ {symbol} готов к анализу", reply_markup=get_main_keyboard())
    except Exception as e:
        logger.exception("Failed to load candles for added symbol", symbol=symbol, error=str(e))
        await message.answer(f"⚠️ {symbol} добавлен, но данные не загрузились: {e}", reply_markup=get_main_keyboard())


@router.message(F.text == "📜 История")
async def btn_history(message: Message, state: FSMContext):
    if not _authorized(message):
        return
    kb = _tokens_inline_keyboard("history")
    if not kb:
        await message.answer("Список пуст", reply_markup=get_main_keyboard())
        return
    await message.answer("Выбери монету для просмотра истории:", reply_markup=kb)


@router.callback_query(F.data.startswith("history:"))
async def cb_history(callback: CallbackQuery):
    if not _authorized_cb(callback):
        return
    symbol = callback.data.split(":", 1)[1]
    await callback.answer()

    from data.history import get_symbol_history
    events = await get_symbol_history(symbol, limit=20)

    if not events:
        await callback.message.edit_text(f"История {symbol} пуста")
        return

    _icons = {
        "added_screener": "🆕",
        "added_manual": "➕",
        "levels_built": "📐",
        "monitoring_start": "👁",
        "bounce": "✅",
        "breakout": "💥",
        "removed": "🗑",
    }

    lines = [f"📜 История {symbol}\n"]
    for ev in reversed(events):
        icon = _icons.get(ev["event_type"], "•")
        ts = ev["created_at"][:16].replace("T", " ")
        details = ev["details"] or ""
        # Shorten levels_built JSON
        if ev["event_type"] == "levels_built":
            try:
                import json as _json
                lvls = _json.loads(details)
                details = ", ".join(f"{l['level']}({'⭐'*l['strength']})" for l in lvls)
            except Exception:
                pass
        lines.append(f"{icon} {ts}  {details}" if details else f"{icon} {ts}")

    await callback.message.edit_text("\n".join(lines))


@router.message(F.text == "➖ Убрать")
async def btn_remove(message: Message, state: FSMContext):
    if not _authorized(message):
        return
    kb = _tokens_inline_keyboard("remove")
    if not kb:
        await message.answer("Нет активных монет. Сначала добавь монету через ➕ Добавить", reply_markup=get_main_keyboard())
        return
    await message.answer("Выбери монету для удаления:", reply_markup=kb)


@router.callback_query(F.data.startswith("remove:"))
async def cb_remove(callback: CallbackQuery):
    if not _authorized_cb(callback):
        return
    symbol = callback.data.split(":", 1)[1]
    if not token_registry.contains(symbol):
        await callback.answer(f"{symbol} не найден")
        return
    token_registry.remove(symbol)
    from data.collector import candles_1m as c1m_data, candles_15m as c15m_data
    from main import cancel_tasks_for_symbol, clear_analysis_cache
    from models import state_manager
    c1m_data.pop(symbol, None)
    c15m_data.pop(symbol, None)
    cancel_tasks_for_symbol(symbol)
    clear_analysis_cache(symbol)
    state_manager.get_state(symbol).phase = "idle"
    await callback.answer()
    await callback.message.edit_text(f"🛑 {symbol} удалён")


@router.message(F.text == "📋 Список")
async def btn_list(message: Message):
    if not _authorized(message):
        return
    tokens = token_registry.get_all()
    if not tokens:
        await message.answer("Список пуст", reply_markup=get_main_keyboard())
        return
    kb = _build_analyze_keyboard(tokens)
    await message.answer(
        "Активные монеты — нажми для анализа:",
        reply_markup=kb
    )


@router.message(F.text == "👁 Мониторинги")
async def btn_monitors(message: Message):
    if not _authorized(message):
        return
    from models import state_manager, SymbolState
    from data.collector import candles_1m

    all_tasks = state_manager.get_all_active_tasks()

    if not all_tasks:
        await message.answer("Нет активных мониторингов", reply_markup=get_main_keyboard())
        return

    lines = []
    for task_key in all_tasks:
        parsed = SymbolState.parse_task_key(task_key)
        if parsed is None:
            continue
        sym, level = parsed
        c1m = candles_1m.get(sym, [])
        if not c1m:
            lines.append(f"  {sym} @ {level} — нет данных")
            continue
        current_price = c1m[-1]["close"]
        if level != 0:
            distance_pct = (current_price - level) / level * 100
            strength = state_manager.get_state(sym).level_strengths.get(task_key, 0)
            if strength < 3:
                continue
            stars = "⭐️" * strength
            lines.append(f"  {sym} @ {level} {stars} — цена {current_price} ({distance_pct:+.2f}%)")
        else:
            lines.append(f"  {sym} @ {level} — цена {current_price}")

    if not lines:
        await message.answer("Нет активных мониторингов", reply_markup=get_main_keyboard())
        return

    await message.answer("👁 Активные мониторинги:\n" + "\n".join(lines), reply_markup=get_main_keyboard())


@router.message(F.text == "🔍 Проверить уровень")
async def btn_check(message: Message, state: FSMContext):
    if not _authorized(message):
        return
    kb = _tokens_inline_keyboard("check")
    if not kb:
        await message.answer("Нет активных монет. Сначала добавь монету через ➕ Добавить", reply_markup=get_main_keyboard())
        return
    await message.answer("Выбери монету:", reply_markup=kb)


@router.callback_query(F.data.startswith("check:"))
async def cb_check_symbol(callback: CallbackQuery, state: FSMContext):
    if not _authorized_cb(callback):
        return
    symbol = callback.data.split(":", 1)[1]
    await state.update_data(symbol=symbol)
    await callback.answer()

    # Show levels from last analysis if available
    cached = _last_analysis_cache.get(symbol, [])
    if cached:
        buttons = []
        row = []
        for lvl in cached:
            stars = "⭐" * lvl["strength"]
            label = f"{stars} {lvl['level']}"
            row.append(InlineKeyboardButton(
                text=label,
                callback_data=f"checklvl:{symbol}:{lvl['level']}"
            ))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        # Add manual input button
        buttons.append([InlineKeyboardButton(text="✏️ Ввести вручную", callback_data=f"checkmanual:{symbol}")])
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.edit_text(
            f"Выбери уровень {symbol} из последнего анализа или введи вручную:",
            reply_markup=kb
        )
    else:
        # No cache - go straight to manual input
        await state.set_state(CheckLevel.waiting_for_level)
        await callback.message.edit_text(f"Введи уровень для {symbol} (например: 2.65):")


@router.callback_query(F.data.startswith("checklvl:"))
async def cb_check_level_from_cache(callback: CallbackQuery, state: FSMContext):
    if not _authorized_cb(callback):
        return
    _, symbol, level_str = callback.data.split(":", 2)
    await state.clear()
    await callback.answer()
    await callback.message.edit_text(f"Проверяю {symbol} @ {level_str}...")
    await _do_check(callback.message, symbol, float(level_str))


@router.callback_query(F.data.startswith("checkmanual:"))
async def cb_check_manual(callback: CallbackQuery, state: FSMContext):
    if not _authorized_cb(callback):
        return
    symbol = callback.data.split(":", 1)[1]
    await state.update_data(symbol=symbol)
    await state.set_state(CheckLevel.waiting_for_level)
    await callback.answer()
    await callback.message.edit_text(f"Введи уровень для {symbol} (например: 2.65):")


@router.message(CheckLevel.waiting_for_level)
async def btn_check_level(message: Message, state: FSMContext):
    if not _authorized(message):
        return
    data = await state.get_data()
    await state.clear()
    symbol = data["symbol"]
    try:
        level = float(message.text.strip())
    except ValueError:
        await message.answer("Уровень должен быть числом", reply_markup=get_main_keyboard())
        return
    if level <= 0:
        await message.answer("Уровень должен быть положительным числом", reply_markup=get_main_keyboard())
        return
    await _do_check(message, symbol, level)


@router.message(F.text == "📊 S2 статистика")
async def btn_s2_stats(message: Message):
    if not _authorized(message):
        return
    from trading.live_trade_log import get_live_trade_stats, get_open_live_trades
    from data.collector import candles_1m
    try:
        stats = await get_live_trade_stats()
        open_trades = await get_open_live_trades()
    except Exception as e:
        await message.answer(f"Ошибка получения S2-статистики: {e}",
                             reply_markup=get_main_keyboard())
        return

    open_lines = []
    for t in open_trades:
        c1m = candles_1m.get(t["symbol"], [])
        price = c1m[-1]["close"] if c1m else 0
        ep = t.get("entry_price") or 0
        pnl = (price - ep) / ep * 100 if ep > 0 else 0
        fills = t.get("grid_fill_count") or 0
        open_lines.append(f"  {t['symbol']} | fills={fills}/2 | PnL: {pnl:+.2f}%")
    open_str = "\n".join(open_lines) if open_lines else "  нет"

    text = (
        f"📊 S2 Live (grid, Bybit Demo)\n\n"
        f"Открытые позиции:\n{open_str}\n\n"
        f"Закрытые сделки: {stats['total']}\n"
        f"  Win rate: {stats['win_rate']:.0f}% ({stats['wins']}W / {stats['losses']}L)\n"
        f"  PnL итого: {stats['total_pnl_usdt']:+.2f} USDT\n"
        f"  Avg время: {stats['avg_duration_minutes']:.0f} мин"
    )
    await message.answer(text, reply_markup=get_main_keyboard())


@router.message(F.text == "🛑 Стоп")
async def btn_stop(message: Message, state: FSMContext):
    if not _authorized(message):
        return
    from models import state_manager

    from models import SymbolState
    all_tasks = state_manager.get_all_active_tasks()
    monitored_symbols = set()
    for k in all_tasks:
        parsed = SymbolState.parse_task_key(k)
        if parsed is not None:
            monitored_symbols.add(parsed[0])

    if not monitored_symbols:
        await message.answer("Нет активных мониторингов", reply_markup=get_main_keyboard())
        return

    buttons = []
    row = []
    for sym in sorted(monitored_symbols):
        short = sym.replace("USDT", "")
        row.append(InlineKeyboardButton(text=short, callback_data=f"stop:{sym}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="Остановить все", callback_data="stop:__all__")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer("Выбери монету для остановки:", reply_markup=kb)


@router.callback_query(F.data.startswith("stop:"))
async def cb_stop(callback: CallbackQuery):
    if not _authorized_cb(callback):
        return
    from models import state_manager
    from main import cancel_tasks_for_symbol, clear_analysis_cache

    target = callback.data.split(":", 1)[1]
    if target == "__all__":
        from models import SymbolState as _SS
        symbols = set()
        for k in state_manager.get_all_active_tasks():
            parsed = _SS.parse_task_key(k)
            if parsed is not None:
                symbols.add(parsed[0])
        for sym in symbols:
            cancel_tasks_for_symbol(sym)
            clear_analysis_cache(sym)
            state_manager.get_state(sym).phase = "idle"
        await callback.answer()
        await callback.message.edit_text("🛑 Все мониторинги остановлены")
    else:
        symbol = target
        cancel_tasks_for_symbol(symbol)
        clear_analysis_cache(symbol)
        state_manager.get_state(symbol).phase = "idle"
        await callback.answer()
        await callback.message.edit_text(f"🛑 Мониторинг {symbol} остановлен")


@router.message(F.text == "📊 Анализ")
async def btn_analyze(message: Message, state: FSMContext):
    if not _authorized(message):
        return
    kb = _tokens_inline_keyboard("analyze")
    if not kb:
        await message.answer("Нет активных монет. Сначала добавь монету через ➕ Добавить", reply_markup=get_main_keyboard())
        return
    await message.answer("Выбери монету для анализа:", reply_markup=kb)


def _build_analyze_keyboard(symbols: list[str]) -> InlineKeyboardMarkup:
    """Build inline keyboard with symbol buttons for quick analysis."""
    buttons = []
    row = []
    for sym in symbols:
        short = sym.replace("USDT", "")
        row.append(InlineKeyboardButton(text=f"📊 {short}", callback_data=f"analyze:{sym}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def send_screener_with_buttons(text: str, rows: list[tuple]):
    """Send screener message with inline analyze buttons."""
    symbols = [sym for _, _, _, _, sym in rows]
    kb = _build_analyze_keyboard(symbols)
    try:
        await bot.send_message(
            TELEGRAM_CHAT_ID, text,
            parse_mode="Markdown",
            reply_markup=kb
        )
    except Exception:
        logger.exception("Failed to send screener with buttons")


@router.message(F.text == "📊 Рынок")
async def btn_market(message: Message):
    """Show market screener with inline analyze buttons."""
    if not _authorized(message):
        return

    await message.answer("Сканирую рынок...", reply_markup=get_main_keyboard())

    try:
        from datetime import datetime, timezone
        from analysis.screener import run_screener, _format_vol

        rows = await run_screener()
        if not rows:
            await message.answer("Нет монет, прошедших фильтр", reply_markup=get_main_keyboard())
            return

        now_str = datetime.now(timezone.utc).strftime("%d.%m %H:%M")
        lines = [f"📊 Рынок  {now_str} UTC\n"]
        lines.append(f"{'TICKER':<10} {'CHG%':>6}  {'NATR':>4}  {'VOL':>6}")
        lines.append("─" * 34)
        for ticker, chg, natr, vol, _ in rows:
            lines.append(f"{ticker:<10} {chg:>+5.1f}  {natr:>4.1f}  {_format_vol(vol):>6}")

        symbols = [sym for _, _, _, _, sym in rows]
        kb = _build_analyze_keyboard(symbols)
        await message.answer("```\n" + "\n".join(lines) + "\n```",
                             parse_mode="Markdown", reply_markup=kb)
        logger.info("Market screener sent", symbols_count=len(rows))

    except Exception as e:
        logger.exception("Error in market screener", error=str(e))
        await message.answer("Ошибка при сканировании рынка", reply_markup=get_main_keyboard())


_analyzing: set[str] = set()  # prevent double analysis


@router.callback_query(F.data.startswith("analyze:"))
async def cb_analyze(callback: CallbackQuery):
    if not _authorized_cb(callback):
        return
    symbol = callback.data.split(":", 1)[1]
    if symbol in _analyzing:
        await callback.answer("Анализ уже выполняется...")
        return
    _analyzing.add(symbol)
    await callback.answer()
    await callback.message.edit_text(f"Анализирую {symbol}...")
    try:
        await _do_analyze(callback.message, symbol)
    finally:
        _analyzing.discard(symbol)


@router.message(Command("add"))
async def cmd_add(message: Message):
    """Hidden debug command to manually add a symbol."""
    if not _authorized(message):
        return
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /add SYMBOL", reply_markup=get_main_keyboard())
        return
    symbol = normalize_symbol(args[1])
    if token_registry.contains(symbol):
        await message.answer(f"{symbol} уже в списке", reply_markup=get_main_keyboard())
        return
    token_registry.add(symbol)
    from data.history import log_event
    await log_event(symbol, "added_manual")

    # Immediately load candle data so the symbol is ready for analysis
    await message.answer(f"✅ {symbol} добавлен — загружаю данные...", reply_markup=get_main_keyboard())
    try:
        from binance import AsyncClient
        from data.collector import _parse_kline, candles_1m, candles_15m
        client = await AsyncClient.create()
        try:
            raw_15m = await client.futures_klines(symbol=symbol, interval="15m", limit=500)
            raw_1m  = await client.futures_klines(symbol=symbol, interval="1m",  limit=300)
            candles_15m[symbol] = [_parse_kline(k) for k in raw_15m]
            candles_1m[symbol]  = [_parse_kline(k) for k in raw_1m]
        finally:
            await client.close_connection()
        await message.answer(f"✅ {symbol} готов к анализу", reply_markup=get_main_keyboard())
    except Exception as e:
        logger.exception("Failed to load candles for added symbol", symbol=symbol, error=str(e))
        await message.answer(f"⚠️ {symbol} добавлен, но данные не загрузились: {e}", reply_markup=get_main_keyboard())


@router.message(Command("remove"))
async def cmd_remove(message: Message):
    if not _authorized(message):
        return
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /remove SYMBOL", reply_markup=get_main_keyboard())
        return
    symbol = normalize_symbol(args[1])
    if not token_registry.contains(symbol):
        await message.answer(f"{symbol} не найден в списке", reply_markup=get_main_keyboard())
        return
    token_registry.remove(symbol)
    from data.collector import candles_1m as c1m_data, candles_15m as c15m_data
    from main import cancel_tasks_for_symbol, clear_analysis_cache
    from models import state_manager
    c1m_data.pop(symbol, None)
    c15m_data.pop(symbol, None)
    cancel_tasks_for_symbol(symbol)
    clear_analysis_cache(symbol)
    state_manager.get_state(symbol).phase = "idle"
    await message.answer(f"🛑 {symbol} удалён", reply_markup=get_main_keyboard())


@router.message(Command("blacklist"))
async def cmd_blacklist(message: Message):
    if not _authorized(message):
        return
    args = message.text.split()
    if len(args) < 2:
        bl = blacklist.get_all()
        if bl:
            await message.answer("🚫 Блэклист:\n" + "\n".join(bl), reply_markup=get_main_keyboard())
        else:
            await message.answer("Блэклист пуст", reply_markup=get_main_keyboard())
        return
    symbol = normalize_symbol(args[1])
    blacklist.add(symbol)
    # Удаляем из token_registry чтобы скринер не добавил снова
    token_registry.remove(symbol)
    # Если символ активно мониторится — останавливаем
    from main import cancel_tasks_for_symbol, clear_analysis_cache
    from models import state_manager
    cancel_tasks_for_symbol(symbol)
    clear_analysis_cache(symbol)
    state_manager.get_state(symbol).phase = "idle"
    await message.answer(f"🚫 {symbol} добавлен в блэклист и удалён из мониторинга", reply_markup=get_main_keyboard())


@router.message(Command("unblacklist"))
async def cmd_unblacklist(message: Message):
    if not _authorized(message):
        return
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /unblacklist SYMBOL", reply_markup=get_main_keyboard())
        return
    symbol = normalize_symbol(args[1])
    blacklist.remove(symbol)
    await message.answer(f"✅ {symbol} убран из блэклиста", reply_markup=get_main_keyboard())


@router.message(Command("list"))
async def cmd_list(message: Message):
    if not _authorized(message):
        return
    tokens = token_registry.get_all()
    if not tokens:
        await message.answer("Список пуст", reply_markup=get_main_keyboard())
        return
    await message.answer("Активные монеты:\n" + "\n".join(tokens), reply_markup=get_main_keyboard())


@router.message(Command("export_db"))
async def cmd_export_db(message: Message):
    """Send history.db file to Telegram."""
    if not _authorized(message):
        return
    import os
    from aiogram.types import BufferedInputFile
    from config import HISTORY_DB_FILE

    # Try volume path first, then local
    db_path = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "")
    if db_path:
        db_path = os.path.join(db_path, "history.db")
    else:
        db_path = HISTORY_DB_FILE

    if not os.path.exists(db_path):
        await message.answer("❌ history.db не найден", reply_markup=get_main_keyboard())
        return

    try:
        with open(db_path, "rb") as f:
            data = f.read()
        size_kb = len(data) / 1024
        await message.answer_document(
            BufferedInputFile(data, filename="history.db"),
            caption=f"📦 history.db ({size_kb:.1f} KB)"
        )
    except Exception as e:
        logger.exception("Failed to export db", error=str(e))
        await message.answer(f"❌ Ошибка: {e}", reply_markup=get_main_keyboard())


@router.message(Command("analyze"))
async def cmd_analyze(message: Message):
    if not _authorized(message):
        return
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /analyze SYMBOL", reply_markup=get_main_keyboard())
        return
    symbol = normalize_symbol(args[1])
    await _do_analyze(message, symbol)


async def _do_analyze(message: Message, symbol: str):
    from data.collector import candles_1m, candles_15m, _parse_kline
    from analysis.level_builder import build_levels
    from analysis.trigger import calculate_atr, _count_approaches, calculate_strength
    from binance import AsyncClient

    c1m = candles_1m.get(symbol)
    if not c1m:
        # Load on demand — don't make user wait and retry
        loading_msg = await message.answer(f"⏳ Загружаю данные {symbol}...", reply_markup=get_main_keyboard())
        try:
            client = await AsyncClient.create()
            try:
                raw_15m = await client.futures_klines(symbol=symbol, interval="15m", limit=500)
                raw_1m  = await client.futures_klines(symbol=symbol, interval="1m",  limit=300)
                candles_15m[symbol] = [_parse_kline(k) for k in raw_15m]
                candles_1m[symbol]  = [_parse_kline(k) for k in raw_1m]
                c1m = candles_1m[symbol]
                logger.info("Candles loaded on-demand for analyze", symbol=symbol)
            finally:
                await client.close_connection()
        except Exception as e:
            await loading_msg.delete()
            await message.answer(f"{symbol} — не удалось загрузить данные: {e}", reply_markup=get_main_keyboard())
            return
        await loading_msg.delete()

    current_price = c1m[-1]["close"]
    atr = calculate_atr(symbol)
    if atr == 0:
        await message.answer(f"{symbol} — недостаточно данных для ATR", reply_markup=get_main_keyboard())
        return

    # Подгружаем расширенную историю для покрытия диапазона 40%
    try:
        client = await AsyncClient.create()
        try:
            raw_1m = await client.futures_klines(symbol=symbol, interval="1m", limit=1500)
            raw_15m = await client.futures_klines(symbol=symbol, interval="15m", limit=1000)
            ext_c1m = [_parse_kline(k) for k in raw_1m]
            ext_c15m = [_parse_kline(k) for k in raw_15m]
        finally:
            await client.close_connection()
    except Exception:
        ext_c1m = None
        ext_c15m = None

    all_levels = build_levels(symbol, c1m_override=ext_c1m, c15m_override=ext_c15m)
    if not all_levels:
        await message.answer(f"{symbol} — уровни не найдены", reply_markup=get_main_keyboard())
        return

    range_limit = current_price * 0.40
    supports = [
        lvl for lvl in all_levels
        if lvl["level"] < current_price and (current_price - lvl["level"]) <= range_limit
    ]
    if not supports:
        broken_levels = [lvl for lvl in all_levels if lvl["level"] > current_price]
        for lvl in broken_levels:
            lvl["symbol"] = symbol
            lvl["approach"] = _count_approaches(symbol, lvl["level"], atr)[0] if atr > 0 else 0
            # считаем strength без штрафа за пробой — уровень УЖЕ пробит, это ожидаемо
            lvl_copy = dict(lvl)
            lvl_copy["was_broken"] = False
            lvl_copy["sweep_reclaimed"] = False
            calculate_strength(lvl_copy)
            lvl["strength"] = lvl_copy["strength"]
            lvl["verdict"] = lvl_copy["verdict"]
        broken_strong = [lvl for lvl in broken_levels if lvl["strength"] >= 4]

        text = f"{symbol} — нет поддержек в диапазоне 40% от цены"
        if broken_strong:
            from analysis.trigger import get_breakout_info
            text += "\n\nПробитые уровни:"
            for lvl in broken_strong:
                info = get_breakout_info(symbol, lvl["level"])
                lvl["breakout_type"] = info["type"]
                count = lvl.get("candle_count", "?")
                type_desc = "тела 15М" if lvl["type"] == "body_level" else "фитили 15М"
                if info["type"] == "zakol":
                    text += f"\n⬆️ {lvl['level']} — {count} {type_desc}, заколот (закол -{info['zakol_pct']}%, отскок +{info['rebound_pct']}%)"
                else:
                    text += f"\n🔴 {lvl['level']} — {count} {type_desc}, пробит без отскока"

        await message.answer(text, reply_markup=get_main_keyboard())
        try:
            from analysis.chart import generate_chart
            from aiogram.types import BufferedInputFile
            chart_bytes = generate_chart(symbol, [], broken_levels=broken_strong if broken_strong else None)
            if chart_bytes:
                await message.answer_photo(BufferedInputFile(chart_bytes, filename=f"{symbol}.png"))
        except Exception:
            pass
        return
    min_distance = atr * 1.5

    # Collect levels that are actively monitored for this symbol — they must
    # always appear in the analysis output regardless of distance to price.
    from models import state_manager as _sm_analyze, SymbolState as _SS_analyze
    _sym_state_analyze = _sm_analyze.get_state(symbol)
    monitored_levels_set: set[float] = set()
    for _tk in _sym_state_analyze.tasks:
        _p = _SS_analyze.parse_task_key(_tk)
        if _p is not None:
            monitored_levels_set.add(_p[1])

    # Include a level if it passes the min_distance filter OR is actively monitored.
    filtered = [
        lvl for lvl in supports
        if (current_price - lvl["level"]) >= min_distance
        or lvl["level"] in monitored_levels_set
    ]

    if not filtered:
        atr_pct = (atr / current_price) * 100
        await message.answer(
            f"🔍 {symbol} — уровней в рабочей зоне нет\n"
            f"   Цена: {current_price} | ATR: {atr_pct:.2f}%\n"
            f"   Ближайшие уровни слишком близко к цене (< 1.5 ATR)\n"
            f"   Жди отхода цены или нового пампа",
            reply_markup=get_main_keyboard(),
        )
        return

    for lvl in filtered:
        lvl["symbol"] = symbol
        lvl["approach"] = _count_approaches(symbol, lvl["level"], atr)[0] if atr > 0 else 1

    # Python pre-calculates strength and history — Claude uses this as context
    from analysis.trigger import get_level_history
    for lvl in filtered:
        if atr > 0:
            lvl.update(get_level_history(symbol, lvl["level"], atr))
        calculate_strength(lvl)
        lvl["python_strength"] = lvl["strength"]  # save for cap enforcement

    # Use Claude for strength calculation if enabled
    from constants import CLAUDE_STRENGTH_ENABLED
    
    if CLAUDE_STRENGTH_ENABLED:
        try:
            # Extract POC from levels
            poc_price = None
            for lvl in filtered:
                if lvl.get("poc_aligned"):
                    poc_price = lvl["level"]
                    break
            
            # Import and use Claude
            from analysis.claude_strength import calculate_strength_with_claude
            
            filtered = await calculate_strength_with_claude(symbol, ext_c15m if ext_c15m else [], filtered, poc_price)

            # Cap: Claude cannot exceed Python score when approach>=2 or broken
            for lvl in filtered:
                py = lvl.get("python_strength", lvl["strength"])
                approach = lvl.get("approach", 0)
                was_broken = lvl.get("was_broken", False)
                sweep = lvl.get("sweep_reclaimed", False)
                if approach >= 2 or (was_broken and not sweep):
                    lvl["strength"] = min(lvl["strength"], py)

            logger.info("Claude strength calculation completed for analyze",
                       symbol=symbol,
                       levels_count=len(filtered))
        except Exception as e:
            logger.error("Failed to use Claude for analyze, falling back to Python",
                        symbol=symbol,
                        error=str(e))
            # Fallback to Python
            for lvl in filtered:
                calculate_strength(lvl)
    else:
        # Use Python calculation
        for lvl in filtered:
            calculate_strength(lvl)

    # ML scoring
    # 1.4: approach_style должен быть выставлен ДО вызова ML
    # 1.3: impulse + strength=0 → 0% bounce, блокируем без ML
    try:
        from analysis.ml_score import apply_ml_to_level
        from analysis.trigger import detect_approach_style as _das
        _approach_style_analyze = _das(symbol)
        for lvl in filtered:
            lvl["approach_style"] = _approach_style_analyze
            # 1.3: hard-block impulse + strength=0
            if lvl.get("approach_style") == "impulse" and lvl.get("strength", 0) == 0:
                lvl["p_bounce"] = 0.0
                lvl["ml_delta"] = -2
                lvl["ml_blocked"] = True
                lvl["strength_pre_ml"] = lvl.get("strength", 0)
                logger.debug("blocked impulse+strength=0 in analyze: %s @ %s", symbol, lvl.get("level"))
                continue
            apply_ml_to_level(lvl)
    except Exception as e:
        logger.warning("ml_score failed in analyze: %s", e)

    # Исключаем ml_blocked уровни из strong/average
    strong = [lvl for lvl in filtered if lvl["strength"] >= 4 and not lvl.get("ml_blocked")]
    average = [lvl for lvl in filtered if lvl["strength"] == 3 and not lvl.get("ml_blocked")]
    weak = [lvl for lvl in filtered if lvl["strength"] < 3 or lvl.get("ml_blocked")]

    strong_sorted = sorted(strong, key=lambda l: l["strength"], reverse=True)
    average_sorted = sorted(average, key=lambda l: l["level"], reverse=True)

    atr_pct = (atr / current_price) * 100
    header = (
        f"🔍 {symbol} — поддержки в диапазоне 40%\n"
        f"   Цена: {current_price} | ATR: {atr_pct:.2f}%\n"
    )

    lines = []
    # Show Strong levels
    for lvl in strong_sorted:
        stars = "⭐️" * lvl["strength"]
        distance = current_price - lvl["level"]
        close_mark = "  (близко)" if distance < atr * 3 else ""
        lines.append(f"{stars} {lvl['level']} — {lvl['type']}, {lvl.get('position', '?')}, подход {lvl.get('approach', 1)}{close_mark}")
        if lvl.get("claude_reason"):
            lines.append(f"   💭 {lvl['claude_reason']}")
        p_b = lvl.get("p_bounce")
        e_d = lvl.get("expected_depth")
        if p_b is not None:
            depth_str = f" | прокол ~{e_d:.1f}%" if e_d is not None else ""
            ml_delta = lvl.get("ml_delta", 0)
            delta_str = f" | ML: {ml_delta:+d}" if ml_delta != 0 else ""
            lines.append(f"   🤖 P(отбой): {p_b:.0%}{depth_str}{delta_str}")

    # Show Average levels if there are no strong ones or just to provide more info
    if average_sorted:
        if lines:
            lines.append("\n⚖️ Средние уровни:")
        else:
            lines.append("⚖️ Средние уровни (4-5⭐️ не найдены):")
            
        for lvl in average_sorted:
            stars = "⭐️" * 3
            lines.append(f"{stars} {lvl['level']} — {lvl['type']}, подход {lvl.get('approach', 1)}")
            if lvl.get("claude_reason"):
                lines.append(f"   💭 {lvl['claude_reason']}")

    text = header + "\n".join(lines)

    if weak:
        weak_str = ", ".join(str(lvl["level"]) for lvl in weak)
        text += f"\n——————————\nСлабые (1-2⭐️): {weak_str}"

    # Save all levels (strong + weak) to cache for quick check access
    _last_analysis_cache[symbol] = [
        {
            "level": lvl["level"],
            "strength": lvl["strength"],
            "type": lvl["type"],
            "p_bounce": lvl.get("p_bounce", 0.0),
            "expected_depth": lvl.get("expected_depth", 0.0),
        }
        for lvl in sorted(filtered, key=lambda l: l["level"])
    ]

    await message.answer(text, reply_markup=get_main_keyboard())

    try:
        from analysis.chart import generate_chart
        from aiogram.types import BufferedInputFile
        # Pass both strong and average levels to be drawn on chart
        chart_levels = strong_sorted + average_sorted
        chart_bytes = generate_chart(symbol, chart_levels, c15m_override=ext_c15m)
        if chart_bytes:
            await message.answer_photo(BufferedInputFile(chart_bytes, filename=f"{symbol}.png"))
    except Exception:
        logger.exception("Failed to generate chart for %s", symbol)

    # Auto-start monitoring for the nearest strong level
    if strong:
        from models import state_manager, SymbolState
        from main import _monitored
        import asyncio

        sym_state = state_manager.get_state(symbol)

        # Find nearest strong level to current price
        nearest = min(strong, key=lambda l: abs(current_price - l["level"]))
        level_side = "support" if current_price > nearest["level"] else "resistance"
        
        # Check if we already have a monitor for this symbol near this price (within 1.5 ATR)
        already_monitored = False
        threshold = max(atr * 3.0, current_price * 0.03)
        for task_key in sym_state.tasks:
            parsed_tk = SymbolState.parse_task_key(task_key)
            if parsed_tk is None:
                continue
            if abs(parsed_tk[1] - nearest["level"]) <= threshold:
                already_monitored = True
                break

        if not already_monitored:
            task = asyncio.create_task(
                _monitored(symbol, nearest["level"], level_side,
                           level_type=nearest["type"],
                           strength=nearest["strength"],
                           p_bounce=nearest.get("p_bounce", 0.0),
                           expected_depth=nearest.get("expected_depth", 0.0))
            )
            sym_state.add_task(nearest["level"], task, strength=nearest.get("strength", 0))
            sym_state.phase = "phase2"
            await message.answer(
                f"👁 Мониторинг запущен: {symbol} @ {nearest['level']}",
                reply_markup=get_main_keyboard()
            )
            logger.info("Auto-monitoring started from analyze",
                       symbol=symbol, level=nearest["level"])
        else:
            # Find the actual level being monitored for the message
            m_level = nearest["level"]
            for task_key in sym_state.tasks:
                parsed_tk = SymbolState.parse_task_key(task_key)
                if parsed_tk is None:
                    continue
                if abs(parsed_tk[1] - nearest["level"]) <= threshold:
                    m_level = parsed_tk[1]
                    break
                
            await message.answer(
                f"👁 Мониторинг уже активен: {symbol} @ {m_level}",
                reply_markup=get_main_keyboard()
            )


@router.message(Command("check"))
async def cmd_check(message: Message):
    if not _authorized(message):
        return
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /check SYMBOL LEVEL\nПример: /check TSTUSDT 0.014007", reply_markup=get_main_keyboard())
        return
    symbol = normalize_symbol(args[1])
    try:
        level = float(args[2])
    except ValueError:
        await message.answer("Уровень должен быть числом", reply_markup=get_main_keyboard())
        return
    if level <= 0:
        await message.answer("Уровень должен быть положительным числом", reply_markup=get_main_keyboard())
        return
    await _do_check(message, symbol, level)


async def _do_check(message: Message, symbol: str, level: float):
    await message.answer(f"Оцениваю уровень {level} для {symbol}...")

    from analysis.trigger import calculate_atr, calculate_atr_pct, _calc_vol_ratio, _count_approaches, get_level_history, find_real_level, calculate_strength
    from analysis.level_builder import build_levels
    from data.collector import candles_1m, candles_15m, _parse_kline
    from ai.claude_client import analyze_levels

    c1m = candles_1m.get(symbol)
    if not c1m:
        try:
            from binance import AsyncClient
            client = await AsyncClient.create()
            try:
                raw_15m = await client.futures_klines(symbol=symbol, interval="15m", limit=500)
                raw_1m  = await client.futures_klines(symbol=symbol, interval="1m",  limit=300)
                candles_15m[symbol] = [_parse_kline(k) for k in raw_15m]
                candles_1m[symbol]  = [_parse_kline(k) for k in raw_1m]
                c1m = candles_1m[symbol]
            finally:
                await client.close_connection()
        except Exception as e:
            await message.answer(f"{symbol} — не удалось загрузить данные: {e}", reply_markup=get_main_keyboard())
            return

    original_level = level
    real_level, touch_count = find_real_level(symbol, level)
    level_adjusted = real_level != level
    if level_adjusted:
        level = real_level

    atr = calculate_atr(symbol)
    approach = _count_approaches(symbol, level, atr)[0] if atr > 0 else 1
    vol_ratio = _calc_vol_ratio(symbol)
    history = get_level_history(symbol, level, atr) if atr > 0 else {}

    current_price = c1m[-1]["close"]
    atr_pct = calculate_atr_pct(symbol)
    zone_radius = atr_pct / 100 * current_price if current_price > 0 else 0
    all_levels = build_levels(symbol)

    # Подсказка ближайшего уровня
    if all_levels and level != 0:
        nearest = min(all_levels, key=lambda l: abs(l["level"] - level))
        distance_pct = abs(nearest["level"] - level) / level * 100

        if distance_pct <= 2.0 and nearest["level"] != level:
            await message.answer(
                f"🔍 Ближайший уровень в данных: {nearest['level']}\n"
                f"   Оцениваю {nearest['level']}..."
            )
            level = nearest["level"]

    match = None
    for lvl in all_levels:
        if level != 0 and abs(lvl["level"] - level) / level <= 0.003:
            if match is None or abs(lvl["level"] - level) < abs(match["level"] - level):
                match = lvl

    if not match:
        match = {"type": "body_level", "position": "mid_move", "cluster": False, "pump_volume_ratio": 1.0, "level": level}

    nearby = [
        other for other in all_levels
        if abs(other["level"] - level) <= zone_radius and other["level"] != level
    ]
    zone_approaches = sum(
        _count_approaches(symbol, other["level"], atr)[0]
        for other in nearby
    ) if atr > 0 else 0

    lvl_data = {
        "symbol": symbol,
        "level": level,
        "type": match["type"],
        "position": match.get("position", "mid_move"),
        "approach": approach,
        "vol_ratio": vol_ratio,
        "engulf_15m": False,
        "cluster": match.get("cluster", False),
        "zone_approaches": zone_approaches,
        "atr_pct": round(atr_pct, 3),
        "pump_volume_ratio": match.get("pump_volume_ratio", 1.5),
        "candle_count": match.get("candle_count", touch_count),
        "poc_aligned": match.get("poc_aligned", False),
        "hourly_open_bonus": match.get("hourly_open_bonus", 0),
        "round_number_bonus": match.get("round_number_bonus", 0),
        **history,
    }

    # Python strength first (with all penalties)
    calculate_strength(lvl_data)
    lvl_data["python_strength"] = lvl_data["strength"]

    # Claude strength (same system as /analyze)
    from constants import CLAUDE_STRENGTH_ENABLED
    from analysis.claude_strength import calculate_strength_with_claude
    c15m_data = candles_15m.get(symbol, [])
    poc_price = next((l["level"] for l in all_levels if l.get("poc_aligned")), None)

    if CLAUDE_STRENGTH_ENABLED and c15m_data:
        try:
            result_levels = await calculate_strength_with_claude(symbol, c15m_data, [lvl_data], poc_price)
            lvl_data = result_levels[0]
            # Cap: Claude cannot exceed Python when approach>=2 or broken without reclaim
            py = lvl_data.get("python_strength", lvl_data["strength"])
            if approach >= 2 or (history.get("was_broken") and not history.get("sweep_reclaimed")):
                lvl_data["strength"] = min(lvl_data["strength"], py)
        except Exception as e:
            logger.error("Claude failed in check", error=str(e))

    # 1.4: approach_style выставляем ДО вызова ML (иначе ML всегда получает "unknown")
    from analysis.trigger import detect_approach_style, calculate_atr_ratio, get_vol_ratio_current
    p_approach_style = detect_approach_style(symbol)
    lvl_data["approach_style"] = p_approach_style

    # ML scoring
    try:
        from analysis.ml_score import apply_ml_to_level
        # 1.3: hard-block impulse + strength=0 → 0% bounce из данных
        if p_approach_style == "impulse" and lvl_data.get("strength", 0) == 0:
            lvl_data["p_bounce"] = 0.0
            lvl_data["ml_delta"] = -2
            lvl_data["ml_blocked"] = True
            lvl_data["strength_pre_ml"] = lvl_data.get("strength", 0)
            logger.debug("blocked impulse+strength=0 in check: %s @ %s", symbol, level)
        else:
            apply_ml_to_level(lvl_data)
    except Exception as e:
        logger.warning("ml_score failed in check: %s", e)

    # Get profile context for reason/grid_advice/confidence
    from data.history import get_outcome_probs

    # p_approach_style уже вычислен выше (перед ML scoring)
    p_atr_ratio = calculate_atr_ratio(symbol, level)
    p_vol_ratio = get_vol_ratio_current(symbol)
    outcome_probs = await get_outcome_probs(
        symbol=symbol,
        level_type=match["type"],
        approach_style=p_approach_style,
    )

    # Old Claude for reason + grid_advice + confidence
    from main import claude_semaphore
    async with claude_semaphore:
        results = await analyze_levels(
            [lvl_data],
            outcome_probs=outcome_probs,
            approach_style=p_approach_style,
            atr_ratio=p_atr_ratio,
            vol_ratio=p_vol_ratio,
        )

    r = results[0] if results else lvl_data
    stars = "⭐️" * r.get("strength", 0)
    header = f"📊 {symbol} @ {original_level}"
    if level_adjusted:
        header += (
            f" (твой уровень)\n"
            f"   🔍 Уточнение: реальный кластер на {level}\n"
            f"      ({touch_count} касаний 15М)"
        )
    elif touch_count == 0:
        header += f"\n   🔍 Кластер касаний не найден — оцениваю как указано"

    p_bounce = r.get("p_bounce")
    expected_depth = r.get("expected_depth")
    ml_line = ""
    if p_bounce is not None:
        depth_str = f" | прокол ~{expected_depth:.1f}%" if expected_depth is not None else ""
        ml_delta = r.get("ml_delta", 0)
        delta_str = f" | ML: {ml_delta:+d}" if ml_delta != 0 else ""
        ml_line = f"\n   🤖 P(отбой): {p_bounce:.0%}{depth_str}{delta_str}"

    text = (
        f"{header}\n"
        f"   Сила: {r.get('strength', '?')} {stars}\n"
        f"   Вердикт: {r.get('verdict', '?')}\n"
        f"   Причина: {r.get('reason', '?')}\n"
        f"   Подход: {approach} | Vol ratio: {vol_ratio}"
        f"{ml_line}"
    )
    if zone_approaches >= 1:
        text += f"\n   Зона: {zone_approaches} подходов в радиусе {atr_pct:.2f}%"

    # Statistics block
    ss = outcome_probs.get("sample_size", 0) if outcome_probs else 0
    if ss >= 10:
        text += (
            f"\n\n📊 Статистика по токену ({ss} наблюдений):\n"
            f"├ Не дошла: {outcome_probs['no_reach'] * 100:.0f}%\n"
            f"├ Частичная: {outcome_probs['partial'] * 100:.0f}%\n"
            f"├ Отскок: {outcome_probs['bounce'] * 100:.0f}%\n"
            f"└ Пробой: {outcome_probs['breakout'] * 100:.0f}%"
        )
    else:
        text += f"\n\n📊 Статистики пока нет (<10 наблюдений)"

    grid_advice = r.get("grid_advice", "normal")
    confidence = r.get("confidence", 0.0)
    text += f"\n\n📐 Сетка: {grid_advice}\n🎯 Уверенность Claude: {confidence * 100:.0f}%"

    await message.answer(text, reply_markup=get_main_keyboard())

    if r.get("strength", 0) >= 3:
        from models import state_manager
        from main import _monitored
        current_price = c1m[-1]["close"]
        level_side = "support" if current_price > level else "resistance"
        sym_state = state_manager.get_state(symbol)
        
        # Check if we already have a monitor for this symbol near this price (within 1.5 ATR)
        from models import SymbolState as _SS_check
        already_monitored = False
        threshold = max(atr * 3.0, current_price * 0.03)
        for task_key in sym_state.tasks:
            parsed_tk = _SS_check.parse_task_key(task_key)
            if parsed_tk is None:
                continue
            if abs(parsed_tk[1] - level) <= threshold:
                already_monitored = True
                break

        if already_monitored:
            # Find the actual level being monitored for the message
            m_level = level
            for task_key in sym_state.tasks:
                parsed_tk = _SS_check.parse_task_key(task_key)
                if parsed_tk is None:
                    continue
                if abs(parsed_tk[1] - level) <= threshold:
                    m_level = parsed_tk[1]
                    break
            await message.answer(f"⚠️ Мониторинг {symbol} @ {m_level} уже активен")
        else:
            task = asyncio.create_task(
                _monitored(symbol, level, level_side,
                           level_type=match["type"],
                           strength=r.get("strength", 0),
                           p_bounce=r.get("p_bounce", 0.0),
                           expected_depth=r.get("expected_depth", 0.0))
            )
            sym_state.add_task(level, task, strength=r.get("strength", 0))
            sym_state.phase = "phase2"
            await message.answer(
                f"👁 Мониторинг запущен: {symbol} @ {level}",
                reply_markup=get_main_keyboard()
            )
            logger.info("Monitoring started from check",
                       symbol=symbol, level=level)


@router.message(Command("monitors"))
async def cmd_monitors(message: Message):
    if not _authorized(message):
        return
    from models import state_manager
    from data.collector import candles_1m

    all_tasks = state_manager.get_all_active_tasks()
    if not all_tasks:
        await message.answer("Нет активных мониторингов", reply_markup=get_main_keyboard())
        return

    from models import SymbolState as _SS2
    lines = []
    for task_key in all_tasks:
        parsed = _SS2.parse_task_key(task_key)
        if parsed is None:
            continue
        symbol, level = parsed
        c1m = candles_1m.get(symbol, [])
        if not c1m:
            lines.append(f"  {symbol} @ {level} — нет данных")
            continue
        current_price = c1m[-1]["close"]
        if level != 0:
            distance_pct = (current_price - level) / level * 100
            lines.append(f"  {symbol} @ {level} — цена {current_price} ({distance_pct:+.2f}%)")
        else:
            lines.append(f"  {symbol} @ {level} — цена {current_price}")

    await message.answer("👁 Активные мониторинги:\n" + "\n".join(lines), reply_markup=get_main_keyboard())


@router.message(Command("stop"))
async def cmd_stop(message: Message):
    if not _authorized(message):
        return
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /stop SYMBOL", reply_markup=get_main_keyboard())
        return
    symbol = normalize_symbol(args[1])

    from models import state_manager
    from main import cancel_tasks_for_symbol, clear_analysis_cache

    sym_state = state_manager.get_state(symbol)
    if not sym_state.has_active_tasks():
        await message.answer(f"⚠️ Нет активных мониторингов для {symbol}", reply_markup=get_main_keyboard())
        return

    cancel_tasks_for_symbol(symbol)
    clear_analysis_cache(symbol)
    sym_state.phase = "idle"
    await message.answer(f"🛑 Мониторинг {symbol} остановлен", reply_markup=get_main_keyboard())


@router.message(Command("restart"))
async def cmd_restart(message: Message):
    if not _authorized(message):
        return
    import os
    import sys

    await message.answer("🔄 Перезапуск бота...")
    logger.info("Restart requested via /restart")
    os.execv(sys.executable, [sys.executable] + sys.argv)


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    """Show paper trading statistics for all three strategies."""
    if not _authorized(message):
        return

    from trading.trade_log import get_trade_stats

    lines = ["📊 Статистика стратегий\n"]
    strategy_labels = [
        (1, "S1 Bounce"),
        (2, "S2 Grid"),
        (3, "S3 Breakout"),
        (4, "S4 Breakout Long"),
    ]

    for strategy_id, label in strategy_labels:
        try:
            s = await get_trade_stats(strategy_id)
        except Exception as e:
            lines.append(f"[{label}]\n  Ошибка: {e}\n")
            continue

        if s["total"] == 0:
            lines.append(f"[{label}]\n  Сделок нет\n")
            continue

        lines.append(
            f"[{label}]\n"
            f"  Сделок: {s['total']} ({s['total_with_fills']}) | Win rate: {s['win_rate']:.0f}%"
            f" | PnL: {s['total_pnl_usdt']:+.2f} USDT\n"
            f"  Avg win: {s['avg_win_pct']:+.1f}%"
            f" | Avg loss: {s['avg_loss_pct']:+.1f}%"
            f" | Avg time: {s['avg_duration_minutes']:.0f} мин\n"
        )

    await message.answer("\n".join(lines), reply_markup=get_main_keyboard())


async def send_message(text: str):
    try:
        if len(text) > 4096:
            text = text[:4093] + "..."
        await bot.send_message(TELEGRAM_CHAT_ID, text, reply_markup=get_main_keyboard())
    except Exception:
        logger.exception("Failed to send Telegram message")


async def send_photo_with_caption(image_bytes: bytes, caption: str):
    try:
        if len(caption) > 1024:
            caption = caption[:1021] + "..."
        from aiogram.types import BufferedInputFile
        photo = BufferedInputFile(image_bytes, filename="chart.png")
        await bot.send_photo(TELEGRAM_CHAT_ID, photo, caption=caption)
    except Exception:
        logger.exception("Failed to send Telegram photo")
        await send_message(caption)


async def send_close_with_chart(text: str, symbol: str, entry_price=None, exit_price=None, level=None,
                                 entry_time=None, exit_time=None):
    try:
        import os
        import time as _time
        from data.collector import candles_1m
        from bot.chart import generate_close_chart

        CHARTS_DIR = "trade_charts"
        os.makedirs(CHARTS_DIR, exist_ok=True)

        c1m = candles_1m.get(symbol, [])
        logger.info(f"chart candles available: {symbol} {len(c1m)}")
        if c1m:
            # Normalize candle field: collector stores "open_time", chart expects "time"
            normalized = [
                {**c, "time": c.get("time", c.get("open_time", 0))}
                for c in c1m
            ]
            # Имя файла: SYMBOL_YYYYMMDD_HHMMSS.png
            ts = _time.strftime("%Y%m%d_%H%M%S")
            filename = f"{symbol}_{ts}.png"
            save_path = os.path.join(CHARTS_DIR, filename)

            img = generate_close_chart(
                symbol=symbol,
                candles=normalized,
                entry_price=entry_price,
                exit_price=exit_price,
                level=level,
                entry_time=entry_time,
                exit_time=exit_time,
                n_candles=120,   # 2 часа на 1m ТФ
                save_path=save_path,
            )
            if img:
                await send_photo_with_caption(img, caption=text)
                return
    except Exception:
        logger.exception("Failed to generate close chart")
    await send_message(text)


async def start_bot():
    global bot
    from aiogram.types import BotCommand
    
    # Initialize bot with proxy support if configured
    if TELEGRAM_PROXY:
        logger.info("Using proxy for Telegram", proxy=TELEGRAM_PROXY)
        
        # For SOCKS5 proxy, we need aiohttp-socks
        if TELEGRAM_PROXY.startswith("socks5://"):
            try:
                from aiohttp_socks import ProxyConnector
                from aiogram.client.session.aiohttp import AiohttpSession
                import aiohttp
                
                # Create connector with proxy inside async context
                connector = ProxyConnector.from_url(TELEGRAM_PROXY)
                
                # Create custom session with connector
                class ProxySession(AiohttpSession):
                    def __init__(self):
                        super().__init__()
                        self._connector = connector
                    
                    async def create_session(self) -> aiohttp.ClientSession:
                        return aiohttp.ClientSession(connector=self._connector)
                
                bot = Bot(token=TELEGRAM_TOKEN, session=ProxySession())
                logger.info("SOCKS5 proxy configured successfully")
            except ImportError:
                logger.error("aiohttp-socks not installed. Install it: pip install aiohttp-socks")
                bot = Bot(token=TELEGRAM_TOKEN)
        else:
            # For HTTP proxy, aiogram supports it natively
            from aiogram.client.session.aiohttp import AiohttpSession
            session = AiohttpSession(proxy=TELEGRAM_PROXY)
            bot = Bot(token=TELEGRAM_TOKEN, session=session)
    else:
        bot = Bot(token=TELEGRAM_TOKEN)

    bot_ready.set()

    await bot.set_my_commands([
        BotCommand(command="add", description="Добавить монету — /add SYMBOL"),
        BotCommand(command="remove", description="Убрать монету — /remove SYMBOL"),
        BotCommand(command="list", description="Список активных монет"),
        BotCommand(command="check", description="Оценить уровень — /check SYMBOL LEVEL"),
        BotCommand(command="monitors", description="Активные мониторинги с дистанцией до цены"),
        BotCommand(command="stop", description="Остановить мониторинг — /stop SYMBOL"),
        BotCommand(command="restart", description="Перезапустить бота"),
        BotCommand(command="analyze", description="Запустить анализ — /analyze SYMBOL"),
        BotCommand(command="blacklist", description="Блэклист монет — /blacklist SYMBOL"),
        BotCommand(command="unblacklist", description="Убрать из блэклиста — /unblacklist SYMBOL"),
        BotCommand(command="stats", description="Статистика paper trading стратегий"),
    ])

    await bot.send_message(TELEGRAM_CHAT_ID, "Бот запущен", reply_markup=get_main_keyboard())
    await dp.start_polling(bot)

