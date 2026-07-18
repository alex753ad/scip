"""Trading bot configuration constants."""

# Trigger settings
TRIGGER_GROWTH_THRESHOLD = 0.03  # 3% рост на 15М для триггера
TRIGGER_COOLDOWN_SECONDS = 3600  # 1 час между триггерами одного символа

# Level detection
LEVEL_APPROACH_THRESHOLD = 0.5  # ATR × 0.5 для определения "близкого" уровня
LEVEL_CLUSTER_RADIUS_PCT = 0.01  # 1% для определения кластера уровней
LEVEL_REAL_CLUSTER_MIN_TOUCHES = 3  # Минимум касаний для уточнения уровня
LEVEL_REAL_CLUSTER_SHIFT_THRESHOLD = 0.3  # 30% смещения медианы для корректировки

# Monitoring
PROXIMITY_ALERT_DISTANCE_PCT = 0.02   # 2% от уровня для proximity alert
PROXIMITY_ALERT_COOLDOWN_SECONDS = 86400  # 24 часа — фактически однократно за сессию мониторинга
WEAK_BREAKOUT_COOLDOWN_SECONDS = 3600  # 1 час — фактически однократно за сессию мониторинга

# Volume thresholds
VOLUME_BREAKOUT_RATIO = 2.4  # Объём ×2.4 для подтверждения пробоя (повышен с 2.0: vol 2-4x = защита уровня 90.9% bounce)
VOLUME_SPIKE_RATIO = 3.0  # Объём ×3 для алерта о спайке
VOLUME_SPIKE_RESET_RATIO = 1.5  # Объём < ×1.5 для сброса флага спайка
VOLUME_REBOUND_MIN_RATIO = 1.0  # Минимальный объём для подтверждения отбоя

# Distance thresholds
DISTANCE_RESET_ATR_MULTIPLIER = 2.0  # Удаление > 2×ATR для сброса всех флагов
DISTANCE_PARTIAL_RESET_ATR_MULTIPLIER = 1.0  # Удаление > 1×ATR для частичного сброса
PRESSURE_ZONE_MIN_DISTANCE_PCT = 0.002  # 0.2% минимальная дистанция для давления
PRESSURE_ZONE_MAX_DISTANCE_PCT = 0.01  # 1.0% максимальная дистанция для давления

# Pressure detection
PRESSURE_MIN_DIRECTIONAL_CANDLES = 3  # Минимум 3 направленных свечи для давления
PRESSURE_VOLUME_MIN_RATIO = 1.0  # Минимальный объём для подтверждения давления на 15М

# Level strength calculation
STRENGTH_PUMP_VOLUME_LOW_THRESHOLD = 1.5  # Порог низкого объёма пампа
STRENGTH_PUMP_VOLUME_HIGH_THRESHOLD = 2.5  # Порог высокого объёма пампа
STRENGTH_APPROACH_EXIT_THRESHOLD = 2  # Число подходов для автоматического exit

# Data collection
CANDLES_HISTORY_LIMIT = 300  # Количество свечей для хранения в памяти
COLLECTOR_UPDATE_INTERVAL_SECONDS = 5  # Интервал обновления данных с Binance

# Screener settings
SCREENER_MIN_VOLUME_USD = 40_000_000  # Минимальный объём за 24ч для скринера
SCREENER_MIN_GROWTH_PCT = 10.0  # Минимальный рост за 24ч для скринера
SCREENER_MIN_NATR = 2.0  # Минимальный NATR для скринера
SCREENER_MIN_15M_VOLUME_USD = 1_000_000  # Минимальный объём последней 15М свечи для добавления
SCREENER_DELAY_SECONDS = 15  # Задержка перед запуском скринера
SCREENER_AUTO_INTERVAL_SECONDS = 600  # Интервал автоскринера (10 минут)

# Volume fade — автоудаление монеты при падении объёма
LOW_VOLUME_FADE_CANDLES = 4        # Сколько подряд 15М свечей проверять
LOW_VOLUME_FADE_THRESHOLD = 400_000  # Порог объёма ($) каждой из N свечей

# Monitor health — удаление монеты из мониторинга при потере активности
MONITOR_HEALTH_INTERVAL_SECONDS = 60      # Интервал проверки здоровья мониторов
MONITOR_MIN_NATR_5M = 0.8                 # Минимальный NATR(5m, 14 баров) для удержания в мониторинге
MONITOR_MIN_1M_TRADES = 200               # Минимум сделок в последней закрытой 1m свече
MONITOR_NATR_FAIL_DURATION_SECONDS = 3600   # Удалять если NATR ниже порога 1 час подряд
MONITOR_TRADES_FAIL_DURATION_SECONDS = 1800  # Удалять если сделок мало 30 минут подряд

# API limits
CLAUDE_MAX_CONCURRENT_REQUESTS = 2  # Максимум параллельных запросов к Claude API
CLAUDE_MODEL = "claude-haiku-4-5-20251001"  # Исправленный ID модели Haiku
CLAUDE_MAX_TOKENS = 1024  # Максимум токенов в ответе Claude
CLAUDE_STRENGTH_ENABLED = True  # Использовать Claude для определения силы уровней

# ATR settings
ATR_PERIOD = 14  # Период для расчёта ATR

# Price rounding rules (price range -> decimal places)
PRICE_ROUNDING_RULES = [
    (100, 2),      # > 100 → 2 знака
    (1, 4),        # 1-100 → 4 знака
    (0.1, 5),      # 0.1-1 → 5 знаков
    (0.01, 6),     # 0.01-0.1 → 6 знаков
    (0, 8),        # < 0.01 → 8 знаков
]

# Level building
PUMP_MIN_GROWTH_PCT = 0.05  # 5% минимальный рост для определения пампа
BODY_CLUSTER_WEIGHT_15M = 3  # Вес 15М свечи в кластеризации тел
BODY_CLUSTER_WEIGHT_1M = 1  # Вес 1М свечи в кластеризации тел
BODY_CLUSTER_MIN_WEIGHT = 6  # Минимальный вес для body_level
WICK_CLUSTER_MIN_TOUCHES = 4  # Минимум касаний для wick_level
CONSOLIDATION_MIN_CANDLES = 3  # Минимум свечей для consolidation
CONSOLIDATION_RANGE_ATR_MULTIPLIER = 4  # Множитель ATR для определения узкого range

# Monitoring events
LEVEL_BROKEN_MIN_CANDLES = 5  # Минимум свечей ниже уровня для алерта

# ── Trading Strategies ────────────────────────────────────────────
STRATEGY_POSITION_SIZE_USDT: float = 100.0    # размер позиции в USDT
STRATEGY_MAX_OPEN_TRADES: int = 3             # макс. одновременных сделок на стратегию
STRATEGY_TRADE_TIMEOUT_MINUTES: float = 60.0  # таймаут позиции в минутах

# Strategy 1 (Bounce)
S1_MIN_STRENGTH: int = 4
S1_MIN_P_BOUNCE: float = 0.86
S1_MIN_VOL_RATIO: float = 1.5   # DEPRECATED: не используется; используй S1_MAX_VOL_RATIO
S1_MAX_VOL_RATIO: float = 1.2   # S1 входит только при тихом касании уровня
S1_TP1_RR: float = 1.5    # risk:reward для TP1
S1_TP2_RR: float = 3.0    # risk:reward для TP2

# --- S1 (bounce) exit bracket ---
S1_SL_PCT = 0.025   # стоп-лосс: -2.5% от цены входа
S1_TP_PCT = 0.020   # тейк-профит: +2.0% от цены входа

# Strategy 2 (Grid)
S2_MIN_STRENGTH: int = 3
S2_MIN_P_BOUNCE: float = 0.60
S2_PRESSURE_COOLDOWN_SECONDS: int = 300   # не входить N сек после pressure события
S2_GRID_ORDERS: int = 2
S2_POSITION_SIZE_USDT: float = 200.0   # 2 ордера × 100 USDT

# Strategy 3 (Breakout)
S3_MIN_BREAKOUT_VOL_RATIO: float = 2.4
S3_MIN_BREAKOUT_VOL_RATIO_STRONG: float = 3.5  # порог для «сильного» пробоя (быстрый TP)
S3_SWEEP_COOLDOWN_SECONDS: int = 120      # не входить N сек после sweep
S3_TP1_ATR_MULT: float = 2.0
S3_TP2_ATR_MULT: float = 4.0
S3_SL_ATR_MULT: float = 0.5
S3_MIN_TRADE_DURATION_MINUTES: float = 5.0   # не закрывать по SL/TP раньше этого времени (было 45.0)
S3_MIN_STRENGTH  = 4      # фильтр входа по strength (задача 1)
S3_MAX_NATR_5M   = 0.0    # выключен пока нет статы; поставить ~0.03 после накопления данных

# ── Pump Phase Detection ──────────────────────────────────────────
PUMP_HEALTH_MIN_SCORE: int = 50           # min score to allow monitoring
PUMP_HEALTH_CAUTION_SCORE: int = 70       # score below which only strength>=4 levels are monitored
PUMP_MAX_AGE_HOURS: float = 8.0           # max pump age before -40 freshness penalty
PUMP_MAX_CORRECTION_PCT: float = 0.65     # correction > 65% of pump body → -40 penalty
PUMP_MAX_BROKEN_LEVELS: int = 2           # broken levels without bounce → pump dead
PUMP_BLEED_MIN_RED_CANDLES: int = 4       # FIX BUG-17: было 5 из 6 — слишком строго, практически никогда не срабатывало; теперь 4 из 6
PUMP_BLEED_MIN_VOL_TREND: int = 3         # FIX BUG-17: было 4 из 5 пар — снижено до 3

# ── Market Phase Detection (analysis/market_phase.py) ────────────────────────
PHASE_RANGE_WINDOW_1M              = 90     # окно 1m-свечей для определения диапазона (90 мин)
PHASE_PUMP_WINDOW_1M               = 40     # окно 1m-свечей для детекции памп-фазы (40 мин)
PHASE_BOUNCE_WINDOW_5M             = 24     # окно 5m-свечей для расчёта bounce quality (2 часа)
PHASE_STRUCT_WINDOW_5M             = 288    # окно 5m-свечей для структурного 24ч-диапазона (288×5м=24ч), для позиционного гейта (P7)
PHASE_FLAT_RANGE_MAX_PCT           = 4.0   # максимальный диапазон % для флета
PHASE_FLAT_DIR_EFF_MAX             = 0.25  # максимальная направленность для флета
PHASE_PUMP_PRICE_CHANGE_MIN_PCT    = 5.0   # минимальный рост % за pump_window для памп-фазы
PHASE_PUMP_ATR_RATIO_MIN           = 1.8   # минимальный atr/baseline_atr для памп-фазы
PHASE_BLEED_BOUNCE_QUALITY_MAX     = 0.35  # ниже этого — блид (не торговать)
PHASE_TRADEABLE_BOUNCE_QUALITY_MIN = 0.50  # выше этого — торгуемый откат
PHASE_FLAT_LEVEL_POSITION_MAX      = 0.30  # максимальная позиция уровня в диапазоне для флета

# ── SL Rules для Strategy 2 (trading/strategy2_signal_filter.py) ─────────────
S2_SL_MIN_DIST_ATR   = 0.8   # минимальное расстояние SL от входа в ATR
S2_SL_RR_MIN         = 0.7   # минимальный RR = (TP1 - entry) / (entry - SL); было 1.0/1.2. ПРИЧИНА 0.7: TP1=высота сетки над входом, а SL=та же высота + буфер (0.3..0.5·ATR) ниже → sl_dist = tp1_dist + буфер, поэтому формальный RR структурно <1 во ВСЕХ фазах (потолок ~0.83 pump/unknown, ~0.89 flat). Порог 1.0 блокировал 100% входов. Защиту от «SL внутри сетки» теперь даёт гарантия SL<grid_bottom в signal_filter, так что этот порог — лишь нижний предохранитель от вырожденной геометрии. Реальный реализованный RR ≈3.75 (трейл/TP2)
S2_SL_FLAT_BUFFER    = 0.3   # буфер ниже range_low для SL в ATR (флет)
S2_SL_IMPULSE_BUFFER = 0.5   # буфер ниже swing_low для SL при импульсе

# ── No-fill grace (trading/strategy2_live.py) ────────────────────────────────
# Было 180 (захардкожено в _reconcile_one) → фактическая отмена ~3-4 мин с учётом
# 60-секундного шага reconcile. Поднято до 600 (~10-11 мин), чтобы не срезать
# медленные/блидовые подходы до наполнения сетки.
S2_NO_FILL_GRACE_SECONDS = 600

# Старт TP/трейлинга: не управлять позицией младше N сек (_price_loop в strategy2_live).
# Было 180 (3 мин) захардкожено → 120 (2 мин): раньше включать сопровождение прибыли.
S2_TRAILING_START_SECONDS = 120

# Макс. дистанция цены НАД УРОВНЕМ, при которой ещё ставим сетку (Фильтр 7).
# Раньше порог был эффективно ~1.1% (current_price > grid_anchor*1.005) и резал
# ранние касания (4378 skip/день) → вход уходил на поздние подходы. 2% = ставить
# сетку заранее под ПЕРВОЕ касание; быстрый 5с-wick исполняет лимитки на бирже сам.
# Анкор сетки НЕ меняется — фактические филлы всё равно начинаются у уровня, ранний
# placement не «покупает верх» (верхние ордера ждут, пока цена дойдёт).
S2_MAX_PRICE_ABOVE_LEVEL_PCT = 0.02

# Глубина сетки: ограничить ширину зоной отскоков. По history.db отскоки идут мелко
# (медиана 0.53% ниже уровня; 0–1% → 51–57% bounce, >2% → ~0%). Раньше ширина была
# atr*2.5 и на волатильных монетах залезала на 5–7% вниз, в «мёртвую зону», где
# bounce≈0 и копится нож. Теперь grid_width = min(atr*2.5, level*S2_GRID_DEPTH_PCT).
S2_GRID_DEPTH_PCT = 0.015

# Верхнее утяжеление сетки: больше объёма у уровня (мелкая зона = высокий bounce),
# меньше у дна (глубоко = bounce≈0). Линейная развесовка от ratio (верхний ордер)
# до 1.0 (нижний). 3.0 ≈ верхний ордер втрое больше нижнего; на 200 USDT/10 ордеров
# даёт ~30 USDT сверху и ~10 снизу (все ≥ мин. ноционала, дроблений нет).
S2_GRID_TOP_WEIGHT_RATIO = 1.0
S2_GRID_BOTTOM_WEIGHT_RATIO = 1.0  # 2 равные позиции по 100 USDT (было 0.5 при 10-ордерной сетке)

# ── No-fill spam guard + grid anchor (trading/strategy2_signal_filter.py) ────
S2_REPLACE_COOLDOWN_SECONDS = 300    # не размещать сетку на том же symbol:level N сек после размещения
# Полоса уровня для кулдаунов: повтор считается «тем же уровнем», если он в пределах
# ±band от недавнего размещения/закрытия. Закрывает дыру точного ключа (переокругление
# find_real_level и заходы «рядом»). 0.005 = ±0.5%.
S2_COOLDOWN_LEVEL_BAND_PCT = 0.005
# Жёсткий блок повторного входа на тот же уровень-полосу: после любого размещения
# ИЛИ закрытия на уровне (в пределах band) не входить повторно N секунд. Несёт
# основную защиту от re-entry, т.к. touches-хардфильтр ненадёжен — _count_approaches
# сбрасывает счётчик касаний на каждом новом ценовом пике (pump_high_time), поэтому
# возврат к уровню после движения вверх читается как «свежее первое касание».
# Первые входы не трогает (на новом уровне прошлой активности в полосе нет).
S2_LEVEL_REENTRY_BLOCK_SECONDS = 1800
S2_GRID_ANCHOR_PCT_PUMP     = 1.006  # верхний ордер сетки выше уровня (pump_base), было 1.004
S2_GRID_ANCHOR_PCT_DEFAULT  = 1.004  # верхний ордер сетки выше уровня (остальные типы), было 1.0015

# ════════════════════════════════════════════════════════════════════════════
# СТРАТЕГИЯ G1/G2/G4 — фазовый gating + флип + управление (добавлено 25.06)
# Скоуп: торгуем G1 / G2 (осторожно, трейлер) / G4 (флип). Блокируем G3 / G5.
# Хард-фильтр touches>=2 в ml_score СНЯТ; блок 4+ подходов перенесён в фильтр.
# [H4 26.06] approach=3 разрешён как G2/cautious (approach=1-3 → +15.9 USDT).
# ════════════════════════════════════════════════════════════════════════════

# ── Номер подхода к уровню (approach_count из trigger._count_approaches) ──────
S2_APPROACH_BLOCK    = 4   # approach_count >= 4 → блок (G3, уровень исчерпан); было 3 (H4: 3-й подход прибыльный)
S2_APPROACH_CAUTIOUS = 2   # approach_count >= 2 → осторожный режим (G2): approach=2 и approach=3 оба cautious

# ── G2: отдельный нижний флор p_bounce (G1 остаётся на S2_MIN_P_BOUNCE) ───────
# Риск G2 держит управление позицией (трейлер + выход по слому 3й свечи), а не
# входной порог; но совсем мусорные входы (p_bounce≈0) всё равно режутся.
S2_MIN_P_BOUNCE_G2 = 0.40

# ── Объём при подходе (vol_falling): падает → отскок; держится → пробой ───────
# last_vol < mean(prev) × ratio → «падает». Признак G-логики, пишется в БД для анализа.
S2_VOL_FALLING_LOOKBACK = 5     # сколько последних 1m-свечей берём (last + (LOOKBACK-1) предыдущих)
S2_VOL_FALLING_RATIO    = 1.0   # last < mean(предыдущих) × ratio → считаем «падает»
S2_APPROACH_SPEED_LOOKBACK: int = 5   # окно 1m-свечей для оценки скорости подхода к уровню

# [H6 28.06] body_level — не входить в активное падение на объёме (Фильтр 10c).
# Подтверждено: VELVET×2 / HEI / BEAT (~−10.6 за 2 дня) — лонг body_level в начале
# обвала. Режем только при связке «крутой нисходящий импульс + всплеск объёма продаж»,
# чтобы не задеть обычную коррекцию к уровню (там объём затухает).
S2_FALL_LOOKBACK     = 5     # окно 1m-свечей для оценки нисходящего импульса
S2_FALL_DROP_PCT     = 1.2   # падение close за окно >= этого % → «активное падение»
S2_FALL_VOL_BASELINE = 10    # сколько свечей ДО окна берём за базовую линию объёма
S2_FALL_VOL_SPIKE    = 1.5   # средний объём окна >= базовый × множитель → всплеск продаж

# ── G4: S/R-флип (детект по breakout из history.db / level_outcomes) ──────────
# Флип = уровень был пробит вверх (resistance→support) с объёмом; торгуем первый ретест сверху.
S2_FLIP_ENABLED        = True
S2_FLIP_MAX_AGE_HOURS  = 24.0   # пробой не старше N часов, иначе уровень не считаем флипнутым
S2_FLIP_MATCH_ATR_MULT = 0.5    # матч уровня к прошлому пробою: ±0.5·ATR ...
S2_FLIP_MATCH_PCT      = 0.003  # ... или ±0.3% — берём меньшее из двух
S2_FLIP_MAX_RETEST     = 1      # торгуем только ПЕРВЫЙ ретест после флипа

# ── Слой 3 (управление позицией, НЕ входной гейт): подтверждение / слом ───────
# Вход остаётся в момент касания (сетка ставится сразу — ловим закол). Эти параметры —
# для пост-филл сопровождения: 2-3 свечи без возврата = держим/тянем; слом 3й свечи = выход.
S2_MGMT_CONFIRM_CANDLES = 3     # сколько 1m-свечей смотрим после первого филла
S2_MGMT_SLOM_EXIT       = False # ОТКЛЮЧЁН: слом-выход продавал локальное дно за минуты до отскока (UB/SLX/OUSDT/LAB — все дошли бы до TP1). Было True

# ─── Proximity-arm (доармить спящие монеты по приближению к уровню, в обход check_trigger) ───
# check_trigger требует рост ≥3% в ПОСЛЕДНЕМ часе и потому пропускает поздние откаты к
# pump_base (памп был раньше). Этот путь армит спящие известные монеты по приближению к
# уже сильной поддержке. По умолчанию "shadow" — только логирует would-arm, ничего не армит.
PROXIMITY_ARM_MODE                 = "active"       # "off" | "shadow" | "active"
PROXIMITY_ARM_SCAN_INTERVAL        = 30             # сек между сканами
PROXIMITY_ARM_DIST_ATR_MIN         = 1.0           # поддержка не ближе N·ATR (иначе уже у уровня)
PROXIMITY_ARM_DIST_ATR_MAX         = 4.0           # и не дальше N·ATR (иначе попадёт под stale-cancel >10%)
PROXIMITY_ARM_SHADOW_MIN_STRENGTH  = 3             # порог логирования в shadow (широкий, для наблюдения)
PROXIMITY_ARM_MIN_STRENGTH         = 4             # порог арминга в active (узкий)
PROXIMITY_ARM_LEVEL_TYPES          = ("pump_base",) # active: только эти типы уровней
PROXIMITY_ARM_COOLDOWN_SECONDS     = 1800          # 30 мин на символ между проксимити-армингами
PROXIMITY_ARM_MAX_CONCURRENT       = 3             # кап одновременных проксимити-армингов
