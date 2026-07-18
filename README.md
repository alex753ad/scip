# mark

Торговый бот для **Binance Futures**. Мониторит уровни поддержки/сопротивления, оценивает их силу через Python + ML + Claude AI и отправляет алерты в Telegram.

## Что умеет

- **Скринер рынка** — находит монеты с объёмом > 40M USDT, ростом > 10% и NATR > 2% на 5М
- **Автоматическое добавление монет** — скринер каждые 10 минут, новые монеты сразу уходят в мониторинг
- **Построение уровней** — pump_base, body_level, wick_level, mid_impulse_pause из 15М свечей
- **Тройная оценка силы уровня:**
  - Python (ATR, объём пампа, касания, sweep)
  - Claude Haiku (визуальный анализ по ASCII-графику)
  - ML (RandomForest: вероятность bounce + глубина пробоя)
- **Мониторинг в реальном времени** каждые 5 сек: bounce / breakout / sweep / давление / volume spike
- **Telegram-бот** с кнопками и FSM, поддержка proxy

## Быстрый старт

### 1. Клонировать и установить зависимости

```bash
git clone https://github.com/alex753ad/mark.git
cd mark
pip install -r requirements.txt
```

### 2. Создать `.env`

```env
CLAUDE_API_KEY=sk-ant-...
TELEGRAM_TOKEN=...
TELEGRAM_CHAT_ID=...
# TELEGRAM_PROXY=socks5://127.0.0.1:1080
```

### 3. Запустить

```bash
python main.py
```

## Команды Telegram

| Команда / Кнопка | Действие |
|-----------------|----------|
| `/add SYMBOL` | Добавить монету (например `/add BTCUSDT`) |
| `/remove SYMBOL` | Удалить монету |
| `/analyze SYMBOL` | Полный анализ: уровни + Claude + ML + график |
| `/check SYMBOL LEVEL` | Оценить конкретный уровень |
| `/monitors` | Список активных мониторов |
| `/stop SYMBOL` | Остановить мониторинг символа |
| 📊 Рынок | Скринер + кнопки быстрого анализа |
| 📜 История | Последние события по монете |
| `/export_db` | Скачать history.db |

## Структура проекта

```
mark/
├── main.py              # Оркестратор: запуск, фазы, фоновые loops
├── config.py            # Конфигурация, TokenRegistry
├── constants.py         # Все числовые пороги
├── models.py            # SymbolState, StateManager
├── data/
│   ├── collector.py     # Свечи Binance REST + aggTrades WebSocket
│   └── history.py       # SQLite: исходы, профили, события
├── analysis/
│   ├── level_builder.py # Построение уровней из 15М свечей
│   ├── trigger.py       # ATR, сила уровня, триггеры
│   ├── monitor.py       # Мониторинг: bounce/breakout/sweep/давление
│   ├── screener.py      # Скринер монет
│   ├── ml_score.py      # ML: p_bounce + expected_depth
│   ├── claude_strength.py # Claude: оценка силы уровней
│   └── ml/              # Обученные sklearn-модели (.pkl)
├── ai/
│   └── claude_client.py # HTTP-клиент Anthropic
├── bot/
│   └── telegram.py      # aiogram v3: команды, кнопки, FSM
└── train_ml.py          # Переобучение ML по history.db
```

## Стек

| | |
|-|-|
| **Язык** | Python 3.11+, asyncio |
| **Биржа** | Binance Futures (python-binance 1.0.36) |
| **AI** | Claude Haiku (anthropic 0.86.0) |
| **Telegram** | aiogram 3.27.0 |
| **ML** | scikit-learn (RandomForest) |
| **БД** | SQLite + aiosqlite 0.22.1 |
| **Деплой** | Railway (Procfile: `worker: python main.py`) |

## Жизненный цикл сигнала

```
Монета добавлена
    │
    ├── триггер: рост 3%+ на 15М
    │       └── build_levels → calculate_strength → Claude + ML
    │               └── strength >= 3 → start_monitor
    │
    └── автоскринер: рост 10%+ + NATR > 2%
            └── build_levels → Claude + ML → мониторинг ближайшего уровня

Мониторинг (каждые 5 сек)
    ├── proximity alert    <- цена < 2% от уровня
    ├── давление           <- 3+ направленных свечи в зону
    ├── volume spike       <- объём x3
    ├── sweep              <- пробой тени + возврат тела
    ├── bounce             <- отскок + объём → save_outcome
    └── breakout           <- пробой 5+ свечей + объём x2.4 → next level
```

## Деплой на Railway

1. Подключить репозиторий к Railway
2. Добавить Volume и примонтировать в `/data`
3. Задать переменные окружения: `CLAUDE_API_KEY`, `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`
4. Railway автоматически запустит `worker: python main.py` из Procfile

Файлы данных (`tokens.json`, `history.db`, `active_monitors.json`, `trigger_times.json`) сохраняются в Volume между деплоями.

## Переобучение ML

ML-модели обучаются на накопленных исходах из `history.db`:

```bash
python train_ml.py
```

Бот автоматически перезагружает модели после завершения обучения (`_ml_retrain_loop`).

## Известные ограничения

- Медленный памп (> 60 свечей) не обнаруживается `level_builder`
- `monitoring_age` не используется как признак ML (планируется добавить)
- `cluster_radius = atr x 0.3` может давать широкие кластеры для мелких монет

Подробнее — в [ARCHITECTURE.md](./ARCHITECTURE.md).
