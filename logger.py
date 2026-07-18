"""Logging configuration with structured logging support."""

import os
import sys
from loguru import logger

# Remove default handler
logger.remove()

os.makedirs("logs", exist_ok=True)

# Console handler - simple format for readability
logger.add(
    sys.stderr,
    format="{time:HH:mm:ss} {level.icon} {message}",
    level="INFO",
    colorize=True,
)

# Main log - INFO and above, all modules
# Сюда идёт всё важное: триггеры, мониторы, сигналы, ошибки
logger.add(
    "logs/bot_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="7 days",
    level="INFO",
    encoding="utf-8",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message} | {extra}",
    serialize=False,
)

# Trading debug log - DEBUG, только модули trading/*
# Сюда идут все детали стратегий, входы/выходы, события
logger.add(
    "logs/trading_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="2 days",
    level="DEBUG",
    encoding="utf-8",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message} | {extra}",
    filter=lambda record: record["name"].startswith("trading"),
    serialize=False,
)

# Error log - WARNING и выше, все модули, хранить дольше
# Отдельный файл для быстрой диагностики проблем
logger.add(
    "logs/errors_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="14 days",
    level="WARNING",
    encoding="utf-8",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message} | {extra}",
    serialize=False,
)


def log_with_context(level: str, message: str, **kwargs):
    """
    Log with structured context data.

    Example:
        log_with_context("info", "Level triggered", symbol="BTCUSDT", level=50000, strength=5)
    """
    logger.bind(**kwargs).log(level.upper(), message)
