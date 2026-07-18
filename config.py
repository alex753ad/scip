"""Configuration management for trading bot."""

import os
import json
from dotenv import load_dotenv
from logger import logger

load_dotenv()

# API Configuration
CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))
TELEGRAM_PROXY = os.getenv("TELEGRAM_PROXY")  # Optional proxy URL

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")

# File paths
TOKENS_FILE = "tokens.json"
BLACKLIST_FILE = "blacklist.json"
TRIGGER_TIMES_FILE = "trigger_times.json"
ACTIVE_MONITORS_FILE = "active_monitors.json"

_DATA_DIR = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "")
RAILWAY_VOLUME_MOUNT_PATH: str = _DATA_DIR   # exported for trade_log.py and other modules
HISTORY_DB_FILE = os.path.join(_DATA_DIR, "history.db") if _DATA_DIR else "history.db"


class TokenRegistry:
    """Registry for managing active trading symbols."""
    
    def __init__(self):
        self._tokens: list[str] = []
        self._load()

    def _load(self):
        """Load tokens from file."""
        if os.path.exists(TOKENS_FILE):
            try:
                with open(TOKENS_FILE) as f:
                    self._tokens = json.load(f)
                logger.info("Loaded tokens", count=len(self._tokens), tokens=self._tokens)
            except Exception as e:
                logger.error("Failed to load tokens", error=str(e))
                self._tokens = []

    def _save(self):
        """Save tokens to file (atomic write to avoid corruption on crash)."""
        try:
            tmp = TOKENS_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self._tokens, f, indent=2)
            os.replace(tmp, TOKENS_FILE)
            logger.debug("Saved tokens", count=len(self._tokens))
        except Exception as e:
            logger.error("Failed to save tokens", error=str(e))

    def get_all(self) -> list[str]:
        """Get all registered tokens."""
        return list(self._tokens)

    def add(self, symbol: str):
        """Add symbol to registry."""
        if symbol not in self._tokens:
            self._tokens.append(symbol)
            self._save()
            logger.info("Token added", symbol=symbol)

    def remove(self, symbol: str):
        """Remove symbol from registry."""
        if symbol in self._tokens:
            self._tokens.remove(symbol)
            self._save()
            logger.info("Token removed", symbol=symbol)

    def contains(self, symbol: str) -> bool:
        """Check if symbol is registered."""
        return symbol in self._tokens


class BlacklistRegistry:
    """Registry for symbols that should never be monitored or traded."""

    def __init__(self):
        self._symbols: set[str] = set()
        self._load()

    def _load(self):
        if os.path.exists(BLACKLIST_FILE):
            try:
                with open(BLACKLIST_FILE) as f:
                    self._symbols = set(json.load(f))
                logger.info("Loaded blacklist", count=len(self._symbols), symbols=list(self._symbols))
            except Exception as e:
                logger.error("Failed to load blacklist", error=str(e))

    def _save(self):
        try:
            tmp = BLACKLIST_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(sorted(self._symbols), f, indent=2)
            os.replace(tmp, BLACKLIST_FILE)
        except Exception as e:
            logger.error("Failed to save blacklist", error=str(e))

    def add(self, symbol: str):
        self._symbols.add(symbol)
        self._save()
        logger.info("Blacklisted", symbol=symbol)

    def remove(self, symbol: str):
        self._symbols.discard(symbol)
        self._save()
        logger.info("Removed from blacklist", symbol=symbol)

    def contains(self, symbol: str) -> bool:
        return symbol in self._symbols

    def get_all(self) -> list[str]:
        return sorted(self._symbols)


# Global token registry instance
token_registry = TokenRegistry()
blacklist = BlacklistRegistry()


def validate_config() -> bool:
    """Validate that all required configuration is present."""
    if not CLAUDE_API_KEY:
        logger.error("Missing CLAUDE_API_KEY in environment")
        return False
    if not TELEGRAM_TOKEN:
        logger.error("Missing TELEGRAM_TOKEN in environment")
        return False
    if TELEGRAM_CHAT_ID == 0:
        logger.error("Missing or invalid TELEGRAM_CHAT_ID in environment")
        return False
    return True


def validate_bybit_config() -> bool:
    """Проверить наличие Bybit ключей перед включением live-торговли."""
    if not BYBIT_API_KEY:
        logger.error("Missing BYBIT_API_KEY in environment")
        return False
    if not BYBIT_API_SECRET:
        logger.error("Missing BYBIT_API_SECRET in environment")
        return False
    return True
