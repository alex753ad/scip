"""Data models for trading bot state management."""

from dataclasses import dataclass, field
from typing import Literal
import asyncio


@dataclass
class SymbolState:
    """State management for a single trading symbol."""
    
    symbol: str
    phase: Literal["idle", "phase1", "phase2"] = "idle"
    tasks: dict[str, asyncio.Task] = field(default_factory=dict)
    stop_flags: dict[str, asyncio.Event] = field(default_factory=dict)
    last_trigger_time: float = 0.0
    proximity_notified: dict[str, float] = field(default_factory=dict)
    # Separate storage for weak (unmonitored) level touch state.
    # Keys: "touch_idx_{symbol}_{level}" -> int (candle index)
    #       "min_price_{symbol}_{level}"  -> float (min price during touch)
    #       "resolved_{symbol}_{level}"   -> float (timestamp of resolution)
    weak_touch_state: dict = field(default_factory=dict)
    analyzed_levels: set[str] = field(default_factory=set)
    level_strengths: dict[str, int] = field(default_factory=dict)  # task_key -> strength
    # P6: task_key'и уровней, помеченных filter'ом как "мёртвые" (approach >= S2_APPROACH_BLOCK).
    # Монитор перестаёт публиковать proximity-события для них (см. monitor.py) — approach
    # только растёт, поэтому обратного "пробуждения" не предусмотрено: уровень живёт в
    # таком режиме до конца обычного цикла мониторинга (пробой/таймаут/рестарт).
    dead_levels: set[str] = field(default_factory=set)

    # ── Pump Phase Detection ──────────────────────────────────────────
    pump_high: float = 0.0              # peak price of current pump
    pump_high_time: float = 0.0         # unix timestamp of peak candle
    pump_base_price: float = 0.0        # base (origin) price of pump
    broken_since_pump: int = 0          # levels broken without confirmed bounce
    last_bounce_time: float = 0.0       # unix timestamp of last confirmed bounce
    pump_phase: str = "unknown"         # active | caution | degraded | dead | unknown
    pump_health: int = 0                # latest calculated health score 0-100

    def make_task_key(self, level: float) -> str:
        """Generate unique task key for symbol-level pair.
        
        Uses '::' separator (not '_') to avoid ambiguity with symbols
        that contain underscores (e.g. hypothetical FOO_BAR futures).
        """
        from analysis.level_builder import _round_level
        return f"{self.symbol}::{_round_level(level)}"

    @staticmethod
    def parse_task_key(task_key: str) -> tuple[str, float] | None:
        """Parse task_key → (symbol, level). Returns None if key is malformed."""
        parts = task_key.rsplit("::", 1)
        if len(parts) != 2:
            return None
        try:
            return parts[0], float(parts[1])
        except ValueError:
            return None

    def add_task(self, level: float, task: asyncio.Task, strength: int = 0) -> str:
        """Add monitoring task for a level. Cancels existing tasks on nearby levels."""
        key = self.make_task_key(level)

        # Cancel existing tasks on levels within 3% (duplicates)
        if level > 0:
            for existing_key in list(self.tasks.keys()):
                parsed = self.parse_task_key(existing_key)
                if parsed is None:
                    continue
                existing_level = parsed[1]
                if existing_level > 0 and abs(existing_level - level) / level < 0.03:
                    self.tasks[existing_key].cancel()
                    stop = self.stop_flags.get(existing_key)
                    if stop:
                        stop.set()
                    del self.tasks[existing_key]
                    self.stop_flags.pop(existing_key, None)
                    self.level_strengths.pop(existing_key, None)
                    self.dead_levels.discard(existing_key)

        self.tasks[key] = task
        self.stop_flags[key] = asyncio.Event()
        self.level_strengths[key] = strength
        return key

    def remove_task(self, task_key: str):
        """Remove monitoring task."""
        self.tasks.pop(task_key, None)
        self.stop_flags.pop(task_key, None)
        self.proximity_notified.pop(task_key, None)
        self.level_strengths.pop(task_key, None)
        self.dead_levels.discard(task_key)

    def mark_level_dead(self, task_key: str) -> None:
        """P6: пометить уровень мёртвым (вызывается из strategy2_signal_filter при G3).

        Монитор (monitor.py) проверяет этот флаг перед публикацией proximity-события
        и не публикует его для мёртвых уровней — фильтр больше не дёргается впустую
        каждые 5 сек, лог не засоряется повторным "approach>=block".
        """
        self.dead_levels.add(task_key)

    def is_level_dead(self, task_key: str) -> bool:
        """P6: проверить, помечен ли уровень мёртвым."""
        return task_key in self.dead_levels

    def cancel_all_tasks(self):
        """Cancel all monitoring tasks for this symbol."""
        for task in self.tasks.values():
            task.cancel()
        for event in self.stop_flags.values():
            event.set()
    
    def has_active_tasks(self) -> bool:
        """Check if symbol has any active monitoring tasks."""
        return len(self.tasks) > 0
    
    def mark_level_analyzed(self, level: float):
        """Mark level as analyzed to prevent re-analysis."""
        self.analyzed_levels.add(f"{self.symbol}:{level}")
    
    def is_level_analyzed(self, level: float) -> bool:
        """Check if level was already analyzed."""
        return f"{self.symbol}:{level}" in self.analyzed_levels
    
    def clear_analyzed_levels(self):
        """Clear analyzed levels cache."""
        self.analyzed_levels.clear()


@dataclass
class LevelData:
    """Data structure for a support/resistance level."""
    
    level: float
    type: str  # pump_base, body_level, wick_level, etc.
    symbol: str
    level_side: Literal["support", "resistance"]
    strength: int = 0
    verdict: Literal["hold", "exit", "exit_fast"] = "hold"
    reason: str = ""
    
    # Technical indicators
    approach: int = 0
    vol_ratio: float = 1.0
    atr_pct: float = 0.0
    zone_approaches: int = 0
    
    # Level characteristics
    position: str = "mid_move"  # origin or mid_move
    cluster: bool = False
    pump_volume_ratio: float = 1.5
    
    # History
    was_broken: bool = False
    sweep_reclaimed: bool = False
    price_min_since_level: float = 0.0
    max_vol_on_approach: float = 0.0
    engulf_15m: bool = False
    
    def to_dict(self) -> dict:
        """Convert to dictionary for API calls."""
        return {
            "level": self.level,
            "type": self.type,
            "symbol": self.symbol,
            "level_side": self.level_side,
            "strength": self.strength,
            "verdict": self.verdict,
            "reason": self.reason,
            "approach": self.approach,
            "vol_ratio": self.vol_ratio,
            "atr_pct": self.atr_pct,
            "zone_approaches": self.zone_approaches,
            "position": self.position,
            "cluster": self.cluster,
            "pump_volume_ratio": self.pump_volume_ratio,
            "was_broken": self.was_broken,
            "sweep_reclaimed": self.sweep_reclaimed,
            "max_vol_on_approach": self.max_vol_on_approach,
            "engulf_15m": self.engulf_15m,
        }


class StateManager:
    """Global state manager for all symbols."""
    
    def __init__(self):
        self._states: dict[str, SymbolState] = {}
    
    def get_state(self, symbol: str) -> SymbolState:
        """Get or create state for symbol."""
        if symbol not in self._states:
            self._states[symbol] = SymbolState(symbol=symbol)
        return self._states[symbol]
    
    def remove_state(self, symbol: str):
        """Remove state for symbol."""
        if symbol in self._states:
            self._states[symbol].cancel_all_tasks()
            del self._states[symbol]
    
    def get_all_active_tasks(self) -> dict[str, asyncio.Task]:
        """Get all active tasks across all symbols."""
        tasks = {}
        for state in self._states.values():
            tasks.update(state.tasks)
        return tasks
    
    def cancel_all_tasks(self):
        """Cancel all tasks for all symbols."""
        for state in self._states.values():
            state.cancel_all_tasks()
    
    def get_active_monitors_count(self) -> int:
        """Get total number of active monitors."""
        return sum(len(state.tasks) for state in self._states.values())


# Global state manager instance
state_manager = StateManager()
