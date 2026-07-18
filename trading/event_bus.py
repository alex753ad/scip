"""Event bus for inter-module communication (monitor → strategies).

A single asyncio.Queue per process. monitor.py publishes events; strategy_runner
subscribes and fans out to all three strategies.

Event dict schema (all fields):
    event_type: str        — "proximity" | "sweep" | "bounce" | "pressure"
                             | "breakout" | "weak_breakout" | "volume_spike"
    symbol: str            — e.g. "BTCUSDT"
    level: float           — support / resistance price
    level_side: str        — "support" | "resistance"
    level_type: str        — "pump_base" | "body_level" | "wick_level" | "mid_impulse_pause"
    strength: int          — 1–5
    p_bounce: float        — 0.0–1.0
    expected_depth: float  — % expected pierce
    approach_style: str    — "flash" | "impulse" | "bleed" | "unknown"
    vol_ratio: float       — current_volume / avg_volume
    atr: float             — ATR in absolute price units
    current_price: float   — price at event moment
    timestamp: float       — time.time()
    # event-specific extra fields:
    breakout_vol_ratio: float  — only for "breakout" / "weak_breakout"
    sweep_vol_ratio: float     — only for "sweep"
    spike_ratio: int           — only for "volume_spike"
"""

from __future__ import annotations

import asyncio

_queue: asyncio.Queue = asyncio.Queue()


async def publish(event: dict) -> None:
    """Put an event on the bus. Never raises."""
    await _queue.put(event)


async def subscribe() -> dict:
    """Block until the next event is available, then return it."""
    return await _queue.get()


def subscribe_nowait() -> dict | None:
    """Non-blocking get. Returns None if the queue is empty."""
    try:
        return _queue.get_nowait()
    except asyncio.QueueEmpty:
        return None
