"""bybit_client_s5.py — ИЗОЛИРОВАННЫЙ клиент Bybit для стратегии S5.

Вариант Б3: S2 работает через trading/bybit_client.py (ключ основного аккаунта),
S5 — через ЭТОТ модуль под ключом ОТДЕЛЬНОГО sub-аккаунта. Полная изоляция:
разные ключи, разные балансы, S5 физически не может тронуть позиции S2.

Ключи читаются из окружения (НЕ из config, чтобы не смешивать с S2):
    BYBIT_S5_API_KEY
    BYBIT_S5_API_SECRET
    BYBIT_S5_DEMO=true|false   (по умолчанию demo)

В коде секретов нет. Если переменные не заданы — модуль поднимает понятную ошибку
при первом запросе, торговля S5 просто не стартует (S2 не затрагивается).

Реализованы только методы, нужные S5 (один вход, без сетки): set_leverage,
place_market_order, place_stop_market_order, cancel_all_orders, get_position,
get_instrument_info, get_best_bid, get_executions_by_symbol.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import uuid
from typing import Optional

import aiohttp

from logger import logger

_DEMO = os.getenv("BYBIT_S5_DEMO", "true").lower() != "false"
BASE_URL = "https://api-demo.bybit.com" if _DEMO else "https://api.bybit.com"
RECV_WINDOW = "20000"


def _keys() -> tuple[str, str]:
    k = os.getenv("BYBIT_S5_API_KEY")
    s = os.getenv("BYBIT_S5_API_SECRET")
    if not k or not s:
        raise RuntimeError(
            "S5 API keys not set: экспортируй BYBIT_S5_API_KEY и BYBIT_S5_API_SECRET "
            "(ключ отдельного Bybit sub-аккаунта). S5-live не стартует без них."
        )
    return k, s


def _sign(payload_str: str, timestamp: str) -> str:
    api_key, api_secret = _keys()
    param_str = timestamp + api_key + RECV_WINDOW + payload_str
    return hmac.new(api_secret.encode(), param_str.encode(), hashlib.sha256).hexdigest()


def _query_string(params: dict) -> str:
    return "&".join(f"{k}={v}" for k, v in sorted(params.items()))


def _post_headers(body: dict) -> dict:
    ts = str(int(time.time() * 1000))
    body_str = json.dumps(body, separators=(",", ":"))
    api_key, _ = _keys()
    return {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": _sign(body_str, ts),
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "Content-Type": "application/json",
    }


def _get_headers(params: dict) -> dict:
    ts = str(int(time.time() * 1000))
    api_key, _ = _keys()
    return {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": _sign(_query_string(params), ts),
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "Content-Type": "application/json",
    }


async def _post(path: str, body: dict) -> dict:
    url = BASE_URL + path
    async with aiohttp.ClientSession() as session:
        body_str = json.dumps(body, separators=(",", ":"))
        async with session.post(url, data=body_str, headers=_post_headers(body)) as resp:
            return await resp.json()


async def _get(path: str, params: dict) -> dict:
    url = BASE_URL + path
    qs = "?" + _query_string(params) if params else ""
    async with aiohttp.ClientSession() as session:
        async with session.get(url + qs, headers=_get_headers(params)) as resp:
            return await resp.json()


# ── Leverage ──────────────────────────────────────────────────────────────────

async def set_leverage(symbol: str, leverage: int = 20) -> None:
    body = {"category": "linear", "symbol": symbol,
            "buyLeverage": str(leverage), "sellLeverage": str(leverage)}
    resp = await _post("/v5/position/set-leverage", body)
    if resp.get("retCode") not in (0, 110043):  # 110043 = already set
        logger.warning("S5 set_leverage unexpected response", symbol=symbol, resp=resp)


# ── Orders ────────────────────────────────────────────────────────────────────

async def place_market_order(symbol: str, side: str, qty: float,
                             reduce_only: bool = False,
                             order_link_id: Optional[str] = None) -> dict:
    body = {
        "category": "linear", "symbol": symbol, "side": side,
        "orderType": "Market", "qty": str(qty),
        "reduceOnly": reduce_only, "timeInForce": "IOC",
        "orderLinkId": order_link_id or str(uuid.uuid4()),
    }
    resp = await _post("/v5/order/create", body)
    if resp.get("retCode") != 0:
        raise RuntimeError(f"S5 place_market_order failed: retCode={resp.get('retCode')} "
                           f"retMsg={resp.get('retMsg')} symbol={symbol} qty={qty}")
    return resp["result"]


async def place_stop_market_order(symbol: str, side: str, qty: float,
                                  trigger_price: float,
                                  order_link_id: Optional[str] = None) -> dict:
    """Стоп-МАРКЕТ (защитный SL). Sell → триггер ниже (Falling)."""
    trigger_direction = 2 if side == "Sell" else 1
    body = {
        "category": "linear", "symbol": symbol, "side": side,
        "orderType": "Market", "qty": str(qty),
        "triggerPrice": str(trigger_price), "triggerBy": "LastPrice",
        "triggerDirection": trigger_direction, "timeInForce": "IOC",
        "reduceOnly": True, "orderLinkId": order_link_id or str(uuid.uuid4()),
    }
    resp = await _post("/v5/order/create", body)
    if resp.get("retCode") != 0:
        raise RuntimeError(f"S5 place_stop_market_order failed: retCode={resp.get('retCode')} "
                           f"retMsg={resp.get('retMsg')} symbol={symbol} trigger={trigger_price}")
    return resp["result"]


async def cancel_all_orders(symbol: str) -> None:
    body = {"category": "linear", "symbol": symbol}
    resp = await _post("/v5/order/cancel-all", body)
    if resp.get("retCode") != 0:
        logger.warning("S5 cancel_all_orders unexpected", symbol=symbol, resp=resp)


# ── Reads ─────────────────────────────────────────────────────────────────────

async def get_position(symbol: str) -> Optional[dict]:
    resp = await _get("/v5/position/list", {"category": "linear", "symbol": symbol})
    if resp.get("retCode") != 0:
        return None
    lst = resp.get("result", {}).get("list", [])
    if not lst:
        return None
    p = lst[0]
    return {"size": float(p.get("size", 0) or 0),
            "avg_price": float(p.get("avgPrice", 0) or 0),
            "side": p.get("side", "")}


async def get_instrument_info(symbol: str) -> Optional[dict]:
    resp = await _get("/v5/market/instruments-info", {"category": "linear", "symbol": symbol})
    if resp.get("retCode") != 0:
        return None
    lst = resp.get("result", {}).get("list", [])
    return lst[0] if lst else None


async def get_best_bid(symbol: str) -> Optional[float]:
    resp = await _get("/v5/market/orderbook", {"category": "linear", "symbol": symbol, "limit": 1})
    if resp.get("retCode") != 0:
        return None
    bids = resp.get("result", {}).get("b", [])
    return float(bids[0][0]) if bids else None


async def get_executions_by_symbol(symbol: str, start_ms: Optional[int] = None,
                                   limit: int = 50) -> list[dict]:
    params = {"category": "linear", "symbol": symbol, "limit": limit}
    if start_ms:
        params["startTime"] = start_ms
    resp = await _get("/v5/execution/list", params)
    if resp.get("retCode") != 0:
        return []
    return resp.get("result", {}).get("list", [])
