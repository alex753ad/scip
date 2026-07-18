"""Bybit Demo REST client — лимитные входы, рыночный выход, стоп-лимит SL.

Используется только strategy2_live.py. Все запросы идут на demo endpoint.

Подпись Bybit v5:
  GET:  param_str = timestamp + api_key + recv_window + query_string
  POST: param_str = timestamp + api_key + recv_window + json_body_string
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from typing import Optional

import aiohttp

from config import BYBIT_API_KEY, BYBIT_API_SECRET
from logger import logger

BYBIT_DEMO_BASE = "https://api-demo.bybit.com"
RECV_WINDOW = "20000"


def _sign(payload_str: str, timestamp: str) -> str:
    param_str = timestamp + BYBIT_API_KEY + RECV_WINDOW + payload_str
    return hmac.new(BYBIT_API_SECRET.encode(), param_str.encode(), hashlib.sha256).hexdigest()


def _query_string(params: dict) -> str:
    return "&".join(f"{k}={v}" for k, v in sorted(params.items()))


def _post_headers(body: dict) -> dict:
    """Заголовки для POST — подпись по JSON-строке тела."""
    ts = str(int(time.time() * 1000))
    body_str = json.dumps(body, separators=(",", ":"))
    return {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-SIGN": _sign(body_str, ts),
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "Content-Type": "application/json",
    }


def _get_headers(params: dict) -> dict:
    """Заголовки для GET — подпись по query string параметров."""
    ts = str(int(time.time() * 1000))
    return {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-SIGN": _sign(_query_string(params), ts),
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "Content-Type": "application/json",
    }


async def _post(path: str, body: dict) -> dict:
    url = BYBIT_DEMO_BASE + path
    async with aiohttp.ClientSession() as session:
        body_str = json.dumps(body, separators=(",", ":"))
        async with session.post(url, data=body_str, headers=_post_headers(body)) as resp:
            data = await resp.json()
    return data


async def _get(path: str, params: dict) -> dict:
    url = BYBIT_DEMO_BASE + path
    qs = "?" + _query_string(params) if params else ""
    async with aiohttp.ClientSession() as session:
        async with session.get(url + qs, headers=_get_headers(params)) as resp:
            data = await resp.json()
    return data


# ── Leverage ──────────────────────────────────────────────────────────────────

async def set_leverage(symbol: str, leverage: int = 20) -> None:
    body = {
        "category": "linear",
        "symbol": symbol,
        "buyLeverage": str(leverage),
        "sellLeverage": str(leverage),
    }
    resp = await _post("/v5/position/set-leverage", body)
    if resp.get("retCode") not in (0, 110043):  # 110043 = already set
        logger.warning(
            "bybit set_leverage unexpected response",
            symbol=symbol,
            resp=resp,
        )


# ── Place orders ──────────────────────────────────────────────────────────────

async def place_limit_order(
    symbol: str,
    side: str,          # "Buy" | "Sell"
    qty: float,
    price: float,
    order_link_id: Optional[str] = None,
    reduce_only: bool = False,
) -> dict:
    """Разместить лимитный ордер. Возвращает {"orderId": ..., "orderLinkId": ...} или выбрасывает."""
    body = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Limit",
        "qty": str(qty),
        "price": str(price),
        "timeInForce": "GTC",
        "orderLinkId": order_link_id or str(uuid.uuid4()),
    }
    if reduce_only:
        body["reduceOnly"] = True

    resp = await _post("/v5/order/create", body)
    if resp.get("retCode") != 0:
        raise RuntimeError(
            f"place_limit_order failed: retCode={resp.get('retCode')} "
            f"retMsg={resp.get('retMsg')} symbol={symbol} price={price} qty={qty}"
        )
    return resp["result"]


async def place_market_order(
    symbol: str,
    side: str,          # "Buy" | "Sell"
    qty: float,
    reduce_only: bool = True,
) -> dict:
    """Рыночное закрытие позиции."""
    body = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": str(qty),
        "timeInForce": "IOC",
        "reduceOnly": reduce_only,
    }
    resp = await _post("/v5/order/create", body)
    if resp.get("retCode") != 0:
        raise RuntimeError(
            f"place_market_order failed: retCode={resp.get('retCode')} "
            f"retMsg={resp.get('retMsg')} symbol={symbol} qty={qty}"
        )
    return resp["result"]


async def place_stop_limit_order(
    symbol: str,
    side: str,
    qty: float,
    trigger_price: float,
    order_price: float,
    order_link_id: Optional[str] = None,
) -> dict:
    """Стоп-лимитный ордер (SL). trigger_price = уровень активации, order_price = цена ордера.

    ⚠ УСТАРЕЛ для SL лонга: на быстром проливе цена проскакивает лимит, ордер зависает
    неисполненным → reconcile добивает рынком сильно ниже (CLO -8$ кейс 08.07).
    Используй place_stop_market_order для защитных стопов.

    triggerDirection определяется по side:
      Sell (SL лонга) → 2 (Falling, триггер ниже текущей цены)
      Buy  (SL шорта) → 1 (Rising,  триггер выше  текущей цены)
    """
    trigger_direction = 2 if side == "Sell" else 1
    body = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Limit",
        "qty": str(qty),
        "price": str(order_price),
        "triggerPrice": str(trigger_price),
        "triggerBy": "LastPrice",
        "triggerDirection": trigger_direction,
        "timeInForce": "GTC",
        "reduceOnly": True,
        "orderLinkId": order_link_id or str(uuid.uuid4()),
    }
    resp = await _post("/v5/order/create", body)
    if resp.get("retCode") != 0:
        raise RuntimeError(
            f"place_stop_limit_order failed: retCode={resp.get('retCode')} "
            f"retMsg={resp.get('retMsg')} symbol={symbol} trigger={trigger_price}"
        )
    return resp["result"]


async def place_stop_market_order(
    symbol: str,
    side: str,
    qty: float,
    trigger_price: float,
    order_link_id: Optional[str] = None,
) -> dict:
    """[FIX STOP-MARKET] Стоп-МАРКЕТ ордер для защитных стопов (SL лонга/шорта).

    Гарантирует исполнение при любом гэпе или быстром проливе: нет лимитной цены →
    нет риска «лимит повис ниже рынка». Цена хуже на slippage (~0.05–0.1% на
    ликвидных альтах), но зато всегда исполняется.

    Замена place_stop_limit_order для всех биржевых SL в S2Live.
    """
    trigger_direction = 2 if side == "Sell" else 1
    body = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": str(qty),
        "triggerPrice": str(trigger_price),
        "triggerBy": "LastPrice",
        "triggerDirection": trigger_direction,
        "timeInForce": "IOC",
        "reduceOnly": True,
        "orderLinkId": order_link_id or str(uuid.uuid4()),
    }
    resp = await _post("/v5/order/create", body)
    if resp.get("retCode") != 0:
        raise RuntimeError(
            f"place_stop_market_order failed: retCode={resp.get('retCode')} "
            f"retMsg={resp.get('retMsg')} symbol={symbol} trigger={trigger_price}"
        )
    return resp["result"]


async def cancel_order(symbol: str, order_id: str) -> bool:
    """Отменить ордер по orderId. Возвращает True если успешно."""
    body = {
        "category": "linear",
        "symbol": symbol,
        "orderId": order_id,
    }
    resp = await _post("/v5/order/cancel", body)
    if resp.get("retCode") not in (0, 20001):   # 20001 = уже исполнен/отменён
        logger.warning(
            "bybit cancel_order unexpected response",
            symbol=symbol,
            order_id=order_id,
            resp=resp,
        )
        return False
    return True


async def cancel_all_orders(symbol: str) -> None:
    """Отменить все открытые ордера по символу."""
    body = {"category": "linear", "symbol": symbol}
    resp = await _post("/v5/order/cancel-all", body)
    if resp.get("retCode") != 0:
        logger.warning(
            "bybit cancel_all_orders failed",
            symbol=symbol,
            resp=resp,
        )


# ── Position info ─────────────────────────────────────────────────────────────

async def get_position(symbol: str) -> Optional[dict]:
    """Вернуть текущую позицию по символу или None."""
    params = {"category": "linear", "symbol": symbol}
    resp = await _get("/v5/position/list", params)
    if resp.get("retCode") != 0:
        logger.warning("bybit get_position failed", symbol=symbol, resp=resp)
        return None
    items = resp.get("result", {}).get("list", [])
    for item in items:
        if float(item.get("size", 0)) > 0:
            return item
    return None


async def get_order_status(symbol: str, order_id: str) -> Optional[str]:
    """Вернуть статус ордера: 'Filled', 'New', 'Cancelled', etc."""
    params = {
        "category": "linear",
        "symbol": symbol,
        "orderId": order_id,
    }
    resp = await _get("/v5/order/realtime", params)
    if resp.get("retCode") != 0:
        return None
    items = resp.get("result", {}).get("list", [])
    return items[0]["orderStatus"] if items else None


async def get_open_orders_for_symbol(symbol: str) -> dict[str, str]:
    """Вернуть словарь {orderId: orderStatus} для всех активных ордеров по символу.

    Один запрос вместо N — используется в _sync_grid_fills для проверки fills.
    Статусы: 'New', 'PartiallyFilled', 'Filled', 'Cancelled', 'Rejected'.
    """
    params = {"category": "linear", "symbol": symbol, "limit": "50"}
    resp = await _get("/v5/order/realtime", params)
    if resp.get("retCode") != 0:
        logger.warning("bybit get_open_orders_for_symbol failed", symbol=symbol, resp=resp)
        return {}
    items = resp.get("result", {}).get("list", [])
    return {item["orderId"]: item["orderStatus"] for item in items}


async def get_order_history(symbol: str, limit: int = 50) -> dict[str, dict]:
    """Вернуть словарь {orderId: order_dict} из истории ордеров (исполненные/отменённые).

    Нужно для проверки fills ордеров которых уже нет в realtime (уже Filled/Cancelled).
    """
    params = {"category": "linear", "symbol": symbol, "limit": str(limit)}
    resp = await _get("/v5/order/history", params)
    if resp.get("retCode") != 0:
        logger.warning("bybit get_order_history failed", symbol=symbol, resp=resp)
        return {}
    items = resp.get("result", {}).get("list", [])
    return {item["orderId"]: item for item in items}


async def get_instrument_info(symbol: str) -> Optional[dict]:
    """Вернуть lotSizeFilter для расчёта минимального qty и шага."""
    params = {"category": "linear", "symbol": symbol}
    resp = await _get("/v5/market/instruments-info", params)
    if resp.get("retCode") != 0:
        return None
    items = resp.get("result", {}).get("list", [])
    return items[0] if items else None


async def get_best_bid(symbol: str) -> Optional[float]:
    """[H1] Лучший бид по символу из тикера. None при ошибке/отсутствии."""
    params = {"category": "linear", "symbol": symbol}
    resp = await _get("/v5/market/tickers", params)
    if resp.get("retCode") != 0:
        logger.warning("bybit get_best_bid failed", symbol=symbol, resp=resp)
        return None
    items = resp.get("result", {}).get("list", [])
    if not items:
        return None
    raw = items[0].get("bid1Price")
    try:
        return float(raw) if raw else None
    except (ValueError, TypeError):
        return None


async def get_orderbook(symbol: str, limit: int = 50) -> Optional[dict]:
    """[L2] Снимок стакана на момент сигнала. limit ∈ {1,50,200,500} для linear.

    Возвращает {"b": [[price, size], ...], "a": [[price, size], ...]} (как у Bybit:
    b — биды по убыванию цены, a — аски по возрастанию) или None при ошибке.
    """
    params = {"category": "linear", "symbol": symbol, "limit": str(limit)}
    resp = await _get("/v5/market/orderbook", params)
    if resp.get("retCode") != 0:
        logger.warning("bybit get_orderbook failed", symbol=symbol, resp=resp)
        return None
    result = resp.get("result")
    if not result or not result.get("b") or not result.get("a"):
        return None
    return result


async def get_tickers(symbol: str) -> Optional[dict]:
    """[OI/funding] Тикер linear-символа. Содержит fundingRate, openInterest,
    openInterestValue (USD), nextFundingTime. Возвращает dict тикера или None.
    """
    params = {"category": "linear", "symbol": symbol}
    resp = await _get("/v5/market/tickers", params)
    if resp.get("retCode") != 0:
        logger.warning("bybit get_tickers failed", symbol=symbol, resp=resp)
        return None
    items = resp.get("result", {}).get("list", [])
    return items[0] if items else None


async def get_open_interest_history(
    symbol: str, interval_time: str = "5min", limit: int = 13
) -> list[dict]:
    """[OI] История открытого интереса. interval_time ∈ {5min,15min,30min,1h,4h,1d}
    (поминутной истории у Bybit нет). limit=13×5min ≈ 65 мин — хватает на 1ч-дельту.
    Возвращает list из {openInterest, timestamp} (порядок не гарантирован — сортируй
    по timestamp) или [] при ошибке.
    """
    params = {
        "category": "linear",
        "symbol": symbol,
        "intervalTime": interval_time,
        "limit": str(limit),
    }
    resp = await _get("/v5/market/open-interest", params)
    if resp.get("retCode") != 0:
        logger.warning("bybit get_open_interest_history failed", symbol=symbol, resp=resp)
        return []
    return resp.get("result", {}).get("list", [])


async def get_executions(symbol: str, order_id: str, limit: int = 20) -> list[dict]:
    """Вернуть список исполнений (trades) по конкретному orderId.

    Используется для получения реальной цены и qty исполнения рыночного ордера.
    Возвращает пустой список при ошибке.
    """
    params = {
        "category": "linear",
        "symbol": symbol,
        "orderId": order_id,
        "limit": str(limit),
    }
    resp = await _get("/v5/execution/list", params)
    if resp.get("retCode") != 0:
        logger.warning("bybit get_executions failed", symbol=symbol, order_id=order_id, resp=resp)
        return []
    return resp.get("result", {}).get("list", [])


async def get_executions_by_symbol(symbol: str, start_ms: Optional[int] = None,
                                   limit: int = 100) -> list[dict]:
    """Все исполнения по символу с момента start_ms (не по одному orderId).

    Источник истины для восстановления сделки, когда бот не успел увидеть позицию
    (быстрый круг залив→SL закрылся между опросами). Возвращает пустой список при ошибке.
    """
    params = {"category": "linear", "symbol": symbol, "limit": str(limit)}
    if start_ms:
        params["startTime"] = str(int(start_ms))
    resp = await _get("/v5/execution/list", params)
    if resp.get("retCode") != 0:
        logger.warning("bybit get_executions_by_symbol failed", symbol=symbol, resp=resp)
        return []
    return resp.get("result", {}).get("list", [])
