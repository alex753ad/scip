"""
Тест подключения к Bybit Demo и базовых операций.
Запуск: python test_bybit.py

Что проверяет:
  1. Подпись (авторизация)
  2. Баланс демосчёта
  3. Параметры инструмента (BTCUSDT)
  4. Установка плеча x20
  5. Размещение лимитного ордера далеко от рынка
  6. Проверка статуса ордера
  7. Отмена ордера
  8. Стоп-лимитный ордер (SL)
  9. Отмена SL ордера
"""

import asyncio
import sys
import os

# Добавляем корень проекта в путь чтобы импортировать config и trading
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import BYBIT_API_KEY, BYBIT_API_SECRET
from trading.bybit_client import (
    cancel_all_orders,
    cancel_order,
    get_instrument_info,
    get_order_status,
    get_position,
    place_limit_order,
    place_stop_limit_order,
    set_leverage,
    _get,
)

SYMBOL = "BTCUSDT"
SEP = "-" * 50


def ok(msg): print(f"  ✅ {msg}")
def fail(msg): print(f"  ❌ {msg}")
def info(msg): print(f"  ℹ️  {msg}")


async def test_auth():
    print(f"\n{SEP}")
    print("1. Авторизация и баланс")
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        fail("BYBIT_API_KEY или BYBIT_API_SECRET не заданы в .env")
        return False
    info(f"API Key: {BYBIT_API_KEY[:6]}...{BYBIT_API_KEY[-4:]}")
    try:
        resp = await _get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        if resp.get("retCode") != 0:
            fail(f"retCode={resp.get('retCode')} retMsg={resp.get('retMsg')}")
            return False
        coins = resp["result"]["list"][0].get("coin", [])
        usdt = next((c for c in coins if c["coin"] == "USDT"), None)
        if usdt:
            ok(f"Баланс USDT: {float(usdt.get('walletBalance', 0)):.2f}")
        else:
            ok("Авторизация успешна (USDT не найден в балансе — возможно нет средств)")
        return True
    except Exception as e:
        fail(f"Ошибка: {e}")
        return False


async def test_instrument():
    print(f"\n{SEP}")
    print(f"2. Параметры инструмента {SYMBOL}")
    try:
        info_data = await get_instrument_info(SYMBOL)
        if info_data is None:
            fail("Инструмент не найден")
            return None
        lot = info_data.get("lotSizeFilter", {})
        price = info_data.get("priceFilter", {})
        ok(f"qtyStep={lot.get('qtyStep')} minQty={lot.get('minOrderQty')} tickSize={price.get('tickSize')}")
        return info_data
    except Exception as e:
        fail(f"Ошибка: {e}")
        return None


async def test_leverage():
    print(f"\n{SEP}")
    print(f"3. Установка плеча x20 для {SYMBOL}")
    try:
        await set_leverage(SYMBOL, 20)
        ok("Плечо x20 установлено")
        return True
    except Exception as e:
        fail(f"Ошибка: {e}")
        return False


async def test_limit_order(instrument):
    print(f"\n{SEP}")
    print(f"4. Лимитный ордер Buy {SYMBOL} (далеко от рынка)")
    try:
        # Получить текущую цену
        resp = await _get("/v5/market/tickers", {"category": "linear", "symbol": SYMBOL})
        last_price = float(resp["result"]["list"][0]["lastPrice"])
        info(f"Текущая цена: {last_price}")

        # Ордер на 30% ниже рынка — никогда не исполнится
        test_price = round(last_price * 0.70, 1)
        qty = 0.001  # минимальный размер

        info(f"Размещаю Buy limit qty={qty} price={test_price}")
        result = await place_limit_order(
            symbol=SYMBOL,
            side="Buy",
            qty=qty,
            price=test_price,
            order_link_id="test_bybit_limit_001",
        )
        order_id = result["orderId"]
        ok(f"Ордер создан: orderId={order_id}")
        return order_id, last_price
    except Exception as e:
        fail(f"Ошибка: {e}")
        return None, None


async def test_order_status(order_id):
    print(f"\n{SEP}")
    print(f"5. Статус ордера {order_id[:12]}...")
    try:
        status = await get_order_status(SYMBOL, order_id)
        if status:
            ok(f"Статус: {status}")
        else:
            fail("Статус не получен")
    except Exception as e:
        fail(f"Ошибка: {e}")


async def test_cancel(order_id):
    print(f"\n{SEP}")
    print(f"6. Отмена ордера {order_id[:12]}...")
    try:
        success = await cancel_order(SYMBOL, order_id)
        if success:
            ok("Ордер отменён")
        else:
            fail("Отмена не подтверждена")
    except Exception as e:
        fail(f"Ошибка: {e}")


async def test_stop_limit(last_price):
    print(f"\n{SEP}")
    print(f"7. Стоп-лимитный ордер SL (Sell) {SYMBOL}")
    try:
        # SL далеко ниже рынка — не сработает
        trigger = round(last_price * 0.60, 1)
        order_price = round(last_price * 0.595, 1)
        qty = 0.001

        info(f"trigger={trigger} order_price={order_price} qty={qty}")
        result = await place_stop_limit_order(
            symbol=SYMBOL,
            side="Sell",
            qty=qty,
            trigger_price=trigger,
            order_price=order_price,
            order_link_id="test_bybit_sl_001",
        )
        sl_order_id = result["orderId"]
        ok(f"SL ордер создан: orderId={sl_order_id}")

        # Отменить
        await cancel_order(SYMBOL, sl_order_id)
        ok("SL ордер отменён")
    except Exception as e:
        fail(f"Ошибка: {e}")


async def test_position():
    print(f"\n{SEP}")
    print(f"8. Текущая позиция {SYMBOL}")
    try:
        pos = await get_position(SYMBOL)
        if pos:
            info(f"Открытая позиция: size={pos.get('size')} side={pos.get('side')} entryPrice={pos.get('avgPrice')}")
        else:
            ok("Открытых позиций нет")
    except Exception as e:
        fail(f"Ошибка: {e}")


async def main():
    print("=" * 50)
    print("Bybit Demo — тест подключения и ордеров")
    print("=" * 50)

    if not await test_auth():
        print("\n⛔ Авторизация провалилась, остальные тесты пропущены")
        return

    instrument = await test_instrument()
    await test_leverage()

    order_id, last_price = await test_limit_order(instrument)
    if order_id:
        await test_order_status(order_id)
        await test_cancel(order_id)

    if last_price:
        await test_stop_limit(last_price)

    await test_position()

    print(f"\n{SEP}")
    print("Готово. Все ✅ — бот готов к торговле на Bybit Demo.")
    print(SEP)


if __name__ == "__main__":
    asyncio.run(main())
