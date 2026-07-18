"""Reset Telegram webhook to allow polling."""

import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_PROXY = os.getenv("TELEGRAM_PROXY")


async def reset_webhook():
    """Delete webhook and allow polling."""
    print("🔧 Resetting Telegram webhook...")
    
    try:
        from aiogram import Bot
        
        # Create bot with proxy if configured
        if TELEGRAM_PROXY and TELEGRAM_PROXY.startswith("socks5://"):
            try:
                from aiohttp_socks import ProxyConnector
                from aiogram.client.session.aiohttp import AiohttpSession
                import aiohttp
                
                connector = ProxyConnector.from_url(TELEGRAM_PROXY)
                
                class ProxySession(AiohttpSession):
                    def __init__(self):
                        super().__init__()
                        self._connector = connector
                    
                    async def create_session(self) -> aiohttp.ClientSession:
                        return aiohttp.ClientSession(connector=self._connector)
                
                bot = Bot(token=TELEGRAM_TOKEN, session=ProxySession())
                print(f"✅ Using SOCKS5 proxy: {TELEGRAM_PROXY}")
            except ImportError:
                bot = Bot(token=TELEGRAM_TOKEN)
        else:
            bot = Bot(token=TELEGRAM_TOKEN)
        
        # Delete webhook
        result = await bot.delete_webhook(drop_pending_updates=True)
        
        if result:
            print("✅ Webhook deleted successfully")
            print("✅ Pending updates dropped")
            print("✅ Bot ready for polling")
        else:
            print("⚠️  Webhook deletion returned False")
        
        # Get webhook info to verify
        info = await bot.get_webhook_info()
        print(f"\n📊 Webhook status:")
        print(f"   URL: {info.url or 'None (polling mode)'}")
        print(f"   Pending updates: {info.pending_update_count}")
        
        await bot.session.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    return True


if __name__ == "__main__":
    asyncio.run(reset_webhook())
