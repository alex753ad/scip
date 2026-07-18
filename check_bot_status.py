"""Check if bot is running somewhere else."""

import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_PROXY = os.getenv("TELEGRAM_PROXY")


async def check_status():
    """Check bot status and webhook info."""
    print("🔍 Checking bot status...\n")
    
    try:
        from aiogram import Bot
        
        # Create bot with proxy
        if TELEGRAM_PROXY and TELEGRAM_PROXY.startswith("socks5://"):
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
        else:
            bot = Bot(token=TELEGRAM_TOKEN)
        
        # Get bot info
        me = await bot.get_me()
        print(f"🤖 Bot: @{me.username} (ID: {me.id})")
        print(f"   Name: {me.first_name}")
        print()
        
        # Get webhook info
        info = await bot.get_webhook_info()
        print(f"📡 Connection mode:")
        
        if info.url:
            print(f"   ⚠️  WEBHOOK MODE (Railway/Cloud)")
            print(f"   URL: {info.url}")
            print(f"   Pending updates: {info.pending_update_count}")
            print()
            print("❌ Cannot run locally while webhook is active!")
            print()
            print("💡 To run locally:")
            print("   1. Stop Railway deployment")
            print("   2. Run: python reset_webhook.py")
            print("   3. Run: python main.py")
        else:
            print(f"   ✅ POLLING MODE (Local)")
            print(f"   Pending updates: {info.pending_update_count}")
            print()
            
            if info.pending_update_count > 0:
                print("⚠️  There are pending updates")
                print("   Another bot instance might be running")
                print()
                print("💡 To fix:")
                print("   1. Stop all python processes")
                print("   2. Run: python reset_webhook.py")
            else:
                print("✅ Bot is ready for local polling")
        
        await bot.session.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(check_status())
