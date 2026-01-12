from __future__ import annotations

import logging
from datetime import datetime

from telethon import TelegramClient

logger = logging.getLogger('Notifier')


class Notifier:
    def __init__(self, client: TelegramClient, chat_id: int):
        self.client = client
        self.chat_id = chat_id

    async def send(self, message: str):
        try:
            await self.client.send_message(self.chat_id, message)
        except Exception as e:
            logger.error(f'Telegram notify failed: {e}')

    async def started_bot(self):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        await self.send(f'Bot stared at {now}')

    async def order_placed(self, symbol: str, qty: int, price: float):
        await self.send(f'✅ ORDER PLACED\n📌 {symbol}\nQty: {qty}\nPrice: {price}')

    async def retrying(self, symbol: str, reason: str):
        await self.send(f'⏳ RETRYING ORDER\n📌 {symbol}\nReason: {reason}')

    async def order_failed(self, symbol: str, reason: str):
        await self.send(f'❌ ORDER FAILED\n📌 {symbol}\nReason: {reason}')

    async def squared_off(self, symbol: str, reason: str):
        await self.send(f'🧯 SQUARED OFF\n📌 {symbol}\nReason: {reason}')

    async def kill_switch(self, pnl: float):
        await self.send(f'🚨 KILL SWITCH TRIGGERED\nPnL: {pnl}')
