"""
Notifier Module.

Sends trading notifications to admin via Telegram. Used for order
confirmations, error alerts, and kill switch notifications.
"""

from __future__ import annotations

import logging
from datetime import datetime

from telethon import TelegramClient

logger = logging.getLogger('Notifier')


class Notifier:
    """
    Telegram notification sender for trading events.

    Attributes:
        client: Telethon client instance.
        chat_id: Admin chat ID to send notifications to.

    Example:
        >>> notifier = Notifier(client, admin_id)
        >>> await notifier.order_placed("NIFTY 24500 CE", 100, 125.50)
    """

    def __init__(self, client: TelegramClient, chat_id: int) -> None:
        """
        Initialize the notifier.

        Args:
            client: Connected Telethon client.
            chat_id: Telegram chat ID for notifications.
        """
        self.client = client
        self.chat_id = chat_id

    async def send(self, message: str) -> None:
        """
        Send a message to the admin chat.

        Args:
            message: Text message to send.
        """
        try:
            await self.client.send_message(self.chat_id, message)
        except Exception as e:
            logger.error(f'Telegram notify failed: {e}')

    async def started_bot(self) -> None:
        """Notify that the bot has started."""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        await self.send(f'🤖 Bot started at {now}')

    async def order_placed(self, symbol: str, qty: int, price: float) -> None:
        """
        Notify of successful order placement.

        Args:
            symbol: Trading symbol.
            qty: Order quantity.
            price: Entry price.
        """
        await self.send(f'✅ ORDER PLACED\n📌 {symbol}\nQty: {qty}\nPrice: {price:.2f}')

    async def retrying(self, symbol: str, reason: str) -> None:
        """
        Notify that order is being retried.

        Args:
            symbol: Trading symbol.
            reason: Retry reason (PRICE_LOW, PRICE_HIGH, etc).
        """
        await self.send(f'⏳ RETRYING ORDER\n📌 {symbol}\nReason: {reason}')

    async def order_failed(self, symbol: str, reason: str) -> None:
        """
        Notify of order failure.

        Args:
            symbol: Trading symbol.
            reason: Failure reason.
        """
        await self.send(f'❌ ORDER FAILED\n📌 {symbol}\nReason: {reason}')

    async def squared_off(self, symbol: str, reason: str) -> None:
        """
        Notify that a position was squared off.

        Args:
            symbol: Trading symbol.
            reason: Exit reason.
        """
        await self.send(f'🧯 SQUARED OFF\n📌 {symbol}\nReason: {reason}')

    async def kill_switch(self, pnl: float) -> None:
        """
        Notify that kill switch was triggered.

        Args:
            pnl: Current P&L that triggered the switch.
        """
        await self.send(f'🚨 KILL SWITCH TRIGGERED\nPnL: ₹{pnl:,.0f}')
