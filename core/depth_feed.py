"""
Depth Feed Module.

Connects to Dhan's 20-level depth WebSocket feed and parses binary
market depth data. Provides real-time bid/ask levels for order book
imbalance analysis.

Protocol:
- WebSocket connection with auto-reconnect
- Binary message format with header + depth levels
- Subscription management for multiple instruments
"""

from __future__ import annotations

import asyncio
import json
import logging
import struct
from typing import Callable, Dict, List, Optional, Protocol, runtime_checkable

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger('DepthFeed')

# Dhan WebSocket endpoint
DEPTH_ENDPOINT = 'wss://depth-api-feed.dhan.co/twentydepth'

# Request codes
REQ_SUBSCRIBE = 23
REQ_UNSUBSCRIBE = 24
REQ_DISCONNECT = 12

# Feed message types
FEED_DEPTH_BID = 41
FEED_DEPTH_ASK = 51

# Binary format constants
HEADER_FMT = '<HBBII'  # msg_len, feed_code, exch_seg, sec_id, reserved
HEADER_SIZE = struct.calcsize(HEADER_FMT)
DEPTH_LEVEL_SIZE = 16  # <dII (price, qty, orders)

# Type aliases
DepthCallback = Callable[[Dict[str, object]], None]
DepthLevel = Dict[str, float | int]


@runtime_checkable
class WSConnection(Protocol):
    """Protocol for WebSocket connection interface."""

    async def send(self, message: str | bytes, text: bool | None = None) -> None:
        """Send a message over the WebSocket."""
        ...

    async def close(self) -> None:
        """Close the WebSocket connection."""
        ...


class DepthFeed:
    """
    Real-time 20-level depth feed from Dhan.

    Maintains a persistent WebSocket connection with automatic reconnection.
    Parses binary depth data and invokes registered callbacks.

    Attributes:
        url: WebSocket URL with authentication parameters.

    Example:
        >>> feed = DepthFeed(token="xxx", client_id="yyy")
        >>> feed.register_callback(on_depth_update)
        >>> await feed.connect()  # Runs until disconnect
    """

    SUBSCRIPTION_CHUNK_SIZE = 50  # Max instruments per subscription request

    def __init__(self, token: str, client_id: str) -> None:
        """
        Initialize the depth feed.

        Args:
            token: Dhan access token.
            client_id: Dhan client ID.
        """
        self.url = f'{DEPTH_ENDPOINT}?version=2&token={token}&clientId={client_id}&authType=2'
        self._ws: Optional[WSConnection] = None
        self._stop = False
        self._callbacks: List[DepthCallback] = []
        self._subscriptions: Dict[str, Dict[str, str]] = {}

    # =========================================================================
    # Public API
    # =========================================================================

    def register_callback(self, callback: DepthCallback) -> None:
        """
        Register a callback for depth updates.

        Args:
            callback: Function called with parsed depth data dict containing:
                - security_id: str
                - exchange_segment: int
                - side: 'bid' or 'ask'
                - levels: List of {price, qty, orders}
        """
        self._callbacks.append(callback)

    async def connect(self) -> None:
        """
        Main connection loop with automatic reconnection.

        Runs indefinitely until disconnect() is called. Automatically
        resubscribes to instruments after reconnection.
        """
        while not self._stop:
            try:
                logger.info('Connecting to Depth Feed...')
                async with websockets.connect(self.url, ping_interval=10, max_size=None) as ws:
                    self._ws = ws
                    logger.info('Depth Feed connected')

                    # Resubscribe on reconnect
                    if self._subscriptions:
                        await self._send_subscription_packet(list(self._subscriptions.values()))

                    # Process messages
                    async for message in ws:
                        if isinstance(message, (bytes, bytearray)):
                            self._parse_binary(message)
                        elif isinstance(message, str):
                            logger.debug(f'Text frame: {message}')

            except ConnectionClosed as e:
                if self._stop:
                    break
                logger.warning(f'Connection closed: {e}')
                await asyncio.sleep(2)

            except Exception as e:
                if self._stop:
                    break
                logger.error(f'Feed error: {e}', exc_info=True)
                await asyncio.sleep(2)

            finally:
                self._ws = None

    async def subscribe(self, instruments: List[Dict[str, str]]) -> None:
        """
        Subscribe to depth data for instruments.

        Args:
            instruments: List of dicts with 'ExchangeSegment' and 'SecurityId'.

        Raises:
            ValueError: If instrument dict is missing required keys.
        """
        for inst in instruments:
            if 'ExchangeSegment' not in inst or 'SecurityId' not in inst:
                raise ValueError(f'Invalid instrument: {inst}')
            key = f'{inst["ExchangeSegment"]}:{inst["SecurityId"]}'
            self._subscriptions[key] = inst

        if self._ws is not None:
            try:
                await self._send_subscription_packet(instruments)
            except Exception:
                logger.info(f'Queued {len(instruments)} subscriptions')
        else:
            logger.info(f'Queued {len(instruments)} subscriptions (connecting)')

    async def unsubscribe(self, instruments: List[Dict[str, str]]) -> None:
        """
        Unsubscribe from depth data for instruments.

        Args:
            instruments: List of dicts with 'ExchangeSegment' and 'SecurityId'.
        """
        for inst in instruments:
            key = f'{inst["ExchangeSegment"]}:{inst["SecurityId"]}'
            self._subscriptions.pop(key, None)

        if self._ws is None:
            logger.info(f'Queued {len(instruments)} unsubscriptions')
            return

        payload = {
            'RequestCode': REQ_UNSUBSCRIBE,
            'InstrumentCount': len(instruments),
            'InstrumentList': instruments,
        }

        try:
            await self._ws.send(json.dumps(payload))
            logger.info(f'🧹 Unsubscribed {len(instruments)} symbols')
        except Exception as e:
            logger.error(f'Unsubscribe failed: {e}')

    async def disconnect(self) -> None:
        """Gracefully stop the feed and close connection."""
        self._stop = True

        if self._ws:
            try:
                await self._ws.send(json.dumps({'RequestCode': REQ_DISCONNECT}))
            except Exception:
                pass
            finally:
                try:
                    await self._ws.close()
                except Exception:
                    pass

    # =========================================================================
    # Internal Methods
    # =========================================================================

    async def _send_subscription_packet(self, instruments: List[Dict[str, str]]) -> None:
        """Send subscription request in chunks."""
        if self._ws is None:
            return

        for i in range(0, len(instruments), self.SUBSCRIPTION_CHUNK_SIZE):
            chunk = instruments[i : i + self.SUBSCRIPTION_CHUNK_SIZE]
            payload = {
                'RequestCode': REQ_SUBSCRIBE,
                'InstrumentCount': len(chunk),
                'InstrumentList': chunk,
            }

            try:
                await self._ws.send(json.dumps(payload))
                logger.info(f'📡 Subscribed to {len(chunk)} symbols')
            except ConnectionClosed as e:
                logger.warning(f'Subscription failed (closed): {e}')
                raise
            except Exception as e:
                logger.error(f'Subscription error: {e}')
                raise

    def _parse_binary(self, data: bytes) -> None:
        """Parse binary depth feed message."""
        offset = 0
        total_len = len(data)

        while offset + HEADER_SIZE <= total_len:
            try:
                msg_len, feed_code, exch_seg, sec_id, _ = struct.unpack_from(
                    HEADER_FMT, data, offset
                )

                end = offset + msg_len
                if end > total_len:
                    break

                payload = data[offset + HEADER_SIZE : end]
                offset = end

                if feed_code not in (FEED_DEPTH_BID, FEED_DEPTH_ASK):
                    continue

                levels = self._parse_depth_levels(payload)
                if not levels:
                    continue

                parsed = {
                    'security_id': str(sec_id),
                    'exchange_segment': int(exch_seg),
                    'side': 'bid' if feed_code == FEED_DEPTH_BID else 'ask',
                    'levels': levels,
                }

                self._invoke_callbacks(parsed)

            except Exception as e:
                logger.error(f'Parse error: {e}', exc_info=True)
                break

    def _invoke_callbacks(self, data: Dict[str, object]) -> None:
        """Invoke all registered callbacks with parsed data."""
        for callback in self._callbacks:
            try:
                callback(data)
            except Exception as e:
                logger.error(f'Callback error: {e}', exc_info=True)

    @staticmethod
    def _parse_depth_levels(payload: bytes) -> List[DepthLevel]:
        """
        Parse binary depth level data.

        Args:
            payload: Binary data containing packed depth levels.

        Returns:
            List of depth level dicts with price, qty, orders.
        """
        if len(payload) % DEPTH_LEVEL_SIZE != 0:
            logger.warning(f'Unexpected payload size: {len(payload)}')

        levels: List[DepthLevel] = []

        for i in range(0, len(payload), DEPTH_LEVEL_SIZE):
            if i + DEPTH_LEVEL_SIZE > len(payload):
                break

            price, qty, orders = struct.unpack_from('<dII', payload, i)
            levels.append({'price': float(price), 'qty': int(qty), 'orders': int(orders)})

        return levels
