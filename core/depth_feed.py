from __future__ import annotations

import asyncio
import json
import logging
import struct
from typing import Callable, Dict, List, Optional, Protocol, runtime_checkable

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger('DepthFeed')

DEPTH_ENDPOINT_20 = 'wss://depth-api-feed.dhan.co/twentydepth'

REQ_SUBSCRIBE = 23
REQ_UNSUBSCRIBE = 24
REQ_DISCONNECT = 12

FEED_DEPTH_BID = 41
FEED_DEPTH_ASK = 51

HEADER_FMT = '<HBBII'
HEADER_SIZE = struct.calcsize(HEADER_FMT)
DEPTH_LEVEL_SIZE = 16  # <dII


@runtime_checkable
class WSConnection(Protocol):
    async def send(self, message: str | bytes, text: bool | None = None) -> None: ...
    async def close(self) -> None: ...


class DepthFeed:
    def __init__(self, token: str, client_id: str) -> None:
        self.url = f'{DEPTH_ENDPOINT_20}?version=2&token={token}&clientId={client_id}&authType=2'

        self._ws: Optional[WSConnection] = None
        self._stop: bool = False

        self._callbacks: List[Callable[[Dict[str, object]], None]] = []
        self._subscriptions: Dict[str, Dict[str, str]] = {}

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def register_callback(self, cb: Callable[[Dict[str, object]], None]) -> None:
        self._callbacks.append(cb)

    async def connect(self) -> None:
        """Main connection + reconnect loop."""
        while not self._stop:
            try:
                logger.info('Connecting to Depth Feed...')
                async with websockets.connect(self.url, ping_interval=10, max_size=None) as ws:
                    self._ws = ws
                    logger.info('Depth Feed Connected')

                    # Re-subscribe on reconnect
                    if self._subscriptions:
                        await self._send_subscription_packet(list(self._subscriptions.values()))

                    async for message in ws:
                        if isinstance(message, (bytes, bytearray)):
                            self._parse_binary(message)
                        elif isinstance(message, str):
                            logger.debug(f'Text frame: {message}')

            except Exception as e:
                if self._stop:
                    break

                logger.error(f'Depth Feed Error: {e}', exc_info=True)
                await asyncio.sleep(2)

            finally:
                self._ws = None

    async def subscribe(self, instruments: List[Dict[str, str]]) -> None:
        """Queue + send instrument subscriptions."""
        for inst in instruments:
            if 'ExchangeSegment' not in inst or 'SecurityId' not in inst:
                raise ValueError(f'Invalid instrument payload: {inst}')

            key = f'{inst["ExchangeSegment"]}:{inst["SecurityId"]}'
            self._subscriptions[key] = inst

        if self._ws is not None:
            try:
                await self._send_subscription_packet(instruments)
            except Exception:
                logger.info(f'Queued {len(instruments)} subscriptions (socket not ready)')
        else:
            logger.info(f'Queued {len(instruments)} subscriptions (waiting for connection)')

    async def unsubscribe(self, instruments: List[Dict[str, str]]) -> None:
        """
        Remove instruments from feed + local subscription registry.
        """
        for inst in instruments:
            key = f'{inst["ExchangeSegment"]}:{inst["SecurityId"]}'
            self._subscriptions.pop(key, None)

        if self._ws is None:
            logger.info(f'Queued {len(instruments)} unsubscriptions (socket not ready)')
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
        """Gracefully stop the feed."""
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

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    async def _send_subscription_packet(self, instruments: List[Dict[str, str]]) -> None:
        if self._ws is None:
            return

        CHUNK = 50
        for i in range(0, len(instruments), CHUNK):
            chunk = instruments[i : i + CHUNK]

            payload = {
                'RequestCode': REQ_SUBSCRIBE,
                'InstrumentCount': len(chunk),
                'InstrumentList': chunk,
            }

            try:
                await self._ws.send(json.dumps(payload))
                logger.info(f'📡 Sent subscription for {len(chunk)} symbols')
            except ConnectionClosed as e:
                logger.warning(f'Subscription failed (connection closed): {e}')
                raise
            except Exception as e:
                logger.error(f'Subscription packet error: {e}')
                raise

    def _parse_binary(self, data: bytes) -> None:
        offset = 0
        total_len = len(data)

        while offset + HEADER_SIZE <= total_len:
            try:
                (msg_len, feed_code, exch_seg, sec_id, _) = struct.unpack_from(
                    HEADER_FMT, data, offset
                )

                end = offset + msg_len

                if end > total_len:
                    break

                start = offset + HEADER_SIZE

                payload = data[start:end]

                offset = end

                if feed_code in (FEED_DEPTH_BID, FEED_DEPTH_ASK):
                    side = 'bid' if feed_code == FEED_DEPTH_BID else 'ask'
                    levels = self._parse_depth_levels(payload)

                    if not levels:
                        continue

                    parsed = {
                        'security_id': str(sec_id),
                        'exchange_segment': int(exch_seg),
                        'side': side,
                        'levels': levels,
                    }

                    for cb in self._callbacks:
                        try:
                            cb(parsed)
                        except Exception as e:
                            logger.error(f'Callback error: {e}', exc_info=True)

            except Exception as e:
                logger.error(f'Binary parse error: {e}', exc_info=True)
                break

    @staticmethod
    def _parse_depth_levels(payload: bytes) -> List[Dict[str, float | int]]:
        if len(payload) % DEPTH_LEVEL_SIZE != 0:
            logger.warning(f'Unexpected depth payload size: {len(payload)} bytes')

        levels: List[Dict[str, float | int]] = []

        for i in range(0, len(payload), DEPTH_LEVEL_SIZE):
            if i + DEPTH_LEVEL_SIZE > len(payload):
                break

            price, qty, orders = struct.unpack_from('<dII', payload, i)
            levels.append({'price': float(price), 'qty': int(qty), 'orders': int(orders)})

        return levels
