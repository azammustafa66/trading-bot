from __future__ import annotations

import asyncio
import json
import logging
import struct
from typing import Callable, Dict, List, Optional, Protocol, runtime_checkable

import websockets

logger = logging.getLogger('DepthFeed')

# Constants
DEPTH_ENDPOINT_20 = 'wss://depth-api-feed.dhan.co/twentydepth'
REQ_SUBSCRIBE = 23
REQ_DISCONNECT = 12
FEED_DEPTH_BID = 41
FEED_DEPTH_ASK = 51


@runtime_checkable
class WSConnection(Protocol):
    closed: bool

    async def send(self, data: str | bytes) -> None: ...
    async def close(self) -> None: ...


class DepthFeed:
    def __init__(self, token: str, client_id: str) -> None:
        self.url = f'{DEPTH_ENDPOINT_20}?version=2&token={token}&clientId={client_id}&authType=2'
        self._ws: Optional[WSConnection] = None
        self._stop: bool = False
        self._callbacks: List[Callable[[Dict[str, object]], None]] = []
        self._subscriptions: Dict[str, Dict[str, str]] = {}

    def register_callback(self, cb: Callable[[Dict[str, object]], None]) -> None:
        self._callbacks.append(cb)

    async def connect(self) -> None:
        while not self._stop:
            try:
                logger.info('Connecting to Depth Feed...')
                async with websockets.connect(self.url, ping_interval=10, max_size=None) as ws:
                    self._ws = ws  # type: ignore
                    logger.info('✅ Depth Feed Connected')
                    if self._subscriptions:
                        await self._send_subscription_packet(list(self._subscriptions.values()))
                    async for message in ws:
                        if isinstance(message, (bytes, bytearray)):
                            self._parse_binary(message)
            except Exception as e:
                if self._stop:
                    break
                logger.error(f'Depth Feed Error: {e}')
                await asyncio.sleep(2)
            finally:
                self._ws = None

    async def subscribe(self, instruments: List[Dict[str, str]]) -> None:
        for inst in instruments:
            key = f'{inst["ExchangeSegment"]}:{inst["SecurityId"]}'
            self._subscriptions[key] = inst

        if self._ws is not None and not self._ws.closed:
            await self._send_subscription_packet(instruments)

    async def _send_subscription_packet(self, instruments: List[Dict[str, str]]) -> None:
        if self._ws is None or self._ws.closed:
            return
        CHUNK = 100
        for i in range(0, len(instruments), CHUNK):
            chunk = instruments[i : i + CHUNK]
            payload = {
                'RequestCode': REQ_SUBSCRIBE,
                'InstrumentCount': len(chunk),
                'InstrumentList': chunk,
            }
            try:
                await self._ws.send(json.dumps(payload))
                logger.info(f'Subscribed to {len(chunk)} instruments')
            except Exception as e:
                logger.error(f'Subscription error: {e}')

    def _parse_binary(self, data: bytes) -> None:
        offset = 0
        total_len = len(data)
        while offset + 12 <= total_len:
            try:
                msg_len, feed_code, exch_seg, sec_id, _ = struct.unpack_from('<HBBII', data, offset)
                start = offset + 12
                end = start + msg_len
                if end > total_len:
                    break
                payload = data[start:end]
                offset = end

                if feed_code in (FEED_DEPTH_BID, FEED_DEPTH_ASK):
                    side = 'bid' if feed_code == FEED_DEPTH_BID else 'ask'
                    levels = self._parse_depth_levels(payload)
                    parsed = {
                        'security_id': str(sec_id),
                        'exchange_segment': int(exch_seg),
                        'side': side,
                        'levels': levels,
                    }
                    if levels:
                        for cb in self._callbacks:
                            cb(parsed)
            except Exception as e:
                logger.error(f'Binary parse error: {e}', exc_info=True)
                break

    @staticmethod
    def _parse_depth_levels(payload: bytes) -> List[Dict[str, float | int]]:
        levels = []
        for i in range(0, len(payload), 16):
            if i + 16 > len(payload):
                break
            price, qty, orders = struct.unpack_from('<dII', payload, i)
            levels.append({'price': float(price), 'qty': int(qty), 'orders': int(orders)})
        return levels

    async def disconnect(self) -> None:
        self._stop = True
        if self._ws and not self._ws.closed:
            try:
                await self._ws.send(json.dumps({'RequestCode': REQ_DISCONNECT}))
            except Exception:
                pass
            await self._ws.close()
