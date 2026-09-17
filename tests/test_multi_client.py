"""다중 스트림 연결별 구독, 출처, 실패 전달을 검증한다."""

import asyncio
import json
import unittest
from collections.abc import Callable
from typing import Any

from src.collector.client import MessageHandlerError
from src.collector.multi_client import StreamConnection
from src.collector.streams import (
    SPOT_MARKET_GROUP,
    build_stream_specs,
    group_stream_specs,
)
from src.models.event import ReceivedMessage


class FakeWebSocket:
    def __init__(self, messages: list[str | BaseException]) -> None:
        self.messages: asyncio.Queue[str | BaseException] = asyncio.Queue()
        for message in messages:
            self.messages.put_nowait(message)
        self.sent: list[str] = []

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str:
        item = await self.messages.get()
        if isinstance(item, BaseException):
            raise item
        return item


class FakeConnectionContext:
    def __init__(self, websocket: FakeWebSocket) -> None:
        self.websocket = websocket

    async def __aenter__(self) -> FakeWebSocket:
        return self.websocket

    async def __aexit__(self, *args: object) -> None:
        return None


class FakeConnectionFactory:
    def __init__(self, websockets: list[FakeWebSocket]) -> None:
        self.websockets = websockets
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, url: str, **kwargs: Any) -> FakeConnectionContext:
        self.calls.append((url, kwargs))
        return FakeConnectionContext(self.websockets.pop(0))


class StreamConnectionTests(unittest.IsolatedAsyncioTestCase):
    def build_connection(
        self,
        factory: FakeConnectionFactory,
        *,
        statuses: list[tuple[str, str, str]] | None = None,
    ) -> StreamConnection:
        grouped = group_stream_specs(build_stream_specs(("BTCUSDT",)))
        return StreamConnection(
            SPOT_MARKET_GROUP,
            grouped["spot_market"],
            connection_factory=factory,
            connection_id_factory=lambda group, attempt: f"run-{group}-{attempt}",
            received_clock_ms=lambda: 123456789,
            status_handler=(
                (lambda group, category, value: statuses.append((group, category, value)))
                if statuses is not None
                else None
            ),
        )

    async def test_subscribes_group_and_delivers_ack_and_data_with_source(self) -> None:
        websocket = FakeWebSocket(
            [
                '{"result":null,"id":1}',
                '{"stream":"btcusdt@bookTicker","data":'
                '{"u":1,"s":"BTCUSDT","b":"1","B":"2",'
                '"a":"3","A":"4"}}',
            ]
        )
        factory = FakeConnectionFactory([websocket])
        statuses: list[tuple[str, str, str]] = []
        connection = self.build_connection(factory, statuses=statuses)
        received: list[ReceivedMessage] = []
        stop = asyncio.Event()

        async def handler(message: ReceivedMessage) -> None:
            received.append(message)
            if len(received) == 2:
                stop.set()

        await connection.run(handler, stop)

        request = json.loads(websocket.sent[0])
        self.assertEqual(
            request["params"],
            [
                "btcusdt@aggTrade",
                "btcusdt@bookTicker",
                "btcusdt@kline_1m",
            ],
        )
        self.assertEqual([message.receive_sequence for message in received], [1, 2])
        self.assertEqual(received[1].connection_group, "spot_market")
        self.assertEqual(received[1].connection_id, "run-spot_market-1")
        self.assertEqual(received[1].received_at_ms, 123456789)
        self.assertIn(("spot_market", "subscription", "3개 구독 확인"), statuses)
        self.assertIn(("spot_market", "data", "수신 중"), statuses)
        self.assertEqual(factory.calls[0][0], SPOT_MARKET_GROUP.url)
        self.assertEqual(factory.calls[0][1]["max_queue"], 16)

    async def test_handler_failure_is_not_retried_as_network_failure(self) -> None:
        websocket = FakeWebSocket(['{"result":null,"id":1}'])
        factory = FakeConnectionFactory([websocket])
        connection = self.build_connection(factory)

        async def fail(message: ReceivedMessage) -> None:
            raise RuntimeError("downstream")

        with self.assertRaises(MessageHandlerError):
            await connection.run(fail, asyncio.Event())
        self.assertEqual(len(factory.calls), 1)


if __name__ == "__main__":
    unittest.main()
