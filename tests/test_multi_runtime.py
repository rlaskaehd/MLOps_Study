"""확장 프로필의 상품 확인, 수집, 출력과 종료 연결을 검증한다."""

import asyncio
import unittest
from collections.abc import Awaitable, Callable, Sequence

from src.collector.streams import build_stream_specs
from src.config import MultiStreamConfig
from src.models.event import Event, ReceivedMessage
from src.monitoring.multi_stats import MultiStreamStatsCollector
from src.multi_runtime import run_multi_stream_collection


class MemoryOutput:
    def __init__(self, order: list[str]) -> None:
        self.events: list[Event] = []
        self.order = order
        self.closed = False

    async def open(self) -> None:
        self.order.append("output-open")

    async def write(self, event: Event) -> None:
        self.events.append(event)

    async def close(self) -> None:
        self.closed = True


class SequenceMultiCollector:
    def __init__(self, messages: Sequence[ReceivedMessage]) -> None:
        self.messages = messages

    async def run(
        self,
        handler: Callable[[ReceivedMessage], Awaitable[None]],
        stop_event: asyncio.Event,
    ) -> None:
        for message in self.messages:
            await handler(message)
        stop_event.set()


class QuietTui:
    async def run(self, stop_event: asyncio.Event) -> None:
        await stop_event.wait()


class MultiStreamRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_validates_before_open_and_preserves_all_messages(self) -> None:
        config = MultiStreamConfig(symbols=("BTCUSDT",), validate_symbols=True)
        specs = build_stream_specs(config.symbols)
        stats = MultiStreamStatsCollector(config.symbols)
        order: list[str] = []
        output = MemoryOutput(order)
        messages = (
            ReceivedMessage(
                payload='{\"result\":null,\"id\":1}',
                connection_group="spot_market",
                connection_id="c1",
                receive_sequence=1,
                received_at_ms=1_100,
            ),
            ReceivedMessage(
                payload=(
                    '{\"stream\":\"btcusdt@bookTicker\",\"data\":'
                    '{\"u\":1,\"s\":\"BTCUSDT\",\"b\":\"1\",\"B\":\"2\",'
                    '\"a\":\"3\",\"A\":\"4\"}}'
                ),
                connection_group="spot_market",
                connection_id="c1",
                receive_sequence=2,
                received_at_ms=1_101,
            ),
        )

        async def validate(symbols: tuple[str, ...]) -> object:
            self.assertEqual(symbols, ("BTCUSDT",))
            order.append("validated")
            return object()

        await run_multi_stream_collection(
            config,
            specs,
            output=output,
            stats=stats,
            collector=SequenceMultiCollector(messages),
            tui=QuietTui(),
            stop_event=asyncio.Event(),
            product_validator=validate,  # type: ignore[arg-type]
            manage_signals=False,
        )

        self.assertEqual(order, ["validated", "output-open"])
        self.assertTrue(output.closed)
        self.assertEqual(len(output.events), 2)
        self.assertEqual(output.events[0]["meta"]["stream_type"], "control")
        self.assertEqual(output.events[1]["data"]["ask_price"], "3")
        self.assertEqual(stats.snapshot().forwarded_messages, 2)


if __name__ == "__main__":
    unittest.main()
