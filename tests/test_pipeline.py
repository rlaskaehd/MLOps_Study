"""표준화부터 후속 출력까지 이어지는 공통 전달 경로를 검증한다."""

import asyncio
import json
import logging
import unittest

from src.config import DEFAULT_SYMBOLS
from src.main import run_collector
from src.models.event import Event
from src.monitoring.stats import StatsCollector
from src.pipeline import EventDispatcher, OutputWriteError


class MemoryOutput:
    def __init__(self) -> None:
        self.events: list[Event] = []
        self.calls: list[str] = []

    async def open(self) -> None:
        self.calls.append("open")

    async def write(self, event: Event) -> None:
        self.calls.append("write")
        self.events.append(event)

    async def close(self) -> None:
        self.calls.append("close")


class FailingOutput(MemoryOutput):
    async def write(self, event: Event) -> None:
        self.calls.append("write")
        raise OSError("sink unavailable")


class GatedOutput(MemoryOutput):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def write(self, event: Event) -> None:
        self.started.set()
        await self.release.wait()
        await super().write(event)


class OneMessageCollector:
    def __init__(self, message: str) -> None:
        self.message = message

    async def run(self, handler: object, stop_event: asyncio.Event) -> None:
        await handler(self.message)  # type: ignore[operator]
        stop_event.set()


class EventDispatcherTests(unittest.IsolatedAsyncioTestCase):
    def make_dispatcher(
        self,
        output: MemoryOutput | None,
    ) -> tuple[EventDispatcher, StatsCollector]:
        stats = StatsCollector(
            ("BTCUSDT", "ETHUSDT"),
            output_connected=output is not None,
        )
        dispatcher = EventDispatcher(
            output=output,
            stats=stats,
            logger=logging.getLogger("test_pipeline"),
        )
        return dispatcher, stats

    async def test_forwards_all_messages_once_in_order_with_values_preserved(
        self,
    ) -> None:
        output = MemoryOutput()
        dispatcher, stats = self.make_dispatcher(output)
        messages = [
            '{"result":null,"id":1}',
            '{"e":"aggTrade","s":"BTCUSDT","a":7,"p":"1.2300","x":9}',
            '{"e":"aggTrade","s":"BTCUSDT","a":7,"p":"1.2300","x":9}',
            '{"e":"aggTrade"',
        ]

        await dispatcher.open()
        for message in messages:
            await dispatcher.handle(message)
        await dispatcher.close()

        self.assertEqual(
            output.calls, ["open", "write", "write", "write", "write", "close"]
        )
        self.assertEqual(
            output.events,
            [
                {"result": None, "id": 1},
                {
                    "event_type": "aggTrade",
                    "symbol": "BTCUSDT",
                    "trade_id": 7,
                    "price": "1.2300",
                    "x": 9,
                },
                {
                    "event_type": "aggTrade",
                    "symbol": "BTCUSDT",
                    "trade_id": 7,
                    "price": "1.2300",
                    "x": 9,
                },
                {"raw_message": '{"e":"aggTrade"'},
            ],
        )
        snapshot = stats.snapshot()
        self.assertEqual(snapshot.cumulative_trades, 2)
        self.assertEqual(snapshot.control_or_unclassified, 2)
        self.assertEqual(snapshot.forwarded_messages, 4)

    async def test_forwards_all_ten_symbols_duplicates_controls_and_fallbacks(
        self,
    ) -> None:
        output = MemoryOutput()
        stats = StatsCollector(DEFAULT_SYMBOLS, output_connected=True)
        dispatcher = EventDispatcher(
            output=output,
            stats=stats,
            logger=logging.getLogger("test_pipeline_all_symbols"),
        )
        trade_messages = [
            json.dumps(
                {
                    "e": "aggTrade",
                    "s": symbol,
                    "a": index,
                    "extra": symbol.lower(),
                }
            )
            for index, symbol in enumerate(DEFAULT_SYMBOLS, start=1)
        ]
        messages = [
            '{"result":null,"id":1}',
            *trade_messages,
            trade_messages[0],
            '{"e":"aggTrade"',
        ]

        await dispatcher.open()
        for message in messages:
            await dispatcher.handle(message)
        await dispatcher.close()

        received_symbols = [event["symbol"] for event in output.events[1:11]]
        self.assertEqual(received_symbols, list(DEFAULT_SYMBOLS))
        self.assertEqual(output.events[11], output.events[1])
        self.assertEqual(output.events[-1], {"raw_message": messages[-1]})
        self.assertEqual(len(output.events), len(messages))
        snapshot = stats.snapshot()
        self.assertEqual(snapshot.cumulative_trades, 11)
        self.assertEqual(snapshot.control_or_unclassified, 2)
        self.assertEqual(snapshot.forwarded_messages, len(messages))

    async def test_records_receive_before_waiting_for_slow_output(self) -> None:
        output = GatedOutput()
        dispatcher, stats = self.make_dispatcher(output)
        await dispatcher.open()

        task = asyncio.create_task(
            dispatcher.handle('{"e":"aggTrade","s":"BTCUSDT","a":1}')
        )
        await output.started.wait()

        self.assertEqual(stats.snapshot().cumulative_trades, 1)
        self.assertEqual(stats.snapshot().forwarded_messages, 0)
        self.assertFalse(task.done())

        output.release.set()
        await task
        await dispatcher.close()
        self.assertEqual(stats.snapshot().forwarded_messages, 1)

    async def test_output_failure_stops_without_false_success_count(self) -> None:
        output = FailingOutput()
        dispatcher, stats = self.make_dispatcher(output)
        await dispatcher.open()

        with self.assertRaises(OutputWriteError):
            await dispatcher.handle('{"e":"aggTrade","s":"BTCUSDT","a":1}')

        self.assertEqual(stats.snapshot().cumulative_trades, 1)
        self.assertEqual(stats.snapshot().forwarded_messages, 0)
        self.assertIn("후속 출력 전달 실패", stats.snapshot().recent_error or "")
        await dispatcher.close()

    async def test_no_output_still_counts_every_received_message(self) -> None:
        dispatcher, stats = self.make_dispatcher(None)

        await dispatcher.open()
        await dispatcher.handle('{"e":"aggTrade","s":"ETHUSDT","a":1}')
        await dispatcher.close()

        snapshot = stats.snapshot()
        self.assertEqual(snapshot.cumulative_trades, 1)
        self.assertEqual(snapshot.forwarded_messages, 0)
        self.assertFalse(snapshot.output_connected)

    async def test_run_collector_opens_writes_and_closes_custom_output(self) -> None:
        output = MemoryOutput()
        stats = StatsCollector(("BTCUSDT",), output_connected=True)

        await run_collector(
            symbols=("BTCUSDT",),
            collector=OneMessageCollector('{"e":"aggTrade","s":"BTCUSDT","a":1}'),  # type: ignore[arg-type]
            output=output,
            stats=stats,
            stop_event=asyncio.Event(),
            manage_signals=False,
        )

        self.assertEqual(output.calls, ["open", "write", "close"])
        self.assertEqual(output.events[0]["symbol"], "BTCUSDT")
        self.assertEqual(stats.snapshot().forwarded_messages, 1)


if __name__ == "__main__":
    unittest.main()
