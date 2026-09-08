"""MongoDB 전용 실행 진입점의 객체 연결을 검증한다."""

import asyncio
import unittest
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from src.config import load_mongo_config
from src.main import run_collector
from src.models.event import Event
from src.run_mongodb import build_mongodb_runtime, parse_symbols


class RecordingMongoOutput:
    def __init__(
        self,
        config: object,
        *,
        on_batch_persisted: Callable[[int, float], None],
        on_state_changed: Callable[[str], None],
        **options: Any,
    ) -> None:
        self.events: list[Event] = []
        self._on_batch_persisted = on_batch_persisted
        self._on_state_changed = on_state_changed

    async def open(self) -> None:
        self._on_state_changed("연결됨")

    async def write(self, event: Event) -> None:
        self.events.append(event)

    async def close(self) -> None:
        self._on_batch_persisted(len(self.events), 0.01)
        self._on_state_changed("종료됨")


class SequenceCollector:
    def __init__(self, messages: Sequence[str]) -> None:
        self._messages = messages

    async def run(
        self,
        handler: Callable[[str], Awaitable[None]],
        stop_event: asyncio.Event,
    ) -> None:
        for message in self._messages:
            await handler(message)
        stop_event.set()


class QuietTui:
    async def run(self, stop_event: asyncio.Event) -> None:
        await stop_event.wait()


class MongoRunnerTests(unittest.IsolatedAsyncioTestCase):
    def test_default_and_custom_symbols(self) -> None:
        self.assertEqual(len(parse_symbols([])), 10)
        self.assertEqual(
            parse_symbols(["--symbols", "btcusdt", "ETHUSDT"]),
            ("BTCUSDT", "ETHUSDT"),
        )

    async def test_runtime_wires_tui_events_and_persistence_stats(self) -> None:
        messages = (
            '{"result":null,"id":1}',
            '{"e":"aggTrade","s":"BTCUSDT","p":"1.2300"}',
        )
        stats, output = build_mongodb_runtime(
            ("BTCUSDT",),
            load_mongo_config(environ={}),
            output_factory=RecordingMongoOutput,
        )

        await run_collector(
            symbols=("BTCUSDT",),
            mode="tui",
            collector=SequenceCollector(messages),  # type: ignore[arg-type]
            output=output,
            stats=stats,
            tui=QuietTui(),  # type: ignore[arg-type]
            stop_event=asyncio.Event(),
            manage_signals=False,
        )

        self.assertIsInstance(output, RecordingMongoOutput)
        self.assertEqual(len(output.events), 2)  # type: ignore[attr-defined]
        self.assertEqual(output.events[1]["price"], "1.2300")  # type: ignore[attr-defined]
        snapshot = stats.snapshot()
        self.assertEqual(snapshot.forwarded_messages, 2)
        self.assertEqual(snapshot.persisted_messages, 2)
        self.assertEqual(snapshot.pending_messages, 0)
        self.assertEqual(snapshot.storage_state, "종료됨")


if __name__ == "__main__":
    unittest.main()
