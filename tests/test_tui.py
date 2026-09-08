"""TUI 렌더링과 후속 출력의 독립 실행을 검증한다."""

import asyncio
import io
import json
import logging
import unittest
from collections.abc import Awaitable, Callable, Sequence

from rich.console import Console

from src.main import run_collector
from src.models.event import Event
from src.monitoring.logging import configure_application_logging
from src.monitoring.stats import StatsCollector
from src.monitoring.tui import TuiRenderer
from src.outputs.stdout import StdoutOutput


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


class SequenceCollector:
    def __init__(self, messages: Sequence[str | bytes]) -> None:
        self._messages = messages

    async def run(
        self,
        handler: Callable[[str | bytes], Awaitable[None]],
        stop_event: asyncio.Event,
    ) -> None:
        for message in self._messages:
            await handler(message)
        stop_event.set()


class MemoryOutput:
    def __init__(self, gate: asyncio.Event | None = None) -> None:
        self.events: list[Event] = []
        self.gate = gate
        self.write_started = asyncio.Event()

    async def open(self) -> None:
        return None

    async def write(self, event: Event) -> None:
        self.write_started.set()
        if self.gate is not None:
            await self.gate.wait()
        self.events.append(event)

    async def close(self) -> None:
        return None


class CountingTui:
    def __init__(self, stream: io.StringIO | None = None) -> None:
        self.ticks = 0
        self.ticked = asyncio.Event()
        self.stream = stream

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            self.ticks += 1
            self.ticked.set()
            if self.stream is not None:
                self.stream.write("\x1b[2Jtui\n")
            await asyncio.sleep(0)


class TuiRenderTests(unittest.TestCase):
    def test_renders_all_symbols_bars_totals_and_output_state(self) -> None:
        clock = FakeClock()
        symbols = ("BTCUSDT", "ETHUSDT", "BNBUSDT")
        stats = StatsCollector(
            symbols,
            output_connected=False,
            clock=clock,
        )
        stats.record_received({"event_type": "aggTrade", "symbol": "BTCUSDT"})
        stats.record_received({"event_type": "aggTrade", "symbol": "BTCUSDT"})
        stats.record_received({"event_type": "aggTrade", "symbol": "ETHUSDT"})
        clock.value = 1.0
        stream = io.StringIO()
        console = Console(
            file=stream,
            force_terminal=False,
            color_system=None,
            width=120,
        )

        console.print(TuiRenderer.render(stats.snapshot()))
        rendered = stream.getvalue()

        for symbol in symbols:
            self.assertIn(symbol, rendered)
        self.assertIn("전체 초당 수집량: 3 events/sec", rendered)
        self.assertIn("동작 시간: 00:00:01", rendered)
        self.assertIn("누적 수집량: 3", rendered)
        self.assertIn("후속 출력: 미연결", rendered)

    def test_tui_logging_records_warning_without_writing_stream(self) -> None:
        stats = StatsCollector(("BTCUSDT",), output_connected=False)
        stream = io.StringIO()
        configure_application_logging("tui", stats=stats, stream=stream)

        logging.getLogger("tui_test").warning("sample warning")

        self.assertEqual(stream.getvalue(), "")
        self.assertIn("sample warning", stats.snapshot().recent_error or "")

    def test_renders_mongodb_queue_and_persistence_separately(self) -> None:
        stats = StatsCollector(
            ("BTCUSDT",),
            output_connected=True,
            storage_name="MongoDB",
        )
        stats.record_forwarded()
        stats.record_forwarded()
        stats.record_persisted(1, 0.008)
        stats.set_storage_state("연결됨")
        stream = io.StringIO()
        console = Console(
            file=stream,
            force_terminal=False,
            color_system=None,
            width=140,
        )

        console.print(TuiRenderer.render(stats.snapshot()))
        rendered = stream.getvalue()

        self.assertIn("큐 접수: 2", rendered)
        self.assertIn("MongoDB: 연결됨", rendered)
        self.assertIn("적재 확인: 1", rendered)
        self.assertIn("미확인: 1", rendered)
        self.assertIn("최근 배치: 1건 / 8.0ms", rendered)


class TuiIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_tui_and_custom_output_receive_same_events(self) -> None:
        messages = [
            '{"result":null,"id":1}',
            '{"e":"aggTrade","s":"BTCUSDT","a":1,"p":"1.00"}',
            '{"e":"aggTrade"',
        ]
        output = MemoryOutput()
        tui = CountingTui()
        stats = StatsCollector(("BTCUSDT",), output_connected=True)

        await run_collector(
            symbols=("BTCUSDT",),
            mode="tui",
            collector=SequenceCollector(messages),  # type: ignore[arg-type]
            output=output,
            stats=stats,
            tui=tui,  # type: ignore[arg-type]
            stop_event=asyncio.Event(),
            manage_signals=False,
        )

        self.assertEqual(len(output.events), len(messages))
        self.assertEqual(output.events[1]["price"], "1.00")
        self.assertEqual(output.events[2], {"raw_message": messages[2]})
        self.assertEqual(stats.snapshot().forwarded_messages, len(messages))
        self.assertGreaterEqual(tui.ticks, 1)

    async def test_tui_keeps_running_while_output_waits(self) -> None:
        gate = asyncio.Event()
        output = MemoryOutput(gate)
        tui = CountingTui()
        task = asyncio.create_task(
            run_collector(
                symbols=("BTCUSDT",),
                mode="tui",
                collector=SequenceCollector(('{"e":"aggTrade","s":"BTCUSDT","a":1}',)),  # type: ignore[arg-type]
                output=output,
                tui=tui,  # type: ignore[arg-type]
                stop_event=asyncio.Event(),
                manage_signals=False,
            )
        )

        await output.write_started.wait()
        await tui.ticked.wait()
        self.assertFalse(task.done())
        self.assertGreaterEqual(tui.ticks, 1)

        gate.set()
        await asyncio.wait_for(task, timeout=1)
        self.assertEqual(len(output.events), 1)

    async def test_tui_control_sequences_never_enter_stdout_json(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()
        tui = CountingTui(stderr)

        await run_collector(
            symbols=("BTCUSDT",),
            mode="tui",
            collector=SequenceCollector(('{"e":"aggTrade","s":"BTCUSDT","a":1}',)),  # type: ignore[arg-type]
            output=StdoutOutput(stdout),
            tui=tui,  # type: ignore[arg-type]
            stop_event=asyncio.Event(),
            manage_signals=False,
        )

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["symbol"], "BTCUSDT")
        self.assertNotIn("\x1b", stdout.getvalue())
        self.assertIn("\x1b", stderr.getvalue())

    async def test_json_and_tui_modes_preserve_identical_event_lists(self) -> None:
        messages = (
            '{"result":null,"id":1}',
            '{"e":"aggTrade","s":"BTCUSDT","a":1,"extra":true}',
        )
        json_output = MemoryOutput()
        tui_output = MemoryOutput()

        await run_collector(
            symbols=("BTCUSDT",),
            mode="json",
            collector=SequenceCollector(messages),  # type: ignore[arg-type]
            output=json_output,
            stop_event=asyncio.Event(),
            manage_signals=False,
        )
        await run_collector(
            symbols=("BTCUSDT",),
            mode="tui",
            collector=SequenceCollector(messages),  # type: ignore[arg-type]
            output=tui_output,
            tui=CountingTui(),  # type: ignore[arg-type]
            stop_event=asyncio.Event(),
            manage_signals=False,
        )

        self.assertEqual(json_output.events, tui_output.events)


if __name__ == "__main__":
    unittest.main()
