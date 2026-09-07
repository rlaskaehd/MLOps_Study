"""수집기의 데이터 보존 및 출력 계약을 검증한다."""

import asyncio
import base64
import inspect
import io
import json
import logging
import unittest
from collections.abc import Sequence
from typing import Any

from src.collector.client import BinanceCollector, MessageHandlerError
from src.collector.parser import normalize_message
from src.collector.reconnect import ReconnectPolicy
from src.main import create_message_handler, run_collector
from src.models.event import Event
from src.monitoring.stats import StatsCollector
from src.outputs.stdout import StdoutOutput


class RecordingStream(io.StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.flush_count = 0

    def flush(self) -> None:
        self.flush_count += 1
        super().flush()


class FakeWebSocket:
    def __init__(self, messages: Sequence[str | bytes]) -> None:
        self._messages = iter(messages)
        self.sent: list[str] = []

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str | bytes:
        try:
            message = next(self._messages)
        except StopIteration:
            await asyncio.Future()
            raise AssertionError("unreachable")
        await asyncio.sleep(0)
        return message


class FakeConnectionContext:
    def __init__(self, outcome: Sequence[str | bytes] | BaseException) -> None:
        self._outcome = outcome
        self.websocket: FakeWebSocket | None = None

    async def __aenter__(self) -> FakeWebSocket:
        if isinstance(self._outcome, BaseException):
            raise self._outcome
        self.websocket = FakeWebSocket(self._outcome)
        return self.websocket

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any,
    ) -> None:
        return None


class FakeConnectionFactory:
    def __init__(
        self,
        outcomes: Sequence[Sequence[str | bytes] | BaseException],
    ) -> None:
        self._outcomes = iter(outcomes)
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.contexts: list[FakeConnectionContext] = []

    def __call__(self, url: str, **kwargs: Any) -> FakeConnectionContext:
        self.calls.append((url, kwargs))
        context = FakeConnectionContext(next(self._outcomes))
        self.contexts.append(context)
        return context


class FakeCollectorRunner:
    def __init__(self, messages: Sequence[str | bytes]) -> None:
        self._messages = messages

    async def run(self, handler: Any, stop_event: asyncio.Event) -> None:
        for message in self._messages:
            result = handler(message)
            if inspect.isawaitable(result):
                await result
        stop_event.set()


class StopAfterOutput:
    def __init__(self, stop_event: asyncio.Event, expected: int) -> None:
        self._stop_event = stop_event
        self._expected = expected
        self.events: list[Event] = []
        self.opened = False
        self.closed = False

    async def open(self) -> None:
        self.opened = True

    async def write(self, event: Event) -> None:
        self.events.append(event)
        if len(self.events) == self._expected:
            self._stop_event.set()

    async def close(self) -> None:
        self.closed = True


class NormalizeMessageTests(unittest.TestCase):
    def test_renames_known_fields_and_preserves_every_value(self) -> None:
        raw_event = {
            "e": "aggTrade",
            "E": 1_788_783_123_456,
            "s": "BTCUSDT",
            "a": 123_456_789,
            "p": "111234.50000000",
            "q": "0.00420000",
            "f": 987_654_320,
            "l": 987_654_322,
            "T": 1_788_783_123_450,
            "m": True,
            "M": True,
            "extra": {"nested": [1, None, "unchanged"]},
        }

        result = normalize_message(json.dumps(raw_event))

        self.assertIsNone(result.warning)
        self.assertEqual(
            result.event,
            {
                "event_type": "aggTrade",
                "event_time": 1_788_783_123_456,
                "symbol": "BTCUSDT",
                "trade_id": 123_456_789,
                "price": "111234.50000000",
                "quantity": "0.00420000",
                "first_trade_id": 987_654_320,
                "last_trade_id": 987_654_322,
                "trade_time": 1_788_783_123_450,
                "is_buyer_maker": True,
                "M": True,
                "extra": {"nested": [1, None, "unchanged"]},
            },
        )

    def test_does_not_create_missing_fields_or_change_unexpected_types(self) -> None:
        result = normalize_message('{"e":null,"p":123,"custom":false}')

        self.assertIsNone(result.warning)
        self.assertEqual(
            result.event,
            {"event_type": None, "price": 123, "custom": False},
        )

    def test_preserves_invalid_json_as_raw_message(self) -> None:
        message = '{"e":"aggTrade"'

        result = normalize_message(message)

        self.assertEqual(result.warning, "invalid_json")
        self.assertEqual(result.event, {"raw_message": message})

    def test_preserves_non_object_json_as_raw_message(self) -> None:
        message = '[{"e":"aggTrade"}]'

        result = normalize_message(message)

        self.assertEqual(result.warning, "non_object_json")
        self.assertEqual(result.event, {"raw_message": message})

    def test_preserves_duplicate_key_message_as_raw_message(self) -> None:
        message = '{"e":"aggTrade","e":"serverShutdown"}'

        result = normalize_message(message)

        self.assertEqual(result.warning, "duplicate_key")
        self.assertEqual(result.event, {"raw_message": message})

    def test_preserves_field_name_collision_as_raw_message(self) -> None:
        message = '{"e":"aggTrade","event_type":"custom"}'

        result = normalize_message(message)

        self.assertEqual(result.warning, "field_name_collision")
        self.assertEqual(result.event, {"raw_message": message})

    def test_preserves_bare_floating_point_number_as_raw_message(self) -> None:
        message = '{"unexpected":1.234567890123456789}'

        result = normalize_message(message)

        self.assertEqual(result.warning, "unsupported_number")
        self.assertEqual(result.event, {"raw_message": message})

    def test_preserves_binary_message_as_base64(self) -> None:
        message = b"\x00\xffbinance"

        result = normalize_message(message)

        self.assertEqual(result.warning, "binary_message")
        self.assertEqual(result.event["raw_encoding"], "base64")
        self.assertEqual(base64.b64decode(result.event["raw_message"]), message)


class StdoutOutputTests(unittest.IsolatedAsyncioTestCase):
    async def test_writes_one_compact_json_object_per_line_and_flushes(self) -> None:
        stream = RecordingStream()
        output = StdoutOutput(stream)
        events = [
            {"symbol": "BTCUSDT", "price": "1.00"},
            {"symbol": "BTCUSDT", "quantity": "0.10"},
        ]

        await output.open()
        for event in events:
            await output.write(event)
        await output.close()

        lines = stream.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual([json.loads(line) for line in lines], events)
        self.assertEqual(stream.flush_count, 2)


class ReconnectPolicyTests(unittest.TestCase):
    def test_uses_planned_delays_and_caps_at_thirty_seconds(self) -> None:
        policy = ReconnectPolicy()

        delays = [policy.next_delay() for _ in range(8)]

        self.assertEqual(delays, [1, 2, 4, 8, 16, 30, 30, 30])

    def test_reset_starts_again_from_first_delay(self) -> None:
        policy = ReconnectPolicy()
        policy.next_delay()
        policy.next_delay()

        policy.reset()

        self.assertEqual(policy.next_delay(), 1)


class BinanceCollectorTests(unittest.IsolatedAsyncioTestCase):
    async def test_delivers_every_message_in_receive_order(self) -> None:
        ack = '{"result":null,"id":1}'
        messages = [
            ack,
            '{"e":"aggTrade","a":1}',
            '{"e":"aggTrade","a":1}',
            '{"e":"aggTrade","a":2}',
        ]
        factory = FakeConnectionFactory([messages])
        collector = BinanceCollector(
            connection_factory=factory,
            reconnect_policy=ReconnectPolicy(delays=(0,)),
        )
        stop_event = asyncio.Event()
        received: list[str | bytes] = []

        def handler(message: str | bytes) -> None:
            received.append(message)
            if len(received) == len(messages):
                stop_event.set()

        await collector.run(handler, stop_event)

        self.assertEqual(received, messages)
        self.assertEqual(factory.calls[0][1]["max_queue"], 16)
        assert factory.contexts[0].websocket is not None
        request = json.loads(factory.contexts[0].websocket.sent[0])
        self.assertEqual(request["method"], "SUBSCRIBE")
        self.assertEqual(len(request["params"]), 10)

    async def test_reconnects_after_transient_connection_failure(self) -> None:
        ack = '{"result":null,"id":1}'
        message = '{"e":"aggTrade","a":1}'
        factory = FakeConnectionFactory([OSError("temporary"), [ack, message]])
        collector = BinanceCollector(
            connection_factory=factory,
            reconnect_policy=ReconnectPolicy(delays=(0,)),
        )
        stop_event = asyncio.Event()
        received: list[str | bytes] = []

        def handler(value: str | bytes) -> None:
            received.append(value)
            if value == message:
                stop_event.set()

        await collector.run(handler, stop_event)

        self.assertEqual(received, [ack, message])
        self.assertEqual(len(factory.calls), 2)

    async def test_reconnect_keeps_stats_and_same_output_object(self) -> None:
        ack = '{"result":null,"id":1}'
        btc = '{"e":"aggTrade","s":"BTCUSDT","a":1}'
        eth = '{"e":"aggTrade","s":"ETHUSDT","a":2}'
        factory = FakeConnectionFactory([OSError("temporary"), [ack, btc, eth]])
        collector = BinanceCollector(
            symbols=("BTCUSDT", "ETHUSDT"),
            connection_factory=factory,
            reconnect_policy=ReconnectPolicy(delays=(0,)),
        )
        stop_event = asyncio.Event()
        output = StopAfterOutput(stop_event, expected=3)
        stats = StatsCollector(
            ("BTCUSDT", "ETHUSDT"),
            output_connected=True,
        )

        await run_collector(
            symbols=("BTCUSDT", "ETHUSDT"),
            collector=collector,
            output=output,
            stats=stats,
            stop_event=stop_event,
            manage_signals=False,
        )

        self.assertEqual(len(factory.calls), 2)
        self.assertTrue(output.opened)
        self.assertTrue(output.closed)
        self.assertEqual(
            [event.get("symbol") for event in output.events],
            [None, "BTCUSDT", "ETHUSDT"],
        )
        snapshot = stats.snapshot()
        self.assertEqual(snapshot.cumulative_trades, 2)
        self.assertEqual(snapshot.forwarded_messages, 3)

    async def test_outputs_server_shutdown_before_reconnecting(self) -> None:
        ack = '{"result":null,"id":1}'
        shutdown = '{"e":"serverShutdown","E":1788783123456}'
        trade = '{"e":"aggTrade","a":1}'
        factory = FakeConnectionFactory([[ack, shutdown], [ack, trade]])
        collector = BinanceCollector(
            connection_factory=factory,
            reconnect_policy=ReconnectPolicy(delays=(0,)),
        )
        stop_event = asyncio.Event()
        received: list[str | bytes] = []

        def handler(message: str | bytes) -> None:
            received.append(message)
            if message == trade:
                stop_event.set()

        await collector.run(handler, stop_event)

        self.assertEqual(received, [ack, shutdown, ack, trade])
        self.assertEqual(len(factory.calls), 2)

    async def test_reconnects_and_resubscribes_after_subscription_error(self) -> None:
        rejected = '{"code":2,"msg":"invalid request","id":1}'
        ack = '{"result":null,"id":1}'
        trade = '{"e":"aggTrade","s":"BTCUSDT","a":1}'
        factory = FakeConnectionFactory([[rejected], [ack, trade]])
        collector = BinanceCollector(
            symbols=("BTCUSDT", "ETHUSDT"),
            connection_factory=factory,
            reconnect_policy=ReconnectPolicy(delays=(0,)),
        )
        stop_event = asyncio.Event()
        received: list[str | bytes] = []

        def handler(message: str | bytes) -> None:
            received.append(message)
            if message == trade:
                stop_event.set()

        await collector.run(handler, stop_event)

        self.assertEqual(received, [rejected, ack, trade])
        self.assertEqual(len(factory.contexts), 2)
        sent = []
        for context in factory.contexts:
            assert context.websocket is not None
            sent.append(json.loads(context.websocket.sent[0]))
        self.assertEqual(sent[0]["params"], sent[1]["params"])

    async def test_reconnects_after_subscription_timeout(self) -> None:
        ack = '{"result":null,"id":1}'
        trade = '{"e":"aggTrade","s":"BTCUSDT","a":1}'
        factory = FakeConnectionFactory([[], [ack, trade]])
        collector = BinanceCollector(
            symbols=("BTCUSDT",),
            subscription_timeout=0.01,
            connection_factory=factory,
            reconnect_policy=ReconnectPolicy(delays=(0,)),
        )
        stop_event = asyncio.Event()
        received: list[str | bytes] = []

        def handler(message: str | bytes) -> None:
            received.append(message)
            if message == trade:
                stop_event.set()

        await asyncio.wait_for(collector.run(handler, stop_event), timeout=1)

        self.assertEqual(received, [ack, trade])
        self.assertEqual(len(factory.contexts), 2)

    async def test_stop_event_interrupts_blocked_receive(self) -> None:
        factory = FakeConnectionFactory([[]])
        collector = BinanceCollector(
            connection_factory=factory,
            reconnect_policy=ReconnectPolicy(delays=(0,)),
        )
        stop_event = asyncio.Event()
        task = asyncio.create_task(collector.run(lambda message: None, stop_event))

        await asyncio.sleep(0)
        await asyncio.sleep(0)
        stop_event.set()
        await asyncio.wait_for(task, timeout=1)

        self.assertTrue(task.done())

    async def test_stop_event_cancels_unfinished_message_delivery(self) -> None:
        ack = '{"result":null,"id":1}'
        trade = '{"e":"aggTrade","s":"BTCUSDT","a":1}'
        factory = FakeConnectionFactory([[ack, trade]])
        collector = BinanceCollector(
            symbols=("BTCUSDT",),
            connection_factory=factory,
            reconnect_policy=ReconnectPolicy(delays=(0,)),
        )
        stop_event = asyncio.Event()
        write_started = asyncio.Event()
        write_cancelled = asyncio.Event()

        async def handler(message: str | bytes) -> None:
            if message == ack:
                return
            write_started.set()
            try:
                await asyncio.Future()
            finally:
                write_cancelled.set()

        task = asyncio.create_task(collector.run(handler, stop_event))
        await write_started.wait()
        stop_event.set()
        await asyncio.wait_for(task, timeout=1)

        self.assertTrue(write_cancelled.is_set())

    async def test_reports_connection_and_subscription_status(self) -> None:
        ack = '{"result":null,"id":1}'
        trade = '{"e":"aggTrade","s":"BTCUSDT","a":1}'
        factory = FakeConnectionFactory([[ack, trade]])
        statuses: list[tuple[str, str]] = []
        collector = BinanceCollector(
            symbols=("BTCUSDT",),
            connection_factory=factory,
            reconnect_policy=ReconnectPolicy(delays=(0,)),
            status_handler=lambda category, value: statuses.append((category, value)),
        )
        stop_event = asyncio.Event()

        def handler(message: str | bytes) -> None:
            if message == trade:
                stop_event.set()

        await collector.run(handler, stop_event)

        self.assertIn(("connection", "연결됨"), statuses)
        self.assertIn(("subscription", "1개 구독 확인"), statuses)
        self.assertEqual(statuses[-1], ("connection", "종료됨"))

    async def test_does_not_hide_handler_failure_as_reconnect(self) -> None:
        factory = FakeConnectionFactory([["message"]])
        collector = BinanceCollector(
            connection_factory=factory,
            reconnect_policy=ReconnectPolicy(delays=(0,)),
        )
        stop_event = asyncio.Event()

        def failing_handler(message: str | bytes) -> None:
            raise RuntimeError("output failed")

        with self.assertRaises(MessageHandlerError):
            await collector.run(failing_handler, stop_event)

        self.assertEqual(len(factory.calls), 1)


class MainIntegrationTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def make_logger(stream: io.StringIO) -> logging.Logger:
        logger = logging.Logger("test_binance_collector")
        logger.addHandler(logging.StreamHandler(stream))
        return logger

    async def test_handler_writes_data_to_stdout_and_warning_to_logger(self) -> None:
        stdout = RecordingStream()
        stderr = io.StringIO()
        handler = create_message_handler(
            StdoutOutput(stdout),
            self.make_logger(stderr),
        )

        await handler('{"e":"aggTrade","p":"1.00"}')
        invalid_message = '{"e":"aggTrade"'
        await handler(invalid_message)

        output_events = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(
            output_events,
            [
                {"event_type": "aggTrade", "price": "1.00"},
                {"raw_message": invalid_message},
            ],
        )
        self.assertNotIn("aggTrade", stderr.getvalue())
        self.assertIn("invalid_json", stderr.getvalue())

    async def test_run_collector_keeps_duplicate_messages(self) -> None:
        message = '{"e":"aggTrade","a":1}'
        collector = FakeCollectorRunner([message, message])
        stdout = RecordingStream()
        stderr = io.StringIO()
        stop_event = asyncio.Event()

        await run_collector(
            collector=collector,  # type: ignore[arg-type]
            output=StdoutOutput(stdout),
            stop_event=stop_event,
            logger=self.make_logger(stderr),
            manage_signals=False,
        )

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0]), json.loads(lines[1]))


if __name__ == "__main__":
    unittest.main()
