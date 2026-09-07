"""수집기의 데이터 보존 및 출력 계약을 검증한다."""

import asyncio
import base64
import io
import json
import unittest
from collections.abc import Sequence
from typing import Any

from src.collector.client import BinanceCollector, MessageHandlerError
from src.collector.parser import normalize_message
from src.collector.reconnect import ReconnectPolicy
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

    async def __aenter__(self) -> FakeWebSocket:
        if isinstance(self._outcome, BaseException):
            raise self._outcome
        return FakeWebSocket(self._outcome)

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

    def __call__(self, url: str, **kwargs: Any) -> FakeConnectionContext:
        self.calls.append((url, kwargs))
        return FakeConnectionContext(next(self._outcomes))


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


class StdoutOutputTests(unittest.TestCase):
    def test_writes_one_compact_json_object_per_line_and_flushes(self) -> None:
        stream = RecordingStream()
        output = StdoutOutput(stream)
        events = [
            {"symbol": "BTCUSDT", "price": "1.00"},
            {"symbol": "BTCUSDT", "quantity": "0.10"},
        ]

        for event in events:
            output.write(event)

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
        messages = [
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

    async def test_reconnects_after_transient_connection_failure(self) -> None:
        message = '{"e":"aggTrade","a":1}'
        factory = FakeConnectionFactory([OSError("temporary"), [message]])
        collector = BinanceCollector(
            connection_factory=factory,
            reconnect_policy=ReconnectPolicy(delays=(0,)),
        )
        stop_event = asyncio.Event()
        received: list[str | bytes] = []

        def handler(value: str | bytes) -> None:
            received.append(value)
            stop_event.set()

        await collector.run(handler, stop_event)

        self.assertEqual(received, [message])
        self.assertEqual(len(factory.calls), 2)

    async def test_outputs_server_shutdown_before_reconnecting(self) -> None:
        shutdown = '{"e":"serverShutdown","E":1788783123456}'
        trade = '{"e":"aggTrade","a":1}'
        factory = FakeConnectionFactory([[shutdown], [trade]])
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

        self.assertEqual(received, [shutdown, trade])
        self.assertEqual(len(factory.calls), 2)

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


if __name__ == "__main__":
    unittest.main()
