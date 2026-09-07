"""수집기의 데이터 보존 및 출력 계약을 검증한다."""

import base64
import io
import json
import unittest

from src.collector.parser import normalize_message
from src.outputs.stdout import StdoutOutput


class RecordingStream(io.StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.flush_count = 0

    def flush(self) -> None:
        self.flush_count += 1
        super().flush()


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


if __name__ == "__main__":
    unittest.main()
