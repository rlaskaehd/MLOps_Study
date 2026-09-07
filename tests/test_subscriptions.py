"""다중 심볼 설정과 Binance 구독 요청을 검증한다."""

import json
import unittest

from src.collector.subscriptions import (
    SubscriptionResponseKind,
    agg_trade_streams,
    build_subscribe_request,
    parse_subscription_response,
)
from src.config import DEFAULT_SYMBOLS, normalize_symbols, parse_args


class SymbolConfigTests(unittest.TestCase):
    def test_default_contains_planned_ten_symbols_in_display_order(self) -> None:
        self.assertEqual(len(DEFAULT_SYMBOLS), 10)
        self.assertEqual(DEFAULT_SYMBOLS[0], "BTCUSDT")
        self.assertEqual(DEFAULT_SYMBOLS[-1], "LTCUSDT")
        self.assertEqual(parse_args([]).symbols, DEFAULT_SYMBOLS)

    def test_command_line_symbols_replace_defaults_and_are_normalized(self) -> None:
        config = parse_args(["--symbols", "ethusdt", "btcusdt"])

        self.assertEqual(config.symbols, ("ETHUSDT", "BTCUSDT"))

    def test_rejects_duplicate_or_invalid_symbols(self) -> None:
        with self.assertRaises(ValueError):
            normalize_symbols(("BTCUSDT", "btcusdt"))
        with self.assertRaises(ValueError):
            normalize_symbols(("BTC-USDT",))


class SubscriptionRequestTests(unittest.TestCase):
    def test_builds_one_raw_subscription_request_for_all_symbols(self) -> None:
        symbols = ("BTCUSDT", "ETHUSDT")

        streams = agg_trade_streams(symbols)
        request = json.loads(build_subscribe_request(symbols))

        self.assertEqual(streams, ("btcusdt@aggTrade", "ethusdt@aggTrade"))
        self.assertEqual(
            request,
            {
                "method": "SUBSCRIBE",
                "params": list(streams),
                "id": 1,
            },
        )

    def test_identifies_only_matching_success_and_error_responses(self) -> None:
        success = parse_subscription_response('{"result":null,"id":1}')
        error = parse_subscription_response(
            '{"code":2,"msg":"invalid request","id":1}'
        )

        self.assertIsNotNone(success)
        self.assertIsNotNone(error)
        assert success is not None and error is not None
        self.assertIs(success.kind, SubscriptionResponseKind.SUCCESS)
        self.assertIs(error.kind, SubscriptionResponseKind.ERROR)
        self.assertEqual(error.detail, "invalid request")
        self.assertIsNone(
            parse_subscription_response('{"result":null,"id":99}')
        )
        self.assertIsNone(
            parse_subscription_response('{"e":"aggTrade","id":1}')
        )


if __name__ == "__main__":
    unittest.main()
