"""다중 심볼 설정과 Binance 구독 요청을 검증한다."""

import json
import io
import unittest

from src.collector.subscriptions import (
    SubscriptionResponseKind,
    agg_trade_streams,
    build_subscribe_request,
    parse_subscription_response,
)
from src.config import (
    DEFAULT_SYMBOLS,
    ConfigurationError,
    normalize_symbols,
    parse_args,
    validate_terminal_config,
)


class TtyStream(io.StringIO):
    def __init__(self, is_terminal: bool) -> None:
        super().__init__()
        self._is_terminal = is_terminal

    def isatty(self) -> bool:
        return self._is_terminal


class SymbolConfigTests(unittest.TestCase):
    def test_default_contains_planned_ten_symbols_in_display_order(self) -> None:
        self.assertEqual(len(DEFAULT_SYMBOLS), 10)
        self.assertEqual(DEFAULT_SYMBOLS[0], "BTCUSDT")
        self.assertEqual(DEFAULT_SYMBOLS[-1], "LTCUSDT")
        config = parse_args([])
        self.assertEqual(config.symbols, DEFAULT_SYMBOLS)
        self.assertEqual(config.mode, "json")
        self.assertEqual(config.output, "stdout")

    def test_command_line_symbols_replace_defaults_and_are_normalized(self) -> None:
        config = parse_args(["--symbols", "ethusdt", "btcusdt"])

        self.assertEqual(config.symbols, ("ETHUSDT", "BTCUSDT"))

    def test_rejects_duplicate_or_invalid_symbols(self) -> None:
        with self.assertRaises(ValueError):
            normalize_symbols(("BTCUSDT", "btcusdt"))
        with self.assertRaises(ValueError):
            normalize_symbols(("BTC-USDT",))

    def test_tui_defaults_to_no_output_and_can_select_stdout(self) -> None:
        no_output = parse_args(["--mode", "tui"])
        stdout_output = parse_args(["--mode", "tui", "--output", "stdout"])

        self.assertIsNone(no_output.output)
        self.assertEqual(stdout_output.output, "stdout")

    def test_tui_requires_stderr_terminal(self) -> None:
        config = parse_args(["--mode", "tui"])

        with self.assertRaises(ConfigurationError):
            validate_terminal_config(
                config,
                stdout=TtyStream(False),
                stderr=TtyStream(False),
            )

    def test_tui_stdout_output_rejects_same_terminal(self) -> None:
        config = parse_args(["--mode", "tui", "--output", "stdout"])

        with self.assertRaises(ConfigurationError):
            validate_terminal_config(
                config,
                stdout=TtyStream(True),
                stderr=TtyStream(True),
            )
        validate_terminal_config(
            config,
            stdout=TtyStream(False),
            stderr=TtyStream(True),
        )


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
        error = parse_subscription_response('{"code":2,"msg":"invalid request","id":1}')

        self.assertIsNotNone(success)
        self.assertIsNotNone(error)
        assert success is not None and error is not None
        self.assertIs(success.kind, SubscriptionResponseKind.SUCCESS)
        self.assertIs(error.kind, SubscriptionResponseKind.ERROR)
        self.assertEqual(error.detail, "invalid request")
        self.assertIsNone(parse_subscription_response('{"result":null,"id":99}'))
        self.assertIsNone(parse_subscription_response('{"e":"aggTrade","id":1}'))
        self.assertIsNone(parse_subscription_response('{"result":null,"id":2,"id":1}'))


if __name__ == "__main__":
    unittest.main()
