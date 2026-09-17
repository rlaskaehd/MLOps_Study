"""다중 스트림 설정과 연결 그룹 명세를 검증한다."""

import unittest

from src.collector.streams import (
    CONNECTION_GROUP_BY_KEY,
    Market,
    ROUTE_BY_STREAM,
    StreamType,
    build_stream_specs,
    group_stream_specs,
    index_stream_specs,
)
from src.config import DEFAULT_SYMBOLS, MultiStreamConfig


class MultiStreamConfigTests(unittest.TestCase):
    def test_default_builds_fifty_streams_in_three_connection_groups(self) -> None:
        config = MultiStreamConfig()
        specs = build_stream_specs(
            config.symbols,
            kline_interval=config.kline_interval,
            depth_speed=config.depth_speed,
            mark_price_speed=config.mark_price_speed,
        )
        grouped = group_stream_specs(specs)

        self.assertEqual(config.symbols, DEFAULT_SYMBOLS)
        self.assertEqual(len(specs), 50)
        self.assertEqual(len(grouped["spot_market"]), 30)
        self.assertEqual(len(grouped["spot_depth"]), 10)
        self.assertEqual(len(grouped["usdm_mark"]), 10)
        self.assertEqual(set(grouped), set(CONNECTION_GROUP_BY_KEY))

    def test_generates_expected_stream_names_and_collections(self) -> None:
        specs = build_stream_specs(("BTCUSDT",))
        by_type = {spec.stream_type: spec for spec in specs}

        self.assertEqual(by_type[StreamType.AGG_TRADE].stream_name, "btcusdt@aggTrade")
        self.assertEqual(by_type[StreamType.DEPTH].stream_name, "btcusdt@depth@100ms")
        self.assertEqual(
            by_type[StreamType.BOOK_TICKER].stream_name,
            "btcusdt@bookTicker",
        )
        self.assertEqual(by_type[StreamType.KLINE].stream_name, "btcusdt@kline_1m")
        self.assertEqual(
            by_type[StreamType.MARK_PRICE].stream_name,
            "btcusdt@markPrice@1s",
        )
        self.assertEqual(by_type[StreamType.AGG_TRADE].market, Market.SPOT)
        self.assertEqual(
            by_type[StreamType.MARK_PRICE].market,
            Market.USDM_FUTURES,
        )
        for stream_type, spec in by_type.items():
            self.assertEqual(spec.storage_route, ROUTE_BY_STREAM[stream_type])

    def test_custom_options_are_validated_without_changing_symbols(self) -> None:
        config = MultiStreamConfig(
            symbols=("ethusdt",),
            kline_interval="5m",
            depth_speed="1000ms",
            mark_price_speed="3s",
            validate_symbols=False,
        )
        specs = build_stream_specs(
            config.symbols,
            kline_interval=config.kline_interval,
            depth_speed=config.depth_speed,
            mark_price_speed=config.mark_price_speed,
        )

        self.assertEqual(config.symbols, ("ETHUSDT",))
        self.assertIn("ethusdt@kline_5m", {spec.stream_name for spec in specs})
        self.assertIn("ethusdt@depth@1000ms", {spec.stream_name for spec in specs})
        self.assertIn("ethusdt@markPrice@3s", {spec.stream_name for spec in specs})
        with self.assertRaises(ValueError):
            MultiStreamConfig(depth_speed="250ms")

    def test_stream_index_uses_connection_group_and_stream_name(self) -> None:
        specs = build_stream_specs(("BTCUSDT", "ETHUSDT"))
        index = index_stream_specs(specs)

        self.assertEqual(len(index), 10)
        self.assertEqual(
            index[("spot_market", "ethusdt@bookTicker")].symbol,
            "ETHUSDT",
        )


if __name__ == "__main__":
    unittest.main()
