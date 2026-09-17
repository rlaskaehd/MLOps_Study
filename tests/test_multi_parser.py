"""다중 스트림 v2 이벤트의 의미와 원본 보존을 검증한다."""

import unittest

from src.collector.multi_parser import normalize_received_message
from src.collector.streams import build_stream_specs, index_stream_specs
from src.models.event import ReceivedMessage


class MultiStreamParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.index = index_stream_specs(build_stream_specs(("BTCUSDT",)))

    @staticmethod
    def received(payload: str | bytes, group: str = "spot_market") -> ReceivedMessage:
        return ReceivedMessage(
            payload=payload,
            connection_group=group,
            connection_id=f"run-{group}-1",
            receive_sequence=7,
            received_at_ms=1_800_000_000_100,
        )

    def test_agg_trade_uses_trade_specific_field_names(self) -> None:
        result = normalize_received_message(
            self.received(
                '{"stream":"btcusdt@aggTrade","data":'
                '{"e":"aggTrade","E":1,"s":"BTCUSDT","a":2,'
                '"p":"3.00","q":"4.0","T":5,"m":true,"M":true}}'
            ),
            self.index,
        )

        self.assertIsNone(result.warning)
        self.assertEqual(result.event["meta"]["market"], "spot")
        self.assertEqual(result.event["meta"]["stream_type"], "aggTrade")
        self.assertEqual(result.event["data"]["trade_id"], 2)
        self.assertEqual(result.event["data"]["trade_time"], 5)
        self.assertEqual(result.event["data"]["price"], "3.00")
        self.assertIs(result.event["data"]["is_buyer_maker"], True)
        self.assertIs(result.event["data"]["M"], True)

    def test_depth_maps_asks_as_array_and_keeps_zero_quantity(self) -> None:
        result = normalize_received_message(
            self.received(
                '{"stream":"btcusdt@depth@100ms","data":'
                '{"e":"depthUpdate","E":1,"s":"BTCUSDT",'
                '"U":10,"u":12,"b":[["1","0"]],"a":[["2","3"]]}}',
                group="spot_depth",
            ),
            self.index,
        )

        self.assertEqual(result.event["data"]["first_update_id"], 10)
        self.assertEqual(result.event["data"]["final_update_id"], 12)
        self.assertEqual(result.event["data"]["bids"], [["1", "0"]])
        self.assertEqual(result.event["data"]["asks"], [["2", "3"]])
        self.assertNotIn("trade_id", result.event["data"])

    def test_book_ticker_does_not_invent_event_type_or_event_time(self) -> None:
        result = normalize_received_message(
            self.received(
                '{"stream":"btcusdt@bookTicker","data":'
                '{"u":1,"s":"BTCUSDT","b":"10","B":"2",'
                '"a":"11","A":"3"},"future_outer":"kept"}'
            ),
            self.index,
        )

        data = result.event["data"]
        self.assertNotIn("event_type", data)
        self.assertNotIn("event_time", data)
        self.assertEqual(data["ask_price"], "11")
        self.assertEqual(data["ask_quantity"], "3")
        self.assertEqual(result.event["transport"], {"future_outer": "kept"})

    def test_kline_keeps_nested_payload_and_close_state_unchanged(self) -> None:
        result = normalize_received_message(
            self.received(
                '{"stream":"btcusdt@kline_1m","data":'
                '{"e":"kline","E":1,"s":"BTCUSDT","k":'
                '{"t":2,"T":3,"i":"1m","o":"10.0","x":false}}}'
            ),
            self.index,
        )

        self.assertEqual(
            result.event["data"]["k"],
            {"t": 2, "T": 3, "i": "1m", "o": "10.0", "x": False},
        )

    def test_mark_price_uses_funding_meaning_for_T(self) -> None:
        result = normalize_received_message(
            self.received(
                '{"stream":"btcusdt@markPrice@1s","data":'
                '{"e":"markPriceUpdate","E":1,"s":"BTCUSDT",'
                '"p":"100","i":"99","P":null,"r":"0.001",'
                '"T":8,"ap":"100.1","st":1}}',
                group="usdm_mark",
            ),
            self.index,
        )

        data = result.event["data"]
        self.assertEqual(result.event["meta"]["market"], "usdm_futures")
        self.assertEqual(data["next_funding_time"], 8)
        self.assertEqual(data["mark_price"], "100")
        self.assertIsNone(data["estimated_settle_price"])
        self.assertEqual(data["ap"], "100.1")

    def test_ack_unknown_stream_and_invalid_message_are_control_events(self) -> None:
        ack = normalize_received_message(
            self.received('{"result":null,"id":1}'),
            self.index,
        )
        unknown = normalize_received_message(
            self.received('{"stream":"btcusdt@other","data":{"x":1}}'),
            self.index,
        )
        invalid = normalize_received_message(self.received('{"broken"'), self.index)

        self.assertEqual(ack.event["meta"]["stream_type"], "control")
        self.assertEqual(ack.event["data"], {"result": None, "id": 1})
        self.assertEqual(unknown.warning, "unknown_stream")
        self.assertEqual(invalid.warning, "invalid_json")
        self.assertEqual(invalid.event["data"]["raw_message"], '{"broken"')

    def test_collision_preserves_whole_message_as_control(self) -> None:
        raw = (
            '{"stream":"btcusdt@bookTicker","data":'
            '{"a":"11","ask_price":"12","s":"BTCUSDT"}}'
        )
        result = normalize_received_message(self.received(raw), self.index)

        self.assertEqual(result.warning, "field_name_collision")
        self.assertEqual(result.event["meta"]["stream_type"], "control")
        self.assertEqual(result.event["data"]["raw_message"], raw)


if __name__ == "__main__":
    unittest.main()
