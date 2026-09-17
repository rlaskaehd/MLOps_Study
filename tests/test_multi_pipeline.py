"""다중 스트림 표준화에서 출력까지 한 번씩 전달되는지 검증한다."""

import logging
import unittest
from typing import Any

from src.collector.streams import build_stream_specs, index_stream_specs
from src.models.event import Event, ReceivedMessage
from src.monitoring.multi_stats import MultiStreamStatsCollector
from src.multi_pipeline import MultiStreamEventDispatcher
from src.storage_routes import StorageRoute


class MemoryOutput:
    def __init__(self) -> None:
        self.events: list[Event] = []
        self.opened = False
        self.closed = False

    async def open(self) -> None:
        self.opened = True

    async def write(self, event: Event) -> None:
        self.events.append(event)

    async def close(self) -> None:
        self.closed = True


def received(payload: str, sequence: int) -> ReceivedMessage:
    return ReceivedMessage(
        payload=payload,
        connection_group="spot_market",
        connection_id="c1",
        receive_sequence=sequence,
        received_at_ms=1_100,
    )


class MultiStreamEventDispatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_forwards_market_control_and_duplicate_messages_once(self) -> None:
        output = MemoryOutput()
        stats = MultiStreamStatsCollector(("BTCUSDT",))
        dispatcher = MultiStreamEventDispatcher(
            stream_index=index_stream_specs(build_stream_specs(("BTCUSDT",))),
            output=output,
            stats=stats,
            logger=logging.getLogger("multi-pipeline-test"),
        )
        trade = (
            '{"stream":"btcusdt@aggTrade","data":'
            '{"e":"aggTrade","E":1000,"s":"BTCUSDT","a":1,'
            '"p":"10.0","q":"1.0","T":999,"m":false}}'
        )

        await dispatcher.open()
        await dispatcher.handle(received('{"result":null,"id":1}', 1))
        await dispatcher.handle(received(trade, 2))
        await dispatcher.handle(received(trade, 3))
        await dispatcher.close()

        self.assertTrue(output.opened)
        self.assertTrue(output.closed)
        self.assertEqual(len(output.events), 3)
        self.assertEqual(output.events[0]["meta"]["stream_type"], "control")
        self.assertEqual(output.events[1]["data"]["price"], "10.0")
        self.assertEqual(output.events[1]["data"], output.events[2]["data"])
        snapshot = stats.snapshot()
        self.assertEqual(snapshot.control_or_unclassified, 1)
        self.assertEqual(snapshot.cumulative_by_stream["aggTrade"], 2)
        self.assertEqual(snapshot.forwarded_messages, 3)
        self.assertEqual(snapshot.collections[StorageRoute.AGG_TRADE].accepted, 2)
        self.assertEqual(snapshot.collections[StorageRoute.CONTROL].accepted, 1)


if __name__ == "__main__":
    unittest.main()
