"""실행 중인 로컬 MongoDB와 전체 표준화·적재 경로를 선택적으로 검증한다."""

import asyncio
import os
import unittest
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import replace

from pymongo import AsyncMongoClient

from src.config import load_mongo_config
from src.main import run_collector
from src.monitoring.stats import StatsCollector
from src.outputs.mongodb import MongoBatchOutput


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


@unittest.skipUnless(
    os.environ.get("STUDYGROUP_MONGO_INTEGRATION") == "1",
    "STUDYGROUP_MONGO_INTEGRATION=1일 때 로컬 MongoDB를 검증합니다.",
)
class LocalMongoIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_pipeline_preserves_every_document_and_count(self) -> None:
        collection_name = f"binance_events_test_{uuid.uuid4().hex}"
        config = replace(
            load_mongo_config(),
            collection=collection_name,
        )
        messages = (
            '{"result":null,"id":1}',
            '{"e":"aggTrade","s":"BTCUSDT","a":7,"p":"1.2300","nested":{"ok":true}}',
            '{"e":"aggTrade","s":"BTCUSDT","a":7,"p":"1.2300","nested":{"ok":true}}',
            '{"e":"aggTrade"',
        )
        stats = StatsCollector(
            ("BTCUSDT",),
            output_connected=True,
            storage_name="MongoDB",
        )
        output = MongoBatchOutput(
            config,
            flush_interval=0.05,
            on_batch_persisted=stats.record_persisted,
            on_state_changed=stats.set_storage_state,
        )
        verification_client = AsyncMongoClient(
            config.uri,
            serverSelectionTimeoutMS=2_000,
        )
        collection = verification_client[config.database][config.collection]

        try:
            await run_collector(
                symbols=("BTCUSDT",),
                mode="json",
                collector=SequenceCollector(messages),  # type: ignore[arg-type]
                output=output,
                stats=stats,
                stop_event=asyncio.Event(),
                manage_signals=False,
            )
            documents = await collection.find({}).sort("_id", 1).to_list(None)
        finally:
            await verification_client[config.database].drop_collection(
                config.collection
            )
            await verification_client.close()

        without_ids = [
            {key: value for key, value in document.items() if key != "_id"}
            for document in documents
        ]
        expected_trade = {
            "event_type": "aggTrade",
            "symbol": "BTCUSDT",
            "trade_id": 7,
            "price": "1.2300",
            "nested": {"ok": True},
        }
        self.assertEqual(
            without_ids,
            [
                {"result": None, "id": 1},
                expected_trade,
                expected_trade,
                {"raw_message": messages[-1]},
            ],
        )
        snapshot = stats.snapshot()
        self.assertEqual(snapshot.forwarded_messages, len(messages))
        self.assertEqual(snapshot.persisted_messages, len(messages))
        self.assertEqual(snapshot.pending_messages, 0)


if __name__ == "__main__":
    unittest.main()
