"""명시적으로 활성화할 때 실제 MongoDB의 6개 임시 컬렉션을 검증한다."""

import os
import unittest
import uuid
from contextlib import suppress

from pymongo import AsyncMongoClient

from src.config import (
    MULTI_COLLECTION_ENV_BY_ROUTE,
    load_mongo_config,
    load_multi_mongo_collection_config,
)
from src.outputs.mongodb_multi import MongoMultiCollectionOutput
from src.storage_routes import STORAGE_ROUTES


def event(stream_type: str) -> dict[str, object]:
    return {
        "meta": {
            "schema_version": 2,
            "exchange": "binance",
            "market": "spot",
            "stream_type": stream_type,
            "received_at_ms": 1,
            "connection_id": "integration-c1",
            "receive_sequence": 1,
        },
        "data": {"symbol": "BTCUSDT", "price": "1.2300"},
    }


@unittest.skipUnless(
    os.environ.get("STUDYGROUP_MONGO_MULTI_INTEGRATION") == "1",
    "STUDYGROUP_MONGO_MULTI_INTEGRATION=1일 때 다중 컬렉션을 검증합니다.",
)
class LocalMultiMongoIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_each_event_is_stored_once_in_its_temporary_collection(self) -> None:
        config = load_mongo_config()
        prefix = f"multi_integration_{uuid.uuid4().hex}_"
        names = {route: f"{prefix}{route.value}" for route in STORAGE_ROUTES}
        collection_config = load_multi_mongo_collection_config(
            environ={
                MULTI_COLLECTION_ENV_BY_ROUTE[route]: physical_name
                for route, physical_name in names.items()
            }
        )
        output = MongoMultiCollectionOutput(
            config,
            collection_name_map=collection_config.collection_name_by_route,
            flush_interval=0.01,
        )
        inspector = AsyncMongoClient(
            config.uri,
            retryWrites=False,
            serverSelectionTimeoutMS=5_000,
            connectTimeoutMS=5_000,
            socketTimeoutMS=5_000,
        )
        opened = False
        try:
            await output.open()
            opened = True
            for stream_type in (
                "aggTrade",
                "depth",
                "bookTicker",
                "kline",
                "markPrice",
                "control",
            ):
                await output.write(event(stream_type))
            await output.close()
            opened = False

            database = inspector[config.database]
            for route, physical in names.items():
                with self.subTest(route=route.value):
                    self.assertEqual(
                        await database[physical].count_documents({}),
                        1,
                    )
                    document = await database[physical].find_one({})
                    assert document is not None
                    self.assertEqual(document["data"]["price"], "1.2300")
            self.assertEqual(output.pending_count, 0)
        finally:
            if opened:
                await output.close()
            database = inspector[config.database]
            for physical in names.values():
                with suppress(Exception):
                    await database.drop_collection(physical)
            await inspector.close()


if __name__ == "__main__":
    unittest.main()
