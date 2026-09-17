"""다중 컬렉션 라우팅과 전체 버퍼·확인 적재 계약을 검증한다."""

import asyncio
import unittest
from typing import Any

from pymongo.errors import BulkWriteError

from src.config import load_mongo_config
from src.outputs.mongodb_multi import (
    BufferCapacityError,
    MongoMultiCollectionOutput,
    MultiMongoBatchWriteError,
)
from src.storage_routes import STORAGE_ROUTES, StorageRoute


TEST_COLLECTION_NAMES = {
    StorageRoute.AGG_TRADE: "agg_trades",
    StorageRoute.ORDER_BOOK_DEPTH: "order_book_depth",
    StorageRoute.BOOK_TICKER: "book_tickers",
    StorageRoute.KLINE: "klines",
    StorageRoute.MARK_PRICE: "mark_prices",
    StorageRoute.CONTROL: "collector_control",
}


class FakeAdmin:
    async def command(self, name: str) -> dict[str, int]:
        return {"ok": 1}


class FakeCollection:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls: list[tuple[list[dict[str, Any]], bool]] = []
        self.indexes: list[tuple[list[tuple[str, int]], str]] = []
        self.called = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()
        self.error: Exception | None = None

    async def create_index(
        self,
        keys: list[tuple[str, int]],
        *,
        name: str,
    ) -> str:
        self.indexes.append((keys, name))
        return name

    async def insert_many(
        self,
        documents: list[dict[str, Any]],
        *,
        ordered: bool,
    ) -> object:
        self.calls.append((documents, ordered))
        self.called.set()
        await self.release.wait()
        if self.error is not None:
            raise self.error
        return object()


class FakeDatabase:
    def __init__(self) -> None:
        self.collections: dict[str, FakeCollection] = {}

    def __getitem__(self, name: str) -> FakeCollection:
        return self.collections.setdefault(name, FakeCollection(name))


class FakeClient:
    def __init__(self) -> None:
        self.admin = FakeAdmin()
        self.database = FakeDatabase()
        self.closed = False

    def __getitem__(self, name: str) -> FakeDatabase:
        return self.database

    async def close(self) -> None:
        self.closed = True


def event(stream_type: str, symbol: str = "BTCUSDT") -> dict[str, Any]:
    return {
        "meta": {
            "schema_version": 2,
            "market": "spot",
            "stream_type": stream_type,
            "received_at_ms": 1,
        },
        "data": {"symbol": symbol, "value": stream_type},
    }


class MongoMultiCollectionOutputTests(unittest.IsolatedAsyncioTestCase):
    def make_output(
        self,
        *,
        client: FakeClient | None = None,
        **options: Any,
    ) -> tuple[MongoMultiCollectionOutput, FakeClient]:
        active_client = client or FakeClient()
        active_options: dict[str, Any] = {
            "collection_name_map": TEST_COLLECTION_NAMES,
            **options,
        }
        output = MongoMultiCollectionOutput(
            load_mongo_config(environ={}),
            flush_interval=0.02,
            operation_timeout=1,
            shutdown_timeout=1,
            client_factory=lambda uri, **kwargs: active_client,
            **active_options,
        )
        return output, active_client

    def test_requires_physical_name_for_every_storage_route(self) -> None:
        with self.assertRaisesRegex(ValueError, "물리 컬렉션 이름이 없는"):
            self.make_output(
                collection_name_map={StorageRoute.AGG_TRADE: "only_trades"}
            )

    async def test_routes_all_streams_and_control_without_mutating_inputs(self) -> None:
        persisted: list[tuple[StorageRoute, int]] = []
        output, client = self.make_output(
            on_batch_persisted=lambda collection, count, duration: persisted.append(
                (collection, count)
            )
        )
        inputs = [
            event("aggTrade"),
            event("depth"),
            event("bookTicker"),
            event("kline"),
            event("markPrice"),
            event("control"),
        ]

        await output.open()
        for item in inputs:
            await output.write(item)
        await output.close()

        expected = {
            "agg_trades",
            "order_book_depth",
            "book_tickers",
            "klines",
            "mark_prices",
            "collector_control",
        }
        self.assertEqual(set(client.database.collections), expected)
        for name in expected:
            self.assertEqual(len(client.database.collections[name].calls), 1)
            documents, ordered = client.database.collections[name].calls[0]
            self.assertEqual(len(documents), 1)
            self.assertTrue(ordered)
        self.assertEqual({route for route, count in persisted}, set(STORAGE_ROUTES))
        self.assertEqual(output.pending_count, 0)
        self.assertTrue(client.closed)
        self.assertNotIn("_id", inputs[0])
        for name in expected - {"collector_control"}:
            self.assertEqual(
                client.database.collections[name].indexes[0][1],
                "market_symbol_received_at",
            )

    async def test_inflight_document_keeps_global_capacity_reserved(self) -> None:
        client = FakeClient()
        collection = client.database["agg_trades"]
        collection.release.clear()
        output, _ = self.make_output(
            client=client,
            buffer_max_documents=1,
            batch_max_documents=1,
            create_indexes=False,
        )
        await output.open()
        await output.write(event("aggTrade"))
        await asyncio.wait_for(collection.called.wait(), timeout=1)

        blocked = asyncio.create_task(output.write(event("bookTicker")))
        await asyncio.sleep(0)
        self.assertFalse(blocked.done())
        self.assertEqual(output.pending_count, 1)

        collection.release.set()
        await asyncio.wait_for(blocked, timeout=1)
        await output.close()
        self.assertEqual(output.pending_count, 0)

    async def test_rejects_single_document_larger_than_buffer_budget(self) -> None:
        output, _ = self.make_output(
            buffer_max_bytes=100,
            batch_max_bytes=100,
            create_indexes=False,
        )
        await output.open()
        with self.assertRaises(BufferCapacityError):
            await output.write(event("aggTrade", symbol="B" * 500))
        await output.close()
        self.assertEqual(output.pending_count, 0)

    async def test_can_use_isolated_physical_collection_names(self) -> None:
        prefix = "probe_"
        mapping = {route: f"{prefix}{route.value}" for route in STORAGE_ROUTES}
        output, client = self.make_output(
            collection_name_map=mapping,
            create_indexes=False,
        )

        await output.open()
        await output.write(event("markPrice"))
        await output.close()

        self.assertIn("probe_mark_price", client.database.collections)
        self.assertEqual(
            len(client.database.collections["probe_mark_price"].calls),
            1,
        )

    async def test_partial_bulk_failure_reports_only_confirmed_documents(self) -> None:
        client = FakeClient()
        collection = client.database["agg_trades"]
        collection.error = BulkWriteError(
            {
                "writeErrors": [{"index": 1, "code": 1, "errmsg": "failed"}],
                "writeConcernErrors": [],
                "nInserted": 1,
                "nUpserted": 0,
                "nMatched": 0,
                "nModified": 0,
                "nRemoved": 0,
                "upserted": [],
            }
        )
        persisted: list[tuple[StorageRoute, int]] = []
        output, _ = self.make_output(
            client=client,
            batch_max_documents=2,
            create_indexes=False,
            on_batch_persisted=lambda name, count, duration: persisted.append(
                (name, count)
            ),
        )
        await output.open()
        await output.write(event("aggTrade", "BTCUSDT"))
        await output.write(event("aggTrade", "ETHUSDT"))

        with self.assertRaises(MultiMongoBatchWriteError):
            await asyncio.wait_for(output.wait_failed(), timeout=1)

        self.assertEqual(persisted, [(StorageRoute.AGG_TRADE, 1)])
        self.assertEqual(output.pending_count, 1)
        self.assertEqual(output.pending_by_route[StorageRoute.AGG_TRADE], 1)
        await output.close()
        self.assertTrue(client.closed)


if __name__ == "__main__":
    unittest.main()
