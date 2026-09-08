"""MongoDB 제한 큐, 1초 배치, 오류와 종료 동작을 검증한다."""

import asyncio
import unittest
from typing import Any

from src.config import load_mongo_config
from src.outputs.mongodb import MongoBatchOutput, MongoBatchWriteError


class FakeAdmin:
    def __init__(self) -> None:
        self.commands: list[str] = []

    async def command(self, name: str) -> dict[str, int]:
        self.commands.append(name)
        return {"ok": 1}


class FakeCollection:
    def __init__(self) -> None:
        self.calls: list[tuple[list[dict[str, Any]], bool]] = []
        self.called = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()
        self.error: Exception | None = None

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
    def __init__(self, collection: FakeCollection) -> None:
        self.collection = collection
        self.requested_collection: str | None = None

    def __getitem__(self, name: str) -> FakeCollection:
        self.requested_collection = name
        return self.collection


class FakeClient:
    def __init__(self, collection: FakeCollection) -> None:
        self.admin = FakeAdmin()
        self.database = FakeDatabase(collection)
        self.requested_database: str | None = None
        self.closed = False

    def __getitem__(self, name: str) -> FakeDatabase:
        self.requested_database = name
        return self.database

    async def close(self) -> None:
        self.closed = True


class FakeClientFactory:
    def __init__(self, client: FakeClient) -> None:
        self.client = client
        self.uri: str | None = None
        self.options: dict[str, Any] = {}

    def __call__(self, uri: str, **options: Any) -> FakeClient:
        self.uri = uri
        self.options = options
        return self.client


class MongoBatchOutputTests(unittest.IsolatedAsyncioTestCase):
    def make_output(
        self,
        *,
        collection: FakeCollection | None = None,
        flush_interval: float = 0.02,
        queue_maxsize: int = 10,
        on_batch_persisted: Any = None,
        on_state_changed: Any = None,
    ) -> tuple[MongoBatchOutput, FakeClient, FakeCollection, FakeClientFactory]:
        active_collection = collection or FakeCollection()
        client = FakeClient(active_collection)
        factory = FakeClientFactory(client)
        output = MongoBatchOutput(
            load_mongo_config(environ={}),
            flush_interval=flush_interval,
            queue_maxsize=queue_maxsize,
            operation_timeout=1,
            shutdown_timeout=1,
            client_factory=factory,
            on_batch_persisted=on_batch_persisted,
            on_state_changed=on_state_changed,
        )
        return output, client, active_collection, factory

    async def test_batches_events_without_mutating_or_filtering_them(self) -> None:
        output, client, collection, factory = self.make_output()
        first = {
            "event_type": "aggTrade",
            "symbol": "BTCUSDT",
            "price": "1.2300",
            "nested": {"value": [1, None, True]},
        }
        duplicate = {
            "event_type": "aggTrade",
            "symbol": "BTCUSDT",
            "price": "1.2300",
            "nested": {"value": [1, None, True]},
        }
        control = {"result": None, "id": 1}

        await output.open()
        await output.write(first)
        await output.write(duplicate)
        await output.write(control)
        await asyncio.wait_for(collection.called.wait(), timeout=1)
        await output.close()

        self.assertEqual(factory.uri, "mongodb://localhost:27017")
        self.assertEqual(client.admin.commands, ["ping"])
        self.assertEqual(client.requested_database, "studygroup")
        self.assertEqual(client.database.requested_collection, "binance_events")
        self.assertTrue(client.closed)
        self.assertEqual(len(collection.calls), 1)
        documents, ordered = collection.calls[0]
        self.assertEqual(documents, [first, duplicate, control])
        self.assertTrue(ordered)
        self.assertIsNot(documents[0], first)
        self.assertIsNot(documents[0]["nested"], first["nested"])
        self.assertEqual(first["price"], "1.2300")
        self.assertNotIn("_id", first)

    async def test_empty_intervals_do_not_issue_database_writes(self) -> None:
        output, _, collection, _ = self.make_output(flush_interval=0.01)

        await output.open()
        await asyncio.sleep(0.04)
        await output.close()

        self.assertEqual(collection.calls, [])

    async def test_close_flushes_pending_batch_without_waiting_for_interval(self) -> None:
        output, _, collection, _ = self.make_output(flush_interval=60)

        await output.open()
        await output.write({"raw_message": "preserve me"})
        await asyncio.wait_for(output.close(), timeout=1)

        self.assertEqual(len(collection.calls), 1)
        self.assertEqual(collection.calls[0][0], [{"raw_message": "preserve me"}])
        self.assertEqual(output.pending_count, 0)

    async def test_full_queue_applies_backpressure_until_worker_drains(self) -> None:
        collection = FakeCollection()
        collection.release.clear()
        output, _, _, _ = self.make_output(
            collection=collection,
            flush_interval=0.01,
            queue_maxsize=1,
        )
        await output.open()
        await output.write({"sequence": 1})
        await asyncio.wait_for(collection.called.wait(), timeout=1)
        await output.write({"sequence": 2})

        third_write = asyncio.create_task(output.write({"sequence": 3}))
        await asyncio.sleep(0)
        self.assertFalse(third_write.done())

        collection.release.set()
        await asyncio.wait_for(third_write, timeout=1)
        await output.close()

        sequences = [
            document["sequence"]
            for batch, _ in collection.calls
            for document in batch
        ]
        self.assertEqual(sequences, [1, 2, 3])

    async def test_background_insert_failure_is_reported_without_new_event(self) -> None:
        collection = FakeCollection()
        collection.error = OSError("mongo unavailable")
        output, client, _, _ = self.make_output(
            collection=collection,
            flush_interval=0.01,
        )
        await output.open()
        await output.write({"sequence": 1})

        with self.assertRaises(MongoBatchWriteError):
            await asyncio.wait_for(output.wait_failed(), timeout=1)

        self.assertEqual(output.pending_count, 1)
        await output.close()
        self.assertTrue(client.closed)

    async def test_reports_confirmed_batch_and_storage_states(self) -> None:
        persisted: list[tuple[int, float]] = []
        states: list[str] = []
        output, _, collection, _ = self.make_output(
            flush_interval=0.01,
            on_batch_persisted=lambda count, duration: persisted.append(
                (count, duration)
            ),
            on_state_changed=states.append,
        )

        await output.open()
        await output.write({"sequence": 1})
        await output.write({"sequence": 2})
        await asyncio.wait_for(collection.called.wait(), timeout=1)
        await output.close()

        self.assertEqual(len(persisted), 1)
        self.assertEqual(persisted[0][0], 2)
        self.assertGreaterEqual(persisted[0][1], 0)
        self.assertEqual(states[0:2], ["연결 확인 중", "연결됨"])
        self.assertIn("적재 중", states)
        self.assertEqual(states[-1], "종료됨")


if __name__ == "__main__":
    unittest.main()
