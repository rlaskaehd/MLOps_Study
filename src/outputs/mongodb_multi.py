"""v2 이벤트를 컬렉션별 제한 배치로 MongoDB에 적재한다."""

from __future__ import annotations

import asyncio
import copy
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from bson import BSON
from pymongo import ASCENDING, AsyncMongoClient
from pymongo.errors import BulkWriteError

from src.config import MongoConfig
from src.models.event import Event
from src.outputs.base import EventOutput
from src.storage_routes import (
    DATA_STORAGE_ROUTES,
    STORAGE_ROUTES,
    StorageRoute,
    storage_route_for_stream_type,
)


class MultiMongoOutputError(RuntimeError):
    """다중 컬렉션 출력의 준비, 적재 또는 종료가 실패한 경우."""


class MultiMongoStateError(MultiMongoOutputError):
    """출력 생명주기에 맞지 않는 호출인 경우."""


class MultiMongoBatchWriteError(MultiMongoOutputError):
    """한 컬렉션 이상의 배치 저장 결과를 모두 확인할 수 없는 경우."""


class BufferCapacityError(MultiMongoOutputError):
    """한 문서가 전체 버퍼 바이트 상한을 초과하는 경우."""


@dataclass(frozen=True, slots=True)
class BufferedDocument:
    event: Event
    bson_size: int


class BufferBudget:
    """큐와 처리 중 배치를 합친 문서 수와 직렬화 바이트를 제한한다."""

    def __init__(self, max_documents: int, max_bytes: int) -> None:
        self.max_documents = max_documents
        self.max_bytes = max_bytes
        self.documents = 0
        self.bytes = 0
        self._failed = False
        self._condition = asyncio.Condition()

    async def acquire(self, size: int) -> None:
        if size > self.max_bytes:
            raise BufferCapacityError(
                f"문서 크기 {size}바이트가 전체 버퍼 상한 "
                f"{self.max_bytes}바이트를 초과합니다."
            )
        async with self._condition:
            await self._condition.wait_for(
                lambda: self._failed
                or (
                    self.documents < self.max_documents
                    and self.bytes + size <= self.max_bytes
                )
            )
            if self._failed:
                raise MultiMongoBatchWriteError(
                    "MongoDB 배치 실패로 새 문서를 접수할 수 없습니다."
                )
            self.documents += 1
            self.bytes += size

    async def release(self, documents: int, size: int) -> None:
        async with self._condition:
            self.documents -= documents
            self.bytes -= size
            if self.documents < 0 or self.bytes < 0:
                raise RuntimeError("MongoDB 버퍼 예산 계수가 음수가 되었습니다.")
            self._condition.notify_all()

    async def mark_failed(self) -> None:
        async with self._condition:
            self._failed = True
            self._condition.notify_all()


BatchPersistedCallback = Callable[[StorageRoute, int, float], None]
StateChangedCallback = Callable[[StorageRoute | str, str], None]
BufferChangedCallback = Callable[[StorageRoute, int, int, int, int], None]


class MongoMultiCollectionOutput(EventOutput):
    """이벤트 종류를 라우팅하고 컬렉션별 순차 writer로 적재한다."""

    def __init__(
        self,
        config: MongoConfig,
        *,
        collection_name_map: Mapping[StorageRoute, str],
        routes: Sequence[StorageRoute] = STORAGE_ROUTES,
        flush_interval: float = 1.0,
        buffer_max_documents: int = 10_000,
        buffer_max_bytes: int = 64 * 1024 * 1024,
        batch_max_documents: int = 1_000,
        batch_max_bytes: int = 4 * 1024 * 1024,
        max_concurrent_writes: int = 3,
        operation_timeout: float = 5.0,
        shutdown_timeout: float = 10.0,
        create_indexes: bool = True,
        client_factory: Callable[..., Any] = AsyncMongoClient,
        on_batch_persisted: BatchPersistedCallback | None = None,
        on_state_changed: StateChangedCallback | None = None,
        on_buffer_changed: BufferChangedCallback | None = None,
    ) -> None:
        for name, value in (
            ("flush_interval", flush_interval),
            ("buffer_max_documents", buffer_max_documents),
            ("buffer_max_bytes", buffer_max_bytes),
            ("batch_max_documents", batch_max_documents),
            ("batch_max_bytes", batch_max_bytes),
            ("max_concurrent_writes", max_concurrent_writes),
            ("operation_timeout", operation_timeout),
            ("shutdown_timeout", shutdown_timeout),
        ):
            if value <= 0:
                raise ValueError(f"{name}은 0보다 커야 합니다.")
        active_routes = tuple(dict.fromkeys(routes))
        if set(STORAGE_ROUTES) - set(active_routes):
            raise ValueError("다중 스트림의 5개 데이터 경로와 제어 경로가 필요합니다.")
        provided_names = dict(collection_name_map)
        unknown_routes = set(provided_names) - set(active_routes)
        if unknown_routes:
            raise ValueError(
                "알 수 없는 논리 저장 경로입니다: "
                + ", ".join(sorted(route.value for route in unknown_routes))
            )
        missing_routes = set(active_routes) - set(provided_names)
        if missing_routes:
            raise ValueError(
                "물리 컬렉션 이름이 없는 논리 저장 경로입니다: "
                + ", ".join(sorted(route.value for route in missing_routes))
            )
        physical_names = {
            route: provided_names[route].strip() for route in active_routes
        }
        if any(not name.strip() for name in physical_names.values()):
            raise ValueError("MongoDB 물리 컬렉션 이름은 비어 있을 수 없습니다.")
        if len(set(physical_names.values())) != len(physical_names):
            raise ValueError("서로 다른 이벤트 종류가 같은 물리 컬렉션을 사용할 수 없습니다.")

        self.config = config
        self.routes = active_routes
        self.collection_name_by_route = MappingProxyType(physical_names)
        self.flush_interval = flush_interval
        self.batch_max_documents = min(batch_max_documents, buffer_max_documents)
        self.batch_max_bytes = min(batch_max_bytes, buffer_max_bytes)
        self.max_concurrent_writes = max_concurrent_writes
        self.operation_timeout = operation_timeout
        self.shutdown_timeout = shutdown_timeout
        self.create_indexes = create_indexes
        self._client_factory = client_factory
        self._on_batch_persisted = on_batch_persisted
        self._on_state_changed = on_state_changed
        self._on_buffer_changed = on_buffer_changed
        self._budget = BufferBudget(buffer_max_documents, buffer_max_bytes)
        self._write_semaphore = asyncio.Semaphore(max_concurrent_writes)
        self._queues: dict[StorageRoute, asyncio.Queue[BufferedDocument]] = {
            route: asyncio.Queue() for route in self.routes
        }
        self._pending_documents = {route: 0 for route in self.routes}
        self._pending_bytes = {route: 0 for route in self.routes}
        self._inflight: dict[StorageRoute, list[BufferedDocument]] = {
            route: [] for route in self.routes
        }
        self._close_requested = asyncio.Event()
        self._failure_event = asyncio.Event()
        self._client: Any | None = None
        self._mongo_collections: dict[StorageRoute, Any] = {}
        self._workers: dict[StorageRoute, asyncio.Task[None]] = {}
        self._worker_errors: dict[StorageRoute, Exception] = {}
        self._failure_reported = False
        self._state = "new"

    @property
    def pending_count(self) -> int:
        return self._budget.documents

    @property
    def pending_bytes(self) -> int:
        return self._budget.bytes

    @property
    def pending_by_route(self) -> Mapping[StorageRoute, int]:
        return dict(self._pending_documents)

    async def open(self) -> None:
        if self._state != "new":
            raise MultiMongoStateError("MongoDB 출력은 한 번만 열 수 있습니다.")
        self._set_state("MongoDB", "연결 확인 중")
        timeout_ms = max(1, int(self.operation_timeout * 1000))
        client = self._client_factory(
            self.config.uri,
            serverSelectionTimeoutMS=timeout_ms,
            connectTimeoutMS=timeout_ms,
            socketTimeoutMS=timeout_ms,
            retryWrites=False,
        )
        self._client = client
        try:
            await asyncio.wait_for(
                client.admin.command("ping"),
                timeout=self.operation_timeout,
            )
            database = client[self.config.database]
            self._mongo_collections = {
                route: database[self.collection_name_by_route[route]]
                for route in self.routes
            }
            if self.create_indexes:
                await self._create_query_indexes()
        except BaseException:
            self._set_state("MongoDB", "연결 실패")
            with suppress(Exception):
                await asyncio.wait_for(
                    client.close(),
                    timeout=self.operation_timeout,
                )
            self._client = None
            self._mongo_collections = {}
            self._state = "closed"
            raise

        self._state = "open"
        self._set_state("MongoDB", "연결됨")
        self._workers = {
            route: asyncio.create_task(
                self._run_worker(route),
                name=f"mongodb-{route.value}",
            )
            for route in self.routes
        }

    async def _create_query_indexes(self) -> None:
        keys = [
            ("meta.market", ASCENDING),
            ("data.symbol", ASCENDING),
            ("meta.received_at_ms", ASCENDING),
        ]
        for route in DATA_STORAGE_ROUTES:
            await asyncio.wait_for(
                self._mongo_collections[route].create_index(
                    keys,
                    name="market_symbol_received_at",
                ),
                timeout=self.operation_timeout,
            )

    async def write(self, event: Event) -> None:
        if self._state != "open":
            self._raise_worker_error()
            raise MultiMongoStateError("열려 있지 않은 MongoDB 출력입니다.")
        self._raise_worker_error()
        document = copy.deepcopy(event)
        try:
            bson_size = len(BSON.encode(document))
        except Exception as error:
            raise MultiMongoOutputError("이벤트를 BSON으로 직렬화할 수 없습니다.") from error
        route = self._route(document)
        buffered = BufferedDocument(document, bson_size)
        await self._budget.acquire(bson_size)
        try:
            self._raise_worker_error()
            self._pending_documents[route] += 1
            self._pending_bytes[route] += bson_size
            self._queues[route].put_nowait(buffered)
            self._report_buffer(route)
        except BaseException:
            await self._budget.release(1, bson_size)
            raise

    def _route(self, event: Event) -> StorageRoute:
        meta = event.get("meta")
        stream_type = meta.get("stream_type") if isinstance(meta, dict) else "control"
        route = storage_route_for_stream_type(
            stream_type if isinstance(stream_type, str) else "control"
        )
        if route not in self._queues:
            raise MultiMongoOutputError(f"설정되지 않은 저장 경로입니다: {route.value}")
        return route

    async def wait_failed(self) -> None:
        if self._state == "new":
            raise MultiMongoStateError("MongoDB 출력을 먼저 열어야 합니다.")
        await self._failure_event.wait()
        self._failure_reported = True
        self._raise_worker_error()
        raise MultiMongoBatchWriteError("MongoDB 배치 작업이 종료되었습니다.")

    async def close(self) -> None:
        if self._state == "new":
            self._state = "closed"
            return
        if self._state == "closed":
            return

        self._state = "closing"
        self._set_state("MongoDB", "종료 중")
        self._close_requested.set()
        shutdown_error: Exception | None = None
        if self._workers:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._workers.values()),
                    timeout=self.shutdown_timeout,
                )
            except TimeoutError as error:
                shutdown_error = MultiMongoOutputError(
                    "MongoDB 잔여 배치를 제한 시간 안에 정리하지 못했습니다."
                )
                shutdown_error.__cause__ = error
                for task in self._workers.values():
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*self._workers.values(), return_exceptions=True)

        close_error: Exception | None = None
        if self._client is not None:
            try:
                await asyncio.wait_for(
                    self._client.close(),
                    timeout=self.operation_timeout,
                )
            except Exception as error:
                close_error = error
        self._client = None
        self._mongo_collections = {}
        self._state = "closed"

        if self._worker_errors and not self._failure_reported:
            self._set_state("MongoDB", "오류")
            self._raise_worker_error()
        if shutdown_error is not None:
            self._set_state("MongoDB", "오류")
            raise shutdown_error
        if close_error is not None:
            self._set_state("MongoDB", "오류")
            raise MultiMongoOutputError("MongoDB 연결 정리에 실패했습니다.") from close_error
        self._set_state("MongoDB", "종료됨")

    async def _run_worker(self, route: StorageRoute) -> None:
        carry: BufferedDocument | None = None
        try:
            while True:
                first = carry
                carry = None
                if first is None:
                    first = await self._first_document(route)
                if first is None:
                    return

                batch = [first]
                batch_bytes = first.bson_size
                deadline = asyncio.get_running_loop().time() + self.flush_interval
                while len(batch) < self.batch_max_documents:
                    candidate = await self._next_batch_document(
                        route,
                        deadline=deadline,
                    )
                    if candidate is None:
                        break
                    if batch_bytes + candidate.bson_size > self.batch_max_bytes:
                        carry = candidate
                        break
                    batch.append(candidate)
                    batch_bytes += candidate.bson_size
                    if batch_bytes >= self.batch_max_bytes:
                        break

                await self._persist_batch(route, batch)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._worker_errors[route] = error
            self._set_state(route, "오류")
            self._failure_event.set()
            await self._budget.mark_failed()

    async def _first_document(self, route: StorageRoute) -> BufferedDocument | None:
        queue = self._queues[route]
        if not queue.empty():
            return queue.get_nowait()
        if self._close_requested.is_set():
            return None

        get_task = asyncio.create_task(queue.get())
        close_task = asyncio.create_task(self._close_requested.wait())
        try:
            completed, _ = await asyncio.wait(
                {get_task, close_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if get_task in completed:
                return get_task.result()
            if not queue.empty():
                return queue.get_nowait()
            return None
        finally:
            for task in (get_task, close_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(get_task, close_task, return_exceptions=True)

    async def _next_batch_document(
        self,
        route: StorageRoute,
        *,
        deadline: float,
    ) -> BufferedDocument | None:
        queue = self._queues[route]
        if not queue.empty():
            return queue.get_nowait()
        if self._close_requested.is_set():
            return None
        timeout = max(0.0, deadline - asyncio.get_running_loop().time())
        if timeout == 0:
            return None
        try:
            return await asyncio.wait_for(queue.get(), timeout=timeout)
        except TimeoutError:
            return None

    async def _persist_batch(
        self,
        route: StorageRoute,
        batch: list[BufferedDocument],
    ) -> None:
        mongo_collection = self._mongo_collections.get(route)
        if mongo_collection is None:
            raise MultiMongoStateError(f"{route.value} 저장 경로가 준비되지 않았습니다.")
        self._inflight[route] = batch
        self._set_state(route, "적재 중")
        started_at = time.monotonic()
        try:
            async with self._write_semaphore:
                await asyncio.wait_for(
                    mongo_collection.insert_many(
                        [item.event for item in batch],
                        ordered=True,
                    ),
                    timeout=self.operation_timeout,
                )
        except BulkWriteError as error:
            confirmed = max(0, min(len(batch), int(error.details.get("nInserted", 0))))
            if confirmed:
                await self._confirm_batch(
                    route,
                    batch[:confirmed],
                    time.monotonic() - started_at,
                )
            self._inflight[route] = batch[confirmed:]
            raise

        duration = time.monotonic() - started_at
        await self._confirm_batch(route, batch, duration)
        self._inflight[route] = []
        self._set_state(route, "연결됨")

    async def _confirm_batch(
        self,
        route: StorageRoute,
        confirmed: list[BufferedDocument],
        duration: float,
    ) -> None:
        count = len(confirmed)
        byte_count = sum(item.bson_size for item in confirmed)
        self._pending_documents[route] -= count
        self._pending_bytes[route] -= byte_count
        for _ in confirmed:
            self._queues[route].task_done()
        await self._budget.release(count, byte_count)
        if self._on_batch_persisted is not None:
            self._on_batch_persisted(route, count, duration)
        self._report_buffer(route)

    def _raise_worker_error(self) -> None:
        if self._worker_errors:
            route, error = next(iter(self._worker_errors.items()))
            raise MultiMongoBatchWriteError(
                f"{route.value} 배치의 저장 성공 여부를 모두 확인할 수 없습니다."
            ) from error

    def _set_state(self, target: StorageRoute | str, state: str) -> None:
        if self._on_state_changed is not None:
            self._on_state_changed(target, state)

    def _report_buffer(self, route: StorageRoute) -> None:
        if self._on_buffer_changed is not None:
            self._on_buffer_changed(
                route,
                self._pending_documents[route],
                self._pending_bytes[route],
                self._budget.documents,
                self._budget.bytes,
            )
