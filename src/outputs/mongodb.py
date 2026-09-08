"""표준 이벤트를 제한 큐에 받고 1초 단위로 MongoDB에 적재한다."""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from pymongo import AsyncMongoClient

from src.config import MongoConfig
from src.models.event import Event
from src.outputs.base import EventOutput


class MongoOutputError(RuntimeError):
    """MongoDB 출력의 준비, 적재 또는 종료에 실패한 경우."""


class MongoOutputStateError(MongoOutputError):
    """MongoDB 출력 생명주기에 맞지 않는 호출인 경우."""


class MongoBatchWriteError(MongoOutputError):
    """MongoDB 배치의 저장 성공 여부를 확인할 수 없는 경우."""


class MongoBatchOutput(EventOutput):
    """이벤트를 빠르게 접수하고 별도 작업에서 순차 배치 적재한다."""

    def __init__(
        self,
        config: MongoConfig,
        *,
        flush_interval: float = 1.0,
        queue_maxsize: int = 10_000,
        batch_maxsize: int = 10_000,
        operation_timeout: float = 5.0,
        shutdown_timeout: float = 10.0,
        client_factory: Callable[..., Any] = AsyncMongoClient,
    ) -> None:
        if flush_interval <= 0:
            raise ValueError("flush_interval은 0보다 커야 합니다.")
        if queue_maxsize <= 0:
            raise ValueError("queue_maxsize는 0보다 커야 합니다.")
        if batch_maxsize <= 0:
            raise ValueError("batch_maxsize는 0보다 커야 합니다.")
        if operation_timeout <= 0:
            raise ValueError("operation_timeout은 0보다 커야 합니다.")
        if shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout은 0보다 커야 합니다.")

        self.config = config
        self.flush_interval = flush_interval
        self.batch_maxsize = min(batch_maxsize, queue_maxsize)
        self.operation_timeout = operation_timeout
        self.shutdown_timeout = shutdown_timeout
        self._client_factory = client_factory
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=queue_maxsize)
        self._close_requested = asyncio.Event()
        self._failure_event = asyncio.Event()
        self._client: Any | None = None
        self._collection: Any | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._worker_error: Exception | None = None
        self._failure_reported = False
        self._inflight_batch: list[Event] = []
        self._state = "new"

    @property
    def pending_count(self) -> int:
        """큐와 저장 결과를 기다리는 현재 배치의 이벤트 수다."""

        return self._queue.qsize() + len(self._inflight_batch)

    async def open(self) -> None:
        """연결을 확인한 뒤 한 개의 배치 작업을 시작한다."""

        if self._state != "new":
            raise MongoOutputStateError("MongoDB 출력은 한 번만 열 수 있습니다.")

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
            self._collection = client[self.config.database][self.config.collection]
        except BaseException:
            with suppress(Exception):
                await asyncio.wait_for(
                    client.close(),
                    timeout=self.operation_timeout,
                )
            self._client = None
            self._state = "closed"
            raise

        self._state = "open"
        self._worker_task = asyncio.create_task(
            self._run_worker(),
            name="mongodb-batch-output",
        )

    async def write(self, event: Event) -> None:
        """표준 이벤트의 독립된 복사본을 제한 큐에 접수한다."""

        if self._state != "open":
            self._raise_worker_error()
            raise MongoOutputStateError("열려 있지 않은 MongoDB 출력입니다.")

        self._raise_worker_error()
        document = copy.deepcopy(event)
        put_task = asyncio.create_task(self._queue.put(document))
        failure_task = asyncio.create_task(self._failure_event.wait())
        try:
            completed, _ = await asyncio.wait(
                {put_task, failure_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if failure_task in completed:
                if not put_task.done():
                    put_task.cancel()
                    await asyncio.gather(put_task, return_exceptions=True)
                self._raise_worker_error()
            await put_task
            self._raise_worker_error()
        finally:
            if not failure_task.done():
                failure_task.cancel()
            await asyncio.gather(failure_task, return_exceptions=True)

    async def wait_failed(self) -> None:
        """백그라운드 적재 실패를 실행 관리자에 전달한다."""

        if self._state == "new":
            raise MongoOutputStateError("MongoDB 출력을 먼저 열어야 합니다.")
        await self._failure_event.wait()
        self._failure_reported = True
        self._raise_worker_error()
        raise MongoBatchWriteError("MongoDB 배치 작업이 종료되었습니다.")

    async def close(self) -> None:
        """새 접수를 막고 남은 큐를 적재한 뒤 연결을 정리한다."""

        if self._state == "new":
            self._state = "closed"
            return
        if self._state == "closed":
            return

        self._state = "closing"
        self._close_requested.set()
        worker = self._worker_task
        shutdown_error: Exception | None = None
        if worker is not None:
            try:
                await asyncio.wait_for(worker, timeout=self.shutdown_timeout)
            except TimeoutError as error:
                shutdown_error = MongoOutputError(
                    "MongoDB 잔여 배치를 제한 시간 안에 정리하지 못했습니다."
                )
                shutdown_error.__cause__ = error

        close_error: Exception | None = None
        client = self._client
        if client is not None:
            try:
                await asyncio.wait_for(
                    client.close(),
                    timeout=self.operation_timeout,
                )
            except Exception as error:
                close_error = error

        self._client = None
        self._collection = None
        self._state = "closed"

        if self._worker_error is not None and not self._failure_reported:
            self._raise_worker_error()
        if shutdown_error is not None:
            raise shutdown_error
        if close_error is not None:
            raise MongoOutputError("MongoDB 연결 정리에 실패했습니다.") from close_error

    async def _run_worker(self) -> None:
        loop = asyncio.get_running_loop()
        next_flush = loop.time() + self.flush_interval
        try:
            while True:
                if not self._close_requested.is_set():
                    timeout = max(0.0, next_flush - loop.time())
                    try:
                        await asyncio.wait_for(
                            self._close_requested.wait(),
                            timeout=timeout,
                        )
                    except TimeoutError:
                        pass

                batch = self._drain_batch()
                if batch:
                    await self._persist_batch(batch)

                if self._close_requested.is_set():
                    if self._queue.empty():
                        return
                    continue

                next_flush += self.flush_interval
                if self._queue.empty() and next_flush <= loop.time():
                    next_flush = loop.time() + self.flush_interval
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._worker_error = error
            self._failure_event.set()

    def _drain_batch(self) -> list[Event]:
        batch: list[Event] = []
        while len(batch) < self.batch_maxsize:
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return batch

    async def _persist_batch(self, batch: list[Event]) -> None:
        collection = self._collection
        if collection is None:
            raise MongoOutputStateError("MongoDB 컬렉션이 준비되지 않았습니다.")

        self._inflight_batch = batch
        await asyncio.wait_for(
            collection.insert_many(batch, ordered=True),
            timeout=self.operation_timeout,
        )
        self._inflight_batch = []
        for _ in batch:
            self._queue.task_done()

    def _raise_worker_error(self) -> None:
        if self._worker_error is not None:
            raise MongoBatchWriteError(
                "MongoDB 배치의 저장 성공 여부를 확인할 수 없습니다."
            ) from self._worker_error
