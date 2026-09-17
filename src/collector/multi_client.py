"""연결 그룹별 Binance WebSocket을 독립적으로 수집하고 감독한다."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from src.collector.client import MessageHandlerError, SubscriptionError
from src.collector.reconnect import ReconnectPolicy, wait_for_reconnect
from src.collector.streams import (
    CONNECTION_GROUP_BY_KEY,
    ConnectionGroupSpec,
    StreamSpec,
    group_stream_specs,
)
from src.collector.subscriptions import (
    SubscriptionResponseKind,
    build_stream_subscribe_request,
    parse_subscription_response,
)
from src.models.event import ReceivedMessage, WireMessage


MultiMessageHandler = Callable[[ReceivedMessage], None | Awaitable[None]]
MultiStatusHandler = Callable[[str, str, str], None]
ConnectionIdFactory = Callable[[str, int], str]


class StreamConnection:
    """하나의 연결 그룹을 구독하고 자체 재연결 상태를 관리한다."""

    def __init__(
        self,
        group: ConnectionGroupSpec,
        specs: tuple[StreamSpec, ...],
        *,
        subscription_timeout: float = 10.0,
        reconnect_policy: ReconnectPolicy | None = None,
        connection_factory: Callable[..., Any] = connect,
        connection_id_factory: ConnectionIdFactory | None = None,
        received_clock_ms: Callable[[], int] | None = None,
        logger: logging.Logger | None = None,
        status_handler: MultiStatusHandler | None = None,
    ) -> None:
        if not specs:
            raise ValueError(f"{group.key} 연결에는 구독 명세가 필요합니다.")
        if any(spec.connection_group != group.key for spec in specs):
            raise ValueError(f"{group.key}와 다른 연결 그룹의 명세가 포함되었습니다.")
        self.group = group
        self.specs = specs
        self.subscription_timeout = subscription_timeout
        self.reconnect_policy = reconnect_policy or ReconnectPolicy()
        self._connection_factory = connection_factory
        run_id = uuid.uuid4().hex
        self._connection_id_factory = connection_id_factory or (
            lambda group_key, attempt: f"{run_id}-{group_key}-{attempt}"
        )
        self._received_clock_ms = received_clock_ms or (
            lambda: time.time_ns() // 1_000_000
        )
        self._logger = logger or logging.getLogger(__name__)
        self._status_handler = status_handler
        self._stream_names = tuple(spec.stream_name for spec in specs)
        self._stream_name_set = set(self._stream_names)
        self._attempt = 0

    def _report_status(self, category: str, value: str) -> None:
        if self._status_handler is not None:
            self._status_handler(self.group.key, category, value)

    async def run(
        self,
        handler: MultiMessageHandler,
        stop_event: asyncio.Event,
    ) -> None:
        self.reconnect_policy.reset()
        try:
            while not stop_event.is_set():
                self._report_status("connection", "연결 중")
                self._report_status("subscription", "구독 대기")
                try:
                    await self._collect_once(handler, stop_event)
                except asyncio.CancelledError:
                    raise
                except MessageHandlerError:
                    raise
                except SubscriptionError as error:
                    self._report_status("subscription", "구독 실패")
                    self._report_status("error", str(error))
                    self._logger.warning(
                        "%s 구독 실패: %s",
                        self.group.key,
                        error,
                    )
                except (ConnectionClosed, OSError, TimeoutError) as error:
                    error_name = type(error).__name__
                    self._report_status("connection", "연결 끊김")
                    self._report_status("error", error_name)
                    self._logger.warning(
                        "%s WebSocket 연결 종료: %s",
                        self.group.key,
                        error_name,
                    )

                if stop_event.is_set():
                    return
                delay = self.reconnect_policy.next_delay()
                self._report_status("connection", f"재연결 대기 {delay:.0f}초")
                if await wait_for_reconnect(delay, stop_event):
                    return
        finally:
            self._report_status("connection", "종료됨")

    async def _collect_once(
        self,
        handler: MultiMessageHandler,
        stop_event: asyncio.Event,
    ) -> None:
        self._attempt += 1
        connection_id = self._connection_id_factory(self.group.key, self._attempt)
        async with self._connection_factory(
            self.group.url,
            open_timeout=10,
            close_timeout=10,
            ping_interval=20,
            ping_timeout=20,
            max_queue=16,
        ) as websocket:
            self._report_status("connection", "연결됨")
            self._report_status("subscription", "구독 요청 중")
            await websocket.send(build_stream_subscribe_request(self._stream_names))
            deadline = asyncio.get_running_loop().time() + self.subscription_timeout
            subscription_confirmed = False
            receive_sequence = 0

            while not stop_event.is_set():
                timeout = None
                if not subscription_confirmed:
                    timeout = max(0.0, deadline - asyncio.get_running_loop().time())
                payload = await self._receive_or_stop(
                    websocket,
                    stop_event,
                    timeout=timeout,
                )
                if payload is None:
                    return

                receive_sequence += 1
                received = ReceivedMessage(
                    payload=payload,
                    connection_group=self.group.key,
                    connection_id=connection_id,
                    receive_sequence=receive_sequence,
                    received_at_ms=self._received_clock_ms(),
                )
                await self._handle_message(handler, received)
                self.reconnect_policy.reset()

                response = parse_subscription_response(payload)
                if response is not None:
                    if response.kind is SubscriptionResponseKind.ERROR:
                        raise SubscriptionError(response.detail or "unknown")
                    subscription_confirmed = True
                    self._report_status(
                        "subscription",
                        f"{len(self.specs)}개 구독 확인",
                    )

                if self._is_known_stream(payload):
                    self._report_status("data", "수신 중")
                if self._is_server_shutdown(payload):
                    self._logger.warning(
                        "%s serverShutdown 메시지를 수신했습니다.",
                        self.group.key,
                    )
                    return

    @staticmethod
    async def _handle_message(
        handler: MultiMessageHandler,
        message: ReceivedMessage,
    ) -> None:
        try:
            result = handler(message)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise MessageHandlerError(
                "수신 메시지의 표준화 또는 출력에 실패했습니다."
            ) from error

    @staticmethod
    async def _receive_or_stop(
        websocket: Any,
        stop_event: asyncio.Event,
        *,
        timeout: float | None,
    ) -> WireMessage | None:
        receive_task = asyncio.create_task(websocket.recv())
        stop_task = asyncio.create_task(stop_event.wait())
        try:
            completed, _ = await asyncio.wait(
                {receive_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
                timeout=timeout,
            )
            if not completed:
                receive_task.cancel()
                await asyncio.gather(receive_task, return_exceptions=True)
                raise SubscriptionError("구독 확인 응답 시간이 초과되었습니다.")
            if receive_task in completed:
                return receive_task.result()
            receive_task.cancel()
            await asyncio.gather(receive_task, return_exceptions=True)
            return None
        finally:
            if not stop_task.done():
                stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)

    @staticmethod
    def _decode_object(payload: WireMessage) -> dict[str, Any] | None:
        if not isinstance(payload, str):
            return None
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            return None
        return decoded if isinstance(decoded, dict) else None

    def _is_known_stream(self, payload: WireMessage) -> bool:
        decoded = self._decode_object(payload)
        return (
            decoded is not None
            and isinstance(decoded.get("stream"), str)
            and decoded["stream"] in self._stream_name_set
            and isinstance(decoded.get("data"), dict)
        )

    @classmethod
    def _is_server_shutdown(cls, payload: WireMessage) -> bool:
        decoded = cls._decode_object(payload)
        if decoded is None:
            return False
        data = decoded.get("data", decoded)
        return isinstance(data, dict) and data.get("e") == "serverShutdown"


class MultiStreamCollector:
    """세 연결 그룹을 함께 실행하고 치명적 실패 시 전체를 정리한다."""

    def __init__(
        self,
        specs: Iterable[StreamSpec],
        *,
        connection_factory: Callable[..., Any] = connect,
        logger: logging.Logger | None = None,
        status_handler: MultiStatusHandler | None = None,
    ) -> None:
        grouped = group_stream_specs(specs)
        active_logger = logger or logging.getLogger(__name__)
        self.connections = tuple(
            StreamConnection(
                CONNECTION_GROUP_BY_KEY[group_key],
                grouped[group_key],
                connection_factory=connection_factory,
                logger=active_logger,
                status_handler=status_handler,
            )
            for group_key in CONNECTION_GROUP_BY_KEY
        )

    async def run(
        self,
        handler: MultiMessageHandler,
        stop_event: asyncio.Event,
    ) -> None:
        tasks = {
            asyncio.create_task(
                connection.run(handler, stop_event),
                name=f"binance-{connection.group.key}",
            )
            for connection in self.connections
        }
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            failure: BaseException | None = None
            for task in done:
                try:
                    await task
                except BaseException as error:
                    failure = error
                    break
            if failure is not None:
                stop_event.set()
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise failure

            if not stop_event.is_set():
                stop_event.set()
                raise RuntimeError("다중 스트림 연결이 예기치 않게 종료되었습니다.")
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
