"""Binance WebSocket 연결, 메시지 수신, 재연결을 담당한다."""

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from src.collector.parser import normalize_message
from src.collector.reconnect import ReconnectPolicy, wait_for_reconnect
from src.collector.subscriptions import (
    SubscriptionResponseKind,
    build_subscribe_request,
    parse_subscription_response,
)
from src.config import DEFAULT_SYMBOLS, normalize_symbols


BINANCE_WEBSOCKET_URL = "wss://stream.binance.com:9443/ws"

Message = str | bytes
MessageHandler = Callable[[Message], None | Awaitable[None]]
StatusHandler = Callable[[str, str], None]


class MessageHandlerError(RuntimeError):
    """수신 이후의 표준화 또는 출력 처리가 실패한 경우."""


class SubscriptionError(RuntimeError):
    """구독 확인에 실패해 현재 연결을 다시 만들어야 하는 경우."""


class BinanceCollector:
    """한 연결에서 여러 aggTrade 스트림을 순서대로 수신한다."""

    def __init__(
        self,
        url: str = BINANCE_WEBSOCKET_URL,
        *,
        symbols: tuple[str, ...] = DEFAULT_SYMBOLS,
        subscription_timeout: float = 10.0,
        reconnect_policy: ReconnectPolicy | None = None,
        connection_factory: Callable[..., Any] = connect,
        logger: logging.Logger | None = None,
        status_handler: StatusHandler | None = None,
    ) -> None:
        self.url = url
        self.symbols = normalize_symbols(symbols)
        self.subscription_timeout = subscription_timeout
        self.reconnect_policy = reconnect_policy or ReconnectPolicy()
        self._connection_factory = connection_factory
        self._logger = logger or logging.getLogger(__name__)
        self._status_handler = status_handler

    def _report_status(self, category: str, value: str) -> None:
        if self._status_handler is not None:
            self._status_handler(category, value)

    async def run(
        self,
        handler: MessageHandler,
        stop_event: asyncio.Event,
    ) -> None:
        """종료 요청 전까지 연결, 수신, 재연결을 반복한다."""

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
                    message = str(error)
                    self._report_status("subscription", "구독 실패")
                    self._report_status("error", message)
                    self._logger.warning(
                        "Binance 스트림 구독에 실패했습니다: %s", error
                    )
                except (ConnectionClosed, OSError, TimeoutError) as error:
                    error_name = type(error).__name__
                    self._report_status("connection", "연결 끊김")
                    self._report_status("error", error_name)
                    self._logger.warning(
                        "Binance WebSocket 연결이 종료되었습니다: %s",
                        error_name,
                    )

                if stop_event.is_set():
                    return

                delay = self.reconnect_policy.next_delay()
                self._report_status("connection", f"재연결 대기 {delay:.0f}초")
                self._logger.info("%.0f초 후 Binance에 재연결합니다.", delay)
                if await wait_for_reconnect(delay, stop_event):
                    return
        finally:
            self._report_status("connection", "종료됨")

    async def _collect_once(
        self,
        handler: MessageHandler,
        stop_event: asyncio.Event,
    ) -> None:
        async with self._connection_factory(
            self.url,
            open_timeout=10,
            close_timeout=10,
            ping_interval=20,
            ping_timeout=20,
            max_queue=16,
        ) as websocket:
            self._logger.info("Binance WebSocket에 연결되었습니다.")
            self._report_status("connection", "연결됨")
            self._report_status("subscription", "구독 요청 중")
            await websocket.send(build_subscribe_request(self.symbols))
            subscription_deadline = (
                asyncio.get_running_loop().time() + self.subscription_timeout
            )
            subscription_confirmed = False

            while not stop_event.is_set():
                timeout = None
                if not subscription_confirmed:
                    timeout = max(
                        0.0,
                        subscription_deadline - asyncio.get_running_loop().time(),
                    )
                message = await self._receive_or_stop(
                    websocket,
                    stop_event,
                    timeout=timeout,
                )
                if message is None:
                    return

                handled = await self._handle_message_or_stop(
                    handler,
                    message,
                    stop_event,
                )
                if not handled:
                    return
                self.reconnect_policy.reset()
                response = parse_subscription_response(message)
                if response is not None:
                    if response.kind is SubscriptionResponseKind.ERROR:
                        raise SubscriptionError(response.detail or "unknown")
                    subscription_confirmed = True
                    self._report_status(
                        "subscription",
                        f"{len(self.symbols)}개 구독 확인",
                    )
                    self._logger.info(
                        "%d개 aggTrade 스트림 구독이 확인되었습니다.",
                        len(self.symbols),
                    )

                if self._is_server_shutdown(message):
                    self._logger.warning(
                        "Binance serverShutdown 메시지를 수신했습니다."
                    )
                    return

    @staticmethod
    async def _receive_or_stop(
        websocket: Any,
        stop_event: asyncio.Event,
        *,
        timeout: float | None = None,
    ) -> Message | None:
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

            # 동시에 완료되었다면 이미 수신된 메시지를 먼저 보존한다.
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
    async def _handle_message(
        handler: MessageHandler,
        message: Message,
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

    @classmethod
    async def _handle_message_or_stop(
        cls,
        handler: MessageHandler,
        message: Message,
        stop_event: asyncio.Event,
    ) -> bool:
        """전달 완료와 종료 요청을 함께 기다리고 미완료 전달을 취소한다."""

        handler_task = asyncio.create_task(cls._handle_message(handler, message))
        stop_task = asyncio.create_task(stop_event.wait())
        try:
            completed, _ = await asyncio.wait(
                {handler_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if handler_task in completed:
                await handler_task
                return True

            handler_task.cancel()
            await asyncio.gather(handler_task, return_exceptions=True)
            return False
        finally:
            for task in (handler_task, stop_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                handler_task,
                stop_task,
                return_exceptions=True,
            )

    @staticmethod
    def _is_server_shutdown(message: Message) -> bool:
        if not isinstance(message, str):
            return False
        normalized = normalize_message(message)
        return (
            normalized.warning is None
            and normalized.event.get("event_type") == "serverShutdown"
        )
