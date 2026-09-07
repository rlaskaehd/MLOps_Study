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


BINANCE_AGG_TRADE_URL = (
    "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
)

Message = str | bytes
MessageHandler = Callable[[Message], None | Awaitable[None]]


class MessageHandlerError(RuntimeError):
    """수신 이후의 표준화 또는 출력 처리가 실패한 경우."""


class BinanceCollector:
    """단일 aggTrade 스트림을 순서대로 수신한다."""

    def __init__(
        self,
        url: str = BINANCE_AGG_TRADE_URL,
        *,
        reconnect_policy: ReconnectPolicy | None = None,
        connection_factory: Callable[..., Any] = connect,
        logger: logging.Logger | None = None,
    ) -> None:
        self.url = url
        self.reconnect_policy = reconnect_policy or ReconnectPolicy()
        self._connection_factory = connection_factory
        self._logger = logger or logging.getLogger(__name__)

    async def run(
        self,
        handler: MessageHandler,
        stop_event: asyncio.Event,
    ) -> None:
        """종료 요청 전까지 연결, 수신, 재연결을 반복한다."""

        self.reconnect_policy.reset()

        while not stop_event.is_set():
            try:
                await self._collect_once(handler, stop_event)
            except asyncio.CancelledError:
                raise
            except MessageHandlerError:
                raise
            except (ConnectionClosed, OSError, TimeoutError) as error:
                self._logger.warning(
                    "Binance WebSocket 연결이 종료되었습니다: %s",
                    type(error).__name__,
                )

            if stop_event.is_set():
                return

            delay = self.reconnect_policy.next_delay()
            self._logger.info("%.0f초 후 Binance에 재연결합니다.", delay)
            if await wait_for_reconnect(delay, stop_event):
                return

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

            while not stop_event.is_set():
                message = await self._receive_or_stop(websocket, stop_event)
                if message is None:
                    return

                await self._handle_message(handler, message)
                self.reconnect_policy.reset()

                if self._is_server_shutdown(message):
                    self._logger.warning(
                        "Binance serverShutdown 메시지를 수신했습니다."
                    )
                    return

    @staticmethod
    async def _receive_or_stop(
        websocket: Any,
        stop_event: asyncio.Event,
    ) -> Message | None:
        receive_task = asyncio.create_task(websocket.recv())
        stop_task = asyncio.create_task(stop_event.wait())

        try:
            completed, _ = await asyncio.wait(
                {receive_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

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

    @staticmethod
    def _is_server_shutdown(message: Message) -> bool:
        if not isinstance(message, str):
            return False
        normalized = normalize_message(message)
        return (
            normalized.warning is None
            and normalized.event.get("event_type") == "serverShutdown"
        )
