"""표준화, 통계, 후속 출력을 하나의 순차 이벤트 경로로 연결한다."""

from __future__ import annotations

import asyncio
import logging

from src.collector.client import Message
from src.collector.parser import normalize_message
from src.monitoring.stats import StatsCollector
from src.outputs.base import EventOutput


class OutputLifecycleError(RuntimeError):
    """후속 출력의 준비 또는 정리에 실패한 경우."""


class OutputWriteError(RuntimeError):
    """후속 출력에 이벤트 한 건을 전달하지 못한 경우."""


class EventDispatcher:
    """모든 메시지를 계수하고 연결된 출력에 수신 순서대로 전달한다."""

    def __init__(
        self,
        *,
        output: EventOutput | None,
        stats: StatsCollector,
        logger: logging.Logger,
    ) -> None:
        self.output = output
        self.stats = stats
        self._logger = logger

    async def open(self) -> None:
        if self.output is None:
            return
        try:
            await self.output.open()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            message = f"후속 출력 준비 실패: {type(error).__name__}"
            self.stats.record_error(message)
            raise OutputLifecycleError(message) from error

    async def handle(self, message: Message) -> None:
        normalized = normalize_message(message)
        self.stats.record_received(normalized.event)
        if normalized.warning is not None:
            self._logger.warning(
                "메시지를 raw_message로 보존했습니다: %s",
                normalized.warning,
            )

        if self.output is None:
            return
        try:
            await self.output.write(normalized.event)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            message_text = f"후속 출력 전달 실패: {type(error).__name__}"
            self.stats.record_error(message_text)
            raise OutputWriteError(message_text) from error
        self.stats.record_forwarded()

    async def close(self) -> None:
        if self.output is None:
            return
        try:
            await self.output.close()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            message = f"후속 출력 정리 실패: {type(error).__name__}"
            self.stats.record_error(message)
            raise OutputLifecycleError(message) from error
