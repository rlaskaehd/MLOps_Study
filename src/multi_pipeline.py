"""다중 스트림의 표준화, 통계와 후속 출력을 한 경로로 연결한다."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping

from src.collector.multi_parser import normalize_received_message
from src.collector.streams import StreamSpec
from src.models.event import ReceivedMessage
from src.monitoring.multi_stats import MultiStreamStatsCollector
from src.outputs.base import EventOutput, FailureAwareEventOutput
from src.pipeline import OutputLifecycleError, OutputWriteError


class MultiStreamEventDispatcher:
    """출처가 있는 메시지를 v2 이벤트로 만들어 순차 전달한다."""

    def __init__(
        self,
        *,
        stream_index: Mapping[tuple[str, str], StreamSpec],
        output: EventOutput,
        stats: MultiStreamStatsCollector,
        logger: logging.Logger,
    ) -> None:
        self.stream_index = stream_index
        self.output = output
        self.stats = stats
        self._logger = logger

    async def open(self) -> None:
        try:
            await self.output.open()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            message = f"후속 출력 준비 실패: {type(error).__name__}"
            self.stats.record_error(message)
            raise OutputLifecycleError(message) from error

    async def handle(self, message: ReceivedMessage) -> None:
        normalized = normalize_received_message(message, self.stream_index)
        self.stats.record_received(normalized.event)
        if normalized.warning is not None:
            self._logger.warning(
                "메시지를 제어 컬렉션에 보존했습니다: %s",
                normalized.warning,
            )
        try:
            await self.output.write(normalized.event)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            message_text = f"후속 출력 전달 실패: {type(error).__name__}"
            self.stats.record_error(message_text)
            raise OutputWriteError(message_text) from error
        self.stats.record_forwarded(normalized.event)

    async def close(self) -> None:
        try:
            await self.output.close()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            message = f"후속 출력 정리 실패: {type(error).__name__}"
            self.stats.record_error(message)
            raise OutputLifecycleError(message) from error

    def supports_failure_monitor(self) -> bool:
        return isinstance(self.output, FailureAwareEventOutput)

    async def wait_for_output_failure(self) -> None:
        if not isinstance(self.output, FailureAwareEventOutput):
            raise RuntimeError("실패 감시를 지원하지 않는 출력입니다.")
        try:
            await self.output.wait_failed()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            message = f"후속 출력 비동기 작업 실패: {type(error).__name__}"
            self.stats.record_error(message)
            raise OutputWriteError(message) from error
        raise OutputWriteError("후속 출력 작업이 예기치 않게 종료되었습니다.")
