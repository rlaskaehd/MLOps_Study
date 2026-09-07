"""표준화된 이벤트를 stdout에 JSON 한 줄로 출력한다."""

import asyncio
import json
import sys
from typing import TextIO

from src.models.event import Event


class StdoutOutput:
    """데이터 이벤트만 한 줄씩 쓰고 즉시 flush한다."""

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout

    async def open(self) -> None:
        """stdout 출력은 별도 연결 준비가 필요하지 않다."""

    async def write(self, event: Event) -> None:
        """직렬화와 동기 쓰기를 작업 스레드에서 순서대로 수행한다."""

        await asyncio.to_thread(self._write_sync, event)

    def _write_sync(self, event: Event) -> None:
        serialized = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        self._stream.write(f"{serialized}\n")
        self._stream.flush()

    async def close(self) -> None:
        """매 이벤트를 flush하므로 종료 시 추가 동작이 필요하지 않다."""
