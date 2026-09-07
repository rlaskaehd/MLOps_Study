"""표준화된 이벤트를 stdout에 JSON 한 줄로 출력한다."""

import json
import sys
from typing import TextIO

from src.models.event import Event


class StdoutOutput:
    """데이터 이벤트만 한 줄씩 쓰고 즉시 flush한다."""

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout

    def write(self, event: Event) -> None:
        serialized = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        self._stream.write(f"{serialized}\n")
        self._stream.flush()
