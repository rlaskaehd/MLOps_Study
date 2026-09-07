"""표준화된 메시지의 내부 표현을 정의한다."""

from dataclasses import dataclass
from typing import Any, TypeAlias


Event: TypeAlias = dict[str, Any]


@dataclass(frozen=True, slots=True)
class NormalizedMessage:
    """출력할 이벤트와 원문 보존 사유를 함께 전달한다."""

    event: Event
    warning: str | None = None
