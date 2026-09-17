"""표준화된 메시지와 수신 출처의 내부 표현을 정의한다."""

from dataclasses import dataclass
from typing import Any, TypeAlias


Event: TypeAlias = dict[str, Any]


@dataclass(frozen=True, slots=True)
class NormalizedMessage:
    """출력할 이벤트와 원문 보존 사유를 함께 전달한다."""

    event: Event
    warning: str | None = None


WireMessage: TypeAlias = str | bytes


@dataclass(frozen=True, slots=True)
class ReceivedMessage:
    """한 WebSocket 연결에서 받은 메시지와 수집 시점의 출처 정보."""

    payload: WireMessage
    connection_group: str
    connection_id: str
    receive_sequence: int
    received_at_ms: int
