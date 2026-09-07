"""후속 적재·소비 계층이 구현할 공통 출력 계약을 정의한다."""

from typing import Protocol

from src.models.event import Event


class EventOutput(Protocol):
    """표준 이벤트를 순서대로 전달받는 비동기 출력 객체."""

    async def open(self) -> None:
        """수집을 시작하기 전에 출력 자원을 준비한다."""

    async def write(self, event: Event) -> None:
        """표준 이벤트 한 건을 처리한다."""

    async def close(self) -> None:
        """출력 객체가 가진 자원을 정리한다."""
