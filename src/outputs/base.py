"""후속 적재·소비 계층이 구현할 공통 출력 계약을 정의한다."""
from typing import Protocol, runtime_checkable

from src.models.event import Event


class EventOutput(Protocol):
    """표준 이벤트를 순서대로 전달받는 비동기 출력 객체."""

    async def open(self) -> None:
        """수집을 시작하기 전에 출력 자원을 준비한다."""

    async def write(self, event: Event) -> None:
        """표준 이벤트 한 건을 처리한다."""

    async def close(self) -> None:
        """출력 객체가 가진 자원을 정리한다."""


@runtime_checkable
class FailureAwareEventOutput(Protocol):
    """백그라운드 작업의 실패를 실행 관리자에 알리는 출력 계약."""

    async def wait_failed(self) -> None:
        """실패 전까지 기다리고 실패 원인을 예외로 전달한다."""
