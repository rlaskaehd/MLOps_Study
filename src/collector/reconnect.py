"""연결 실패가 반복될 때 사용할 재연결 대기 정책을 정의한다."""

import asyncio
from dataclasses import dataclass, field


DEFAULT_DELAYS = (1.0, 2.0, 4.0, 8.0, 16.0, 30.0)


@dataclass(slots=True)
class ReconnectPolicy:
    """정해진 순서로 대기 시간을 늘리고 마지막 값을 상한으로 사용한다."""

    delays: tuple[float, ...] = DEFAULT_DELAYS
    _attempt: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if not self.delays:
            raise ValueError("delays must contain at least one value")
        if any(delay < 0 for delay in self.delays):
            raise ValueError("reconnect delays must not be negative")

    def next_delay(self) -> float:
        index = min(self._attempt, len(self.delays) - 1)
        delay = self.delays[index]
        self._attempt += 1
        return delay

    def reset(self) -> None:
        self._attempt = 0


async def wait_for_reconnect(delay: float, stop_event: asyncio.Event) -> bool:
    """대기 중 종료 요청을 받으면 True를 반환한다."""

    if stop_event.is_set():
        return True

    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay)
    except TimeoutError:
        return False
    return True
