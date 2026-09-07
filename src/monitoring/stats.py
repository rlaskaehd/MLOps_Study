"""이벤트를 변경하지 않고 수신·전달 통계를 기록한다."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

from src.config import normalize_symbols
from src.models.event import Event


@dataclass(frozen=True, slots=True)
class StatsSnapshot:
    """한 시점의 일관된 화면 표시용 통계."""

    per_symbol_rate: Mapping[str, int]
    total_rate: int
    uptime_seconds: float
    cumulative_trades: int
    control_or_unclassified: int
    forwarded_messages: int
    output_connected: bool
    connection_state: str
    subscription_state: str
    recent_error: str | None


class StatsCollector:
    """단조 증가 시계를 기준으로 1초 구간 통계를 계산한다."""

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        output_connected: bool,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.symbols = normalize_symbols(symbols)
        self._symbol_set = set(self.symbols)
        self._clock = clock
        self._started_at = clock()
        self._buckets: dict[int, dict[str, int]] = {}
        self._cumulative_trades = 0
        self._control_or_unclassified = 0
        self._forwarded_messages = 0
        self._output_connected = output_connected
        self._connection_state = "시작 중"
        self._subscription_state = "구독 대기"
        self._recent_error: str | None = None

    def _elapsed(self) -> float:
        return max(0.0, self._clock() - self._started_at)

    def _bucket_index(self) -> int:
        return int(self._elapsed())

    def _prune_buckets(self, current_index: int) -> None:
        oldest_needed = current_index - 1
        self._buckets = {
            index: counts
            for index, counts in self._buckets.items()
            if index >= oldest_needed
        }

    def record_received(self, event: Event) -> None:
        """출력 호출 전에 수신 이벤트 한 건을 계수한다."""

        symbol = event.get("symbol")
        is_selected_trade = (
            event.get("event_type") == "aggTrade"
            and isinstance(symbol, str)
            and symbol in self._symbol_set
        )
        if not is_selected_trade:
            self._control_or_unclassified += 1
            return

        index = self._bucket_index()
        bucket = self._buckets.setdefault(index, {})
        bucket[symbol] = bucket.get(symbol, 0) + 1
        self._cumulative_trades += 1
        self._prune_buckets(index)

    def record_forwarded(self) -> None:
        """후속 출력의 write가 성공한 메시지만 계수한다."""

        self._forwarded_messages += 1

    def set_output_connected(self, connected: bool) -> None:
        self._output_connected = connected

    def set_connection_state(self, state: str) -> None:
        self._connection_state = state

    def set_subscription_state(self, state: str) -> None:
        self._subscription_state = state

    def record_error(self, message: str) -> None:
        self._recent_error = message

    def record_collector_status(self, category: str, value: str) -> None:
        """수집기 상태 콜백을 화면 상태 필드에 반영한다."""

        if category == "connection":
            self.set_connection_state(value)
        elif category == "subscription":
            self.set_subscription_state(value)
        elif category == "error":
            self.record_error(value)

    def snapshot(self) -> StatsSnapshot:
        """직전 완료된 1초 구간과 누적 상태를 함께 반환한다."""

        elapsed = self._elapsed()
        current_index = int(elapsed)
        completed_index = current_index - 1
        completed = self._buckets.get(completed_index, {})
        rates = {symbol: completed.get(symbol, 0) for symbol in self.symbols}
        self._prune_buckets(current_index)
        return StatsSnapshot(
            per_symbol_rate=MappingProxyType(rates),
            total_rate=sum(rates.values()),
            uptime_seconds=elapsed,
            cumulative_trades=self._cumulative_trades,
            control_or_unclassified=self._control_or_unclassified,
            forwarded_messages=self._forwarded_messages,
            output_connected=self._output_connected,
            connection_state=self._connection_state,
            subscription_state=self._subscription_state,
            recent_error=self._recent_error,
        )
