"""다중 스트림의 수신, 연결, 버퍼와 적재 상태를 유한하게 집계한다."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

from src.collector.streams import (
    CONNECTION_GROUP_BY_KEY,
    StreamType,
)
from src.config import normalize_symbols
from src.models.event import Event
from src.monitoring.depth import DepthContinuityObserver, DepthGap
from src.storage_routes import (
    STORAGE_ROUTES,
    StorageRoute,
    storage_route_for_stream_type,
)


STREAM_ORDER = tuple(stream_type.value for stream_type in StreamType)


@dataclass(frozen=True, slots=True)
class ConnectionStatsSnapshot:
    connection: str
    subscription: str
    data: str
    connection_changes: int
    last_data_age_seconds: float | None


@dataclass(frozen=True, slots=True)
class CollectionStatsSnapshot:
    accepted: int
    persisted: int
    pending: int
    pending_bytes: int
    state: str
    last_batch_size: int
    last_batch_duration_ms: float | None


@dataclass(frozen=True, slots=True)
class MultiStreamStatsSnapshot:
    per_symbol_stream_rate: Mapping[str, Mapping[str, int]]
    total_rate: int
    cumulative_by_stream: Mapping[str, int]
    control_or_unclassified: int
    forwarded_messages: int
    uptime_seconds: float
    connections: Mapping[str, ConnectionStatsSnapshot]
    collections: Mapping[StorageRoute, CollectionStatsSnapshot]
    latest_latency_ms: Mapping[str, int]
    depth_gap_count: int
    depth_connection_resets: int
    last_depth_gap: DepthGap | None
    recent_error: str | None


class MultiStreamStatsCollector:
    """이벤트와 저장 문서를 변경하지 않고 화면용 집계만 유지한다."""

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        clock: Callable[[], float] = time.monotonic,
        depth_observer: DepthContinuityObserver | None = None,
    ) -> None:
        self.symbols = normalize_symbols(symbols)
        self._symbol_set = set(self.symbols)
        self._clock = clock
        self._started_at = clock()
        self._buckets: dict[int, dict[tuple[str, str], int]] = {}
        self._cumulative_by_stream = {stream: 0 for stream in STREAM_ORDER}
        self._control_or_unclassified = 0
        self._forwarded_messages = 0
        self._connections = {
            group: {
                "connection": "시작 중",
                "subscription": "구독 대기",
                "data": "수신 대기",
                "last_data_at": None,
            }
            for group in CONNECTION_GROUP_BY_KEY
        }
        self._last_connection_ids: dict[str, str] = {}
        self._connection_changes = {group: 0 for group in CONNECTION_GROUP_BY_KEY}
        self._collections = {
            route: {
                "accepted": 0,
                "persisted": 0,
                "pending": 0,
                "pending_bytes": 0,
                "state": "연결 대기",
                "last_batch_size": 0,
                "last_batch_duration_ms": None,
            }
            for route in STORAGE_ROUTES
        }
        self._latest_latency_ms: dict[str, int] = {}
        self._depth = depth_observer or DepthContinuityObserver()
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
        meta = event.get("meta")
        data = event.get("data")
        if not isinstance(meta, dict) or not isinstance(data, dict):
            self._control_or_unclassified += 1
            return
        stream_type = meta.get("stream_type")
        symbol = data.get("symbol")
        if (
            not isinstance(stream_type, str)
            or stream_type not in self._cumulative_by_stream
            or not isinstance(symbol, str)
            or symbol not in self._symbol_set
        ):
            self._control_or_unclassified += 1
            return

        index = self._bucket_index()
        bucket = self._buckets.setdefault(index, {})
        key = (symbol, stream_type)
        bucket[key] = bucket.get(key, 0) + 1
        self._cumulative_by_stream[stream_type] += 1
        self._prune_buckets(index)

        group = meta.get("connection_group")
        connection_id = meta.get("connection_id")
        if isinstance(group, str) and isinstance(connection_id, str):
            previous = self._last_connection_ids.get(group)
            if previous is not None and previous != connection_id:
                self._connection_changes[group] += 1
            self._last_connection_ids[group] = connection_id

        event_time = data.get("event_time")
        received_at = meta.get("received_at_ms")
        if isinstance(event_time, int) and isinstance(received_at, int):
            self._latest_latency_ms[stream_type] = received_at - event_time
        self._depth.observe(event)

    def record_forwarded(self, event: Event) -> None:
        meta = event.get("meta")
        stream_type = meta.get("stream_type") if isinstance(meta, dict) else "control"
        route = storage_route_for_stream_type(
            stream_type if isinstance(stream_type, str) else "control"
        )
        self._forwarded_messages += 1
        self._collections[route]["accepted"] += 1

    def record_batch_persisted(
        self,
        route: StorageRoute,
        count: int,
        duration_seconds: float,
    ) -> None:
        target = self._collections[route]
        target["persisted"] += count
        target["last_batch_size"] = count
        target["last_batch_duration_ms"] = duration_seconds * 1000

    def record_buffer_changed(
        self,
        route: StorageRoute,
        pending: int,
        pending_bytes: int,
        total_pending: int,
        total_bytes: int,
    ) -> None:
        del total_pending, total_bytes
        target = self._collections[route]
        target["pending"] = pending
        target["pending_bytes"] = pending_bytes

    def record_storage_state(self, target: StorageRoute | str, state: str) -> None:
        if target == "MongoDB":
            for collection in self._collections.values():
                collection["state"] = state
        elif target in self._collections:
            self._collections[target]["state"] = state

    def record_connection_status(self, group: str, category: str, value: str) -> None:
        state = self._connections.get(group)
        if state is None:
            return
        if category in {"connection", "subscription", "data"}:
            state[category] = value
            if category == "data":
                state["last_data_at"] = self._clock()
        elif category == "error":
            self._recent_error = f"{group}: {value}"

    def record_error(self, message: str) -> None:
        self._recent_error = message

    def snapshot(self) -> MultiStreamStatsSnapshot:
        elapsed = self._elapsed()
        current_index = int(elapsed)
        completed = self._buckets.get(current_index - 1, {})
        rates = {
            symbol: MappingProxyType(
                {
                    stream: completed.get((symbol, stream), 0)
                    for stream in STREAM_ORDER
                }
            )
            for symbol in self.symbols
        }
        self._prune_buckets(current_index)

        connections: dict[str, ConnectionStatsSnapshot] = {}
        now = self._clock()
        for group, state in self._connections.items():
            last_data_at = state["last_data_at"]
            age = None
            if isinstance(last_data_at, (float, int)):
                age = max(0.0, now - last_data_at)
            connections[group] = ConnectionStatsSnapshot(
                connection=str(state["connection"]),
                subscription=str(state["subscription"]),
                data=str(state["data"]),
                connection_changes=self._connection_changes[group],
                last_data_age_seconds=age,
            )

        collection_snapshots: dict[StorageRoute, CollectionStatsSnapshot] = {}
        for route, state in self._collections.items():
            duration = state["last_batch_duration_ms"]
            collection_snapshots[route] = CollectionStatsSnapshot(
                accepted=int(state["accepted"]),
                persisted=int(state["persisted"]),
                pending=int(state["pending"]),
                pending_bytes=int(state["pending_bytes"]),
                state=str(state["state"]),
                last_batch_size=int(state["last_batch_size"]),
                last_batch_duration_ms=(
                    float(duration) if isinstance(duration, (float, int)) else None
                ),
            )

        return MultiStreamStatsSnapshot(
            per_symbol_stream_rate=MappingProxyType(rates),
            total_rate=sum(sum(row.values()) for row in rates.values()),
            cumulative_by_stream=MappingProxyType(dict(self._cumulative_by_stream)),
            control_or_unclassified=self._control_or_unclassified,
            forwarded_messages=self._forwarded_messages,
            uptime_seconds=elapsed,
            connections=MappingProxyType(connections),
            collections=MappingProxyType(collection_snapshots),
            latest_latency_ms=MappingProxyType(dict(self._latest_latency_ms)),
            depth_gap_count=self._depth.gap_count,
            depth_connection_resets=self._depth.connection_resets,
            last_depth_gap=self._depth.last_gap,
            recent_error=self._recent_error,
        )
