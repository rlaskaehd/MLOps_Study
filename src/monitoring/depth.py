"""depth update ID 범위의 관측상 연속성을 이벤트 변경 없이 기록한다."""

from __future__ import annotations

from dataclasses import dataclass

from src.models.event import Event


@dataclass(frozen=True, slots=True)
class DepthGap:
    symbol: str
    connection_id: str
    previous_final_update_id: int
    next_first_update_id: int


@dataclass(slots=True)
class _DepthState:
    connection_id: str
    maximum_final_update_id: int


class DepthContinuityObserver:
    """연결 안에서 관측한 최대 update ID와 다음 범위 사이의 공백을 센다."""

    def __init__(self) -> None:
        self._states: dict[str, _DepthState] = {}
        self.gap_count = 0
        self.connection_resets = 0
        self.last_gap: DepthGap | None = None

    def observe(self, event: Event) -> None:
        meta = event.get("meta")
        data = event.get("data")
        if not isinstance(meta, dict) or not isinstance(data, dict):
            return
        if meta.get("stream_type") != "depth":
            return
        symbol = data.get("symbol")
        connection_id = meta.get("connection_id")
        first_update_id = data.get("first_update_id")
        final_update_id = data.get("final_update_id")
        if not (
            isinstance(symbol, str)
            and isinstance(connection_id, str)
            and isinstance(first_update_id, int)
            and isinstance(final_update_id, int)
        ):
            return

        state = self._states.get(symbol)
        if state is None:
            self._states[symbol] = _DepthState(connection_id, final_update_id)
            return
        if state.connection_id != connection_id:
            self.connection_resets += 1
            self._states[symbol] = _DepthState(connection_id, final_update_id)
            return

        if first_update_id > state.maximum_final_update_id + 1:
            self.gap_count += 1
            self.last_gap = DepthGap(
                symbol=symbol,
                connection_id=connection_id,
                previous_final_update_id=state.maximum_final_update_id,
                next_first_update_id=first_update_id,
            )
        state.maximum_final_update_id = max(
            state.maximum_final_update_id,
            final_update_id,
        )
