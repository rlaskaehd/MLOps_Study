"""다중 스트림 수신량과 연결·컬렉션 상태를 터미널에 표시한다."""

from __future__ import annotations

import asyncio
from typing import TextIO

from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.monitoring.multi_stats import (
    STREAM_ORDER,
    MultiStreamStatsCollector,
    MultiStreamStatsSnapshot,
)


def _format_uptime(seconds: float) -> str:
    total = max(0, int(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, seconds_value = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds_value:02d}"


def _format_bytes(value: int) -> str:
    if value < 1024:
        return f"{value} B"
    if value < 1024 * 1024:
        return f"{value / 1024:,.1f} KiB"
    return f"{value / (1024 * 1024):,.1f} MiB"


class MultiStreamTuiRenderer:
    """v2 통계 스냅샷만 읽어 전체 화면을 갱신한다."""

    def __init__(
        self,
        stats: MultiStreamStatsCollector,
        *,
        console: Console | None = None,
        stream: TextIO | None = None,
        refresh_interval: float = 1.0,
        screen: bool = True,
    ) -> None:
        self._stats = stats
        if console is not None:
            self._console = console
        elif stream is not None:
            self._console = Console(file=stream)
        else:
            self._console = Console(stderr=True)
        self._refresh_interval = refresh_interval
        self._screen = screen

    @staticmethod
    def render(snapshot: MultiStreamStatsSnapshot) -> RenderableType:
        rates = Table(expand=True, box=None, pad_edge=False)
        rates.add_column("symbol", no_wrap=True, width=12)
        for stream in STREAM_ORDER:
            rates.add_column(stream, justify="right")
        rates.add_column("total", justify="right")
        for symbol, row in snapshot.per_symbol_stream_rate.items():
            row_total = sum(row.values())
            rates.add_row(
                symbol,
                *(f"{row[stream]:,}" for stream in STREAM_ORDER),
                f"{row_total:,}",
            )

        summary = Table.grid(expand=True)
        summary.add_column(justify="left")
        summary.add_column(justify="center")
        summary.add_column(justify="right")
        summary.add_row(
            f"전체: {snapshot.total_rate:,} events/sec",
            f"동작 시간: {_format_uptime(snapshot.uptime_seconds)}",
            f"제어·미분류: {snapshot.control_or_unclassified:,}",
        )
        cumulative = " | ".join(
            f"{stream} {snapshot.cumulative_by_stream[stream]:,}"
            for stream in STREAM_ORDER
        )
        summary.add_row(
            f"누적: {cumulative}",
            f"큐 접수: {snapshot.forwarded_messages:,}",
            (
                f"depth 공백 의심: {snapshot.depth_gap_count:,} | "
                f"연결 기준 재설정: {snapshot.depth_connection_resets:,}"
            ),
        )

        connections = Table(expand=True, box=None, pad_edge=False)
        connections.add_column("연결 그룹")
        connections.add_column("연결")
        connections.add_column("구독")
        connections.add_column("데이터")
        connections.add_column("연결 교체", justify="right")
        connections.add_column("최근 수신", justify="right")
        for group, state in snapshot.connections.items():
            age = "-"
            if state.last_data_age_seconds is not None:
                age = f"{state.last_data_age_seconds:,.1f}s 전"
            connections.add_row(
                group,
                state.connection,
                state.subscription,
                state.data,
                f"{state.connection_changes:,}",
                age,
            )

        collections = Table(expand=True, box=None, pad_edge=False)
        collections.add_column("저장 경로")
        collections.add_column("MongoDB 컬렉션")
        collections.add_column("상태")
        collections.add_column("접수", justify="right")
        collections.add_column("적재 확인", justify="right")
        collections.add_column("미확인", justify="right")
        collections.add_column("버퍼", justify="right")
        collections.add_column("최근 배치", justify="right")
        for route, state in snapshot.collections.items():
            duration = "-"
            if state.last_batch_duration_ms is not None:
                duration = f"{state.last_batch_size:,} / {state.last_batch_duration_ms:,.1f}ms"
            collections.add_row(
                route.value,
                state.collection_name,
                state.state,
                f"{state.accepted:,}",
                f"{state.persisted:,}",
                f"{state.pending:,}",
                _format_bytes(state.pending_bytes),
                duration,
            )

        extra: list[RenderableType] = []
        if snapshot.latest_latency_ms:
            latency = " | ".join(
                f"{stream} {value:+,}ms"
                for stream, value in snapshot.latest_latency_ms.items()
            )
            extra.append(Text(f"최근 event_time 차이: {latency}"))
        if snapshot.last_depth_gap is not None:
            gap = snapshot.last_depth_gap
            extra.append(
                Text(
                    f"최근 depth 공백 의심: {gap.symbol} "
                    f"{gap.previous_final_update_id} → {gap.next_first_update_id}",
                    style="yellow",
                )
            )
        if snapshot.recent_error is not None:
            extra.append(Text(f"최근 오류: {snapshot.recent_error}", style="red"))

        return Panel(
            Group(
                rates,
                Text(""),
                summary,
                Text(""),
                connections,
                Text(""),
                collections,
                *extra,
            ),
            title="Binance 다중 스트림 수집 현황",
            border_style="cyan",
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        with Live(
            self.render(self._stats.snapshot()),
            console=self._console,
            screen=self._screen,
            auto_refresh=False,
            redirect_stdout=False,
            redirect_stderr=False,
        ) as live:
            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=self._refresh_interval,
                    )
                except TimeoutError:
                    pass
                live.update(self.render(self._stats.snapshot()), refresh=True)
