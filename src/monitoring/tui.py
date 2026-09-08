"""심볼별 초당 수집량과 누적 상태를 터미널에 표시한다."""

from __future__ import annotations

import asyncio
from typing import TextIO

from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.monitoring.stats import StatsCollector, StatsSnapshot


BAR_WIDTH = 30


def _format_uptime(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds_value = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds_value:02d}"


class TuiRenderer:
    """통계 스냅샷만 읽어 1초 간격으로 전체 화면을 갱신한다."""

    def __init__(
        self,
        stats: StatsCollector,
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
    def render(snapshot: StatsSnapshot) -> RenderableType:
        """모든 심볼에 같은 축을 적용한 가로 바차트를 만든다."""

        chart = Table(expand=True, box=None, pad_edge=False)
        chart.add_column("심볼", no_wrap=True, width=12)
        chart.add_column("초당 수집량", ratio=1)
        chart.add_column("events/sec", justify="right", width=12)

        maximum = max(1, *snapshot.per_symbol_rate.values())
        for symbol, rate in snapshot.per_symbol_rate.items():
            filled = round(BAR_WIDTH * rate / maximum) if rate else 0
            bar = Text()
            bar.append("█" * filled, style="bold cyan")
            bar.append("░" * (BAR_WIDTH - filled), style="dim")
            chart.add_row(symbol, bar, f"{rate:,}")

        totals = Table.grid(expand=True)
        totals.add_column(justify="left")
        totals.add_column(justify="center")
        totals.add_column(justify="right")
        totals.add_row(
            f"전체 초당 수집량: {snapshot.total_rate:,} events/sec",
            f"동작 시간: {_format_uptime(snapshot.uptime_seconds)}",
            f"누적 수집량: {snapshot.cumulative_trades:,}",
        )

        output_state = "연결됨" if snapshot.output_connected else "미연결"
        forwarded_label = "큐 접수" if snapshot.storage_name else "전달 완료"
        status = Table.grid(expand=True)
        status.add_column(ratio=1)
        status.add_row(
            Text(
                " | ".join(
                    (
                        f"연결: {snapshot.connection_state}",
                        f"구독: {snapshot.subscription_state}",
                        f"후속 출력: {output_state}",
                        f"{forwarded_label}: {snapshot.forwarded_messages:,}",
                        f"제어·미분류: {snapshot.control_or_unclassified:,}",
                    )
                )
            )
        )
        if snapshot.storage_name is not None:
            duration = "-"
            if snapshot.last_batch_duration_ms is not None:
                duration = f"{snapshot.last_batch_duration_ms:,.1f}ms"
            status.add_row(
                Text(
                    " | ".join(
                        (
                            f"{snapshot.storage_name}: {snapshot.storage_state}",
                            f"적재 확인: {snapshot.persisted_messages:,}",
                            f"미확인: {snapshot.pending_messages:,}",
                            f"최근 배치: {snapshot.last_batch_size:,}건 / {duration}",
                        )
                    )
                )
            )
        if snapshot.recent_error is not None:
            status.add_row(Text(f"최근 오류: {snapshot.recent_error}", style="red"))

        return Panel(
            Group(chart, Text(""), totals, Text(""), status),
            title="Binance aggTrade 실시간 수집 현황",
            border_style="cyan",
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        """종료 요청 전까지 스냅샷을 갱신하고 화면을 안전하게 복원한다."""

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
