"""JSON 모드와 TUI 모드의 운영 로그 경로를 분리한다."""

from __future__ import annotations

import logging
import sys
from typing import TextIO

from src.monitoring.stats import StatsCollector


class TuiStatusLogHandler(logging.Handler):
    """화면을 밀어내지 않고 최근 경고·오류만 통계 상태에 기록한다."""

    def __init__(self, stats: StatsCollector) -> None:
        super().__init__(level=logging.WARNING)
        self._stats = stats

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._stats.record_error(self.format(record))
        except Exception:
            self.handleError(record)


def configure_application_logging(
    mode: str,
    *,
    stats: StatsCollector,
    stream: TextIO = sys.stderr,
) -> None:
    """TUI 중에는 로그를 화면 상태로, JSON 모드에서는 stderr로 보낸다."""

    if mode == "tui":
        handler: logging.Handler = TuiStatusLogHandler(stats)
        handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    else:
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler],
        force=True,
    )
