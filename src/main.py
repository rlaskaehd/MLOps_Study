"""Binance 수집기를 JSON 또는 TUI 모드로 실행한다."""

import asyncio
import logging
import signal
import sys
from collections.abc import Awaitable, Callable, Sequence
from typing import Protocol

from src.collector.client import BinanceCollector, Message, MessageHandlerError
from src.config import (
    DEFAULT_SYMBOLS,
    ConfigurationError,
    parse_args,
    validate_terminal_config,
)
from src.monitoring.logging import configure_application_logging
from src.monitoring.stats import StatsCollector
from src.monitoring.tui import TuiRenderer
from src.outputs.base import EventOutput
from src.outputs.factory import create_output
from src.pipeline import EventDispatcher


LOGGER = logging.getLogger("binance_collector")


class TuiMonitor(Protocol):
    async def run(self, stop_event: asyncio.Event) -> None: ...


class _ModeDefaultOutput:
    pass


_MODE_DEFAULT_OUTPUT = _ModeDefaultOutput()


def configure_logging(
    mode: str = "json",
    *,
    stats: StatsCollector | None = None,
) -> None:
    """운영 로그가 데이터 stdout과 섞이지 않도록 stderr로 설정한다."""

    active_stats = stats or StatsCollector(
        DEFAULT_SYMBOLS,
        output_connected=mode == "json",
    )
    configure_application_logging(mode, stats=active_stats)


def create_message_handler(
    output: EventOutput,
    logger: logging.Logger,
    stats: StatsCollector | None = None,
) -> Callable[[Message], Awaitable[None]]:
    """기존 호출 지점을 공통 EventDispatcher 경로에 연결한다."""

    active_stats = stats or StatsCollector(
        DEFAULT_SYMBOLS,
        output_connected=True,
    )
    return EventDispatcher(
        output=output,
        stats=active_stats,
        logger=logger,
    ).handle


def install_signal_handlers(stop_event: asyncio.Event) -> list[signal.Signals]:
    """지원되는 환경에서 SIGINT와 SIGTERM을 종료 이벤트로 바꾼다."""

    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop_event.set)
        except (NotImplementedError, RuntimeError):
            continue
        installed.append(signum)

    return installed


def remove_signal_handlers(installed: list[signal.Signals]) -> None:
    loop = asyncio.get_running_loop()
    for signum in installed:
        loop.remove_signal_handler(signum)


async def run_collector(
    *,
    symbols: Sequence[str] = DEFAULT_SYMBOLS,
    mode: str = "json",
    collector: BinanceCollector | None = None,
    output: EventOutput | None | _ModeDefaultOutput = _MODE_DEFAULT_OUTPUT,
    stats: StatsCollector | None = None,
    tui: TuiMonitor | None = None,
    stop_event: asyncio.Event | None = None,
    logger: logging.Logger = LOGGER,
    manage_signals: bool = True,
) -> None:
    """실행 구성요소를 연결하고 종료 시 WebSocket을 정리한다."""

    if mode not in {"json", "tui"}:
        raise ValueError(f"지원하지 않는 실행 모드입니다: {mode}")
    if isinstance(output, _ModeDefaultOutput):
        active_output = create_output("stdout" if mode == "json" else None)
    else:
        active_output = output
    active_stats = stats or StatsCollector(
        symbols,
        output_connected=active_output is not None,
    )
    active_stats.set_output_connected(active_output is not None)
    active_collector = collector or BinanceCollector(
        symbols=tuple(symbols),
        logger=logger,
        status_handler=active_stats.record_collector_status,
    )
    dispatcher = EventDispatcher(
        output=active_output,
        stats=active_stats,
        logger=logger,
    )
    active_stop_event = stop_event or asyncio.Event()
    installed = install_signal_handlers(active_stop_event) if manage_signals else []

    opened = False
    try:
        await dispatcher.open()
        opened = True
        if mode == "tui":
            active_tui = tui or TuiRenderer(active_stats)
            await _run_with_tui(
                active_collector,
                dispatcher,
                active_tui,
                active_stop_event,
            )
        else:
            await active_collector.run(dispatcher.handle, active_stop_event)
    finally:
        active_stop_event.set()
        try:
            if opened:
                await dispatcher.close()
        finally:
            remove_signal_handlers(installed)


async def _run_with_tui(
    collector: BinanceCollector,
    dispatcher: EventDispatcher,
    tui: TuiMonitor,
    stop_event: asyncio.Event,
) -> None:
    """수집과 TUI를 함께 실행하고 어느 쪽의 실패도 정리 경로로 보낸다."""

    collector_task: asyncio.Task[None] | None = None
    tui_task = asyncio.create_task(tui.run(stop_event))
    try:
        # 화면이 준비된 뒤 수집을 시작해 짧은 실행에서도 상태를 한 번 표시한다.
        await asyncio.sleep(0)
        collector_task = asyncio.create_task(
            collector.run(dispatcher.handle, stop_event)
        )
        done, _ = await asyncio.wait(
            {collector_task, tui_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if tui_task in done and collector_task not in done:
            try:
                await tui_task
            except BaseException:
                collector_task.cancel()
                await asyncio.gather(collector_task, return_exceptions=True)
                raise
            stop_event.set()

        try:
            await collector_task
        finally:
            stop_event.set()
            await tui_task
    finally:
        stop_event.set()
        tasks = [tui_task]
        if collector_task is not None:
            tasks.append(collector_task)
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _report_failure(mode: str, message: str) -> None:
    if mode == "tui":
        print(f"[ERROR] {message}", file=sys.stderr)
    else:
        LOGGER.exception(message)


def main(argv: Sequence[str] | None = None) -> int:
    config = parse_args(argv)
    try:
        validate_terminal_config(config, stdout=sys.stdout, stderr=sys.stderr)
    except ConfigurationError as error:
        print(f"설정 오류: {error}", file=sys.stderr)
        return 2

    output = create_output(config.output)
    stats = StatsCollector(
        config.symbols,
        output_connected=output is not None,
    )
    configure_logging(config.mode, stats=stats)

    try:
        asyncio.run(
            run_collector(
                symbols=config.symbols,
                mode=config.mode,
                output=output,
                stats=stats,
            )
        )
    except KeyboardInterrupt:
        LOGGER.info("종료 요청을 받아 수집기를 종료했습니다.")
    except MessageHandlerError:
        _report_failure(config.mode, "메시지 출력에 실패해 수집기를 종료합니다.")
        return 1
    except Exception:
        _report_failure(config.mode, "복구할 수 없는 오류로 수집기를 종료합니다.")
        return 1

    LOGGER.info("수집기를 안전하게 종료했습니다.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
