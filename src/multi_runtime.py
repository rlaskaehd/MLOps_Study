"""다중 스트림 수집기, 출력 감시와 TUI의 생명주기를 감독한다."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterable
from typing import Protocol

from src.collector.multi_client import MultiStreamCollector
from src.collector.product_catalog import ProductValidationResult, validate_products
from src.collector.streams import StreamSpec, index_stream_specs
from src.config import MultiStreamConfig
from src.main import install_signal_handlers, remove_signal_handlers
from src.models.event import ReceivedMessage
from src.monitoring.multi_stats import MultiStreamStatsCollector
from src.monitoring.multi_tui import MultiStreamTuiRenderer
from src.multi_pipeline import MultiStreamEventDispatcher
from src.outputs.base import EventOutput


LOGGER = logging.getLogger("binance_multi_stream_collector")
ProductValidator = Callable[
    [tuple[str, ...]],
    Awaitable[ProductValidationResult],
]


class MultiCollectorRunner(Protocol):
    async def run(
        self,
        handler: Callable[[ReceivedMessage], Awaitable[None]],
        stop_event: asyncio.Event,
    ) -> None: ...


class MultiTuiMonitor(Protocol):
    async def run(self, stop_event: asyncio.Event) -> None: ...


async def run_multi_stream_collection(
    config: MultiStreamConfig,
    specs: Iterable[StreamSpec],
    *,
    output: EventOutput,
    stats: MultiStreamStatsCollector,
    collector: MultiCollectorRunner | None = None,
    tui: MultiTuiMonitor | None = None,
    stop_event: asyncio.Event | None = None,
    logger: logging.Logger = LOGGER,
    product_validator: ProductValidator = validate_products,
    manage_signals: bool = True,
) -> None:
    """확장 프로필의 검증, 수집, 출력과 화면을 한 종료 경로로 묶는다."""

    active_specs = tuple(specs)
    if config.validate_symbols:
        await product_validator(config.symbols)
    active_collector = collector or MultiStreamCollector(
        active_specs,
        logger=logger,
        status_handler=stats.record_connection_status,
    )
    dispatcher = MultiStreamEventDispatcher(
        stream_index=index_stream_specs(active_specs),
        output=output,
        stats=stats,
        logger=logger,
    )
    active_tui = tui or MultiStreamTuiRenderer(stats)
    active_stop_event = stop_event or asyncio.Event()
    installed = install_signal_handlers(active_stop_event) if manage_signals else []

    opened = False
    primary_error: BaseException | None = None
    try:
        await dispatcher.open()
        opened = True
        await _run_with_tui(
            active_collector,
            dispatcher,
            active_tui,
            active_stop_event,
        )
    except BaseException as error:
        primary_error = error
        raise
    finally:
        active_stop_event.set()
        try:
            if opened:
                try:
                    await dispatcher.close()
                except BaseException:
                    if primary_error is None:
                        raise
                    logger.exception("다중 스트림 출력 정리 중 추가 오류가 발생했습니다.")
        finally:
            remove_signal_handlers(installed)


async def _run_collector_with_output_monitor(
    collector: MultiCollectorRunner,
    dispatcher: MultiStreamEventDispatcher,
    stop_event: asyncio.Event,
) -> None:
    collector_task = asyncio.create_task(collector.run(dispatcher.handle, stop_event))
    failure_task: asyncio.Task[None] | None = None
    if dispatcher.supports_failure_monitor():
        failure_task = asyncio.create_task(dispatcher.wait_for_output_failure())
    try:
        if failure_task is None:
            await collector_task
            return
        completed, _ = await asyncio.wait(
            {collector_task, failure_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if failure_task in completed:
            stop_event.set()
            if not collector_task.done():
                collector_task.cancel()
            await asyncio.gather(collector_task, return_exceptions=True)
            await failure_task
        await collector_task
    finally:
        tasks = [collector_task]
        if failure_task is not None:
            tasks.append(failure_task)
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _run_with_tui(
    collector: MultiCollectorRunner,
    dispatcher: MultiStreamEventDispatcher,
    tui: MultiTuiMonitor,
    stop_event: asyncio.Event,
) -> None:
    collector_task: asyncio.Task[None] | None = None
    tui_task = asyncio.create_task(tui.run(stop_event))
    try:
        await asyncio.sleep(0)
        collector_task = asyncio.create_task(
            _run_collector_with_output_monitor(collector, dispatcher, stop_event)
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
