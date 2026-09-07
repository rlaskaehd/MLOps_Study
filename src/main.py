"""Binance 수집기를 실행해 표준화된 이벤트를 stdout으로 보낸다."""

import asyncio
import logging
import signal
import sys
from collections.abc import Callable

from src.collector.client import BinanceCollector, Message, MessageHandlerError
from src.collector.parser import normalize_message
from src.outputs.stdout import StdoutOutput


LOGGER = logging.getLogger("binance_collector")


def configure_logging() -> None:
    """운영 로그가 데이터 stdout과 섞이지 않도록 stderr로 설정한다."""

    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(message)s",
        stream=sys.stderr,
    )


def create_message_handler(
    output: StdoutOutput,
    logger: logging.Logger,
) -> Callable[[Message], None]:
    """수신 메시지를 표준화하고 한 줄의 JSON으로 출력한다."""

    def handle(message: Message) -> None:
        normalized = normalize_message(message)
        output.write(normalized.event)
        if normalized.warning is not None:
            logger.warning(
                "메시지를 raw_message로 보존했습니다: %s",
                normalized.warning,
            )

    return handle


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
    collector: BinanceCollector | None = None,
    output: StdoutOutput | None = None,
    stop_event: asyncio.Event | None = None,
    logger: logging.Logger = LOGGER,
    manage_signals: bool = True,
) -> None:
    """실행 구성요소를 연결하고 종료 시 WebSocket을 정리한다."""

    active_collector = collector or BinanceCollector(logger=logger)
    active_output = output or StdoutOutput()
    active_stop_event = stop_event or asyncio.Event()
    installed = (
        install_signal_handlers(active_stop_event) if manage_signals else []
    )

    try:
        await active_collector.run(
            create_message_handler(active_output, logger),
            active_stop_event,
        )
    finally:
        remove_signal_handlers(installed)


def main() -> int:
    configure_logging()

    try:
        asyncio.run(run_collector())
    except KeyboardInterrupt:
        LOGGER.info("종료 요청을 받아 수집기를 종료했습니다.")
    except MessageHandlerError:
        LOGGER.exception("메시지 출력에 실패해 수집기를 종료합니다.")
        return 1
    except Exception:
        LOGGER.exception("복구할 수 없는 오류로 수집기를 종료합니다.")
        return 1

    LOGGER.info("수집기를 안전하게 종료했습니다.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
