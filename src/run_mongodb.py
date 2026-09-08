"""TUI를 표시하면서 표준 이벤트를 로컬 MongoDB에 적재한다."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Callable, Sequence
from typing import Any

from src.collector.client import MessageHandlerError
from src.config import (
    DEFAULT_SYMBOLS,
    CollectorConfig,
    ConfigurationError,
    MongoConfig,
    load_mongo_config,
    normalize_symbols,
    validate_terminal_config,
)
from src.main import configure_logging, run_collector
from src.monitoring.stats import StatsCollector
from src.outputs.base import EventOutput
from src.outputs.mongodb import MongoBatchOutput
from src.pipeline import OutputLifecycleError, OutputWriteError


LOGGER = logging.getLogger("binance_mongodb_collector")


def parse_symbols(argv: Sequence[str] | None = None) -> tuple[str, ...]:
    parser = argparse.ArgumentParser(
        description="Binance aggTrade TUI와 로컬 MongoDB 1초 배치 적재",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=list(DEFAULT_SYMBOLS),
        metavar="SYMBOL",
        help="수집할 심볼 목록 (기본값: 10개 학습용 심볼)",
    )
    namespace = parser.parse_args(argv)
    try:
        return normalize_symbols(namespace.symbols)
    except ValueError as error:
        parser.error(str(error))


def build_mongodb_runtime(
    symbols: Sequence[str],
    mongo_config: MongoConfig,
    *,
    output_factory: Callable[..., EventOutput] = MongoBatchOutput,
    output_options: dict[str, Any] | None = None,
) -> tuple[StatsCollector, EventOutput]:
    """MongoDB 적재 콜백과 TUI 통계를 같은 실행 객체로 연결한다."""

    stats = StatsCollector(
        symbols,
        output_connected=True,
        storage_name="MongoDB",
    )
    options = dict(output_options or {})
    output = output_factory(
        mongo_config,
        on_batch_persisted=stats.record_persisted,
        on_state_changed=stats.set_storage_state,
        **options,
    )
    return stats, output


def main(argv: Sequence[str] | None = None) -> int:
    symbols = parse_symbols(argv)
    try:
        mongo_config = load_mongo_config()
        terminal_config = CollectorConfig(
            symbols=symbols,
            mode="tui",
            output=None,
        )
        validate_terminal_config(
            terminal_config,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
    except ConfigurationError as error:
        print(f"설정 오류: {error}", file=sys.stderr)
        return 2

    stats, output = build_mongodb_runtime(symbols, mongo_config)
    configure_logging("tui", stats=stats)
    try:
        asyncio.run(
            run_collector(
                symbols=symbols,
                mode="tui",
                output=output,
                stats=stats,
            )
        )
    except KeyboardInterrupt:
        LOGGER.info("종료 요청을 받아 수집기를 종료했습니다.")
    except (MessageHandlerError, OutputWriteError, OutputLifecycleError):
        print("[ERROR] MongoDB 출력 실패로 수집기를 종료합니다.", file=sys.stderr)
        return 1
    except Exception:
        LOGGER.exception("복구할 수 없는 오류로 수집기를 종료합니다.")
        return 1

    LOGGER.info("MongoDB 수집기를 안전하게 종료했습니다.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
