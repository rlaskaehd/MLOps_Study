"""TUI를 표시하면서 표준 이벤트를 로컬 MongoDB에 적재한다."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from src.collector.product_catalog import ProductCatalogError
from src.collector.streams import StreamSpec, build_stream_specs
from src.collector.client import MessageHandlerError
from src.config import (
    DEFAULT_SYMBOLS,
    CollectorConfig,
    ConfigurationError,
    MongoConfig,
    MultiMongoCollectionConfig,
    MultiStreamConfig,
    load_mongo_config,
    load_multi_mongo_collection_config,
    normalize_symbols,
    validate_terminal_config,
)
from src.main import configure_logging, run_collector
from src.monitoring.logging import configure_application_logging
from src.monitoring.multi_stats import MultiStreamStatsCollector
from src.monitoring.stats import StatsCollector
from src.multi_runtime import run_multi_stream_collection
from src.outputs.base import EventOutput
from src.outputs.mongodb import MongoBatchOutput
from src.outputs.mongodb_multi import MongoMultiCollectionOutput
from src.pipeline import OutputLifecycleError, OutputWriteError


LOGGER = logging.getLogger("binance_mongodb_collector")


@dataclass(frozen=True, slots=True)
class MongoRunnerArguments:
    profile: str
    symbols: tuple[str, ...]
    kline_interval: str
    depth_speed: str
    mark_price_speed: str
    validate_symbols: bool


def parse_mongodb_args(
    argv: Sequence[str] | None = None,
) -> MongoRunnerArguments:
    parser = argparse.ArgumentParser(
        description="Binance 실시간 수집기와 로컬 MongoDB 배치 적재",
    )
    parser.add_argument(
        "--profile",
        choices=("legacy", "multi-stream"),
        default="legacy",
        help="수집 프로필 (기본값: legacy aggTrade)",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=list(DEFAULT_SYMBOLS),
        metavar="SYMBOL",
        help="수집할 심볼 목록 (기본값: 10개 학습용 심볼)",
    )
    parser.add_argument("--kline-interval", default="1m")
    parser.add_argument("--depth-speed", default="100ms")
    parser.add_argument("--mark-price-speed", default="1s")
    parser.add_argument(
        "--skip-symbol-validation",
        action="store_true",
        help="오프라인 점검에서만 실행 전 상품 목록 확인을 생략",
    )
    namespace = parser.parse_args(argv)
    try:
        symbols = normalize_symbols(namespace.symbols)
        stream_config = MultiStreamConfig(
            symbols=symbols,
            kline_interval=namespace.kline_interval,
            depth_speed=namespace.depth_speed,
            mark_price_speed=namespace.mark_price_speed,
            validate_symbols=not namespace.skip_symbol_validation,
        )
    except ValueError as error:
        parser.error(str(error))
    return MongoRunnerArguments(
        profile=namespace.profile,
        symbols=stream_config.symbols,
        kline_interval=stream_config.kline_interval,
        depth_speed=stream_config.depth_speed,
        mark_price_speed=stream_config.mark_price_speed,
        validate_symbols=stream_config.validate_symbols,
    )


def parse_symbols(argv: Sequence[str] | None = None) -> tuple[str, ...]:
    return parse_mongodb_args(argv).symbols


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


def build_multi_stream_runtime(
    config: MultiStreamConfig,
    mongo_config: MongoConfig,
    collection_config: MultiMongoCollectionConfig,
    *,
    output_factory: Callable[..., EventOutput] = MongoMultiCollectionOutput,
    output_options: dict[str, Any] | None = None,
) -> tuple[tuple[StreamSpec, ...], MultiStreamStatsCollector, EventOutput]:
    """다중 스트림 명세와 MongoDB 콜백을 하나의 통계 객체에 연결한다."""

    specs = build_stream_specs(
        config.symbols,
        kline_interval=config.kline_interval,
        depth_speed=config.depth_speed,
        mark_price_speed=config.mark_price_speed,
    )
    collection_names = collection_config.collection_name_by_route
    stats = MultiStreamStatsCollector(
        config.symbols,
        collection_name_by_route=collection_names,
    )
    options = dict(output_options or {})
    options["collection_name_map"] = collection_names
    output = output_factory(
        mongo_config,
        on_batch_persisted=stats.record_batch_persisted,
        on_state_changed=stats.record_storage_state,
        on_buffer_changed=stats.record_buffer_changed,
        **options,
    )
    return specs, stats, output


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parse_mongodb_args(argv)
    try:
        mongo_config = load_mongo_config()
        collection_config = (
            load_multi_mongo_collection_config()
            if arguments.profile == "multi-stream"
            else None
        )
        terminal_config = CollectorConfig(
            symbols=arguments.symbols,
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

    if arguments.profile == "legacy":
        stats, output = build_mongodb_runtime(arguments.symbols, mongo_config)
        configure_logging("tui", stats=stats)
    else:
        assert collection_config is not None
        stream_config = MultiStreamConfig(
            symbols=arguments.symbols,
            kline_interval=arguments.kline_interval,
            depth_speed=arguments.depth_speed,
            mark_price_speed=arguments.mark_price_speed,
            validate_symbols=arguments.validate_symbols,
        )
        specs, stats, output = build_multi_stream_runtime(
            stream_config,
            mongo_config,
            collection_config,
        )
        configure_application_logging("tui", stats=stats)
    try:
        if arguments.profile == "legacy":
            asyncio.run(
                run_collector(
                    symbols=arguments.symbols,
                    mode="tui",
                    output=output,
                    stats=stats,
                )
            )
        else:
            asyncio.run(
                run_multi_stream_collection(
                    stream_config,
                    specs,
                    output=output,
                    stats=stats,
                )
            )
    except KeyboardInterrupt:
        LOGGER.info("종료 요청을 받아 수집기를 종료했습니다.")
    except ProductCatalogError as error:
        print(f"상품 확인 오류: {error}", file=sys.stderr)
        return 2
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
