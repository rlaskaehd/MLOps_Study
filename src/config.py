"""수집 대상 심볼과 명령행 설정을 정의한다."""

from __future__ import annotations

import argparse
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TextIO
from urllib.parse import quote_plus

from dotenv import load_dotenv

from src.storage_routes import STORAGE_ROUTES, StorageRoute


DEFAULT_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "LTCUSDT",
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MONGODB_HOST = "localhost"
DEFAULT_MONGODB_PORT = 27017
DEFAULT_MONGODB_DATABASE = "studygroup"
DEFAULT_MONGODB_COLLECTION = "binance_events"

MULTI_COLLECTION_ENV_BY_ROUTE: Mapping[StorageRoute, str] = MappingProxyType(
    {
        StorageRoute.AGG_TRADE: "DATALAKE_AGG_TRADES_COLLECTION_NAME",
        StorageRoute.ORDER_BOOK_DEPTH: (
            "DATALAKE_ORDER_BOOK_DEPTH_COLLECTION_NAME"
        ),
        StorageRoute.BOOK_TICKER: "DATALAKE_BOOK_TICKERS_COLLECTION_NAME",
        StorageRoute.KLINE: "DATALAKE_KLINES_COLLECTION_NAME",
        StorageRoute.MARK_PRICE: "DATALAKE_MARK_PRICES_COLLECTION_NAME",
        StorageRoute.CONTROL: "DATALAKE_COLLECTOR_CONTROL_COLLECTION_NAME",
    }
)

_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+$")


class ConfigurationError(ValueError):
    """프로그램을 안전하게 시작할 수 없는 설정인 경우."""


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    """명령행에서 확정된 수집 설정."""

    symbols: tuple[str, ...]
    mode: str
    output: str | None


@dataclass(frozen=True, slots=True)
class MongoConfig:
    """로컬 MongoDB 출력에 필요한 연결 위치를 보관한다."""

    host: str
    port: int
    username: str | None
    password: str | None
    auth_source: str | None
    database: str
    collection: str

    @property
    def uri(self) -> str:
        if self.username is None or self.password is None:
            return f"mongodb://{self.host}:{self.port}"

        credentials = (
            f"{quote_plus(self.username)}:{quote_plus(self.password)}@"
        )
        auth_source = quote_plus(self.auth_source or self.database)
        return (
            f"mongodb://{credentials}{self.host}:{self.port}/"
            f"?authSource={auth_source}"
        )


@dataclass(frozen=True, slots=True)
class MultiMongoCollectionConfig:
    """논리 저장 경로별 MongoDB 물리 컬렉션 이름."""

    collection_name_by_route: Mapping[StorageRoute, str]

    def __post_init__(self) -> None:
        names = {
            route: collection_name.strip()
            for route, collection_name in self.collection_name_by_route.items()
        }
        if set(names) != set(STORAGE_ROUTES):
            raise ConfigurationError(
                "다중 스트림의 5개 데이터 경로와 제어 경로 설정이 필요합니다."
            )
        if any(not collection_name for collection_name in names.values()):
            raise ConfigurationError(
                "다중 스트림 컬렉션 이름은 비어 있을 수 없습니다."
            )
        if len(set(names.values())) != len(names):
            raise ConfigurationError(
                "다중 스트림의 물리 컬렉션 이름은 서로 달라야 합니다."
            )
        object.__setattr__(
            self,
            "collection_name_by_route",
            MappingProxyType(names),
        )


@dataclass(frozen=True, slots=True)
class MultiStreamConfig:
    """다중 스트림 확장 실행에 필요한 수집 설정."""

    symbols: tuple[str, ...] = DEFAULT_SYMBOLS
    kline_interval: str = "1m"
    depth_speed: str = "100ms"
    mark_price_speed: str = "1s"
    validate_symbols: bool = True

    def __post_init__(self) -> None:
        from src.collector.streams import validate_stream_options

        object.__setattr__(self, "symbols", normalize_symbols(self.symbols))
        validate_stream_options(
            kline_interval=self.kline_interval,
            depth_speed=self.depth_speed,
            mark_price_speed=self.mark_price_speed,
        )


def _optional_setting(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _environment_source(
    *,
    environ: Mapping[str, str] | None,
    env_file: Path | None,
) -> Mapping[str, str]:
    if environ is not None:
        return environ
    load_dotenv(env_file or PROJECT_ROOT / ".env", override=False)
    return os.environ


def load_mongo_config(
    *,
    environ: Mapping[str, str] | None = None,
    env_file: Path | None = None,
) -> MongoConfig:
    """프로젝트 루트의 .env와 환경 변수에서 MongoDB 설정을 읽는다."""

    source = _environment_source(environ=environ, env_file=env_file)

    host = (source.get("DATALAKE_HOST") or DEFAULT_MONGODB_HOST).strip()
    port_text = (source.get("DATALAKE_PORT") or str(DEFAULT_MONGODB_PORT)).strip()
    database = (
        source.get("DATALAKE_DB_NAME") or DEFAULT_MONGODB_DATABASE
    ).strip()
    collection = (
        source.get("DATALAKE_COLLECTION_NAME") or DEFAULT_MONGODB_COLLECTION
    ).strip()
    username = _optional_setting(source.get("DATALAKE_USER"))
    password = _optional_setting(source.get("DATALAKE_PASSWORD"))
    configured_auth_source = _optional_setting(
        source.get("DATALAKE_AUTH_SOURCE")
    )

    try:
        port = int(port_text)
    except ValueError as error:
        raise ConfigurationError("DATALAKE_PORT는 정수여야 합니다.") from error

    if not host:
        raise ConfigurationError("DATALAKE_HOST는 비어 있을 수 없습니다.")
    if not 1 <= port <= 65535:
        raise ConfigurationError("DATALAKE_PORT는 1~65535 범위여야 합니다.")
    if not database:
        raise ConfigurationError("DATALAKE_DB_NAME은 비어 있을 수 없습니다.")
    if not collection:
        raise ConfigurationError("DATALAKE_COLLECTION_NAME은 비어 있을 수 없습니다.")
    if (username is None) != (password is None):
        raise ConfigurationError(
            "DATALAKE_USER와 DATALAKE_PASSWORD는 함께 설정해야 합니다."
        )
    if username is None:
        if configured_auth_source is not None:
            raise ConfigurationError(
                "DATALAKE_AUTH_SOURCE는 MongoDB 인증정보와 함께 설정해야 합니다."
            )
        auth_source = None
    else:
        auth_source = configured_auth_source or database

    return MongoConfig(
        host=host,
        port=port,
        username=username,
        password=password,
        auth_source=auth_source,
        database=database,
        collection=collection,
    )


def load_multi_mongo_collection_config(
    *,
    environ: Mapping[str, str] | None = None,
    env_file: Path | None = None,
) -> MultiMongoCollectionConfig:
    """환경변수에서 확장 프로필의 물리 컬렉션 이름을 읽는다."""

    source = _environment_source(environ=environ, env_file=env_file)
    names: dict[StorageRoute, str] = {}
    missing: list[str] = []
    blank: list[str] = []
    for route, environment_name in MULTI_COLLECTION_ENV_BY_ROUTE.items():
        raw_value = source.get(environment_name)
        if raw_value is None:
            missing.append(environment_name)
            continue
        collection_name = raw_value.strip()
        if not collection_name:
            blank.append(environment_name)
            continue
        names[route] = collection_name

    if missing:
        raise ConfigurationError(
            "필수 다중 스트림 컬렉션 환경변수가 없습니다: "
            + ", ".join(missing)
        )
    if blank:
        raise ConfigurationError(
            "다중 스트림 컬렉션 이름은 비어 있을 수 없습니다: "
            + ", ".join(blank)
        )
    return MultiMongoCollectionConfig(names)


def normalize_symbols(values: Sequence[str]) -> tuple[str, ...]:
    """심볼을 대문자로 통일하고 잘못된 설정을 거부한다."""

    symbols = tuple(value.strip().upper() for value in values)
    if not symbols or any(not symbol for symbol in symbols):
        raise ValueError("수집할 심볼을 한 개 이상 지정해야 합니다.")
    if any(_SYMBOL_PATTERN.fullmatch(symbol) is None for symbol in symbols):
        raise ValueError("심볼에는 영문자와 숫자만 사용할 수 있습니다.")
    if len(set(symbols)) != len(symbols):
        raise ValueError("같은 심볼을 중복해서 지정할 수 없습니다.")
    return symbols


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Binance aggTrade 실시간 수집기",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=list(DEFAULT_SYMBOLS),
        metavar="SYMBOL",
        help="수집할 심볼 목록 (기본값: 10개 학습용 심볼)",
    )
    parser.add_argument(
        "--mode",
        choices=("json", "tui"),
        default="json",
        help="화면 모드 (기본값: json)",
    )
    parser.add_argument(
        "--output",
        choices=("stdout",),
        default=None,
        help="후속 출력 대상. TUI 모드에서는 지정하지 않으면 미연결",
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> CollectorConfig:
    parser = build_argument_parser()
    namespace = parser.parse_args(argv)
    try:
        symbols = normalize_symbols(namespace.symbols)
    except ValueError as error:
        parser.error(str(error))
    output = namespace.output
    if output is None and namespace.mode == "json":
        output = "stdout"
    return CollectorConfig(
        symbols=symbols,
        mode=namespace.mode,
        output=output,
    )


def validate_terminal_config(
    config: CollectorConfig,
    *,
    stdout: TextIO,
    stderr: TextIO,
) -> None:
    """TUI 제어 문자와 JSON 데이터가 같은 터미널에 섞이지 않게 한다."""

    if config.mode != "tui":
        return
    if not stderr.isatty():
        raise ConfigurationError(
            "TUI 모드는 stderr가 터미널일 때만 사용할 수 있습니다. "
            "--mode json을 사용해 주세요."
        )
    if config.output == "stdout" and stdout.isatty():
        raise ConfigurationError(
            "TUI와 JSON을 같은 터미널에 표시할 수 없습니다. "
            "stdout을 파이프로 연결하거나 --output을 생략해 주세요."
        )
