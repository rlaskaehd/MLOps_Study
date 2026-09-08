"""수집 대상 심볼과 명령행 설정을 정의한다."""

from __future__ import annotations

import argparse
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO
from urllib.parse import quote_plus

from dotenv import load_dotenv


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

_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+$")


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
    database: str
    collection: str

    @property
    def uri(self) -> str:
        credentials = ""
        if self.username is not None and self.password is not None:
            credentials = (
                f"{quote_plus(self.username)}:{quote_plus(self.password)}@"
            )
        return f"mongodb://{credentials}{self.host}:{self.port}"


class ConfigurationError(ValueError):
    """프로그램을 안전하게 시작할 수 없는 설정인 경우."""


def _optional_setting(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def load_mongo_config(
    *,
    environ: Mapping[str, str] | None = None,
    env_file: Path | None = None,
) -> MongoConfig:
    """프로젝트 루트의 .env와 환경 변수에서 MongoDB 설정을 읽는다."""

    if environ is None:
        load_dotenv(env_file or PROJECT_ROOT / ".env", override=False)
        source: Mapping[str, str] = os.environ
    else:
        source = environ

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

    return MongoConfig(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        collection=collection,
    )


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
