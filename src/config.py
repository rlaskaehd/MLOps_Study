"""수집 대상 심볼과 명령행 설정을 정의한다."""

from __future__ import annotations

import argparse
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TextIO


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

_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+$")


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    """명령행에서 확정된 수집 설정."""

    symbols: tuple[str, ...]
    mode: str
    output: str | None


class ConfigurationError(ValueError):
    """터미널 스트림과 실행 모드의 조합이 안전하지 않은 경우."""


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
