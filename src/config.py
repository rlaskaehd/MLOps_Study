"""수집 대상 심볼과 명령행 설정을 정의한다."""

from __future__ import annotations

import argparse
import re
from collections.abc import Sequence
from dataclasses import dataclass


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
    return parser


def parse_args(argv: Sequence[str] | None = None) -> CollectorConfig:
    parser = build_argument_parser()
    namespace = parser.parse_args(argv)
    try:
        symbols = normalize_symbols(namespace.symbols)
    except ValueError as error:
        parser.error(str(error))
    return CollectorConfig(symbols=symbols)
