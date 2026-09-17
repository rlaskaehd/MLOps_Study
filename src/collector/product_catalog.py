"""Spot과 USDⓈ-M 공개 상품 목록에서 수집 대상을 확인한다."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen


SPOT_EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"
USDM_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

JsonFetcher = Callable[[str, float], Mapping[str, Any]]


class ProductCatalogError(RuntimeError):
    """요청한 상품을 확인할 수 없거나 수집 조건을 충족하지 않는 경우."""


@dataclass(frozen=True, slots=True)
class ProductValidationResult:
    spot_symbols: tuple[str, ...]
    usdm_perpetual_symbols: tuple[str, ...]


def _fetch_json(url: str, timeout: float) -> Mapping[str, Any]:
    request = Request(url, headers={"User-Agent": "binance-study-collector/2"})
    with urlopen(request, timeout=timeout) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ProductCatalogError("Binance 상품 응답이 JSON 객체가 아닙니다.")
    return payload


def _eligible_symbols(
    payload: Mapping[str, Any],
    *,
    futures: bool,
) -> set[str]:
    entries = payload.get("symbols")
    if not isinstance(entries, list):
        raise ProductCatalogError("Binance 상품 응답에 symbols 배열이 없습니다.")

    eligible: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        symbol = entry.get("symbol")
        if not isinstance(symbol, str) or entry.get("status") != "TRADING":
            continue
        if futures and (
            entry.get("contractType") != "PERPETUAL"
            or entry.get("quoteAsset") != "USDT"
        ):
            continue
        eligible.add(symbol)
    return eligible


async def validate_products(
    symbols: Sequence[str],
    *,
    timeout: float = 10.0,
    fetcher: JsonFetcher = _fetch_json,
) -> ProductValidationResult:
    """모든 symbol이 Spot과 USDⓈ-M 무기한 선물에서 거래 중인지 확인한다."""

    try:
        spot_payload, usdm_payload = await asyncio.gather(
            asyncio.to_thread(fetcher, SPOT_EXCHANGE_INFO_URL, timeout),
            asyncio.to_thread(fetcher, USDM_EXCHANGE_INFO_URL, timeout),
        )
    except ProductCatalogError:
        raise
    except Exception as error:
        raise ProductCatalogError(
            f"Binance 상품 목록 조회에 실패했습니다: {type(error).__name__}"
        ) from error

    spot = _eligible_symbols(spot_payload, futures=False)
    usdm = _eligible_symbols(usdm_payload, futures=True)
    requested = tuple(symbols)
    missing_spot = tuple(symbol for symbol in requested if symbol not in spot)
    missing_usdm = tuple(symbol for symbol in requested if symbol not in usdm)
    if missing_spot or missing_usdm:
        details: list[str] = []
        if missing_spot:
            details.append(f"Spot: {', '.join(missing_spot)}")
        if missing_usdm:
            details.append(f"USDⓈ-M PERPETUAL: {', '.join(missing_usdm)}")
        raise ProductCatalogError(
            "수집 조건을 충족하지 않는 symbol이 있습니다 (" + "; ".join(details) + ")"
        )

    return ProductValidationResult(
        spot_symbols=requested,
        usdm_perpetual_symbols=requested,
    )
