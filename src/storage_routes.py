"""수집 이벤트가 향할 논리 저장 경로를 정의한다."""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from types import MappingProxyType


class StorageRoute(str, Enum):
    """물리 저장소 이름과 독립적인 이벤트 목적지."""

    AGG_TRADE = "agg_trade"
    ORDER_BOOK_DEPTH = "depth"
    BOOK_TICKER = "book_ticker"
    KLINE = "kline"
    MARK_PRICE = "mark_price"
    CONTROL = "control"


ROUTE_BY_STREAM_TYPE: Mapping[str, StorageRoute] = MappingProxyType(
    {
        "aggTrade": StorageRoute.AGG_TRADE,
        "depth": StorageRoute.ORDER_BOOK_DEPTH,
        "bookTicker": StorageRoute.BOOK_TICKER,
        "kline": StorageRoute.KLINE,
        "markPrice": StorageRoute.MARK_PRICE,
    }
)
DATA_STORAGE_ROUTES = tuple(ROUTE_BY_STREAM_TYPE.values())
STORAGE_ROUTES = (*DATA_STORAGE_ROUTES, StorageRoute.CONTROL)


def storage_route_for_stream_type(stream_type: str) -> StorageRoute:
    """알려진 시장 이벤트를 논리 경로로, 나머지를 제어 경로로 보낸다."""

    return ROUTE_BY_STREAM_TYPE.get(stream_type, StorageRoute.CONTROL)
