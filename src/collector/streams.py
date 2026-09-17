"""다중 시장 WebSocket 스트림과 저장 대상을 선언한다."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType


SCHEMA_VERSION = 2
CONTROL_COLLECTION = "collector_control"


class Market(str, Enum):
    SPOT = "spot"
    USDM_FUTURES = "usdm_futures"


class StreamType(str, Enum):
    AGG_TRADE = "aggTrade"
    DEPTH = "depth"
    BOOK_TICKER = "bookTicker"
    KLINE = "kline"
    MARK_PRICE = "markPrice"


COLLECTION_BY_STREAM: Mapping[StreamType, str] = MappingProxyType(
    {
        StreamType.AGG_TRADE: "agg_trades",
        StreamType.DEPTH: "order_book_depth",
        StreamType.BOOK_TICKER: "book_tickers",
        StreamType.KLINE: "klines",
        StreamType.MARK_PRICE: "mark_prices",
    }
)


@dataclass(frozen=True, slots=True)
class ConnectionGroupSpec:
    key: str
    market: Market
    url: str
    stream_types: tuple[StreamType, ...]


SPOT_MARKET_GROUP = ConnectionGroupSpec(
    key="spot_market",
    market=Market.SPOT,
    url="wss://stream.binance.com:9443/stream",
    stream_types=(
        StreamType.AGG_TRADE,
        StreamType.BOOK_TICKER,
        StreamType.KLINE,
    ),
)
SPOT_DEPTH_GROUP = ConnectionGroupSpec(
    key="spot_depth",
    market=Market.SPOT,
    url="wss://stream.binance.com:9443/stream",
    stream_types=(StreamType.DEPTH,),
)
USDM_MARK_GROUP = ConnectionGroupSpec(
    key="usdm_mark",
    market=Market.USDM_FUTURES,
    url="wss://fstream.binance.com/market/stream",
    stream_types=(StreamType.MARK_PRICE,),
)
CONNECTION_GROUPS = (
    SPOT_MARKET_GROUP,
    SPOT_DEPTH_GROUP,
    USDM_MARK_GROUP,
)
CONNECTION_GROUP_BY_KEY: Mapping[str, ConnectionGroupSpec] = MappingProxyType(
    {group.key: group for group in CONNECTION_GROUPS}
)


@dataclass(frozen=True, slots=True)
class StreamSpec:
    market: Market
    stream_type: StreamType
    symbol: str
    stream_name: str
    collection: str
    connection_group: str


SUPPORTED_KLINE_INTERVALS = frozenset(
    {
        "1s",
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1M",
    }
)
SUPPORTED_DEPTH_SPEEDS = frozenset({"100ms", "1000ms"})
SUPPORTED_MARK_PRICE_SPEEDS = frozenset({"1s", "3s"})


def validate_stream_options(
    *,
    kline_interval: str,
    depth_speed: str,
    mark_price_speed: str,
) -> None:
    if kline_interval not in SUPPORTED_KLINE_INTERVALS:
        raise ValueError(f"지원하지 않는 kline 간격입니다: {kline_interval}")
    if depth_speed not in SUPPORTED_DEPTH_SPEEDS:
        raise ValueError(f"지원하지 않는 depth 갱신 속도입니다: {depth_speed}")
    if mark_price_speed not in SUPPORTED_MARK_PRICE_SPEEDS:
        raise ValueError(
            f"지원하지 않는 markPrice 갱신 속도입니다: {mark_price_speed}"
        )


def _stream_name(
    symbol: str,
    stream_type: StreamType,
    *,
    kline_interval: str,
    depth_speed: str,
    mark_price_speed: str,
) -> str:
    lowered = symbol.lower()
    if stream_type is StreamType.AGG_TRADE:
        return f"{lowered}@aggTrade"
    if stream_type is StreamType.DEPTH:
        return f"{lowered}@depth@{depth_speed}"
    if stream_type is StreamType.BOOK_TICKER:
        return f"{lowered}@bookTicker"
    if stream_type is StreamType.KLINE:
        return f"{lowered}@kline_{kline_interval}"
    if stream_type is StreamType.MARK_PRICE:
        return f"{lowered}@markPrice@{mark_price_speed}"
    raise ValueError(f"지원하지 않는 스트림입니다: {stream_type}")


def build_stream_specs(
    symbols: Sequence[str],
    *,
    kline_interval: str = "1m",
    depth_speed: str = "100ms",
    mark_price_speed: str = "1s",
) -> tuple[StreamSpec, ...]:
    """연결 그룹 순서대로 모든 symbol의 구독 명세를 만든다."""

    validate_stream_options(
        kline_interval=kline_interval,
        depth_speed=depth_speed,
        mark_price_speed=mark_price_speed,
    )
    specs: list[StreamSpec] = []
    for group in CONNECTION_GROUPS:
        for symbol in symbols:
            for stream_type in group.stream_types:
                specs.append(
                    StreamSpec(
                        market=group.market,
                        stream_type=stream_type,
                        symbol=symbol,
                        stream_name=_stream_name(
                            symbol,
                            stream_type,
                            kline_interval=kline_interval,
                            depth_speed=depth_speed,
                            mark_price_speed=mark_price_speed,
                        ),
                        collection=COLLECTION_BY_STREAM[stream_type],
                        connection_group=group.key,
                    )
                )
    return tuple(specs)


def group_stream_specs(
    specs: Iterable[StreamSpec],
) -> Mapping[str, tuple[StreamSpec, ...]]:
    grouped: dict[str, list[StreamSpec]] = defaultdict(list)
    for spec in specs:
        grouped[spec.connection_group].append(spec)
    return MappingProxyType(
        {key: tuple(grouped.get(key, ())) for key in CONNECTION_GROUP_BY_KEY}
    )


def index_stream_specs(
    specs: Iterable[StreamSpec],
) -> Mapping[tuple[str, str], StreamSpec]:
    index: dict[tuple[str, str], StreamSpec] = {}
    for spec in specs:
        key = (spec.connection_group, spec.stream_name)
        if key in index:
            raise ValueError(f"중복 스트림 명세입니다: {key}")
        index[key] = spec
    return MappingProxyType(index)


def collection_for_event_type(stream_type: str) -> str:
    try:
        return COLLECTION_BY_STREAM[StreamType(stream_type)]
    except ValueError:
        return CONTROL_COLLECTION
