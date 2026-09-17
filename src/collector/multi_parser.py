"""Combined 메시지를 출처와 스트림별 의미를 보존하는 v2 이벤트로 바꾼다."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.collector.parser import (
    FieldNameCollisionError,
    normalize_message,
    rename_fields,
)
from src.collector.streams import (
    CONNECTION_GROUP_BY_KEY,
    SCHEMA_VERSION,
    StreamSpec,
    StreamType,
)
from src.models.event import Event, NormalizedMessage, ReceivedMessage


COMMON_FIELD_NAMES = {
    "e": "event_type",
    "E": "event_time",
    "s": "symbol",
}
FIELD_NAMES_BY_STREAM: Mapping[StreamType, Mapping[str, str]] = {
    StreamType.AGG_TRADE: {
        **COMMON_FIELD_NAMES,
        "a": "trade_id",
        "p": "price",
        "q": "quantity",
        "f": "first_trade_id",
        "l": "last_trade_id",
        "T": "trade_time",
        "m": "is_buyer_maker",
    },
    StreamType.DEPTH: {
        **COMMON_FIELD_NAMES,
        "U": "first_update_id",
        "u": "final_update_id",
        "b": "bids",
        "a": "asks",
    },
    StreamType.BOOK_TICKER: {
        **COMMON_FIELD_NAMES,
        "u": "update_id",
        "b": "bid_price",
        "B": "bid_quantity",
        "a": "ask_price",
        "A": "ask_quantity",
    },
    StreamType.KLINE: COMMON_FIELD_NAMES,
    StreamType.MARK_PRICE: {
        **COMMON_FIELD_NAMES,
        "p": "mark_price",
        "i": "index_price",
        "P": "estimated_settle_price",
        "r": "funding_rate",
        "T": "next_funding_time",
    },
}


def _base_meta(message: ReceivedMessage) -> dict[str, Any]:
    group = CONNECTION_GROUP_BY_KEY.get(message.connection_group)
    return {
        "schema_version": SCHEMA_VERSION,
        "exchange": "binance",
        "market": group.market.value if group is not None else "unknown",
        "connection_group": message.connection_group,
        "received_at_ms": message.received_at_ms,
        "connection_id": message.connection_id,
        "receive_sequence": message.receive_sequence,
    }


def _control_event(
    message: ReceivedMessage,
    data: Event,
    *,
    warning: str | None = None,
    stream_name: str | None = None,
) -> NormalizedMessage:
    meta = _base_meta(message)
    meta["stream_type"] = "control"
    if stream_name is not None:
        meta["stream_name"] = stream_name
    if warning is not None:
        meta["warning"] = warning
    return NormalizedMessage(
        event={"meta": meta, "data": data},
        warning=warning,
    )


def normalize_received_message(
    message: ReceivedMessage,
    stream_index: Mapping[tuple[str, str], StreamSpec],
) -> NormalizedMessage:
    """수신 메시지를 v2 event로 만들고 알 수 없는 형식도 제어 이벤트로 보존한다."""

    decoded = normalize_message(message.payload)
    if decoded.warning is not None:
        return _control_event(
            message,
            decoded.event,
            warning=decoded.warning,
        )

    envelope = decoded.event
    stream_name = envelope.get("stream")
    payload = envelope.get("data")
    if not isinstance(stream_name, str) or not isinstance(payload, dict):
        return _control_event(message, envelope)

    spec = stream_index.get((message.connection_group, stream_name))
    if spec is None:
        return _control_event(
            message,
            envelope,
            warning="unknown_stream",
            stream_name=stream_name,
        )

    try:
        normalized_payload = rename_fields(
            payload,
            FIELD_NAMES_BY_STREAM[spec.stream_type],
        )
    except FieldNameCollisionError:
        raw_event: Event = {"raw_message": message.payload}
        return _control_event(
            message,
            raw_event,
            warning="field_name_collision",
            stream_name=stream_name,
        )

    meta = _base_meta(message)
    meta.update(
        {
            "market": spec.market.value,
            "stream_type": spec.stream_type.value,
            "stream_name": spec.stream_name,
        }
    )
    event: Event = {"meta": meta, "data": normalized_payload}
    transport = {
        name: value
        for name, value in envelope.items()
        if name not in {"stream", "data"}
    }
    if transport:
        event["transport"] = transport
    return NormalizedMessage(event=event)
