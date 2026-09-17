"""Binance aggTrade 다중 구독 요청과 응답 판별을 담당한다."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum

from src.collector.parser import normalize_message


SUBSCRIPTION_REQUEST_ID = 1


class SubscriptionResponseKind(Enum):
    SUCCESS = "success"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class SubscriptionResponse:
    kind: SubscriptionResponseKind
    detail: str | None = None


def agg_trade_streams(symbols: Sequence[str]) -> tuple[str, ...]:
    """표시 순서와 같은 순서로 Raw aggTrade 스트림 이름을 만든다."""

    return tuple(f"{symbol.lower()}@aggTrade" for symbol in symbols)


def build_subscribe_request(
    symbols: Sequence[str],
    *,
    request_id: int = SUBSCRIPTION_REQUEST_ID,
) -> str:
    """한 연결에서 여러 스트림을 구독하는 JSON 요청을 만든다."""

    return build_stream_subscribe_request(
        agg_trade_streams(symbols),
        request_id=request_id,
    )


def build_stream_subscribe_request(
    stream_names: Sequence[str],
    *,
    request_id: int = SUBSCRIPTION_REQUEST_ID,
) -> str:
    """이미 확정된 스트림 이름 목록으로 한 SUBSCRIBE 요청을 만든다."""

    if not stream_names:
        raise ValueError("구독할 스트림을 한 개 이상 지정해야 합니다.")
    payload = {
        "method": "SUBSCRIBE",
        "params": list(stream_names),
        "id": request_id,
    }
    return json.dumps(payload, separators=(",", ":"))


def parse_subscription_response(
    message: str | bytes,
    *,
    request_id: int = SUBSCRIPTION_REQUEST_ID,
) -> SubscriptionResponse | None:
    """현재 구독 요청에 해당하는 성공 또는 오류 응답만 판별한다."""

    normalized = normalize_message(message)
    if normalized.warning is not None:
        return None
    payload = normalized.event
    if not isinstance(payload, dict) or payload.get("id") != request_id:
        return None
    if "result" in payload and payload["result"] is None:
        return SubscriptionResponse(SubscriptionResponseKind.SUCCESS)
    if "code" in payload or "msg" in payload:
        detail = str(payload.get("msg") or payload.get("code") or "unknown")
        return SubscriptionResponse(SubscriptionResponseKind.ERROR, detail)
    return None
