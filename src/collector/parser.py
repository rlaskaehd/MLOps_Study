"""Binance 메시지의 값은 유지하고 최상위 필드 이름만 표준화한다."""

import base64
import json
from collections.abc import Mapping
from typing import Any

from src.models.event import NormalizedMessage


FIELD_NAMES = {
    "e": "event_type",
    "s": "symbol",
    "E": "event_time",
    "T": "trade_time",
    "a": "trade_id",
    "p": "price",
    "q": "quantity",
    "f": "first_trade_id",
    "l": "last_trade_id",
    "m": "is_buyer_maker",
}


class DuplicateKeyError(ValueError):
    """JSON 객체 안에 같은 키가 두 번 등장한 경우."""


class UnsupportedNumberError(ValueError):
    """표준 json 직렬화 과정에서 원문 정밀도를 잃을 수 있는 숫자."""


class FieldNameCollisionError(ValueError):
    """필드 이름을 바꾼 결과 기존 필드 이름과 충돌한 경우."""


def _pairs_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKeyError(key)
        result[key] = value
    return result


def _reject_floating_point_number(raw_value: str) -> Any:
    raise UnsupportedNumberError(raw_value)


def _reject_non_standard_number(raw_value: str) -> Any:
    raise UnsupportedNumberError(raw_value)


def _raw_text_message(message: str, warning: str) -> NormalizedMessage:
    return NormalizedMessage(event={"raw_message": message}, warning=warning)


def _raw_binary_message(message: bytes) -> NormalizedMessage:
    encoded = base64.b64encode(message).decode("ascii")
    return NormalizedMessage(
        event={"raw_message": encoded, "raw_encoding": "base64"},
        warning="binary_message",
    )


def rename_fields(
    payload: Mapping[str, Any],
    field_names: Mapping[str, str],
) -> dict[str, Any]:
    """값과 순서를 유지하면서 지정된 최상위 필드 이름만 바꾼다."""

    normalized: dict[str, Any] = {}
    for original_name, value in payload.items():
        normalized_name = field_names.get(original_name, original_name)
        if normalized_name in normalized:
            raise FieldNameCollisionError(normalized_name)
        normalized[normalized_name] = value
    return normalized


def _rename_fields(payload: dict[str, Any]) -> dict[str, Any]:
    return rename_fields(payload, FIELD_NAMES)


def normalize_message(message: str | bytes) -> NormalizedMessage:
    """수신 메시지를 손실 없는 JSON 출력 형태로 변환한다.

    정상적인 JSON 객체는 알려진 최상위 키의 이름만 바꾼다. 원문을
    안전하게 표현할 수 없는 입력은 버리지 않고 raw_message로 감싼다.
    """

    if isinstance(message, bytes):
        return _raw_binary_message(message)

    try:
        payload = json.loads(
            message,
            object_pairs_hook=_pairs_without_duplicates,
            parse_float=_reject_floating_point_number,
            parse_constant=_reject_non_standard_number,
        )
    except json.JSONDecodeError:
        return _raw_text_message(message, "invalid_json")
    except DuplicateKeyError:
        return _raw_text_message(message, "duplicate_key")
    except UnsupportedNumberError:
        return _raw_text_message(message, "unsupported_number")

    if not isinstance(payload, dict):
        return _raw_text_message(message, "non_object_json")

    try:
        normalized = _rename_fields(payload)
    except FieldNameCollisionError:
        return _raw_text_message(message, "field_name_collision")

    return NormalizedMessage(event=normalized)
