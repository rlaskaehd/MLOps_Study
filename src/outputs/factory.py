"""명령행 출력 이름을 실제 출력 객체로 변환한다."""

from typing import TextIO

from src.outputs.base import EventOutput
from src.outputs.stdout import StdoutOutput


def create_output(
    name: str | None,
    *,
    stream: TextIO | None = None,
) -> EventOutput | None:
    """지원되는 출력 이름에 맞는 객체를 만든다."""

    if name is None or name == "none":
        return None
    if name == "stdout":
        return StdoutOutput(stream)
    raise ValueError(f"지원하지 않는 출력 대상입니다: {name}")
