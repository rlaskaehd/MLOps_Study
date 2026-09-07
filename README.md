# Binance 다중 심볼 실시간 수집기

Binance Spot의 10개 심볼 `aggTrade` 이벤트를 한 WebSocket에서 받아, 값은 바꾸지 않고 최상위 필드 이름만 표준화한다. JSON 모드에서는 이벤트를 stdout으로 전달하고, TUI 모드에서는 심볼별 수집 상황을 보면서 별도의 적재·소비 객체를 연결할 수 있다.

```text
Binance 다중 aggTrade 구독 → 표준화 → 수집 통계 → TUI
                                  └→ EventOutput → 후속 적재·소비 계층
```

## 지원 환경

- Python 3.11 이상
- 인터넷에 연결할 수 있는 macOS, Linux 또는 Windows
- Binance 공개 WebSocket 접속이 허용되는 네트워크와 지역

프로젝트 실행 의존성은 `requirements.txt`에 고정되어 있다. 저장소의 가상환경을 공유하지 않고 각 참여자가 자신의 로컬에서 만든다.

## 설치

저장소 루트에서 아래 명령을 실행한다.

macOS / Linux:

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip check
```

Windows PowerShell:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip check
```

Python 3.12 이상을 사용하는 경우 첫 번째 명령에서 자신의 Python 실행 파일을 지정한다. 모든 참여자는 같은 `requirements.txt`를 설치한다.

## JSON 모드

macOS / Linux:

```bash
.venv/bin/python -u -m src.main
```

Windows PowerShell:

```powershell
.\.venv\Scripts\python.exe -u -m src.main
```

인자를 생략하면 다음 10개 심볼을 구독한다.

```text
BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT
DOGEUSDT ADAUSDT AVAXUSDT LINKUSDT LTCUSDT
```

심볼 목록을 직접 지정하면 기본 목록을 교체한다. 단일 심볼도 같은 방식으로 실행한다.

```bash
.venv/bin/python -u -m src.main --symbols BTCUSDT ETHUSDT
.venv/bin/python -u -m src.main --symbols BTCUSDT
```

stdout에는 구독 응답을 포함해 Binance가 보낸 모든 애플리케이션 메시지가 수신 순서대로 JSON 한 줄씩 출력된다. 다음 값은 거래 이벤트의 형식 예시다.

```json
{"event_type":"aggTrade","event_time":1788783123456,"symbol":"BTCUSDT","trade_id":123456789,"price":"111234.50000000","quantity":"0.00420000","first_trade_id":987654320,"last_trade_id":987654322,"trade_time":1788783123450,"is_buyer_maker":true,"M":true}
```

연결, 재연결, 오류, 종료 로그는 stderr로 출력되므로 stdout의 JSON을 깨뜨리지 않는다.

## TUI 모드

```bash
.venv/bin/python -m src.main --mode tui
```

TUI에는 각 심볼의 직전 1초 `events/sec`를 같은 축의 가로 막대로 표시한다. 아래에는 전체 초당 수집량, 동작 시간, 누적 거래 이벤트 수가 나오며, 상태 행에는 연결·구독 상태, 후속 출력 연결 여부, 전달 완료 수, 제어·미분류 수와 최근 오류를 표시한다.

이 명령은 기본적으로 후속 출력이 연결되지 않은 관찰 모드다. 화면의 `후속 출력: 미연결`은 수신 이벤트가 저장되거나 재생되지 않는다는 뜻이다.

TUI를 보면서 stdout을 후속 프로세스에 연결하려면 stdout을 파이프로 보내고 출력 대상을 명시한다.

```bash
.venv/bin/python -u -m src.main --mode tui --output stdout | your-consumer
```

TUI는 stderr 터미널에 남고, 파이프에는 제어 문자나 로그가 없는 JSONL만 전달된다. stderr가 터미널이 아니거나 TUI와 JSON을 같은 터미널에 쓰는 조합은 시작 전에 오류로 종료한다.

`Ctrl+C`를 누르면 새 수신을 중단하고 진행 중인 전달과 출력 자원을 정리한 뒤 TUI 화면과 커서를 복원한다. 완료되지 않은 전달은 성공 건수로 기록하지 않는다.

## 후속 적재·소비 객체 연결

후속 계층은 [src/outputs/base.py](src/outputs/base.py)의 비동기 `EventOutput` 계약을 구현한다.

```python
class EventOutput(Protocol):
    async def open(self) -> None: ...
    async def write(self, event: Event) -> None: ...
    async def close(self) -> None: ...
```

구현한 객체는 수집 함수에 직접 주입할 수 있다.

```python
import asyncio

from src.main import run_collector


class MyOutput:
    async def open(self) -> None:
        ...

    async def write(self, event: dict) -> None:
        # 파일, 메시지 브로커, 데이터베이스 등 학습자가 구현할 부분
        ...

    async def close(self) -> None:
        ...


asyncio.run(run_collector(mode="tui", output=MyOutput()))
```

수집기는 `write()`를 한 건씩 수신 순서대로 기다린다. 통계는 호출 전에 기록하고, `write()`가 반환된 뒤에만 전달 완료 수를 증가시킨다. 출력 실패는 네트워크 재연결로 숨기거나 자동 재전송하지 않고 수집을 오류 종료한다. 실제 저장 성공의 의미, 재처리, 버퍼, 중복 처리와 체크포인트는 각 출력 구현의 책임이다.

## 표준화 규칙

수집기는 다음 최상위 필드의 이름만 바꾼다.

| Binance 키 | 출력 키 |
|---|---|
| `e` | `event_type` |
| `s` | `symbol` |
| `E` | `event_time` |
| `T` | `trade_time` |
| `a` | `trade_id` |
| `p` | `price` |
| `q` | `quantity` |
| `f` | `first_trade_id` |
| `l` | `last_trade_id` |
| `m` | `is_buyer_maker` |

`M`과 매핑되지 않은 필드는 원래 이름으로 남는다. 중첩 필드를 포함한 모든 값과 자료형을 유지하고, 없는 필드를 만들지 않는다. 따라서 가격과 수량도 Binance가 보낸 문자열 그대로 출력한다.

아래 처리는 수행하지 않는다.

- 필터링, 샘플링 또는 중복 제거
- null이나 누락 필드의 보정
- 문자열을 숫자나 Timestamp로 변환
- 집계, 이상치 제거 또는 파생 변수 생성
- 파일, Kafka, Spark, 데이터레이크 또는 데이터베이스 저장

JSON 객체로 안전하게 표준화할 수 없는 텍스트 메시지는 원문을 `raw_message`에 담는다. 바이너리 메시지는 Base64 문자열과 `"raw_encoding":"base64"`를 출력한다. 어느 경우에도 수신 메시지를 조용히 버리지 않는다.

## 연결 동작

- 설정된 모든 심볼을 한 Raw WebSocket 연결에서 `SUBSCRIBE`하고, 새 연결마다 전체 목록을 다시 구독한다.
- 구독 응답, 거래 이벤트, 서버 안내와 예외 원문을 모두 같은 전달 경로로 보낸다.
- 수신 메시지는 한 건씩 수신 순서대로 처리하고 stdout 출력은 즉시 flush한다.
- WebSocket 수신 버퍼의 상한은 16 프레임이다.
- 연결 종료 후 1, 2, 4, 8, 16, 30초 순서로 기다린 뒤 재연결한다. 이후 대기는 30초가 상한이다.
- 메시지 수신과 출력이 재개되면 재연결 대기 시간을 1초부터 다시 시작한다.
- 구독 거절이나 10초 안에 확인 응답이 없으면 정상 구독으로 표시하지 않고 재연결한다.
- `serverShutdown` 메시지는 먼저 출력하고 재연결한다.
- 연결이 끊긴 동안의 거래 복구, 체크포인트, 재시작 복원 및 중복 제거는 제공하지 않는다.

## 테스트

macOS / Linux:

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Windows PowerShell:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

테스트는 필드·값 보존, 다중 구독과 재구독, 1초 통계, JSON/TUI 모드의 데이터 동일성, 비동기 출력 생명주기, 느린 출력, 출력 오류, TUI 스트림 분리와 종료 이벤트를 확인한다.

## 현재 검증 기록

2026-09-08에 macOS 26.6.2(arm64), Python 3.12.13 환경에서 다음 항목을 확인했다.

- 새 가상환경에 고정된 5개 패키지 설치 및 `pip check` 성공
- 자동화 테스트 47개 성공
- 실제 TUI와 사용자 정의 메모리 출력 객체를 동시에 연결해 65초 수집 요청 수행
- 종료 정리까지 74.431초 동안 거래 이벤트 1,297건과 구독 응답 1건을 수신하고, 전체 1,298건이 출력 객체에 전달됨
- `BTCUSDT` 437, `ETHUSDT` 230, `BNBUSDT` 168, `SOLUSDT` 86, `XRPUSDT` 38, `DOGEUSDT` 39, `ADAUSDT` 18, `AVAXUSDT` 57, `LINKUSDT` 92, `LTCUSDT` 132건으로 10개 심볼 모두 실제 수신 확인
- 사용자 정의 출력의 `open()`과 `close()` 실행, 전달 완료 수 1,298, 최근 오류 없음 확인
- 후속 출력 없는 실제 TUI 단독 모드에서 SIGINT 후 종료 코드 0과 화면 복원 확인

Python 3.11, Linux, Windows에서는 아직 직접 실행하지 않았다. `requirements.txt`의 현재 고정 버전은 `websockets==17.0.1`, `rich==14.1.0`, `markdown-it-py==4.2.0`, `mdurl==0.1.2`, `Pygments==2.21.0`이다.

상세 구현 범위와 완료 기준은 [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md)를 따른다.
