이 문서는 Binance 실시간 수집기의 구현 범위와 완료 기준을 기록한다. 구현 위치는 `/Users/ahh/playground/studygroup`이다.

2026-09-08 기준 Phase 1–3은 `fe642c3`, `94c615a`, `1973cba`로 구현되었다. 아래 초기 계획은 해당 구현의 기준으로 보존한다. 문서 후반의 Phase 4–6은 다중 심볼, 수집 통계, TUI, 후속 데이터 전달 경로에 대한 확정 확장 계획이며 아직 구현하지 않았다. 확장 작업에서 수집 심볼·실행 모드·출력 연결에 대한 판단은 Phase 4–6의 계약을 우선한다.

목표는 각 참여자가 자신의 로컬에서 같은 버전의 의존성을 설치하고, 명령 하나로 수집기를 실행해 표준화된 JSON을 터미널에서 계속 확인하는 것이다.

```text
Binance BTCUSDT aggTrade
    → WebSocket 메시지 수신
    → 필드 이름 표준화
    → stdout에 메시지 한 건당 JSON 한 줄 출력
```

수집 대상은 Binance Spot의 `btcusdt@aggTrade` 단일 스트림이며, 접속 주소는 `wss://stream.binance.com:9443/ws/btcusdt@aggTrade`로 고정한다. 여기서 모든 데이터란 이 연결에서 수신한 모든 애플리케이션 메시지와 그 안의 모든 필드를 뜻한다. `aggTrade` 한 건은 집계 체결 이벤트다. [Binance 공식 스트림 명세](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#aggregate-trade-streams)

표준화는 최상위 필드 이름 변경과 JSON Lines 출력 형식 적용으로 한정한다.

| 항목 | 확정 기준 |
|---|---|
| 메시지 | 수신 순서대로 한 건씩 출력한다. 중복 이벤트도 그대로 출력한다. |
| 필드 | 원본 필드 전체를 보존한다. 매핑표에 없는 필드와 중첩 데이터도 그대로 유지한다. |
| 값·자료형 | 원본 JSON 값과 자료형을 유지한다. 가격·수량의 문자열을 숫자로 변환하지 않는다. |
| 시간 | 수신한 시각 값을 그대로 유지한다. 시간대 변경이나 Timestamp 변환을 하지 않는다. |
| 누락·이상값 | 필드 누락, null, 예상과 다른 값·자료형을 이유로 삭제하거나 보정하지 않는다. |
| 출력 | 완전한 JSON 객체 한 줄과 줄바꿈을 출력하고 즉시 flush한다. 들여쓰기·색상·배너를 넣지 않는다. |
| 로그 | 연결, 재연결, 오류, 종료 설명은 stderr에 기록한다. stdout에는 데이터만 기록한다. |

필드 이름은 다음과 같이 매핑한다. `M`은 원래 키와 값을 유지한다. `event_time`과 `trade_time`은 각각 `E`와 `T`이며, 둘을 합치거나 대체하지 않는다. [Binance 원본 필드 정의](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#aggregate-trade-streams)

| 원본 키 | 출력 키 | 원본 자료형 |
|---|---|---|
| `e` | `event_type` | 문자열 |
| `s` | `symbol` | 문자열 |
| `E` | `event_time` | 정수 |
| `T` | `trade_time` | 정수 |
| `a` | `trade_id` | 정수 |
| `p` | `price` | 문자열 |
| `q` | `quantity` | 문자열 |
| `f` | `first_trade_id` | 정수 |
| `l` | `last_trade_id` | 정수 |
| `m` | `is_buyer_maker` | 불리언 |
| `M` | `M` | 불리언 |

표의 자료형은 정상 메시지의 설명이며, 수신 데이터를 걸러내는 검증 조건으로 사용하지 않는다. 존재하는 필드만 이름을 바꾸며, 없는 필드를 생성하지 않는다.

정상 출력의 형식 예시는 다음과 같다. 실제 수신 결과를 제시한 것은 아니다.

```json
{"event_type":"aggTrade","symbol":"BTCUSDT","event_time":1788783123456,"trade_time":1788783123450,"trade_id":123456789,"price":"111234.50000000","quantity":"0.00420000","first_trade_id":987654320,"last_trade_id":987654322,"is_buyer_maker":true,"M":true}
```

JSON 객체로 해석할 수 없거나, 중복 키·변환 후 키 충돌 등으로 원문을 손실 없이 표준화하기 어려운 텍스트 메시지도 버리지 않는다. 이 경우 원문 전체를 다음 형식으로 출력하고 사유는 stderr에 남긴다. 예상하지 못한 숫자 표현도 반올림하거나 null로 바꾸는 대신, 손실 없이 보존할 수 없으면 이 방식을 적용한다.

```json
{"raw_message":"수신한 원문 전체"}
```

예상하지 못한 바이너리 메시지는 원본 바이트를 복원할 수 있도록 Base64로 감싸고 `raw_encoding: "base64"`를 함께 출력한다. 이는 데이터 선별이나 보정이 아닌, 모든 수신 메시지를 JSON으로 표현하기 위한 예외 출력 규칙이다.

프로젝트의 실행 의존성은 버전을 고정한 `requirements.txt`로 관리한다. Python은 3.11 이상을 지원 대상으로 명시하며, 공통 검증 기준은 Python 3.11로 둔다. 사용할 `websockets` 17.0.1은 Python 3.11 이상을 요구한다. [websockets 17.0.1 배포 정보](https://pypi.org/project/websockets/17.0.1/)

`requirements.txt`의 확정 내용은 다음과 같다.

```text
websockets==17.0.1
```

비동기 실행, JSON 처리, 로깅, 테스트에는 Python 표준 라이브러리인 `asyncio`, `json`, `logging`, `unittest` 등을 사용한다. 프로젝트 자체는 특정 Conda 환경 이름이나 활성화 명령을 요구하지 않는다. 각자 원하는 환경 관리 도구를 사용할 수 있으며, 공식 실행 안내는 프로젝트별 `.venv`를 생성하는 방식으로 제공한다. 가상환경의 Python을 직접 실행하면 활성화 명령 없이도 설치와 실행이 가능하다. [Python venv 문서](https://docs.python.org/3.11/library/venv.html)

예정 파일 구조는 다음과 같다. 이 트리의 루트가 실제 구현 위치이며, 별도의 `binance-collector` 하위 프로젝트를 만들지 않는다.

```text
/Users/ahh/playground/studygroup/
├── IMPLEMENTATION_PLAN.md
├── README.md
├── requirements.txt
├── .gitignore
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── collector/
│   │   ├── __init__.py
│   │   ├── client.py
│   │   ├── parser.py
│   │   └── reconnect.py
│   ├── models/
│   │   ├── __init__.py
│   │   └── event.py
│   └── outputs/
│       ├── __init__.py
│       └── stdout.py
└── tests/
    └── test_collector.py
```

아래 순서대로 구현한다. 산출물 열의 파일명은 위 트리의 파일과 대응한다.

| Phase · Step | 구현 내용 | 산출물 | 완료 기준 |
|---|---|---|---|
| Phase 1 · Step 1 — 실행 기반 | 패키지 구조와 고정 의존성을 정의하고, `.venv`와 Python 캐시를 Git 제외 대상으로 지정한다. | `requirements.txt`, `.gitignore`, 각 `__init__.py` | 새로운 가상환경에서 requirements 설치와 `pip check`가 성공한다. |
| Phase 1 · Step 2 — 표준화 계약 | `Event`는 임의 필드를 담는 딕셔너리 구조로 정의한다. 원본 JSON을 읽고 필드 이름만 매핑하며 예외 원문도 보존한다. | `event.py`, `parser.py` | 원본 전체 필드와 값·자료형이 보존된다. 누락·null·중복 이벤트가 제거되지 않는다. |
| Phase 1 · Step 3 — 출력 | `StdoutOutput.write(event)`가 한 건을 JSON 한 줄로 출력하고 flush하도록 한다. | `stdout.py` | 각 줄을 독립적인 JSON으로 읽을 수 있고, 로그가 섞이지 않는다. |
| Phase 2 · Step 1 — 수신 | `asyncio`와 `websockets`로 단일 스트림에 연결하고 수신한 메시지를 순서대로 전달한다. | `client.py` | 실제 Binance 메시지를 지속해서 수신한다. |
| Phase 2 · Step 2 — 연결 복구 | 연결 실패·종료의 재시도와 대기 정책을 구현한다. | `reconnect.py`, `client.py` | 일시적 연결 장애 뒤 프로세스를 다시 실행하지 않고 출력이 재개된다. |
| Phase 3 · Step 1 — 실행 통합 | 수신 → 표준화 → 출력을 연결하고 stderr 로그와 Ctrl+C 종료를 조정한다. | `main.py` | 한 명령으로 실행된다. 수신 중·재연결 대기 중 모두 Ctrl+C로 종료된다. |
| Phase 3 · Step 2 — 검증·안내 | 데이터 보존, 출력, 재연결, 종료를 검증하고 각 OS의 설치·실행 방법을 작성한다. | `test_collector.py`, `README.md` | 아래 완료 기준을 충족하고 실제 실행 결과와 미검증 환경이 구분되어 기록된다. |

실행 중 동작은 다음 기준으로 구현한다.

- 정상 메시지는 수신 → 표준화 → 출력 순서로 한 건씩 처리한다. 출력 주기를 만들거나 샘플링하지 않는다.
- WebSocket의 수신 버퍼는 유한하게 유지한다. 출력 지연을 이유로 이미 수신한 메시지를 버리는 정책을 넣지 않는다.
- 일시적 연결 실패·종료가 반복되면 1, 2, 4, 8, 16, 30초 순서로 대기한다. 30초를 상한으로 하고 정상 수신·출력이 재개되면 대기를 초기화한다.
- Ping/Pong은 WebSocket 라이브러리가 처리한다. Ping/Pong 프레임은 JSON 애플리케이션 메시지의 출력 대상에 포함되지 않는다.
- Binance의 24시간 연결 종료를 재연결 대상으로 처리한다. `serverShutdown` 애플리케이션 메시지도 먼저 출력한 다음 재연결한다. [Binance 연결 규칙](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#general-wss-information)
- Ctrl+C는 새 연결 시도를 중단하고, 진행 중인 작업과 연결을 정리한다.
- 출력 실패나 복구 불가능한 설정 오류는 stderr에 원인을 남기고 종료한다. 이를 네트워크 재연결로 숨기거나 데이터를 버리며 계속 실행하지 않는다.

재연결은 이후의 실시간 수신을 재개하는 기능이다. 연결 단절 구간의 거래 복구, 재시작 복원, 중복 제거, 체크포인트는 이번 범위에 포함하지 않는다. 메시지 수 일치 검증은 수집기가 수신한 메시지를 기준으로 하며, 거래소에서 발생한 모든 거래의 수집을 보장한다는 의미가 아니다.

설치·실행 안내는 구현 완료 후 다음 명령을 사용할 수 있도록 작성한다. 참여자는 각자 내려받은 프로젝트 루트에서 실행한다. `.venv` 디렉터리는 공유하지 않고 각자의 로컬에서 생성한다.

macOS / Linux:

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip check
.venv/bin/python -u -m src.main
```

Windows PowerShell:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -u -m src.main
```

위 명령은 공통 검증 기준인 Python 3.11을 사용한다. 다른 지원 버전을 사용하는 참여자는 가상환경 생성 시 자신의 Python 실행 파일을 지정한다. 의존성 버전은 동일한 `requirements.txt`를 따른다.

최종 완료 기준은 다음과 같다. 테스트 프레임워크는 `unittest`를 사용하며, 테스트용 메시지는 코드 안에 두고 stdout은 메모리에서 캡처한다.

| 검증 | 완료 기준 |
|---|---|
| 의존성 설치 | 새 가상환경에서 requirements 설치 및 `pip check`가 성공하고 고정한 패키지 버전이 확인된다. |
| 전체 필드 보존 | 알려진 11개 필드와 추가·중첩 필드가 이름 매핑 외의 값·자료형 변경 없이 보존된다. |
| 전처리 없음 | 누락, null, 예상과 다른 자료형, 반복 수신한 동일 이벤트가 보정·삭제되지 않는다. |
| 수량·순서 | 로컬 모의 연결에서 전달한 N개의 애플리케이션 메시지가 수신 순서대로 N개의 JSON 줄로 출력된다. |
| 예외 원문 보존 | 잘못된 JSON, 객체가 아닌 JSON, 중복 키, 키 충돌, 손실 위험이 있는 숫자 표현, 바이너리 메시지의 원문을 복원할 수 있다. |
| 로그 분리 | 연결·오류·종료 로그가 stderr에만 기록되고 stdout의 각 줄은 유효한 JSON이다. |
| 재연결 | 로컬 모의 연결에서 정상 종료·일시적 실패 후 재접속하며, 반복 실패 시 대기 상한이 적용된다. 서버 종료 안내는 출력 후 재연결한다. |
| 종료 | 수신 중과 재연결 대기 중의 Ctrl+C가 연결과 작업을 정리하고 종료한다. |
| 실제 수신 | 실제 Binance에 1분 이상 연결해 이벤트 출력이 지속되는 것을 확인한다. 모의 연결 검증과 별도로 결과를 기록한다. |
| 실행 환경 기록 | 실제 확인한 OS·Python·패키지 버전을 README에 기록한다. 실행하지 않은 환경을 검증 완료로 표시하지 않는다. |

파일 저장, JSONL 파일 관리, 소켓 서버, Kafka, Spark, 데이터레이크, 데이터베이스, 집계·전처리 로직은 스터디 참여자가 후속 학습에서 구현한다. 이번에는 `StdoutOutput.write(event)` 하나를 구현하고, 수신·표준화 코드에서 출력 책임을 분리해 후속 구현의 연결 지점만 제공한다.

---

Phase 4–6 확장 계획은 다음 내용으로 확정한다. 목적은 약 10개 심볼의 수집 상황을 TUI로 관찰하면서, 같은 실행 안에서 후속 적재 또는 소비 객체에 모든 표준 이벤트를 전달할 수 있게 하는 것이다.

수집 데이터의 필드·값·자료형 보존 규칙은 초기 계약을 따른다. 통계는 별도 메모리 상태에 기록하며 이벤트에 통계 필드를 추가하거나, 이벤트를 선별·보정·집계해 대체하지 않는다.

```text
Binance — 단일 WebSocket, 여러 aggTrade 구독
    ↓
모든 애플리케이션 메시지 수신
    ↓
원본 보존 표준화
    ↓
EventDispatcher
    ├─ StatsCollector에 수신 통계 기록
    │       └─ TUI가 통계 스냅샷을 1초마다 읽어 화면 갱신
    └─ EventOutput.write(event)를 순서대로 await
            ├─ 기본 제공: StdoutOutput
            └─ 후속 학습: 사용자 정의 적재·소비 출력 객체
```

TUI는 이벤트를 전달하는 주체가 아니다. `EventDispatcher`가 후속 출력을 호출하며, TUI 켜짐·꺼짐은 이 호출 경로에 영향을 주지 않는다. 인터페이스 선언뿐 아니라 실제 호출과 테스트까지 이번 확장의 산출물에 포함한다.

수집 설정과 구독 동작은 다음과 같이 확정한다.

| 항목 | 확정 내용 |
|---|---|
| 기본 심볼 | `BTCUSDT`, `ETHUSDT`, `BNBUSDT`, `SOLUSDT`, `XRPUSDT`, `DOGEUSDT`, `ADAUSDT`, `AVAXUSDT`, `LINKUSDT`, `LTCUSDT` |
| 사용자 설정 | `--symbols BTCUSDT ETHUSDT ...`로 기본 목록을 교체한다. 설정 목록의 순서를 표시 순서로 사용한다. |
| 스트림 | 각 심볼의 Spot `aggTrade`를 하나의 WebSocket 연결에서 구독한다. |
| 전송 형식 | Raw 형식으로 여러 스트림을 `SUBSCRIBE`해 기존의 평평한 이벤트 구조를 유지한다. |
| 재연결 | 새로운 연결마다 설정된 전체 심볼 목록을 다시 구독한다. 기존 재연결 대기 정책을 사용한다. |
| 상태 | 연결, 구독 확인, 재연결 대기, 오류를 구분한다. 심볼별 실제 수신 여부는 각 심볼의 수신 기록으로 판단한다. |

Binance의 여러 스트림 구독 요청과 요청 ID에 대응하는 응답을 처리한다. 구독 확인 전 도착한 이벤트도 정상 전달 경로로 보낸다. 구독 응답·서버 종료 안내·예외 원문도 보존해서 출력 객체에 전달하며, 거래 이벤트 통계와 구분한다. 구독 오류나 응답 대기 시간 초과를 정상 구독으로 표시하지 않는다. [Binance 다중 구독 및 Raw 형식](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#live-subscribingunsubscribing-to-streams)

기본 심볼 목록은 학습용 설정이다. 구현 시 실제 구독과 심볼별 수신을 확인하고, 수신이 없는 심볼은 0건으로 표시한다. 누락된 심볼을 임의로 다른 종목으로 교체하거나 모든 심볼 수신 검증을 통과한 것으로 기록하지 않는다.

후속 계층을 연결하는 공통 계약은 `src/outputs/base.py`의 `EventOutput`으로 정의한다.

```python
class EventOutput(Protocol):
    async def open(self) -> None: ...
    async def write(self, event: Event) -> None: ...
    async def close(self) -> None: ...
```

각 메서드의 책임은 다음과 같다.

| 메서드 | 책임 |
|---|---|
| `open()` | 수집 시작 전에 후속 연결을 준비한다. 실패하면 수집을 시작하지 않는다. |
| `write(event)` | 표준 이벤트 한 건을 적재하거나 소비 계층에 전달한다. 모든 원본 필드와 값을 받은 상태에서 후속 동작을 수행한다. |
| `close()` | 종료 시 출력 객체가 가진 연결과 자원을 정리한다. |

`src/main.py`의 공개 실행 함수는 `run_collector(symbols=..., mode="tui", output=custom_output)`처럼 출력 객체를 직접 주입할 수 있게 한다. CLI에서 기본 출력을 선택하는 코드는 `src/outputs/factory.py`에 둔다. 후속 학습자는 출력 클래스를 작성하고 이 구성 지점 또는 자신의 실행 스크립트에서 객체를 연결한다. 수집기·통계·TUI 코드를 바꾸지 않고 연결할 수 있어야 한다.

`write()`는 수신 순서대로 한 건씩 기다린다. 출력 객체는 전달받은 이벤트를 읽기 전용으로 취급하고, 자체 가공이 필요하면 자기 소유 복사본에서 처리한다. 통계는 출력 호출 전에 기록하므로 후속 출력이 원본 계수에 영향을 주지 않는다.

호출 결과와 장애 처리는 다음 기준을 따른다.

- `write()`가 성공한 뒤에만 후속 전달 완료 수를 증가시킨다. 이 수치는 해당 출력 객체가 정의한 성공을 뜻하며, 공통 수집기가 데이터베이스 영속 적재까지 보장한다고 표시하지 않는다.
- 후속 출력이 느리면 다음 메시지 전달도 기다린다. 이벤트를 버리는 큐, 무제한 버퍼, 중복을 만들 수 있는 자동 재전송을 추가하지 않는다.
- 출력 I/O는 이벤트 루프를 막지 않는 비동기 방식으로 구현한다. 기존 stdout의 동기 쓰기는 별도 실행 경로에서 순차 처리해, 후속 출력 대기 중에도 TUI와 연결 유지 작업이 실행되게 한다.
- 출력 실패는 수집 연결 오류와 구분한다. 원인을 상태에 반영하고 수집을 중단한 뒤 자원을 정리하며 오류 종료한다.
- Ctrl+C는 새 수신을 중단하고 진행 중인 전달과 출력 정리를 처리한다. 취소되거나 완료를 확인할 수 없는 전달은 성공으로 계수하지 않는다.
- 수집 연결이 복구되어도 통계와 후속 출력 객체는 유지한다. 프로그램을 새로 실행할 때 통계가 초기화된다.

이번 확장의 실제 데이터 출력은 한 객체만 연결하는 구조로 둔다. 파일·DB·Kafka 등의 구현체와 다중 목적지 전달 프레임워크는 후속 학습 범위다.

실행 모드와 데이터 출력 대상은 다음처럼 독립적으로 선택한다.

| 실행 조합 | 터미널 표시 | 후속 데이터 전달 |
|---|---|---|
| `--mode json` | 기존처럼 stdout JSON, stderr 운영 로그 | 기본 `StdoutOutput` |
| `--mode tui` | TUI만 표시 | 기본 미연결. 화면에 `후속 출력: 미연결` 표시 |
| `--mode tui --output stdout` | stderr의 TUI | stdout의 JSON을 다른 프로세스가 읽을 수 있음 |
| `run_collector(mode="tui", output=custom_output, ...)` | TUI | 주입한 적재·소비 객체에 모든 표준 이벤트 전달 |

인자 없는 실행은 `--mode json`이며 기본 10개 심볼을 수집한다. 단일 심볼은 `--symbols BTCUSDT`로 선택한다. `--mode tui`에서 출력 미연결은 명시적인 관찰 상태이며, 이벤트 이력을 저장하거나 나중에 재생할 수 있다는 의미가 아니다. 출력 객체가 연결되면 같은 TUI 실행 중에도 매 이벤트에 대해 `write()`를 호출한다.

TUI는 stderr의 실제 터미널에 표시한다. `--mode tui --output stdout`은 stdout이 파이프 등으로 터미널 밖에 연결된 경우에 사용한다. stdout도 같은 터미널에 연결되어 있으면 JSON과 TUI를 섞지 않고 실행 전에 설정 오류를 안내한다. TUI는 연결된 출력 객체를 몰래 비활성화하지 않는다. stderr가 터미널이 아니면 JSON 모드 사용을 안내한다.

통계는 프로세스의 단조 증가 시계를 기준으로 측정한다. 심볼별 막대와 합계는 같은 스냅샷에서 계산한다. 화면 갱신 주기는 1초이며 메시지마다 화면을 다시 그리지 않는다.

| 지표 | 정의 |
|---|---|
| 심볼별 초당 수집량 | 직전 완료된 1초 구간에 `recv()`가 반환한 메시지 중 해당 심볼의 `aggTrade` 수 |
| 전체 초당 수집량 | 동일 구간에 표시된 심볼별 `aggTrade` 수의 합계 |
| 동작 시간 | 프로그램 시작 이후 경과 시간. 재연결과 출력 대기도 포함 |
| 누적 수집량 | 실행 이후 표시 대상 심볼에서 수신한 `aggTrade` 수의 합계. 중복 이벤트도 포함 |
| 제어·미분류 수 | 구독 응답, 서버 안내, 심볼을 식별할 수 없는 원문 등. 거래 통계와 별도로 상태에 기록 |
| 후속 전달 완료 수 | 연결된 출력 객체의 `write()`가 성공한 전체 메시지 수. 제어·예외 메시지도 포함 |

심볼에 귀속되지 않는 메시지도 후속 출력 경로에는 전달한다. 표시 대상 심볼로 분류하는 동작은 통계용이며 데이터 필터로 사용하지 않는다. 수신이 없는 1초 구간은 0으로 표시하고, 재연결 중에는 상태와 함께 0건 구간을 표시한다. 전체 수집량은 거래 메시지 기준이고, 후속 전달 완료 수는 전체 메시지 기준임을 UI 라벨에서 구분한다.

TUI에는 고정된 심볼 순서의 가로 바차트와 정확한 `events/sec` 숫자를 표시한다. 같은 화면의 모든 바에는 같은 축을 적용한다. 아래에는 전체 초당 수집량, 동작 시간, 누적 수집량을 표시하고, 상태 행에는 연결·구독 상태, 출력 연결 여부, 후속 전달 완료 수, 제어·미분류 수, 최근 오류 한 건을 표시한다.

TUI 모드의 운영 로그는 고정 상태 영역에 반영한다. 원본 이벤트나 누적 로그가 화면을 밀어내지 않게 하고, 최근 오류 등 필요한 상태만 유한한 메모리에 보관한다. 종료할 때 화면과 커서를 복원하고, 오류 종료 시에는 복원 후 stderr에 오류 요약을 남긴다.

화면은 Rich `Live`로 구성한다. 기본 표준 출력·표준 오류 리다이렉션 동작이 데이터 경로를 가로채지 않도록 설정하고, TUI의 `Console`은 stderr를 사용한다. [Rich Live의 화면 갱신과 출력 리다이렉션](https://rich.readthedocs.io/en/stable/live.html)

`requirements.txt`에는 기존 `websockets==17.0.1`을 유지하고 `rich==14.1.0`을 추가한다. Rich의 간접 의존성도 새 가상환경에서 설치·검증한 버전을 `==`로 고정해 같은 파일에 기록한다. 이는 Phase 6의 산출물이며 현재 의존성 파일을 변경했다는 의미는 아니다. [Rich 14.1.0 배포 정보](https://pypi.org/project/rich/14.1.0/)

구현은 다음 Phase–Step 순서로 진행한다. 아래 산출물 경로는 모두 `/Users/ahh/playground/studygroup` 기준이다.

| Phase · Step | 구현 내용 | 산출물 | 완료 기준 |
|---|---|---|---|
| Phase 4 · Step 1 | 기본 10개 심볼과 `--symbols` 설정, 구독 요청 생성 | `src/config.py`, `src/collector/subscriptions.py`, `tests/test_subscriptions.py` | 설정 목록이 정확한 스트림 목록으로 바뀌며 단일 심볼도 설정 가능 |
| Phase 4 · Step 2 | 단일 연결의 다중 구독, 구독 응답과 재구독 처리 | `src/collector/client.py`, `src/main.py`, `tests/test_subscriptions.py`, `tests/test_collector.py` | ACK 전후의 데이터를 보존하고 재연결 때 전체 목록을 다시 구독. 오류 응답·시간 초과를 정상으로 표시하지 않음 |
| Phase 5 · Step 1 | 출력 공통 인터페이스와 실행 객체 주입, 공통 전달 경로 | `src/outputs/base.py`, `src/outputs/factory.py`, `src/outputs/stdout.py`, `src/pipeline.py`, `src/main.py`, `tests/test_pipeline.py` | 테스트용 비동기 출력 객체가 모든 메시지를 순서·값 보존 상태로 받고 open/write/close 실행이 확인됨 |
| Phase 5 · Step 2 | 수신·전달 통계와 일관된 화면 스냅샷 | `src/monitoring/__init__.py`, `src/monitoring/stats.py`, `src/pipeline.py`, `tests/test_stats.py` | 동일 1초 구간에서 심볼 합계와 전체가 일치. 빈 구간 0, 재연결 후 누적 유지, 전달 실패 시 성공 계수 증가 없음 |
| Phase 6 · Step 1 | TUI 렌더러, 상태 로그, 모드·출력 대상 구성 | `src/monitoring/tui.py`, `src/monitoring/logging.py`, `src/config.py`, `src/main.py`, `requirements.txt`, `tests/test_tui.py` | 10개 심볼과 하단 지표가 갱신되며 TUI가 출력 전달을 가로채지 않음. 모드별 스트림 분리와 잘못된 TTY 조합 안내 확인 |
| Phase 6 · Step 2 | 후속 출력과 TUI 동시 실행, 오류·종료·라이브 검증, 안내 | `tests/test_pipeline.py`, `tests/test_tui.py`, `README.md` | 아래 최종 완료 기준 충족. 후속 객체 연결 예제와 OS별 설치·실행 방법 및 검증 범위 기록 |

기존 `src/collector/parser.py`와 `src/models/event.py`의 데이터 보존 계약을 따른다. 모드 확장에 필요한 공개 함수 변경과 기존 테스트의 호환 수정은 위 Phase에 포함한다. 파일·DB 등 실제 저장 구현체는 만들지 않는다.

최종 완료 기준은 다음과 같다. 로컬 모의 검증과 실제 외부 연결 검증은 구분해서 기록한다.

| 검증 | 완료 기준 |
|---|---|
| 다중 심볼 수신 | 모의 연결에서 10개 심볼, 중복 이벤트, 추가 필드, 구독 응답, 예외 원문을 섞어 보내도 후속 출력이 전체를 순서대로 받음 |
| 구독 복구 | 접속 실패·연결 종료 이후 전체 심볼을 재구독하고 누적 통계와 출력 객체가 유지됨 |
| 원본 보존 회귀 | 기존 표준화 테스트 통과. 통계·TUI 사용이 필드·값·자료형·이벤트 수에 영향을 주지 않음 |
| 모드 독립성 | 같은 N개 메시지를 JSON 모드와 TUI 모드에서 같은 테스트용 출력에 주입했을 때 전달된 이벤트 목록이 동일함 |
| 통계 | 가짜 시계로 1초 경계, 빈 구간, 총합, 누적값, 재연결 대기 시간을 검증하고 거래 수와 전체 메시지 전달 수를 구분함 |
| 느린 후속 출력 | 의도적으로 write를 대기시켜도 화면·종료 요청 처리가 진행되며 순서가 바뀌거나 무제한 대기 큐가 생기지 않음 |
| 후속 실패 | write 실패 시 성공 계수가 증가하지 않고 오류 상태를 보인 뒤 정리·종료함. 자동 재전송이나 거짓 적재 성공 표시 없음 |
| TUI 스트림 분리 | TUI와 stdout 후속 출력을 함께 실행해도 후속 프로세스가 받는 각 줄은 순수 JSON이며 제어 문자·운영 로그가 섞이지 않음 |
| 종료·화면 | 정상 수신 중과 재연결·출력 대기 중 Ctrl+C의 정리 경로를 확인하고 종료 후 터미널 화면·커서가 복원됨 |
| 실제 확인 | 10개 심볼로 최소 60초 수집하며 심볼별 실제 수신 여부를 기록. TUI 단독 및 TUI와 메모리 테스트 출력의 동시 동작을 확인 |
| 의존성·안내 | 새로운 로컬 가상환경에서 고정 의존성 설치, pip check, 테스트 성공. 실제 확인한 OS·Python·터미널과 미검증 환경 명시 |

후속 출력 연결 테스트는 메모리 기반 테스트 구현체로 수행한다. 저장소·브로커를 만들지 않고도 TUI가 실행되는 동안 데이터가 다음 객체로 전달됨을 검증한다. 연결 단절 구간 복구, 디스크 버퍼, 재처리, 정확히 한 번 전달 보장은 이번 수집기 범위에 포함하지 않는다.

버전 관리는 각 Phase 검증 후 명시적인 파일 목록을 스테이징하고 `git diff --cached --check`를 통과한 뒤 독립 커밋을 만드는 방식으로 진행한다. 예정 커밋 경계는 Phase 4 다중 구독, Phase 5 출력 연결과 통계, Phase 6 TUI와 통합 검증이다. 이번 확정 단계에서는 계획 문서만 변경하며, 확장 코드는 후속 구현 단계에서 작성한다.
