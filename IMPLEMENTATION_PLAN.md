이 문서는 Binance 실시간 수집기의 구현 범위와 완료 기준을 확정한다. 구현 위치는 `/Users/ahh/playground/studygroup`이다. 아래 소스 코드, 의존성 파일, 테스트 및 사용 안내는 후속 구현의 산출물이다.

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
