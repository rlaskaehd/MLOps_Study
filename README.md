# Binance 실시간 JSON 수집기

Binance Spot의 `BTCUSDT` 집계 체결 이벤트를 받아, 값은 바꾸지 않고 최상위 필드 이름만 표준화해 터미널에 JSON 한 줄씩 출력한다.

```text
Binance btcusdt@aggTrade → WebSocket 수신 → 필드 이름 표준화 → stdout
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

## 실행과 종료

macOS / Linux:

```bash
.venv/bin/python -u -m src.main
```

Windows PowerShell:

```powershell
.\.venv\Scripts\python.exe -u -m src.main
```

실행하면 stdout에 다음 형태의 JSON이 계속 출력된다. 아래 값은 형식 예시다.

```json
{"event_type":"aggTrade","event_time":1788783123456,"symbol":"BTCUSDT","trade_id":123456789,"price":"111234.50000000","quantity":"0.00420000","first_trade_id":987654320,"last_trade_id":987654322,"trade_time":1788783123450,"is_buyer_maker":true,"M":true}
```

`Ctrl+C`를 누르면 현재 연결과 비동기 작업을 정리한 뒤 종료한다. 연결, 재연결, 오류, 종료 로그는 stderr로 출력되므로 stdout의 JSON 형식을 깨뜨리지 않는다.

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

- 수신 메시지는 한 건씩 수신 순서대로 처리하고 즉시 flush한다.
- WebSocket 수신 버퍼의 상한은 16 프레임이다.
- 연결 종료 후 1, 2, 4, 8, 16, 30초 순서로 기다린 뒤 재연결한다. 이후 대기는 30초가 상한이다.
- 메시지 수신과 출력이 재개되면 재연결 대기 시간을 1초부터 다시 시작한다.
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

테스트는 필드·값 보존, 예외 원문 보존, JSON 한 줄 출력, 수신 순서, 재연결, 서버 종료 메시지, 출력 오류 전파, 종료 이벤트를 확인한다.

## 현재 검증 기록

2026-09-08에 macOS 26.6.2(arm64), Python 3.12.13, `websockets==17.0.1` 환경에서 다음 항목을 확인했다.

- 새 가상환경에 `requirements.txt` 설치 및 `pip check` 성공
- 자동화 테스트 18개 성공
- 실제 실행 명령으로 Binance에 60초간 연결
- stdout의 JSON 775건이 모두 `BTCUSDT` `aggTrade` 이벤트임을 확인
- 모든 실제 이벤트에서 11개 표준 필드가 존재하고 `price`, `quantity`가 문자열임을 확인
- SIGINT 후 종료 코드 0, stderr에는 데이터가 아닌 운영 로그만 2줄 출력

Python 3.11, Linux, Windows에서는 아직 직접 실행하지 않았다.

상세 구현 범위와 완료 기준은 [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md)를 따른다.
