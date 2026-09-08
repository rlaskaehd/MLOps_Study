# Binance 실시간 데이터를 활용한 Spark Structured Streaming 학습 프로젝트

## 1. 프로젝트 개요

이 프로젝트의 목적은 Binance의 실시간 거래 데이터를 이용해 Spark Structured Streaming의 핵심 개념을 단계적으로 학습하는 것이다.

초기에는 외부 시스템 접속에 필요한 최소 수집기만 AI의 도움으로 구현한다. 이후 데이터 처리, 집계, 상태 관리, 저장소 연동은 Spark 학습 진도에 맞춰 직접 구현한다. 따라서 이 프로젝트의 중심은 **수집기 개발이 아니라 Spark가 처리할 살아 있는 데이터 소스를 준비하고, 그 처리 파이프라인을 직접 진화시키는 것**이다.

초기 목표는 다음 네 단계로 제한한다.

```text
Binance WebSocket 연결
        ↓
실시간 이벤트 수신
        ↓
최소 공통 포맷으로 변환
        ↓
터미널에 JSON Lines 출력
```

## 2. 핵심 원칙

### 2.1 수집기와 처리기의 책임을 분리한다

수집기는 Binance WebSocket과 내부 데이터 처리 시스템 사이의 얇은 연결 계층이다. 데이터를 받아 공통 이벤트 형식으로 바꾸고 다음 시스템으로 전달하는 데까지만 책임진다.

수집기가 담당하는 범위는 다음과 같다.

- WebSocket 연결
- 연결 종료 시 자동 재연결
- 메시지 수신
- JSON 디코딩
- 최소한의 공통 이벤트 형식 변환
- 출력 계층으로 이벤트 전달
- 기본 로그 기록
- 종료 신호 수신 시 안전한 종료

수집기에서 담당하지 않는 범위는 다음과 같다.

- 데이터 집계
- OHLCV 계산
- 이동평균 등 파생 변수 생성
- 데이터베이스 저장
- 복잡한 비즈니스 검증
- 머신러닝
- Spark 처리 로직

이 경계를 지키면 Spark를 도입한 뒤에도 수집기와 처리기의 역할이 겹치지 않는다.

### 2.2 수집기는 유지하고 출력 방식만 교체한다

수집기 내부에서는 출력 대상을 직접 결정하지 않는다. 수집한 이벤트를 출력 인터페이스에 전달하고, 학습 단계에 따라 구현체만 바꾼다.

```python
raw_event = collector.receive()
event = parser.parse(raw_event)
output.write(event)
```

초기에는 터미널 출력 구현체를 사용한다.

```python
output = StdoutOutput()
```

이후에는 파일, 소켓, Kafka 출력 구현체를 추가할 수 있다.

```python
output = FileOutput()
output = SocketOutput()
output = KafkaOutput()
```

핵심은 출력 대상이 바뀌어도 WebSocket 연결과 이벤트 파싱 코드는 그대로 유지하는 것이다.

### 2.3 학습하지 않은 기술을 미리 구현하지 않는다

Kafka, 데이터베이스, 복잡한 운영 환경을 초기에 한꺼번에 만들지 않는다. 각 기술을 학습할 때 기존 시스템의 한 부분을 직접 교체하거나 확장한다.

```text
stdout → JSONL/Socket → Kafka
Console → Parquet → PostgreSQL
단순 집계 → Window → Watermark → Feature Engineering
```

이 방식은 이미 생성된 코드를 역으로 해석하는 대신, 학습한 개념을 기존 시스템에 직접 적용하게 해준다.

## 3. 단계별 아키텍처

### 3.1 MVP 0 — 실시간 수집기

```text
Binance
   ↓
Python Collector
   ↓
stdout
```

Binance의 `BTCUSDT` 집계 체결 스트림을 수신하고, 표준화한 이벤트를 터미널에 JSON Lines 형식으로 출력한다.

### 3.2 MVP 1 — Spark Structured Streaming

```text
Binance
   ↓
Python Collector
   ↓
JSONL 또는 Socket
   ↓
Spark Structured Streaming
   ↓
Console Sink
```

수집기의 출력 대상을 Spark가 읽을 수 있는 JSON Lines 파일 또는 소켓으로 교체한다. Spark는 스키마 적용, 시간 변환, 윈도우 집계, 워터마크, 체크포인트를 담당한다.

### 3.3 MVP 2 — 저장소

```text
Binance
   ↓
Collector
   ↓
Spark Structured Streaming
   ├── Raw Event → Parquet
   └── Aggregated Event → PostgreSQL
```

거래 단위 원본 이벤트는 Spark와 궁합이 좋은 Parquet에 저장하고, 조회와 활용이 잦은 집계 결과는 PostgreSQL에 저장한다.

### 3.4 MVP 3 — Kafka

```text
Binance
   ↓
Collector
   ↓
Kafka
   ↓
Spark Structured Streaming
   ↓
Parquet / PostgreSQL
```

Kafka를 학습한 뒤 수집기의 출력 구현체를 Kafka로 교체한다. Spark의 스키마, 윈도우, 워터마크, 집계 로직은 재사용한다.

## 4. MVP 0 상세 명세

### 4.1 대상

- 거래 심볼: `BTCUSDT`
- Binance 스트림: `aggTrade`
- 실행 방식: Python `asyncio` 기반 비동기 처리
- 출력 방식: 표준 출력의 JSON Lines

### 4.2 이벤트 스키마

수집기는 Binance 원본 메시지 전체를 비즈니스 데이터로 가공하지 않는다. Spark가 안정적으로 읽을 수 있도록 필요한 필드만 공통 이벤트 형식으로 변환한다.

| 필드 | 의미 | 예시 |
|---|---|---|
| `event_type` | 이벤트 유형 | `aggTrade` |
| `symbol` | 거래 심볼 | `BTCUSDT` |
| `event_time` | 이벤트 발생 시각(Unix milliseconds) | `1788783123456` |
| `trade_id` | 집계 거래 식별자 | `123456789` |
| `price` | 체결 가격 | `111234.50` |
| `quantity` | 체결 수량 | `0.0042` |

출력 예시는 다음과 같다.

```json
{"event_type":"aggTrade","symbol":"BTCUSDT","event_time":1788783123456,"price":111234.50,"quantity":0.0042,"trade_id":123456789}
```

각 줄은 하나의 완전한 JSON 객체여야 한다. 로그는 데이터 출력과 섞이지 않도록 표준 오류 또는 별도 로거로 분리하는 편이 좋다.

### 4.3 권장 모듈 구조

```text
src/
├── collector/
│   ├── client.py       # WebSocket 연결과 메시지 수신
│   ├── parser.py       # Binance 메시지를 내부 Event로 변환
│   └── reconnect.py    # 재연결 정책
├── outputs/
│   └── stdout.py       # JSON Lines 표준 출력
├── models/
│   └── event.py        # 공통 Event 구조
└── main.py             # 실행 흐름과 graceful shutdown 조정
```

향후에는 기존 모듈을 수정하기보다 출력 구현체만 추가한다.

```text
outputs/
├── stdout.py
├── file.py
├── socket.py
└── kafka.py
```

### 4.4 완료 기준

MVP 0은 다음 조건을 모두 만족하면 완료한다.

- `BTCUSDT`의 `aggTrade` 이벤트를 지속해서 수신한다.
- 수신 메시지를 정의된 여섯 개 필드로 변환한다.
- 이벤트 한 건을 JSON 한 줄로 출력한다.
- 일시적인 연결 종료 후 자동으로 재연결한다.
- `SIGINT` 수신 시 실행 중인 연결과 작업을 정리하고 종료한다.
- 로그와 이벤트 데이터가 서로의 형식을 깨뜨리지 않는다.
- Kafka, Spark, 데이터베이스, 집계 로직을 포함하지 않는다.

## 5. MVP 1: Spark 학습 범위

Spark를 WebSocket에 직접 연결하기보다 수집기의 출력을 Spark 입력 소스로 교체한다. Spark Structured Streaming은 WebSocket 연결 자체보다 파일, 소켓, Kafka와 같은 스트리밍 소스를 소비하고 처리하는 역할에 집중한다.

첫 번째 Spark 파이프라인은 다음 순서로 구현한다.

```text
aggTrade JSON
    ↓
명시적 Schema 적용
    ↓
event_time을 Timestamp로 변환
    ↓
1분 Tumbling Window 생성
    ↓
symbol과 window 기준으로 그룹화
    ↓
OHLCV와 거래 건수 집계
    ↓
Console Sink 출력
```

목표 결과는 다음과 같다.

```text
BTCUSDT | 21:00:00 ~ 21:01:00

open         111,220
high         111,310
low          111,180
close        111,290
volume        17.821
trade_count    4,321
```

기본 윈도우 집계가 안정적으로 동작한 뒤 워터마크를 추가한다.

```python
.withWatermark("event_time", "10 seconds")
```

이 단계에서 직접 확인할 핵심 개념은 다음과 같다.

- Event Time과 Processing Time의 차이
- 고정 길이 Tumbling Window
- 상태를 유지하는 집계
- 지연 도착 데이터
- Watermark와 상태 정리 기준
- 장애 복구를 위한 Checkpoint

### 완료 기준

- 입력 JSON에 명시적 스키마를 적용한다.
- 밀리초 단위 `event_time`을 Spark Timestamp로 변환한다.
- 심볼별 1분 Tumbling Window를 생성한다.
- 각 윈도우의 시가, 고가, 저가, 종가, 거래량, 거래 건수를 계산한다.
- 10초 워터마크를 적용하고 지연 데이터 처리 결과를 관찰한다.
- 체크포인트를 설정한 뒤 재시작 시 처리 상태가 복구되는지 확인한다.

## 6. MVP 2: 저장소 구성

저장소는 데이터 성격에 따라 분리한다.

| 데이터 | 저장소 | 이유 |
|---|---|---|
| 거래 단위 원본 이벤트 | Parquet | 대량 적재, Spark 재처리, 파일 기반 분석에 적합 |
| 1분 단위 집계 이벤트 | PostgreSQL | 조회, 조건 검색, 애플리케이션 연동에 적합 |

초기 논리 데이터셋은 두 개면 충분하다.

- `market_trade_raw`: 표준화된 거래 단위 원본 이벤트
- `market_ohlcv_1m`: 심볼별 1분 OHLCV 집계 결과

원본 거래 전체를 PostgreSQL에 계속 적재하는 설계는 초기 범위에서 제외한다. 원본은 Parquet으로 보존하고 PostgreSQL에는 필요한 집계 결과만 저장해 각 저장소의 역할을 명확히 한다.

### 완료 기준

- 표준화된 원본 이벤트가 Parquet에 누락 없이 저장된다.
- 1분 집계 결과가 PostgreSQL에 저장된다.
- 재실행 시 체크포인트와 저장 결과의 중복 여부를 확인한다.
- 원본과 집계 결과의 저장 책임이 섞이지 않는다.

## 7. MVP 3: Kafka 도입

Kafka는 관련 개념을 학습한 뒤 도입한다. 이 단계의 목표는 새로운 전체 시스템을 만드는 것이 아니라, 기존 수집기와 Spark 사이의 입력 인터페이스를 교체하는 것이다.

변경 범위는 다음과 같다.

- 수집기에 `KafkaOutput`을 추가한다.
- Spark 입력 소스를 JSONL 또는 Socket에서 Kafka로 변경한다.
- 기존 Event 스키마를 유지한다.
- 기존 윈도우, 워터마크, 집계, 저장 로직을 재사용한다.

### 완료 기준

- 수집기가 표준 이벤트를 Kafka 토픽에 발행한다.
- Spark가 같은 이벤트 스키마로 Kafka 메시지를 읽는다.
- 입력 소스 변경 후에도 기존 1분 집계 결과가 유지된다.
- 수집, 전달, 처리, 저장의 책임 경계가 유지된다.

## 8. 단계별 범위 요약

| 단계 | 직접 만드는 핵심 | 주요 산출물 | 제외 범위 |
|---|---|---|---|
| MVP 0 | 외부 데이터 접속부 | 비동기 수집기, 공통 Event, stdout 출력 | Spark, Kafka, DB, 집계 |
| MVP 1 | 스트리밍 처리 | Schema, Window, Watermark, Checkpoint, OHLCV | Kafka, 복잡한 저장 구조 |
| MVP 2 | 데이터 저장 | Raw Parquet, 집계 PostgreSQL | 운영용 데이터 플랫폼 |
| MVP 3 | 메시지 전달 계층 | Collector → Kafka → Spark | 새로운 처리 로직 재작성 |

## 9. MVP 0 초기 구현 제외 범위

초기 구현에는 다음 항목을 포함하지 않는다.

- Kafka
- Spark
- PostgreSQL 또는 기타 데이터베이스
- Parquet 저장
- OHLCV를 포함한 모든 집계
- Feature Engineering
- 머신러닝
- Airflow
- Docker 기반 오케스트레이션
- 운영 수준의 모니터링과 배포 자동화

이 항목들은 불필요해서 제외하는 것이 아니라, 각 학습 단계에서 직접 구현하기 위해 의도적으로 뒤로 미룬다.

## 10. 최종 방향

이 프로젝트에서 AI 지원은 학습의 중심이 아닌 외부 시스템 접속부를 빠르게 준비하는 데 사용한다. Binance WebSocket 연결, 재연결, 최소 파싱, 안전한 종료까지는 수집기의 기반 기능으로 만든다. 이후 Spark의 스키마, 시간 처리, 윈도우, 상태, 지연 데이터, 워터마크, 체크포인트는 직접 구현하고 검증한다.

결과적으로 프로젝트는 다음 순서로 성장한다.

```text
살아 있는 데이터 소스 확보
        ↓
Spark 기본 스트리밍 처리
        ↓
이벤트 시간과 상태 처리
        ↓
원본 및 집계 데이터 저장
        ↓
Kafka 기반 입력으로 교체
```

각 단계에서는 앞선 결과물을 버리지 않고 입력 또는 출력 인터페이스만 교체한다. 이를 통해 작은 범위로 시작하면서도 Spark Structured Streaming의 핵심 개념을 실제 데이터로 끝까지 학습할 수 있다.
