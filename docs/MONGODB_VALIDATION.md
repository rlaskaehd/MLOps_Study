# MongoDB 1초 배치 적재 검증 기록

## 검증 대상

- 표준화된 모든 애플리케이션 메시지의 MongoDB 전달
- 제한된 메모리 큐와 1초 `insert_many()` 배치
- 이벤트 한 건당 MongoDB 문서 한 건
- 큐 접수와 실제 적재 확인 통계의 분리
- 연결·적재 오류 전파와 정상 종료 시 잔여 배치 처리

## 검증 환경

2026-09-08에 다음 환경에서 확인했다.

| 항목 | 값 |
|---|---|
| 운영체제 | macOS 26.6.2, arm64 |
| Python | 3.12.13 |
| MongoDB Server | 8.0.28, 로컬 `localhost:27017` |
| PyMongo | 4.18.0 (`dnspython==2.8.0` 포함) |
| 수집 대상 | 기본 10개 Binance `aggTrade` 심볼 |

## 자동화 검증

기본 테스트는 외부 서버가 없어도 실행할 수 있다.

```bash
.venv/bin/python -m unittest discover -s tests -v
```

65개 테스트가 성공하고 실제 MongoDB 테스트 1개는 기본 실행에서 건너뛴다. 가짜 클라이언트·컬렉션을 사용해 다음 항목을 검증한다.

- 이벤트 값, 중첩 구조, 문자열 타입, 중복과 제어 메시지 보존
- 1초 동안 받은 이벤트의 단일 배치 처리와 순서 유지
- 빈 구간의 쓰기 생략
- 큐 포화 시 다음 `write()` 대기
- 시작 연결 실패, 배치 쓰기 실패, 종료 제한 시간 초과
- 실패한 배치를 적재 성공으로 계수하지 않는 동작
- 새 이벤트가 없는 상태에서도 백그라운드 실패를 수집기에 전달하는 동작

실제 MongoDB 통합 테스트는 다음 명령으로 실행했다.

```bash
STUDYGROUP_MONGO_INTEGRATION=1 \
  .venv/bin/python -m unittest tests.test_mongodb_integration -v
```

고유한 `studygroup_integration_test.binance_events_test_<uuid>` 컬렉션에 구독 응답, 같은 거래 이벤트 두 건, 잘못된 JSON 원문을 전달했다. 총 4개 문서와 입력 순서, 표준 필드 이름, `price` 문자열, `raw_message`, 큐 접수 4건, 적재 확인 4건, 미확인 0건을 확인했다. 테스트 종료 후 컬렉션이 삭제된 것도 확인했다.

## 실제 Binance 수집 검증

전용 TUI 진입점으로 약 66초 실행한 뒤 `SIGINT`로 정상 종료했다. 프로세스 종료 코드는 0이었다. 운영 컬렉션과 분리한 고유 검증 컬렉션을 사용했다.

```bash
.venv/bin/python -u -m src.run_mongodb
```

MongoDB 조회 결과는 다음과 같다.

| 구분 | 문서 수 |
|---|---:|
| 전체 | 16,330 |
| `aggTrade` | 16,329 |
| 구독 응답 | 1 |

| 심볼 | 문서 수 |
|---|---:|
| BTCUSDT | 13,919 |
| ETHUSDT | 960 |
| BNBUSDT | 614 |
| XRPUSDT | 192 |
| DOGEUSDT | 170 |
| SOLUSDT | 167 |
| AVAXUSDT | 147 |
| LINKUSDT | 86 |
| LTCUSDT | 46 |
| ADAUSDT | 28 |

표본 문서의 `price`는 `"78488.01000000"`, `quantity`는 `"0.07000000"`이었고 BSON 타입은 둘 다 문자열이었다. 10개 기본 심볼이 모두 저장된 것을 확인했다. 검증이 끝난 뒤 고유 컬렉션을 삭제하고 같은 이름의 컬렉션이 남아 있지 않은 것도 확인했다. 사용자의 `studygroup.binance_events` 컬렉션은 이 검증에서 변경하지 않았다.

## 보장 범위

애플리케이션은 MongoDB의 성공 응답을 받은 문서만 적재 완료로 센다. 결과를 확인할 수 없는 배치는 자동으로 다시 보내지 않는다. 메모리 큐를 사용하므로 프로세스 강제 종료 뒤의 복구, 디스크 버퍼, 재처리, 연결 단절 구간 복원과 정확히 한 번 전달은 제공하지 않는다.
