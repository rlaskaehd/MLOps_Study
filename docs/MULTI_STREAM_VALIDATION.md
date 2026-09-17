# 다중 스트림 확장 검증 기록

검증일은 2026-09-17이며 작업 경로는 `/Users/ahh/Sandbox/finance/collector`다. 구현 범위와 장시간 검증 계획은 [MULTI_STREAM_EXPANSION_PLAN.md](MULTI_STREAM_EXPANSION_PLAN.md)를 따른다.

## 환경

| 항목 | 확인값 |
|---|---|
| 운영체제 | macOS Darwin 25.6.0, arm64 |
| Python | Conda `sandbox`, Python 3.12.13 |
| MongoDB | 8.0.28 |
| PyMongo | 4.18.0 |
| websockets | 17.0.1 |

## 자동화 검증

다음 명령으로 설정, 5종 표준화, 세 연결 그룹, MongoDB 라우팅·버퍼·부분 성공, 통계·TUI, 실행 생명주기와 기존 profile 회귀를 확인했다.

```bash
conda activate sandbox
python -m pip check
python -m unittest discover -s tests -v
```

- `pip check`: 손상된 의존성 없음
- 전체 자동화 테스트: 99개 중 97개 성공, 기존 실제 MongoDB 선택 테스트 1개와 신규 다중 컬렉션 선택 테스트 1개는 기본 실행에서 제외
- `python -m src.run_mongodb --help`: `legacy`, `multi-stream` profile과 세 스트림 옵션 노출 확인
- Python 소스 및 테스트 `compileall`: 성공

실제 MongoDB 통합 테스트는 다음 명령으로 별도 실행했다.

```bash
conda activate sandbox
STUDYGROUP_MONGO_MULTI_INTEGRATION=1 \
  python -m unittest tests.test_mongodb_multi_integration -v
```

6개 고유 임시 컬렉션에 스트림별 문서를 한 건씩 저장하고 원본 문자열 타입과 문서 수를 확인했다. 검증 컬렉션은 테스트 종료 시 삭제했다.

## 실제 Binance 상품과 WebSocket 검증

공개 Spot 및 USDⓈ-M exchangeInfo를 조회해 기본 10개 symbol 모두가 Spot `TRADING` 상태이고 USDⓈ-M `PERPETUAL`, `USDT`, `TRADING` 조건을 충족함을 확인했다.

MongoDB를 연결하지 않은 20초 메모리 검증에서는 세 연결 모두 구독 ACK를 받았으며 결과는 다음과 같았다.

| 스트림 | 수신 문서 | 수신이 확인된 symbol 수 |
|---|---:|---:|
| `aggTrade` | 333 | 10 |
| `depth` | 1,289 | 10 |
| `bookTicker` | 4,111 | 10 |
| `kline` | 70 | 10 |
| `markPrice` | 200 | 10 |
| 연결 제어 ACK | 3 | 해당 없음 |
| 합계 | 6,006 | 5종 모두 기본 10개 확인 |

연결 그룹별 수신은 `spot_market` 4,515건, `spot_depth` 1,290건, `usdm_mark` 201건이었다. 종료 요청 뒤 세 WebSocket context가 정리되고 프로세스가 정상 종료됐다.

## 실제 MongoDB sink 검증

운영 컬렉션과 충돌하지 않는 `codex_probe_<고유값>_` 접두사의 6개 임시 컬렉션을 사용했다. 기본 10개 symbol과 50개 스트림을 12초 동안 실제 수집한 결과는 다음과 같다.

| 컬렉션 | 적재 확인 문서 |
|---|---:|
| `agg_trades` | 110 |
| `order_book_depth` | 670 |
| `book_tickers` | 1,453 |
| `klines` | 28 |
| `mark_prices` | 120 |
| `collector_control` | 3 |
| 합계 | 2,384 |

수신 2,384건, 출력 큐 접수 2,384건, MongoDB 적재 확인 2,384건이 일치했고 종료 후 미확인은 0건이었다. 12초 구간에서는 `aggTrade`와 `kline`이 각각 9개 symbol에서 발생했으며, 앞선 20초 메모리 검증에서 두 스트림을 포함한 5종 모두 10개 symbol 수신을 확인했다. 임시 컬렉션은 결과 확인 후 삭제했다.

## 확인된 경계

- `order_book_depth`는 diff 이벤트 저장이며 REST snapshot과 결합한 완성 호가창 복구는 구현 범위가 아니다.
- 메모리 큐이므로 프로세스 강제 종료와 연결 중단 구간의 무손실·정확히 한 번 전달을 보장하지 않는다.
- 서로 다른 WebSocket 연결 사이의 전역 이벤트 순서를 보장하지 않는다.
- Linux, Windows, Python 3.11은 이번 확장에서 직접 실행하지 않았다.
- 계획에 포함된 30분 통합 수집과 26시간 연결 수명·메모리 안정성 검증은 수행하지 않았다. 따라서 장시간 안정성을 검증 완료로 표시하지 않는다.
