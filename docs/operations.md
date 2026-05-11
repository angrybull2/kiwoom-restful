# Operations

## 1~9 반영 사항

- [x] 1. 외부 logger 주입
  - `Bot`, `API`, `Client`, `Socket` 생성자에서 `logger`를 받을 수 있다.
  - 주요 호출 메소드의 `logger` 인자를 통해 호출 단위 logger를 전달할 수 있다.
  - logger를 전달하지 않으면 패키지 내부 module logger를 사용한다.

- [x] 2. 닫힌 WebSocket 전송 방지
  - `Socket.send()`는 내부 websocket이 `None`이거나 닫힌 경우 `ConnectionError`를 발생시킨다.
  - `API.is_connected()`는 HTTP/WebSocket/수신 task 상태를 함께 확인한다.
  - `register_real()` / `remove_register()`는 전송 전에 연결 상태를 검증한다.

- [x] 3. WebSocket 수신 루프 종료 감지
  - `Socket.run()`에서 `WSMsgType.CLOSED` 같은 비문자 메시지를 받으면 연결 상태를 `CLOSED`로 전환한다.
  - `API.wait_closed()`, `API.closed_event`, `API.last_close_reason`으로 외부 supervisor가 재연결 판단을 할 수 있다.
  - 내부 `print()` 대신 `logging`을 사용해 운영 로그와 같은 경로로 남긴다.

- [x] 4. REST API 오류코드 분류
  - `kiwoom.http.errors`에 오류코드별 category/action/exception을 정의했다.
  - `Client.request()`는 `return_code`를 공통 mapper로 해석하고 의미 있는 예외를 발생시킨다.
  - `KiwoomAPIError` 계열은 `code`, `category`, `action`, `api_id`, `endpoint`, `message` 속성을 제공한다.

- [x] 5. 토큰 오류 자동 복구와 fatal 분리
  - `3`, `8005`, `8031`, `8103`은 토큰 refresh 후 1회 재시도한다.
  - AppKey/IP/단말/실전-모의 불일치 계열은 자동 재시도하지 않고 `fatal` action으로 올린다.

- [x] 6. Rate limit, ticker, 서버 오류 대응 근거 제공
  - `1700`은 `KiwoomRateLimitError`와 `retry_backoff` action으로 분류한다.
  - `1901`, `1902`는 `KiwoomInvalidTickerError`와 `skip_record` action으로 분류한다.
  - `1999`는 `KiwoomServerError`와 `retry_limited` action으로 분류한다.

- [x] 7. WebSocket queue/backpressure
  - `WEBSOCKET_QUEUE_MAX_SIZE=100_000`, `WEBSOCKET_QUEUE_OVERFLOW_POLICY="block"`를 기본값으로 사용한다.
  - 기본 block 정책은 queue가 가득 차면 최대 `WEBSOCKET_QUEUE_PUT_TIMEOUT=5.0`초 대기하고, timeout 시 `ConnectionError`를 발생시킨다.
  - queue 사용률이 70%/90%를 넘으면 warning/error log를 남긴다.

- [x] 8. Callback backpressure와 실패 격리
  - 실시간 callback은 bounded callback queue와 worker task에서 실행된다.
  - callback queue put timeout과 close 시 drain timeout을 설정할 수 있다.
  - callback 예외는 worker에서 logging하고 수신 루프 전체를 직접 중단시키지 않는다.

- [x] 9. REST TR helper deprecation 및 고빈도 객체 생성 옵션화
  - `Client.request()`, `Client.request_until()`, `API.stock_list()`, `API.sector_list()`, `API.candle()`, `API.trade()`는 하위 호환용 deprecated helper로 문서화했다.
  - `copy_real_values=True`가 기본값이므로 기존처럼 `RealData.values`는 안전한 `bytes` 복사본이다.
  - 성능 실험이 필요한 외부 코드베이스는 `API(..., copy_real_values=False)`로 `msgspec.Raw`를 전달받을 수 있다.

## Logger 주입 규칙

`kiwoom-restful`은 표준 `logging.Logger` 또는 `logging.LoggerAdapter`처럼
`debug`, `info`, `warning`, `error`, `exception` 메소드를 가진 logger 객체를 받을 수 있다.

```python
import logging

from kiwoom import Bot, REAL

collector_logger = logging.getLogger("data_collector.kiwoom")

bot = Bot(REAL, appkey, secretkey, logger=collector_logger)
await bot.connect()
```

생성자 또는 `connect(logger=...)`로 전달한 logger는 HTTP 요청, WebSocket 연결,
WebSocket background receive loop, 기본 control callback 로그에 계속 사용된다.

```python
await bot.connect(logger=collector_logger)
```

개별 호출에서만 다른 logger를 쓰고 싶으면 메소드 인자로 전달한다.

```python
job_logger = logging.getLogger("data_collector.jobs.list_refresh")

codes = await bot.stock_list("0", logger=job_logger)
await bot.api.request("/api/dostk/stkinfo", "ka10099", data={"mrkt_tp": "0"}, logger=job_logger)
```

지원되는 public 진입점은 다음과 같다.

| 객체 | logger 지원 위치 |
| --- | --- |
| `Bot` | 생성자, `set_logger`, `connect`, `close`, `stock_list`, `sector_list`, `candle`, `trade` |
| `API` | 생성자, `set_logger`, `connect`, `close`, `stock_list`, `sector_list`, `candle`, `trade`, `register_*`, `remove_register` |
| `Client` | 생성자, `set_logger`, `connect`, `close`, `request`, `request_until`, `post` |
| `Socket` | 생성자, `set_logger`, `connect`, `close`, `send` |
| `proc.trade.to_csv` | `logger` 인자 |

logger를 전달하지 않으면 `logging.getLogger(__name__)` 기반의 패키지 module logger를 사용한다.
패키지는 handler를 직접 설정하지 않으므로, 운영 애플리케이션에서 handler/formatter/level을 구성한다.

## 오류코드 운영 정책

| 코드 | category | action | 운영 판단 |
| --- | --- | --- | --- |
| `3`, `8005`, `8031`, `8103` | `auth` | `refresh_token` | 토큰 재발급 후 제한적으로 재시도 |
| `8001`, `8002`, `8003`, `8006`, `8009`, `8010`, `8011`, `8012`, `8015`, `8016`, `8020`, `8030`, `8040`, `8050` | `auth` | `fatal` | AppKey/Secret/IP/단말/실전-모의 설정 확인 |
| `1700` | `rate_limit` | `retry_backoff` | caller에서 backoff+jitter 후 재시도 |
| `1901`, `1902` | `invalid_ticker` | `skip_record` | 종목 단위 작업은 bad ticker로 기록하고 다음 종목 진행 |
| `1501`, `1504`, `1505`, `1511`, `1512`, `1513`, `1514`, `1515`, `1516`, `1517` | `invalid_request` | `fail_fast` | URI/API ID/header/payload 매핑 수정 |
| `1687` | `invalid_request` | `deduplicate_or_fail_fast` | scheduler 중복 실행 또는 같은 API 재진입 여부 확인 |
| `1999` | `server` | `retry_limited` | 실패 사유를 남기고 제한적으로 재시도 |

## Collector 연동 포인트

`data-collector` 같은 상위 프로세스에서는 `KiwoomAPIError`의 속성을 그대로 로그에 넣는 것을 권장한다.

```python
try:
    body = await api.request(endpoint, api_id, data=payload)
except KiwoomAPIError as err:
    logger.warning(
        "Kiwoom request failed api_id=%s endpoint=%s error_code=%s "
        "error_category=%s action=%s message=%s",
        err.api_id,
        err.endpoint,
        err.code,
        err.category,
        err.action,
        err.message,
    )
    raise
```

실시간 수집에서는 `ConnectionError`를 reconnect 신호로 취급하고, `KiwoomInvalidTickerError`는 종목 단위 skip, `KiwoomRateLimitError`와 `KiwoomServerError`는 제한적 retry 대상으로 분리하면 원인분석과 자동 복구가 쉬워진다.

## 개발/배포 메모

다음 커밋/푸시 작업에서 참고할 사항은 아래와 같다.

- Codex 기본 샌드박스 실행이 `bubblewrap is unavailable`로 실패할 수 있다. 이 경우 git/rg/sed/pytest 같은 확인 명령은 승인된 외부 실행으로 진행한다.
- 로컬 기본 Python에는 `pytest`가 없고, `.venv/bin/pytest`도 없다. 현재 확인 가능한 테스트 실행 경로는 `conda run -n nnml pytest ...`이다.
- 전체 `pytest`는 기존 async 테스트에 `pytest-asyncio` 같은 플러그인/fixture 설정이 없어 실패할 수 있다. 이번 변경 검증은 `conda run -n nnml pytest test/test_errors.py`로 수행했고 `8 passed`를 확인했다.
- `origin` remote는 HTTPS(`https://github.com/angrybull2/kiwoom-restful.git`)라 비대화식 환경에서 `git push origin main`이 GitHub username 입력 실패로 막힐 수 있다.
- SSH 키는 사용 가능했으므로 푸시는 `git push git@github.com:angrybull2/kiwoom-restful.git main`으로 진행한다.
- SSH 직접 푸시 뒤 로컬 추적 브랜치가 `ahead 1`로 남으면 `git fetch git@github.com:angrybull2/kiwoom-restful.git main:refs/remotes/origin/main`으로 `origin/main` 참조를 맞춘다.
