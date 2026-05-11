import asyncio
from enum import Enum, auto

__all__ = [
    "REQ_LIMIT_TIME",
    "REQ_LIMIT_PER_SECOND",
    "HTTP_TOTAL_TIMEOUT",
    "HTTP_CONNECT_TIMEOUT",
    "HTTP_READ_TIMEOUT",
    "HTTP_TCP_CONNECTORS",
    "WEBSOCKET_HEARTBEAT",
    "WEBSOCKET_MAX_CONCURRENCY",
    "WEBSOCKET_QUEUE_ERROR_RATIO",
    "WEBSOCKET_QUEUE_MAX_SIZE",
    "WEBSOCKET_QUEUE_OVERFLOW_POLICY",
    "WEBSOCKET_QUEUE_PUT_TIMEOUT",
    "WEBSOCKET_QUEUE_WARN_RATIO",
    "WEBSOCKET_CALLBACK_DRAIN_TIMEOUT",
    "WEBSOCKET_CALLBACK_QUEUE_MAX_SIZE",
    "WEBSOCKET_CALLBACK_QUEUE_PUT_TIMEOUT",
    "WEBSOCKET_ACK_TIMEOUT",
    "WEBSOCKET_WAIT_ACK",
    "WEBSOCKET_STALE_CHECK_INTERVAL",
    "WEBSOCKET_STALE_TIMEOUT",
    "WEBSOCKET_COPY_REAL_VALUES",
    "State",
]

# Kiwoom REST API Request Limit Policy
#   API 호출 횟수 제한 정책은 다음 각 호와 같다.
#       1. 조회횟수 초당 5건
#       2. 주문횟수 초당 5건
#       3. 실시간 조건검색 개수 로그인 1개당 10건
#       4. 모의투자의 경우 조회횟수 초당 1건
REQ_LIMIT_TIME: float = 0.205  # sec
REQ_LIMIT_PER_SECOND: int = 5
REQ_LIMIT_PER_SECOND_MOCK: int = 1

# Client & Socket Connection Settings
HTTP_TOTAL_TIMEOUT: float = 10.0
HTTP_CONNECT_TIMEOUT: float = 3.0
HTTP_READ_TIMEOUT: float = 5.0
HTTP_TCP_CONNECTORS: int = 100

WEBSOCKET_HEARTBEAT: int = 30
WEBSOCKET_MAX_CONCURRENCY: int = 1000
WEBSOCKET_QUEUE_MAX_SIZE: int = 100_000
WEBSOCKET_QUEUE_OVERFLOW_POLICY: str = "block"  # block | drop_oldest | drop_latest
WEBSOCKET_QUEUE_PUT_TIMEOUT: float = 5.0
WEBSOCKET_QUEUE_WARN_RATIO: float = 0.7
WEBSOCKET_QUEUE_ERROR_RATIO: float = 0.9

WEBSOCKET_CALLBACK_QUEUE_MAX_SIZE: int = 100_000
WEBSOCKET_CALLBACK_QUEUE_PUT_TIMEOUT: float = 5.0
WEBSOCKET_CALLBACK_DRAIN_TIMEOUT: float = 5.0

WEBSOCKET_WAIT_ACK: bool = True
WEBSOCKET_ACK_TIMEOUT: float = 3.0

WEBSOCKET_STALE_TIMEOUT: float | None = None
WEBSOCKET_STALE_CHECK_INTERVAL: float = 5.0

WEBSOCKET_COPY_REAL_VALUES: bool = True


# Connection State
class State(Enum):
    """
    Connection State
    """

    CONNECTING = auto()
    CONNECTED = auto()
    CLOSING = auto()
    CLOSED = auto()


# Http Response Status Code
STATUS_CODE = {200: "OK", 400: "Bad Request", 404: "Not Found", 500: "Internal Server Error"}

# Suppress exceptions to close gracefully
EXCEPTIONS_TO_SUPPRESS = (asyncio.CancelledError, KeyboardInterrupt, Exception)
