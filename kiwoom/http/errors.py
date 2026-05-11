from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = [
    "ErrorInfo",
    "KiwoomAPIError",
    "KiwoomAuthError",
    "KiwoomFatalAuthError",
    "KiwoomInvalidInputError",
    "KiwoomInvalidTickerError",
    "KiwoomRateLimitError",
    "KiwoomRealtimeError",
    "KiwoomRegisterError",
    "KiwoomRemoveRegisterError",
    "KiwoomAckTimeoutError",
    "KiwoomServerError",
    "build_api_error",
    "is_success_code",
    "normalize_error_code",
]


SUCCESS_CODES = {0, 20}
TOKEN_REFRESH_CODES = {3, 8005, 8031, 8103}
FATAL_AUTH_CODES = {
    8001,
    8002,
    8003,
    8006,
    8009,
    8010,
    8011,
    8012,
    8015,
    8016,
    8020,
    8030,
    8040,
    8050,
}
INVALID_INPUT_CODES = {1501, 1504, 1505, 1511, 1512, 1513, 1514, 1515, 1516, 1517, 1687}
INVALID_TICKER_CODES = {1901, 1902}
RATE_LIMIT_CODES = {1700}
SERVER_TRANSIENT_CODES = {1999}


@dataclass(frozen=True)
class ErrorInfo:
    code: int | str
    message: str
    category: str
    action: str


class KiwoomAPIError(RuntimeError):
    def __init__(
        self,
        info: ErrorInfo,
        *,
        endpoint: str = "",
        api_id: str = "",
        body: dict[str, Any] | None = None,
        detail: str = "",
    ):
        self.code = info.code
        self.message = info.message
        self.category = info.category
        self.action = info.action
        self.endpoint = endpoint
        self.api_id = api_id
        self.body = body or {}
        self.detail = detail

        parts = [
            f"Kiwoom API error code={self.code}",
            f"category={self.category}",
            f"action={self.action}",
        ]
        if api_id:
            parts.append(f"api_id={api_id}")
        if endpoint:
            parts.append(f"endpoint={endpoint}")
        if self.message:
            parts.append(f"message={self.message}")
        if detail:
            parts.append(f"detail={detail}")
        super().__init__(" ".join(parts))

    @property
    def should_refresh_token(self) -> bool:
        return self.action == "refresh_token"


class KiwoomAuthError(KiwoomAPIError):
    pass


class KiwoomFatalAuthError(KiwoomAuthError):
    pass


class KiwoomInvalidInputError(KiwoomAPIError):
    pass


class KiwoomInvalidTickerError(KiwoomAPIError):
    pass


class KiwoomRateLimitError(KiwoomAPIError):
    pass


class KiwoomServerError(KiwoomAPIError):
    pass


class KiwoomRealtimeError(KiwoomAPIError):
    pass


class KiwoomRegisterError(KiwoomRealtimeError):
    pass


class KiwoomRemoveRegisterError(KiwoomRealtimeError):
    pass


class KiwoomAckTimeoutError(TimeoutError):
    def __init__(self, *, trnm: str, timeout: float):
        self.trnm = trnm
        self.timeout = timeout
        super().__init__(f"Kiwoom websocket {trnm} ACK timed out after {timeout:.1f}s.")


def normalize_error_code(code: Any) -> int | str:
    if isinstance(code, int):
        return code
    if isinstance(code, str):
        stripped = code.strip()
        if stripped.lstrip("-").isdigit():
            return int(stripped)
        return stripped
    return str(code)


def is_success_code(code: Any) -> bool:
    return normalize_error_code(code) in SUCCESS_CODES


def _extract_message(body: dict[str, Any]) -> str:
    for key in ("return_msg", "message", "msg", "error_message", "error"):
        if body.get(key):
            return str(body[key])
    return ""


def classify_error(code: int | str, message: str = "") -> tuple[type[KiwoomAPIError], str, str]:
    if code in TOKEN_REFRESH_CODES:
        return KiwoomAuthError, "auth", "refresh_token"
    if code in FATAL_AUTH_CODES:
        return KiwoomFatalAuthError, "auth", "fatal"
    if code in RATE_LIMIT_CODES:
        return KiwoomRateLimitError, "rate_limit", "retry_backoff"
    if code in INVALID_TICKER_CODES:
        return KiwoomInvalidTickerError, "invalid_ticker", "skip_record"
    if code in INVALID_INPUT_CODES:
        action = "deduplicate_or_fail_fast" if code == 1687 else "fail_fast"
        return KiwoomInvalidInputError, "invalid_request", action
    if code in SERVER_TRANSIENT_CODES:
        return KiwoomServerError, "server", "retry_limited"
    if "token" in message.lower():
        return KiwoomAuthError, "auth", "refresh_token"
    return KiwoomAPIError, "unknown", "operator_action"


def build_api_error(
    body: dict[str, Any],
    *,
    endpoint: str = "",
    api_id: str = "",
    detail: str = "",
) -> KiwoomAPIError:
    code = normalize_error_code(body.get("return_code", body.get("code", "")))
    message = _extract_message(body)
    exc_type, category, action = classify_error(code, message)
    info = ErrorInfo(code=code, message=message, category=category, action=action)
    return exc_type(info, endpoint=endpoint, api_id=api_id, body=body, detail=detail)
