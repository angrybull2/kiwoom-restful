import asyncio
import logging

import pytest

from kiwoom import API, REAL
from kiwoom.http.client import Client
from kiwoom.http.errors import (
    KiwoomAckTimeoutError,
    KiwoomInvalidTickerError,
    KiwoomRateLimitError,
    KiwoomRegisterError,
    build_api_error,
)
from kiwoom.http.response import Response
from kiwoom.http.socket import Socket


def test_build_api_error_classifies_known_codes():
    rate_limit = build_api_error({"return_code": 1700, "return_msg": "limit"})
    bad_ticker = build_api_error({"return_code": "1902", "return_msg": "bad ticker"})

    assert isinstance(rate_limit, KiwoomRateLimitError)
    assert rate_limit.category == "rate_limit"
    assert rate_limit.action == "retry_backoff"
    assert isinstance(bad_ticker, KiwoomInvalidTickerError)
    assert bad_ticker.category == "invalid_ticker"
    assert bad_ticker.action == "skip_record"


def test_api_rejects_websocket_send_when_disconnected():
    api = API(REAL, "app", "secret")

    with pytest.raises(ConnectionError, match="reconnect before register_real"):
        api._ensure_websocket_connected(action="register_real")


def test_request_refreshes_token_once():
    class DummyClient(Client):
        def __init__(self):
            super().__init__("https://example.com", "app", "secret")
            self.responses = [
                Response("https://example.com", 200, {}, {"return_code": 8005}),
                Response("https://example.com", 200, {}, {"return_code": 0}),
            ]
            self.refresh_count = 0

        async def post(self, endpoint, api_id, headers=None, data=None, logger=None):
            return self.responses.pop(0)

        async def refresh_auth(self, logger=None):
            self.refresh_count += 1

    async def run():
        client = DummyClient()
        with pytest.warns(DeprecationWarning):
            response = await client.request("/api/test", "ka-test")
        assert response.json()["return_code"] == 0
        assert client.refresh_count == 1

    asyncio.run(run())


def test_request_uses_call_logger():
    class ListHandler(logging.Handler):
        def __init__(self):
            super().__init__()
            self.messages = []

        def emit(self, record):
            self.messages.append(record.getMessage())

    class DummyClient(Client):
        def __init__(self):
            super().__init__("https://example.com", "app", "secret")

        async def post(self, endpoint, api_id, headers=None, data=None, logger=None):
            return Response("https://example.com", 200, {}, {"return_code": 1700})

    async def run():
        handler = ListHandler()
        logger = logging.getLogger("kiwoom-test-call-logger")
        logger.handlers.clear()
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)
        logger.propagate = False

        client = DummyClient()
        with pytest.warns(DeprecationWarning):
            with pytest.raises(KiwoomRateLimitError):
                await client.request("/api/test", "ka-test", logger=logger)

        assert any("return_code=1700" in message for message in handler.messages)

    asyncio.run(run())


def test_websocket_control_ack_timeout_cleans_waiter():
    async def run():
        api = API(REAL, "app", "secret")

        class DummySocket:
            async def send(self, payload, logger=None):
                return None

        api.socket = DummySocket()

        with pytest.raises(KiwoomAckTimeoutError):
            await api._send_control(
                {"trnm": "REG", "grp_no": "1", "data": []},
                wait_ack=True,
                ack_timeout=0.01,
                logger=logging.getLogger("kiwoom-test-ack-timeout"),
            )

        assert not api._control_waiters["REG"]

    asyncio.run(run())


def test_websocket_control_failure_sets_register_error():
    async def run():
        api = API(REAL, "app", "secret")
        future = asyncio.get_running_loop().create_future()
        api._control_waiters["REG"].append(future)

        api._log_websocket_control_response(
            {"trnm": "REG", "return_code": 1700, "return_msg": "limit"}
        )

        with pytest.raises(KiwoomRegisterError):
            await future

    asyncio.run(run())


def test_callback_queue_worker_dispatches_payload():
    async def run():
        api = API(REAL, "app", "secret", callback_workers=1)
        seen = []

        async def callback(payload):
            seen.append(payload)

        api._stop_event.clear()
        api._start_callback_workers()
        await api._enqueue_callback(callback, {"ok": True}, real_type="TEST")
        await asyncio.wait_for(api._callback_queue.join(), timeout=1)
        await api._stop_callback_workers(
            drain=False,
            logger=logging.getLogger("kiwoom-test-callback-worker"),
        )

        assert seen == [{"ok": True}]

    asyncio.run(run())


def test_socket_block_policy_times_out_when_queue_is_full():
    async def run():
        queue = asyncio.Queue(maxsize=1)
        await queue.put("old")
        socket = Socket("wss://example.invalid", queue, queue_put_timeout=0.01)

        with pytest.raises(ConnectionError, match="queue put timed out"):
            await socket._put_queue("new")

        assert socket.queue_put_timeouts == 1

    asyncio.run(run())
