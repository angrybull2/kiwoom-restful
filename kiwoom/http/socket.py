import asyncio
import contextlib
import time

import orjson
from aiohttp import ClientSession, ClientWebSocketResponse, WSMessageTypeError, WSMsgType

from kiwoom.config.http import (
    WEBSOCKET_HEARTBEAT,
    WEBSOCKET_QUEUE_ERROR_RATIO,
    WEBSOCKET_QUEUE_OVERFLOW_POLICY,
    WEBSOCKET_QUEUE_PUT_TIMEOUT,
    WEBSOCKET_QUEUE_WARN_RATIO,
    State,
)
from kiwoom.http.logging_utils import LoggerLike, get_logger
from kiwoom.http.utils import cancel

module_logger = get_logger(__name__)


class Socket:
    REAL = "wss://api.kiwoom.com:10000"
    MOCK = "wss://mockapi.kiwoom.com:10000"  # KRX Only
    ENDPOINT = "/api/dostk/websocket"

    def __init__(
        self,
        url: str,
        queue: asyncio.Queue,
        logger: LoggerLike | None = None,
        queue_overflow_policy: str = WEBSOCKET_QUEUE_OVERFLOW_POLICY,
        queue_put_timeout: float = WEBSOCKET_QUEUE_PUT_TIMEOUT,
        queue_warn_ratio: float = WEBSOCKET_QUEUE_WARN_RATIO,
        queue_error_ratio: float = WEBSOCKET_QUEUE_ERROR_RATIO,
    ):
        """
        Initialize Socket class.

        Args:
            url (str): url of Kiwoom websocket server
            queue (asyncio.Queue): queue to put received data
            logger (LoggerLike, optional): logger used by this socket.
            queue_overflow_policy (str): block, drop_oldest, or drop_latest.
            queue_put_timeout (float): max seconds to wait when policy is block.
            queue_warn_ratio (float): queue usage ratio to log warning.
            queue_error_ratio (float): queue usage ratio to log error.
        """
        self.url = url
        self._queue = queue
        self._logger = logger if logger is not None else module_logger
        self._queue_overflow_policy = queue_overflow_policy
        self._queue_put_timeout = queue_put_timeout
        self._queue_warn_ratio = queue_warn_ratio
        self._queue_error_ratio = queue_error_ratio
        self._queue_level_logged = ""
        self.queue_dropped = 0
        self.queue_put_timeouts = 0
        self.last_recv_monotonic_ns: int | None = None
        self.last_close_reason: str = ""
        self.closed_event = asyncio.Event()
        self.closed_event.set()
        self._session: ClientSession | None = None
        self._websocket: ClientWebSocketResponse | None = None

        self._state = State.CLOSED
        self._state_lock = asyncio.Lock()
        self._queue_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._stop_event.set()

    def set_logger(self, logger: LoggerLike | None = None) -> None:
        """
        Set logger used by this socket. If omitted, module-level logger is used.
        """
        self._logger = logger if logger is not None else module_logger

    def _resolve_logger(self, logger: LoggerLike | None = None) -> LoggerLike:
        return logger if logger is not None else self._logger

    def is_connected(self) -> bool:
        """
        Return True when websocket transport is usable for send/receive.
        """
        return (
            self._state == State.CONNECTED
            and self._websocket is not None
            and not self._websocket.closed
        )

    async def wait_closed(self, timeout: float | None = None) -> bool:
        """
        Wait until the websocket receive loop is closed.

        Args:
            timeout (float | None): max seconds to wait.

        Returns:
            bool: True if closed_event was set, False on timeout.
        """
        try:
            await asyncio.wait_for(self.closed_event.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def _close_resources(self, *, cancel_queue_task: bool) -> None:
        self._stop_event.set()

        task = self._queue_task
        current = asyncio.current_task()
        if cancel_queue_task and task and task is not current:
            await cancel(task)

        if self._websocket and not self._websocket.closed:
            with contextlib.suppress(Exception):
                await self._websocket.close()

        self._session = None
        self._websocket = None
        if cancel_queue_task or task is current:
            self._queue_task = None

    async def connect(
        self,
        session: ClientSession,
        token: str,
        logger: LoggerLike | None = None,
    ):
        """
        Connect to Kiwoom websocket server.

        Args:
            session (ClientSession): aiohttp ClientSession from API.connect()
            token (str): token for authentication
            logger (LoggerLike, optional): logger used for this connection.
        """
        if logger is not None:
            self.set_logger(logger)
        active_logger = self._resolve_logger()

        async with self._state_lock:
            if self._state == State.CONNECTING:
                return
            if self._state == State.CONNECTED and self.is_connected():
                return

            self._state = State.CONNECTING
            try:
                # Close existing websocket & task
                await self._close_resources(cancel_queue_task=True)
                self.closed_event.clear()
                self.last_close_reason = ""
                self._queue_level_logged = ""

                self._session = session
                self._websocket = await session.ws_connect(
                    self.url, autoping=True, heartbeat=WEBSOCKET_HEARTBEAT
                )

                self._stop_event.clear()
                self._queue_task = asyncio.create_task(self.run(), name="enqueue")
                await self.send({"trnm": "LOGIN", "token": token})
                self._state = State.CONNECTED

            except Exception as err:
                active_logger.exception("Kiwoom websocket failed to connect url=%s", self.url)
                self._state = State.CLOSED
                self.last_close_reason = str(err)
                await self._close_resources(cancel_queue_task=True)
                raise ConnectionError(f"Kiwoom websocket failed to connect url={self.url}") from err

    async def close(self, logger: LoggerLike | None = None):
        """
        Close the websocket and the task.
        """
        if logger is not None:
            self.set_logger(logger)
        async with self._state_lock:
            self._state = State.CLOSING
            self.last_close_reason = self.last_close_reason or "closed by user"
            await self._close_resources(cancel_queue_task=True)
            self._state = State.CLOSED
            self.closed_event.set()

    async def send(self, msg: str | dict, logger: LoggerLike | None = None) -> None:
        """
        Send data to Kiwoom websocket server.

        Args:
            msg (str | dict): msg should be in json format
            logger (LoggerLike, optional): logger used for this send.
        """
        active_logger = self._resolve_logger(logger)
        if isinstance(msg, dict):
            # msg = json.dumps(msg)  # slow
            msg = orjson.dumps(msg).decode("utf-8")
        if self._websocket is None or self._websocket.closed:
            self._state = State.CLOSED
            active_logger.warning("Kiwoom websocket is closed; send aborted.")
            raise ConnectionError("Kiwoom websocket is closed; reconnect before sending.")
        await self._websocket.send_str(msg)

    async def recv(self) -> str:
        """
        Receive data from Kiwoom websocket server and return data.
        If message type is not str, mark the websocket closed and raise ConnectionError.

        Raises:
            ConnectionError: Websocket Connection Error

        Returns:
            str: received json formatted data from websocket
        """
        websocket = self._websocket
        if websocket is None or websocket.closed:
            self._state = State.CLOSED
            raise ConnectionError("Kiwoom websocket is closed; reconnect before receiving.")

        try:
            raw = await websocket.receive_str()
            self.last_recv_monotonic_ns = time.monotonic_ns()
            return raw
        except WSMessageTypeError as err:
            msg = await websocket.receive()
            if msg.type == WSMsgType.BINARY:
                msg.data = msg.data.decode("utf-8")
            self.last_close_reason = f"non-text websocket message: {msg}"
            raise ConnectionError(f"Kiwoom websocket received non-text message: {msg}") from err

    def _log_queue_level(self) -> None:
        maxsize = self._queue.maxsize
        if maxsize <= 0:
            return
        ratio = self._queue.qsize() / maxsize
        level = ""
        if ratio >= self._queue_error_ratio:
            level = "error"
        elif ratio >= self._queue_warn_ratio:
            level = "warning"
        if not level or level == self._queue_level_logged:
            return
        self._queue_level_logged = level
        msg = "Kiwoom websocket queue usage %.1f%% qsize=%s maxsize=%s"
        if level == "error":
            self._logger.error(msg, ratio * 100, self._queue.qsize(), maxsize)
            return
        self._logger.warning(msg, ratio * 100, self._queue.qsize(), maxsize)

    async def _put_queue(self, raw: str) -> None:
        self._log_queue_level()
        try:
            match self._queue_overflow_policy:
                case "block":
                    await asyncio.wait_for(self._queue.put(raw), self._queue_put_timeout)
                case "drop_latest":
                    self._queue.put_nowait(raw)
                case "drop_oldest" | "latest_only":
                    if self._queue.full():
                        with contextlib.suppress(asyncio.QueueEmpty):
                            self._queue.get_nowait()
                            self._queue.task_done()
                            self.queue_dropped += 1
                    self._queue.put_nowait(raw)
                case _:
                    raise ValueError(
                        "queue_overflow_policy must be one of "
                        "'block', 'drop_oldest', 'drop_latest', 'latest_only'."
                    )
        except asyncio.TimeoutError as err:
            self.queue_put_timeouts += 1
            self.last_close_reason = (
                f"websocket queue put timed out after {self._queue_put_timeout:.1f}s"
            )
            raise ConnectionError(self.last_close_reason) from err
        except asyncio.QueueFull:
            self.queue_dropped += 1
            if self._queue_overflow_policy == "drop_latest":
                self._logger.warning(
                    "Kiwoom websocket queue full; dropped latest message count=%s",
                    self.queue_dropped,
                )
                return
            raise

    async def run(self):
        """
        Receive data from websocket and put data to the queue.
        If WEBSOCKET_QUEUE_MAX_SIZE is set and queue gets full,
        then backpressure will be applied to the websocket.
        Run this task in background with asyncio.create_task().
        """
        try:
            while not self._stop_event.is_set():
                await self._put_queue(await self.recv())

        except asyncio.CancelledError:
            self.last_close_reason = self.last_close_reason or "receive loop cancelled"
            raise
        except Exception as err:
            self.last_close_reason = self.last_close_reason or str(err)
            self._logger.warning("Kiwoom websocket receive loop stopped: %s", err)
        finally:
            self._state = State.CLOSED
            await self._close_resources(cancel_queue_task=False)
            self.closed_event.set()
