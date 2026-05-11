import asyncio
import contextlib
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from inspect import iscoroutinefunction
from typing import Callable, Optional

import msgspec
import orjson
from pandas import bdate_range

from kiwoom import config
from kiwoom.config.candle import (
    PERIOD_TO_API_ID,
    PERIOD_TO_BODY_KEY,
    PERIOD_TO_DATA,
    PERIOD_TO_TIME_KEY,
    valid,
)
from kiwoom.config.http import (
    REQ_LIMIT_PER_SECOND,
    REQ_LIMIT_PER_SECOND_MOCK,
    EXCEPTIONS_TO_SUPPRESS,
    WEBSOCKET_ACK_TIMEOUT,
    WEBSOCKET_CALLBACK_DRAIN_TIMEOUT,
    WEBSOCKET_CALLBACK_QUEUE_MAX_SIZE,
    WEBSOCKET_CALLBACK_QUEUE_PUT_TIMEOUT,
    WEBSOCKET_COPY_REAL_VALUES,
    WEBSOCKET_MAX_CONCURRENCY,
    WEBSOCKET_QUEUE_MAX_SIZE,
    WEBSOCKET_STALE_CHECK_INTERVAL,
    WEBSOCKET_STALE_TIMEOUT,
    WEBSOCKET_WAIT_ACK,
    State,
)
from kiwoom.config.real import RealData, RealType
from kiwoom.config.trade import (
    REQUEST_LIMIT_DAYS,
)
from kiwoom.http.client import Client
from kiwoom.http.errors import (
    ErrorInfo,
    KiwoomAPIError,
    KiwoomAckTimeoutError,
    KiwoomRegisterError,
    KiwoomRemoveRegisterError,
    build_api_error,
    is_success_code,
    normalize_error_code,
)
from kiwoom.http.logging_utils import LoggerLike, get_logger
from kiwoom.http.socket import Socket
from kiwoom.http.utils import (
    cancel,
    wrap_async_callback,
    wrap_sync_callback,
)

module_logger = get_logger(__name__)


class API(Client):
    """
    Kiwoom REST API 서버와 직접 요청과 응답을 주고받는 클래스입니다.

    데이터 조회, 주문 요청 등 저수준 통신을 담당하며,
    직접 API 스펙을 구현하여 활용합니다.
    """

    def __init__(
        self,
        host: str,
        appkey: str,
        secretkey: str,
        logger: LoggerLike | None = None,
        wait_ack: bool = WEBSOCKET_WAIT_ACK,
        ack_timeout: float = WEBSOCKET_ACK_TIMEOUT,
        callback_queue_max_size: int = WEBSOCKET_CALLBACK_QUEUE_MAX_SIZE,
        callback_workers: int = WEBSOCKET_MAX_CONCURRENCY,
        callback_queue_put_timeout: float = WEBSOCKET_CALLBACK_QUEUE_PUT_TIMEOUT,
        callback_drain_timeout: float = WEBSOCKET_CALLBACK_DRAIN_TIMEOUT,
        stale_timeout: float | None = WEBSOCKET_STALE_TIMEOUT,
        stale_check_interval: float = WEBSOCKET_STALE_CHECK_INTERVAL,
        copy_real_values: bool = WEBSOCKET_COPY_REAL_VALUES,
    ):
        """
        API 클래스 인스턴스를 초기화합니다.

        Args:
            host (str): 실서버 / 모의서버 도메인
            appkey (str): 파일경로 / 앱키
            secretkey (str): 파일경로 / 시크릿키
            logger (LoggerLike, optional): API 내부 로그에 사용할 logger
            wait_ack (bool): REG/REMOVE 서버 ACK를 기다릴지 여부
            ack_timeout (float): REG/REMOVE ACK 대기 시간(초)
            callback_queue_max_size (int): 실시간 callback queue 최대 크기
            callback_workers (int): callback worker task 개수
            callback_queue_put_timeout (float): callback queue put timeout(초)
            callback_drain_timeout (float): close 시 callback queue drain timeout(초)
            stale_timeout (float | None): 수신 정지 판단 시간. None이면 watchdog 비활성화
            stale_check_interval (float): stale watchdog 검사 주기(초)
            copy_real_values (bool): RealData.values를 bytes로 복사할지 여부

        Raises:
            ValueError: 유효하지 않은 도메인
        """
        match host:
            case config.REAL:
                wss_url = Socket.REAL + Socket.ENDPOINT
                rps = REQ_LIMIT_PER_SECOND
            case config.MOCK:
                wss_url = Socket.MOCK + Socket.ENDPOINT
                rps = REQ_LIMIT_PER_SECOND_MOCK
            case _:
                raise ValueError(f"Invalid host: {host}")

        super().__init__(host, appkey, secretkey, logger=logger)
        self.queue = asyncio.Queue(maxsize=WEBSOCKET_QUEUE_MAX_SIZE)
        self.socket = Socket(url=wss_url, queue=self.queue, logger=self._logger)
        self._limiter.set(rps)
        self._wait_ack = wait_ack
        self._ack_timeout = ack_timeout
        self._callback_queue_put_timeout = callback_queue_put_timeout
        self._callback_drain_timeout = callback_drain_timeout
        self._copy_real_values = copy_real_values
        self._stale_timeout = stale_timeout
        self._stale_check_interval = stale_check_interval

        self._state = State.CLOSED
        self._state_lock = asyncio.Lock()
        self._recv_task: asyncio.Task | None = None
        self._stale_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._stop_event.set()

        self._sem = asyncio.Semaphore(config.http.WEBSOCKET_MAX_CONCURRENCY)
        self._callback_queue: asyncio.Queue = asyncio.Queue(maxsize=callback_queue_max_size)
        self._callback_workers = callback_workers
        self._callback_tasks: list[asyncio.Task] = []
        self._callback_failures = 0
        self._callback_put_timeouts = 0
        self._control_waiters: defaultdict[str, deque[asyncio.Future]] = defaultdict(deque)
        self._control_lock = asyncio.Lock()
        self._real_type_cache: dict[str, str] = {}
        self.last_real_monotonic_ns: int | None = None
        self.last_control_monotonic_ns: int | None = None
        self._callbacks = defaultdict(
            lambda: wrap_async_callback(self._sem, self._default_callback_on_real_data)
        )
        self._add_default_callback_on_real_data()

    def set_logger(self, logger: LoggerLike | None = None) -> None:
        """
        Set logger used by API and websocket internals.
        """
        super().set_logger(logger if logger is not None else module_logger)
        self.socket.set_logger(self._logger)

    def is_connected(self) -> bool:
        """
        Return True when HTTP session, websocket, and receive task are all usable.
        """
        recv_running = self._recv_task is not None and not self._recv_task.done()
        return self._state == State.CONNECTED and recv_running and self.socket.is_connected()

    @property
    def last_close_reason(self) -> str:
        """
        Last known websocket close reason reported by the socket receive loop.
        """
        return self.socket.last_close_reason

    @property
    def closed_event(self) -> asyncio.Event:
        """
        Event set when the websocket receive loop is closed.
        """
        return self.socket.closed_event

    async def wait_closed(self, timeout: float | None = None) -> bool:
        """
        Wait until the websocket receive loop is closed.

        Args:
            timeout (float | None): max seconds to wait.

        Returns:
            bool: True if closed_event was set, False on timeout.
        """
        return await self.socket.wait_closed(timeout)

    def callback_queue_size(self) -> int:
        """
        Return pending callback queue size.
        """
        return self._callback_queue.qsize()

    def _ensure_websocket_connected(self, *, action: str) -> None:
        if self.is_connected():
            return
        raise ConnectionError(f"Kiwoom websocket is closed; reconnect before {action}.")

    async def refresh_auth(self, logger: LoggerLike | None = None) -> None:
        """
        Reconnect HTTP and websocket sessions with a fresh access token.
        """
        if logger is not None:
            self.set_logger(logger)
        await self.close(logger=logger)
        await self.connect(self._headers, logger=logger)

    def _normalize_real_type(self, real_type: str) -> str:
        cached = self._real_type_cache.get(real_type)
        if cached is not None:
            return cached
        normalized = real_type.upper()
        self._real_type_cache[real_type] = normalized
        return normalized

    def _real_values(self, values):
        if self._copy_real_values:
            return bytes(values)
        return values

    def _start_callback_workers(self) -> None:
        if self._callback_tasks:
            return
        worker_count = max(1, self._callback_workers)
        for idx in range(worker_count):
            task = asyncio.create_task(self._callback_worker(idx), name=f"kiwoom-callback-{idx}")
            self._callback_tasks.append(task)

    async def _stop_callback_workers(self, *, drain: bool, logger: LoggerLike) -> None:
        if drain and not self._callback_queue.empty():
            try:
                await asyncio.wait_for(
                    self._callback_queue.join(),
                    timeout=self._callback_drain_timeout,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Kiwoom callback queue drain timed out qsize=%s timeout=%.1fs",
                    self._callback_queue.qsize(),
                    self._callback_drain_timeout,
                )
        for task in self._callback_tasks:
            task.cancel()
        for task in self._callback_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._callback_tasks.clear()

    async def _callback_worker(self, idx: int) -> None:
        while True:
            try:
                callback, payload, real_type, item = await self._callback_queue.get()
            except asyncio.CancelledError:
                break
            try:
                await callback(payload)
            except Exception:
                self._callback_failures += 1
                self._logger.exception(
                    "Kiwoom realtime callback failed worker=%s real_type=%s item=%s "
                    "failures=%s pending=%s",
                    idx,
                    real_type,
                    item,
                    self._callback_failures,
                    self._callback_queue.qsize(),
                )
            finally:
                self._callback_queue.task_done()

    async def _enqueue_callback(
        self,
        callback: Callable,
        payload: RealData | dict,
        *,
        real_type: str,
        item: str = "",
    ) -> None:
        try:
            await asyncio.wait_for(
                self._callback_queue.put((callback, payload, real_type, item)),
                timeout=self._callback_queue_put_timeout,
            )
        except asyncio.TimeoutError as err:
            self._callback_put_timeouts += 1
            raise ConnectionError(
                "Kiwoom callback queue put timed out "
                f"after {self._callback_queue_put_timeout:.1f}s."
            ) from err

    async def _stale_watchdog(self) -> None:
        if self._stale_timeout is None:
            return
        while not self._stop_event.is_set():
            await asyncio.sleep(self._stale_check_interval)
            if not self.is_connected():
                continue
            last_seen = max(
                ts
                for ts in (
                    self.socket.last_recv_monotonic_ns,
                    self.last_real_monotonic_ns,
                    self.last_control_monotonic_ns,
                    0,
                )
                if ts is not None
            )
            if last_seen <= 0:
                continue
            elapsed = (time.monotonic_ns() - last_seen) / 1_000_000_000
            if elapsed < self._stale_timeout:
                continue
            reason = f"websocket stale for {elapsed:.1f}s"
            self.socket.last_close_reason = reason
            self._logger.warning("Kiwoom websocket stale detected: %s", reason)
            await self.socket.close(logger=self._logger)
            return

    async def connect(
        self,
        headers: Optional[dict] = None,
        logger: LoggerLike | None = None,
    ):
        """
        키움 REST API HTTP 서버와 Websocket 서버에 접속하고 토큰을 발급받습니다.

        Args:
            headers (dict): 서버 연결 시 사용할 헤더 (User-Agent 등)
            logger (LoggerLike, optional): 연결 및 WebSocket background task 로그에 사용할 logger

        Raises:
            RuntimeError: 토큰을 발급받지 못한 경우
            Exception: 예상하지 못한 에러
        """
        if logger is not None:
            self.set_logger(logger)
        active_logger = self._resolve_logger(logger)

        async with self._state_lock:
            if self._state == State.CONNECTING:
                return
            if self._state == State.CONNECTED and self.is_connected():
                return

            self._state = State.CONNECTING
            try:
                # Cancel existing tasks and callback workers from a previous session.
                self._stop_event.set()
                with contextlib.suppress(asyncio.CancelledError):
                    await cancel(self._stale_task)
                self._stale_task = None
                await cancel(self._recv_task)
                self._recv_task = None
                await self._stop_callback_workers(drain=False, logger=active_logger)

                # Connect http server
                await super().connect(
                    self._appkey,
                    self._secretkey,
                    headers,
                    logger=active_logger,
                )
                if not (token := self.token()):
                    raise RuntimeError("Not connected: token is not available.")

                # Connect websocket server
                await self.socket.connect(self._session, token, logger=active_logger)

                # Run websocket receiving and callback worker tasks
                self._stop_event.clear()
                self._start_callback_workers()
                self._recv_task = asyncio.create_task(self._on_receive_websocket(), name="dequeue")
                if self._stale_timeout is not None:
                    self._stale_task = asyncio.create_task(
                        self._stale_watchdog(),
                        name="kiwoom-stale-watchdog",
                    )
                self._state = State.CONNECTED

            except Exception as err:
                self._state = State.CLOSED
                with contextlib.suppress(Exception):
                    await self.socket.close(logger=active_logger)
                with contextlib.suppress(Exception):
                    await self._stop_callback_workers(drain=False, logger=active_logger)
                with contextlib.suppress(Exception):
                    await super().close(logger=active_logger)
                raise ConnectionError("Failed to connect Kiwoom API.") from err

    async def close(self, logger: LoggerLike | None = None):
        """
        키움 REST API 서버와 연결을 해제하고 리소스를 정리합니다.
        """
        if logger is not None:
            self.set_logger(logger)
        active_logger = self._resolve_logger(logger)

        async with self._state_lock:
            if self._state in (State.CLOSED, State.CLOSING):
                return

            self._state = State.CLOSING
            try:
                # Cancel existing task
                self._stop_event.set()
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.shield(cancel(self._stale_task))
                self._stale_task = None
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.shield(cancel(self._recv_task))
                self._recv_task = None

                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.shield(
                        self._stop_callback_workers(drain=True, logger=active_logger)
                    )

                # Close websocket server
                try:
                    await asyncio.shield(self.socket.close(logger=active_logger))
                except EXCEPTIONS_TO_SUPPRESS as err:
                    active_logger.warning("Kiwoom websocket close suppressed: %s", err)
                # Close http server
                try:
                    await asyncio.shield(super().close(logger=active_logger))
                except EXCEPTIONS_TO_SUPPRESS as err:
                    active_logger.warning("Kiwoom HTTP close suppressed: %s", err)

            finally:
                self._state = State.CLOSED

    async def stock_list(self, market: str, logger: LoggerLike | None = None) -> dict:
        """
        주어진 market 코드에 대해 'ka10099' API 요청을 하고 응답을 반환합니다.

        Deprecated:
            REST TR 조회 helper는 하위 호환성을 위해 유지됩니다.
            신규 코드는 외부 REST TR client 사용을 권장합니다.

        Args:
            market (str): 조회할 주식 시장코드
            logger (LoggerLike, optional): 요청 로그에 사용할 logger

        Raises:
            ValueError: 종목코드 목록이 없을 경우

        Returns:
            dict: 종목코드 목록을 포함하는 응답
        """
        endpoint = "/api/dostk/stkinfo"
        api_id = "ka10099"

        res = await self.request(endpoint, api_id, data={"mrkt_tp": market}, logger=logger)
        body = res.json()
        if not body["list"] or len(body["list"]) <= 1:
            raise ValueError(f"Stock list is not available for market code, {market}.")
        return body

    async def sector_list(self, market: str, logger: LoggerLike | None = None) -> dict:
        """
        주어진 market 코드에 대해 'ka10101' API 요청을 하고 응답을 반환합니다.

        Deprecated:
            REST TR 조회 helper는 하위 호환성을 위해 유지됩니다.
            신규 코드는 외부 REST TR client 사용을 권장합니다.

        Args:
            market (str): 조회할 주식 시장코드
            logger (LoggerLike, optional): 요청 로그에 사용할 logger

        Raises:
            ValueError: 업종코드 목록이 없을 경우

        Returns:
            dict: 업종코드 목록을 포함하는 응답
        """
        endpoint = "/api/dostk/stkinfo"
        api_id = "ka10101"

        res = await self.request(endpoint, api_id, data={"mrkt_tp": market}, logger=logger)
        body = res.json()
        if not body["list"] or len(body["list"]) <= 1:
            raise ValueError(f"Sector list is not available for sector code, {market}.")
        return body

    async def candle(
        self,
        code: str,
        period: str,
        ctype: str,
        start: str = None,
        end: str = None,
        logger: LoggerLike | None = None,
    ) -> dict:
        """
        주어진 코드, 기간, 종목/업종 유형에 해당하는 API 요청을 하고 응답을 반환합니다.

        Deprecated:
            REST TR 조회 helper는 하위 호환성을 위해 유지됩니다.
            신규 코드는 외부 REST TR client 사용을 권장합니다.

        "stock": {"tick": "ka10079", "min": "ka10080", "day": "ka10081"}

        "sector": {"tick": "ka20004", "min": "ka20005", "day": "ka20006"}

        Args:
            code (str): 종목코드 / 업종코드
            period (str): 캔들 기간유형, {"tick", "min", "day"}.
            ctype (str): 종목 / 업종 유형, {"stock", "sector"}.
            start (str, optional): 시작일자 in YYYYMMDD format.
            end (str, optional): 종료일자 in YYYYMMDD format.
            logger (LoggerLike, optional): 요청 로그에 사용할 logger

        Raises:
            ValueError: 유효하지 않은 'ctype' 또는 'period'

        Returns:
            dict: 캔들 데이터를 포함하는 json 응답
        """

        ctype = ctype.lower()
        endpoint = "/api/dostk/chart"
        api_id = PERIOD_TO_API_ID[ctype][period]
        data = dict(PERIOD_TO_DATA[ctype][period])
        match ctype:
            case "stock":
                data["stk_cd"] = code
            case "sector":
                data["inds_cd"] = code
            case _:
                raise ValueError(f"'ctype' must be one of [stock, sector], not {ctype=}.")
        if period == "day":
            end = end if end else datetime.now().strftime("%Y%m%d")
            data["base_dt"] = end

        ymd: int = len("YYYYMMDD")  # 8 digit compare
        key: str = PERIOD_TO_BODY_KEY[ctype][period]
        time: str = PERIOD_TO_TIME_KEY[period]

        def should_continue(body: dict) -> bool:
            # Validate
            if not valid(body, period, ctype):
                return False
            # Request full data
            if not start:
                return True
            # Condition to continue
            chart = body[key]
            earliest = chart[-1][time][:ymd]
            return start <= earliest

        body = await self.request_until(should_continue, endpoint, api_id, data=data, logger=logger)
        return body

    async def trade(
        self,
        start: str,
        end: str = "",
        logger: LoggerLike | None = None,
    ) -> list[dict]:
        """
        주어진 시작일자와 종료일자에 해당하는 체결내역을
        키움증권 '0343' 계좌 체결내역 화면과 동일한 구성으로 반환합니다.
        데이터 조회 제한으로 최근 2개월 데이터만 조회할 수 있습니다.

        Deprecated:
            REST TR 조회 helper는 하위 호환성을 위해 유지됩니다.
            신규 코드는 외부 REST TR client 사용을 권장합니다.

        체결내역 데이터는 [알파노트](http://alphanote.io)를 통해
        간편하게 진입/청산 시각화 및 성과 지표들을 확인할 수 있습니다.

        Args:
            start (str): 시작일자 in YYYYMMDD format
            end (str, optional): 종료일자 in YYYYMMDD format
            logger (LoggerLike, optional): 요청 로그에 사용할 logger

        Returns:
            list[dict]: 체결내역 데이터를 포함하는 json 응답 리스트
        """
        endpoint = "/api/dostk/acnt"
        api_id = "kt00009"
        data = {
            "ord_dt": "",  # YYYYMMDD (Optional)
            "qry_tp": "1",  # 전체/체결
            "stk_bond_tp": "1",  # 전체/주식/채권
            "mrkt_tp": "0",  # 전체/코스피/코스닥/OTCBB/ECN
            "sell_tp": "0",  # 전체/매도/매수
            "dmst_stex_tp": "%",  # 전체/KRX/NXT/SOR
            # 'stk_cd': '',  # 종목코드 (Optional)
            # 'fr_ord_no': '',  # 시작주문번호 (Optional)
        }

        today = datetime.today()
        start = datetime.strptime(start, "%Y%m%d")
        start = max(start, today - timedelta(days=REQUEST_LIMIT_DAYS))
        end = datetime.strptime(end, "%Y%m%d") if end else datetime.today()
        end = min(end, datetime.today())

        trs = []
        key = "acnt_ord_cntr_prst_array"
        for bday in bdate_range(start, end):
            dic = dict(data)
            dic["ord_dt"] = bday.strftime("%Y%m%d")  # manually set ord_dt
            body = await self.request_until(
                lambda x: True,
                endpoint,
                api_id,
                data=dic,
                logger=logger,
            )
            if key in body:
                # Append order date to each record
                for rec in body[key]:
                    rec["ord_dt"] = bday.strftime("%Y-%m-%d")
                trs.extend(body[key])
        return trs

    def add_callback_on_real_data(self, real_type: str, callback: Callable) -> None:
        """
        실시간 데이터 수신 시 호출될 콜백 함수를 추가합니다. (trnm이 'REAL'인 경우)

        * 실시간 시세 메시지의 callback 함수는 RealData 인스턴스를 인자로 받습니다.
        * 'PING', 'LOGIN', 'REG', 'REMOVE' callback 함수는 dict 인자를 받습니다.
        * real_type을 'PING', 'LOGIN', 'REG', 'REMOVE'로 설정하면 기본 콜백 함수를 덮어씁니다.

        콜백 함수는 비동기 콜백 함수를 추가하는 것을 권장합니다.
        비동기 및 동기 콜백 함수 모두 루프를 블로킹하지 않도록
        내부 callback queue와 worker를 통해 실행됩니다. 따라서 데이터 처리 완료 순서가
        반드시 데이터 수신 순서에 따라 실행되지 않을 수 있습니다.

        ex) tick 체결 데이터 (type 'OB')가 수신될 때마다 데이터 출력하기

            > fn = lambda raw: print(raw)

            > add_callback_on_real_data(real_type='OB', callback=fn)

        Args:
            real_type (str): 키움 REST API에 정의된 실시간 데이터 타입
            callback (Callable): RealData 또는 dict를 인자로 받는 콜백 함수
        """

        real_type = real_type.upper()
        # Asnyc Callback
        if iscoroutinefunction(callback):
            self._callbacks[real_type] = wrap_async_callback(self._sem, callback)
        # Sync Callback
        else:
            self._callbacks[real_type] = wrap_sync_callback(self._sem, callback)

    def _add_default_callback_on_real_data(self) -> None:
        """
        Add default callback functions on real data receive.
        """

        # Ping
        async def callback_on_ping(msg: dict):
            self._ensure_websocket_connected(action="PING response")
            await self.socket.send(msg)

        self.add_callback_on_real_data(real_type="PING", callback=callback_on_ping)

        # Websocket control responses
        async def callback_on_control(msg: dict):
            self._log_websocket_control_response(msg)

        for real_type in ("LOGIN", "REG", "REMOVE"):
            self.add_callback_on_real_data(real_type=real_type, callback=callback_on_control)

    async def _default_callback_on_real_data(self, msg: dict) -> None:
        self._logger.debug("Unhandled Kiwoom websocket message: %s", msg)

    def _control_error(self, msg: dict) -> KiwoomAPIError:
        trnm = msg.get("trnm", "")
        base = build_api_error(msg)
        info = ErrorInfo(
            code=base.code,
            message=base.message,
            category=base.category,
            action=base.action,
        )
        if trnm == "REG":
            return KiwoomRegisterError(info, body=msg)
        if trnm == "REMOVE":
            return KiwoomRemoveRegisterError(info, body=msg)
        return base

    def _pop_control_waiter(self, trnm: str) -> asyncio.Future | None:
        waiters = self._control_waiters.get(trnm)
        if not waiters:
            return None
        while waiters:
            future = waiters.popleft()
            if not future.done():
                return future
        return None

    def _remove_control_waiter(self, trnm: str, future: asyncio.Future) -> None:
        waiters = self._control_waiters.get(trnm)
        if not waiters:
            return
        with contextlib.suppress(ValueError):
            waiters.remove(future)

    async def _send_control(
        self,
        payload: dict,
        *,
        wait_ack: bool,
        ack_timeout: float,
        logger: LoggerLike,
    ) -> dict | None:
        trnm = payload["trnm"]
        future: asyncio.Future | None = None
        if wait_ack:
            future = asyncio.get_running_loop().create_future()
            self._control_waiters[trnm].append(future)
        try:
            await self.socket.send(payload, logger=logger)
            if future is None:
                return None
            return await asyncio.wait_for(future, ack_timeout)
        except asyncio.TimeoutError as err:
            if future is not None:
                self._remove_control_waiter(trnm, future)
            raise KiwoomAckTimeoutError(trnm=trnm, timeout=ack_timeout) from err
        except Exception:
            if future is not None:
                self._remove_control_waiter(trnm, future)
                if not future.done():
                    future.cancel()
            raise

    def _log_websocket_control_response(
        self,
        msg: dict,
        logger: LoggerLike | None = None,
    ) -> None:
        active_logger = self._resolve_logger(logger)
        trnm = msg.get("trnm", "")
        self.last_control_monotonic_ns = time.monotonic_ns()
        return_code = normalize_error_code(msg.get("return_code", 0))
        if is_success_code(return_code):
            future = self._pop_control_waiter(trnm)
            if future is not None:
                future.set_result(msg)
            active_logger.debug(
                "Kiwoom websocket response trnm=%s return_code=%s",
                trnm,
                return_code,
            )
            return

        error = self._control_error(msg)
        future = self._pop_control_waiter(trnm)
        if future is not None:
            future.set_exception(error)
        active_logger.error(
            "Kiwoom websocket response failed trnm=%s return_code=%s category=%s "
            "action=%s message=%s body=%s",
            trnm,
            error.code,
            error.category,
            error.action,
            error.message,
            msg,
        )

    async def _on_receive_websocket(self) -> None:
        """
        Receive websocket data and dispatch to the callback function.
        Decoder patially checks 'trnm' and 'type' in order to speed up.

        If trnm is "REAL", the argument to callback function is RealData instance.
        Otherwise, the argument to callback function is json dict.

        Raises:
            Exception: Exception raised by the callback function or decoder
        """
        try:
            decoder = msgspec.json.Decoder(type=RealType)
            while not self._stop_event.is_set():
                try:
                    raw: str = await self.queue.get()
                except asyncio.CancelledError:
                    break

                try:
                    msg = decoder.decode(raw)  # partially decoded for speed up
                    if msg.trnm == "REAL":
                        self.last_real_monotonic_ns = time.monotonic_ns()
                        for data in msg.data or []:
                            normalized_type = self._normalize_real_type(data.type)
                            real_data = RealData(
                                self._real_values(data.values),
                                data.type,
                                data.name,
                                data.item,
                            )
                            await self._enqueue_callback(
                                self._callbacks[normalized_type],
                                real_data,
                                real_type=normalized_type,
                                item=data.item,
                            )
                        continue

                    dic = orjson.loads(raw)
                    trnm = self._normalize_real_type(msg.trnm)
                    await self._enqueue_callback(
                        self._callbacks[trnm],
                        dic,
                        real_type=trnm,
                    )

                except Exception as err:
                    raise RuntimeError("Failed to handle websocket data.") from err

                finally:
                    self.queue.task_done()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._logger.exception("Kiwoom websocket dispatch loop stopped.")
            raise

    async def register_real(
        self,
        grp_no: str,
        codes: list[str],
        types: str | list[str],
        refresh: str = "1",
        logger: LoggerLike | None = None,
        wait_ack: bool | None = None,
        ack_timeout: float | None = None,
    ) -> None:
        """
        주어진 그룹번호와 종목 코드에 대해 실시간 데이터를 등록합니다.

        Args:
            grp_no (str): 그룹번호
            codes (list[str]): 종목코드 리스트
            types (str | list[str]): 실시간 데이터 타입 ex) '0B', '0D', '1h', '0s'
            refresh (str, optional): 기존등록유지여부 (기존유지:'1', 신규등록:'0').
            logger (LoggerLike, optional): WebSocket 전송 로그에 사용할 logger
            wait_ack (bool | None): REG ACK를 기다릴지 여부. None이면 API 기본값 사용
            ack_timeout (float | None): ACK timeout. None이면 API 기본값 사용
        """

        active_logger = self._resolve_logger(logger)
        wait_ack = self._wait_ack if wait_ack is None else wait_ack
        ack_timeout = self._ack_timeout if ack_timeout is None else ack_timeout
        if isinstance(types, str):
            types = [types]
        assert len(codes) <= 100, f"Max 100 codes per group, got {len(codes)} codes."
        self._ensure_websocket_connected(action="register_real")
        async with self._control_lock:
            await self._send_control(
                {
                    "trnm": "REG",
                    "grp_no": grp_no,
                    "refresh": refresh,
                    "data": [
                        {
                            "item": codes,
                            "type": types,
                        }
                    ],
                },
                wait_ack=wait_ack,
                ack_timeout=ack_timeout,
                logger=active_logger,
            )

    async def register_tick(
        self,
        grp_no: str,
        codes: list[str],
        refresh: str = "1",
        logger: LoggerLike | None = None,
        wait_ack: bool | None = None,
        ack_timeout: float | None = None,
    ) -> None:
        """
        주어진 그룹번호와 종목 코드에 대해 주식체결 데이터를 등록합니다. (타입 '0B')

        Args:
            grp_no (str): 그룹번호
            codes (list[str]): 종목코드 리스트
            refresh (str, optional): 기존등록유지여부 (기존유지:'1', 신규등록:'0').
            logger (LoggerLike, optional): WebSocket 전송 로그에 사용할 logger
            wait_ack (bool | None): REG ACK를 기다릴지 여부. None이면 API 기본값 사용
            ack_timeout (float | None): ACK timeout. None이면 API 기본값 사용
        """

        await self.register_real(
            grp_no,
            codes,
            "0B",
            refresh,
            logger=logger,
            wait_ack=wait_ack,
            ack_timeout=ack_timeout,
        )

    async def register_hoga(
        self,
        grp_no: str,
        codes: list[str],
        refresh: str = "1",
        logger: LoggerLike | None = None,
        wait_ack: bool | None = None,
        ack_timeout: float | None = None,
    ) -> None:
        """
        주어진 그룹번호와 종목 코드에 대해 주식호가잔량 데이터를 등록합니다. (타입 '0D')

        Args:
            grp_no (str): 그룹번호
            codes (list[str]): 종목코드 리스트
            refresh (str, optional): 기존등록유지여부 (기존유지:'1', 신규등록:'0').
            logger (LoggerLike, optional): WebSocket 전송 로그에 사용할 logger
            wait_ack (bool | None): REG ACK를 기다릴지 여부. None이면 API 기본값 사용
            ack_timeout (float | None): ACK timeout. None이면 API 기본값 사용
        """

        await self.register_real(
            grp_no,
            codes,
            "0D",
            refresh,
            logger=logger,
            wait_ack=wait_ack,
            ack_timeout=ack_timeout,
        )

    async def register_vi(
        self,
        grp_no: str,
        codes: list[str],
        refresh: str = "1",
        logger: LoggerLike | None = None,
        wait_ack: bool | None = None,
        ack_timeout: float | None = None,
    ) -> None:
        """
        주어진 그룹번호와 종목 코드에 대해 VI발동/해제 데이터를 등록합니다. (타입 '1h')

        Args:
            grp_no (str): 그룹번호
            codes (list[str]): 종목코드 리스트
            refresh (str, optional): 기존등록유지여부 (기존유지:'1', 신규등록:'0').
            logger (LoggerLike, optional): WebSocket 전송 로그에 사용할 logger
            wait_ack (bool | None): REG ACK를 기다릴지 여부. None이면 API 기본값 사용
            ack_timeout (float | None): ACK timeout. None이면 API 기본값 사용
        """

        await self.register_real(
            grp_no,
            codes,
            "1h",
            refresh,
            logger=logger,
            wait_ack=wait_ack,
            ack_timeout=ack_timeout,
        )

    async def register_market_open_time(
        self,
        grp_no: str,
        refresh: str = "1",
        logger: LoggerLike | None = None,
        wait_ack: bool | None = None,
        ack_timeout: float | None = None,
    ) -> None:
        """
        장시작시간 데이터를 등록합니다. (타입 '0s')

        Args:
            grp_no (str): 그룹번호
            refresh (str, optional): 기존등록유지여부 (기존유지:'1', 신규등록:'0').
            logger (LoggerLike, optional): WebSocket 전송 로그에 사용할 logger
            wait_ack (bool | None): REG ACK를 기다릴지 여부. None이면 API 기본값 사용
            ack_timeout (float | None): ACK timeout. None이면 API 기본값 사용
        """

        await self.register_real(
            grp_no,
            [""],
            "0s",
            refresh,
            logger=logger,
            wait_ack=wait_ack,
            ack_timeout=ack_timeout,
        )

    async def remove_register(
        self,
        grp_no: str,
        codes: list[str],
        type: str | list[str],
        logger: LoggerLike | None = None,
        wait_ack: bool | None = None,
        ack_timeout: float | None = None,
    ) -> None:
        """
        주어진 그룹번호와 실시간 데이터 타입에 대해 등록된 데이터를 제거합니다.

        Args:
            grp_no (str): 그룹번호
            type (str | list[str]): 실시간 데이터 타입 ex) '0B', '0D', '1h', '0s'
            logger (LoggerLike, optional): WebSocket 전송 로그에 사용할 logger
            wait_ack (bool | None): REMOVE ACK를 기다릴지 여부. None이면 API 기본값 사용
            ack_timeout (float | None): ACK timeout. None이면 API 기본값 사용
        """
        if not grp_no or not type:
            return
        active_logger = self._resolve_logger(logger)
        wait_ack = self._wait_ack if wait_ack is None else wait_ack
        ack_timeout = self._ack_timeout if ack_timeout is None else ack_timeout
        if isinstance(type, str):
            type = [type]
        self._ensure_websocket_connected(action="remove_register")
        async with self._control_lock:
            await self._send_control(
                {
                    "trnm": "REMOVE",
                    "grp_no": grp_no,
                    "refresh": "",
                    "data": [{"item": codes, "type": type}],
                },
                wait_ack=wait_ack,
                ack_timeout=ack_timeout,
                logger=active_logger,
            )
