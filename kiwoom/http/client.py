import asyncio
import warnings
from itertools import chain
from os.path import isfile
from typing import Callable, Optional

import aiohttp
from aiohttp import ClientSession

from kiwoom.config.http import (
    HTTP_CONNECT_TIMEOUT,
    HTTP_READ_TIMEOUT,
    HTTP_TCP_CONNECTORS,
    HTTP_TOTAL_TIMEOUT,
    State,
)
from kiwoom.http.debug import debugger, dumps
from kiwoom.http.errors import build_api_error, is_success_code, normalize_error_code
from kiwoom.http.logging_utils import LoggerLike, get_logger
from kiwoom.http.response import Response
from kiwoom.http.utils import RateLimiter

__all__ = ["Client"]

module_logger = get_logger(__name__)


class Client:
    def __init__(
        self,
        host: str,
        appkey: str,
        secretkey: str,
        logger: LoggerLike | None = None,
    ):
        """
        Initialize Client instance.

        Args:
            host (str): domain
            appkey (str): file path or raw appkey
            secretkey (str): file path or raw secretkey
            logger (LoggerLike, optional): logger used by this client.
        """
        self.host: str = host
        self.debugging: bool = False
        self._logger = logger if logger is not None else module_logger

        self._auth: str = ""
        self._appkey: str = appkey
        self._secretkey: str = secretkey
        self._headers: Optional[dict] = None

        self._state_http = State.CLOSED
        self._ready_event = asyncio.Event()
        self._limiter: RateLimiter = RateLimiter()
        self._session: ClientSession = None
        self._rest_tr_deprecation_warned = False

    def set_logger(self, logger: LoggerLike | None = None) -> None:
        """
        Set logger used by this client. If omitted, module-level logger is used.
        """
        self._logger = logger if logger is not None else module_logger

    def _resolve_logger(self, logger: LoggerLike | None = None) -> LoggerLike:
        return logger if logger is not None else self._logger

    def _warn_rest_tr_deprecated(self) -> None:
        """
        Warn once per client instance that built-in REST TR request helpers are deprecated.

        The package keeps these methods for backward compatibility, but new code should
        prefer an external REST TR adapter and use this package primarily for token and
        realtime WebSocket workflows.
        """
        if self._rest_tr_deprecation_warned:
            return
        self._rest_tr_deprecation_warned = True
        warnings.warn(
            "kiwoom-restful REST TR helpers are deprecated and kept for backward "
            "compatibility. Prefer an external REST TR client; use this package "
            "primarily for token and realtime WebSocket workflows.",
            DeprecationWarning,
            stacklevel=3,
        )

    async def connect(
        self,
        appkey: str,
        secretkey: str,
        headers: Optional[dict] = None,
        logger: LoggerLike | None = None,
    ) -> None:
        """
        Connect to Kiwoom REST API server and receive token.

        Args:
            appkey (str): file path or raw appkey
            secretkey (str): file path or raw secretkey
            headers (dict): 서버 연결 시 사용할 헤더 (User-Agent 등)
            logger (LoggerLike, optional): logger used for this connection.
        """
        if logger is not None:
            self.set_logger(logger)

        if isfile(appkey):
            with open(appkey, "r") as f:
                self._appkey = f.read().strip()
        if isfile(secretkey):
            with open(secretkey, "r") as f:
                self._secretkey = f.read().strip()
        if headers:
            self._headers = headers

        # Already connected
        if self._session and not self._session.closed:
            return

        # Establish HTTP session
        self._ready_event.clear()
        self._session = ClientSession(
            headers=self._headers,
            timeout=aiohttp.ClientTimeout(
                total=HTTP_TOTAL_TIMEOUT,
                sock_connect=HTTP_CONNECT_TIMEOUT,
                sock_read=HTTP_READ_TIMEOUT,
            ),
            connector=aiohttp.TCPConnector(limit=HTTP_TCP_CONNECTORS, enable_cleanup_closed=True),
        )

        # Request token
        endpoint = "/oauth2/token"
        api_id = ""
        headers = self.headers(api_id)
        data = {
            "grant_type": "client_credentials",
            "appkey": self._appkey,
            "secretkey": self._secretkey,
        }
        async with self._session.post(self.host + endpoint, headers=headers, json=data) as res:
            res.raise_for_status()
            body = await res.json()
            resp = Response(res.url, res.status, res.headers, body)

        # Set token
        if "token" not in body:
            msg = dumps(self, endpoint, api_id, headers, data, resp)
            if "return_code" in body:
                raise build_api_error(body, endpoint=endpoint, api_id=api_id, detail=msg)
            raise RuntimeError(f"Failed to get token: {msg}")
        token = body["token"]
        self._auth = f"Bearer {token}"
        self._session.headers.update(
            {
                "Content-Type": "application/json;charset=UTF-8",
                "authorization": self._auth,
            }
        )
        self._state_http = State.CONNECTED
        self._ready_event.set()

    async def close(self, logger: LoggerLike | None = None) -> None:
        """
        Close HTTP session.
        """
        if logger is not None:
            self.set_logger(logger)
        self._ready_event.clear()
        if self._session:
            await asyncio.shield(self._session.close())

        self._auth = ""
        self._session = None
        self._state_http = State.CLOSED

    async def refresh_auth(self, logger: LoggerLike | None = None) -> None:
        """
        Recreate the HTTP session and request a fresh access token.
        """
        if logger is not None:
            self.set_logger(logger)
        await self.close(logger=logger)
        await self.connect(self._appkey, self._secretkey, self._headers, logger=logger)

    def token(self) -> str:
        """
        Returns token if available, otherwise empty string.

        Raises:
            ValueError: Invalid token.

        Returns:
            str: token
        """
        if not self._auth:
            return ""
        if "Bearer " in self._auth:
            return self._auth[len("Bearer ") :]
        raise ValueError(f"Invalid token: {self._auth}")

    def headers(
        self, api_id: str, cont_yn: str = "N", next_key: str = "", headers: dict | None = None
    ) -> dict[str, str]:
        """
        Generate headers for the request.

        Args:
            api_id (str): api_id in Kiwoom API
            cont_yn (str, optional): cont_yn in Kiwoom API
            next_key (str, optional): next_key in Kiwoom API
            headers (dict | None, optional): headers to be updated with

        Returns:
            dict[str, str]: headers
        """
        base = {
            # 'Content-Type': 'application/json;charset=UTF-8',
            # 'authorization': self._auth,
            "cont-yn": cont_yn,
            "next-key": next_key,
            "api-id": api_id,
        }
        if headers is not None:
            headers.update(base)
            return headers
        return base

    async def ready(self):
        """
        Wait until request limit is lifted and connection is established.

        Raises:
            RuntimeError: Connection timeout.
        """
        try:
            await asyncio.wait_for(self._ready_event.wait(), HTTP_TOTAL_TIMEOUT)
        except asyncio.TimeoutError as err:
            msg = f"Connection timeout: waited for {HTTP_TOTAL_TIMEOUT} seconds."
            raise RuntimeError(msg) from err
        await self._limiter.acquire()

    @debugger
    async def post(
        self,
        endpoint: str,
        api_id: str,
        headers: dict | None = None,
        data: dict | None = None,
        logger: LoggerLike | None = None,
    ) -> aiohttp.ClientResponse:
        """
        Post request to the server, but using client.request function is recommended.
        Request limit and connection status are checked globally and automatically.

        Args:
            endpoint (str): endpoint to Kiwoom REST API server
            api_id (str): api id
            headers (dict | None, optional): headers of the request.
            data (dict | None, optional): data to be sent in json format
            logger (LoggerLike, optional): logger used for this request.

        Returns:
            aiohttp.ClientResponse: async response from the server,
                but this will be converted to kiwoom.http.response.Response by debugger.
        """

        # Warn not connected
        if not self._state_http == State.CONNECTED:
            warnings.warn("Not connected, wait for timeout...", RuntimeWarning, stacklevel=1)

        # Wait connection and request limits
        await self.ready()

        # Post Request
        if headers is None:
            headers = self.headers(api_id)
        return await self._session.post(self.host + endpoint, headers=headers, json=data)

    async def request(
        self,
        endpoint: str,
        api_id: str,
        headers: dict | None = None,
        data: dict | None = None,
        logger: LoggerLike | None = None,
        _auth_retries: int = 0,
    ) -> Response:
        """
        Requests to the server and returns response with error handling.

        Deprecated:
            Built-in REST TR request helpers are retained for backward compatibility.
            Prefer an external REST TR client for new code.

        Args:
            endpoint (str): endpoint of the server
            api_id (str): api id
            headers (dict | None, optional): headers of the request. Defaults to None.
            data (dict | None, optional): data of the request. Defaults to None.
            logger (LoggerLike, optional): logger used for this request.

        Raises:
            KiwoomAPIError: Kiwoom API returned a non-success return_code.

        Returns:
            Response: response wrapped by kiwoom.http.response.Response
        """

        self._warn_rest_tr_deprecated()
        active_logger = self._resolve_logger(logger)
        res: Response = await self.post(
            endpoint,
            api_id,
            headers=headers,
            data=data,
            logger=active_logger,
        )
        body = res.json()
        if "return_code" not in body:
            msg = dumps(self, endpoint, api_id, headers, data, res)
            raise RuntimeError(f"Kiwoom response has no return_code: {msg}")

        return_code = normalize_error_code(body["return_code"])
        if is_success_code(return_code):
            # 0: Success, 20: No Data
            return res

        detail = "" if self.debugging else dumps(self, endpoint, api_id, headers, data, res)
        error = build_api_error(body, endpoint=endpoint, api_id=api_id, detail=detail)
        active_logger.warning(
            "Kiwoom API request failed api_id=%s endpoint=%s return_code=%s "
            "category=%s action=%s message=%s",
            api_id,
            endpoint,
            error.code,
            error.category,
            error.action,
            error.message,
        )

        if error.should_refresh_token and _auth_retries < 1:
            active_logger.warning(
                "Kiwoom API auth error; refreshing token api_id=%s endpoint=%s return_code=%s",
                api_id,
                endpoint,
                error.code,
            )
            await self.refresh_auth(logger=active_logger)
            return await self.request(
                endpoint,
                api_id,
                headers=headers,
                data=data,
                logger=active_logger,
                _auth_retries=_auth_retries + 1,
            )

        raise error

    async def request_until(
        self,
        should_continue: Callable,
        endpoint: str,
        api_id: str,
        headers: dict | None = None,
        data: dict | None = None,
        logger: LoggerLike | None = None,
    ) -> dict:
        """
        Request until 'cont-yn' in response header is 'Y',
        and should_continue(body) evaluates to True.

        Deprecated:
            Built-in REST TR pagination helpers are retained for backward compatibility.
            Prefer an external REST TR client for new code.

        Args:
            should_continue (Callable):
                callable that takes body(dict) and
                returns boolean value to request again or not
            endpoint (str):
                endpoint of the server
            api_id (str):
                api id
            headers (dict | None, optional):
                headers of the request. Defaults to None.
            data (dict | None, optional):
                data of the request. Defaults to None.
            logger (LoggerLike, optional):
                logger used for this request chain.

        Returns:
            dict: response body
        """

        # Initial request
        res = await self.request(endpoint, api_id, headers=headers, data=data, logger=logger)
        body = res.json()

        # If condition to chain is not met
        if callable(should_continue) and not should_continue(body):
            return body

        # Extract list data only
        bodies = dict()
        for key in body.keys():
            if isinstance(body[key], list):
                bodies[key] = [body[key]]
                continue
            bodies[key] = body[key]

        # Rercursive call
        while res.headers.get("cont-yn") == "Y" and should_continue(body):
            next_key = res.headers.get("next-key")
            headers = self.headers(api_id, cont_yn="Y", next_key=next_key, headers=headers)

            # Continue request
            res = await self.request(endpoint, api_id, headers=headers, data=data, logger=logger)
            body = res.json()

            # Append list data
            for key in body.keys():
                if isinstance(body[key], list):
                    bodies[key].append(body[key])

        # Flatten list data as if it was one list
        for key in bodies:
            if isinstance(bodies[key], list):
                bodies[key] = list(chain.from_iterable(bodies[key]))
        return bodies
