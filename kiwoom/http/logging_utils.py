from __future__ import annotations

import logging
from typing import Any, Protocol

__all__ = ["LoggerLike", "get_logger"]


class LoggerLike(Protocol):
    def debug(self, msg: object, *args: Any, **kwargs: Any) -> None:
        ...

    def info(self, msg: object, *args: Any, **kwargs: Any) -> None:
        ...

    def warning(self, msg: object, *args: Any, **kwargs: Any) -> None:
        ...

    def error(self, msg: object, *args: Any, **kwargs: Any) -> None:
        ...

    def exception(self, msg: object, *args: Any, **kwargs: Any) -> None:
        ...


def get_logger(name: str, logger: LoggerLike | None = None) -> LoggerLike:
    return logger if logger is not None else logging.getLogger(name)
