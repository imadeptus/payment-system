"""Structured JSON stdout logging."""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

import structlog
from structlog.typing import EventDict, Processor, WrappedLogger


def _json_dumps(value: Any, **kwargs: Any) -> str:
    kwargs["default"] = str
    return json.dumps(value, **kwargs)


class JsonLogFormatter(logging.Formatter):
    """Render stdlib and Uvicorn records with the same JSON-only contract."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        event: dict[str, Any] = {
            "event": record.getMessage(),
            "level": record.levelname.lower(),
            "timestamp": datetime.now(UTC).isoformat(),
            "service": self._service,
            "logger": record.name,
        }
        if record.exc_info is not None:
            exception_type = record.exc_info[0]
            if exception_type is not None:
                event["error_type"] = exception_type.__name__
        return _json_dumps(event, separators=(",", ":"), sort_keys=True)


def configure_logging(service: str, *, level: str = "INFO") -> None:
    """Configure structlog without file handlers or secret-enriching tracebacks."""

    numeric_level = getattr(logging, level.upper(), logging.INFO)
    stdlib_handler = logging.StreamHandler(sys.stdout)
    stdlib_handler.setFormatter(JsonLogFormatter(service))
    root_logger = logging.getLogger()
    root_logger.handlers = [stdlib_handler]
    root_logger.setLevel(numeric_level)
    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        stdlib_logger = logging.getLogger(logger_name)
        stdlib_logger.handlers = []
        stdlib_logger.propagate = True
        stdlib_logger.setLevel(numeric_level)

    def add_service(
        _: WrappedLogger,
        __: str,
        event_dict: EventDict,
    ) -> EventDict:
        event_dict["service"] = service
        return event_dict

    processors: list[Processor] = [
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        add_service,
        structlog.processors.JSONRenderer(serializer=_json_dumps),
    ]
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=False,
    )
