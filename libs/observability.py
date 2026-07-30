"""Structured JSON stdout logging."""

import json
import logging
import sys
from typing import Any

import structlog
from structlog.typing import EventDict, Processor, WrappedLogger


def _json_dumps(value: Any, **kwargs: Any) -> str:
    kwargs["default"] = str
    return json.dumps(value, **kwargs)


def configure_logging(service: str, *, level: str = "INFO") -> None:
    """Configure structlog without file handlers or secret-enriching tracebacks."""

    numeric_level = getattr(logging, level.upper(), logging.INFO)

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
