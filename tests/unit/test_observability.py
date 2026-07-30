import json
import logging
from uuid import UUID

import structlog

from libs.observability import configure_logging


def test_logging_is_json_stdout_with_correlation_context(capsys: object) -> None:
    configure_logging("order-service", level="INFO")
    structlog.get_logger().info(
        "saga_transition",
        message_id=UUID("00000000-0000-0000-0000-000000000401"),
        order_id=UUID("00000000-0000-0000-0000-000000000402"),
        correlation_id=UUID("00000000-0000-0000-0000-000000000403"),
    )

    line = capsys.readouterr().out.strip()  # type: ignore[attr-defined]
    record = json.loads(line)

    assert record["event"] == "saga_transition"
    assert record["level"] == "info"
    assert record["service"] == "order-service"
    assert record["message_id"] == "00000000-0000-0000-0000-000000000401"
    assert "timestamp" in record


def test_uvicorn_and_stdlib_logs_are_json_stdout(capsys: object) -> None:
    configure_logging("payment-service", level="INFO")

    logging.getLogger("uvicorn.error").warning("worker restarted")

    captured = capsys.readouterr()  # type: ignore[attr-defined]
    record = json.loads(captured.out.strip())
    assert captured.err == ""
    assert record["event"] == "worker restarted"
    assert record["level"] == "warning"
    assert record["service"] == "payment-service"
    assert record["logger"] == "uvicorn.error"
    assert "timestamp" in record
