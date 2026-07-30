"""Shared health state for independently deployable services."""

import asyncio
import os
import signal
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import structlog
from fastapi import FastAPI, Response, status

logger = structlog.get_logger()


@dataclass(slots=True)
class Readiness:
    database: bool = False
    kafka: bool = False

    @property
    def ready(self) -> bool:
        return self.database and self.kafka


def add_health_routes(app: FastAPI, readiness: Readiness) -> None:
    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live"}

    @app.get("/health/ready")
    async def ready(response: Response) -> dict[str, object]:
        if not readiness.ready:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "ready" if readiness.ready else "not_ready",
            "database": readiness.database,
            "kafka": readiness.kafka,
        }


def terminate_process() -> None:
    """Ask the process manager to restart a runtime with a dead worker."""

    os.kill(os.getpid(), signal.SIGTERM)


async def supervise_tasks(
    tasks: Sequence[asyncio.Task[None]],
    readiness: Readiness,
    *,
    terminate: Callable[[], None] = terminate_process,
) -> None:
    """Degrade readiness and terminate when a long-running worker exits."""

    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    failed = next(iter(done))
    error_type = "UnexpectedWorkerExit"
    if not failed.cancelled():
        exception = failed.exception()
        if exception is not None:
            error_type = type(exception).__name__
    readiness.kafka = False
    logger.error(
        "background_task_failed",
        task=failed.get_name(),
        error_type=error_type,
    )
    terminate()


async def wait_for_worker_start(
    started: asyncio.Event,
    tasks: Sequence[asyncio.Task[None]],
) -> None:
    """Wait for consumer readiness while surfacing early worker failure."""

    started_wait = asyncio.create_task(started.wait(), name="consumer-started")
    try:
        done, _ = await asyncio.wait(
            [started_wait, *tasks],
            return_when=asyncio.FIRST_COMPLETED,
        )
        if started_wait in done:
            return
        failed = next(iter(done))
        if failed.cancelled():
            raise RuntimeError(f"Worker cancelled during startup: {failed.get_name()}")
        exception = failed.exception()
        if exception is not None:
            raise exception
        raise RuntimeError(f"Worker exited during startup: {failed.get_name()}")
    finally:
        if not started_wait.done():
            started_wait.cancel()
        await asyncio.gather(started_wait, return_exceptions=True)
