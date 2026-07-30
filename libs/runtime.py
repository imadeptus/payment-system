"""Shared health state for independently deployable services."""

from dataclasses import dataclass

from fastapi import FastAPI, Response, status


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
