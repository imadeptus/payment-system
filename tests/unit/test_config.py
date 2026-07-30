import httpx
import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from libs.contracts import topic_for
from libs.runtime import Readiness, add_health_routes
from services.inventory_service.config import InventorySettings
from services.order_service.config import OrderSettings
from services.payment_service.config import PaymentSettings


@pytest.mark.parametrize(
    ("settings_class", "database_field"),
    [
        (OrderSettings, "order_database_url"),
        (PaymentSettings, "payment_database_url"),
        (InventorySettings, "inventory_database_url"),
    ],
)
def test_service_settings_require_database_and_kafka_urls(
    settings_class: type,
    database_field: str,
) -> None:
    with pytest.raises(ValidationError):
        settings_class(_env_file=None)

    settings = settings_class(
        _env_file=None,
        **{
            database_field: "postgresql+asyncpg://user:secret@db/service",
            "kafka_bootstrap_servers": "kafka:9092",
        },
    )

    assert "secret" not in repr(settings)


def test_topic_names_are_overridden_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORDER_DATABASE_URL", "postgresql+asyncpg://u:p@db/orders")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("PAYMENT_COMMANDS_TOPIC", "custom.payment.commands")

    settings = OrderSettings(_env_file=None)

    assert settings.payment_commands_topic == "custom.payment.commands"
    assert topic_for("AuthorizePayment") == "custom.payment.commands"


@pytest.mark.asyncio
async def test_readiness_requires_both_database_and_kafka() -> None:
    readiness = Readiness()
    app = FastAPI()
    add_health_routes(app, readiness)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        live = await client.get("/health/live")
        before = await client.get("/health/ready")
        readiness.database = True
        middle = await client.get("/health/ready")
        readiness.kafka = True
        after = await client.get("/health/ready")

    assert live.status_code == 200
    assert before.status_code == 503
    assert middle.status_code == 503
    assert after.status_code == 200
