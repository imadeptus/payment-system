"""Order Service ASGI entrypoint."""

from services.order_service.api import create_app
from services.order_service.config import OrderSettings
from services.order_service.db import build_database

settings = OrderSettings()
engine, session_factory = build_database(settings.order_database_url.get_secret_value())
app = create_app(session_factory)
