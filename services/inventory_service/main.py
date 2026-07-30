"""Inventory Service ASGI entrypoint."""

from fastapi import FastAPI

app = FastAPI(title="Inventory Service", version="1.0.0")
