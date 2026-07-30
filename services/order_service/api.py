"""HTTP API for creating and inspecting Saga orders."""

from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Response, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.types import Lifespan

from libs.runtime import Readiness, add_health_routes
from services.order_service.models import Order
from services.order_service.repository import (
    IdempotencyConflict,
    NewOrder,
    create_order,
    get_order,
)


class OrderCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sku: str = Field(min_length=1, max_length=200)
    quantity: int = Field(strict=True, gt=0)
    amount_minor: int = Field(strict=True, gt=0)
    currency: str = Field(pattern=r"^[A-Z]{3}$")


class OrderResponse(BaseModel):
    order_id: UUID
    status: str
    sku: str
    quantity: int
    amount_minor: int
    currency: str


SessionFactory = async_sessionmaker[AsyncSession]


def create_app(
    session_factory: SessionFactory,
    *,
    lifespan: Lifespan[FastAPI] | None = None,
    readiness: Readiness | None = None,
) -> FastAPI:
    app = FastAPI(title="Order Service", version="1.0.0", lifespan=lifespan)
    if readiness is not None:
        add_health_routes(app, readiness)

    async def session_dependency() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    def to_response(order: Order) -> OrderResponse:
        return OrderResponse(
            order_id=order.id,
            status=order.status,
            sku=order.sku,
            quantity=order.quantity,
            amount_minor=order.amount_minor,
            currency=order.currency,
        )

    @app.post(
        "/orders",
        response_model=OrderResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def post_order(
        payload: OrderCreate,
        response: Response,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
        session: Annotated[AsyncSession, Depends(session_dependency)],
    ) -> OrderResponse:
        try:
            order, created = await create_order(
                session,
                NewOrder(**payload.model_dump()),
                idempotency_key,
            )
        except IdempotencyConflict as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        if not created:
            response.status_code = status.HTTP_200_OK
        return to_response(order)

    @app.get("/orders/{order_id}", response_model=OrderResponse)
    async def read_order(
        order_id: UUID,
        session: Annotated[AsyncSession, Depends(session_dependency)],
    ) -> OrderResponse:
        order = await get_order(session, order_id)
        if order is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
        return to_response(order)

    return app
