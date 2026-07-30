"""Versioned message envelope shared by all services."""

from typing import Generic, Literal, TypeVar
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

PayloadT = TypeVar("PayloadT", bound=BaseModel)


class MessageEnvelope(BaseModel, Generic[PayloadT]):
    """Immutable metadata and typed payload for at-least-once delivery."""

    model_config = ConfigDict(frozen=True)

    message_id: UUID
    message_type: str = Field(min_length=1)
    schema_version: Literal[1] = 1
    occurred_at: AwareDatetime
    correlation_id: UUID
    causation_id: UUID | None
    order_id: UUID
    payload: PayloadT
