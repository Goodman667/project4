from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# Keep payload simple: string or normal JSON-like data.
MessagePayload = str | int | float | bool | None | dict[str, Any] | list[Any]


class MessageStatus(str, Enum):
    QUEUED = "queued"
    IN_FLIGHT = "in_flight"
    ACKED = "acked"
    DEAD_LETTER = "dead_letter"


class Message(BaseModel):
    """Core message model used by the broker."""

    model_config = ConfigDict(extra="forbid")

    id: str
    topic: str
    payload: MessagePayload
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    retry_count: int = Field(default=0, ge=0)
    status: MessageStatus = MessageStatus.QUEUED


class PublishMessageRequest(BaseModel):
    """Request body for publishing a new message."""

    model_config = ConfigDict(extra="forbid")

    payload: MessagePayload


class PublishMessageResponse(BaseModel):
    """Response returned after publish succeeds."""

    model_config = ConfigDict(extra="forbid")

    message: Message


class PullMessageResponse(BaseModel):
    """Response returned when a consumer pulls one message."""

    model_config = ConfigDict(extra="forbid")

    delivery_id: str
    consumer_id: str
    message: Message


class ActionResponse(BaseModel):
    """Generic response for ack / nack operations."""

    model_config = ConfigDict(extra="forbid")

    success: bool
    detail: str


@dataclass(slots=True)
class DeliveryRecord:
    delivery_id: str
    consumer_id: str
    message: Message
    delivered_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


@dataclass(slots=True)
class TopicMetrics:
    published_count: int = 0
    delivered_count: int = 0
    acked_count: int = 0
    nacked_count: int = 0
    requeued_count: int = 0
    dead_lettered_count: int = 0


@dataclass(slots=True)
class TopicState:
    name: str
    ready_queue: deque[Message] = field(default_factory=deque)
    inflight: dict[str, DeliveryRecord] = field(default_factory=dict)
    dead_letter_queue: deque[Message] = field(default_factory=deque)
    metrics: TopicMetrics = field(default_factory=TopicMetrics)

    @property
    def queue_depth(self) -> int:
        return len(self.ready_queue)

    @property
    def inflight_count(self) -> int:
        return len(self.inflight)

    @property
    def dead_letter_count(self) -> int:
        return len(self.dead_letter_queue)
