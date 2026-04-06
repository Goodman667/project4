from __future__ import annotations

from typing import Any
from uuid import uuid4

from app.models import DeliveryRecord, Message, MessageStatus, TopicState


class TopicQueueError(Exception):
    """Base exception for queue manager errors."""


class TopicNotFoundError(TopicQueueError):
    """Raised when a topic does not exist."""


class MessageNotInFlightError(TopicQueueError):
    """Raised when ack/nack targets a message that is not in flight."""


class TopicQueueManager:
    """Simple in-memory FIFO queue manager grouped by topic."""

    def __init__(self) -> None:
        self._topics: dict[str, TopicState] = {}
        self._inflight_index: dict[str, str] = {}

    def create_topic(self, topic: str) -> TopicState:
        topic_name = self._validate_topic_name(topic)
        if topic_name not in self._topics:
            self._topics[topic_name] = TopicState(name=topic_name)
        return self._topics[topic_name]

    def ensure_topic(self, topic: str) -> TopicState:
        """Compatibility helper for existing skeleton code."""

        return self.create_topic(topic)

    def get_topic(self, topic: str) -> TopicState | None:
        return self._topics.get(topic)

    def list_topics(self) -> list[str]:
        return sorted(self._topics.keys())

    def enqueue(self, topic: str, message: Message) -> Message:
        topic_state = self._require_topic(topic)
        if message.topic != topic_state.name:
            raise ValueError(
                f"Message topic '{message.topic}' does not match target topic '{topic_state.name}'."
            )

        message.status = MessageStatus.QUEUED
        topic_state.ready_queue.append(message)
        topic_state.metrics.published_count += 1
        return message

    def dequeue(self, topic: str) -> Message | None:
        topic_state = self._require_topic(topic)
        if not topic_state.ready_queue:
            return None

        message = topic_state.ready_queue.popleft()
        message.status = MessageStatus.IN_FLIGHT

        topic_state.inflight[message.id] = DeliveryRecord(
            delivery_id=message.id,
            consumer_id="",
            message=message,
        )
        topic_state.metrics.delivered_count += 1
        self._inflight_index[message.id] = topic_state.name
        return message

    def ack(self, message_id: str) -> Message:
        topic_state, record = self._pop_inflight_record(message_id)
        record.message.status = MessageStatus.ACKED
        topic_state.metrics.acked_count += 1
        return record.message

    def nack(self, message_id: str) -> Message:
        topic_state, record = self._pop_inflight_record(message_id)
        message = record.message
        message.retry_count += 1
        message.status = MessageStatus.QUEUED

        topic_state.ready_queue.append(message)
        topic_state.metrics.nacked_count += 1
        topic_state.metrics.requeued_count += 1
        return message

    def get_queue_depth(self, topic: str) -> int:
        topic_state = self._require_topic(topic)
        return topic_state.queue_depth

    def publish(self, topic: str, payload: Any) -> Message:
        """Small helper for the next API step."""

        self._require_topic(topic)
        message = Message(
            id=str(uuid4()),
            topic=topic,
            payload=payload,
        )
        return self.enqueue(topic, message)

    def pull(self, topic: str, consumer_id: str = "") -> dict[str, Any] | None:
        """Compatibility helper for the future pull API."""

        message = self.dequeue(topic)
        if message is None:
            return None

        topic_state = self._require_topic(topic)
        record = topic_state.inflight[message.id]
        record.consumer_id = consumer_id
        return {
            "delivery_id": record.delivery_id,
            "consumer_id": consumer_id,
            "message": message,
        }

    def _require_topic(self, topic: str) -> TopicState:
        topic_name = self._validate_topic_name(topic)
        topic_state = self._topics.get(topic_name)
        if topic_state is None:
            raise TopicNotFoundError(f"Topic '{topic_name}' does not exist.")
        return topic_state

    def _pop_inflight_record(self, message_id: str) -> tuple[TopicState, DeliveryRecord]:
        topic_name = self._inflight_index.pop(message_id, None)
        if topic_name is None:
            raise MessageNotInFlightError(
                f"Message '{message_id}' is not currently in flight."
            )

        topic_state = self._topics[topic_name]
        record = topic_state.inflight.pop(message_id, None)
        if record is None:
            raise MessageNotInFlightError(
                f"Message '{message_id}' is not currently in flight."
            )
        return topic_state, record

    @staticmethod
    def _validate_topic_name(topic: str) -> str:
        topic_name = topic.strip()
        if not topic_name:
            raise ValueError("Topic name cannot be empty.")
        return topic_name


# Keep the original name so existing imports continue to work.
QueueManager = TopicQueueManager
