from __future__ import annotations

import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from app.queue_manager import QueueManager


@dataclass(slots=True)
class PublishEvent:
    """One publish event kept in a short rolling window."""

    timestamp: float
    byte_size: int


@dataclass(slots=True)
class TopicMonitoringState:
    """Per-topic counters stored fully in memory."""

    total_published: int = 0
    total_consumed: int = 0
    total_acked: int = 0
    total_nacked: int = 0
    total_dead_lettered: int = 0
    total_retries: int = 0
    bytes_received: int = 0
    current_queue_depth: int = 0
    total_ack_latency_seconds: float = 0.0
    recent_publish_events: deque[PublishEvent] = field(default_factory=deque)


class MonitoringService:
    """
    Low-complexity per-topic monitoring.

    - Counters live in memory only
    - Rate and throughput are based on recent publish events
    - Queue depth can be updated manually or refreshed from queue_manager
    """

    def __init__(
        self,
        queue_manager: QueueManager | None = None,
        rate_window_seconds: int = 60,
        burst_threshold_messages_per_second: float = 0.1,
    ) -> None:
        if rate_window_seconds <= 0:
            raise ValueError("rate_window_seconds must be greater than 0.")
        if burst_threshold_messages_per_second < 0:
            raise ValueError(
                "burst_threshold_messages_per_second must be greater than or equal to 0."
            )

        self.queue_manager = queue_manager
        self.rate_window_seconds = rate_window_seconds
        self.burst_threshold_messages_per_second = burst_threshold_messages_per_second
        self._topics: dict[str, TopicMonitoringState] = {}

    def ensure_topic(self, topic: str) -> TopicMonitoringState:
        topic_name = self._validate_topic_name(topic)
        if topic_name not in self._topics:
            self._topics[topic_name] = TopicMonitoringState()
        return self._topics[topic_name]

    def record_publish(
        self,
        topic: str,
        payload: Any,
        queue_depth: int | None = None,
        timestamp: float | None = None,
    ) -> None:
        topic_name = self._validate_topic_name(topic)
        state = self.ensure_topic(topic_name)
        event_time = self._resolve_time(timestamp)
        byte_size = self._estimate_payload_bytes(payload)

        state.total_published += 1
        state.bytes_received += byte_size
        state.current_queue_depth = (
            self._normalize_depth(queue_depth)
            if queue_depth is not None
            else state.current_queue_depth + 1
        )
        state.recent_publish_events.append(
            PublishEvent(timestamp=event_time, byte_size=byte_size)
        )
        self._trim_old_events(state, event_time)

    def record_consume(
        self,
        topic: str,
        queue_depth: int | None = None,
        timestamp: float | None = None,
    ) -> None:
        topic_name = self._validate_topic_name(topic)
        state = self.ensure_topic(topic_name)
        state.total_consumed += 1
        state.current_queue_depth = (
            self._normalize_depth(queue_depth)
            if queue_depth is not None
            else max(0, state.current_queue_depth - 1)
        )
        self._trim_old_events(state, self._resolve_time(timestamp))

    def record_ack(
        self,
        topic: str,
        queue_depth: int | None = None,
        timestamp: float | None = None,
        latency_seconds: float | None = None,
    ) -> None:
        topic_name = self._validate_topic_name(topic)
        state = self.ensure_topic(topic_name)
        state.total_acked += 1
        if latency_seconds is not None:
            state.total_ack_latency_seconds += max(0.0, latency_seconds)
        if queue_depth is not None:
            state.current_queue_depth = self._normalize_depth(queue_depth)
        self._trim_old_events(state, self._resolve_time(timestamp))

    def record_nack(
        self,
        topic: str,
        queue_depth: int | None = None,
        timestamp: float | None = None,
        retried: bool = True,
    ) -> None:
        topic_name = self._validate_topic_name(topic)
        state = self.ensure_topic(topic_name)
        state.total_nacked += 1
        if retried:
            state.total_retries += 1
        state.current_queue_depth = (
            self._normalize_depth(queue_depth)
            if queue_depth is not None
            else state.current_queue_depth + 1
        )
        self._trim_old_events(state, self._resolve_time(timestamp))

    def record_dead_letter(
        self,
        topic: str,
        queue_depth: int | None = None,
        timestamp: float | None = None,
    ) -> None:
        topic_name = self._validate_topic_name(topic)
        state = self.ensure_topic(topic_name)
        state.total_dead_lettered += 1
        if queue_depth is not None:
            state.current_queue_depth = self._normalize_depth(queue_depth)
        self._trim_old_events(state, self._resolve_time(timestamp))

    def set_queue_depth(self, topic: str, depth: int) -> None:
        state = self.ensure_topic(topic)
        state.current_queue_depth = self._normalize_depth(depth)

    def snapshot_topic(
        self, topic: str
    ) -> dict[str, str | int | float] | None:
        topic_name = self._validate_topic_name(topic)
        state = self._topics.get(topic_name)

        if state is None:
            if self.queue_manager is None or self.queue_manager.get_topic(topic_name) is None:
                return None
            state = self.ensure_topic(topic_name)

        self._refresh_queue_depth_from_manager(topic_name)
        return self._serialize_topic(topic_name, state)

    def snapshot_all(self) -> dict[str, dict[str, str | int | float]]:
        topic_names = set(self._topics.keys())
        if self.queue_manager is not None:
            topic_names.update(self.queue_manager.list_topics())

        snapshots: dict[str, dict[str, str | int | float]] = {}
        for topic in sorted(topic_names):
            snapshot = self.snapshot_topic(topic)
            if snapshot is not None:
                snapshots[topic] = snapshot
        return snapshots

    def _refresh_queue_depth_from_manager(self, topic: str) -> None:
        if self.queue_manager is None:
            return

        topic_state = self.queue_manager.get_topic(topic)
        if topic_state is None:
            return

        state = self.ensure_topic(topic)
        state.current_queue_depth = topic_state.queue_depth

    def _serialize_topic(
        self, topic: str, state: TopicMonitoringState
    ) -> dict[str, str | int | float]:
        now = time.time()
        self._trim_old_events(state, now)

        window_message_count = len(state.recent_publish_events)
        window_bytes = sum(event.byte_size for event in state.recent_publish_events)
        message_rate = round(window_message_count / self.rate_window_seconds, 3)
        byte_throughput = round(window_bytes / self.rate_window_seconds, 3)
        average_delivery_latency_ms = 0.0
        if state.total_acked:
            average_delivery_latency_ms = round(
                (state.total_ack_latency_seconds / state.total_acked) * 1000,
                3,
            )

        completion_count = state.total_acked + state.total_dead_lettered
        delivery_success_rate = 0.0
        if completion_count:
            delivery_success_rate = round(state.total_acked / completion_count, 3)

        burst_detected = message_rate >= self.burst_threshold_messages_per_second

        return {
            "topic": topic,
            "total_published": state.total_published,
            "total_consumed": state.total_consumed,
            "total_acked": state.total_acked,
            "total_nacked": state.total_nacked,
            "total_dead_lettered": state.total_dead_lettered,
            "total_retries": state.total_retries,
            "bytes_received": state.bytes_received,
            "current_queue_depth": state.current_queue_depth,
            "average_delivery_latency_ms": average_delivery_latency_ms,
            "delivery_success_rate": delivery_success_rate,
            "message_rate": message_rate,
            "byte_throughput": byte_throughput,
            "burst_detected": burst_detected,
            "burst_threshold_messages_per_second": self.burst_threshold_messages_per_second,
        }

    def _trim_old_events(self, state: TopicMonitoringState, now: float) -> None:
        cutoff = now - self.rate_window_seconds
        while state.recent_publish_events and state.recent_publish_events[0].timestamp < cutoff:
            state.recent_publish_events.popleft()

    @staticmethod
    def _estimate_payload_bytes(payload: Any) -> int:
        if isinstance(payload, bytes):
            return len(payload)
        if isinstance(payload, str):
            return len(payload.encode("utf-8"))

        try:
            encoded = json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
            return len(encoded)
        except (TypeError, ValueError):
            return len(str(payload).encode("utf-8"))

    @staticmethod
    def _normalize_depth(depth: int) -> int:
        return max(0, depth)

    @staticmethod
    def _resolve_time(timestamp: float | None) -> float:
        return time.time() if timestamp is None else timestamp

    @staticmethod
    def _validate_topic_name(topic: str) -> str:
        topic_name = topic.strip()
        if not topic_name:
            raise ValueError("Topic name cannot be empty.")
        return topic_name
