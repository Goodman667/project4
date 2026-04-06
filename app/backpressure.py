from __future__ import annotations

from app.models import TopicState


class BackpressureManager:
    """
    Simple queue-depth based backpressure.

    Trigger condition:
    - Reject new publish requests when queue_depth >= max_queue_depth

    Recovery condition:
    - Allow publish again when queue_depth < max_queue_depth

    This keeps the logic easy to explain for a course project:
    we only look at the current queue depth, with no adaptive tuning.
    """

    def __init__(
        self,
        default_max_queue_depth: int = 100,
        topic_limits: dict[str, int] | None = None,
        *,
        max_queue_size: int | None = None,
    ) -> None:
        # Keep backward compatibility with the earlier API skeleton.
        if max_queue_size is not None:
            default_max_queue_depth = max_queue_size

        if default_max_queue_depth <= 0:
            raise ValueError("default_max_queue_depth must be greater than 0.")

        self.default_max_queue_depth = default_max_queue_depth
        self._topic_limits: dict[str, int] = {}

        if topic_limits:
            for topic, limit in topic_limits.items():
                self.set_topic_limit(topic, limit)

    def set_topic_limit(self, topic: str, max_queue_depth: int) -> None:
        topic_name = self._validate_topic_name(topic)
        if max_queue_depth <= 0:
            raise ValueError("max_queue_depth must be greater than 0.")
        self._topic_limits[topic_name] = max_queue_depth

    def get_topic_limit(self, topic: str) -> int:
        topic_name = self._validate_topic_name(topic)
        return self._topic_limits.get(topic_name, self.default_max_queue_depth)

    def should_throttle(self, topic: str, depth: int) -> bool:
        """
        Return True when publish should be rejected for this topic.

        We throttle once the queue depth reaches the configured limit.
        """

        topic_name = self._validate_topic_name(topic)
        normalized_depth = max(0, depth)
        return normalized_depth >= self.get_topic_limit(topic_name)

    def allow_publish(self, topic: str, depth: int) -> bool:
        return not self.should_throttle(topic, depth)

    def status(self, topic_state: TopicState) -> dict[str, int | bool | str]:
        return self.topic_status(topic_state.name, topic_state.queue_depth)

    def topic_status(self, topic: str, depth: int) -> dict[str, int | bool | str]:
        topic_name = self._validate_topic_name(topic)
        max_queue_depth = self.get_topic_limit(topic_name)
        normalized_depth = max(0, depth)

        return {
            "topic": topic_name,
            "max_queue_depth": max_queue_depth,
            "queue_depth": normalized_depth,
            "throttled": self.should_throttle(topic_name, normalized_depth),
            "accepting_messages": self.allow_publish(topic_name, normalized_depth),
        }

    @staticmethod
    def _validate_topic_name(topic: str) -> str:
        topic_name = topic.strip()
        if not topic_name:
            raise ValueError("Topic name cannot be empty.")
        return topic_name


# Keep the old name so current imports continue to work.
BackpressurePolicy = BackpressureManager
