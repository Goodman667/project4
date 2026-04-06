import pytest

from app.models import Message, MessageStatus
from app.queue_manager import TopicNotFoundError, TopicQueueManager


def build_message(message_id: str, topic: str, payload: object = "hello") -> Message:
    return Message(id=message_id, topic=topic, payload=payload)


def test_create_topic_successfully_adds_topic() -> None:
    manager = TopicQueueManager()

    topic_state = manager.create_topic("orders")

    assert topic_state.name == "orders"
    assert manager.list_topics() == ["orders"]


def test_enqueue_increases_queue_depth() -> None:
    manager = TopicQueueManager()
    manager.create_topic("orders")
    message = build_message("msg-1", "orders")

    manager.enqueue("orders", message)

    assert manager.get_queue_depth("orders") == 1
    assert manager.get_topic("orders").ready_queue[0].id == "msg-1"


def test_dequeue_moves_message_to_in_flight() -> None:
    manager = TopicQueueManager()
    manager.create_topic("orders")
    manager.enqueue("orders", build_message("msg-1", "orders"))

    message = manager.dequeue("orders")
    topic_state = manager.get_topic("orders")

    assert message is not None
    assert message.id == "msg-1"
    assert message.status == MessageStatus.IN_FLIGHT
    assert manager.get_queue_depth("orders") == 0
    assert "msg-1" in topic_state.inflight
    assert topic_state.inflight["msg-1"].message.id == "msg-1"


def test_ack_removes_message_from_in_flight() -> None:
    manager = TopicQueueManager()
    manager.create_topic("orders")
    manager.enqueue("orders", build_message("msg-1", "orders"))
    manager.dequeue("orders")

    acked_message = manager.ack("msg-1")
    topic_state = manager.get_topic("orders")

    assert acked_message.status == MessageStatus.ACKED
    assert "msg-1" not in topic_state.inflight
    assert topic_state.metrics.acked_count == 1


def test_nack_requeues_message_and_increases_retry_count() -> None:
    manager = TopicQueueManager()
    manager.create_topic("orders")
    manager.enqueue("orders", build_message("msg-1", "orders"))
    manager.dequeue("orders")

    result = manager.nack("msg-1")
    topic_state = manager.get_topic("orders")

    assert result.requeued is True
    assert result.dead_lettered is False
    assert result.message.retry_count == 1
    assert result.message.status == MessageStatus.QUEUED
    assert "msg-1" not in topic_state.inflight
    assert manager.get_queue_depth("orders") == 1
    assert topic_state.ready_queue[-1].id == "msg-1"
    assert topic_state.metrics.nacked_count == 1
    assert topic_state.metrics.requeued_count == 1


def test_nack_moves_message_to_dead_letter_queue_after_retry_limit() -> None:
    manager = TopicQueueManager(max_retries=1)
    manager.create_topic("orders")
    manager.enqueue("orders", build_message("msg-1", "orders"))

    manager.dequeue("orders")
    first_result = manager.nack("msg-1")
    manager.dequeue("orders")
    second_result = manager.nack("msg-1")
    topic_state = manager.get_topic("orders")

    assert first_result.requeued is True
    assert second_result.requeued is False
    assert second_result.dead_lettered is True
    assert second_result.message.retry_count == 2
    assert second_result.message.status == MessageStatus.DEAD_LETTER
    assert manager.get_queue_depth("orders") == 0
    assert topic_state.dead_letter_count == 1
    assert topic_state.dead_letter_queue[-1].id == "msg-1"
    assert topic_state.metrics.dead_lettered_count == 1


def test_missing_topic_operations_raise_clear_error() -> None:
    manager = TopicQueueManager()
    message = build_message("msg-1", "missing")

    with pytest.raises(TopicNotFoundError):
        manager.enqueue("missing", message)

    with pytest.raises(TopicNotFoundError):
        manager.dequeue("missing")

    with pytest.raises(TopicNotFoundError):
        manager.get_queue_depth("missing")
