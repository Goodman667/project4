import app.monitoring as monitoring_module
from app.monitoring import MonitoringService


def test_monitoring_counts_are_updated_per_topic() -> None:
    service = MonitoringService(rate_window_seconds=60)

    service.record_publish("orders", {"id": 1}, queue_depth=1, timestamp=100.0)
    service.record_consume("orders", queue_depth=0, timestamp=101.0)
    service.record_ack("orders", queue_depth=0, timestamp=102.0, latency_seconds=0.25)
    service.record_nack("orders", queue_depth=1, timestamp=103.0, retried=True)
    service.record_dead_letter("orders", queue_depth=0, timestamp=104.0)

    snapshot = service.snapshot_topic("orders")

    assert snapshot is not None
    assert snapshot["total_published"] == 1
    assert snapshot["total_consumed"] == 1
    assert snapshot["total_acked"] == 1
    assert snapshot["total_nacked"] == 1
    assert snapshot["total_dead_lettered"] == 1
    assert snapshot["total_retries"] == 1
    assert snapshot["current_queue_depth"] == 0
    assert snapshot["average_delivery_latency_ms"] == 250.0
    assert snapshot["delivery_success_rate"] == 0.5


def test_monitoring_message_rate_and_byte_throughput_use_recent_window(
    monkeypatch,
) -> None:
    service = MonitoringService(
        rate_window_seconds=10,
        burst_threshold_messages_per_second=0.15,
    )

    service.record_publish("orders", "ignored", queue_depth=1, timestamp=90.0)
    service.record_publish("orders", "abc", queue_depth=2, timestamp=100.0)
    service.record_publish("orders", "hello", queue_depth=3, timestamp=105.0)

    monkeypatch.setattr(monitoring_module.time, "time", lambda: 109.0)
    snapshot = service.snapshot_topic("orders")

    assert snapshot["message_rate"] == 0.2
    assert snapshot["byte_throughput"] == 0.8
    assert snapshot["bytes_received"] == 15
    assert snapshot["burst_detected"] is True
    assert snapshot["burst_threshold_messages_per_second"] == 0.15


def test_snapshot_can_refresh_queue_depth_from_queue_manager() -> None:
    from app.queue_manager import TopicQueueManager

    queue_manager = TopicQueueManager()
    queue_manager.create_topic("orders")
    queue_manager.publish("orders", "hello")

    service = MonitoringService(queue_manager=queue_manager)
    snapshot = service.snapshot_topic("orders")

    assert snapshot is not None
    assert snapshot["current_queue_depth"] == 1


def test_delivery_success_rate_only_uses_completed_outcomes() -> None:
    service = MonitoringService(rate_window_seconds=60)

    service.record_ack("orders", latency_seconds=0.1)
    service.record_nack("orders", retried=True)
    service.record_nack("orders", retried=False)
    service.record_dead_letter("orders")

    snapshot = service.snapshot_topic("orders")

    assert snapshot is not None
    assert snapshot["total_nacked"] == 2
    assert snapshot["total_retries"] == 1
    assert snapshot["total_dead_lettered"] == 1
    assert snapshot["delivery_success_rate"] == 0.5
