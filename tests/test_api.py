import pytest
from fastapi.testclient import TestClient

import app.api as api_module
from app.backpressure import BackpressureManager
from app.main import app
from app.monitoring import MonitoringService
from app.queue_manager import TopicQueueManager


@pytest.fixture
def client() -> TestClient:
    api_module.queue_manager = TopicQueueManager()
    api_module.monitoring_service = MonitoringService(api_module.queue_manager)
    api_module.backpressure_manager = BackpressureManager(default_max_queue_depth=100)
    return TestClient(app)


def test_healthcheck(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_api_healthcheck_alias(client: TestClient) -> None:
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_docs_redirects_to_api_docs(client: TestClient) -> None:
    response = client.get("/docs", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"] == "/api/docs"


def test_dashboard_page_loads(client: TestClient) -> None:
    response = client.get("/dashboard")

    assert response.status_code == 200
    assert "Broker Control Panel" in response.text


def test_topics_starts_empty(client: TestClient) -> None:
    response = client.get("/topics")

    assert response.status_code == 200
    assert response.json() == {"topics": []}


def test_create_topic_publish_consume_and_ack_flow(client: TestClient) -> None:
    create_response = client.post("/topics/orders")
    publish_response = client.post(
        "/topics/orders/publish",
        json={"payload": {"order_id": 1}},
    )
    consume_response = client.get("/topics/orders/consume", params={"consumer_id": "c1"})

    assert create_response.status_code == 201
    assert publish_response.status_code == 201
    assert consume_response.status_code == 200
    assert consume_response.json()["message"]["topic"] == "orders"

    message_id = consume_response.json()["message"]["id"]
    ack_response = client.post(f"/messages/{message_id}/ack")
    depth_response = client.get("/topics/orders/depth")
    metrics_response = client.get("/topics/orders/metrics")

    assert ack_response.status_code == 200
    assert depth_response.json() == {"topic": "orders", "depth": 0}
    assert metrics_response.status_code == 200
    assert metrics_response.json()["metrics"]["total_published"] == 1
    assert metrics_response.json()["metrics"]["total_consumed"] == 1
    assert metrics_response.json()["metrics"]["total_acked"] == 1


def test_consume_returns_clear_result_when_queue_is_empty(client: TestClient) -> None:
    client.post("/topics/orders")

    response = client.get("/topics/orders/consume")

    assert response.status_code == 200
    assert response.json()["message"] is None
    assert response.json()["detail"] == "No messages available."


def test_nack_requeues_message_and_updates_metrics(client: TestClient) -> None:
    client.post("/topics/orders")
    client.post("/topics/orders/publish", json={"payload": "hello"})
    consume_response = client.get("/topics/orders/consume")

    message_id = consume_response.json()["message"]["id"]
    nack_response = client.post(f"/messages/{message_id}/nack")
    depth_response = client.get("/topics/orders/depth")
    consume_again_response = client.get("/topics/orders/consume")
    metrics_response = client.get("/topics/orders/metrics")

    assert nack_response.status_code == 200
    assert nack_response.json()["detail"] == f"Message '{message_id}' requeued."
    assert depth_response.json()["depth"] == 1
    assert consume_again_response.status_code == 200
    assert consume_again_response.json()["message"]["id"] == message_id
    assert consume_again_response.json()["message"]["retry_count"] == 1
    assert metrics_response.json()["metrics"]["total_nacked"] == 1
    assert metrics_response.json()["metrics"]["total_retries"] == 1
    assert metrics_response.json()["metrics"]["total_dead_lettered"] == 0


def test_publish_returns_429_when_backpressure_is_triggered(client: TestClient) -> None:
    api_module.backpressure_manager = BackpressureManager(default_max_queue_depth=1)

    client.post("/topics/orders")
    first_publish = client.post("/topics/orders/publish", json={"payload": "first"})
    second_publish = client.post("/topics/orders/publish", json={"payload": "second"})

    assert first_publish.status_code == 201
    assert second_publish.status_code == 429


def test_publish_returns_404_for_missing_topic(client: TestClient) -> None:
    response = client.post("/topics/missing/publish", json={"payload": "hello"})

    assert response.status_code == 404


def test_message_moves_to_dead_letter_queue_after_retry_limit(client: TestClient) -> None:
    client.post("/topics/orders")
    client.post("/topics/orders/publish", json={"payload": "hello"})

    consume_1 = client.get("/topics/orders/consume")
    message_id = consume_1.json()["message"]["id"]
    nack_1 = client.post(f"/messages/{message_id}/nack")

    consume_2 = client.get("/topics/orders/consume")
    nack_2 = client.post(f"/messages/{message_id}/nack")

    consume_3 = client.get("/topics/orders/consume")
    nack_3 = client.post(f"/messages/{message_id}/nack")

    depth_response = client.get("/topics/orders/depth")
    dead_letter_response = client.get("/topics/orders/dead-letter")
    metrics_response = client.get("/topics/orders/metrics")

    assert consume_2.status_code == 200
    assert consume_3.status_code == 200
    assert nack_1.status_code == 200
    assert nack_2.status_code == 200
    assert nack_3.status_code == 200
    assert nack_3.json()["detail"] == (
        f"Message '{message_id}' reached retry limit and was moved to the dead-letter queue."
    )
    assert depth_response.json()["depth"] == 0
    assert dead_letter_response.status_code == 200
    assert dead_letter_response.json()["count"] == 1
    assert dead_letter_response.json()["messages"][0]["id"] == message_id
    assert dead_letter_response.json()["messages"][0]["status"] == "dead_letter"
    assert dead_letter_response.json()["messages"][0]["retry_count"] == 3
    assert metrics_response.json()["metrics"]["total_dead_lettered"] == 1
    assert metrics_response.json()["metrics"]["total_nacked"] == 3
    assert metrics_response.json()["metrics"]["total_retries"] == 2
    assert metrics_response.json()["retry_policy"]["max_retries"] == 2
    assert metrics_response.json()["retry_policy"]["dead_letter_count"] == 1
    assert metrics_response.json()["metrics"]["delivery_success_rate"] == 0.0


def test_metrics_include_latency_success_rate_and_burst_signal(client: TestClient) -> None:
    api_module.monitoring_service = MonitoringService(
        api_module.queue_manager,
        rate_window_seconds=60,
        burst_threshold_messages_per_second=0.01,
    )

    client.post("/topics/orders")
    client.post("/topics/orders/publish", json={"payload": "hello"})
    consume_response = client.get("/topics/orders/consume")

    message_id = consume_response.json()["message"]["id"]
    client.post(f"/messages/{message_id}/ack")
    metrics_response = client.get("/topics/orders/metrics")

    assert metrics_response.status_code == 200
    assert metrics_response.json()["metrics"]["average_delivery_latency_ms"] >= 0.0
    assert metrics_response.json()["metrics"]["delivery_success_rate"] == 1.0
    assert metrics_response.json()["metrics"]["burst_detected"] is True
    assert metrics_response.json()["retry_policy"]["max_retries"] == 2
