from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import RedirectResponse

from app.backpressure import BackpressureManager
from app.models import ActionResponse, PublishMessageRequest, PublishMessageResponse
from app.monitoring import MonitoringService
from app.queue_manager import (
    MessageNotInFlightError,
    QueueManager,
    TopicNotFoundError,
)


queue_manager = QueueManager()
monitoring_service = MonitoringService(queue_manager)
backpressure_manager = BackpressureManager(default_max_queue_depth=100)

router = APIRouter()


@router.get("/api-info")
def read_root() -> dict[str, object]:
    return {
        "service": "lightweight-message-broker",
        "status": "ready",
        "docs": "/docs",
        "dashboard": "/dashboard",
    }


@router.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/api/health")
def api_healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/docs", include_in_schema=False)
def docs_redirect() -> RedirectResponse:
    return RedirectResponse(url="/api/docs", status_code=status.HTTP_307_TEMPORARY_REDIRECT)


@router.get("/redoc", include_in_schema=False)
def redoc_redirect() -> RedirectResponse:
    return RedirectResponse(url="/api/redoc", status_code=status.HTTP_307_TEMPORARY_REDIRECT)


@router.post("/topics/{topic}", status_code=status.HTTP_201_CREATED)
def create_topic(topic: str) -> dict[str, str]:
    topic_name = _validate_topic_name(topic)
    already_exists = queue_manager.get_topic(topic_name) is not None
    topic_state = queue_manager.create_topic(topic_name)
    monitoring_service.ensure_topic(topic_name)

    return {
        "topic": topic_state.name,
        "detail": "Topic already exists." if already_exists else "Topic created.",
    }


@router.get("/topics")
def list_topics() -> dict[str, list[str]]:
    return {"topics": queue_manager.list_topics()}


@router.post(
    "/topics/{topic}/publish",
    response_model=PublishMessageResponse,
    status_code=status.HTTP_201_CREATED,
)
def publish_message(topic: str, request: PublishMessageRequest) -> PublishMessageResponse:
    topic_name = _validate_topic_name(topic)
    _require_existing_topic(topic_name)

    depth = queue_manager.get_queue_depth(topic_name)
    if backpressure_manager.should_throttle(topic_name, depth):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Topic '{topic_name}' is throttled because queue depth reached the limit.",
        )

    message = queue_manager.publish(topic_name, request.payload)
    monitoring_service.record_publish(
        topic=topic_name,
        payload=request.payload,
        queue_depth=queue_manager.get_queue_depth(topic_name),
    )
    return PublishMessageResponse(message=message)


@router.get("/topics/{topic}/consume")
def consume_message(topic: str, consumer_id: str = "consumer-1") -> dict[str, object]:
    topic_name = _validate_topic_name(topic)
    _require_existing_topic(topic_name)

    result = queue_manager.pull(topic_name, consumer_id)
    if result is None:
        return {
            "topic": topic_name,
            "consumer_id": consumer_id,
            "message": None,
            "detail": "No messages available.",
        }

    monitoring_service.record_consume(
        topic=topic_name,
        queue_depth=queue_manager.get_queue_depth(topic_name),
    )
    return result


@router.post("/messages/{message_id}/ack", response_model=ActionResponse)
def ack_message(message_id: str) -> ActionResponse:
    try:
        message = queue_manager.ack(message_id)
    except MessageNotInFlightError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    monitoring_service.record_ack(
        topic=message.topic,
        queue_depth=queue_manager.get_queue_depth(message.topic),
    )
    return ActionResponse(success=True, detail=f"Message '{message_id}' acknowledged.")


@router.post("/messages/{message_id}/nack", response_model=ActionResponse)
def nack_message(message_id: str) -> ActionResponse:
    try:
        message = queue_manager.nack(message_id)
    except MessageNotInFlightError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    monitoring_service.record_nack(
        topic=message.topic,
        queue_depth=queue_manager.get_queue_depth(message.topic),
    )
    return ActionResponse(success=True, detail=f"Message '{message_id}' requeued.")


@router.get("/topics/{topic}/depth")
def topic_depth(topic: str) -> dict[str, int | str]:
    topic_name = _validate_topic_name(topic)
    depth = _get_queue_depth_or_404(topic_name)
    return {"topic": topic_name, "depth": depth}


@router.get("/topics/{topic}/metrics")
def topic_metrics(topic: str) -> dict[str, object]:
    topic_name = _validate_topic_name(topic)
    topic_state = _require_existing_topic(topic_name)
    snapshot = monitoring_service.snapshot_topic(topic_name)

    return {
        "topic": topic_name,
        "metrics": snapshot,
        "backpressure": backpressure_manager.topic_status(
            topic_name,
            topic_state.queue_depth,
        ),
    }


@router.get("/topics/{topic}/stats")
def topic_stats(topic: str) -> dict[str, object]:
    # Keep the previous route as an alias for convenience.
    return topic_metrics(topic)


@router.get("/metrics")
def metrics() -> dict[str, dict[str, int | str]]:
    return monitoring_service.snapshot_all()


def _validate_topic_name(topic: str) -> str:
    topic_name = topic.strip()
    if not topic_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Topic name cannot be empty.",
        )
    return topic_name


def _require_existing_topic(topic: str):
    try:
        topic_state = queue_manager.get_topic(topic)
    except TopicNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    if topic_state is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Topic '{topic}' does not exist.",
        )
    return topic_state


def _get_queue_depth_or_404(topic: str) -> int:
    try:
        return queue_manager.get_queue_depth(topic)
    except TopicNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
