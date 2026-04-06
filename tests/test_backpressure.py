import pytest

from app.backpressure import BackpressureManager


def test_should_throttle_when_depth_reaches_limit() -> None:
    manager = BackpressureManager(default_max_queue_depth=2)

    assert manager.should_throttle("orders", 2) is True
    assert manager.should_throttle("orders", 3) is True


def test_should_allow_publish_again_when_depth_drops_below_limit() -> None:
    manager = BackpressureManager(default_max_queue_depth=2)

    assert manager.should_throttle("orders", 2) is True
    assert manager.should_throttle("orders", 1) is False


def test_topic_specific_limit_overrides_default_limit() -> None:
    manager = BackpressureManager(default_max_queue_depth=5)
    manager.set_topic_limit("orders", 2)

    assert manager.should_throttle("orders", 2) is True
    assert manager.should_throttle("payments", 2) is False


def test_invalid_topic_limit_raises_value_error() -> None:
    manager = BackpressureManager()

    with pytest.raises(ValueError):
        manager.set_topic_limit("orders", 0)
