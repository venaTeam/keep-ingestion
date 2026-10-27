"""
Tests that an alert diverted to the dead-letter topic is not reported as a
successful ingestion.

Only the main topic is consumed, so an event on the DLQ topic is never processed.
Reporting it as `202` + `status="success"` would hide real alert loss.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.routes import alerts
from src.services.producers.base_event_handler import (
    DLQ_TASK_NAME,
    MAIN_TASK_NAME,
    ProduceResult,
    result_from_task_name,
)
from src.services.producers.kafka_producer import KafkaEventProducer


def test_result_from_task_name_classifies_the_sink():
    assert result_from_task_name(MAIN_TASK_NAME) is ProduceResult.MAIN
    assert result_from_task_name(DLQ_TASK_NAME) is ProduceResult.DLQ
    assert result_from_task_name(None) is ProduceResult.MAIN
    assert result_from_task_name("async-task") is ProduceResult.MAIN


def test_main_topic_reports_202_and_success_metric():
    with patch.object(alerts, "alert_ingestion_total") as metric:
        response = alerts._ingestion_response(MAIN_TASK_NAME, source="grafana")

    assert response.status_code == 202
    metric.labels.assert_called_once_with(source="grafana", status="success")
    metric.labels.return_value.inc.assert_called_once()


def test_dlq_reports_503_and_dlq_metric():
    with patch.object(alerts, "alert_ingestion_total") as metric:
        response = alerts._ingestion_response(DLQ_TASK_NAME, source="grafana")

    assert response.status_code == 503
    assert response.headers["Retry-After"]
    metric.labels.assert_called_once_with(source="grafana", status="dlq")
    metric.labels.return_value.inc.assert_called_once()


def test_dlq_can_be_accepted_when_explicitly_configured(monkeypatch):
    """Opt-out for senders that must never see an error; the metric still says
    `dlq`, so the loss stays visible."""
    monkeypatch.setattr(alerts, "KEEP_ALERT_DLQ_ACCEPT", True)

    with patch.object(alerts, "alert_ingestion_total") as metric:
        response = alerts._ingestion_response(DLQ_TASK_NAME, source="generic")

    assert response.status_code == 202
    metric.labels.assert_called_once_with(source="generic", status="dlq")


def test_missing_task_name_falls_back_to_async_task():
    with patch.object(alerts, "alert_ingestion_total"):
        response = alerts._ingestion_response(None, source="generic")

    assert response.status_code == 202
    assert b"async-task" in response.body


def test_publish_failure_answers_503_not_500():
    """Neither topic accepted the event, but the sender must still get a
    retryable status — unhandled, this is a 500, which senders don't retry."""
    with patch.object(alerts, "alert_ingestion_error_total") as metric:
        response = alerts._publish_failed_response(
            RuntimeError("no brokers"), source="grafana", trace_id="t-1"
        )

    assert response.status_code == 503
    assert response.headers["Retry-After"]
    # Carried so a sender reporting a 503 gives us something to grep for.
    assert b"t-1" in response.body
    metric.labels.assert_called_once_with(source="grafana", error_type="RuntimeError")
    metric.labels.return_value.inc.assert_called_once()


@pytest.mark.asyncio
async def test_produce_raises_when_both_topics_are_unreachable():
    """Why the route needs its own 503: KAFKA_DLQ_BOOTSTRAP_SERVERS defaults to
    the *main* brokers, so an outage usually takes the fallback with it."""
    with patch("src.services.producers.kafka_producer.AIOKafkaProducer"):
        producer = KafkaEventProducer()

    producer._started = True
    producer.producer = MagicMock()
    producer.producer.send_and_wait = AsyncMock(side_effect=RuntimeError("no broker"))
    producer.dlq_producer = MagicMock()
    producer.dlq_producer.start = AsyncMock()
    producer.dlq_producer.send_and_wait = AsyncMock(
        side_effect=RuntimeError("no broker")
    )

    with pytest.raises(Exception):
        await producer.produce(event={"a": 1}, trace_id="t-2")


@pytest.mark.asyncio
async def test_kafka_producer_marks_the_dlq_sink():
    """The producer's return value carries the DLQ marker, which is what makes
    the route's per-request classification possible."""
    from src.services.producers.kafka_producer import KafkaEventProducer

    with patch("src.services.producers.kafka_producer.AIOKafkaProducer"):
        producer = KafkaEventProducer()

    producer._started = True
    producer.producer = MagicMock()
    producer.producer.send_and_wait = AsyncMock(side_effect=RuntimeError("no broker"))
    producer.dlq_producer = MagicMock()
    producer.dlq_producer.start = AsyncMock()
    producer.dlq_producer.send_and_wait = AsyncMock()

    task_name = await producer.produce(event={"a": 1}, trace_id="t-1")

    assert task_name == DLQ_TASK_NAME
    assert result_from_task_name(task_name) is ProduceResult.DLQ
    assert producer.last_produce_result() is ProduceResult.DLQ


@pytest.mark.asyncio
async def test_kafka_producer_health_reflects_connection_state():
    from src.services.producers.kafka_producer import KafkaEventProducer

    with patch("src.services.producers.kafka_producer.AIOKafkaProducer"):
        producer = KafkaEventProducer()

    producer.producer = MagicMock()
    producer.producer.start = AsyncMock(side_effect=OSError("no route to broker"))
    producer.dlq_producer = MagicMock()
    producer.dlq_producer.start = AsyncMock()

    healthy, detail = await producer.health(attempt_reconnect=True)
    assert healthy is False
    assert "OSError" in detail["last_error"]

    # Now let the bootstrap succeed: the probe-driven reconnect brings it up.
    producer.producer.start = AsyncMock()
    healthy, detail = await producer.health(attempt_reconnect=True)
    assert healthy is True
    assert "last_error" not in detail


@pytest.mark.asyncio
async def test_stop_closes_both_producers():
    """Started eagerly and never closed, they are reclaimed by process exit and
    aiokafka logs "Unclosed AIOKafkaProducer" on every restart."""
    with patch("src.services.producers.kafka_producer.AIOKafkaProducer"):
        producer = KafkaEventProducer()

    producer._started = True
    producer.producer = MagicMock(stop=AsyncMock())
    producer.dlq_producer = MagicMock(stop=AsyncMock())

    await producer.stop()

    producer.producer.stop.assert_awaited_once()
    producer.dlq_producer.stop.assert_awaited_once()
    assert producer._started is False


@pytest.mark.asyncio
async def test_stop_survives_a_broker_that_has_gone_away():
    """Shutdown must not hang or raise because a close failed."""
    with patch("src.services.producers.kafka_producer.AIOKafkaProducer"):
        producer = KafkaEventProducer()

    producer.producer = MagicMock(stop=AsyncMock(side_effect=OSError("gone")))
    producer.dlq_producer = MagicMock(stop=AsyncMock())

    await producer.stop()

    # The second producer is still closed despite the first one failing.
    producer.dlq_producer.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_eager_start_never_raises():
    """A broker that isn't up yet must not stop the gateway from starting."""
    from src.services.producers import factory

    producer = MagicMock()
    producer.start = AsyncMock(side_effect=OSError("brokers down"))

    with patch.object(factory, "get_event_producer", AsyncMock(return_value=producer)):
        result = await factory.start_event_producer()

    assert result is producer
    producer.start.assert_awaited_once()
