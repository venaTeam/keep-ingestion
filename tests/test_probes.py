"""Tests for the keep-ingestion probe endpoints.

`/healthcheck` returns `{}` unconditionally. That is correct for **liveness** -
if an HTTP server replies at all it can serve. `/readyz` is the probe target that
means something: it checks only the Kafka producer, because this service has no
database dependency.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.routes import healthcheck


@pytest.fixture
def probe_client():
    app = FastAPI()
    app.include_router(healthcheck.router)
    return TestClient(app)


def _producer(healthy=True, detail=None):
    producer = MagicMock()
    producer.health = AsyncMock(return_value=(healthy, detail or {"started": healthy}))
    return producer


def test_healthcheck_is_liveness_and_dependency_free(probe_client):
    response = probe_client.get("/healthcheck")
    assert response.status_code == 200
    assert response.json() == {}


def test_readyz_ok_when_producer_connected(probe_client):
    with patch(
        "src.services.producers.factory.get_producer_instance",
        return_value=_producer(True),
    ):
        response = probe_client.get("/readyz")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_readyz_ignores_a_cold_producer_when_not_required(probe_client, monkeypatch):
    """/readyz backs the startupProbe, so a failing check kills the container:
    with the brokers down, no pod could finish starting. This is the lever out."""
    monkeypatch.setattr(healthcheck, "REQUIRE_PRODUCER", False)

    with patch(
        "src.services.producers.factory.get_producer_instance",
        return_value=_producer(False),
    ):
        response = probe_client.get("/readyz")

    assert response.status_code == 200
    # Still reported, just not gating - the operator can see it is cold.
    assert response.json()["checks"]["producer"]["required"] is False


def test_readyz_503_when_producer_is_cold(probe_client):
    """A cold producer means the next alert goes to the DLQ topic and is never
    ingested, so the pod is not ready to receive traffic."""
    with patch(
        "src.services.producers.factory.get_producer_instance",
        return_value=_producer(False, {"started": False}),
    ):
        response = probe_client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["checks"]["producer"]["started"] is False


def test_readyz_503_before_the_producer_exists(probe_client):
    with patch(
        "src.services.producers.factory.get_producer_instance", return_value=None
    ):
        response = probe_client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["checks"]["producer"]["created"] is False


def test_readyz_asks_the_producer_to_reconnect(probe_client):
    """The probe doubles as a reconnect trigger, so a pod that lost the brokers
    keeps retrying instead of quietly DLQ-ing whatever arrives."""
    producer = _producer(True)
    with patch(
        "src.services.producers.factory.get_producer_instance",
        return_value=producer,
    ):
        probe_client.get("/readyz")

    producer.health.assert_awaited_once_with(attempt_reconnect=True)


def test_readyz_survives_a_raising_producer(probe_client):
    producer = MagicMock()
    producer.health = AsyncMock(side_effect=RuntimeError("boom"))
    with patch(
        "src.services.producers.factory.get_producer_instance",
        return_value=producer,
    ):
        response = probe_client.get("/readyz")

    assert response.status_code == 503
    assert "RuntimeError" in response.json()["checks"]["producer"]["error"]


def test_readyz_bounds_a_hanging_producer_reconnect(probe_client, monkeypatch):
    """`attempt_reconnect` must not let a broker bootstrap hold the probe open."""
    monkeypatch.setattr(healthcheck, "READYZ_CHECK_TIMEOUT", 0.1)

    async def never_returns(**kwargs):
        await asyncio.sleep(5)

    producer = MagicMock()
    producer.health = never_returns

    with patch(
        "src.services.producers.factory.get_producer_instance",
        return_value=producer,
    ):
        response = probe_client.get("/readyz")

    assert response.status_code == 503
    assert "timed out" in response.json()["checks"]["producer"]["error"]


def test_readyz_has_no_database_check(probe_client):
    """The probe must not reference database modules or checks."""
    assert not hasattr(healthcheck, "_check_db")
    assert "database" not in healthcheck.readyz.__code__.co_names