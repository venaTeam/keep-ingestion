"""
Tests for the gateway probe endpoints.

`/healthcheck` returns `{}` unconditionally. That is correct for **liveness** —
if an HTTP server replies at all it can serve, and checking dependencies there
would restart every replica at once on a Postgres blip — but useless as
readiness: it stays green with the DB unreachable, the schema behind this image's
migrations, or the producer cold. `/readyz` is the probe target that means
something.
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
    """A liveness probe that fails on a Postgres blip restarts every replica."""
    with patch.object(healthcheck, "_check_db", side_effect=AssertionError("no DB")):
        response = probe_client.get("/healthcheck")

    assert response.status_code == 200
    assert response.json() == {}  # unchanged contract; existing probes keep working


def test_readyz_ok_when_db_at_head_and_producer_connected(probe_client):
    with patch.object(
        healthcheck, "_check_db", return_value=(True, {"reachable": True, "at_head": True})
    ):
        with patch(
            "src.services.producers.factory.get_producer_instance",
            return_value=_producer(True),
        ):
            response = probe_client.get("/readyz")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_readyz_503_when_schema_is_behind(probe_client):
    """A pod whose DB is behind its own migrations must not be advertised ready —
    that is what makes readiness trustworthy as a sync-wave gate for
    keep-event-handler."""
    with patch.object(
        healthcheck,
        "_check_db",
        return_value=(False, {"reachable": True, "at_head": False}),
    ):
        with patch(
            "src.services.producers.factory.get_producer_instance",
            return_value=_producer(True),
        ):
            response = probe_client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["checks"]["database"]["at_head"] is False


def test_readyz_ignores_a_cold_producer_when_not_required(probe_client, monkeypatch):
    """`/readyz` backs the startupProbe, so a failing check kills the container:
    with the brokers down, no pod could finish starting. This is the lever out."""
    monkeypatch.setattr(healthcheck, "REQUIRE_PRODUCER", False)

    with patch.object(
        healthcheck, "_check_db", return_value=(True, {"reachable": True, "at_head": True})
    ):
        with patch(
            "src.services.producers.factory.get_producer_instance",
            return_value=_producer(False),
        ):
            response = probe_client.get("/readyz")

    assert response.status_code == 200
    # Still reported, just not gating — the operator can see it is cold.
    assert response.json()["checks"]["producer"]["required"] is False


def test_readyz_still_fails_on_the_database_when_producer_not_required(
    probe_client, monkeypatch
):
    """The lever must not turn `/readyz` into an unconditional 200."""
    monkeypatch.setattr(healthcheck, "REQUIRE_PRODUCER", False)

    with patch.object(
        healthcheck,
        "_check_db",
        return_value=(False, {"reachable": False}),
    ):
        with patch(
            "src.services.producers.factory.get_producer_instance",
            return_value=_producer(True),
        ):
            response = probe_client.get("/readyz")

    assert response.status_code == 503


def test_readyz_503_when_producer_is_cold(probe_client):
    """A cold producer means the next alert goes to the DLQ topic and is never
    ingested, so the pod is not ready to receive traffic."""
    with patch.object(
        healthcheck, "_check_db", return_value=(True, {"reachable": True, "at_head": True})
    ):
        with patch(
            "src.services.producers.factory.get_producer_instance",
            return_value=_producer(False, {"started": False}),
        ):
            response = probe_client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["checks"]["producer"]["started"] is False


def test_readyz_503_before_the_producer_exists(probe_client):
    with patch.object(
        healthcheck, "_check_db", return_value=(True, {"reachable": True, "at_head": True})
    ):
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
    with patch.object(
        healthcheck, "_check_db", return_value=(True, {"reachable": True, "at_head": True})
    ):
        with patch(
            "src.services.producers.factory.get_producer_instance",
            return_value=producer,
        ):
            probe_client.get("/readyz")

    producer.health.assert_awaited_once_with(attempt_reconnect=True)


def test_readyz_survives_a_raising_producer(probe_client):
    producer = MagicMock()
    producer.health = AsyncMock(side_effect=RuntimeError("boom"))
    with patch.object(
        healthcheck, "_check_db", return_value=(True, {"reachable": True, "at_head": True})
    ):
        with patch(
            "src.services.producers.factory.get_producer_instance",
            return_value=producer,
        ):
            response = probe_client.get("/readyz")

    assert response.status_code == 503
    assert "RuntimeError" in response.json()["checks"]["producer"]["error"]


def test_readyz_does_not_block_the_event_loop_on_a_slow_db(probe_client):
    """The DB check must run off the loop: parking the event loop on every probe
    (~every 10 s) for as long as a sick Postgres takes to answer would stall live
    request handling in that worker."""
    import threading

    loop_thread_ids = []

    def slow_check():
        loop_thread_ids.append(threading.get_ident())
        return True, {"reachable": True, "at_head": True}

    with patch.object(healthcheck, "_check_db", side_effect=slow_check):
        with patch(
            "src.services.producers.factory.get_producer_instance",
            return_value=_producer(True),
        ):
            response = probe_client.get("/readyz")

    assert response.status_code == 200
    # It ran on a worker thread, not the thread running the event loop.
    assert loop_thread_ids and loop_thread_ids[0] != threading.get_ident()


@pytest.mark.asyncio
async def test_readyz_reports_a_timeout_instead_of_hanging(monkeypatch):
    """A hung dependency must produce a prompt 503, not a probe that never
    answers.

    Exercised against the handler directly: a blocking check cannot be
    cancelled, so the orphaned worker thread is joined when the event loop of a
    TestClient portal is torn down — which would be charged to the request and
    hide the property under test. A long-lived server loop has no such teardown.
    """
    import time

    from fastapi import Response

    monkeypatch.setattr(healthcheck, "READYZ_CHECK_TIMEOUT", 0.1)

    def hanging_check():
        time.sleep(2)
        return True, {}

    with patch.object(healthcheck, "_check_db", side_effect=hanging_check):
        with patch(
            "src.services.producers.factory.get_producer_instance",
            return_value=_producer(True),
        ):
            started = time.monotonic()
            body = await healthcheck.readyz(Response())
            elapsed = time.monotonic() - started

    assert elapsed < 1
    assert body["status"] == "unavailable"
    assert "timed out" in body["checks"]["database"]["error"]


def test_readyz_bounds_a_hanging_producer_reconnect(probe_client, monkeypatch):
    """`attempt_reconnect` must not let a broker bootstrap hold the probe open."""
    monkeypatch.setattr(healthcheck, "READYZ_CHECK_TIMEOUT", 0.1)

    async def never_returns(**kwargs):
        await asyncio.sleep(5)

    producer = MagicMock()
    producer.health = never_returns

    with patch.object(
        healthcheck, "_check_db", return_value=(True, {"reachable": True, "at_head": True})
    ):
        with patch(
            "src.services.producers.factory.get_producer_instance",
            return_value=producer,
        ):
            response = probe_client.get("/readyz")

    assert response.status_code == 503
    assert "timed out" in response.json()["checks"]["producer"]["error"]


def test_check_db_reports_unreachable_database():
    engine = MagicMock()
    engine.connect.side_effect = OSError("connection refused")
    with patch("src.repositories.db.engine", engine):
        ok, detail = healthcheck._check_db()

    assert ok is False
    assert detail["reachable"] is False
    assert "OSError" in detail["error"]


def test_check_db_reports_not_ready_while_schema_is_missing():
    """NotReady while the gateway has not finished provisioning the schema.

    Replaces the gateway's `schema_at_head` assertion. This image ships no
    migration scripts, so a head comparison has nothing to compare against — the
    probe asks whether the tables this service reads exist instead.
    """
    engine = MagicMock()
    with patch("src.repositories.db.engine", engine):
        with patch(
            "src.repositories.db_on_start.schema_ready",
            return_value=(False, {"missing_tables": ["operator"]}),
        ):
            ok, detail = healthcheck._check_db()

    assert ok is False
    assert detail["reachable"] is True
    assert detail["schema_ready"] is False
    assert detail["missing_tables"] == ["operator"]


def test_check_db_ready_once_schema_is_present():
    engine = MagicMock()
    with patch("src.repositories.db.engine", engine):
        with patch(
            "src.repositories.db_on_start.schema_ready",
            return_value=(True, {"alembic_head": "rev-abc"}),
        ):
            ok, detail = healthcheck._check_db()

    assert ok is True
    assert detail["schema_ready"] is True
    assert detail["alembic_head"] == "rev-abc"


def test_readyz_never_calls_schema_at_head():
    """The image ships no migration scripts.

    `schema_at_head()` compares the database's stamped revision against the
    scripts in the image; with none present it would report "not at head" on
    every poll, forever. Because `/readyz` gates the startupProbe, that is
    CrashLoopBackOff rather than a bad response — so the symbol must not even be
    reachable from the probe.
    """
    import src.repositories.db_on_start as db_on_start

    assert not hasattr(db_on_start, "schema_at_head")
    # Attribute names the compiled function actually reaches for, so the
    # docstring above (which names schema_at_head to explain its absence) does
    # not make this pass or fail spuriously.
    assert "schema_at_head" not in healthcheck._check_db.__code__.co_names
    assert "schema_ready" in healthcheck._check_db.__code__.co_names
