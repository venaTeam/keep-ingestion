"""The intake contract.

These routes are what Appchi and every other sender is pointed at, so their
request and response shapes are a published interface rather than an
implementation detail. Each test below pins one clause of it:

* 202 with {"sink": "main"} and alert_ingestion_total{status="success"}
* what the typed body actually rejects - a non-object, refused **before**
  anything is produced - and what it accepts despite looking stricter
* 503 with `Retry-After` when the event lands in the DLQ, or reaches no topic
* API-key verification against the configured Secret key
* no database access on any intake path
"""

import os
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.routes import alerts as alerts_route
from src.services.identity_manager.authenticatedentity import AuthenticatedEntity
from src.services.identity_manager.identitymanagerfactory import IdentityManagerFactory
from src.services.producers.base_event_handler import (
    DLQ_TASK_NAME,
    MAIN_TASK_NAME,
    EventProducer,
)

TENANT = "tenant-under-test"
API_KEY = "test-ingestion-api-key"


class _RecordingProducer(EventProducer):
    """Captures what would have been published, and can be told to fail."""

    def __init__(self, task_name=MAIN_TASK_NAME, raises: Exception | None = None):
        self.calls = []
        self._task_name = task_name
        self._raises = raises

    async def produce(self, event, event_type=None, **kwargs):
        if self._raises:
            raise self._raises
        self.calls.append({"event": event, **kwargs})
        return self._task_name


@pytest.fixture(autouse=True)
def _setup_env(monkeypatch):
    """Every test in this file runs with a configured ingestion API key."""
    monkeypatch.setenv("KEEP_INGESTION_API_KEY", API_KEY)
    monkeypatch.setenv("KEEP_INGESTION_TENANT_ID", TENANT)


@pytest.fixture
def client_factory(monkeypatch):
    """Build an app with auth and the producer stubbed out."""

    def _build(producer: EventProducer, override_auth: bool = True):
        app = FastAPI()

        # Middleware sets trace_id in the real app; the routes read it
        # unconditionally, so supply it here the same way.
        @app.middleware("http")
        async def _trace_id(request, call_next):
            request.state.trace_id = "test-trace"
            return await call_next(request)

        app.include_router(alerts_route.router, prefix="/alerts")

        async def _fake_producer():
            return producer

        app.dependency_overrides[alerts_route.get_event_producer] = _fake_producer

        if override_auth:
            # The auth verifier is constructed per-route via IdentityManagerFactory,
            # so override by callable identity on each route's security dependency.
            for route in app.routes:
                dependant = getattr(route, "dependant", None)
                if not dependant:
                    continue
                for dep in dependant.dependencies:
                    if dep.call.__class__.__name__ == "AuthVerifierBase":
                        app.dependency_overrides[dep.call] = (
                            lambda: AuthenticatedEntity(
                                tenant_id=TENANT,
                                email="ingestion",
                                api_key_name="ingestion-secret",
                                role="webhook",
                            )
                        )

        return TestClient(app), producer

    return _build


def _alert_body(**overrides):
    body = {"name": "disk full", "source": ["prometheus"], "severity": "critical"}
    body.update(overrides)
    return body


# --------------------------------------------------------------------------- #
# POST /alerts/event - the Appchi route
# --------------------------------------------------------------------------- #

def test_generic_event_accepts_and_produces(client_factory):
    client, producer = client_factory(_RecordingProducer())
    resp = client.post(
        "/alerts/event",
        json=_alert_body(),
        headers={"X-API-KEY": API_KEY},
    )

    assert resp.status_code == 202
    assert resp.json()["sink"] == "main"
    assert len(producer.calls) == 1
    assert producer.calls[0]["tenant_id"] == TENANT


@pytest.mark.parametrize("body", ["a string", 42, [1, 2, 3]])
def test_generic_event_rejects_a_non_object_body_before_producing(
    client_factory, body
):
    """A body that matches no arm of the union is refused before publishing.

    The declared type is `AlertDto | list[AlertDto] | dict`, so what actually
    gets rejected is a body that is not an object and not a list of objects - a
    bare scalar, or a list of scalars. Nothing is produced, which is the part
    that matters: a malformed payload must fail at the edge rather than become a
    DLQ row nobody is watching.
    """
    client, producer = client_factory(_RecordingProducer())
    resp = client.post(
        "/alerts/event",
        content=body,
        headers={"Content-Type": "application/json", "X-API-KEY": API_KEY},
    )

    assert resp.status_code == 422
    assert producer.calls == []


@pytest.mark.parametrize(
    "body", [{"severity": "critical"}, {}, {"totally": "unrelated"}]
)
def test_generic_event_accepts_any_json_object(client_factory, body):
    """Pins a contract that is looser than it looks - deliberately, for now.

    The route declares `response_model=AlertDto | list[AlertDto]` and a typed
    body, which reads as "this endpoint validates alerts". It does not: the
    trailing `| dict` in the union means pydantic falls through to `dict` for any
    object that fails `AlertDto`, so a body with no `name` - or no keys at all -
    is accepted with 202 and published.

    This is copied behaviour, not new: the gateway does the same, and senders may
    well depend on it. It is pinned here so that tightening it later is a visible
    decision with a failing test attached, rather than an accident. Note the
    consequence for the split: `GET /settings/webhook` advertises
    `AlertDto.schema()` as the contract, but this route does not enforce it.
    """
    client, producer = client_factory(_RecordingProducer())
    resp = client.post(
        "/alerts/event",
        json=body,
        headers={"X-API-KEY": API_KEY},
    )

    assert resp.status_code == 202
    assert len(producer.calls) == 1


def test_generic_event_returns_503_with_retry_after_on_dlq(client_factory):
    client, _ = client_factory(_RecordingProducer(task_name=DLQ_TASK_NAME))
    resp = client.post(
        "/alerts/event",
        json=_alert_body(),
        headers={"X-API-KEY": API_KEY},
    )

    assert resp.status_code == 503
    assert resp.headers["Retry-After"]
    assert resp.json()["sink"] == "dlq"


def test_generic_event_returns_503_when_publish_reaches_no_topic(client_factory):
    client, _ = client_factory(_RecordingProducer(raises=RuntimeError("brokers down")))
    resp = client.post(
        "/alerts/event",
        json=_alert_body(),
        headers={"X-API-KEY": API_KEY},
    )

    assert resp.status_code == 503


# --------------------------------------------------------------------------- #
# POST /alerts/event/{provider_type} - raw body, and the two fixed defects
# --------------------------------------------------------------------------- #

def test_provider_event_passes_a_raw_body_through_unparsed(client_factory):
    """The two routes must not be conflated.

    The per-provider route takes whatever the provider sends and lets the
    consumer normalise it. A body that `/alerts/event` would reject with 422 is
    accepted here by design.
    """
    client, producer = client_factory(_RecordingProducer())
    raw = {"totally": "not-an-alert", "nested": {"x": 1}}
    resp = client.post(
        "/alerts/event/grafana",
        json=raw,
        headers={"X-API-KEY": API_KEY},
    )

    assert resp.status_code == 202
    assert producer.calls[0]["event"] == raw
    assert producer.calls[0]["provider_type"] == "grafana"


def test_provider_event_counts_each_alert_exactly_once(client_factory):
    """Regression guard for the gateway's double-count.

    The gateway increments `alert_ingestion_total` unconditionally AND again
    inside `_ingestion_response`. That counter is what the cutover reconciles
    against the consumer's `events_in_total` to prove no alert was lost, so a 2x
    inflation on one side makes the comparison meaningless.
    """
    client, _ = client_factory(_RecordingProducer())
    counter = MagicMock()
    with patch.object(alerts_route, "alert_ingestion_total", counter):
        resp = client.post(
            "/alerts/event/grafana",
            json={"a": 1},
            headers={"X-API-KEY": API_KEY},
        )

    assert resp.status_code == 202
    assert counter.labels.call_count == 1
    assert counter.labels.call_args.kwargs == {
        "source": "grafana",
        "status": "success",
    }


def test_provider_event_dlq_is_not_also_counted_as_success(client_factory):
    client, _ = client_factory(_RecordingProducer(task_name=DLQ_TASK_NAME))
    counter = MagicMock()
    with patch.object(alerts_route, "alert_ingestion_total", counter):
        resp = client.post(
            "/alerts/event/grafana",
            json={"a": 1},
            headers={"X-API-KEY": API_KEY},
        )

    assert resp.status_code == 503
    statuses = [c.kwargs["status"] for c in counter.labels.call_args_list]
    assert statuses == ["dlq"]


def test_provider_event_returns_503_not_500_when_publish_fails(client_factory):
    """The gateway leaves this route's `produce()` unguarded, so a Kafka outage
    surfaces as a 500 from the catch-all. Senders were asked to retry on 503, so
    a 500 loses per-provider alerts that the generic route would have kept.
    """
    client, _ = client_factory(_RecordingProducer(raises=RuntimeError("brokers down")))
    resp = client.post(
        "/alerts/event/grafana",
        json={"a": 1},
        headers={"X-API-KEY": API_KEY},
    )

    assert resp.status_code == 503
    assert resp.headers["Retry-After"]


# --------------------------------------------------------------------------- #
# Tenant routing
# --------------------------------------------------------------------------- #

def test_alert_routes_to_configured_tenant(monkeypatch):
    configured = "configured-tenant-id"
    monkeypatch.setenv("KEEP_INGESTION_TENANT_ID", configured)
    assert alerts_route._resolve_ingestion_tenant({"operator": "acme"}) == configured


def test_alert_without_configured_tenant_routes_to_general():
    # Ensure the env var is not set for this test.
    with patch.dict(os.environ, {"KEEP_INGESTION_TENANT_ID": ""}, clear=False):
        assert (
            alerts_route._resolve_ingestion_tenant({"name": "no operator here"})
            == alerts_route.GENERIC_TENANT_UUID
        )


# --------------------------------------------------------------------------- #
# API-key verification
# --------------------------------------------------------------------------- #

def test_missing_api_key_returns_401(client_factory):
    client, _ = client_factory(_RecordingProducer(), override_auth=False)
    resp = client.post("/alerts/event", json=_alert_body())
    assert resp.status_code == 401


def test_invalid_api_key_returns_401(client_factory):
    client, _ = client_factory(_RecordingProducer(), override_auth=False)
    resp = client.post(
        "/alerts/event",
        json=_alert_body(),
        headers={"X-API-KEY": "wrong-key"},
    )

    assert resp.status_code == 401


def test_valid_api_key_in_query_param(client_factory):
    client, producer = client_factory(_RecordingProducer(), override_auth=False)
    resp = client.post(f"/alerts/event?api_key={API_KEY}", json=_alert_body())
    assert resp.status_code == 202
    assert len(producer.calls) == 1


def test_auth_verifier_requires_configured_key(monkeypatch):
    monkeypatch.delenv("KEEP_INGESTION_API_KEY", raising=False)
    with pytest.raises(ValueError, match="KEEP_INGESTION_API_KEY"):
        IdentityManagerFactory.get_auth_verifier(["write:alert"])


# --------------------------------------------------------------------------- #
# No database dependency
# --------------------------------------------------------------------------- #

def test_intake_has_no_database_imports():
    """The intake routes must not import any database modules."""
    import sys

    # Remove any cached DB modules from previous imports.
    for name in list(sys.modules):
        if name.startswith("src.repositories.db") or name.startswith("src.models.db"):
            del sys.modules[name]

    # Re-import the route module: it should not pull in DB code.
    import src.routes.alerts as alerts_module

    imported = set(sys.modules.keys())
    db_modules = {
        name for name in imported if name.startswith(("src.repositories.db", "src.models.db"))
    }
    assert not db_modules, f"Unexpected database modules imported: {db_modules}"