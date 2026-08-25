"""Router registration.

Three routers, matching the service's whole reason to exist: alert intake, the
probes Kubernetes reads, and the Prometheus scrape. Everything else the gateway
mounts is UI-facing and stays there.

Notably absent are `operators` and `tenants`. Operator-based routing happens on
the intake path — `_resolve_ingestion_tenant` reads the `operator` table — but
managing operators is admin CRUD that belongs with the API service, and mounting
it here would need write access to a table this service only ever reads.
"""

from fastapi import FastAPI

from src.routes import alerts, healthcheck, metrics


def setup_routers(app: FastAPI):
    # No prefix: /healthcheck (liveness) and /readyz (readiness) are absolute.
    app.include_router(healthcheck.router, tags=["healthcheck"])
    app.include_router(alerts.router, prefix="/alerts", tags=["alerts"])
    app.include_router(metrics.router, prefix="/metrics", tags=["metrics"])
