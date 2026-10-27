"""The Prometheus scrape endpoint.

The gateway's `/metrics` does two unrelated jobs in one handler: it renders
per-tenant BI gauges (`alerts_total`, `open_incidents_total`) from incident
queries, and it appends the process's own `prometheus_client` output. Only the
second belongs here — the BI half needs `get_last_incidents`, the incident models
and `chevron`, none of which an ingestion service should carry, and the numbers it
reports are the API service's to publish.

`Instrumentator.instrument()` in `main.py` is called WITHOUT `.expose()`, so it
registers collectors but serves nothing. This route is the only thing that
answers a scrape — dropping it rather than trimming it would leave the service
with metrics that nothing can read.
"""

import logging

from fastapi import APIRouter, Response
from prometheus_client import (
    CollectorRegistry,
    generate_latest,
    multiprocess,
)

router = APIRouter()
logger = logging.getLogger(__name__)

CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"


@router.get("")
def get_metrics() -> Response:
    """Expose this process's metrics, including `alert_ingestion_total` and
    `alert_ingestion_error_total`.

    Gunicorn runs more than one worker, so the numbers have to come from the
    multiprocess collector reading `PROMETHEUS_MULTIPROC_DIR` rather than from
    the in-process registry, which would report one worker's share and make the
    ingestion counters look like an undercount. The fallback to the default
    registry keeps a single-process local run scrapeable.
    """
    try:
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        export = generate_latest(registry).decode("utf-8")
    except Exception:
        # Fallback to default registry if multiprocess collection fails.
        # This is useful for local development or if configuration is improper.
        logger.warning(
            "Multiprocess metric collection failed; falling back to the "
            "in-process registry (numbers cover this worker only)",
            exc_info=True,
        )
        from prometheus_client import REGISTRY

        export = generate_latest(REGISTRY).decode("utf-8")

    return Response(content=export, media_type=CONTENT_TYPE_LATEST)
