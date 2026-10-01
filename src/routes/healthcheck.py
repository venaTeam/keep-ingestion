"""
Health / probe endpoints.

Two endpoints, one per probe:

* `/healthcheck` — **liveness**. Unchanged, unconditional 200. That is the right
  answer for liveness on an HTTP server: if it replies at all, the process can
  serve. Checking dependencies here would restart every replica at once on a
  Postgres blip. There is no separate `/livez` because this already is it.
* `/readyz` — DB reachable, schema not behind this image, producer connected.

`/readyz` is wired to the **startupProbe**, sized to the worst-case migration:
migrations run before the socket binds, so liveness would otherwise kill the pod
mid-way. Deliberately not the readinessProbe yet — a schema comparison going
false on every replica at once would empty the Service mid-rollout.

Because it gates startup, a false negative kills the container, so both
judgements are levered:

* `KEEP_READYZ_SCHEMA_STRICT` — exact revision equality instead of "not behind".
* `KEEP_READYZ_REQUIRE_PRODUCER` — set false during a Kafka incident, or with
  the brokers down no pod can finish starting, rather than starting and
  answering the retryable 503 the ingestion route exists to give senders.
* `KEEP_READYZ_CHECK_TIMEOUT` — per-check bound, so a sick dependency makes the
  probe answer "not ready" rather than hang and tie up a worker slot. The checks
  run in sequence, so the endpoint's worst case is twice this; keep it under the
  probe's own `timeoutSeconds` or kubelet cuts the connection first.
"""

import asyncio
import logging
import os

from fastapi import APIRouter, Response

from src.services.producers import factory

logger = logging.getLogger(__name__)

READYZ_CHECK_TIMEOUT = float(os.environ.get("KEEP_READYZ_CHECK_TIMEOUT", "2"))
REQUIRE_PRODUCER = os.environ.get("KEEP_READYZ_REQUIRE_PRODUCER", "true") == "true"

router = APIRouter()


@router.get("/healthcheck", description="Liveness: the process can serve requests")
def healthcheck() -> dict:
    """
    Does nothing but return 200 response code

    Returns:
        dict: empty JSON object
    """
    return {}


async def _check_producer() -> tuple[bool, dict]:
    """Kafka producer connected. Also nudges a cold producer to reconnect, so a
    pod that can't reach the brokers stays NotReady instead of DLQ-ing alerts."""
    producer = factory.get_producer_instance()
    if producer is None:
        # startup() creates it, so this means startup hasn't got that far yet.
        logger.warning("Producer check failed: event producer instance not initialized yet")
        return False, {"created": False}

    try:
        healthy, detail = await producer.health(attempt_reconnect=True)
    except Exception as exc:
        logger.error("Producer health check failed with exception: %s", exc, exc_info=True)
        return False, {"created": True, "error": f"{type(exc).__name__}: {exc}"}

    detail["created"] = True
    if not healthy:
        logger.error(
            "Producer check failed: %s",
            detail.get("last_error") or "not connected",
            extra={"producer": detail},
        )
    else:
        logger.info("Producer readiness check passed", extra={"producer": detail})
    return healthy, detail


def _discard(task: "asyncio.Task"):
    """Consume an abandoned check's result so it can't surface as an
    unretrieved-exception warning."""
    if not task.cancelled():
        task.exception()


async def _bounded(awaitable, name: str) -> tuple[bool, dict]:
    """Run a readiness check under a timeout, reporting an overrun as a failure.

    `asyncio.wait` and not `wait_for`: `wait_for` awaits the cancellation it
    requests, and a check blocked in a worker thread cannot be cancelled, so the
    timeout would bound nothing. Here the overrunning check is abandoned.
    """
    task = asyncio.ensure_future(awaitable)
    done, _pending = await asyncio.wait({task}, timeout=READYZ_CHECK_TIMEOUT)

    if not done:
        task.cancel()
        task.add_done_callback(_discard)
        logger.error("Readiness check '%s' timed out after %ss", name, READYZ_CHECK_TIMEOUT)
        return False, {"error": f"{name} check timed out after {READYZ_CHECK_TIMEOUT}s"}

    try:
        return task.result()
    except Exception as exc:
        logger.error(
            "Readiness check '%s' failed with unhandled exception: %s",
            name,
            exc,
            exc_info=True,
        )
        return False, {"error": f"{type(exc).__name__}: {exc}"}


@router.get(
    "/readyz",
    description="Readiness: Kafka producer connected",
)
async def readyz(response: Response) -> dict:
    checks = {}

    # _check_db blocks on socket + DB work; inline it would park this worker's
    # event loop on every probe for as long as a sick Postgres takes to answer.
    db_ok, checks["database"] = await _bounded(
        asyncio.to_thread(_check_db), "database"
    )
    producer_ok, checks["producer"] = await _bounded(_check_producer(), "producer")
    checks["producer"]["required"] = REQUIRE_PRODUCER

    ready = producer_ok or not REQUIRE_PRODUCER

    if not producer_ok and not REQUIRE_PRODUCER:
        # Otherwise the lever hides the thing it was flipped for, and the pod
        # looks healthy while every publish it accepts is failing.
        logger.warning(
            "Kafka producer is unhealthy but not gating readiness "
            "(KEEP_READYZ_REQUIRE_PRODUCER=false)",
            extra={"producer": checks["producer"]},
        )

    if not ready:
        response.status_code = 503
        reasons = []

        if REQUIRE_PRODUCER and not producer_ok:
            prod_detail = checks["producer"]
            if not prod_detail.get("created", True):
                reasons.append("producer not created")
            elif prod_detail.get("error"):
                reasons.append(f"producer ({prod_detail['error']})")
            elif prod_detail.get("last_error"):
                reasons.append(f"producer ({prod_detail['last_error']})")
            elif not prod_detail.get("started", True):
                reasons.append("producer not connected")
            else:
                reasons.append("producer unhealthy")

        reason_str = f": {'; '.join(reasons)}" if reasons else ""
        logger.error(f"Readiness check failed {reason_str}", extra={"checks": checks})
    else:
        logger.debug("Readiness check passed", extra={"checks": checks})

    logger.debug("Readiness check passed", extra={"checks": checks})

    return {"status": "ok" if ready else "unavailable", "checks": checks}


