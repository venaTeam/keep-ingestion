"""Alert intake for `keep-ingestion`.

The three routes external senders reach, and nothing else. Every UI-facing alert
endpoint stays on `keep-api-gateway`; this module is deliberately free of the
enrichment, CEL, Elasticsearch and facet machinery those endpoints need, so the
service keeps a read-only database role and a small dependency set.

`POST /alerts/event` is the route Appchi is configured with and the one
`GET /settings/webhook` advertises, so its request and response shapes are a
published contract: the typed `AlertDto | list[AlertDto] | dict` body, the 202
body, and the 503 + `Retry-After` rejection all have to stay byte-identical to
the gateway's.
"""

import base64
import hashlib
import hmac
import json
import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from src.models.alert import AlertDto
from src.repositories.db import get_operator_by_name
from src.repositories.dependencies import (
    GENERIC_TENANT_UUID,
    extract_generic_body,
)
from src.repositories.metrics import (
    alert_ingestion_error_total,
    alert_ingestion_total,
)
from src.services.identity_manager.authenticatedentity import AuthenticatedEntity
from src.services.identity_manager.identitymanagerfactory import IdentityManagerFactory
from src.services.producers.base_event_handler import (
    EventProducer,
    ProduceResult,
    result_from_task_name,
)
from src.services.producers.factory import get_event_producer

router = APIRouter()
logger = logging.getLogger(__name__)

KEEP_ALERT_DLQ_ACCEPT = os.environ.get("KEEP_ALERT_DLQ_ACCEPT", "false") == "true"
KEEP_ALERT_RETRY_AFTER = os.environ.get(
    "KEEP_ALERT_RETRY_AFTER", os.environ.get("KEEP_ALERT_DLQ_RETRY_AFTER", "5")
)


def _retry_later(detail: str, **body) -> JSONResponse:
    """The single "this alert was not ingested, send it again" answer.

    Both rejection paths go through here so the retry contract cannot drift
    between them — senders were asked to key off 503 plus `Retry-After`.

    `KEEP_ALERT_RETRY_AFTER` sets that header. It is not DLQ-specific: it applies
    to every rejected publish, diverted or not. The old `KEEP_ALERT_DLQ_RETRY_AFTER`
    is still read as a fallback so a chart that sets it keeps working.

    `KEEP_ALERT_DLQ_ACCEPT=true` restores the old "202 accepted" contract for
    senders that must not see an error. The DLQ topic exists, but nothing
    consumes it, so an alert that lands there is retained and never ingested —
    which is why the default is to answer 503 and make the sender retry.
    """
    return JSONResponse(
        content={**body, "detail": detail},
        status_code=503,
        headers={"Retry-After": KEEP_ALERT_RETRY_AFTER},
    )


def _ingestion_response(task_name, source: str) -> JSONResponse:
    """Build the ingestion response, labelling the metric by where the event
    actually landed rather than reporting success unconditionally."""
    result = result_from_task_name(task_name)
    body = {"task_name": task_name or "async-task", "sink": result.value}

    if result is not ProduceResult.DLQ:
        alert_ingestion_total.labels(source=source, status="success").inc()
        return JSONResponse(content=body, status_code=202)

    alert_ingestion_total.labels(source=source, status="dlq").inc()
    logger.error(
        "Alert diverted to the DLQ topic and will not be ingested", extra=body
    )

    if KEEP_ALERT_DLQ_ACCEPT:
        return JSONResponse(content=body, status_code=202)

    return _retry_later(
        "Alert could not be published to the ingestion topic and was written to "
        "the dead-letter topic; it will not be processed. Please retry.",
        **body,
    )


def _publish_failed_response(
    exc: Exception, source: str, trace_id: str
) -> JSONResponse:
    """Answer a publish that reached no topic at all.

    Both the main send and the DLQ fallback failed — the ordinary shape of a
    Kafka outage, since `KAFKA_DLQ_BOOTSTRAP_SERVERS` defaults to the main
    brokers. Unhandled, this reaches the catch-all in `main.py` as a 500, which
    carries no "retry me" semantics; senders were asked to retry on 503.
    """
    alert_ingestion_error_total.labels(
        source=source, error_type=type(exc).__name__
    ).inc()
    logger.exception(
        "Failed to publish alert to any topic; rejecting so the sender retries",
        extra={"trace_id": trace_id, "source": source},
    )
    # trace_id travels in the body so a sender reporting a 503 gives us something
    # to grep for.
    return _retry_later(
        "Alert could not be published to the ingestion topic. Please retry.",
        trace_id=trace_id,
    )


def _extract_operator(event) -> str | None:
    """Best-effort read of the alert's `operator` routing key from an incoming
    event, which may be a single AlertDto, a list, or a raw dict. For a batch we
    use the first alert's operator (VENA-5596 Epic 5)."""
    item = event[0] if isinstance(event, list) and event else event
    if item is None:
        return None
    if isinstance(item, dict):
        return item.get("operator")
    return getattr(item, "operator", None)


def _resolve_ingestion_tenant(event) -> str:
    """Route an alert to the tenant that owns its `operator`. An alert with no
    operator, or an operator that maps to no tenant, goes to the GENERAL tenant --
    NOT the ingesting key's tenant -- so a specific tenant only ever receives its
    own operators' alerts (VENA-5596 Epic 5)."""
    operator_name = _extract_operator(event)
    if not operator_name:
        return GENERIC_TENANT_UUID
    operator = get_operator_by_name(operator_name)
    if operator is None:
        logger.info(
            "Alert operator matched no tenant; routing to general",
            extra={"operator": operator_name, "tenant_id": GENERIC_TENANT_UUID},
        )
        return GENERIC_TENANT_UUID
    logger.info(
        "Routing alert by operator",
        extra={"operator": operator_name, "tenant_id": operator.tenant_id},
    )
    return operator.tenant_id


@router.post(
    "/event",
    description="Receive a generic alert event",
    response_model=AlertDto | list[AlertDto],
    status_code=202,
)
async def receive_generic_event(
    event: AlertDto | list[AlertDto] | dict,
    request: Request,
    provider_id: str | None = None,
    fingerprint: str | None = None,
    authenticated_entity: AuthenticatedEntity = Depends(
        IdentityManagerFactory.get_auth_verifier(["write:alert"])
    ),
    event_producer: EventProducer = Depends(get_event_producer),
):
    """
    A generic webhook endpoint that can be used by any provider to send alerts to Keep.

    Args:
        alert (AlertDto | list[AlertDto]): The alert(s) to be sent to Keep.
        bg_tasks (BackgroundTasks): Background tasks handler.
        tenant_id (str, optional): Defaults to Depends(verify_api_key).
    """
    # Route by operator: an alert whose operator maps to a tenant goes there,
    # else it goes to the GENERAL tenant (never the API-key's tenant), so a
    # specific tenant only receives its own operators' alerts (VENA-5596 Epic 5).
    tenant_id = _resolve_ingestion_tenant(event)
    # Use the abstract event producer (Redis or Kafka)
    try:
        task_name = await event_producer.produce(
            event=event,
            tenant_id=tenant_id,
            provider_type=None,  # Generic event
            provider_id=provider_id,
            fingerprint=fingerprint,
            api_key_name=authenticated_entity.api_key_name,
            trace_id=request.state.trace_id,
            provider_name=None,
        )
    except Exception as e:
        return _publish_failed_response(
            e, source="generic", trace_id=request.state.trace_id
        )

    return _ingestion_response(task_name, source="generic")


# https://learn.netdata.cloud/docs/alerts-&-notifications/notifications/centralized-cloud-notifications/webhook#challenge-secret
@router.get(
    "/event/netdata",
    description="Helper function to complete Netdata webhook challenge",
)
async def webhook_challenge():
    try:
        token = Request.query_params.get("token").encode("ascii")
    except Exception as e:
        logger.exception("Failed to get token", extra={"error": str(e)})
        raise HTTPException(status_code=400, detail="Bad request: failed to get token")
    KEY = "keep-netdata-webhook-integration"

    # creates HMAC SHA-256 hash from incomming token and your consumer secret
    sha256_hash_digest = hmac.new(
        KEY.encode(), msg=token, digestmod=hashlib.sha256
    ).digest()

    # construct response data with base64 encoded hash
    response = {
        "response_token": "sha256="
        + base64.b64encode(sha256_hash_digest).decode("ascii")
    }

    return json.dumps(response)



@router.post(
    "/event/{provider_type}",
    description="Receive an alert event from a provider",
    status_code=202,
)
async def receive_event(
    provider_type: str,
    request: Request,
    provider_id: str | None = None,
    provider_name: str | None = None,
    fingerprint: str | None = None,
    event=Depends(extract_generic_body),
    authenticated_entity: AuthenticatedEntity = Depends(
        IdentityManagerFactory.get_auth_verifier(["write:alert"])
    ),
    event_producer: EventProducer = Depends(get_event_producer),
) -> dict[str, str]:
    """Per-provider intake. Unlike `/alerts/event` the body is NOT parsed here —
    it is passed through raw, so a payload that is not `AlertDto`-shaped is
    still accepted and normalised downstream by the consumer.

    Two defects in the gateway's copy are fixed here rather than carried over:

    * It incremented `alert_ingestion_total` unconditionally AND again inside
      `_ingestion_response`, so every alert counted twice and one diverted to the
      DLQ counted as both `success` and `dlq`. `_ingestion_response` is now the
      only writer, as it already is on the generic route — this counter is what
      the cutover reconciles against the consumer, so it has to be honest.
    * `produce()` was unguarded, so a publish reaching no topic surfaced as a 500
      from the catch-all rather than the 503 + `Retry-After` senders were asked
      to retry on. It now answers exactly as the generic route does.
    """
    trace_id = request.state.trace_id
    # If provider_name is provided, we pass it to the worker to resolve it
    # We do NOT parse the event here anymore, we pass the raw body (event) to the worker
    # We do NOT resolve the provider here anymore, we pass the provider_name to the worker

    # Route by operator: an alert whose operator maps to a tenant goes there,
    # else it goes to the GENERAL tenant (never the API-key's tenant), so a
    # specific tenant only receives its own operators' alerts (VENA-5596 Epic 5).
    tenant_id = _resolve_ingestion_tenant(event)
    # Use the abstract event producer (Redis or Kafka)
    try:
        task_name = await event_producer.produce(
            event=event,
            tenant_id=tenant_id,
            provider_type=provider_type,
            provider_id=provider_id,
            fingerprint=fingerprint,
            api_key_name=authenticated_entity.api_key_name,
            trace_id=trace_id,
            provider_name=provider_name,
        )
    except Exception as e:
        return _publish_failed_response(e, source=provider_type, trace_id=trace_id)

    if not task_name:
        task_name = "async-task"

    return _ingestion_response(task_name, source=provider_type)
