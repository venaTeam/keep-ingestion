import logging
import os
import time

import jwt
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


def _extract_identity(request: Request, attribute="email") -> str:
    try:
        authorization = request.headers.get("Authorization")
        if not authorization:
            return "anonymous"

        token = authorization.split(" ")[1]
        decoded_token = jwt.decode(token, options={"verify_signature": False})
        return decoded_token.get(attribute)
    except Exception:
        return "anonymous"


SENSITIVE_HEADERS = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "x-api-key",
        "x-keep-notify-token",
        "x-forwarded-access-token",
        "x-auth-request-access-token",
    }
)


def _redact_headers(headers) -> dict:
    return {
        key: "[REDACTED]" if key.lower() in SENSITIVE_HEADERS else value
        for key, value in headers.items()
    }


PROBE_PATHS = frozenset(
    {"/readyz", "/healthcheck", "/metrics", "/metrics/processing"}
)


class LoggingMiddleware(BaseHTTPMiddleware):
    """Logs one line per finished request, except the probes.

    Kubelet and Prometheus hit `PROBE_PATHS` every few seconds on every pod,
    and they say nothing worth keeping. Only the logging is skipped for
    them — the rest of the middleware still runs, because `request.state.tenant_id`
    is set here and `catch_exception` below reads it.
    """

    async def dispatch(self, request: Request, call_next):
        identity = _extract_identity(request, attribute="keep_tenant_id")
        is_probe = request.url.path in PROBE_PATHS

        # for debugging purposes, log the payload
        if os.environ.get("LOG_AUTH_PAYLOAD", "false") == "true":
            # opt-in flag, so log at INFO
            logger.info(
                "Request headers",
                extra={"headers": _redact_headers(request.headers)},
            )

        # Default `trace_id` so the ingestion routes can read it unconditionally.
        #
        # `TraceIDMiddleware` sets it, but it is defined INSIDE the
        # `KEEP_OTEL_ENABLED` block in `observability.setup()`, so with OTEL off
        # nothing sets it — while `receive_generic_event` and `receive_event`
        # read `request.state.trace_id` unconditionally. In the gateway that
        # means every ingestion request raises `AttributeError` when OTEL is
        # disabled.
        #
        # Only filled in when absent, never overwritten. `observability.setup()`
        # runs AFTER `add_middleware(LoggingMiddleware)`, and the last middleware
        # registered is the outermost — so with OTEL on, `TraceIDMiddleware` has
        # already written the real span id by the time this runs, and an
        # unconditional assignment here would throw it away on every request.
        if not getattr(request.state, "trace_id", None):
            request.state.trace_id = "no-trace"

        start_time = time.time()
        request.state.tenant_id = identity
        try:
            response = await call_next(request)
        except Exception:
            # traceback is logged once, by `catch_exception`
            if not is_probe:
                _log_request_finished(request, logging.ERROR, 500, start_time)
            raise

        if not is_probe:
            _log_request_finished(
                request, logging.INFO, response.status_code, start_time
            )
        return response


def _log_request_finished(request: Request, level, status_code, start_time):
    logger.log(
        level,
        "Request finished",
        extra={
            "method": request.method,
            "path": request.url.path,
            "status_code": status_code,
            "duration_ms": int((time.time() - start_time) * 1000),
            "tenant_id": getattr(request.state, "tenant_id", None),
            "trace_id": getattr(request.state, "trace_id", None),
        },
    )


async def catch_exception(request: Request, exc: Exception):
    """Catch-all handler, registered in `main.py`."""
    trace_id = getattr(request.state, "trace_id", None)
    logger.error(
        "Unhandled exception",
        exc_info=exc,
        extra={
            "method": request.method,
            "path": request.url.path,
            "tenant_id": getattr(request.state, "tenant_id", None),
            "trace_id": trace_id,
        },
    )
    return JSONResponse(
        status_code=500,
        content={
            "message": "An internal server error occurred.",
            "trace_id": trace_id,
            "error_msg": str(exc),
        },
    )
