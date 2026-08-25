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


PROBE_PATHS = frozenset({"/readyz", "/healthcheck"})


class LoggingMiddleware(BaseHTTPMiddleware):
    """Logs the start and end of every request, except the probes.

    Kubelet hits `PROBE_PATHS` every few seconds on every pod, at two log lines
    each, and they say nothing worth keeping. Only the logging is skipped for
    them — the rest of the middleware still runs, because `request.state.tenant_id`
    is set here and the catch-all exception handler in `main.py` reads it.
    """

    async def dispatch(self, request: Request, call_next):
        identity = _extract_identity(request, attribute="keep_tenant_id")
        is_probe = request.url.path in PROBE_PATHS
        if not is_probe:
            logger.info(
                f"Request started: {request.method} {request.url.path}",
                extra={"tenant_id": identity},
            )

        # for debugging purposes, log the payload
        if os.environ.get("LOG_AUTH_PAYLOAD", "false") == "true":
            logger.info(f"Request headers: {request.headers}")

        # Default `trace_id` so the ingestion routes can read it unconditionally.
        #
        # `TraceIDMiddleware` sets it, but it is defined INSIDE the
        # `KEEP_OTEL_ENABLED` block in `observability.setup()`, so with OTEL off
        # nothing sets it — while `receive_generic_event`, `receive_event` and the
        # catch-all handler in `main.py` all read `request.state.trace_id`
        # unconditionally. In the gateway that means every ingestion request
        # raises `AttributeError` when OTEL is disabled.
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
        response = await call_next(request)

        end_time = time.time()
        identity = getattr(request.state, "tenant_id", identity)
        if not is_probe:
            logger.info(
                f"Request finished: {request.method} {request.url.path} {response.status_code} in {end_time - start_time:.2f}s",
                extra={
                    "tenant_id": identity,
                    "status_code": response.status_code,
                },
            )
        return response
