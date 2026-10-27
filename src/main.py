import asyncio
import functools
import logging
import os

import requests
import uvicorn
from contextlib import asynccontextmanager

from dotenv import find_dotenv, load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette_context import plugins
from starlette_context.middleware import RawContextMiddleware

import src.repositories.metrics
import src.utils.observability
from src.config.consts import (
    MAINTENANCE_WINDOW_ALERT_STRATEGY,
    REDIS,
)

from src.repositories.dependencies import GENERIC_TENANT_UUID
from src.utils.limiter import limiter
from src.utils.logging import CONFIG as logging_config, setup_logging
from src.middlewares import LoggingMiddleware
from src.routes.router_setup import setup_routers
from src.services.producers.factory import start_event_producer, stop_event_producer
from src.services.identity_manager.identitymanagerfactory import (
    IdentityManagerFactory,
    IdentityManagerTypes,
)

# load all providers into cache

from src.config.config import (
    AUTH_TYPE,
    HOST,
    KEEP_API_URL,
    KEEP_DEBUG_TASKS,
    KEEP_LIMIT_CONCURRENCY,
    KEEP_METRICS,
    KEEP_OTEL_ENABLED,
    KEEP_USE_LIMITER,
    KEEP_VERSION,
    KEEP_WORKERS,
    PORT,
    KEEP_CORS_TRUSTED_ORIGINS,
)

load_dotenv(find_dotenv())
setup_logging()
logger = logging.getLogger(__name__)


# Monkey patch requests to disable redirects
original_request = requests.Session.request


def no_redirect_request(self, method, url, **kwargs):
    kwargs["allow_redirects"] = False
    return original_request(self, method, url, **kwargs)


requests.Session.request = no_redirect_request


async def check_pending_tasks(background_tasks: set):
    while True:
        events_in_queue = len(background_tasks)
        logger.info(
            f"{events_in_queue} background tasks pending",
            extra={
                "pending_tasks": events_in_queue,
            },
        )
        await asyncio.sleep(1)


async def startup():
    """
    This runs for every worker on startup.
    Read more about lifespan here: https://fastapi.tiangolo.com/advanced/events/#lifespan

    The producer is connected **eagerly**: a cold producer makes the first alert
    on a fresh pod pay the bootstrap cost, and diverts it to the DLQ if the
    brokers aren't up yet. It never raises — /readyz keeps the pod NotReady until
    the producer connects.

    `EventSubscriber` is deliberately absent. It is retained in
    `keep-api-gateway` for a future pull-based provider; it reaches ingestion over
    HTTP via `BaseProvider._push_alert`, so it is a *client* of this service, not
    part of it. Leaving it out is what keeps the provider framework, the secret
    managers and the kubernetes client out of this image.
    """

    logger.info("Starting the services")

    await start_event_producer()

    logger.info("Services started successfully")


async def shutdown():
    """
    This runs for every worker on shutdown.
    Read more about lifespan here: https://fastapi.tiangolo.com/advanced/events/#lifespan

    Only the producer to stop, so this is far inside gunicorn's 30 s graceful
    timeout — the gateway's careful budgeting between the producer and the
    consumer's thread joins does not apply here, because the consumer machinery
    is not present. Flushing the producer still matters: a UvicornWorker drains
    in-flight requests before running this handler, and any alert accepted during
    that drain is only durable once its batch has been sent.
    """
    logger.info("Shutting down Keep")
    await stop_event_producer()

    logger.info("Keep shutdown complete")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    This runs for every worker on startup and shutdown.
    Read more about lifespan here: https://fastapi.tiangolo.com/advanced/events/#lifespan
    """
    app.state.limiter = limiter
    # create a set of background tasks
    background_tasks = set()
    # if debug tasks are enabled, create a task to check for pending tasks
    if KEEP_DEBUG_TASKS:
        logger.info("Starting background task to check for pending tasks")
        asyncio.create_task(check_pending_tasks(background_tasks))

    # The gateway's product-BI refresh loops (active users, incident gauges) are
    # deliberately not started here. They read tables this service has no grant
    # on, and the numbers belong to the API service, which already publishes them.

    # Startup
    await startup()

    # yield the background tasks, this is available for the app to use in request context
    yield {"background_tasks": background_tasks}

    # Shutdown
    await shutdown()


def get_app(
    auth_type: IdentityManagerTypes = IdentityManagerTypes.NOAUTH.value,
) -> FastAPI:
    if not KEEP_API_URL:
        logger.info(
            "KEEP_API_URL is not set, setting it to default",
            extra={"keep_api_url": f"http://{HOST}:{PORT}"},
        )
        os.environ["KEEP_API_URL"] = f"http://{HOST}:{PORT}"

    logger.info(
        f"Starting Keep with {os.environ['KEEP_API_URL']} as URL and version {KEEP_VERSION}",
        extra={
            "keep_version": KEEP_VERSION,
            "keep_api_url": KEEP_API_URL,
        },
    )

    app = FastAPI(
        title="Keep API",
        description="Rest API powering https://platform.keephq.dev and friends ðŸ„â€â™€ï¸",
        version=KEEP_VERSION,
        lifespan=lifespan,
    )

    @app.get("/", include_in_schema=False)
    async def root():
        """
        App description and version.
        """
        return {"message": app.description, "version": KEEP_VERSION}

    app.add_middleware(RawContextMiddleware, plugins=(plugins.RequestIdPlugin(),))
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(
        GZipMiddleware, minimum_size=30 * 1024 * 1024
    )  # Approximately 30 MiB, https://cloud.google.com/run/quotas
    app.add_middleware(
        CORSMiddleware,
        allow_origins=KEEP_CORS_TRUSTED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    setup_routers(app)
    logger.info(f"Starting Keep with authentication type: {AUTH_TYPE}")
    # The gateway calls `identity_manager.on_start(app)` here, which mounts the
    # sign-in endpoints a browser session needs. There is no interactive login on
    # this service: senders authenticate per-request with an API key, so there is
    # no identity manager to start and nothing to mount.

    @app.exception_handler(Exception)
    async def catch_exception(request: Request, exc: Exception):
        logging.error(
            f"An unhandled exception occurred: {exc}, Trace ID: {request.state.trace_id}. Tenant ID: {request.state.tenant_id}"
        )
        return JSONResponse(
            status_code=500,
            content={
                "message": "An internal server error occurred.",
                "trace_id": request.state.trace_id,
                "error_msg": str(exc),
            },
        )

    app.add_middleware(LoggingMiddleware)
    if KEEP_USE_LIMITER:
        app.add_middleware(SlowAPIMiddleware)

    if KEEP_METRICS:
        instrumentator = Instrumentator(
            excluded_handlers=["/metrics", "/metrics/processing"],
            should_group_status_codes=False,
        )
        instrumentator.instrument(app=app, metric_namespace="keep")

    if KEEP_OTEL_ENABLED:
        src.utils.observability.setup(app)

    return app


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def run(app: FastAPI):
    logger.info("Starting the uvicorn server")
    # call on starting to create the db and tables
    import src.config.config

    src.config.config.on_starting()

    uvicorn.run(
        "src.main:get_app",
        host=HOST,
        port=PORT,
        log_config=logging_config,
        lifespan="on",
        workers=KEEP_WORKERS,
        limit_concurrency=KEEP_LIMIT_CONCURRENCY,
    )

app = get_app()

if __name__ == "__main__":
    run(app)

