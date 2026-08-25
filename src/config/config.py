"""
Application settings, and the gunicorn config file.

This module is passed to gunicorn as `-c src/config/config.py`, so module-level
names matching gunicorn setting names (`on_starting`, `post_worker_init`,
`child_exit`) *are* the server configuration.

`KEEP_CONSUMER_*` bound the EventSubscriber lifecycle. Its consumer threads call
back into this API, so the start is deferred until lifespan startup completes,
and the shutdown bounds keep a wedged consumer from holding shutdown past the
pod's `terminationGracePeriodSeconds`.
"""

import logging
import os
import src.utils.logging
from src.config.core import starlette_config
from src.services.identity_manager.identitymanagerfactory import IdentityManagerTypes
from importlib import metadata

# We read AUTH_TYPE directly to avoid importing keep.api.api which triggers a cascade of imports
# that might fail during early startup or in restricted environments.
# Using cast=str to ensure we always get a string, enforcing type if passing objects by mistake
AUTH_TYPE = starlette_config("AUTH_TYPE", default=IdentityManagerTypes.NOAUTH.value, cast=str).lower()
try:
    KEEP_VERSION = metadata.version("keep")
except Exception:
    KEEP_VERSION = starlette_config("KEEP_VERSION", default="unknown")

HOST = starlette_config("KEEP_HOST", default="0.0.0.0")
PORT = starlette_config("PORT", default=8080, cast=int)
SCHEDULER = starlette_config("SCHEDULER", default="true", cast=bool)
CONSUMER = starlette_config("CONSUMER", default="true", cast=bool)
KEEP_CONSUMER_START_DELAY = starlette_config(
    "KEEP_CONSUMER_START_DELAY", default=0, cast=float
)
KEEP_CONSUMER_JOIN_TIMEOUT = starlette_config(  # per consumer thread
    "KEEP_CONSUMER_JOIN_TIMEOUT", default=10, cast=float
)
KEEP_CONSUMER_STOP_TIMEOUT = starlette_config(  # overall
    "KEEP_CONSUMER_STOP_TIMEOUT", default=20, cast=float
)
TOPOLOGY = starlette_config("KEEP_TOPOLOGY_PROCESSOR", default="false", cast=bool)
WATCHER = starlette_config("WATCHER", default="false", cast=bool)
KEEP_DEBUG_TASKS = starlette_config("KEEP_DEBUG_TASKS", default="false", cast=bool)

KEEP_USE_LIMITER = starlette_config("KEEP_USE_LIMITER", default="false", cast=bool)
MAINTENANCE_WINDOWS = starlette_config("MAINTENANCE_WINDOWS", default="false", cast=bool)

KEEP_API_URL = starlette_config("KEEP_API_URL", default=None)
KEEP_METRICS = starlette_config("KEEP_METRICS", default="true", cast=bool)
KEEP_OTEL_ENABLED = starlette_config("KEEP_OTEL_ENABLED", default="true", cast=bool)
KEEP_WORKERS = starlette_config("KEEP_WORKERS", default=None, cast=int)
KEEP_LIMIT_CONCURRENCY = starlette_config("KEEP_LIMIT_CONCURRENCY", default=None, cast=int)
# Rate-limit windows for SlowAPI, in its own "N/period" string format.
#
# The gateway reads BOTH of these from `KEEP_LIMIT_CONCURRENCY` — the same
# variable uvicorn's integer concurrency limit above uses. Setting it to tune
# uvicorn therefore feeds an integer to SlowAPI, and setting it for SlowAPI feeds
# "100/minute" to `cast=int`. The two cannot be configured independently, and
# whichever you set breaks the other. Given separate names here rather than
# carried across, because this service is the one whose rate limit protects the
# public intake route.
KEEP_LIMITER_DEFAULT_LIMIT = starlette_config(
    "KEEP_LIMITER_DEFAULT_LIMIT", default="100/minute", cast=str
)
KEEP_METRICS_LIMIT = starlette_config(
    "KEEP_METRICS_LIMIT", default="10/minute", cast=str
)

KEEP_READ_ONLY = starlette_config("KEEP_READ_ONLY", default="false", cast=bool)
# Product BI — active-users (DAU/WAU/MAU) refresh job (Phase 1).
KEEP_ACTIVE_USERS_JOB = starlette_config("KEEP_ACTIVE_USERS_JOB", default="true", cast=bool)
KEEP_ACTIVE_USERS_REFRESH_INTERVAL = starlette_config("KEEP_ACTIVE_USERS_REFRESH_INTERVAL", default=300, cast=int)
# Product BI: periodically recompute the point-in-time incident gauges
# (alerts-associated-to-incidents, incidents-with-ticket).
KEEP_INCIDENT_METRICS_JOB = starlette_config("KEEP_INCIDENT_METRICS_JOB", default="true", cast=bool)
KEEP_INCIDENT_METRICS_REFRESH_INTERVAL = starlette_config("KEEP_INCIDENT_METRICS_REFRESH_INTERVAL", default=300, cast=int)
KEEP_PROVIDER_DISTRIBUTION_ENABLED = starlette_config("KEEP_PROVIDER_DISTRIBUTION_ENABLED", default="true", cast=bool)
KEEP_PLATFORM_URL = starlette_config("KEEP_PLATFORM_URL", default="https://platform.keephq.dev")


# CORS: comma-separated list of trusted browser origins allowed to make credentialed requests.
# Defaults to KEEP_PLATFORM_URL. Override with e.g.:
#   KEEP_CORS_TRUSTED_ORIGINS=https://app.example.com,https://staging.example.com
_cors_origins_raw = starlette_config(
    "KEEP_CORS_TRUSTED_ORIGINS",
    default=KEEP_PLATFORM_URL,
)
KEEP_CORS_TRUSTED_ORIGINS: list[str] = [
    o.strip() for o in _cors_origins_raw.split(",") if o.strip()
]

src.utils.logging.setup_logging()
logger = logging.getLogger(__name__)



def _clear_prometheus_multiproc_dir():
    """
    Empty the prometheus multiprocess directory before workers start.

    prometheus_client multiprocess mode writes one set of mmap files per PID and
    never cleans them up on its own. Across restarts these accumulate without
    bound (we observed ~287k files / 18GB), and because the /metrics scrape reads
    *every* file in the dir, collection slows to the point the endpoint hangs and
    starves the worker pool. Clearing the dir once in the gunicorn master (before
    any worker forks/writes) keeps it bounded; child_exit() reaps per-worker
    files as workers die. Required by the prometheus_client multiprocess docs.
    """
    import glob

    prom_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR")
    if not prom_dir or not os.path.isdir(prom_dir):
        return
    removed = 0
    for path in glob.glob(os.path.join(prom_dir, "*.db")):
        try:
            os.remove(path)
            removed += 1
        except OSError:
            pass
    logger.info(
        "Cleared prometheus multiproc dir", extra={"dir": prom_dir, "removed": removed}
    )


def on_starting(server=None):
    """This function is called by the gunicorn server when it starts.

    Runs in the gunicorn master, before workers fork and before the socket binds.

    Where the gateway calls `init_services()` — which runs `alembic upgrade head`
    and provisions dashboards, providers and tenants — this service **waits** for
    the gateway to have finished doing that. It ships no migrations and owns no
    provisioning. `SKIP_DB_CREATION=true` is set as a second guard, so even a
    mistaken call here cannot touch the schema.

    The wait retries indefinitely rather than raising: a gateway still migrating
    must make this service slow to start, not crash it, and the startupProbe
    budget is what decides when to give up on the pod.
    """
    # Must run in the master before workers fork so we never delete a live
    # worker's files.
    _clear_prometheus_multiproc_dir()

    from src.repositories.db_on_start import migrate_db

    migrate_db()


def post_worker_init(worker):
    # We need to reinitialize logging in each worker because gunicorn forks the worker processes
    print("Init logging in worker")
    logging.getLogger().handlers = []  # noqa
    src.utils.logging.setup_logging()  # noqa
    print("Logging initialized in worker")


def child_exit(server, worker):
    """
    gunicorn hook: reap a dead worker's prometheus mmap files.

    Without this, a restarted/crashed worker's gauge files linger and pollute the
    scrape (live* gauges keep counting dead PIDs; max/min retain stale extremes).
    """
    try:
        from prometheus_client import multiprocess

        multiprocess.mark_process_dead(worker.pid)
    except Exception:
        logging.getLogger(__name__).debug(
            "mark_process_dead failed", exc_info=True
        )


post_worker_init = post_worker_init
child_exit = child_exit

