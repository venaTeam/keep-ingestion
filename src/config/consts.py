import os

from dotenv import find_dotenv, load_dotenv

from src.config.core import config

load_dotenv(find_dotenv())
RUNNING_IN_CLOUD_RUN = os.environ.get("K_SERVICE") is not None
DB_CONNECTION_STRING = config("DATABASE_CONNECTION_STRING", default=None)  # pylint: disable=invalid-name
DB_POOL_SIZE = config("DATABASE_POOL_SIZE", default=5, cast=int)  # pylint: disable=invalid-name
DB_MAX_OVERFLOW = config("DATABASE_MAX_OVERFLOW", default=10, cast=int)  # pylint: disable=invalid-name
DB_ECHO = config("DATABASE_ECHO", default=False, cast=bool)  # pylint: disable=invalid-name
KEEP_FORCE_CONNECTION_STRING = config(
    "KEEP_FORCE_CONNECTION_STRING", default=False, cast=bool
)  # pylint: disable=invalid-name
KEEP_DB_PRE_PING_ENABLED = config("KEEP_DB_PRE_PING_ENABLED", default=True, cast=bool)  # pylint: disable=invalid-name
DB_POOL_RECYCLE = config("DATABASE_POOL_RECYCLE", default=300, cast=int)  # pylint: disable=invalid-name
DB_POOL_TIMEOUT = config("DATABASE_POOL_TIMEOUT", default=10, cast=int)  # pylint: disable=invalid-name
PROVIDER_PULL_INTERVAL_MINUTE = int(
    os.environ.get("KEEP_PULL_INTERVAL", 10080)
)  # maximum once a week
# STATIC_PRESETS is not carried over: presets are a UI concept and their
# models live in keep-api-gateway.
MAINTENANCE_WINDOW_ALERT_STRATEGY = os.environ.get(
    "MAINTENANCE_WINDOW_STRATEGY", "default"
)  # recover_previous_status or default
WATCHER_LAPSED_TIME = int(os.environ.get("KEEP_WATCHER_LAPSED_TIME", 60))  # in seconds


REDIS = os.environ.get("REDIS", "false") == "true"

if REDIS:
    pass
else:
    pass

OPENAI_MODEL_NAME = os.environ.get("OPENAI_MODEL_NAME", "gpt-4o-2024-08-06")


KEEP_CORRELATION_ENABLED = os.environ.get("KEEP_CORRELATION_ENABLED", "true") == "true"

MAX_PROCESSING_RETRIES = 3

