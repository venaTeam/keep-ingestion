import os

from dotenv import find_dotenv, load_dotenv

from src.config.core import config

load_dotenv(find_dotenv())
RUNNING_IN_CLOUD_RUN = os.environ.get("K_SERVICE") is not None
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

