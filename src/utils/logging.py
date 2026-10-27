import http.client
import inspect
import logging
import logging.config
import logging.handlers
import os
import sys
import threading
import uuid
import queue
import requests
import json
from datetime import datetime
from threading import Timer

# tb: small hack to avoid the InsecureRequestWarning logs
import urllib3
from pythonjsonlogger import jsonlogger

from src.config.consts import RUNNING_IN_CLOUD_RUN

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



logger = logging.getLogger(__name__)


def get_gunicorn_log_level():
    """
    Check for --log-level flag in gunicorn command line arguments
    Returns the log level or None if not found
    """
    log_level = None
    try:
        for i, arg in enumerate(sys.argv):
            if arg == "--log-level" and i + 1 < len(sys.argv):
                log_level = sys.argv[i + 1].upper()
                break
            elif arg.startswith("--log-level="):
                log_level = arg.split("=", 1)[1].upper()
                break
    except Exception:
        pass

    # Validate the log level
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if log_level in valid_levels:
        return log_level

    # o/w, use Keep's log level
    return LOG_LEVEL








# `ProviderDBHandler` / `ProviderLoggerAdapter` are deliberately absent.
#
# They buffered provider *execution* logs and flushed them into the
# `providerexecutionlog` table on a timer. That is a database WRITE on the
# logging path, which a service holding a SELECT-only role cannot perform — and
# there are no providers here to log: the provider framework lives in
# keep-api-gateway. Logs from this service go to stdout and, when configured,
# fluentbit.


LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
KEEP_LOG_FILE = os.environ.get("KEEP_LOG_FILE", "false").lower() == "true"
KEEP_FLUENTBIT = os.environ.get("KEEP_FLUENTBIT", "true").lower() == "true"
KEEP_FLUENTBIT_HOST = os.environ.get("KEEP_FLUENTBIT_HOST")
KEEP_FLUENTBIT_PORT = os.environ.get("KEEP_FLUENTBIT_PORT", "80")
LOG_FORMAT_OPEN_TELEMETRY = "open_telemetry"
LOG_FORMAT_DEVELOPMENT_TERMINAL = "dev_terminal"

LOG_FORMAT = os.environ.get("LOG_FORMAT", LOG_FORMAT_OPEN_TELEMETRY)


class OtelFieldsFilter(logging.Filter):
    """Guarantee the OTEL correlation fields exist on every record.

    `LoggingInstrumentor().instrument()` is what injects `otelTraceID` and its
    siblings, and it runs inside the `KEEP_OTEL_ENABLED` block in
    `observability.setup()`. With OTEL disabled nothing injects them, while the
    `uvicorn_access` formatter interpolates `%(otelTraceID)s` unconditionally --
    so every access log line raised `ValueError: Formatting field not found in
    record` and was dropped. Logging swallows handler errors, so the symptom was
    silently missing access logs plus a traceback on stderr, not a crash.

    `DevTerminalFormatter` already defended itself this way and
    `CustomJsonFormatter` emits nulls; only the plain `uvicorn_access` formatter
    was exposed. A filter fixes it for every handler at once rather than leaving
    the next formatter to rediscover it.
    """

    DEFAULTS = {
        "otelTraceID": "-",
        "otelSpanID": "-",
        "otelTraceSampled": "-",
        "otelServiceName": "-",
    }

    def filter(self, record: logging.LogRecord) -> bool:
        for field, default in self.DEFAULTS.items():
            if not hasattr(record, field):
                setattr(record, field, default)
        return True


class DevTerminalFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, "otelTraceID"):
            record.otelTraceID = "-"  # or any default value you prefer

        message = super().format(record)
        extra_info = ""

        # Use inspect to go up the stack until we find the _log function
        frame = inspect.currentframe()
        while frame:
            if frame.f_code.co_name == "_log":
                # Extract extra from the _log function's local variables
                extra = frame.f_locals.get("extra", {})
                if extra:
                    extra_info = " ".join(
                        [f"[{k}: {v}]" for k, v in extra.items() if k != "raw_event"]
                    )
                else:
                    extra_info = ""
                break
            frame = frame.f_back

        return f"{message} {extra_info}"


def get_worker_type():
    """Determine if this is a uvicorn or arq worker"""
    import sys

    # Check command line arguments or process name to identify worker type
    # Check command line arguments or process name to identify worker type
    if any("uvicorn" in arg.lower() for arg in sys.argv):
        return "uvicorn"
    else:
        return None


# Set this as a global variable during initialization
WORKER_TYPE = get_worker_type()


class FluentBitHandler(logging.Handler):
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    def __init__(self, host, port, tenant="keep", **kwargs):
        super().__init__()
        self.url = f"http://{host}:{port}"
        self.tenant = tenant
        self.queue = queue.Queue(maxsize=1000)
        self.session = requests.Session()
        self._stop = threading.Event()
        threading.Thread(target=self._send, daemon=True).start()

    def _send(self):
        while not self._stop.is_set():
            try:
                record = self.queue.get(timeout=1)
                if record is None:
                    break
                json_record = json.loads(record)
                json_record["tenant"] = self.tenant
                try:
                    self.session.post(self.url, json=json_record, verify=False)
                except Exception:
                    pass
            except queue.Empty:
                continue

    def emit(self, record):
        try:
            self.queue.put_nowait(self.format(record))
        except queue.Full:
            pass

    def close(self):
        try:
            self.queue.put(None)
            self._stop.set()

        except Exception:
            pass
        super().close()


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def __init__(self, *args, rename_fields=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.rename_fields = rename_fields if RUNNING_IN_CLOUD_RUN else {}

    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        # Add worker type to all logs
        if WORKER_TYPE:
            log_record["worker_type"] = getattr(record, "worker_type", WORKER_TYPE)


CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "()": CustomJsonFormatter,
            "fmt": "%(worker_type) %(asctime)s %(message)s %(levelname)s %(name)s %(filename)s %(otelTraceID)s %(otelSpanID)s %(otelTraceSampled)s %(otelServiceName)s %(threadName)s %(process)s %(module)s",
            "rename_fields": {
                "levelname": "severity",
                "asctime": "timestamp",
                "otelTraceID": "logging.googleapis.com/trace",
                "otelSpanID": "logging.googleapis.com/spanId",
                "otelTraceSampled": "logging.googleapis.com/trace_sampled",
            },
        },
        "dev_terminal": {
            "()": DevTerminalFormatter,
            "format": "%(asctime)s - %(thread)s %(otelTraceID)s %(threadName)s %(levelname)s - %(message)s",
        },
        "uvicorn_access": {  # Add new formatter for uvicorn.access
            "format": "%(asctime)s - %(otelTraceID)s - %(threadName)s - %(message)s"
        },
    },
    "handlers": {
        "default": {
            "level": LOG_LEVEL,
            "formatter": (
                "json" if LOG_FORMAT == LOG_FORMAT_OPEN_TELEMETRY else "dev_terminal"
            ),
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "filters": ["otel_fields"],
        },
        "uvicorn_access": {  # Add new handler for uvicorn.access
            "class": "logging.StreamHandler",
            "formatter": "uvicorn_access",
            "filters": ["otel_fields"],
        },
    },
    "filters": {
        # Runs before formatting, so the OTEL fields are present whether or not
        # KEEP_OTEL_ENABLED is set. See OtelFieldsFilter.
        "otel_fields": {"()": OtelFieldsFilter},
    },
    "loggers": {
        "": {
            "handlers": ["default"],
            "level": "DEBUG",
            "propagate": False,
        },
        "slowapi": {
            "handlers": ["default"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
        "uvicorn.access": {  # Add uvicorn.access logger configuration
            "handlers": ["uvicorn_access"],
            "level": get_gunicorn_log_level(),
            "propagate": False,
        },
        "uvicorn.error": {  # Add uvicorn.error logger configuration
            "()": "CustomizedUvicornLogger",  # Use custom logger class
            "handlers": ["default"],
            "level": get_gunicorn_log_level(),
            "propagate": False,
        },
        "opentelemetry.context": {
            "handlers": [],
            "level": "CRITICAL",
            "propagate": False,
        },
        "Evaluator": {
            "handlers": [],
            "level": "CRITICAL",
            "propagate": False,
        },
        "NameContainer": {
            "handlers": [],
            "level": "CRITICAL",
            "propagate": False,
        },
        "evaluation": {
            "handlers": [],
            "level": "CRITICAL",
            "propagate": False,
        },
        "Environment": {
            "handlers": [],
            "level": "CRITICAL",
            "propagate": False,
        },
        "httpx": {
            "handlers": [],
            "level": "ERROR",
            "propagate": False,
        },
        "http.client": {
            "handlers": ["default"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}


class CustomizedUvicornLogger(logging.Logger):
    """This class overrides the default Uvicorn logger to add trace_id to the log record

    Args:
        logging (_type_): _description_
    """

    def makeRecord(
        self,
        name,
        level,
        fn,
        lno,
        msg,
        args,
        exc_info,
        func=None,
        extra=None,
        sinfo=None,
    ):
        if extra:
            trace_id = extra.pop("otelTraceID", None)
        else:
            trace_id = None
        rv = super().makeRecord(
            name, level, fn, lno, msg, args, exc_info, func, extra, sinfo
        )
        if trace_id:
            rv.__dict__["otelTraceID"] = trace_id
        return rv

    def _log(
        self,
        level,
        msg,
        args,
        exc_info=None,
        extra=None,
        stack_info=False,
        stacklevel=1,
    ):
        # Find trace_id from call stack
        frame = (
            inspect.currentframe().f_back
        )  # Go one level up to get the caller's frame
        while frame:
            found_frame = False
            if frame.f_code.co_name == "run_asgi":
                trace_id = (
                    frame.f_locals.get("self").scope.get("state", {}).get("trace_id", 0)
                )
                tenant_id = (
                    frame.f_locals.get("self")
                    .scope.get("state", {})
                    .get("tenant_id", 0)
                )
                if trace_id:
                    if extra is None:
                        extra = {}
                    extra.update({"otelTraceID": trace_id})
                    found_frame = True
                if tenant_id:
                    if extra is None:
                        extra = {}
                    extra.update({"tenant_id": tenant_id})
                    found_frame = True
            # if we found the frame, we can stop searching
            if found_frame:
                break
            frame = frame.f_back

        # Call the original _log function to handle the logging with trace_id
        logging.Logger._log(
            self, level, msg, args, exc_info, extra, stack_info, stacklevel
        )


def setup_logging():
    # Add file handler if KEEP_LOG_FILE is set
    # TODO: remove this after we move to fluentbit
    if KEEP_LOG_FILE:
        CONFIG["handlers"]["file"] = {
            "level": "DEBUG",
            "formatter": ("json"),
            "class": "logging.handlers.RotatingFileHandler",
            "filename": KEEP_LOG_FILE,
            "mode": "a",
            "maxBytes": 1024 * 1024 * 1024,  # 1GB
            "backupCount": 5,
        }
        # Add file handler to root logger
        CONFIG["loggers"][""]["handlers"].append("file")

    # Add fluentbit handler if KEEP_FLUENTBIT is set
    if KEEP_FLUENTBIT:
        CONFIG["handlers"]["fluentbit"] = {
            "level": "DEBUG",
            "formatter": ("json"),
            "class": "src.utils.logging.FluentBitHandler",
            "host": KEEP_FLUENTBIT_HOST,
            "port": int(KEEP_FLUENTBIT_PORT),
        }
        # Add file handler to root logger
        CONFIG["loggers"][""]["handlers"].append("fluentbit")

    logging.config.dictConfig(CONFIG)
    # MONKEY PATCHING http.client
    # See: https://stackoverflow.com/questions/58738195/python-http-request-and-debug-level-logging-to-the-log-file
    http_client_logger = logging.getLogger("http.client")
    http_client_logger.setLevel(logging.DEBUG)
    http.client.HTTPConnection.debuglevel = 1

    def print_to_log(*args):
        http_client_logger.debug(" ".join(args))

    # monkey-patch a `print` global into the http.client module; all calls to
    # print() in that module will then use our print_to_log implementation
    http.client.print = print_to_log

