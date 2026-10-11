"""Settles PROMETHEUS_MULTIPROC_DIR, then re-exports the metric primitives.

**Modules that define metrics import their primitives from here, not from
prometheus_client.** That is the entire reason this file exists, and it is why
the one import below sits after executable code — an ordering that has to live
somewhere, isolated here instead of spread across every module that defines a
metric. Importing the names from one place also keeps the ordering safe from an
import sorter, which would otherwise hoist `prometheus_client` above a
first-party import and silently undo it.

(`src/routes/metrics.py` and `src/config/config.py` still import
prometheus_client directly. They consume the registry — scrape endpoint,
multiproc cleanup — rather than defining metrics, so they are not part of this
ordering contract.)

Why the order is load-bearing: `prometheus_client.values` picks single-process
vs multiprocess mode when it is first imported, by looking at
PROMETHEUS_MULTIPROC_DIR. Setting the variable after that import is a no-op, so
the resolution has to happen first.

The resolution refuses two silent failure modes:

* A *set-but-empty* value must not win over the default. prometheus_client
  joins the directory with ``counter_<pid>.db``, so an empty string writes
  metric mmap files into the process CWD — this is what committed four ``*.db``
  files to keep-event-handler's repo root.
* An uncreatable directory must not be swallowed. The old code caught it with a
  bare ``except: pass``, which produced the same CWD writes with no signal.
  It now logs and falls back to the system temp dir, never to the CWD.
"""

import logging
import os
import tempfile

logger = logging.getLogger(__name__)

DEFAULT_MULTIPROC_DIR = "/tmp/prometheus"


def _resolve_prometheus_multiproc_dir() -> str:
    configured = os.environ.get("PROMETHEUS_MULTIPROC_DIR") or DEFAULT_MULTIPROC_DIR
    try:
        os.makedirs(configured, exist_ok=True)
        return configured
    except OSError:
        fallback = os.path.join(tempfile.gettempdir(), "prometheus")
        logger.error(
            "PROMETHEUS_MULTIPROC_DIR %r cannot be created; using %r instead",
            configured,
            fallback,
        )
        os.makedirs(fallback, exist_ok=True)
        return fallback


os.environ["PROMETHEUS_MULTIPROC_DIR"] = _resolve_prometheus_multiproc_dir()

from prometheus_client import (  # noqa: E402  (see the module docstring)
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    Summary,
)

__all__ = [
    "REGISTRY",
    "Counter",
    "Gauge",
    "Histogram",
    "Summary",
    "DEFAULT_MULTIPROC_DIR",
]
