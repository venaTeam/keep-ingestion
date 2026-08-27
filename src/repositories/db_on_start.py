"""Startup schema handling for `keep-ingestion`.

**This service ships no migrations and must never create the schema.**
`keep-api-gateway` is the single owner of the shared database: it is the only
service holding `alembic/`, and it runs `alembic upgrade head` in gunicorn's
`on_starting` before its socket binds. This service waits for that to finish and
then runs read-only, exactly as `keep-event-handler` and `keep-workflows` already
do — `_wait_for_schema` below is ported from
`keep-event-handler/src/core/db/db_on_start.py`.

Building the schema here instead would race the gateway's upgrade on a cold
database and collide in `pg_type` (duplicate key `(typname)=(tenant)`), and it
would bypass migration history entirely, since `create_all` materialises the
current model without stamping `alembic_version`.

Readiness is detected by **quiescence of the gateway's alembic head**: the
gateway advances `alembic_version` after each migration, so once the revision
stops changing across several consecutive polls, its run is complete. Checking
only that the core tables exist is not enough — the gateway creates
`alembic_version` and `alert` early but adds columns in later migrations, so a
table-existence check alone lets a cold boot query a column that does not exist
yet. `KEEP_SCHEMA_EXPECTED_REVISION` pins an exact revision when the deploy knows
its target, and is the only guard that tells "the gateway finished" apart from
"the gateway never started" (a down gateway's stale head is perfectly quiescent).
"""

import logging
import os
import threading
import time

from sqlalchemy import inspect as sa_inspect
from sqlalchemy import text
from sqlmodel import SQLModel

from src.repositories.db import engine

# Imported for their side effect: registering the tables on SQLModel.metadata so
# the SQLite branch below can create them for tests.
from src.models.db.operator import *  # noqa: F401,F403  pylint: disable=unused-wildcard-import
from src.models.db.tenant import *  # noqa: F401,F403  pylint: disable=unused-wildcard-import

logger = logging.getLogger(__name__)

_SCHEMA_WAIT_TIMEOUT = int(os.environ.get("KEEP_SCHEMA_WAIT_TIMEOUT", "180"))
_SCHEMA_WAIT_INTERVAL = int(os.environ.get("KEEP_SCHEMA_WAIT_INTERVAL", "2"))
# How many consecutive polls the gateway's alembic head must stay unchanged
# before we treat its `alembic upgrade head` run as finished.
_SCHEMA_STABLE_CHECKS = int(os.environ.get("KEEP_SCHEMA_STABLE_CHECKS", "3"))
# Backoff between whole schema-wait attempts (retry forever, never exit).
_SCHEMA_RETRY_BACKOFF_START = int(os.environ.get("KEEP_SCHEMA_RETRY_BACKOFF_START", "5"))
_SCHEMA_RETRY_BACKOFF_MAX = int(os.environ.get("KEEP_SCHEMA_RETRY_BACKOFF_MAX", "60"))
# Core tables that must exist before the schema counts as ready. Quiescence
# alone is not enough: if the gateway is DOWN, its stale head is trivially
# stable, and this service would proceed against a not-yet-migrated schema.
#
# Narrower than the consumer's default list: this service reads only these three
# tables, and requiring `alert`/`lastalert` here would make intake wait on tables
# it never touches.
_SCHEMA_REQUIRED_TABLES = [
    t.strip()
    for t in os.environ.get(
        "KEEP_SCHEMA_REQUIRED_TABLES", "tenant,tenantapikey,operator"
    ).split(",")
    if t.strip()
]
# Optional exact revision to wait for. Unset by default: the gateway ships
# migrations no other repo's chain contains, so no head can be hardcoded here.
_SCHEMA_EXPECTED_REVISION = os.environ.get("KEEP_SCHEMA_EXPECTED_REVISION") or None

# Set by the startup signal handler so a SIGTERM during the wait aborts promptly
# instead of blocking for the whole grace period.
_abort_event = threading.Event()


class SchemaWaitTimeout(RuntimeError):
    """One schema-wait attempt timed out. Retryable — never fatal."""


class SchemaWaitAborted(RuntimeError):
    """The wait gave up because shutdown was requested.

    Distinct from success on purpose: returning normally would let `migrate_db`
    log "DB schema ready" and let startup carry on against a schema that was
    never verified.
    """


def request_schema_wait_abort():
    """Ask an in-flight schema wait to give up (SIGTERM during startup)."""
    _abort_event.set()


def schema_wait_aborted() -> bool:
    return _abort_event.is_set()


def _missing_required_tables(inspector) -> list:
    existing = set(inspector.get_table_names())
    return [t for t in _SCHEMA_REQUIRED_TABLES if t not in existing]


class AlembicVersionUnreadable(RuntimeError):
    """`alembic_version` exists but this role may not read it.

    Expected, not exceptional: the least-privilege grant this service is designed
    for is `SELECT` on `tenant`, `tenantapikey` and `operator` — `alembic_version`
    is deliberately not in it. Distinguished from "not stamped yet" (None) because
    the two mean opposite things: None is "the gateway has not migrated", this is
    "we are not allowed to look".
    """


def _gateway_alembic_head():
    """Return the alembic revision currently stamped on the shared DB.

    None if keep-api-gateway has not created/stamped `alembic_version` yet;
    raises `AlembicVersionUnreadable` if the table is there but this role cannot
    select from it.
    """
    inspector = sa_inspect(engine)
    if "alembic_version" not in inspector.get_table_names():
        return None
    try:
        with engine.connect() as conn:
            row = conn.execute(text("SELECT version_num FROM alembic_version")).first()
    except Exception as exc:
        raise AlembicVersionUnreadable(str(exc)) from exc
    return row[0] if row else None


def schema_ready() -> tuple[bool, dict]:
    """Non-blocking readiness check, for `/readyz`.

    **This is what `/readyz` must call instead of the gateway's
    `schema_at_head()`.** That function compares the database's stamped revision
    against the migration scripts *in the image*; this image ships none, so it
    would compare against an empty script directory and report "not at head"
    forever. `/readyz` gates the **startupProbe**, so getting this wrong does not
    surface as a bad response — it surfaces as CrashLoopBackOff.

    Answers the same question as `_wait_for_schema` but in one pass, with no
    sleeping: the probe has its own timeout and is polled on its own schedule, so
    it must never block. Quiescence is not observable in a single call, so this
    checks the required tables plus — when pinned — the expected revision.

    **The alembic head is optional here, and that is load-bearing.** The grant
    this service is built for does not include `alembic_version`, so reading it
    raises `InsufficientPrivilege`. Treating that as a failed check would report
    NotReady on every poll under exactly the least-privilege configuration the
    split exists to enable — the CrashLoopBackOff this function was written to
    avoid, reintroduced through the other door. The revision is therefore reported
    when readable and skipped when not.

    Setting `KEEP_SCHEMA_EXPECTED_REVISION` is the one case that genuinely needs
    it, so pinning a revision means also granting `SELECT ON alembic_version`. If
    the pin is set and the table cannot be read, this reports NotReady rather than
    silently ignoring the pin — a revision guard that quietly does nothing is
    worse than one that fails loudly.
    """
    detail: dict = {"required_tables": _SCHEMA_REQUIRED_TABLES}
    try:
        missing = _missing_required_tables(sa_inspect(engine))
        if missing:
            detail["missing_tables"] = missing
            return False, detail

        try:
            head = _gateway_alembic_head()
            detail["alembic_head"] = head
        except AlembicVersionUnreadable as exc:
            head = None
            detail["alembic_head"] = "unreadable (no grant on alembic_version)"
            if _SCHEMA_EXPECTED_REVISION:
                detail["expected_revision"] = _SCHEMA_EXPECTED_REVISION
                detail["error"] = (
                    "KEEP_SCHEMA_EXPECTED_REVISION is set but alembic_version is "
                    f"not readable by this role: {exc}"
                )
                return False, detail

        if _SCHEMA_EXPECTED_REVISION:
            detail["expected_revision"] = _SCHEMA_EXPECTED_REVISION
            return head == _SCHEMA_EXPECTED_REVISION, detail

        # No pinned revision: the tables this service reads are present, which is
        # the whole of its schema dependency. A stamped head is reported for
        # operators but not required — a gateway mid-migration will not remove
        # these three tables.
        return True, detail
    except Exception as exc:
        detail["error"] = str(exc)
        return False, detail


def _wait_for_schema():
    """Block until keep-api-gateway has FINISHED provisioning the shared schema.

    Quiescence of the gateway's alembic head is the primary signal, but it needs
    `SELECT` on `alembic_version`, which the least-privilege grant deliberately
    withholds. When the table is unreadable this degrades to requiring the three
    tables to be present and stay present across `KEEP_SCHEMA_STABLE_CHECKS`
    polls. That is a weaker guarantee — it cannot see a migration still adding
    columns — and it is why `KEEP_SCHEMA_EXPECTED_REVISION` (plus a grant on
    `alembic_version`) is the right setting for a deploy that wants the strong
    one. Without this fallback the wait never returns under the very grant this
    service is designed to run with, and the pod never finishes starting.
    """
    deadline = time.monotonic() + _SCHEMA_WAIT_TIMEOUT
    last_head = None
    stable = 0
    head_unreadable = False
    while True:
        if _abort_event.is_set():
            raise SchemaWaitAborted("schema wait aborted (shutdown requested)")
        try:
            missing = _missing_required_tables(sa_inspect(engine))
            if missing:
                # Catches an empty or half-built database before any head
                # comparison can call it "stable".
                last_head, stable = None, 0
                logger.info(
                    "Waiting for keep-api-gateway: required tables missing %s",
                    missing,
                )
                head = None
                head_is_unreadable = False
            else:
                try:
                    head = _gateway_alembic_head()
                    head_is_unreadable = False
                except AlembicVersionUnreadable as exc:
                    head, head_is_unreadable = None, True
                    unreadable_reason = exc

            if head_is_unreadable and _SCHEMA_EXPECTED_REVISION:
                # The pin cannot be honoured without the grant. Keep waiting and
                # say why, rather than starting against a schema whose revision
                # was never actually checked.
                last_head, stable = None, 0
                logger.warning(
                    "KEEP_SCHEMA_EXPECTED_REVISION is set but this role cannot "
                    "read alembic_version (%s); grant SELECT on it or unset the "
                    "pin",
                    unreadable_reason,
                )
            elif head_is_unreadable:
                # Degraded signal: the three tables are present, so count polls
                # of them staying present instead of polls of a steady revision.
                if not head_unreadable:
                    head_unreadable = True
                    logger.info(
                        "alembic_version is not readable by this role (expected "
                        "under the SELECT-only grant); falling back to "
                        "required-table stability"
                    )
                stable += 1
                if stable >= _SCHEMA_STABLE_CHECKS:
                    logger.info(
                        "DB schema is ready (required tables %s present and "
                        "stable across %s checks)",
                        _SCHEMA_REQUIRED_TABLES,
                        stable,
                    )
                    return
                logger.info(
                    "Required tables present (%s/%s)", stable, _SCHEMA_STABLE_CHECKS
                )
            elif head is not None and _SCHEMA_EXPECTED_REVISION:
                # An exact match beats quiescence: a stale head on a down
                # gateway is perfectly quiescent, and this is what tells the
                # two apart.
                if head == _SCHEMA_EXPECTED_REVISION:
                    logger.info(
                        "DB schema is ready (alembic head %s matches "
                        "KEEP_SCHEMA_EXPECTED_REVISION)",
                        head,
                    )
                    return
                logger.info(
                    "keep-api-gateway alembic head %s != expected %s; waiting",
                    head,
                    _SCHEMA_EXPECTED_REVISION,
                )
                last_head, stable = head, 0
            elif head is None:
                last_head, stable = None, 0
                logger.info(
                    "Waiting for keep-api-gateway to initialize the DB schema "
                    "(alembic_version not stamped yet)"
                )
            elif head == last_head:
                stable += 1
                if stable >= _SCHEMA_STABLE_CHECKS:
                    logger.info(
                        "DB schema is ready (owned by keep-api-gateway; alembic "
                        "head %s stable across %s checks)",
                        head,
                        stable,
                    )
                    return
                logger.info(
                    "keep-api-gateway alembic head %s steady (%s/%s)",
                    head,
                    stable,
                    _SCHEMA_STABLE_CHECKS,
                )
            else:
                logger.info(
                    "keep-api-gateway migrations still advancing "
                    "(alembic head -> %s); waiting for them to settle",
                    head,
                )
                last_head, stable = head, 1
        except Exception as exc:
            logger.warning("Waiting for the DB to become reachable: %s", exc)
            last_head, stable = None, 0
        if time.monotonic() >= deadline:
            raise SchemaWaitTimeout(
                "Timed out waiting for keep-api-gateway to provision the DB "
                f"schema (waited {_SCHEMA_WAIT_TIMEOUT}s; last alembic head "
                f"{last_head!r})"
            )
        # Interruptible: a SIGTERM during the wait must not sit out the sleep.
        if _abort_event.wait(_SCHEMA_WAIT_INTERVAL):
            raise SchemaWaitAborted("schema wait aborted (shutdown requested)")


def migrate_db():
    """Ensure the DB schema is ready before this service serves traffic.

    Named `migrate_db` to match the gateway's `on_starting` hook, but it
    deliberately migrates nothing — see the module docstring.

    A wait that times out is RETRIED with capped backoff, indefinitely, rather
    than raising. Raising would kill the pod at exactly the moment the cluster
    needs it up, because a full sync is when the gateway's migrations are slowest
    to settle. A slow gateway must make this service slow, not dead: the pod
    stays alive reporting "alive, not ready", and the startupProbe budget decides
    when to give up on it.
    """
    if os.environ.get("SKIP_DB_CREATION", "false") == "true":
        logger.info("Skipping DB schema init...")
        return None

    if engine.dialect.name == "sqlite":
        logger.info("SQLite engine — creating tables locally")
        SQLModel.metadata.create_all(engine)
        logger.info("Finished creating tables")
        return None

    logger.info("Waiting for keep-api-gateway to provision the DB schema...")
    backoff = _SCHEMA_RETRY_BACKOFF_START
    while not _abort_event.is_set():
        try:
            _wait_for_schema()
            logger.info("DB schema ready")
            return None
        except SchemaWaitAborted:
            break
        except SchemaWaitTimeout as exc:
            logger.warning("Schema not ready yet (%s); retrying in %ss", exc, backoff)
            if _abort_event.wait(backoff):
                break
            backoff = min(backoff * 2, _SCHEMA_RETRY_BACKOFF_MAX)

    # Never return normally here: the schema was NOT verified.
    logger.info("Stopped waiting for the DB schema (shutdown requested)")
    raise SchemaWaitAborted("startup aborted while waiting for the DB schema")
