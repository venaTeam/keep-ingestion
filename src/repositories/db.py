"""Database access for `keep-ingestion`.

Three reads and nothing else. The gateway's `db.py` is ~5000 lines because it
backs the UI; this service only needs to answer "is this API key valid, and which
tenant does this alert's operator belong to". Keeping the module this small is
what makes the `SELECT`-only grant enforceable by inspection rather than by
trust — there is no write path here to audit.

The grant this service needs is `SELECT` on exactly three tables:

    tenant, tenantapikey    API-key verification
    operator               operator -> tenant routing on the intake path

**This service never runs Alembic.** `keep-api-gateway` owns the schema; see
`db_on_start.py`, which waits for it rather than creating it.
"""

import hashlib
import logging
import os
from contextlib import contextmanager
from typing import Iterator, Optional

from dotenv import find_dotenv, load_dotenv
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlmodel import Session, select

from src.models.db.operator import Operator
from src.models.db.tenant import TenantApiKey
from src.repositories.db_utils import create_db_engine

logger = logging.getLogger(__name__)

# because somehow in gunicorn it doesn't load the .env file
load_dotenv(find_dotenv())


engine = create_db_engine()
SQLAlchemyInstrumentor().instrument(enable_commenter=True, engine=engine)

# Whether API-key verification stamps `tenantapikey.last_used`.
#
# The gateway defaults this on. Here it defaults OFF, and that is the whole
# reason the flag exists: `_verify_api_key` otherwise issues an UPDATE on every
# authenticated request, which would force this service's database role to hold
# write access to `tenantapikey` and defeat the least-privilege boundary the
# split is for. Turning it on in this service requires widening the grant.
KEEP_APIKEY_TRACK_LAST_USED = (
    os.environ.get("KEEP_APIKEY_TRACK_LAST_USED", "false").lower() == "true"
)


def dispose_session():
    logger.info("Disposing engine pool")
    if engine.dialect.name != "sqlite":
        engine.dispose(close=False)
        logger.info("Engine pool disposed")
    else:
        logger.info("Engine pool is sqlite, not disposing")


@contextmanager
def existed_or_new_session(session: Optional[Session] = None) -> Iterator[Session]:
    try:
        if session is not None:
            yield session
        else:
            with Session(engine) as session:
                yield session
    finally:
        pass


def get_session() -> Session:
    """
    Creates a database session.

    Yields:
        Session: A database session
    """
    from opentelemetry import trace  # pylint: disable=import-outside-toplevel

    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("get_session"):
        with Session(engine) as session:
            yield session


def get_session_sync() -> Session:
    """
    Creates a database session.

    Returns:
        Session: A database session
    """
    return Session(engine)


def get_api_key(
    api_key: str,
    include_deleted: bool = False,
    session: Optional[Session] = None,
) -> TenantApiKey:
    with existed_or_new_session(session) as session:
        api_key_hashed = hashlib.sha256(api_key.encode()).hexdigest()
        statement = select(TenantApiKey).where(TenantApiKey.key_hash == api_key_hashed)
        if not include_deleted:
            statement = statement.where(TenantApiKey.is_deleted != True)  # noqa: E712
        tenant_api_key = session.exec(statement).first()
    return tenant_api_key


def update_key_last_used(
    tenant_id: str,
    reference_id: str,
    session: Optional[Session] = None,
) -> None:
    """No-op unless `KEEP_APIKEY_TRACK_LAST_USED` is set.

    Kept as a function rather than deleted so `authverifierbase` stays a
    line-for-line sibling of the gateway's copy — the difference between the two
    services is one environment variable, not a forked auth path. `last_used` is
    a convenience column for the UI's API-key screen, which this service does not
    serve, so nothing here reads what it would have written.
    """
    if not KEEP_APIKEY_TRACK_LAST_USED:
        return
    raise RuntimeError(
        "KEEP_APIKEY_TRACK_LAST_USED is set, but keep-ingestion holds a "
        "SELECT-only database role and has no write path. Unset it, or move "
        "last-used tracking to keep-api-gateway."
    )


def get_operator_by_name(name: str) -> Operator | None:
    """Resolve an operator by its routing-key name (unique). Used at ingestion
    to route an alert to the operator's tenant (VENA-5596 Epic 5)."""
    with Session(engine) as session:
        return session.exec(select(Operator).where(Operator.name == name)).first()
