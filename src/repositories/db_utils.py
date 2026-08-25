"""Engine construction.

Postgres in every deployed environment, SQLite for tests and a standalone local
run. The gateway's copy also carries a Cloud SQL / `mysql+pymysql` path behind
`RUNNING_IN_CLOUD_RUN` and a `DB_CONNECTION_STRING == "impersonate"` mode; both
are dropped here along with `pymysql` and `google-cloud-sql-connector`, because
this service is deployed to the same cluster as `keep-event-handler` and talks to
the same Postgres the gateway migrates.

The JSON dialect helpers the gateway keeps alongside this (`get_json_extract_field`,
`get_aggreated_field`, the `json_table` compilation) exist for CEL-to-SQL
translation on UI queries. Nothing here queries JSON: the three tables this
service reads are plain columns.
"""

import json
import logging

from sqlalchemy import create_engine

from src.config.consts import (
    DB_CONNECTION_STRING,
    DB_ECHO,
    DB_MAX_OVERFLOW,
    DB_POOL_RECYCLE,
    DB_POOL_SIZE,
    DB_POOL_TIMEOUT,
    KEEP_DB_PRE_PING_ENABLED,
)

logger = logging.getLogger(__name__)


def dumps(_json) -> str:
    """
    Overcome the issue of serializing datetime objects to JSON with the default json.dumps.
       Usually seen with PostgreSQL JSONB fields.
    https://stackoverflow.com/questions/36438052/using-a-custom-json-encoder-for-sqlalchemys-postgresql-jsonb-implementation

    Args:
        _json (object): The json object to serialize.

    Returns:
        str: The serialized JSON object.
    """
    return json.dumps(_json, default=str)


def create_db_engine():
    """
    Creates a database engine based on the environment variables.

    The connection string should name a role with `SELECT` on `tenant`,
    `tenantapikey` and `operator` and nothing else — this service has no write
    path, and the grant is what enforces that rather than merely documenting it.
    """
    if DB_CONNECTION_STRING:
        try:
            logger.info(f"Creating a connection pool with size {DB_POOL_SIZE}")
            engine = create_engine(
                DB_CONNECTION_STRING,
                pool_size=DB_POOL_SIZE,
                max_overflow=DB_MAX_OVERFLOW,
                pool_recycle=DB_POOL_RECYCLE,
                pool_timeout=DB_POOL_TIMEOUT,
                json_serializer=dumps,
                echo=DB_ECHO,
                pool_pre_ping=True if KEEP_DB_PRE_PING_ENABLED else False,
            )
        # SQLite does not support pool_size
        except TypeError:
            engine = create_engine(
                DB_CONNECTION_STRING, json_serializer=dumps, echo=DB_ECHO
            )
    else:
        engine = create_engine(
            "sqlite:///./keep.db",
            connect_args={"check_same_thread": False},
            echo=DB_ECHO,
            json_serializer=dumps,
        )
    return engine
