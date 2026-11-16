"""Tests for the per-request log line and the catch-all exception handler."""

import logging

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.middlewares import LoggingMiddleware, catch_exception


def _app():
    app = FastAPI()

    @app.get("/boom")
    async def boom():
        raise RuntimeError("boom")

    app.add_exception_handler(Exception, catch_exception)
    app.add_middleware(LoggingMiddleware)
    return app


def test_unhandled_exception_logs_request_line_and_one_traceback(caplog):
    client = TestClient(_app(), raise_server_exceptions=False)

    with caplog.at_level(logging.INFO, logger="src.middlewares"):
        response = client.get("/boom")

    assert response.status_code == 500
    assert response.json()["trace_id"] == "no-trace"
    finished = [r for r in caplog.records if r.getMessage() == "Request finished"]
    assert len(finished) == 1
    assert finished[0].levelno == logging.ERROR
    assert finished[0].status_code == 500
    assert finished[0].exc_info is None

    unhandled = [r for r in caplog.records if r.getMessage() == "Unhandled exception"]
    assert len(unhandled) == 1
    assert unhandled[0].exc_info is not None
    assert unhandled[0].method == "GET"
    assert unhandled[0].path == "/boom"
    assert unhandled[0].tenant_id == "anonymous"
    assert unhandled[0].trace_id == "no-trace"


def test_uvicorn_duplicate_traceback_is_dropped():
    from src.utils.logging import DropUvicornASGIExceptionFilter

    f = DropUvicornASGIExceptionFilter()

    def rec(msg):
        return logging.LogRecord("uvicorn.error", logging.ERROR, "", 0, msg, None, None)

    assert not f.filter(rec("Exception in ASGI application\n"))
    assert f.filter(rec("Started server process"))


def test_redact_headers_hides_secret_headers_any_case():
    from src.middlewares import _redact_headers

    headers = {
        "Authorization": "Bearer x",
        "X-Api-Key": "k",
        "Cookie": "s=1",
        "X-Gitlab-Token": "t",
        "X-Hub-Signature-256": "sha256=x",
        "X-Grafana-Token": "t",
        "Content-Type": "application/json",
        "User-Agent": "curl",
    }
    redacted = _redact_headers(headers)
    assert redacted.pop("Content-Type") == "application/json"
    assert redacted.pop("User-Agent") == "curl"
    assert set(redacted.values()) == {"[REDACTED]"}
