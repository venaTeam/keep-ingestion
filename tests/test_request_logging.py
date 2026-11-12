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
