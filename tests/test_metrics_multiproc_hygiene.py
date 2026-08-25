"""
Tests for prometheus multiprocess-dir hygiene (the /metrics hang fix):
- on_starting clears stale *.db files from PROMETHEUS_MULTIPROC_DIR
- child_exit reaps a dead worker's files via mark_process_dead
"""

import prometheus_client.multiprocess as mp

from src.config.config import _clear_prometheus_multiproc_dir, child_exit


def test_clear_removes_only_db_files(tmp_path, monkeypatch):
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path))
    (tmp_path / "gauge_livemax_123.db").write_bytes(b"x")
    (tmp_path / "counter_456.db").write_bytes(b"y")
    (tmp_path / "keep.txt").write_text("not a db")  # must be left alone

    _clear_prometheus_multiproc_dir()

    assert sorted(p.name for p in tmp_path.iterdir()) == ["keep.txt"]


def test_clear_is_safe_when_dir_unset(monkeypatch):
    monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)
    # Must not raise even though the dir is not configured.
    _clear_prometheus_multiproc_dir()


def test_child_exit_marks_process_dead(monkeypatch):
    seen = {}
    monkeypatch.setattr(
        mp, "mark_process_dead", lambda pid: seen.setdefault("pid", pid)
    )

    class _Worker:
        pid = 4242

    child_exit(server=None, worker=_Worker())

    assert seen["pid"] == 4242


def test_child_exit_swallows_errors(monkeypatch):
    def _boom(pid):
        raise RuntimeError("nope")

    monkeypatch.setattr(mp, "mark_process_dead", _boom)

    class _Worker:
        pid = 1

    # Must not propagate — worker teardown should never be broken by metrics.
    child_exit(server=None, worker=_Worker())
