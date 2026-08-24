from __future__ import annotations

import inspect
from types import SimpleNamespace

from inefficiency_engine import control_cycle_executor
from inefficiency_engine import cycle_history_bucket_timeout_runtime as runtime


def test_cycle_history_bucket_timeout_is_short_and_clamped(monkeypatch):
    monkeypatch.delenv(
        "CIE_CONTROL_CYCLE_HISTORY_BUCKET_STATEMENT_TIMEOUT_SECONDS",
        raising=False,
    )
    monkeypatch.delenv(
        "CIE_CONTROL_CYCLE_HISTORY_BUCKET_LOCK_TIMEOUT_SECONDS",
        raising=False,
    )
    assert runtime.cycle_history_bucket_statement_timeout_seconds() == 4.0
    assert runtime.cycle_history_bucket_lock_timeout_seconds() == 1.0

    monkeypatch.setenv(
        "CIE_CONTROL_CYCLE_HISTORY_BUCKET_STATEMENT_TIMEOUT_SECONDS",
        "99",
    )
    monkeypatch.setenv(
        "CIE_CONTROL_CYCLE_HISTORY_BUCKET_LOCK_TIMEOUT_SECONDS",
        "99",
    )
    assert runtime.cycle_history_bucket_statement_timeout_seconds() == 6.0
    assert runtime.cycle_history_bucket_lock_timeout_seconds() == 2.0


def test_postgres_cycle_history_timeout_is_transaction_local_and_removed(monkeypatch):
    installed: dict[str, object] = {}
    removed: list[tuple[object, str, object]] = []

    def fake_listen(engine, name, listener):
        installed.update(engine=engine, name=name, listener=listener)

    def fake_remove(engine, name, listener):
        removed.append((engine, name, listener))

    monkeypatch.setattr(runtime.event, "listen", fake_listen)
    monkeypatch.setattr(runtime.event, "remove", fake_remove)

    engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    store = SimpleNamespace(engine=engine)
    with runtime.cycle_history_bucket_database_timeout(store):
        assert installed["engine"] is engine
        assert installed["name"] == "begin"
        statements: list[str] = []
        connection = SimpleNamespace(exec_driver_sql=statements.append)
        installed["listener"](connection)
        assert statements == [
            "SET LOCAL statement_timeout = 4000",
            "SET LOCAL lock_timeout = 1000",
        ]

    assert removed == [(engine, "begin", installed["listener"])]


def test_non_postgres_cycle_history_timeout_does_not_install_listener(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(runtime.event, "listen", lambda *args: calls.append("listen"))
    monkeypatch.setattr(runtime.event, "remove", lambda *args: calls.append("remove"))

    store = SimpleNamespace(engine=SimpleNamespace(dialect=SimpleNamespace(name="sqlite")))
    with runtime.cycle_history_bucket_database_timeout(store):
        pass

    assert calls == []


def test_control_executor_scopes_timeout_only_around_cycle_history_bootstrap():
    source = inspect.getsource(control_cycle_executor.run_one_control_cycle)
    scoped = source.index("with cycle_history_bucket_database_timeout(store):")
    call = source.index(
        "cycle_history_progress = advance_durable_control_cycle_history_cache",
        scoped,
    )
    exception_handler = source.index("except Exception as exc:", call)

    assert scoped < call < exception_handler
    assert "statement_timeout_seconds = max(5.0, deadline - 5.0)" in source
    assert 'float(os.getenv("CIE_CONTROL_CYCLE_DEADLINE_SECONDS", "25.0"))' in source
