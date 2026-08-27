from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from inefficiency_engine import render_combined_postbind_history_projection as production_entrypoint
from inefficiency_engine import startup_database_recovery as recovery


class _Engine:
    def __init__(self) -> None:
        self.dispose_calls = 0

    def dispose(self) -> None:
        self.dispose_calls += 1


class _Store:
    def __init__(self, name: str) -> None:
        self.engine = _Engine()
        self.safe_database_url = name


def _operational_error(message: str) -> OperationalError:
    return OperationalError("connect", {}, RuntimeError(message))


def _postbind(*, build_store, build_services, ensure_cache):
    settings = SimpleNamespace(evidence_db_path="postgresql://test")
    base = SimpleNamespace(
        Settings=SimpleNamespace(from_env=lambda: settings),
        build_evidence_store=build_store,
        _build_control_services=build_services,
    )
    return SimpleNamespace(
        base=base,
        ensure_durable_control_cache_schema=ensure_cache,
        bootstrap_permanent_runtime_schema=lambda: None,
    )


def test_recovery_mode_is_retried_with_fresh_store_and_failed_pool_disposed(monkeypatch):
    stores: list[_Store] = []
    service_attempts = 0
    cache_calls = 0
    sleeps: list[float] = []

    def build_store(_path: str):
        store = _Store(f"store-{len(stores) + 1}")
        stores.append(store)
        return store

    def build_services(_settings, _store):
        nonlocal service_attempts
        service_attempts += 1
        if service_attempts == 1:
            raise _operational_error("FATAL: the database system is in recovery mode")

    def ensure_cache(_store):
        nonlocal cache_calls
        cache_calls += 1

    postbind = _postbind(
        build_store=build_store,
        build_services=build_services,
        ensure_cache=ensure_cache,
    )
    monkeypatch.setattr(recovery.time, "monotonic", lambda: 0.0)
    monkeypatch.setattr(recovery.time, "sleep", lambda seconds: sleeps.append(seconds))

    recovery.install_startup_database_recovery(postbind)
    postbind.bootstrap_permanent_runtime_schema()

    assert service_attempts == 2
    assert len(stores) == 2
    assert stores[0].engine.dispose_calls == 1
    assert stores[1].engine.dispose_calls == 0
    assert sleeps == [recovery.STARTUP_DATABASE_RECOVERY_RETRY_SECONDS]
    assert cache_calls == 1


def test_non_recovery_operational_error_fails_immediately(monkeypatch):
    store = _Store("store")
    sleeps: list[float] = []

    def build_services(_settings, _store):
        raise _operational_error("password authentication failed for user cie")

    postbind = _postbind(
        build_store=lambda _path: store,
        build_services=build_services,
        ensure_cache=lambda _store: None,
    )
    monkeypatch.setattr(recovery.time, "monotonic", lambda: 0.0)
    monkeypatch.setattr(recovery.time, "sleep", lambda seconds: sleeps.append(seconds))

    recovery.install_startup_database_recovery(postbind)
    with pytest.raises(OperationalError, match="password authentication failed"):
        postbind.bootstrap_permanent_runtime_schema()

    assert store.engine.dispose_calls == 1
    assert sleeps == []


def test_recovery_retry_budget_remains_bounded_and_fail_closed(monkeypatch):
    store = _Store("store")
    clock = iter([0.0, recovery.STARTUP_DATABASE_RECOVERY_DEADLINE_SECONDS + 1.0])
    sleeps: list[float] = []

    def build_services(_settings, _store):
        raise _operational_error("FATAL: the database system is starting up")

    postbind = _postbind(
        build_store=lambda _path: store,
        build_services=build_services,
        ensure_cache=lambda _store: None,
    )
    monkeypatch.setattr(recovery.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(recovery.time, "sleep", lambda seconds: sleeps.append(seconds))

    recovery.install_startup_database_recovery(postbind)
    with pytest.raises(OperationalError, match="database system is starting up"):
        postbind.bootstrap_permanent_runtime_schema()

    assert store.engine.dispose_calls == 1
    assert sleeps == []


def test_production_entrypoint_installs_recovery_before_starting_guards():
    source = inspect.getsource(production_entrypoint.main)

    assert "install_startup_database_recovery(base.base)" in source
    assert source.index("install_startup_database_recovery(base.base)") < source.index(
        "history_projection_guard.start()"
    )
    assert recovery.STARTUP_DATABASE_RECOVERY_DEADLINE_SECONDS == 300.0
    assert recovery.STARTUP_DATABASE_RECOVERY_RETRY_SECONDS == 5.0
