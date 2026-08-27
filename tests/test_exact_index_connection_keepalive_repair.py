from __future__ import annotations

from types import SimpleNamespace

from inefficiency_engine import cycle_history_exact_index_direct as direct_index
from inefficiency_engine import cycle_history_index_gate as index_gate
from inefficiency_engine import cycle_history_index_maintenance_child as index_child
from inefficiency_engine import cycle_history_index_runtime_store as runtime_store
from inefficiency_engine import runtime_index_maintenance


def test_postgres_exact_index_store_applies_dedicated_tcp_keepalives(monkeypatch) -> None:
    captured: dict[str, object] = {}
    fake_engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

    monkeypatch.setattr(
        runtime_store,
        "evidence_location_from_env",
        lambda _fallback: "postgresql://example.invalid/db",
    )
    monkeypatch.setattr(
        runtime_store,
        "_database_url",
        lambda _location: "postgresql+psycopg://example.invalid/db",
    )

    def fake_create_engine(url: str, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return fake_engine

    monkeypatch.setattr(runtime_store, "create_engine", fake_create_engine)

    store = runtime_store.build_cycle_history_index_runtime_store(None)
    assert store is not None
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    connect_args = kwargs["connect_args"]
    assert connect_args["connect_timeout"] == 8
    assert connect_args["keepalives"] == 1
    assert connect_args["keepalives_idle"] == 30
    assert connect_args["keepalives_interval"] == 10
    assert connect_args["keepalives_count"] == 3
    assert kwargs["poolclass"] is runtime_store.NullPool

    detail = store.connection_resilience_detail()
    assert detail == {
        "exact_index_tcp_keepalives_enabled": True,
        "exact_index_tcp_keepalives_idle_seconds": 30,
        "exact_index_tcp_keepalives_interval_seconds": 10,
        "exact_index_tcp_keepalives_count": 3,
        "exact_index_connection_resilience_scope": "dedicated_exact_index_only",
    }


def test_non_postgres_exact_index_store_does_not_apply_tcp_keepalive_args(monkeypatch) -> None:
    captured: dict[str, object] = {}
    fake_engine = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))

    monkeypatch.setattr(
        runtime_store,
        "evidence_location_from_env",
        lambda _fallback: "/tmp/exact-index-test.sqlite",
    )
    monkeypatch.setattr(
        runtime_store,
        "_database_url",
        lambda _location: "sqlite:////tmp/exact-index-test.sqlite",
    )

    def fake_create_engine(url: str, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return fake_engine

    monkeypatch.setattr(runtime_store, "create_engine", fake_create_engine)

    store = runtime_store.build_cycle_history_index_runtime_store(None)
    assert store is not None
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    connect_args = kwargs["connect_args"]
    assert connect_args == {"check_same_thread": False}
    assert "keepalives" not in connect_args
    assert store.connection_resilience_detail()["exact_index_tcp_keepalives_enabled"] is False


def test_exact_index_status_surfaces_keepalive_telemetry_without_authority(monkeypatch) -> None:
    class Connection:
        def __enter__(self):
            return object()

        def __exit__(self, *_args):
            return False

    store = SimpleNamespace(
        engine=SimpleNamespace(
            dialect=SimpleNamespace(name="postgresql"),
            connect=lambda: Connection(),
        ),
        connection_resilience_detail=lambda: {
            "exact_index_tcp_keepalives_enabled": True,
            "exact_index_tcp_keepalives_idle_seconds": 30,
            "exact_index_tcp_keepalives_interval_seconds": 10,
            "exact_index_tcp_keepalives_count": 3,
            "exact_index_connection_resilience_scope": "dedicated_exact_index_only",
        },
    )
    monkeypatch.setattr(
        index_gate,
        "_postgres_index_state",
        lambda _db, *, index_name: {"valid": True, "ready": True},
    )
    monkeypatch.setattr(index_gate, "_postgres_index_is_usable", lambda _state: True)

    status = index_gate.cycle_history_exact_index_status(store)

    assert status["ready"] is True
    assert status["exact_index_tcp_keepalives_enabled"] is True
    assert status["exact_index_tcp_keepalives_idle_seconds"] == 30
    assert status["exact_index_tcp_keepalives_interval_seconds"] == 10
    assert status["exact_index_tcp_keepalives_count"] == 3
    assert status["exact_index_connection_resilience_scope"] == "dedicated_exact_index_only"
    assert status["allocation_authority"] is False
    assert status["live_execution_authority"] is False
    assert status["paper_only"] is True


def test_keepalive_repair_preserves_exact_index_deadlines_and_lock_policy() -> None:
    assert runtime_store.EXACT_INDEX_CONNECT_TIMEOUT_SECONDS == 8
    assert runtime_store.EXACT_INDEX_HEARTBEAT_STATEMENT_TIMEOUT_MS == 8_000
    assert runtime_store.EXACT_INDEX_HEARTBEAT_LOCK_TIMEOUT_MS == 3_000
    assert index_child.DEDICATED_CYCLE_HISTORY_INDEX_STATEMENT_TIMEOUT_MS == 3_600_000
    assert direct_index.EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS == 8_000
    assert runtime_index_maintenance.POSTGRES_INDEX_LOCK_TIMEOUT_MS == 5_000

    assert runtime_store.EXACT_INDEX_TCP_KEEPALIVES_ENABLED == 1
    assert runtime_store.EXACT_INDEX_TCP_KEEPALIVES_IDLE_SECONDS == 30
    assert runtime_store.EXACT_INDEX_TCP_KEEPALIVES_INTERVAL_SECONDS == 10
    assert runtime_store.EXACT_INDEX_TCP_KEEPALIVES_COUNT == 3
