from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import event as sqlalchemy_event, select

from inefficiency_engine import control_cycle_executor
from inefficiency_engine import cycle_history_bucket_timeout_runtime as runtime
from inefficiency_engine import durable_control_cycle_history as legacy_cycle_history
from inefficiency_engine import durable_control_cycle_history_target_runtime as target_cycle_history
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind, MarketQuote


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


def _quote(observed_at: datetime, *, mid: float) -> MarketQuote:
    return MarketQuote(
        venue="Coinbase",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        mid=mid,
        observed_at=observed_at,
        source="test",
    )


def _record_quote(store: EvidenceStore, quote: MarketQuote, *, completed_at: datetime) -> None:
    store.record_scan(
        funding_quotes=[],
        market_quotes=[quote],
        opportunities=[],
        providers=[],
        started_at=completed_at - timedelta(seconds=1),
        completed_at=completed_at,
    )


def test_index_aligned_bucket_seek_ranks_observed_at_before_id(tmp_path):
    store = EvidenceStore(tmp_path / "cycle-history-index-order.db")
    legacy_cycle_history.ensure_durable_control_cycle_history_schema(store)
    day_start = datetime(2026, 8, 23, tzinfo=timezone.utc)

    # Deliberately give the newer observation the lower source id. Ranking only by
    # id would select the older row and also prevent PostgreSQL from following the
    # required (venue, asset, observed_at, id) index order.
    _record_quote(
        store,
        _quote(day_start + timedelta(hours=11), mid=111.0),
        completed_at=day_start + timedelta(hours=11, minutes=1),
    )
    _record_quote(
        store,
        _quote(day_start + timedelta(hours=10), mid=110.0),
        completed_at=day_start + timedelta(hours=11, minutes=2),
    )

    statements: list[str] = []

    def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
        if "market_quotes" in statement and statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement)

    sqlalchemy_event.listen(store.engine, "before_cursor_execute", capture)
    try:
        retained = runtime._index_aligned_replace_bucket(
            factory=SimpleNamespace(store=store),
            namespace="cycle-history-index-order-test",
            venue="Coinbase",
            asset="BTC",
            day=day_start.date(),
            start=day_start,
            end=day_start + timedelta(hours=12),
            limit=1,
        )
    finally:
        sqlalchemy_event.remove(store.engine, "before_cursor_execute", capture)

    assert retained == 1
    candidate_queries = [
        statement.lower()
        for statement in statements
        if "order by" in statement.lower() and "observed_at" in statement.lower()
    ]
    assert candidate_queries
    assert any(
        "order by market_quotes.observed_at desc, market_quotes.id desc" in statement
        for statement in candidate_queries
    )

    with store.engine.connect() as db:
        payload = db.execute(
            select(legacy_cycle_history.CONTROL_CYCLE_HISTORY_ROWS.c.payload_json)
            .where(
                legacy_cycle_history.CONTROL_CYCLE_HISTORY_ROWS.c.namespace
                == "cycle-history-index-order-test"
            )
        ).scalar_one()
    selected = MarketQuote.model_validate_json(payload)
    assert selected.mid == 111.0
    assert selected.observed_at == day_start + timedelta(hours=11)


def test_index_aligned_bucket_runtime_patches_legacy_and_frozen_target(monkeypatch):
    monkeypatch.setattr(legacy_cycle_history, "_replace_bucket", lambda **_kwargs: -1)
    monkeypatch.setattr(target_cycle_history, "_replace_bucket", lambda **_kwargs: -2)

    runtime.install_index_aligned_cycle_history_bucket_runtime()

    assert legacy_cycle_history._replace_bucket is runtime._index_aligned_replace_bucket
    assert target_cycle_history._replace_bucket is runtime._index_aligned_replace_bucket
