from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace

from sqlalchemy.exc import OperationalError

from inefficiency_engine import lightweight_portfolio_worker


def _recovery_error() -> OperationalError:
    return OperationalError(
        "SELECT 1",
        {},
        RuntimeError(
            "FATAL: the database system is not yet accepting connections; "
            "Consistent recovery state has not been yet reached."
        ),
    )


def test_permanent_portfolio_process_uses_durable_bridge_without_alpha_factory():
    source = inspect.getsource(lightweight_portfolio_worker)

    assert "ExpandedAlphaFactoryService" not in source
    assert "DisposableExpandedAlphaFactoryService" not in source
    assert "_DurableQualifiedStateHandle" in source
    assert "CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService" in source
    assert "CexDexUniversalOperationallyResilientPaperPortfolioService" in source


def test_permanent_portfolio_process_contains_no_external_source_acquisition():
    source = inspect.getsource(lightweight_portfolio_worker)

    assert "PermanentSourcePlane" not in source
    assert "resolve_top_volume_assets" not in source
    assert "DynamicVolumePublicAdapterRegistry" not in source
    assert "_permanent_source_refresh_loop" not in source
    assert "_volume_universe_refresh_loop" not in source
    assert "provider_calls\": False" in source


def test_bootstrap_retries_render_postgres_recovery_without_weakening_fail_closed(monkeypatch):
    recovered_store = SimpleNamespace(heartbeats=[])

    def record_worker_heartbeat(**kwargs):
        recovered_store.heartbeats.append(kwargs)

    recovered_store.record_worker_heartbeat = record_worker_heartbeat
    calls = {"count": 0}

    def build_store(_path):
        calls["count"] += 1
        if calls["count"] == 1:
            raise _recovery_error()
        return recovered_store

    monkeypatch.setattr(lightweight_portfolio_worker, "build_evidence_store", build_store)
    monkeypatch.setattr(lightweight_portfolio_worker, "operational_recovery_delay_seconds", lambda _attempt: 0.0)
    monkeypatch.setattr(lightweight_portfolio_worker.time, "sleep", lambda _delay: None)

    store = lightweight_portfolio_worker._build_evidence_store_with_retry(
        SimpleNamespace(evidence_db_path="postgresql://test")
    )

    assert store is recovered_store
    assert calls["count"] == 2
    heartbeat = recovered_store.heartbeats[-1]
    assert heartbeat["worker_id"] == "canonical-portfolio-db-recovery"
    assert heartbeat["detail"]["database_recovered"] is True
    assert heartbeat["detail"]["qualification_thresholds_unchanged"] is True
    assert heartbeat["detail"]["allocation_authority"] is False
    assert heartbeat["detail"]["paper_only"] is True


def test_runtime_waits_for_database_recovery_instead_of_exiting(monkeypatch):
    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def exec_driver_sql(self, statement):
            assert statement == "SELECT 1"

    class Engine:
        def __init__(self):
            self.connect_calls = 0
            self.dispose_calls = 0

        def dispose(self):
            self.dispose_calls += 1

        def connect(self):
            self.connect_calls += 1
            if self.connect_calls == 1:
                raise _recovery_error()
            return Connection()

    class Store:
        def __init__(self):
            self.engine = Engine()
            self.heartbeats = []

        def record_worker_heartbeat(self, **kwargs):
            self.heartbeats.append(kwargs)

    store = Store()
    monkeypatch.setattr(lightweight_portfolio_worker, "operational_recovery_delay_seconds", lambda _attempt: 0.0)

    recovered = asyncio.run(
        lightweight_portfolio_worker._wait_for_database_recovery(
            store,
            stop_event=asyncio.Event(),
            initial_error=_recovery_error(),
        )
    )

    assert recovered is True
    assert store.engine.connect_calls == 2
    assert store.engine.dispose_calls == 2
    assert store.heartbeats[-1]["detail"]["stage"] == "runtime_reconnected"
    assert store.heartbeats[-1]["detail"]["allocation_authority"] is False
