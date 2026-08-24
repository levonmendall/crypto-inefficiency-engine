from __future__ import annotations

import asyncio

import pytest
from sqlalchemy.exc import OperationalError

from inefficiency_engine.portfolio_operational_recovery_runtime import (
    install_portfolio_operational_recovery_runtime,
    recycle_pool_after_operational_error,
)


class _Engine:
    def __init__(self):
        self.dispose_count = 0

    def dispose(self):
        self.dispose_count += 1


class _Store:
    def __init__(self):
        self.engine = _Engine()
        self.heartbeats = []

    def record_worker_heartbeat(self, **kwargs):
        self.heartbeats.append(kwargs)


def _operational_error() -> OperationalError:
    return OperationalError("SELECT 1", {}, RuntimeError("connection reset"))


def test_pool_recycle_is_limited_to_operational_errors():
    store = _Store()

    assert recycle_pool_after_operational_error(store, RuntimeError("other")) is False
    assert store.engine.dispose_count == 0

    assert recycle_pool_after_operational_error(store, _operational_error()) is True
    assert store.engine.dispose_count == 1


def test_wrapped_portfolio_cycle_recycles_pool_without_replaying_cycle():
    store = _Store()

    class Portfolio:
        def __init__(self):
            self.store = store
            self.calls = 0

        async def run_cycle(self):
            self.calls += 1
            raise _operational_error()

    install_portfolio_operational_recovery_runtime(Portfolio)
    portfolio = Portfolio()

    with pytest.raises(OperationalError):
        asyncio.run(portfolio.run_cycle())

    assert portfolio.calls == 1
    assert store.engine.dispose_count == 1
    assert len(store.heartbeats) == 1
    heartbeat = store.heartbeats[0]
    assert heartbeat["worker_id"] == "canonical-portfolio-db-recovery"
    assert heartbeat["state"] == "success"
    assert heartbeat["error_type"] == "OperationalError"
    assert heartbeat["detail"]["pool_recycled"] is True
    assert heartbeat["detail"]["cycle_replayed"] is False
    assert heartbeat["detail"]["qualification_thresholds_unchanged"] is True
    assert heartbeat["detail"]["paper_only"] is True
