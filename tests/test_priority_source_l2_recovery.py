from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine import priority_source_collection as source_module
from inefficiency_engine.priority_source_collection import PrioritySourceCollectionService


class _Store:
    def __init__(self, heartbeat=None):
        self.heartbeat = heartbeat

    def latest_worker_heartbeat(self, worker_id):
        assert worker_id == source_module.ALPHA_L2_WORKER_ID
        return self.heartbeat


class _Factory:
    def __init__(self):
        self.calls = 0

    async def refresh_l2_source_snapshot(self):
        self.calls += 1
        return SimpleNamespace(order_books=[object(), object()])


def _service(heartbeat=None):
    service = PrioritySourceCollectionService.__new__(PrioritySourceCollectionService)
    service.store = _Store(heartbeat)
    service.alpha_factory = _Factory()
    service.memory_soft_limit_mb = 2048.0
    return service


@pytest.mark.asyncio
async def test_l2_source_refresh_runs_when_sampler_is_unobserved(monkeypatch):
    monkeypatch.setattr(source_module, "memory_budget_exceeded", lambda _limit: False)
    service = _service()

    result = await service._refresh_l2_source_if_due()

    assert result["state"] == "refreshed"
    assert result["attempted"] is True
    assert result["retained_book_count"] == 2
    assert service.alpha_factory.calls == 1


@pytest.mark.asyncio
async def test_l2_source_refresh_does_not_duplicate_fresh_sampler(monkeypatch):
    monkeypatch.setattr(source_module, "memory_budget_exceeded", lambda _limit: False)
    heartbeat = SimpleNamespace(observed_at=datetime.now(timezone.utc) - timedelta(seconds=30))
    service = _service(heartbeat)

    result = await service._refresh_l2_source_if_due()

    assert result == {"state": "fresh_cached", "attempted": False}
    assert service.alpha_factory.calls == 0
