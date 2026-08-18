from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, build_evidence_store
from inefficiency_engine.models import ShadowCycle
from inefficiency_engine.worker import run_shadow_worker


class FakeWorkerService:
    def __init__(self):
        self.settings = Settings(
            worker_error_backoff_seconds=0.0,
            shadow_cycle_interval_seconds=0.0,
        )
        self.calls = 0

    async def run_shadow_cycle(self):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary provider failure")
        now = datetime.now(timezone.utc)
        return ShadowCycle(
            cycle_id="cycle-ok",
            started_at=now,
            completed_at=now,
            delay_seconds=0.0,
            initial_scan_id="initial",
            verification_scan_id="verification",
            observations=[],
        )


async def no_sleep(_: float) -> None:
    return None


def test_sqlite_url_and_worker_heartbeat_health(tmp_path, monkeypatch):
    path = tmp_path / "durable.sqlite3"
    monkeypatch.setenv("CIE_DATABASE_URL", f"sqlite:///{path}")
    store = build_evidence_store(None)
    assert store is not None
    assert store.backend == "sqlite"
    assert store.ping() is True

    now = datetime.now(timezone.utc)
    store.record_worker_heartbeat(worker_id="worker-a", state="success", observed_at=now)
    health = store.worker_health(stale_after_seconds=60, now=now + timedelta(seconds=10))
    assert health["healthy"] is True
    stale = store.worker_health(stale_after_seconds=60, now=now + timedelta(seconds=61))
    assert stale["healthy"] is False


@pytest.mark.asyncio
async def test_worker_recovers_from_transient_error_and_keeps_collecting(tmp_path):
    store = EvidenceStore(tmp_path / "worker.sqlite3")
    service = FakeWorkerService()
    stats = await run_shadow_worker(
        service,  # type: ignore[arg-type]
        store,
        worker_id="worker-test",
        sleep=no_sleep,
        max_cycles=2,
    )

    assert stats.cycles_attempted == 2
    assert stats.cycles_failed == 1
    assert stats.cycles_succeeded == 1
    assert store.counts().worker_heartbeats >= 6
    latest = store.latest_worker_heartbeat("worker-test")
    assert latest is not None
    assert latest.state == "completed"
