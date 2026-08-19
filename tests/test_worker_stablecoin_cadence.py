from datetime import datetime, timezone

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import ShadowCycle
from inefficiency_engine.worker import run_shadow_worker


class FakeCore:
    def __init__(self):
        self.settings = Settings(shadow_cycle_interval_seconds=0.0, worker_error_backoff_seconds=0.0)

    async def run_shadow_cycle(self):
        now = datetime.now(timezone.utc)
        return ShadowCycle(
            cycle_id=f"core-{now.timestamp()}",
            started_at=now,
            completed_at=now,
            delay_seconds=0.0,
            initial_scan_id="initial",
            verification_scan_id="verification",
            observations=[],
        )


async def no_sleep(_: float) -> None:
    return None


@pytest.mark.asyncio
async def test_stablecoin_shadow_runs_on_quarter_stagger_not_frontier_cadence(tmp_path):
    store = EvidenceStore(tmp_path / "worker.sqlite3")
    calls: list[int] = []

    async def stablecoin_runner():
        calls.append(1)
        return type("StableCycle", (), {"cycle_id": "stable", "initial_quote_count": 1, "observations": []})()

    stats = await run_shadow_worker(
        FakeCore(),  # type: ignore[arg-type]
        store,
        worker_id="stablecoin-cadence",
        sleep=no_sleep,
        max_cycles=3,
        stablecoin_shadow_runner=stablecoin_runner,
        stablecoin_shadow_every_cycles=10,
    )

    assert stats.cycles_succeeded == 3
    assert len(calls) == 1
    heartbeats = store.latest_worker_heartbeat("stablecoin-cadence")
    assert heartbeats is not None
    assert heartbeats.state == "completed"
