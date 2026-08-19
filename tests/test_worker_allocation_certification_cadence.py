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
async def test_allocation_certification_runs_on_independent_third_stagger(tmp_path):
    store = EvidenceStore(tmp_path / "worker.sqlite3")
    calls: list[int] = []

    async def allocation_runner():
        calls.append(1)
        return type(
            "AllocationCycle",
            (),
            {
                "cycle_id": "allocation-cycle",
                "trials_recorded": 1,
                "supported_trials_recorded": 1,
                "outcomes_matured": 0,
            },
        )()

    stats = await run_shadow_worker(
        FakeCore(),  # type: ignore[arg-type]
        store,
        worker_id="allocation-cadence",
        sleep=no_sleep,
        max_cycles=3,
        allocation_certification_runner=allocation_runner,
        allocation_certification_every_cycles=10,
    )

    assert stats.cycles_succeeded == 3
    assert len(calls) == 1
    latest = store.latest_worker_heartbeat("allocation-cadence")
    assert latest is not None
    assert latest.state == "completed"
