from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from inefficiency_engine.certification_worker import CERTIFICATION_WORKER_ID, run_certification_loop


@pytest.mark.asyncio
async def test_certification_errors_are_recorded_without_raising_from_loop():
    heartbeats: list[dict[str, object]] = []

    class FakeStore:
        def latest_worker_heartbeat(self, worker_id):
            return None

        def record_worker_heartbeat(self, **payload):
            heartbeats.append(payload)

    class FailingAllocationCertification:
        async def run_cycle(self, *, total_capital_usd):
            raise RuntimeError("provider failure")

    class FailingOperatingCertification:
        async def run_cycle(self, *, total_capital_usd):
            raise RuntimeError("diagnostic failure")

    stop = asyncio.Event()
    service = SimpleNamespace(settings=SimpleNamespace(shadow_cycle_interval_seconds=30.0))
    attempted = await run_certification_loop(
        service,  # type: ignore[arg-type]
        FakeStore(),  # type: ignore[arg-type]
        allocation_certification=FailingAllocationCertification(),  # type: ignore[arg-type]
        operating_certification=FailingOperatingCertification(),  # type: ignore[arg-type]
        stop_event=stop,
        interval_seconds=60.0,
        max_cycles=1,
    )

    assert attempted == 1
    rows = [row for row in heartbeats if row["worker_id"] == CERTIFICATION_WORKER_ID]
    assert rows[-2]["state"] == "degraded"
    assert rows[-2]["error_type"] == "RuntimeError"
    assert rows[-2]["detail"]["canonical_accounting_independent"] is True
