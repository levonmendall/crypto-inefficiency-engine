from __future__ import annotations

import asyncio

from inefficiency_engine.allocation_certification import AllocationForwardCertificationService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.operating_certification import OperatingCertificationService
from inefficiency_engine.operating_worker import (
    ALLOCATION_CERTIFICATION_TIMEOUT_SECONDS,
    OPERATING_CERTIFICATION_TIMEOUT_SECONDS,
)
from inefficiency_engine.service import OpportunityService


CERTIFICATION_WORKER_ID = "mechanism-certification-loop"


async def _interruptible_wait(seconds: float, stop_event: asyncio.Event) -> None:
    if seconds <= 0 or stop_event.is_set():
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except TimeoutError:
        return


async def run_certification_loop(
    service: OpportunityService,
    store: EvidenceStore,
    *,
    allocation_certification: AllocationForwardCertificationService,
    operating_certification: OperatingCertificationService,
    stop_event: asyncio.Event,
    interval_seconds: float | None = None,
    max_cycles: int | None = None,
) -> int:
    """Run forward/mechanism certification independently of canonical accounting.

    Certification remains fully fail-closed and evidence-driven. Its provider work
    is allowed to degrade or stall without making the canonical paper-account
    service itself fail. This preserves certification while protecting portfolio
    liveness from non-accounting provider paths.
    """

    interval = (
        max(60.0, float(interval_seconds))
        if interval_seconds is not None
        else max(60.0, service.settings.shadow_cycle_interval_seconds * 10.0)
    )
    attempted = 0
    while not stop_event.is_set() and (max_cycles is None or attempted < max_cycles):
        attempted += 1
        latest = store.latest_worker_heartbeat(CERTIFICATION_WORKER_ID)
        del latest
        store.record_worker_heartbeat(
            worker_id=CERTIFICATION_WORKER_ID,
            state="running",
            detail={
                "cycle_attempt": attempted,
                "stage": "allocation_certification",
                "canonical_accounting_independent": True,
                "paper_only": True,
            },
        )

        errors: dict[str, str] = {}
        allocation_cycle = None
        operating_cycle = None
        nav_heartbeat = store.latest_worker_heartbeat("canonical-portfolio-operating-loop")
        nav = None
        if nav_heartbeat is not None:
            value = nav_heartbeat.detail.get("portfolio_nav_usd")
            if isinstance(value, (float, int)) and value > 0:
                nav = float(value)
        total_capital_usd = nav or 250000.0

        try:
            allocation_cycle = await asyncio.wait_for(
                allocation_certification.run_cycle(total_capital_usd=total_capital_usd),
                timeout=ALLOCATION_CERTIFICATION_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            errors["allocation_certification_error_type"] = type(exc).__name__

        store.record_worker_heartbeat(
            worker_id=CERTIFICATION_WORKER_ID,
            state="running",
            detail={
                "cycle_attempt": attempted,
                "stage": "operating_certification",
                "canonical_accounting_independent": True,
                "allocation_certification_cycle_id": getattr(allocation_cycle, "cycle_id", None),
                **errors,
                "paper_only": True,
            },
        )
        try:
            operating_cycle = await asyncio.wait_for(
                operating_certification.run_cycle(total_capital_usd=total_capital_usd),
                timeout=OPERATING_CERTIFICATION_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            errors["operating_certification_error_type"] = type(exc).__name__

        state = "degraded" if errors else "success"
        error_type = (
            errors.get("allocation_certification_error_type")
            or errors.get("operating_certification_error_type")
        )
        store.record_worker_heartbeat(
            worker_id=CERTIFICATION_WORKER_ID,
            state=state,
            cycle_id=getattr(operating_cycle, "cycle_id", None),
            error_type=error_type,
            detail={
                "cycle_attempt": attempted,
                "canonical_accounting_independent": True,
                "allocation_certification_cycle_id": getattr(allocation_cycle, "cycle_id", None),
                "operating_certification_cycle_id": getattr(operating_cycle, "cycle_id", None),
                **errors,
                "paper_only": True,
            },
        )

        if not stop_event.is_set() and (max_cycles is None or attempted < max_cycles):
            await _interruptible_wait(interval, stop_event)

    store.record_worker_heartbeat(
        worker_id=CERTIFICATION_WORKER_ID,
        state="stopped" if stop_event.is_set() else "completed",
        detail={"cycles_attempted": attempted, "paper_only": True},
    )
    return attempted
