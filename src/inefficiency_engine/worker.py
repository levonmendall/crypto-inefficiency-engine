from __future__ import annotations

import asyncio
import os
import signal
import socket
import uuid
from dataclasses import dataclass
from typing import Awaitable, Callable

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.service import OpportunityService


SleepFn = Callable[[float], Awaitable[None]]
RouteShadowRunner = Callable[[], Awaitable[object]]


@dataclass(frozen=True)
class WorkerRunStats:
    worker_id: str
    cycles_attempted: int
    cycles_succeeded: int
    cycles_failed: int


def default_worker_id() -> str:
    return os.getenv("CIE_WORKER_ID") or os.getenv("RENDER_INSTANCE_ID") or os.getenv("HOSTNAME") or socket.gethostname() or uuid.uuid4().hex


async def _interruptible_sleep(seconds: float, stop_event: asyncio.Event, sleep: SleepFn) -> None:
    if seconds <= 0:
        return
    sleeper = asyncio.create_task(sleep(seconds))
    stopper = asyncio.create_task(stop_event.wait())
    done, pending = await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        if task is sleeper:
            task.result()


async def run_shadow_worker(
    service: OpportunityService,
    store: EvidenceStore,
    *,
    worker_id: str | None = None,
    stop_event: asyncio.Event | None = None,
    sleep: SleepFn = asyncio.sleep,
    max_cycles: int | None = None,
    route_shadow_runner: RouteShadowRunner | None = None,
) -> WorkerRunStats:
    worker_id = worker_id or default_worker_id()
    stop_event = stop_event or asyncio.Event()
    attempted = succeeded = failed = 0
    store.record_worker_heartbeat(worker_id=worker_id, state="starting", detail={"paper_only": True, "backend": store.backend})
    while not stop_event.is_set() and (max_cycles is None or attempted < max_cycles):
        attempted += 1
        store.record_worker_heartbeat(worker_id=worker_id, state="running", detail={"cycle_attempt": attempted})

        if route_shadow_runner is None:
            core_result = await asyncio.gather(service.run_shadow_cycle(), return_exceptions=True)
            core_cycle = core_result[0]
            route_cycle = None
        else:
            core_cycle, route_cycle = await asyncio.gather(
                service.run_shadow_cycle(),
                route_shadow_runner(),
                return_exceptions=True,
            )

        if isinstance(core_cycle, BaseException):
            failed += 1
            detail = {"cycle_attempt": attempted, "message": str(core_cycle)[:500]}
            if isinstance(route_cycle, BaseException):
                detail["dex_route_shadow_error_type"] = type(route_cycle).__name__
            elif route_cycle is not None:
                detail["dex_route_shadow_completed"] = True
            store.record_worker_heartbeat(
                worker_id=worker_id,
                state="error",
                error_type=type(core_cycle).__name__,
                detail=detail,
            )
            if max_cycles is None or attempted < max_cycles:
                await _interruptible_sleep(service.settings.worker_error_backoff_seconds, stop_event, sleep)
            continue

        succeeded += 1
        survived = sum(1 for obs in core_cycle.observations if obs.survived)
        detail: dict[str, object] = {
            "cycle_attempt": attempted,
            "observation_count": len(core_cycle.observations),
            "survived_count": survived,
        }
        if isinstance(route_cycle, BaseException):
            # DEX route evidence is a separate family and cannot poison a valid
            # core CEX shadow cycle.
            detail["dex_route_shadow_error_type"] = type(route_cycle).__name__
        elif route_cycle is not None:
            route_observations = getattr(route_cycle, "observations", [])
            detail["dex_route_shadow_cycle_id"] = getattr(route_cycle, "cycle_id", None)
            detail["dex_route_shadow_observation_count"] = len(route_observations)
            detail["dex_route_shadow_survived_count"] = sum(
                1 for obs in route_observations if getattr(obs, "survived", False)
            )

        store.record_worker_heartbeat(
            worker_id=worker_id,
            state="success",
            cycle_id=core_cycle.cycle_id,
            scan_id=core_cycle.verification_scan_id,
            detail=detail,
        )
        if not stop_event.is_set() and (max_cycles is None or attempted < max_cycles):
            await _interruptible_sleep(service.settings.shadow_cycle_interval_seconds, stop_event, sleep)
    final_state = "stopped" if stop_event.is_set() else "completed"
    store.record_worker_heartbeat(
        worker_id=worker_id,
        state=final_state,
        detail={"cycles_attempted": attempted, "cycles_succeeded": succeeded, "cycles_failed": failed},
    )
    return WorkerRunStats(worker_id, attempted, succeeded, failed)


async def run_forever(service: OpportunityService, store: EvidenceStore) -> WorkerRunStats:
    from inefficiency_engine.universal_service import UniversalOpportunityService

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass
    universal = UniversalOpportunityService(service)
    return await run_shadow_worker(
        service,
        store,
        stop_event=stop_event,
        route_shadow_runner=universal.run_dex_route_shadow_cycle,
    )
