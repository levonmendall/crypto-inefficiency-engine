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
TierShadowRunner = Callable[[], Awaitable[object]]
CompositeShadowRunner = Callable[[], Awaitable[object]]
FrontierRunner = Callable[[], Awaitable[list[object]]]


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


def _staggered_due(attempted: int, every_cycles: int) -> bool:
    every_cycles = max(1, every_cycles)
    offset = max(1, every_cycles // 2)
    return (attempted - offset) % every_cycles == 0


async def run_shadow_worker(
    service: OpportunityService,
    store: EvidenceStore,
    *,
    worker_id: str | None = None,
    stop_event: asyncio.Event | None = None,
    sleep: SleepFn = asyncio.sleep,
    max_cycles: int | None = None,
    route_shadow_runner: RouteShadowRunner | None = None,
    tier_shadow_runner: TierShadowRunner | None = None,
    tier_shadow_every_cycles: int = 10,
    composite_shadow_runner: CompositeShadowRunner | None = None,
    composite_shadow_every_cycles: int = 10,
    frontier_runner: FrontierRunner | None = None,
    frontier_every_cycles: int = 10,
) -> WorkerRunStats:
    worker_id = worker_id or default_worker_id()
    stop_event = stop_event or asyncio.Event()
    attempted = succeeded = failed = 0
    frontier_every_cycles = max(1, frontier_every_cycles)
    tier_shadow_every_cycles = max(1, tier_shadow_every_cycles)
    composite_shadow_every_cycles = max(1, composite_shadow_every_cycles)
    store.record_worker_heartbeat(worker_id=worker_id, state="starting", detail={"paper_only": True, "backend": store.backend})
    while not stop_event.is_set() and (max_cycles is None or attempted < max_cycles):
        attempted += 1
        run_frontier = frontier_runner is not None and attempted % frontier_every_cycles == 0
        run_tier_shadow = tier_shadow_runner is not None and attempted % tier_shadow_every_cycles == 0
        run_composite_shadow = composite_shadow_runner is not None and _staggered_due(
            attempted, composite_shadow_every_cycles
        )
        store.record_worker_heartbeat(
            worker_id=worker_id,
            state="running",
            detail={
                "cycle_attempt": attempted,
                "dex_frontier_probe_due": run_frontier,
                "dex_tier_shadow_due": run_tier_shadow,
                "cex_dex_composite_shadow_due": run_composite_shadow,
            },
        )

        tasks: list[Awaitable[object]] = [service.run_shadow_cycle()]
        route_index = None
        tier_index = None
        composite_index = None
        frontier_index = None
        if route_shadow_runner is not None:
            route_index = len(tasks)
            tasks.append(route_shadow_runner())
        if run_tier_shadow and tier_shadow_runner is not None:
            tier_index = len(tasks)
            tasks.append(tier_shadow_runner())
        if run_composite_shadow and composite_shadow_runner is not None:
            composite_index = len(tasks)
            tasks.append(composite_shadow_runner())
        if run_frontier and frontier_runner is not None:
            frontier_index = len(tasks)
            tasks.append(frontier_runner())
        results = await asyncio.gather(*tasks, return_exceptions=True)
        core_cycle = results[0]
        route_cycle = results[route_index] if route_index is not None else None
        tier_cycle = results[tier_index] if tier_index is not None else None
        composite_cycle = results[composite_index] if composite_index is not None else None
        frontier_result = results[frontier_index] if frontier_index is not None else None

        if isinstance(core_cycle, BaseException):
            failed += 1
            detail: dict[str, object] = {"cycle_attempt": attempted, "message": str(core_cycle)[:500]}
            if isinstance(route_cycle, BaseException):
                detail["dex_route_shadow_error_type"] = type(route_cycle).__name__
            elif route_cycle is not None:
                detail["dex_route_shadow_completed"] = True
            if isinstance(tier_cycle, BaseException):
                detail["dex_tier_shadow_error_type"] = type(tier_cycle).__name__
            elif tier_cycle is not None:
                detail["dex_tier_shadow_completed"] = True
            if isinstance(composite_cycle, BaseException):
                detail["cex_dex_composite_shadow_error_type"] = type(composite_cycle).__name__
            elif composite_cycle is not None:
                detail["cex_dex_composite_shadow_completed"] = True
            if isinstance(frontier_result, BaseException):
                detail["dex_route_frontier_error_type"] = type(frontier_result).__name__
            elif frontier_result is not None:
                detail["dex_route_frontier_count"] = len(frontier_result)
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
        detail = {
            "cycle_attempt": attempted,
            "observation_count": len(core_cycle.observations),
            "survived_count": survived,
        }
        if isinstance(route_cycle, BaseException):
            detail["dex_route_shadow_error_type"] = type(route_cycle).__name__
        elif route_cycle is not None:
            route_observations = getattr(route_cycle, "observations", [])
            detail["dex_route_shadow_cycle_id"] = getattr(route_cycle, "cycle_id", None)
            detail["dex_route_shadow_observation_count"] = len(route_observations)
            detail["dex_route_shadow_survived_count"] = sum(
                1 for obs in route_observations if getattr(obs, "survived", False)
            )
        if isinstance(tier_cycle, BaseException):
            detail["dex_tier_shadow_error_type"] = type(tier_cycle).__name__
        elif tier_cycle is not None:
            tier_observations = getattr(tier_cycle, "observations", [])
            detail["dex_tier_shadow_cycle_id"] = getattr(tier_cycle, "cycle_id", None)
            detail["dex_tier_shadow_initial_quote_count"] = getattr(tier_cycle, "initial_quote_count", 0)
            detail["dex_tier_shadow_observation_count"] = len(tier_observations)
            detail["dex_tier_shadow_survived_count"] = sum(
                1 for obs in tier_observations if getattr(obs, "survived", False)
            )
        if isinstance(composite_cycle, BaseException):
            detail["cex_dex_composite_shadow_error_type"] = type(composite_cycle).__name__
        elif composite_cycle is not None:
            composite_observations = getattr(composite_cycle, "observations", [])
            detail["cex_dex_composite_shadow_cycle_id"] = getattr(composite_cycle, "cycle_id", None)
            detail["cex_dex_composite_initial_evidence_count"] = getattr(
                composite_cycle, "initial_evidence_count", 0
            )
            detail["cex_dex_composite_observation_count"] = len(composite_observations)
            detail["cex_dex_composite_matched_count"] = sum(
                1 for obs in composite_observations if getattr(obs, "survived", False)
            )
            detail["cex_dex_composite_hurdle_survived_count"] = sum(
                1 for obs in composite_observations if getattr(obs, "hurdle_survived", False)
            )
        if isinstance(frontier_result, BaseException):
            detail["dex_route_frontier_error_type"] = type(frontier_result).__name__
        elif frontier_result is not None:
            detail["dex_route_frontier_count"] = len(frontier_result)
            detail["dex_route_frontier_capacity_claimed"] = False

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
    from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
    from inefficiency_engine.cex_dex_shadow import CexDexCompositeEdgeShadowService
    from inefficiency_engine.dex_tier_shadow import DexTierShadowService
    from inefficiency_engine.universal_service import UniversalOpportunityService

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass
    universal = UniversalOpportunityService(service)
    tier_shadow = DexTierShadowService(service, evidence_store=store)
    composite_shadow = CexDexCompositeEdgeShadowService(
        CexDexCompositeEvidenceService(service, universal=universal),
        evidence_store=store,
    )
    return await run_shadow_worker(
        service,
        store,
        stop_event=stop_event,
        route_shadow_runner=universal.run_dex_route_shadow_cycle,
        tier_shadow_runner=tier_shadow.run_cycle,
        tier_shadow_every_cycles=service.settings.dex_route_tier_shadow_every_cycles,
        composite_shadow_runner=composite_shadow.run_cycle,
        composite_shadow_every_cycles=10,
        frontier_runner=universal.probe_dex_route_size_frontiers,
        frontier_every_cycles=service.settings.dex_route_frontier_every_cycles,
    )
