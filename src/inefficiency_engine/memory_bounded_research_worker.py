from __future__ import annotations

import asyncio
import gc
from collections.abc import Awaitable, Callable

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.worker import WorkerRunStats


SleepFn = Callable[[float], Awaitable[None]]
Runner = Callable[[], Awaitable[object]]
FrontierRunner = Callable[[], Awaitable[list[object]]]


def _offset_due(attempted: int, every_cycles: int, offset: int) -> bool:
    every_cycles = max(1, every_cycles)
    offset = max(1, min(every_cycles, offset))
    return (attempted - offset) % every_cycles == 0


def _staggered_due(attempted: int, every_cycles: int) -> bool:
    return _offset_due(attempted, every_cycles, max(1, every_cycles // 2))


def _quarter_staggered_due(attempted: int, every_cycles: int) -> bool:
    return _offset_due(attempted, every_cycles, max(1, every_cycles // 4))


def _third_staggered_due(attempted: int, every_cycles: int) -> bool:
    return _offset_due(attempted, every_cycles, max(1, every_cycles // 3))


def _three_quarter_staggered_due(attempted: int, every_cycles: int) -> bool:
    return _offset_due(attempted, every_cycles, max(1, (3 * every_cycles) // 4))


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


async def _capture(runner: Runner) -> object | BaseException:
    try:
        return await runner()
    except BaseException as exc:
        return exc


def _release(value: object | BaseException | None) -> None:
    del value
    gc.collect()


def _route_summary(detail: dict[str, object], value: object | BaseException | None) -> None:
    if isinstance(value, BaseException):
        detail["dex_route_shadow_error_type"] = type(value).__name__
    elif value is not None:
        observations = getattr(value, "observations", [])
        detail["dex_route_shadow_cycle_id"] = getattr(value, "cycle_id", None)
        detail["dex_route_shadow_observation_count"] = len(observations)
        detail["dex_route_shadow_survived_count"] = sum(
            1 for obs in observations if getattr(obs, "survived", False)
        )


def _tier_summary(detail: dict[str, object], value: object | BaseException | None) -> None:
    if isinstance(value, BaseException):
        detail["dex_tier_shadow_error_type"] = type(value).__name__
    elif value is not None:
        observations = getattr(value, "observations", [])
        detail["dex_tier_shadow_cycle_id"] = getattr(value, "cycle_id", None)
        detail["dex_tier_shadow_initial_quote_count"] = getattr(value, "initial_quote_count", 0)
        detail["dex_tier_shadow_observation_count"] = len(observations)
        detail["dex_tier_shadow_survived_count"] = sum(
            1 for obs in observations if getattr(obs, "survived", False)
        )


def _composite_summary(detail: dict[str, object], value: object | BaseException | None) -> None:
    if isinstance(value, BaseException):
        detail["cex_dex_composite_shadow_error_type"] = type(value).__name__
    elif value is not None:
        observations = getattr(value, "observations", [])
        detail["cex_dex_composite_shadow_cycle_id"] = getattr(value, "cycle_id", None)
        detail["cex_dex_composite_initial_evidence_count"] = getattr(value, "initial_evidence_count", 0)
        detail["cex_dex_composite_observation_count"] = len(observations)
        detail["cex_dex_composite_matched_count"] = sum(
            1 for obs in observations if getattr(obs, "survived", False)
        )
        detail["cex_dex_composite_hurdle_survived_count"] = sum(
            1 for obs in observations if getattr(obs, "hurdle_survived", False)
        )


def _stablecoin_summary(detail: dict[str, object], value: object | BaseException | None) -> None:
    if isinstance(value, BaseException):
        detail["stablecoin_depth_shadow_error_type"] = type(value).__name__
    elif value is not None:
        observations = getattr(value, "observations", [])
        detail["stablecoin_depth_shadow_cycle_id"] = getattr(value, "cycle_id", None)
        detail["stablecoin_depth_initial_quote_count"] = getattr(value, "initial_quote_count", 0)
        detail["stablecoin_depth_observation_count"] = len(observations)
        detail["stablecoin_depth_survived_count"] = sum(
            1 for obs in observations if getattr(obs, "survived", False)
        )


def _allocation_summary(detail: dict[str, object], value: object | BaseException | None) -> None:
    if isinstance(value, BaseException):
        detail["allocation_forward_certification_error_type"] = type(value).__name__
    elif value is not None:
        detail["allocation_forward_certification_cycle_id"] = getattr(value, "cycle_id", None)
        detail["allocation_trials_recorded"] = getattr(value, "trials_recorded", 0)
        detail["allocation_supported_trials_recorded"] = getattr(value, "supported_trials_recorded", 0)
        detail["allocation_outcomes_matured"] = getattr(value, "outcomes_matured", 0)


def _alpha_summary(detail: dict[str, object], value: object | BaseException | None) -> None:
    if isinstance(value, BaseException):
        detail["alpha_forward_evidence_error_type"] = type(value).__name__
    elif value is not None:
        detail["alpha_forward_evidence_cycle_id"] = getattr(value, "cycle_id", None)
        detail["alpha_candidate_count"] = getattr(value, "candidate_count", 0)
        detail["alpha_signals_recorded"] = getattr(value, "signals_recorded", 0)
        detail["alpha_outcomes_matured"] = getattr(value, "outcomes_matured", 0)


def _frontier_summary(detail: dict[str, object], value: object | BaseException | None) -> None:
    if isinstance(value, BaseException):
        detail["dex_route_frontier_error_type"] = type(value).__name__
    elif value is not None:
        detail["dex_route_frontier_count"] = len(value)  # type: ignore[arg-type]
        detail["dex_route_frontier_capacity_claimed"] = False


async def _run_and_release(
    runner: Runner | None,
    detail: dict[str, object],
    summarizer: Callable[[dict[str, object], object | BaseException | None], None],
) -> None:
    if runner is None:
        return
    value = await _capture(runner)
    summarizer(detail, value)
    _release(value)


async def run_memory_bounded_research_worker(
    service: OpportunityService,
    store: EvidenceStore,
    *,
    worker_id: str,
    stop_event: asyncio.Event,
    sleep: SleepFn = asyncio.sleep,
    max_cycles: int | None = None,
    route_shadow_runner: Runner | None = None,
    tier_shadow_runner: Runner | None = None,
    tier_shadow_every_cycles: int = 10,
    composite_shadow_runner: Runner | None = None,
    composite_shadow_every_cycles: int = 10,
    stablecoin_shadow_runner: Runner | None = None,
    stablecoin_shadow_every_cycles: int = 10,
    alpha_runner: Runner | None = None,
    alpha_every_cycles: int = 10,
    allocation_certification_runner: Runner | None = None,
    allocation_certification_every_cycles: int = 10,
    frontier_runner: FrontierRunner | None = None,
    frontier_every_cycles: int = 10,
) -> WorkerRunStats:
    """Run every research surface sequentially and release each result before the next.

    A lock around coroutines scheduled by ``asyncio.gather`` is insufficient for a
    512 MB worker: completed task results remain strongly referenced by the gather
    operation until all sibling tasks finish. This loop preserves the same cadence
    and durable evidence writes but never retains more than one heavyweight research
    result at a time.
    """

    attempted = succeeded = failed = 0
    tier_shadow_every_cycles = max(1, tier_shadow_every_cycles)
    composite_shadow_every_cycles = max(1, composite_shadow_every_cycles)
    stablecoin_shadow_every_cycles = max(1, stablecoin_shadow_every_cycles)
    alpha_every_cycles = max(1, alpha_every_cycles)
    allocation_certification_every_cycles = max(1, allocation_certification_every_cycles)
    frontier_every_cycles = max(1, frontier_every_cycles)

    store.record_worker_heartbeat(
        worker_id=worker_id,
        state="starting",
        detail={"paper_only": True, "backend": store.backend, "memory_bounded": True},
    )

    while not stop_event.is_set() and (max_cycles is None or attempted < max_cycles):
        attempted += 1
        run_frontier = frontier_runner is not None and attempted % frontier_every_cycles == 0
        run_tier_shadow = tier_shadow_runner is not None and attempted % tier_shadow_every_cycles == 0
        run_composite_shadow = composite_shadow_runner is not None and _staggered_due(
            attempted, composite_shadow_every_cycles
        )
        run_stablecoin_shadow = stablecoin_shadow_runner is not None and _quarter_staggered_due(
            attempted, stablecoin_shadow_every_cycles
        )
        run_allocation_certification = (
            allocation_certification_runner is not None
            and _third_staggered_due(attempted, allocation_certification_every_cycles)
        )
        run_alpha = alpha_runner is not None and _three_quarter_staggered_due(
            attempted, alpha_every_cycles
        )
        store.record_worker_heartbeat(
            worker_id=worker_id,
            state="running",
            detail={
                "cycle_attempt": attempted,
                "memory_bounded": True,
                "sequential_research_surfaces": True,
                "dex_frontier_probe_due": run_frontier,
                "dex_tier_shadow_due": run_tier_shadow,
                "cex_dex_composite_shadow_due": run_composite_shadow,
                "stablecoin_depth_shadow_due": run_stablecoin_shadow,
                "allocation_forward_certification_due": run_allocation_certification,
                "alpha_forward_evidence_due": run_alpha,
            },
        )

        core_value = await _capture(service.run_shadow_cycle)
        core_error_type: str | None = None
        core_error_message: str | None = None
        cycle_id = None
        scan_id = None
        if isinstance(core_value, BaseException):
            core_error_type = type(core_value).__name__
            core_error_message = str(core_value)[:500]
            detail: dict[str, object] = {
                "cycle_attempt": attempted,
                "message": core_error_message,
                "memory_bounded": True,
            }
        else:
            observations = getattr(core_value, "observations", [])
            detail = {
                "cycle_attempt": attempted,
                "observation_count": len(observations),
                "survived_count": sum(1 for obs in observations if getattr(obs, "survived", False)),
                "memory_bounded": True,
            }
            cycle_id = getattr(core_value, "cycle_id", None)
            scan_id = getattr(core_value, "verification_scan_id", None)
        _release(core_value)

        await _run_and_release(route_shadow_runner, detail, _route_summary)
        if run_tier_shadow:
            await _run_and_release(tier_shadow_runner, detail, _tier_summary)
        if run_composite_shadow:
            await _run_and_release(composite_shadow_runner, detail, _composite_summary)
        if run_stablecoin_shadow:
            await _run_and_release(stablecoin_shadow_runner, detail, _stablecoin_summary)
        if run_allocation_certification:
            await _run_and_release(allocation_certification_runner, detail, _allocation_summary)
        if run_alpha:
            await _run_and_release(alpha_runner, detail, _alpha_summary)
        if run_frontier:
            await _run_and_release(frontier_runner, detail, _frontier_summary)  # type: ignore[arg-type]

        if core_error_type is not None:
            failed += 1
            store.record_worker_heartbeat(
                worker_id=worker_id,
                state="error",
                error_type=core_error_type,
                detail=detail,
            )
            if max_cycles is None or attempted < max_cycles:
                await _interruptible_sleep(
                    service.settings.worker_error_backoff_seconds,
                    stop_event,
                    sleep,
                )
            continue

        succeeded += 1
        store.record_worker_heartbeat(
            worker_id=worker_id,
            state="success",
            cycle_id=cycle_id,
            scan_id=scan_id,
            detail=detail,
        )
        if not stop_event.is_set() and (max_cycles is None or attempted < max_cycles):
            await _interruptible_sleep(
                service.settings.shadow_cycle_interval_seconds,
                stop_event,
                sleep,
            )

    final_state = "stopped" if stop_event.is_set() else "completed"
    store.record_worker_heartbeat(
        worker_id=worker_id,
        state=final_state,
        detail={
            "cycles_attempted": attempted,
            "cycles_succeeded": succeeded,
            "cycles_failed": failed,
            "memory_bounded": True,
        },
    )
    return WorkerRunStats(worker_id, attempted, succeeded, failed)
