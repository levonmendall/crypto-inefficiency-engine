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
Publisher = Callable[[], None]
DEFAULT_RESEARCH_STAGE_TIMEOUT_SECONDS = 120.0


class ResearchStageTimeoutError(TimeoutError):
    """A bounded auxiliary research stage exceeded its wall-clock allowance."""


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


async def _capture(
    runner: Runner,
    *,
    timeout_seconds: float | None = None,
    stage_name: str = "research",
) -> object | BaseException:
    try:
        if timeout_seconds is None or timeout_seconds <= 0:
            return await runner()
        try:
            return await asyncio.wait_for(runner(), timeout=max(0.001, float(timeout_seconds)))
        except TimeoutError as exc:
            error = ResearchStageTimeoutError(
                f"research stage {stage_name!r} exceeded {float(timeout_seconds):.1f}s"
            )
            error.__cause__ = exc
            return error
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


def _source_refresh_summary(detail: dict[str, object], value: object | BaseException | None) -> None:
    if isinstance(value, BaseException):
        detail["source_refresh_error_type"] = type(value).__name__
        detail["source_refresh_complete"] = False
        return
    detail["source_refresh_complete"] = True
    if isinstance(value, dict):
        refresh = value.get("source_refresh")
        coverage = value.get("source_coverage")
        if isinstance(refresh, dict):
            detail["source_refresh_state"] = refresh.get("state")
            detail["source_refresh_memory_deferred_count"] = len(refresh.get("memory_deferred_sources") or [])
            detail["source_refresh_failed_count"] = len(refresh.get("failed_sources") or [])
        if isinstance(coverage, dict):
            detail["source_coverage_sufficient_lane_count"] = coverage.get("sufficient_lane_count")


async def _run_and_release(
    runner: Runner | None,
    detail: dict[str, object],
    summarizer: Callable[[dict[str, object], object | BaseException | None], None],
    *,
    timeout_seconds: float,
    stage_name: str,
) -> None:
    if runner is None:
        return
    value = await _capture(
        runner,
        timeout_seconds=timeout_seconds,
        stage_name=stage_name,
    )
    summarizer(detail, value)
    _release(value)


def _stage_heartbeat(
    store: EvidenceStore,
    *,
    worker_id: str,
    attempted: int,
    stage_name: str,
    timeout_seconds: float,
) -> None:
    store.record_worker_heartbeat(
        worker_id=worker_id,
        state="running",
        detail={
            "cycle_attempt": attempted,
            "stage": stage_name,
            "stage_timeout_seconds": timeout_seconds,
            "memory_bounded": True,
            "sequential_research_surfaces": True,
            "paper_only": True,
        },
    )


async def run_memory_bounded_research_worker(
    service: OpportunityService,
    store: EvidenceStore,
    *,
    worker_id: str,
    stop_event: asyncio.Event,
    sleep: SleepFn = asyncio.sleep,
    max_cycles: int | None = None,
    source_refresh_runner: Runner | None = None,
    source_refresh_every_cycles: int = 1,
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
    post_success_publisher: Publisher | None = None,
    stage_timeout_seconds: float = DEFAULT_RESEARCH_STAGE_TIMEOUT_SECONDS,
) -> WorkerRunStats:
    """Run research surfaces sequentially with bounded stage wall-clock time.

    Fresh source collection is a first-class stage and runs before heavy market/L2
    work. Each stage publishes its name before entering the await so the outer
    supervisor can distinguish a healthy long cycle from an alive-but-wedged thread.
    Completed heavyweight results are released before the next stage begins.
    """

    attempted = succeeded = failed = 0
    source_refresh_every_cycles = max(1, source_refresh_every_cycles)
    tier_shadow_every_cycles = max(1, tier_shadow_every_cycles)
    composite_shadow_every_cycles = max(1, composite_shadow_every_cycles)
    stablecoin_shadow_every_cycles = max(1, stablecoin_shadow_every_cycles)
    alpha_every_cycles = max(1, alpha_every_cycles)
    allocation_certification_every_cycles = max(1, allocation_certification_every_cycles)
    frontier_every_cycles = max(1, frontier_every_cycles)
    stage_timeout_seconds = max(1.0, float(stage_timeout_seconds))

    store.record_worker_heartbeat(
        worker_id=worker_id,
        state="starting",
        detail={
            "paper_only": True,
            "backend": store.backend,
            "memory_bounded": True,
            "stage_deadlines": True,
            "stage_timeout_seconds": stage_timeout_seconds,
            "independent_source_refresh": source_refresh_runner is not None,
        },
    )

    while not stop_event.is_set() and (max_cycles is None or attempted < max_cycles):
        attempted += 1
        run_source_refresh = source_refresh_runner is not None and attempted % source_refresh_every_cycles == 0
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
        detail: dict[str, object] = {
            "cycle_attempt": attempted,
            "memory_bounded": True,
            "sequential_research_surfaces": True,
            "stage_deadlines": True,
            "source_refresh_due": run_source_refresh,
            "dex_frontier_probe_due": run_frontier,
            "dex_tier_shadow_due": run_tier_shadow,
            "cex_dex_composite_shadow_due": run_composite_shadow,
            "stablecoin_depth_shadow_due": run_stablecoin_shadow,
            "allocation_forward_certification_due": run_allocation_certification,
            "alpha_forward_evidence_due": run_alpha,
        }

        if run_source_refresh:
            _stage_heartbeat(
                store,
                worker_id=worker_id,
                attempted=attempted,
                stage_name="source_refresh",
                timeout_seconds=stage_timeout_seconds,
            )
            await _run_and_release(
                source_refresh_runner,
                detail,
                _source_refresh_summary,
                timeout_seconds=stage_timeout_seconds,
                stage_name="source_refresh",
            )

        _stage_heartbeat(
            store,
            worker_id=worker_id,
            attempted=attempted,
            stage_name="core_shadow",
            timeout_seconds=stage_timeout_seconds,
        )
        core_value = await _capture(
            service.run_shadow_cycle,
            timeout_seconds=stage_timeout_seconds,
            stage_name="core_shadow",
        )
        core_error_type: str | None = None
        core_error_message: str | None = None
        cycle_id = None
        scan_id = None
        if isinstance(core_value, BaseException):
            core_error_type = type(core_value).__name__
            core_error_message = str(core_value)[:500]
            detail["message"] = core_error_message
        else:
            observations = getattr(core_value, "observations", [])
            detail["observation_count"] = len(observations)
            detail["survived_count"] = sum(1 for obs in observations if getattr(obs, "survived", False))
            cycle_id = getattr(core_value, "cycle_id", None)
            scan_id = getattr(core_value, "verification_scan_id", None)
        _release(core_value)

        async def run_optional(
            runner: Runner | None,
            summarizer: Callable[[dict[str, object], object | BaseException | None], None],
            stage_name: str,
        ) -> None:
            if runner is None:
                return
            _stage_heartbeat(
                store,
                worker_id=worker_id,
                attempted=attempted,
                stage_name=stage_name,
                timeout_seconds=stage_timeout_seconds,
            )
            await _run_and_release(
                runner,
                detail,
                summarizer,
                timeout_seconds=stage_timeout_seconds,
                stage_name=stage_name,
            )

        if route_shadow_runner is not None:
            _stage_heartbeat(
                store,
                worker_id=worker_id,
                attempted=attempted,
                stage_name="dex_route_shadow",
                timeout_seconds=stage_timeout_seconds,
            )
            await _run_and_release(route_shadow_runner, detail, _route_summary, timeout_seconds=stage_timeout_seconds, stage_name="dex_route_shadow")
        if run_tier_shadow:
            await run_optional(tier_shadow_runner, _tier_summary, "dex_tier_shadow")
        if run_composite_shadow:
            await run_optional(composite_shadow_runner, _composite_summary, "cex_dex_composite_shadow")
        if run_stablecoin_shadow:
            await run_optional(stablecoin_shadow_runner, _stablecoin_summary, "stablecoin_depth_shadow")
        if run_allocation_certification:
            await run_optional(
                allocation_certification_runner,
                _allocation_summary,
                "allocation_operating_certification",
            )
        if run_alpha:
            await run_optional(alpha_runner, _alpha_summary, "alpha_forward_evidence")
        if run_frontier:
            await run_optional(frontier_runner, _frontier_summary, "dex_route_frontier")  # type: ignore[arg-type]

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
        detail["stage"] = "cycle_complete"
        store.record_worker_heartbeat(
            worker_id=worker_id,
            state="success",
            cycle_id=cycle_id,
            scan_id=scan_id,
            detail=detail,
        )
        if post_success_publisher is not None:
            try:
                post_success_publisher()
            except Exception:
                # Presentation projection is explicitly fail-contained. Durable
                # research success cannot be reclassified by a UI projection error.
                pass
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
            "stage_deadlines": True,
        },
    )
    return WorkerRunStats(worker_id, attempted, succeeded, failed)
