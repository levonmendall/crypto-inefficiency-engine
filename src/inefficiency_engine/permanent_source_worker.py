from __future__ import annotations

import asyncio
import signal
import time
from datetime import datetime, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, build_evidence_store
from inefficiency_engine.permanent_source_plane import (
    PERMANENT_SOURCE_WORKER_ID,
    RESEARCH_MARKET_WORKER_ID,
    PermanentSourcePlane,
    source_executable_deadline_seconds,
    source_market_interval_seconds,
    source_priority_interval_seconds,
    source_research_interval_seconds,
)
from inefficiency_engine.priority_source_collection import SOURCE_REFRESH_WORKER_ID
from inefficiency_engine.source_runtime_safety import (
    install_bulk_provider_catalog_runtime,
    install_source_coverage_reconciliation_runtime,
)
from inefficiency_engine.volume_universe import (
    TOP_VOLUME_ASSET_COUNT,
    VOLUME_UNIVERSE_REFRESH_SECONDS,
    resolve_top_volume_assets,
)


VOLUME_UNIVERSE_WORKER_ID = "volume-universe-lightweight-refresh"
VOLUME_UNIVERSE_MAINTENANCE_SECONDS = max(
    60.0,
    min(300.0, VOLUME_UNIVERSE_REFRESH_SECONDS / 3.0),
)
SOURCE_PROGRESS_PULSE_SECONDS = 30.0


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _remaining_cycle_delay(
    *,
    interval_seconds: float,
    started_monotonic: float,
    now_monotonic: float | None = None,
) -> float:
    """Anchor cadence to cycle start instead of adding sleep after cycle runtime."""

    current = time.monotonic() if now_monotonic is None else float(now_monotonic)
    elapsed = max(0.0, current - float(started_monotonic))
    return max(0.0, float(interval_seconds) - elapsed)


async def _wait_for_next_cycle(
    stop_event: asyncio.Event,
    *,
    interval_seconds: float,
    started_monotonic: float,
) -> None:
    delay = _remaining_cycle_delay(
        interval_seconds=interval_seconds,
        started_monotonic=started_monotonic,
    )
    if delay <= 0.0:
        await asyncio.sleep(0)
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay)
    except TimeoutError:
        return


async def _volume_universe_refresh_loop(
    store: EvidenceStore,
    *,
    stop_event: asyncio.Event,
) -> None:
    """Maintain top-volume routing state outside the executable hot path."""

    while not stop_event.is_set():
        try:
            assets = await resolve_top_volume_assets(store, force_refresh=True)
            store.record_worker_heartbeat(
                worker_id=VOLUME_UNIVERSE_WORKER_ID,
                state="success",
                detail={
                    "asset_count": len(assets),
                    "universe_target_count": TOP_VOLUME_ASSET_COUNT,
                    "force_refreshed": True,
                    "executable_hot_path_dependency": False,
                    "isolated_source_process": True,
                    "portfolio_authority": False,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        except Exception as exc:
            try:
                store.record_worker_heartbeat(
                    worker_id=VOLUME_UNIVERSE_WORKER_ID,
                    state="degraded",
                    error_type=type(exc).__name__,
                    detail={
                        "message": str(exc)[:500],
                        "universe_target_count": TOP_VOLUME_ASSET_COUNT,
                        "retrying": True,
                        "executable_hot_path_dependency": False,
                        "isolated_source_process": True,
                        "portfolio_authority": False,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
                    },
                )
            except Exception:
                pass

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=VOLUME_UNIVERSE_MAINTENANCE_SECONDS,
            )
        except TimeoutError:
            continue


async def _source_cycle_progress_pulse(
    store: EvidenceStore,
    *,
    cycle_done: asyncio.Event,
    interval_seconds: float = SOURCE_PROGRESS_PULSE_SECONDS,
) -> None:
    """Publish process progress without falsely claiming fresh executable evidence."""

    interval = max(0.01, float(interval_seconds))
    while not cycle_done.is_set():
        try:
            await asyncio.wait_for(cycle_done.wait(), timeout=interval)
            return
        except TimeoutError:
            pass
        if cycle_done.is_set():
            return
        try:
            store.record_worker_heartbeat(
                worker_id=PERMANENT_SOURCE_WORKER_ID,
                state="running",
                detail={
                    "stage": "executable_market_l2_cycle_in_progress",
                    "progress_pulse": True,
                    "fresh_evidence_published": False,
                    "executable_hot_path": True,
                    "whole_cycle_deadline_seconds": source_executable_deadline_seconds(),
                    "isolated_source_process": True,
                    "resident_with_portfolio_process": False,
                    "separate_python_process": True,
                    "priority_source_tail_decoupled": True,
                    "broad_research_sweep_decoupled": True,
                    "disposable_research_required": False,
                    "portfolio_authority": False,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        except Exception:
            pass


async def _priority_source_progress_pulse(
    store: EvidenceStore,
    *,
    cycle_done: asyncio.Event,
    interval_seconds: float = SOURCE_PROGRESS_PULSE_SECONDS,
) -> None:
    """Keep slow priority-source ownership current without delaying market/L2."""

    interval = max(0.01, float(interval_seconds))
    while not cycle_done.is_set():
        try:
            await asyncio.wait_for(cycle_done.wait(), timeout=interval)
            return
        except TimeoutError:
            pass
        if cycle_done.is_set():
            return
        try:
            store.record_worker_heartbeat(
                worker_id=SOURCE_REFRESH_WORKER_ID,
                state="running",
                detail={
                    "stage": "priority_source_cycle_in_progress",
                    "progress_pulse": True,
                    "market_l2_cadence_independent": True,
                    "isolated_source_process": True,
                    "qualification_thresholds_unchanged": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        except Exception:
            pass


async def _permanent_source_refresh_loop(
    store: EvidenceStore,
    *,
    stop_event: asyncio.Event,
) -> None:
    """Refresh executable market/funding/L2 on a strict start-to-start cadence."""

    source_plane: PermanentSourcePlane | None = None
    sequence = 0
    while not stop_event.is_set():
        sequence += 1
        started_at = _now()
        started_monotonic = time.monotonic()
        try:
            if source_plane is None:
                source_plane = PermanentSourcePlane(store)
            store.record_worker_heartbeat(
                worker_id=PERMANENT_SOURCE_WORKER_ID,
                state="running",
                detail={
                    "sequence": sequence,
                    "stage": "executable_market_l2_refresh",
                    "executable_hot_path": True,
                    "fresh_evidence_published": False,
                    "whole_cycle_deadline_seconds": source_executable_deadline_seconds(),
                    "start_to_start_interval_seconds": source_market_interval_seconds(),
                    "isolated_source_process": True,
                    "separate_python_process": True,
                    "priority_source_tail_decoupled": True,
                    "broad_research_sweep_decoupled": True,
                    "disposable_research_required": False,
                    "portfolio_authority": False,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
            cycle_done = asyncio.Event()
            pulse_task = asyncio.create_task(
                _source_cycle_progress_pulse(store, cycle_done=cycle_done),
                name="source-cycle-progress-pulse",
            )
            try:
                snapshot = await source_plane.refresh_market_l2_snapshot()
            finally:
                cycle_done.set()
                pulse_task.cancel()
                await asyncio.gather(pulse_task, return_exceptions=True)
            store.record_worker_heartbeat(
                worker_id=PERMANENT_SOURCE_WORKER_ID,
                state="success",
                scan_id=snapshot.scan_id,
                detail={
                    "sequence": sequence,
                    "stage": "executable_market_l2_complete",
                    "market_scan_id": snapshot.scan_id,
                    "market_quote_count": len(snapshot.market_quotes),
                    "funding_quote_count": len(snapshot.funding_quotes),
                    "order_book_count": len(snapshot.order_books),
                    "cycle_runtime_seconds": max(0.0, (_now() - started_at).total_seconds()),
                    "executable_hot_path": True,
                    "fresh_evidence_published": True,
                    "whole_cycle_deadline_seconds": source_executable_deadline_seconds(),
                    "start_to_start_interval_seconds": source_market_interval_seconds(),
                    "isolated_source_process": True,
                    "separate_python_process": True,
                    "priority_source_tail_decoupled": True,
                    "broad_research_sweep_decoupled": True,
                    "disposable_research_required": False,
                    "portfolio_authority": False,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        except Exception as exc:
            try:
                store.record_worker_heartbeat(
                    worker_id=PERMANENT_SOURCE_WORKER_ID,
                    state="degraded",
                    error_type=type(exc).__name__,
                    detail={
                        "sequence": sequence,
                        "stage": "executable_market_l2_refresh",
                        "message": str(exc)[:500],
                        "retrying": True,
                        "fresh_evidence_published": False,
                        "executable_hot_path": True,
                        "whole_cycle_deadline_seconds": source_executable_deadline_seconds(),
                        "start_to_start_interval_seconds": source_market_interval_seconds(),
                        "isolated_source_process": True,
                        "separate_python_process": True,
                        "priority_source_tail_decoupled": True,
                        "broad_research_sweep_decoupled": True,
                        "disposable_research_required": False,
                        "portfolio_authority": False,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
                    },
                )
            except Exception:
                pass
            source_plane = None

        await _wait_for_next_cycle(
            stop_event,
            interval_seconds=source_market_interval_seconds(),
            started_monotonic=started_monotonic,
        )


async def _research_market_refresh_loop(
    store: EvidenceStore,
    *,
    stop_event: asyncio.Event,
) -> None:
    """Maintain broad 25-asset market history without participating in L2 freshness."""

    source_plane: PermanentSourcePlane | None = None
    sequence = 0
    while not stop_event.is_set():
        sequence += 1
        started_at = _now()
        started_monotonic = time.monotonic()
        try:
            if source_plane is None:
                source_plane = PermanentSourcePlane(store)
            store.record_worker_heartbeat(
                worker_id=RESEARCH_MARKET_WORKER_ID,
                state="running",
                detail={
                    "sequence": sequence,
                    "stage": "broad_market_research_sweep",
                    "executable_hot_path": False,
                    "can_block_executable_freshness": False,
                    "isolated_source_process": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
            snapshot = await source_plane.refresh_research_market_snapshot()
            store.record_worker_heartbeat(
                worker_id=RESEARCH_MARKET_WORKER_ID,
                state="success",
                scan_id=snapshot.scan_id,
                detail={
                    "sequence": sequence,
                    "stage": "broad_market_research_complete",
                    "market_scan_id": snapshot.scan_id,
                    "market_quote_count": len(snapshot.market_quotes),
                    "funding_quote_count": len(snapshot.funding_quotes),
                    "order_book_count": 0,
                    "cycle_runtime_seconds": max(0.0, (_now() - started_at).total_seconds()),
                    "executable_hot_path": False,
                    "can_block_executable_freshness": False,
                    "isolated_source_process": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        except Exception as exc:
            try:
                store.record_worker_heartbeat(
                    worker_id=RESEARCH_MARKET_WORKER_ID,
                    state="degraded",
                    error_type=type(exc).__name__,
                    detail={
                        "sequence": sequence,
                        "stage": "broad_market_research_sweep",
                        "message": str(exc)[:500],
                        "retrying": True,
                        "executable_hot_path": False,
                        "can_block_executable_freshness": False,
                        "isolated_source_process": True,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
                    },
                )
            except Exception:
                pass
            source_plane = None

        await _wait_for_next_cycle(
            stop_event,
            interval_seconds=source_research_interval_seconds(),
            started_monotonic=started_monotonic,
        )


async def _priority_source_refresh_loop(
    store: EvidenceStore,
    *,
    stop_event: asyncio.Event,
) -> None:
    """Refresh protocol/event/options/trade-flow evidence on an independent cadence."""

    source_plane: PermanentSourcePlane | None = None
    while not stop_event.is_set():
        started_monotonic = time.monotonic()
        cycle_done: asyncio.Event | None = None
        pulse_task: asyncio.Task[None] | None = None
        try:
            if source_plane is None:
                source_plane = PermanentSourcePlane(store)
            cycle_done = asyncio.Event()
            pulse_task = asyncio.create_task(
                _priority_source_progress_pulse(store, cycle_done=cycle_done),
                name="priority-source-progress-pulse",
            )
            await source_plane.priority.run_cycle()
        except Exception as exc:
            try:
                store.record_worker_heartbeat(
                    worker_id=SOURCE_REFRESH_WORKER_ID,
                    state="degraded",
                    error_type=type(exc).__name__,
                    detail={
                        "stage": "independent_priority_source_loop",
                        "message": str(exc)[:500],
                        "retrying": True,
                        "market_l2_cadence_independent": True,
                        "isolated_source_process": True,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
                    },
                )
            except Exception:
                pass
            source_plane = None
        finally:
            if cycle_done is not None:
                cycle_done.set()
            if pulse_task is not None:
                pulse_task.cancel()
                await asyncio.gather(pulse_task, return_exceptions=True)

        await _wait_for_next_cycle(
            stop_event,
            interval_seconds=source_priority_interval_seconds(),
            started_monotonic=started_monotonic,
        )


async def run_permanent_source_worker(
    store: EvidenceStore,
    *,
    stop_event: asyncio.Event | None = None,
) -> int:
    """Own all network-facing source work outside portfolio accounting.

    Executable market/L2, broad research market history, slow priority sources, and
    volume-universe maintenance have independent schedules. The executable cohort is
    hard-deadlined and start-to-start scheduled, so research breadth or one slow
    protocol provider cannot consume the freshness budget required by mechanism and
    qualification stages.
    """

    install_bulk_provider_catalog_runtime()
    install_source_coverage_reconciliation_runtime()

    stop = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass

    source_task = asyncio.create_task(
        _permanent_source_refresh_loop(store, stop_event=stop),
        name="permanent-executable-source-refresh",
    )
    research_market_task = asyncio.create_task(
        _research_market_refresh_loop(store, stop_event=stop),
        name="research-market-universe-refresh",
    )
    priority_task = asyncio.create_task(
        _priority_source_refresh_loop(store, stop_event=stop),
        name="priority-source-refresh",
    )
    volume_task = asyncio.create_task(
        _volume_universe_refresh_loop(store, stop_event=stop),
        name="volume-universe-refresh",
    )
    try:
        await stop.wait()
        return 0
    finally:
        stop.set()
        source_task.cancel()
        research_market_task.cancel()
        priority_task.cancel()
        volume_task.cancel()
        await asyncio.gather(
            source_task,
            research_market_task,
            priority_task,
            volume_task,
            return_exceptions=True,
        )


def main() -> int:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("permanent source worker requires durable evidence persistence")
    return asyncio.run(run_permanent_source_worker(store))


if __name__ == "__main__":
    raise SystemExit(main())
