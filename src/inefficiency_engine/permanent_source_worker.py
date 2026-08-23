from __future__ import annotations

import asyncio
import signal
from datetime import datetime, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, build_evidence_store
from inefficiency_engine.permanent_source_plane import (
    PERMANENT_SOURCE_WORKER_ID,
    PermanentSourcePlane,
    source_market_interval_seconds,
    source_priority_interval_seconds,
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
# Keep the small membership refresh independent from disposable research while also
# keeping every external-network dependency outside canonical portfolio accounting.
VOLUME_UNIVERSE_MAINTENANCE_SECONDS = max(
    60.0,
    min(300.0, VOLUME_UNIVERSE_REFRESH_SECONDS / 3.0),
)
# Retained for compatibility and as a bounded liveness helper for any future long
# market refresh. The normal production path now separates market/L2 from the slow
# priority-source tail, so a slow Aave/options/governance probe cannot hold the L2
# cadence hostage.
SOURCE_PROGRESS_PULSE_SECONDS = 30.0


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _volume_universe_refresh_loop(
    store: EvidenceStore,
    *,
    stop_event: asyncio.Event,
) -> None:
    """Maintain top-volume routing state inside the isolated source process."""

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
    """Publish liveness while one async market/provider cycle is still running."""

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
                    "stage": "market_l2_cycle_in_progress",
                    "progress_pulse": True,
                    "isolated_source_process": True,
                    "resident_with_portfolio_process": False,
                    "separate_python_process": True,
                    "priority_source_tail_decoupled": True,
                    "disposable_research_required": False,
                    "portfolio_authority": False,
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
    """Refresh market/funding/L2 on its own cadence, independent of slow sources."""

    source_plane: PermanentSourcePlane | None = None
    sequence = 0
    while not stop_event.is_set():
        sequence += 1
        started_at = _now()
        try:
            if source_plane is None:
                source_plane = PermanentSourcePlane(store)
            store.record_worker_heartbeat(
                worker_id=PERMANENT_SOURCE_WORKER_ID,
                state="running",
                detail={
                    "sequence": sequence,
                    "stage": "market_l2_refresh",
                    "isolated_source_process": True,
                    "separate_python_process": True,
                    "priority_source_tail_decoupled": True,
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
                    "stage": "market_l2_complete",
                    "market_scan_id": snapshot.scan_id,
                    "market_quote_count": len(snapshot.market_quotes),
                    "funding_quote_count": len(snapshot.funding_quotes),
                    "order_book_count": len(snapshot.order_books),
                    "cycle_runtime_seconds": max(0.0, (_now() - started_at).total_seconds()),
                    "isolated_source_process": True,
                    "separate_python_process": True,
                    "priority_source_tail_decoupled": True,
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
                        "stage": "market_l2_refresh",
                        "message": str(exc)[:500],
                        "retrying": True,
                        "isolated_source_process": True,
                        "separate_python_process": True,
                        "priority_source_tail_decoupled": True,
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

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=source_market_interval_seconds(),
            )
        except TimeoutError:
            continue


async def _priority_source_refresh_loop(
    store: EvidenceStore,
    *,
    stop_event: asyncio.Event,
) -> None:
    """Refresh slower protocol/event/options/trade-flow evidence independently."""

    source_plane: PermanentSourcePlane | None = None
    while not stop_event.is_set():
        try:
            if source_plane is None:
                source_plane = PermanentSourcePlane(store)
            # PrioritySourceCollectionService owns its own durable source-refresh
            # heartbeat and provider-level fail-closed diagnostics.
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

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=source_priority_interval_seconds(),
            )
        except TimeoutError:
            continue


async def run_permanent_source_worker(
    store: EvidenceStore,
    *,
    stop_event: asyncio.Event | None = None,
) -> int:
    """Own network-facing source/universe work outside portfolio accounting.

    Market/L2 and slower priority sources now have separate schedules inside this
    isolated process. A slow protocol or event provider can therefore no longer make
    executable order-book evidence stale. The supervisor can still restart the source
    process when the market/L2 durable heartbeat itself stops advancing.
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
        name="permanent-source-refresh",
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
        priority_task.cancel()
        volume_task.cancel()
        await asyncio.gather(
            source_task,
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
