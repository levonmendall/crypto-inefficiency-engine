from __future__ import annotations

import asyncio
import signal

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, build_evidence_store
from inefficiency_engine.permanent_source_plane import (
    PERMANENT_SOURCE_WORKER_ID,
    PermanentSourcePlane,
    source_market_interval_seconds,
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
# A complete bounded source cycle can legitimately exceed the 180-second supervisor
# freshness boundary while many async provider attempts time out independently. Pulse
# durable liveness during the cycle so the supervisor restarts only a genuinely stuck
# event loop, not a healthy long-running acquisition cycle.
SOURCE_PROGRESS_PULSE_SECONDS = 30.0


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
    """Publish liveness while one async provider cycle is legitimately still running.

    The pulse cannot create evidence, qualification or allocation authority. If a
    synchronous/provider bug actually blocks this event loop, this coroutine also stops
    advancing and the external supervisor's 180-second watchdog still restarts the
    isolated source process.
    """

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
                    "stage": "provider_cycle_in_progress",
                    "progress_pulse": True,
                    "isolated_source_process": True,
                    "resident_with_portfolio_process": False,
                    "separate_python_process": True,
                    "disposable_research_required": False,
                    "portfolio_authority": False,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        except Exception:
            # Heartbeat persistence failure must not cancel source acquisition; the
            # supervisor will fail closed and restart if durable liveness remains old.
            pass


async def _permanent_source_refresh_loop(
    store: EvidenceStore,
    *,
    stop_event: asyncio.Event,
) -> None:
    """Continuously acquire public evidence in its own process/failure domain."""

    source_plane: PermanentSourcePlane | None = None
    while not stop_event.is_set():
        try:
            if source_plane is None:
                source_plane = PermanentSourcePlane(store)

            cycle_done = asyncio.Event()
            pulse_task = asyncio.create_task(
                _source_cycle_progress_pulse(store, cycle_done=cycle_done),
                name="source-cycle-progress-pulse",
            )
            try:
                await source_plane.run_cycle()
            finally:
                # Stop the pulse before writing any outer degraded heartbeat so a
                # late pulse can never overwrite the cycle's terminal durable state.
                cycle_done.set()
                pulse_task.cancel()
                await asyncio.gather(pulse_task, return_exceptions=True)
        except Exception as exc:
            try:
                store.record_worker_heartbeat(
                    worker_id=PERMANENT_SOURCE_WORKER_ID,
                    state="degraded",
                    error_type=type(exc).__name__,
                    detail={
                        "retrying": True,
                        "isolated_source_process": True,
                        "resident_with_portfolio_process": False,
                        "separate_python_process": True,
                        "disposable_research_required": False,
                        "portfolio_authority": False,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
                    },
                )
            except Exception:
                pass
            # Rebuild after an unexpected runtime/construction failure so a poisoned
            # provider client cannot leave this source process permanently dead.
            source_plane = None

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=source_market_interval_seconds(),
            )
        except TimeoutError:
            continue


async def run_permanent_source_worker(
    store: EvidenceStore,
    *,
    stop_event: asyncio.Event | None = None,
) -> int:
    """Own network-facing source/universe work outside portfolio accounting.

    A synchronous provider stall may block this event loop, but it can no longer block
    canonical portfolio accounting because the Render supervisor runs this module in a
    distinct Python process. The supervisor can terminate/restart this process when the
    durable source heartbeat stops advancing.
    """

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
        volume_task.cancel()
        await asyncio.gather(source_task, volume_task, return_exceptions=True)


def main() -> int:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("permanent source worker requires durable evidence persistence")
    return asyncio.run(run_permanent_source_worker(store))


if __name__ == "__main__":
    raise SystemExit(main())
