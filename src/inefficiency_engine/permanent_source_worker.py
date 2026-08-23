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
            await source_plane.run_cycle()
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
