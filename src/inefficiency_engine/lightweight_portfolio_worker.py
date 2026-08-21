from __future__ import annotations

import asyncio
import signal

from inefficiency_engine.canonical_worker import run_canonical_portfolio_loop
from inefficiency_engine.cex_dex_canonical_runtime import (
    CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService as CanonicalPortfolioAllocatorService,
    CexDexUniversalOperationallyResilientPaperPortfolioService as CanonicalPaperPortfolioService,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, build_evidence_store
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.volume_universe import (
    TOP_VOLUME_ASSET_COUNT,
    VOLUME_UNIVERSE_REFRESH_SECONDS,
    resolve_top_volume_assets,
)


VOLUME_UNIVERSE_WORKER_ID = "volume-universe-lightweight-refresh"
# The permanent worker gets several attempts to refresh membership before the
# database-only read plane reaches its stale boundary. This is intentionally tiny
# work (two CoinGecko reads plus one compact DB snapshot), not historical research.
VOLUME_UNIVERSE_MAINTENANCE_SECONDS = max(
    60.0,
    min(300.0, VOLUME_UNIVERSE_REFRESH_SECONDS / 3.0),
)


class _DurableQualifiedStateHandle:
    """Minimal allocator dependency: canonical allocation reads persisted bridge state."""

    def __init__(self, store: EvidenceStore):
        self.store = store


async def _volume_universe_refresh_loop(
    store: EvidenceStore,
    *,
    stop_event: asyncio.Event,
) -> None:
    """Keep current top-volume membership fresh without depending on heavy jobs.

    A history/research process may be deferred by memory pressure. Membership is a
    lightweight market-state input, so it must not disappear merely because a heavy
    process cannot start. Failures here are contained: last-known-good state remains
    durable and the canonical portfolio loop continues independently.
    """

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
                    "lightweight": True,
                },
            )
        except Exception as exc:
            # Membership refresh must never become portfolio authority or crash the
            # canonical accounting process. The read plane remains fail-closed if
            # fresh membership cannot be proven.
            try:
                store.record_worker_heartbeat(
                    worker_id=VOLUME_UNIVERSE_WORKER_ID,
                    state="degraded",
                    error_type=type(exc).__name__,
                    detail={
                        "universe_target_count": TOP_VOLUME_ASSET_COUNT,
                        "retrying": True,
                        "lightweight": True,
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


async def run_lightweight_portfolio_worker(
    store: EvidenceStore,
    *,
    settings: Settings | None = None,
    stop_event: asyncio.Event | None = None,
) -> int:
    settings = settings or Settings.from_env()
    service = OpportunityService(settings=settings, evidence_store=store)
    state_handle = _DurableQualifiedStateHandle(store)
    allocator = CanonicalPortfolioAllocatorService(service, None, state_handle)  # type: ignore[arg-type]
    portfolio = CanonicalPaperPortfolioService(service, allocator, store)

    stop = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass

    # Run membership refresh inside this already-resident lightweight process rather
    # than adding another permanent Python process to the Render memory footprint.
    volume_task = asyncio.create_task(
        _volume_universe_refresh_loop(store, stop_event=stop),
        name="volume-universe-refresh",
    )
    try:
        return await run_canonical_portfolio_loop(
            service,
            store,
            portfolio=portfolio,
            stop_event=stop,
        )
    finally:
        stop.set()
        try:
            await volume_task
        except asyncio.CancelledError:
            pass


def main() -> int:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("canonical portfolio requires durable evidence persistence")
    return asyncio.run(run_lightweight_portfolio_worker(store, settings=settings))


if __name__ == "__main__":
    raise SystemExit(main())
