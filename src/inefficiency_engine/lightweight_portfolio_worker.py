from __future__ import annotations

import asyncio
import signal

from inefficiency_engine.canonical_worker import run_canonical_portfolio_loop
from inefficiency_engine.cex_dex_canonical_runtime import (
    CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService,
    CexDexUniversalOperationallyResilientPaperPortfolioService,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.dashboard_projection import (
    DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
    ResearchDashboardProjectionLedger,
)
from inefficiency_engine.evidence import EvidenceStore, build_evidence_store
from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityLaneSuccessOperationallyResilientPaperPortfolioService,
    EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService,
)
from inefficiency_engine.service import OpportunityService


# Presentation publication is deliberately independent from the disposable heavy
# research child. It projects already-persisted truth only and makes no provider calls.
RESEARCH_PROJECTION_MAINTENANCE_SECONDS = 60.0


# Preserve the permanent worker's durable-bridge and canonical-portfolio lineage.
# Canonical accounting consumes persisted qualified state only. All network-facing
# source/universe acquisition is owned by the separately supervised source process.
CanonicalPortfolioAllocatorService = EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService
CanonicalPaperPortfolioService = EvidenceVelocityLaneSuccessOperationallyResilientPaperPortfolioService
assert issubclass(
    CanonicalPortfolioAllocatorService,
    CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService,
)
assert issubclass(
    CanonicalPaperPortfolioService,
    CexDexUniversalOperationallyResilientPaperPortfolioService,
)


class _DurableQualifiedStateHandle:
    """Minimal allocator dependency: canonical allocation reads durable state only."""

    def __init__(self, store: EvidenceStore):
        self.store = store


async def _research_projection_refresh_loop(
    store: EvidenceStore,
    *,
    settings: Settings,
    stop_event: asyncio.Event,
) -> None:
    """Republish persisted card truth even when heavy research is deferred or fails.

    Publication freshness and research-runtime freshness are separate claims. This
    loop keeps the presentation projection current from durable ledgers, while the
    dashboard still reads the independent research-worker heartbeat and fails closed
    whenever actual research execution is stale or degraded.
    """

    projection = ResearchDashboardProjectionLedger(store)
    while not stop_event.is_set():
        try:
            payload = await asyncio.to_thread(
                projection.publish,
                forward_target=max(1, int(settings.alpha_min_forward_samples)),
                settled_target=max(
                    5,
                    int(getattr(settings, "operating_certification_min_settled_trials", 20)),
                ),
                shadow_horizons_seconds=tuple(
                    getattr(settings, "shadow_horizons_seconds", (60.0,)) or (60.0,)
                ),
                shadow_cycle_interval_seconds=float(settings.shadow_cycle_interval_seconds),
                alpha_evidence_every_cycles=max(1, int(settings.alpha_evidence_every_cycles)),
                heartbeat_stale_seconds=float(settings.worker_heartbeat_stale_seconds),
            )
            store.record_worker_heartbeat(
                worker_id=DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
                state="success",
                detail={
                    "projection_observed_at": payload.get("observed_at"),
                    "publication_stage": "lightweight_persisted_refresh",
                    "research_computation": False,
                    "provider_calls": False,
                    "presentation_only": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        except Exception as exc:
            try:
                store.record_worker_heartbeat(
                    worker_id=DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
                    state="degraded",
                    error_type=type(exc).__name__,
                    detail={
                        "publication_stage": "lightweight_persisted_refresh",
                        "retrying": True,
                        "research_computation": False,
                        "provider_calls": False,
                        "presentation_only": True,
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
                timeout=RESEARCH_PROJECTION_MAINTENANCE_SECONDS,
            )
        except TimeoutError:
            continue


async def run_lightweight_portfolio_worker(
    store: EvidenceStore,
    *,
    settings: Settings | None = None,
    stop_event: asyncio.Event | None = None,
) -> int:
    """Run canonical accounting with no external-provider work on its event loop."""

    settings = settings or Settings.from_env()
    service = OpportunityService(settings=settings, evidence_store=store)
    state_handle = _DurableQualifiedStateHandle(store)
    allocator = CanonicalPortfolioAllocatorService(
        service,
        None,
        state_handle,
    )  # type: ignore[arg-type]
    portfolio = CanonicalPaperPortfolioService(service, allocator, store)

    stop = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass

    # Persisted read-model projection is the only maintenance task allowed to share
    # this process. It performs no provider calls and runs its database work in a
    # thread, so source/network stalls cannot freeze canonical accounting heartbeats.
    projection_task = asyncio.create_task(
        _research_projection_refresh_loop(store, settings=settings, stop_event=stop),
        name="research-dashboard-projection-refresh",
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
            await projection_task
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
