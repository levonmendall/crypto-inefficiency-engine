from __future__ import annotations

import asyncio
import signal

from inefficiency_engine.canonical_worker import run_canonical_portfolio_loop
from inefficiency_engine.cex_dex_canonical_runtime import (
    CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService,
    CexDexUniversalOperationallyResilientPaperPortfolioService,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, build_evidence_store
from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityLaneSuccessOperationallyResilientPaperPortfolioService,
    EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService,
)
from inefficiency_engine.service import OpportunityService


# Preserve the permanent worker's durable-bridge and canonical-portfolio lineage
# explicitly while installing the integrated evidence-velocity + Release D subclasses.
# The process remains provider-free and consumes only persisted qualified state.
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


async def run_lightweight_portfolio_worker(
    store: EvidenceStore,
    *,
    settings: Settings | None = None,
    stop_event: asyncio.Event | None = None,
) -> int:
    """Run provider-free canonical accounting with all-lane paper settlement enabled."""

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

    return await run_canonical_portfolio_loop(
        service,
        store,
        portfolio=portfolio,
        stop_event=stop,
    )


def main() -> int:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("canonical portfolio requires durable evidence persistence")
    return asyncio.run(run_lightweight_portfolio_worker(store, settings=settings))


if __name__ == "__main__":
    raise SystemExit(main())
