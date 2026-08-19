from __future__ import annotations

import asyncio
import signal
from dataclasses import dataclass
from datetime import datetime, timezone

from inefficiency_engine import __version__
from inefficiency_engine.allocation_certification import AllocationForwardCertificationService
from inefficiency_engine.canonical_paper_portfolio import CanonicalPaperPortfolioService
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.cex_dex_shadow import CexDexCompositeEdgeShadowService
from inefficiency_engine.dex_tier_shadow import DexTierShadowService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.expanded_alpha_factory import ExpandedAlphaFactoryService
from inefficiency_engine.operating_certification import OperatingCertificationService
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.stablecoin_depth_service import StablecoinConversionDepthService
from inefficiency_engine.stablecoin_depth_shadow import StablecoinDepthShadowService
from inefficiency_engine.unified_allocation import UnifiedPaperAllocatorService
from inefficiency_engine.universal_service import UniversalOpportunityService
from inefficiency_engine.worker import WorkerRunStats, run_shadow_worker


PORTFOLIO_WORKER_ID = "canonical-portfolio-operating-loop"


@dataclass(frozen=True)
class OperatingBundleResult:
    allocation_cycle: object | None
    portfolio_cycle: object | None
    operating_cycle: object | None
    errors: dict[str, str]
    nav_usd: float
    fallback_snapshot_recorded: bool = False


async def run_operating_bundle_once(
    *,
    portfolio: CanonicalPaperPortfolioService,
    allocation_certification: AllocationForwardCertificationService,
    operating_certification: OperatingCertificationService,
) -> OperatingBundleResult:
    """Run operating subsystems without allowing one failure to freeze accounting.

    Family/provider degradation is explicit and subtractive: it can remove a
    candidate family from the allocator, but it cannot suppress unrelated
    families or erase truthful accounting. If the portfolio cannot obtain the
    market snapshot needed to value open positions, a fallback snapshot carries
    forward account state while explicitly marking valuation as stale/failed.
    """

    current = portfolio.ledger.current_state()
    capital_usd = max(1.0, current.nav_usd)
    errors: dict[str, str] = {}
    allocation_cycle: object | None = None
    portfolio_cycle: object | None = None
    operating_cycle: object | None = None
    fallback_snapshot_recorded = False

    try:
        allocation_cycle = await allocation_certification.run_cycle(total_capital_usd=capital_usd)
    except Exception as exc:
        errors["allocation_certification_error_type"] = type(exc).__name__

    try:
        portfolio_cycle = await portfolio.run_cycle()
        if bool(getattr(portfolio_cycle, "degraded", False)):
            degraded_reason = (
                getattr(portfolio_cycle, "allocation_error_type", None)
                or ("family_failure" if getattr(portfolio_cycle, "allocation_family_failures", None) else None)
                or ("stale_valuation" if getattr(portfolio_cycle, "stale_position_count", 0) else None)
                or "PortfolioCycleDegraded"
            )
            errors["portfolio_cycle_degraded"] = str(degraded_reason)
    except Exception as exc:
        errors["portfolio_error_type"] = type(exc).__name__
        try:
            previous = portfolio.ledger.latest_snapshot()
            fallback = portfolio.ledger.current_state(observed_at=datetime.now(timezone.utc))
            valuation_status = "cash_only" if fallback.open_position_count == 0 else "stale"
            fallback = fallback.model_copy(update={
                "market_evidence_observed_at": (
                    previous.market_evidence_observed_at if previous is not None else None
                ),
                "valuation_status": valuation_status,
                "cycle_status": "failed",
                "fallback_snapshot": True,
                "cycle_error_type": type(exc).__name__,
                "allocation_family_failures": (
                    dict(previous.allocation_family_failures) if previous is not None else {}
                ),
                "stale_position_count": fallback.open_position_count,
            })
            portfolio.ledger.record_snapshot(fallback)
            fallback_snapshot_recorded = True
        except Exception as snapshot_exc:
            errors["portfolio_fallback_snapshot_error_type"] = type(snapshot_exc).__name__

    latest = portfolio.ledger.latest_snapshot()
    nav_usd = max(1.0, latest.nav_usd if latest is not None else current.nav_usd)

    try:
        operating_cycle = await operating_certification.run_cycle(total_capital_usd=nav_usd)
    except Exception as exc:
        errors["operating_certification_error_type"] = type(exc).__name__

    return OperatingBundleResult(
        allocation_cycle=allocation_cycle,
        portfolio_cycle=portfolio_cycle,
        operating_cycle=operating_cycle,
        errors=errors,
        nav_usd=nav_usd,
        fallback_snapshot_recorded=fallback_snapshot_recorded,
    )


async def _interruptible_wait(seconds: float, stop_event: asyncio.Event) -> None:
    if seconds <= 0 or stop_event.is_set():
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except TimeoutError:
        return


async def run_portfolio_operating_loop(
    service: OpportunityService,
    store: EvidenceStore,
    *,
    portfolio: CanonicalPaperPortfolioService,
    allocation_certification: AllocationForwardCertificationService,
    operating_certification: OperatingCertificationService,
    stop_event: asyncio.Event,
    interval_seconds: float | None = None,
    max_cycles: int | None = None,
) -> int:
    """Advance the canonical portfolio independently of long shadow research cycles."""

    interval = (
        max(60.0, float(interval_seconds))
        if interval_seconds is not None
        else max(60.0, service.settings.shadow_cycle_interval_seconds * 10.0)
    )
    portfolio.ledger.ensure_genesis()
    if portfolio.ledger.latest_snapshot() is None:
        portfolio.ledger.record_snapshot(portfolio.ledger.current_state())

    attempted = 0
    while not stop_event.is_set() and (max_cycles is None or attempted < max_cycles):
        attempted += 1
        store.record_worker_heartbeat(
            worker_id=PORTFOLIO_WORKER_ID,
            state="running",
            detail={
                "cycle_attempt": attempted,
                "portfolio_cycle_interval_seconds": interval,
                "paper_only": True,
            },
        )

        result = await run_operating_bundle_once(
            portfolio=portfolio,
            allocation_certification=allocation_certification,
            operating_certification=operating_certification,
        )
        portfolio_failed = "portfolio_error_type" in result.errors
        state = "error" if portfolio_failed else ("degraded" if result.errors else "success")
        error_type = (
            result.errors.get("portfolio_error_type")
            or result.errors.get("portfolio_cycle_degraded")
            or result.errors.get("allocation_certification_error_type")
            or result.errors.get("operating_certification_error_type")
        )
        latest = portfolio.ledger.latest_snapshot()
        detail: dict[str, object] = {
            "cycle_attempt": attempted,
            "portfolio_cycle_interval_seconds": interval,
            "portfolio_nav_usd": result.nav_usd,
            "portfolio_snapshot_observed_at": (
                latest.observed_at.isoformat() if latest is not None else None
            ),
            "market_evidence_observed_at": (
                latest.market_evidence_observed_at.isoformat()
                if latest is not None and latest.market_evidence_observed_at is not None
                else None
            ),
            "portfolio_valuation_status": latest.valuation_status if latest is not None else None,
            "portfolio_cycle_status": latest.cycle_status if latest is not None else None,
            "portfolio_stale_position_count": latest.stale_position_count if latest is not None else None,
            "portfolio_allocation_family_failures": (
                dict(latest.allocation_family_failures) if latest is not None else {}
            ),
            "fallback_snapshot_recorded": result.fallback_snapshot_recorded,
            "allocation_certification_cycle_id": getattr(result.allocation_cycle, "cycle_id", None),
            "portfolio_cycle_id": getattr(result.portfolio_cycle, "cycle_id", None),
            "operating_certification_cycle_id": getattr(result.operating_cycle, "cycle_id", None),
            "paper_only": True,
        }
        detail.update(result.errors)
        store.record_worker_heartbeat(
            worker_id=PORTFOLIO_WORKER_ID,
            state=state,
            cycle_id=getattr(result.portfolio_cycle, "cycle_id", None),
            error_type=error_type,
            detail=detail,
        )

        if not stop_event.is_set() and (max_cycles is None or attempted < max_cycles):
            await _interruptible_wait(interval, stop_event)

    store.record_worker_heartbeat(
        worker_id=PORTFOLIO_WORKER_ID,
        state="stopped" if stop_event.is_set() else "completed",
        detail={"cycles_attempted": attempted, "paper_only": True},
    )
    return attempted


async def run_forever(service: OpportunityService, store: EvidenceStore) -> WorkerRunStats:
    """Production paper worker with independent portfolio and research loops.

    The canonical account begins exactly once with $250,000. Portfolio state is
    durable and append-only; deploys/restarts recover the existing account rather
    than recreating capital. Unsupported settlement paths remain fail-closed.
    """

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass

    universal = UniversalOpportunityService(service)
    tier_shadow = DexTierShadowService(service, evidence_store=store)
    composite_service = CexDexCompositeEvidenceService(service, universal=universal)
    composite_shadow = CexDexCompositeEdgeShadowService(
        composite_service,
        evidence_store=store,
    )
    stablecoin_shadow = StablecoinDepthShadowService(
        StablecoinConversionDepthService(service.settings),
        evidence_store=store,
    )
    alpha_factory = ExpandedAlphaFactoryService(service, store)
    promotion = CexDexPaperPromotionService(service, composite_service, store)
    unified = UnifiedPaperAllocatorService(service, promotion, alpha_factory)
    allocation_certification = AllocationForwardCertificationService(service, unified, store)
    portfolio = CanonicalPaperPortfolioService(service, unified, store)
    operating_certification = OperatingCertificationService(
        service,
        store,
        alpha_factory,
        allocation_certification,
        version=__version__,
    )

    shadow_task = asyncio.create_task(
        run_shadow_worker(
            service,
            store,
            stop_event=stop_event,
            route_shadow_runner=universal.run_dex_route_shadow_cycle,
            tier_shadow_runner=tier_shadow.run_cycle,
            tier_shadow_every_cycles=service.settings.dex_route_tier_shadow_every_cycles,
            composite_shadow_runner=composite_shadow.run_cycle,
            composite_shadow_every_cycles=10,
            stablecoin_shadow_runner=stablecoin_shadow.run_cycle,
            stablecoin_shadow_every_cycles=10,
            alpha_runner=alpha_factory.run_evidence_cycle,
            alpha_every_cycles=service.settings.alpha_evidence_every_cycles,
            frontier_runner=universal.probe_dex_route_size_frontiers,
            frontier_every_cycles=service.settings.dex_route_frontier_every_cycles,
        ),
        name="shadow-research-worker",
    )
    portfolio_task = asyncio.create_task(
        run_portfolio_operating_loop(
            service,
            store,
            portfolio=portfolio,
            allocation_certification=allocation_certification,
            operating_certification=operating_certification,
            stop_event=stop_event,
        ),
        name="canonical-portfolio-worker",
    )

    shadow_stats, _ = await asyncio.gather(shadow_task, portfolio_task)
    return shadow_stats
