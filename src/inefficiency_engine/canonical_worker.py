from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from inefficiency_engine.dashboard_projection import (
    DASHBOARD_PROJECTION_WORKER_ID,
    DashboardProjectionLedger,
)
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.operating_worker import PORTFOLIO_STAGE_TIMEOUT_SECONDS, PORTFOLIO_WORKER_ID
from inefficiency_engine.portfolio_integrity import PortfolioIntegritySnapshot
from inefficiency_engine.resilient_paper_portfolio import OperationallyResilientPaperPortfolioService
from inefficiency_engine.service import OpportunityService


async def _interruptible_wait(seconds: float, stop_event: asyncio.Event) -> None:
    if seconds <= 0 or stop_event.is_set():
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except TimeoutError:
        return


def _publish_dashboard_projection(
    service: OpportunityService,
    store: EvidenceStore,
    projection: DashboardProjectionLedger | None,
) -> None:
    """Publish presentation state without making it part of portfolio authority."""
    if projection is None:
        return
    try:
        payload = projection.publish(
            forward_target=max(1, int(getattr(service.settings, "alpha_min_forward_samples", 30))),
            settled_target=max(
                5,
                int(getattr(service.settings, "operating_certification_min_settled_trials", 20)),
            ),
        )
        store.record_worker_heartbeat(
            worker_id=DASHBOARD_PROJECTION_WORKER_ID,
            state="success",
            detail={
                "projection_observed_at": payload.get("observed_at"),
                "source_portfolio_observed_at": payload.get("source_portfolio_observed_at"),
                "presentation_only": True,
                "paper_only": True,
            },
        )
    except Exception as exc:
        try:
            store.record_worker_heartbeat(
                worker_id=DASHBOARD_PROJECTION_WORKER_ID,
                state="error",
                error_type=type(exc).__name__,
                detail={
                    "message": str(exc)[:500],
                    "presentation_only": True,
                    "portfolio_authority_unchanged": True,
                    "paper_only": True,
                },
            )
        except Exception:
            pass


async def run_canonical_portfolio_loop(
    service: OpportunityService,
    store: EvidenceStore,
    *,
    portfolio: OperationallyResilientPaperPortfolioService,
    stop_event: asyncio.Event,
    dashboard_projection: DashboardProjectionLedger | None = None,
    interval_seconds: float | None = None,
    max_cycles: int | None = None,
) -> int:
    """Advance only the canonical account on the liveness-critical portfolio thread.

    Forward allocation and mechanism certification deliberately do not run here.
    A provider-heavy certification stall must never hold the canonical heartbeat
    open long enough for the process watchdog to kill the Render worker. Dashboard
    projection is a bounded, presentation-only pre-cycle and post-cycle publication.
    """

    interval = (
        max(60.0, float(interval_seconds))
        if interval_seconds is not None
        else max(60.0, service.settings.shadow_cycle_interval_seconds * 10.0)
    )
    if dashboard_projection is None and hasattr(store, "engine"):
        dashboard_projection = DashboardProjectionLedger(store)
    portfolio.ledger.ensure_genesis()
    if portfolio.ledger.latest_snapshot() is None:
        portfolio.ledger.record_snapshot(portfolio.ledger.current_state())
    latest_account = portfolio.ledger.latest_snapshot()
    if latest_account is not None:
        portfolio.integrity.ensure_initial(latest_account)

    attempted = 0
    while not stop_event.is_set() and (max_cycles is None or attempted < max_cycles):
        attempted += 1
        store.record_worker_heartbeat(
            worker_id=PORTFOLIO_WORKER_ID,
            state="running",
            detail={
                "cycle_attempt": attempted,
                "portfolio_cycle_interval_seconds": interval,
                "stage": "canonical_accounting_only",
                "certification_decoupled": True,
                "paper_only": True,
            },
        )
        if attempted == 1:
            # Genesis and its cash-only integrity state are already durable. Publish
            # them before the first potentially slow provider-backed portfolio cycle
            # so UI visibility is never coupled to provider latency.
            _publish_dashboard_projection(service, store, dashboard_projection)

        error_type: str | None = None
        cycle = None
        fallback_snapshot_recorded = False
        try:
            cycle = await asyncio.wait_for(
                portfolio.run_cycle(),
                timeout=PORTFOLIO_STAGE_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            error_type = type(exc).__name__
            try:
                previous_integrity = portfolio.integrity.latest()
                fallback = portfolio.ledger.current_state(observed_at=datetime.now(timezone.utc))
                portfolio.ledger.record_snapshot(fallback)
                valuation_status = "cash_only" if fallback.open_position_count == 0 else "stale"
                portfolio.integrity.record(PortfolioIntegritySnapshot(
                    observed_at=fallback.observed_at,
                    account_snapshot_at=fallback.observed_at,
                    market_evidence_at=(
                        previous_integrity.market_evidence_at if previous_integrity is not None else None
                    ),
                    valuation_status=valuation_status,
                    cycle_status="failed",
                    fallback_snapshot=True,
                    cycle_error_type=error_type,
                    stale_position_count=fallback.open_position_count,
                    open_position_count=fallback.open_position_count,
                    allocation_family_failures=(
                        list(previous_integrity.allocation_family_failures)
                        if previous_integrity is not None else []
                    ),
                    market_snapshot_id=(
                        previous_integrity.market_snapshot_id if previous_integrity is not None else None
                    ),
                ))
                fallback_snapshot_recorded = True
            except Exception:
                pass

        latest = portfolio.ledger.latest_snapshot()
        integrity = portfolio.integrity.latest()
        degraded_reason = None
        if error_type is None and integrity is not None and integrity.cycle_status == "degraded":
            degraded_reason = (
                integrity.cycle_error_type
                or ("family_failure" if integrity.allocation_family_failures else None)
                or ("stale_valuation" if integrity.stale_position_count else None)
                or "PortfolioCycleDegraded"
            )
        state = "error" if error_type else ("degraded" if degraded_reason else "success")
        store.record_worker_heartbeat(
            worker_id=PORTFOLIO_WORKER_ID,
            state=state,
            cycle_id=getattr(cycle, "cycle_id", None),
            error_type=error_type or degraded_reason,
            detail={
                "cycle_attempt": attempted,
                "portfolio_cycle_interval_seconds": interval,
                "stage": "canonical_accounting_only",
                "certification_decoupled": True,
                "portfolio_nav_usd": latest.nav_usd if latest is not None else None,
                "portfolio_snapshot_observed_at": (
                    latest.observed_at.isoformat() if latest is not None else None
                ),
                "market_evidence_observed_at": (
                    integrity.market_evidence_at.isoformat()
                    if integrity is not None and integrity.market_evidence_at is not None else None
                ),
                "portfolio_valuation_status": integrity.valuation_status if integrity is not None else None,
                "portfolio_cycle_status": integrity.cycle_status if integrity is not None else None,
                "portfolio_allocation_family_failures": (
                    list(integrity.allocation_family_failures) if integrity is not None else []
                ),
                "fallback_snapshot_recorded": fallback_snapshot_recorded,
                "paper_only": True,
            },
        )

        _publish_dashboard_projection(service, store, dashboard_projection)

        if not stop_event.is_set() and (max_cycles is None or attempted < max_cycles):
            await _interruptible_wait(interval, stop_event)

    store.record_worker_heartbeat(
        worker_id=PORTFOLIO_WORKER_ID,
        state="stopped" if stop_event.is_set() else "completed",
        detail={"cycles_attempted": attempted, "certification_decoupled": True, "paper_only": True},
    )
    return attempted
