from __future__ import annotations

import asyncio

from inefficiency_engine.dashboard_projection import (
    DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
    ResearchDashboardProjectionLedger,
)


RESEARCH_PROJECTION_MAINTENANCE_SECONDS = 60.0
_PATCH_MARKER = "_cie_research_projection_recovery_installed"


async def resilient_research_projection_refresh_loop(
    store,
    *,
    settings,
    stop_event: asyncio.Event,
) -> None:
    """Keep the persisted research projection retryable after transient DB failures.

    The production portfolio process owns this presentation-only publisher. Ledger
    construction performs schema/existence checks and historically happened outside
    the loop's error boundary, so one transient PostgreSQL failure could terminate the
    task permanently while research itself remained healthy. Construction now occurs
    in a worker thread inside the retry boundary. A failed initialization or publish
    records a degraded heartbeat, discards the ledger handle, and retries on the next
    bounded cadence. No provider, evidence, qualification, allocation, or execution
    authority is introduced here.
    """

    projection = None
    while not stop_event.is_set():
        try:
            if projection is None:
                projection = await asyncio.to_thread(
                    ResearchDashboardProjectionLedger,
                    store,
                )
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
                    "ledger_initialization_retryable": True,
                    "research_computation": False,
                    "provider_calls": False,
                    "presentation_only": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        except Exception as exc:
            # Reconstruct on retry because the failure may have happened during
            # table/schema inspection or connection acquisition rather than publish.
            projection = None
            try:
                store.record_worker_heartbeat(
                    worker_id=DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
                    state="degraded",
                    error_type=type(exc).__name__,
                    detail={
                        "publication_stage": "lightweight_persisted_refresh",
                        "message": str(exc)[:500],
                        "retrying": True,
                        "ledger_initialization_retryable": True,
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


def install_research_projection_recovery_runtime() -> None:
    """Replace only the portfolio process's presentation refresh coroutine."""

    from inefficiency_engine import lightweight_portfolio_worker as worker

    if bool(getattr(worker, _PATCH_MARKER, False)):
        return
    worker._research_projection_refresh_loop = resilient_research_projection_refresh_loop
    setattr(worker, _PATCH_MARKER, True)
