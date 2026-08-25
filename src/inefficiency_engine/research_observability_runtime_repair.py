from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import select

from inefficiency_engine import disposable_heavy_job as heavy_job
from inefficiency_engine import disposable_research_worker as research_worker
from inefficiency_engine.candidate_observatory_runtime import (
    CandidateObservedAllLaneEvidenceFactoryService,
)
from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService
from inefficiency_engine.operating_certification import OperatingCertificationLedger
from inefficiency_engine.research_closure_worker import (
    RESEARCH_CLOSURE_WORKER_ID,
    ResearchClosureCycleSummary,
    ResearchClosureSummaryLedger,
)


RESEARCH_CLOSURE_RECOVERY_STALE_SECONDS = 1_800.0
RESEARCH_OBSERVABILITY_REPAIR_WORKER_ID = "research-observability-runtime-repair"

_ORIGINAL_RESEARCH_CYCLE = research_worker.run_disposable_research_cycle
_ORIGINAL_RESEARCH_CLOSURE = research_worker.run_research_closure_cycle


class ObservableDisposableExpandedAlphaFactoryService(
    DisposableExpandedAlphaFactoryService,
    CandidateObservedAllLaneEvidenceFactoryService,
):
    """Combine the production disposable factory with candidate observatory persistence.

    ``DisposableExpandedAlphaFactoryService`` remains first in the MRO so its bounded
    source reuse and permanent-mechanism delegation stay intact. Its ``super()`` calls
    then flow through ``CandidateObservedAllLaneEvidenceFactoryService`` before the
    unchanged all-lane implementation, which makes observatory recording part of the
    actual production research path without changing any qualification or allocation
    threshold.
    """


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _latest_closure_observed_at(store) -> datetime | None:
    ledger = ResearchClosureSummaryLedger(store)
    with store.engine.connect() as db:
        raw = db.execute(
            select(ledger.table.c.payload_json)
            .order_by(ledger.table.c.id.desc())
            .limit(1)
        ).scalar_one_or_none()
    if raw is None:
        return None
    return _utc(ResearchClosureCycleSummary.model_validate_json(raw).observed_at)


def closure_recovery_required(
    store,
    *,
    now: datetime | None = None,
    stale_seconds: float = RESEARCH_CLOSURE_RECOVERY_STALE_SECONDS,
) -> bool:
    observed_at = _latest_closure_observed_at(store)
    if observed_at is None:
        return True
    current = _utc(now or datetime.now(timezone.utc))
    return max(0.0, (current - observed_at).total_seconds()) > max(1.0, float(stale_seconds))


def _without_mixed_microstructure_funnel(
    summary: ResearchClosureCycleSummary,
) -> ResearchClosureCycleSummary:
    """Keep structural closure from borrowing microstructure counts from another cycle.

    The structural closure scan owns price/carry reconstruction and order-book counts.
    The alpha factory owns the microstructure candidate funnel. Until the same-cycle
    alpha projection is appended, omission is more truthful than joining a current
    scan's raw book count to an unrelated operating snapshot's emitted-candidate count.
    """

    funnels = dict(summary.rejection_funnels)
    if "microstructure" not in funnels:
        return summary
    funnels.pop("microstructure", None)
    return summary.model_copy(
        deep=True,
        update={
            "summary_id": uuid.uuid4().hex,
            "rejection_funnels": funnels,
        },
    )


async def run_truthful_research_closure(*args, **kwargs):
    summary = await _ORIGINAL_RESEARCH_CLOSURE(*args, **kwargs)
    if summary is None:
        return None
    corrected = _without_mixed_microstructure_funnel(summary)
    if corrected.summary_id == summary.summary_id:
        return summary

    store = kwargs.get("store")
    if store is None and len(args) >= 2:
        store = args[1]
    if store is None:
        return corrected

    ResearchClosureSummaryLedger(store).record(corrected)
    try:
        store.record_worker_heartbeat(
            worker_id=RESEARCH_CLOSURE_WORKER_ID,
            state="degraded" if corrected.diagnostic_errors else "success",
            error_type=next(iter(corrected.diagnostic_errors.values()), None),
            detail={
                "source_scan_id": corrected.source_scan_id,
                "summary_id": corrected.summary_id,
                "summary_observed_at": corrected.observed_at.isoformat(),
                "summary_recorded": True,
                "microstructure_funnel_source": "same_cycle_alpha_projection_only",
                "cross_cycle_microstructure_join_allowed": False,
                "paper_only": True,
                "live_execution_authority": False,
            },
        )
    except Exception:
        pass
    return corrected


async def run_research_cycle_with_observability_repair(
    service,
    store,
    *,
    sequence: int,
):
    """Run the normal research cycle, then independently recover stale closure truth."""

    result = await _ORIGINAL_RESEARCH_CYCLE(service, store, sequence=sequence)
    try:
        needs_recovery = closure_recovery_required(store)
    except Exception as exc:
        needs_recovery = True
        try:
            store.record_worker_heartbeat(
                worker_id=RESEARCH_OBSERVABILITY_REPAIR_WORKER_ID,
                state="degraded",
                error_type=type(exc).__name__,
                detail={
                    "stage": "closure_freshness_read",
                    "paper_only": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                },
            )
        except Exception:
            pass

    if not needs_recovery:
        return result

    try:
        operating = SimpleNamespace(ledger=OperatingCertificationLedger(store))
        await run_truthful_research_closure(
            service=service,
            store=store,
            alpha_factory=object(),
            operating_certification=operating,
            total_capital_usd=float(service.settings.alpha_research_capital_usd),
        )
        store.record_worker_heartbeat(
            worker_id=RESEARCH_OBSERVABILITY_REPAIR_WORKER_ID,
            state="success",
            detail={
                "stage": "closure_recovered",
                "closure_stale_seconds": RESEARCH_CLOSURE_RECOVERY_STALE_SECONDS,
                "certification_success_required_for_closure": False,
                "qualification_thresholds_unchanged": True,
                "allocation_authority": False,
                "paper_only": True,
                "live_execution_authority": False,
            },
        )
    except Exception as exc:
        try:
            store.record_worker_heartbeat(
                worker_id=RESEARCH_OBSERVABILITY_REPAIR_WORKER_ID,
                state="degraded",
                error_type=type(exc).__name__,
                detail={
                    "stage": "closure_recovery",
                    "certification_success_required_for_closure": False,
                    "qualification_thresholds_unchanged": True,
                    "allocation_authority": False,
                    "paper_only": True,
                    "live_execution_authority": False,
                },
            )
        except Exception:
            pass
    return result


def install_research_observability_runtime_repair() -> None:
    """Install process-local research wiring before the disposable job starts."""

    research_worker.DisposableExpandedAlphaFactoryService = (
        ObservableDisposableExpandedAlphaFactoryService
    )
    research_worker.run_research_closure_cycle = run_truthful_research_closure
    heavy_job.run_disposable_research_cycle = run_research_cycle_with_observability_repair
