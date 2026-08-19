from __future__ import annotations

import asyncio
import threading
import time

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, build_evidence_store
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.worker import WorkerRunStats
from inefficiency_engine.worker_children import run_portfolio_child, run_research_child


RESEARCH_THREAD_WORKER_ID = "shadow-research-thread"


def _research_thread_entry() -> None:
    """Run broad shadow research on a daemon thread with its own event loop.

    The thread rebuilds its service/persistence objects from environment settings
    rather than sharing loop-bound clients with the canonical portfolio runtime.
    If research blocks synchronously, only this daemon thread is affected; the
    main-thread portfolio loop and its timeouts continue to run.
    """

    while True:
        settings = Settings.from_env()
        store = build_evidence_store(settings.evidence_db_path)
        if store is None:
            time.sleep(5.0)
            continue
        service = OpportunityService(settings=settings, evidence_store=store)
        try:
            asyncio.run(run_research_child(service, store))
        except BaseException as exc:
            try:
                store.record_worker_heartbeat(
                    worker_id=RESEARCH_THREAD_WORKER_ID,
                    state="error",
                    error_type=type(exc).__name__,
                    detail={
                        "message": str(exc)[:500],
                        "thread_isolated": True,
                        "paper_only": True,
                    },
                )
            except Exception:
                pass
            time.sleep(max(1.0, float(settings.worker_error_backoff_seconds)))


async def run_threaded_worker(service: OpportunityService, store: EvidenceStore) -> WorkerRunStats:
    """Run one Python process with portfolio and research on separate event loops.

    Canonical portfolio accounting stays on the main thread so signal handling and
    portfolio/certification deadlines remain authoritative. Broad research runs on
    a daemon thread with its own asyncio loop. This avoids the multi-process memory
    footprint while preventing synchronous research/provider work from starving
    the canonical portfolio event loop.
    """

    research_thread = threading.Thread(
        target=_research_thread_entry,
        name="shadow-research-thread",
        daemon=True,
    )
    research_thread.start()

    attempted = await run_portfolio_child(service, store)
    return WorkerRunStats(
        worker_id="canonical-portfolio-main-thread",
        cycles_attempted=attempted,
        cycles_succeeded=attempted,
        cycles_failed=0,
    )
