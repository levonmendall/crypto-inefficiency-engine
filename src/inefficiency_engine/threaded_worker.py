from __future__ import annotations

import asyncio
import signal
import threading
import time
from datetime import datetime, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, build_evidence_store
from inefficiency_engine.operating_worker import (
    ALLOCATION_CERTIFICATION_TIMEOUT_SECONDS,
    OPERATING_CERTIFICATION_TIMEOUT_SECONDS,
    PORTFOLIO_STAGE_TIMEOUT_SECONDS,
    PORTFOLIO_WORKER_ID,
)
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.worker import WorkerRunStats
from inefficiency_engine.worker_children import run_portfolio_child, run_research_child
from inefficiency_engine.worker_supervisor import (
    record_portfolio_watchdog_fallback,
    recover_stale_portfolio_on_supervisor_startup,
)


RESEARCH_THREAD_WORKER_ID = "shadow-research-thread"
THREAD_SUPERVISOR_WORKER_ID = "cie-thread-supervisor"
PORTFOLIO_THREAD_NAME = "canonical-portfolio-thread"
RESEARCH_THREAD_NAME = "shadow-research-thread"
THREAD_SUPERVISOR_POLL_SECONDS = 15.0
PORTFOLIO_THREAD_STARTUP_GRACE_SECONDS = 90.0
PORTFOLIO_THREAD_WATCHDOG_BUFFER_SECONDS = 60.0


class PortfolioThreadWatchdogError(RuntimeError):
    """Raised when the canonical portfolio thread can no longer prove liveness."""


def _thread_runtime() -> tuple[Settings, EvidenceStore, OpportunityService]:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError(
            "thread worker requires CIE_DATABASE_URL/DATABASE_URL or CIE_EVIDENCE_DB_PATH"
        )
    return settings, store, OpportunityService(settings=settings, evidence_store=store)


def _record_thread_error(
    store: EvidenceStore | None,
    *,
    worker_id: str,
    exc: BaseException,
    thread_name: str,
) -> None:
    if store is None:
        return
    try:
        store.record_worker_heartbeat(
            worker_id=worker_id,
            state="error",
            error_type=type(exc).__name__,
            detail={
                "message": str(exc)[:500],
                "thread_isolated": True,
                "thread_name": thread_name,
                "paper_only": True,
            },
        )
    except Exception:
        pass


def _research_thread_entry() -> None:
    while True:
        store: EvidenceStore | None = None
        backoff = 5.0
        try:
            settings, store, service = _thread_runtime()
            backoff = max(1.0, float(settings.worker_error_backoff_seconds))
            asyncio.run(run_research_child(service, store))
            raise RuntimeError("research child returned unexpectedly")
        except BaseException as exc:
            _record_thread_error(
                store,
                worker_id=RESEARCH_THREAD_WORKER_ID,
                exc=exc,
                thread_name=RESEARCH_THREAD_NAME,
            )
            time.sleep(backoff)


def _portfolio_thread_entry() -> None:
    while True:
        store: EvidenceStore | None = None
        backoff = 5.0
        try:
            settings, store, service = _thread_runtime()
            backoff = max(1.0, float(settings.worker_error_backoff_seconds))
            asyncio.run(run_portfolio_child(service, store))
            raise RuntimeError("portfolio child returned unexpectedly")
        except BaseException as exc:
            _record_thread_error(
                store,
                worker_id=PORTFOLIO_WORKER_ID,
                exc=exc,
                thread_name=PORTFOLIO_THREAD_NAME,
            )
            time.sleep(backoff)


def _daemon_thread(*, target, name: str) -> threading.Thread:
    return threading.Thread(target=target, name=name, daemon=True)


def _portfolio_cycle_interval_seconds(settings: Settings) -> float:
    return max(60.0, float(settings.shadow_cycle_interval_seconds) * 10.0)


def _portfolio_running_watchdog_seconds() -> float:
    return (
        PORTFOLIO_STAGE_TIMEOUT_SECONDS
        + ALLOCATION_CERTIFICATION_TIMEOUT_SECONDS
        + OPERATING_CERTIFICATION_TIMEOUT_SECONDS
        + PORTFOLIO_THREAD_WATCHDOG_BUFFER_SECONDS
    )


def _portfolio_idle_watchdog_seconds(settings: Settings) -> float:
    return _portfolio_cycle_interval_seconds(settings) + PORTFOLIO_THREAD_WATCHDOG_BUFFER_SECONDS


def _portfolio_watchdog_reason(
    store: EvidenceStore,
    *,
    settings: Settings,
    supervisor_started_at: datetime,
    now: datetime | None = None,
) -> tuple[str | None, float | None, float]:
    """Return a watchdog reason without confusing normal idle cadence for a stall."""

    current = now or datetime.now(timezone.utc)
    heartbeat = store.latest_worker_heartbeat(PORTFOLIO_WORKER_ID)
    startup_age = max(0.0, (current - supervisor_started_at).total_seconds())
    if heartbeat is None or heartbeat.observed_at < supervisor_started_at:
        if startup_age <= PORTFOLIO_THREAD_STARTUP_GRACE_SECONDS:
            return None, None, PORTFOLIO_THREAD_STARTUP_GRACE_SECONDS
        return (
            "canonical portfolio thread did not publish a startup heartbeat",
            None,
            PORTFOLIO_THREAD_STARTUP_GRACE_SECONDS,
        )

    heartbeat_age = max(0.0, (current - heartbeat.observed_at).total_seconds())
    if heartbeat.state == "running":
        timeout = _portfolio_running_watchdog_seconds()
        if heartbeat_age > timeout:
            return (
                "canonical portfolio cycle exceeded its combined bounded stage budget",
                heartbeat_age,
                timeout,
            )
    else:
        timeout = _portfolio_idle_watchdog_seconds(settings)
        if heartbeat_age > timeout:
            return (
                "canonical portfolio thread failed to start the next scheduled cycle",
                heartbeat_age,
                timeout,
            )
    return None, heartbeat_age, timeout


async def run_threaded_worker(
    store: EvidenceStore,
    *,
    settings: Settings | None = None,
    stop_event: asyncio.Event | None = None,
    supervisor_poll_seconds: float = THREAD_SUPERVISOR_POLL_SECONDS,
) -> WorkerRunStats:
    """Run heavy loops on daemon threads while the provider-free main thread supervises.

    Python cannot safely kill a synchronously wedged thread. The portfolio and
    research event loops therefore run as daemon threads. If canonical accounting
    stops advancing, the main thread records a truthful fail-closed snapshot and
    raises so the service manager restarts the one worker process, clearing the
    wedged thread without returning to the previous multi-process memory footprint.
    """

    resolved_settings = settings or Settings.from_env()
    startup_recovered_stale = recover_stale_portfolio_on_supervisor_startup(
        store,
        stale_after_seconds=_portfolio_idle_watchdog_seconds(resolved_settings),
    )
    supervisor_started_at = datetime.now(timezone.utc)

    stop_event = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass

    research_thread = _daemon_thread(
        target=_research_thread_entry,
        name=RESEARCH_THREAD_NAME,
    )
    portfolio_thread = _daemon_thread(
        target=_portfolio_thread_entry,
        name=PORTFOLIO_THREAD_NAME,
    )
    research_thread.start()
    portfolio_thread.start()
    research_restart_count = 0

    store.record_worker_heartbeat(
        worker_id=THREAD_SUPERVISOR_WORKER_ID,
        state="running",
        detail={
            "paper_only": True,
            "one_process": True,
            "main_thread_provider_free": True,
            "research_thread_isolated": True,
            "portfolio_thread_isolated": True,
            "startup_recovered_stale_portfolio": startup_recovered_stale,
            "portfolio_running_watchdog_seconds": _portfolio_running_watchdog_seconds(),
            "portfolio_idle_watchdog_seconds": _portfolio_idle_watchdog_seconds(resolved_settings),
        },
    )

    try:
        while not stop_event.is_set():
            if not portfolio_thread.is_alive():
                record_portfolio_watchdog_fallback(
                    store,
                    error_type="PortfolioThreadUnexpectedExit",
                    detail={"thread_name": PORTFOLIO_THREAD_NAME},
                )
                raise PortfolioThreadWatchdogError(
                    "canonical portfolio thread exited unexpectedly"
                )

            if not research_thread.is_alive():
                research_restart_count += 1
                research_thread = _daemon_thread(
                    target=_research_thread_entry,
                    name=RESEARCH_THREAD_NAME,
                )
                research_thread.start()

            reason, heartbeat_age, watchdog_seconds = _portfolio_watchdog_reason(
                store,
                settings=resolved_settings,
                supervisor_started_at=supervisor_started_at,
            )
            if reason is not None:
                record_portfolio_watchdog_fallback(
                    store,
                    error_type="PortfolioThreadWatchdogTimeout",
                    detail={
                        "thread_name": PORTFOLIO_THREAD_NAME,
                        "watchdog_reason": reason,
                        "portfolio_heartbeat_age_seconds": heartbeat_age,
                        "portfolio_watchdog_seconds": watchdog_seconds,
                    },
                )
                store.record_worker_heartbeat(
                    worker_id=THREAD_SUPERVISOR_WORKER_ID,
                    state="error",
                    error_type="PortfolioThreadWatchdogTimeout",
                    detail={
                        "paper_only": True,
                        "one_process": True,
                        "main_thread_provider_free": True,
                        "watchdog_reason": reason,
                        "portfolio_heartbeat_age_seconds": heartbeat_age,
                        "portfolio_watchdog_seconds": watchdog_seconds,
                        "restart_required": True,
                    },
                )
                raise PortfolioThreadWatchdogError(reason)

            store.record_worker_heartbeat(
                worker_id=THREAD_SUPERVISOR_WORKER_ID,
                state="running",
                detail={
                    "paper_only": True,
                    "one_process": True,
                    "main_thread_provider_free": True,
                    "research_thread_isolated": True,
                    "portfolio_thread_isolated": True,
                    "research_restart_count": research_restart_count,
                    "portfolio_heartbeat_age_seconds": heartbeat_age,
                    "portfolio_watchdog_seconds": watchdog_seconds,
                },
            )
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=max(1.0, float(supervisor_poll_seconds)),
                )
            except TimeoutError:
                pass
    finally:
        if stop_event.is_set():
            store.record_worker_heartbeat(
                worker_id=THREAD_SUPERVISOR_WORKER_ID,
                state="stopped",
                detail={
                    "paper_only": True,
                    "one_process": True,
                    "research_restart_count": research_restart_count,
                },
            )

    return WorkerRunStats(
        worker_id=THREAD_SUPERVISOR_WORKER_ID,
        cycles_attempted=0,
        cycles_succeeded=0,
        cycles_failed=0,
    )
