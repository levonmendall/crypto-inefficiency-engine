from __future__ import annotations

import asyncio
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Mapping, Sequence

from inefficiency_engine.canonical_paper_portfolio import CanonicalPaperPortfolioLedger
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.operating_worker import PORTFOLIO_WORKER_ID
from inefficiency_engine.portfolio_integrity import PortfolioIntegrityLedger, PortfolioIntegritySnapshot


SUPERVISOR_WORKER_ID = "cie-process-supervisor"
PORTFOLIO_PROCESS_WATCHDOG_SECONDS = 180.0
PORTFOLIO_PROCESS_STARTUP_GRACE_SECONDS = 30.0
SUPERVISOR_POLL_SECONDS = 15.0
CHILD_RESTART_DELAY_SECONDS = 5.0
PORTFOLIO_EXIT_FALLBACK_MIN_AGE_SECONDS = 60.0


def default_worker_child_commands() -> dict[str, list[str]]:
    """Run research and canonical portfolio on different OS event loops."""

    base = [sys.executable, "-m", "inefficiency_engine.cli"]
    return {
        "research": [*base, "research-worker"],
        "portfolio": [*base, "portfolio-worker"],
    }


def _latest_snapshot_age_seconds(ledger: CanonicalPaperPortfolioLedger) -> float:
    latest = ledger.latest_snapshot()
    if latest is None:
        return float("inf")
    return max(0.0, (datetime.now(timezone.utc) - latest.observed_at).total_seconds())


def record_portfolio_watchdog_fallback(
    store: EvidenceStore,
    *,
    error_type: str = "PortfolioProcessWatchdogTimeout",
    detail: dict[str, object] | None = None,
) -> None:
    """Persist truthful account liveness after a portfolio-process failure.

    This intentionally does not invent market marks, trades, or opportunity
    evidence. Cash-only accounts remain exact; accounts with open positions are
    explicitly stale until the restarted portfolio child obtains fresh evidence.
    """

    now = datetime.now(timezone.utc)
    ledger = CanonicalPaperPortfolioLedger(store)
    integrity_ledger = PortfolioIntegrityLedger(store)
    ledger.ensure_genesis(observed_at=now)
    previous_integrity = integrity_ledger.latest()
    account = ledger.current_state(observed_at=now)
    ledger.record_snapshot(account)
    valuation_status = "cash_only" if account.open_position_count == 0 else "stale"
    integrity_ledger.record(PortfolioIntegritySnapshot(
        observed_at=now,
        account_snapshot_at=now,
        market_evidence_at=(
            previous_integrity.market_evidence_at if previous_integrity is not None else None
        ),
        valuation_status=valuation_status,
        cycle_status="failed",
        fallback_snapshot=True,
        cycle_error_type=error_type,
        stale_position_count=account.open_position_count,
        open_position_count=account.open_position_count,
        allocation_family_failures=(
            list(previous_integrity.allocation_family_failures)
            if previous_integrity is not None else []
        ),
        market_snapshot_id=(
            previous_integrity.market_snapshot_id if previous_integrity is not None else None
        ),
    ))
    heartbeat_detail: dict[str, object] = {
        "paper_only": True,
        "supervisor_fallback_recorded": True,
        "portfolio_nav_usd": account.nav_usd,
        "portfolio_snapshot_observed_at": now.isoformat(),
    }
    if detail:
        heartbeat_detail.update(detail)
    store.record_worker_heartbeat(
        worker_id=PORTFOLIO_WORKER_ID,
        state="error",
        error_type=error_type,
        detail=heartbeat_detail,
    )


def recover_stale_portfolio_on_supervisor_startup(
    store: EvidenceStore,
    *,
    stale_after_seconds: float = PORTFOLIO_PROCESS_WATCHDOG_SECONDS,
) -> bool:
    """Immediately make pre-existing stale accounting explicit on worker startup."""

    ledger = CanonicalPaperPortfolioLedger(store)
    ledger.ensure_genesis()
    latest = ledger.latest_snapshot()
    if latest is None:
        ledger.record_snapshot(ledger.current_state())
        return True
    if _latest_snapshot_age_seconds(ledger) <= max(1.0, float(stale_after_seconds)):
        return False
    record_portfolio_watchdog_fallback(
        store,
        error_type="PortfolioSupervisorStartupStaleRecovery",
        detail={"recovery_reason": "snapshot was stale when process supervisor started"},
    )
    return True


def recover_unexpected_portfolio_exit(
    store: EvidenceStore,
    *,
    exit_code: int | None,
    fallback_min_age_seconds: float = PORTFOLIO_EXIT_FALLBACK_MIN_AGE_SECONDS,
) -> bool:
    """Record a bounded fallback for a crash-looping portfolio child.

    The age guard prevents a rapid crash loop from appending a new canonical
    snapshot every few seconds. The first stale exit becomes visible immediately;
    subsequent exits remain observable through the supervisor heartbeat/restarts.
    """

    ledger = CanonicalPaperPortfolioLedger(store)
    ledger.ensure_genesis()
    if _latest_snapshot_age_seconds(ledger) <= max(1.0, float(fallback_min_age_seconds)):
        return False
    record_portfolio_watchdog_fallback(
        store,
        error_type="PortfolioProcessUnexpectedExit",
        detail={"portfolio_child_exit_code": exit_code},
    )
    return True


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=10.0)
    except TimeoutError:
        process.kill()
        await process.wait()


async def supervise_worker_processes(
    store: EvidenceStore,
    *,
    stop_event: asyncio.Event | None = None,
    child_commands: Mapping[str, Sequence[str]] | None = None,
    portfolio_watchdog_seconds: float = PORTFOLIO_PROCESS_WATCHDOG_SECONDS,
    startup_grace_seconds: float = PORTFOLIO_PROCESS_STARTUP_GRACE_SECONDS,
    poll_seconds: float = SUPERVISOR_POLL_SECONDS,
    restart_delay_seconds: float = CHILD_RESTART_DELAY_SECONDS,
) -> None:
    """Supervise research and portfolio workers as independent OS processes.

    Process isolation protects the canonical account from synchronous blocking in
    research. The parent also covers both failure classes a child process can
    exhibit: an alive-but-wedged process and an unexpected/crash-looping exit.
    """

    stop_event = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass

    commands = {
        name: list(command)
        for name, command in (child_commands or default_worker_child_commands()).items()
    }
    required = {"research", "portfolio"}
    if set(commands) != required:
        raise ValueError("worker supervisor requires exactly research and portfolio child commands")

    processes: dict[str, asyncio.subprocess.Process] = {}
    started_monotonic: dict[str, float] = {}
    restart_counts = {"research": 0, "portfolio": 0}
    ledger = CanonicalPaperPortfolioLedger(store)
    ledger.ensure_genesis()

    startup_recovered_stale = recover_stale_portfolio_on_supervisor_startup(
        store,
        stale_after_seconds=portfolio_watchdog_seconds,
    )

    async def launch(name: str, *, restart: bool = False) -> None:
        command = commands[name]
        process = await asyncio.create_subprocess_exec(*command)
        processes[name] = process
        started_monotonic[name] = time.monotonic()
        if restart:
            restart_counts[name] += 1
        print(
            f"==> supervisor launched {name} worker pid={process.pid} command={' '.join(command)}",
            flush=True,
        )

    for child_name in ("research", "portfolio"):
        await launch(child_name)

    store.record_worker_heartbeat(
        worker_id=SUPERVISOR_WORKER_ID,
        state="running",
        detail={
            "paper_only": True,
            "process_isolation": True,
            "startup_recovered_stale_portfolio": startup_recovered_stale,
            "research_pid": processes["research"].pid,
            "portfolio_pid": processes["portfolio"].pid,
            "research_restart_count": restart_counts["research"],
            "portfolio_restart_count": restart_counts["portfolio"],
        },
    )

    try:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=max(1.0, float(poll_seconds)))
                break
            except TimeoutError:
                pass

            portfolio_exit_recovered = False
            for name in ("research", "portfolio"):
                process = processes[name]
                if process.returncode is None:
                    continue
                print(
                    f"==> supervisor detected {name} worker exit code={process.returncode}; restarting",
                    flush=True,
                )
                if name == "portfolio":
                    portfolio_exit_recovered = recover_unexpected_portfolio_exit(
                        store,
                        exit_code=process.returncode,
                    )
                if restart_delay_seconds > 0:
                    await asyncio.sleep(restart_delay_seconds)
                await launch(name, restart=True)

            portfolio_process = processes["portfolio"]
            portfolio_uptime = time.monotonic() - started_monotonic["portfolio"]
            snapshot_age = _latest_snapshot_age_seconds(ledger)
            watchdog_due = (
                portfolio_process.returncode is None
                and portfolio_uptime >= max(1.0, float(startup_grace_seconds))
                and snapshot_age > max(1.0, float(portfolio_watchdog_seconds))
            )
            watchdog_restarted = False
            if watchdog_due:
                print(
                    f"==> portfolio watchdog snapshot_age={snapshot_age:.1f}s; terminating wedged child",
                    flush=True,
                )
                await _terminate_process(portfolio_process)
                record_portfolio_watchdog_fallback(store)
                if restart_delay_seconds > 0:
                    await asyncio.sleep(restart_delay_seconds)
                await launch("portfolio", restart=True)
                watchdog_restarted = True
                snapshot_age = _latest_snapshot_age_seconds(ledger)

            store.record_worker_heartbeat(
                worker_id=SUPERVISOR_WORKER_ID,
                state="degraded" if (portfolio_exit_recovered or watchdog_restarted) else "running",
                error_type=(
                    "PortfolioProcessUnexpectedExit"
                    if portfolio_exit_recovered
                    else ("PortfolioProcessWatchdogTimeout" if watchdog_restarted else None)
                ),
                detail={
                    "paper_only": True,
                    "process_isolation": True,
                    "research_pid": processes["research"].pid,
                    "portfolio_pid": processes["portfolio"].pid,
                    "research_restart_count": restart_counts["research"],
                    "portfolio_restart_count": restart_counts["portfolio"],
                    "portfolio_snapshot_age_seconds": snapshot_age,
                    "portfolio_watchdog_seconds": portfolio_watchdog_seconds,
                },
            )
    finally:
        await asyncio.gather(
            *(_terminate_process(process) for process in processes.values()),
            return_exceptions=True,
        )
        store.record_worker_heartbeat(
            worker_id=SUPERVISOR_WORKER_ID,
            state="stopped",
            detail={
                "paper_only": True,
                "process_isolation": True,
                "research_restart_count": restart_counts["research"],
                "portfolio_restart_count": restart_counts["portfolio"],
            },
        )
