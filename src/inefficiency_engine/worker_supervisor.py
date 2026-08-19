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
PORTFOLIO_PROCESS_WATCHDOG_SECONDS = 480.0
PORTFOLIO_PROCESS_STARTUP_GRACE_SECONDS = 180.0
SUPERVISOR_POLL_SECONDS = 15.0
CHILD_RESTART_DELAY_SECONDS = 5.0


def default_worker_child_commands() -> dict[str, list[str]]:
    """Run research and canonical portfolio on different OS event loops."""

    base = [sys.executable, "-m", "inefficiency_engine.cli"]
    return {
        "research": [*base, "research-worker"],
        "portfolio": [*base, "portfolio-worker"],
    }


def record_portfolio_watchdog_fallback(
    store: EvidenceStore,
    *,
    error_type: str = "PortfolioProcessWatchdogTimeout",
) -> None:
    """Persist truthful account liveness after terminating a wedged portfolio child.

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
    store.record_worker_heartbeat(
        worker_id=PORTFOLIO_WORKER_ID,
        state="error",
        error_type=error_type,
        detail={
            "paper_only": True,
            "supervisor_fallback_recorded": True,
            "portfolio_nav_usd": account.nav_usd,
            "portfolio_snapshot_observed_at": now.isoformat(),
        },
    )


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

    asyncio task separation is not sufficient when a dependency performs
    synchronous blocking work. Separate processes provide distinct event loops.
    The parent additionally watches canonical snapshot age and can terminate a
    wedged portfolio child even when that child's event loop cannot run a timeout.
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
    ledger = CanonicalPaperPortfolioLedger(store)
    ledger.ensure_genesis()

    async def launch(name: str) -> None:
        command = commands[name]
        process = await asyncio.create_subprocess_exec(*command)
        processes[name] = process
        started_monotonic[name] = time.monotonic()
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
            "research_pid": processes["research"].pid,
            "portfolio_pid": processes["portfolio"].pid,
        },
    )

    try:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=max(1.0, float(poll_seconds)))
                break
            except TimeoutError:
                pass

            for name in ("research", "portfolio"):
                process = processes[name]
                if process.returncode is None:
                    continue
                print(
                    f"==> supervisor detected {name} worker exit code={process.returncode}; restarting",
                    flush=True,
                )
                if restart_delay_seconds > 0:
                    await asyncio.sleep(restart_delay_seconds)
                await launch(name)

            portfolio_process = processes["portfolio"]
            portfolio_uptime = time.monotonic() - started_monotonic["portfolio"]
            latest = ledger.latest_snapshot()
            snapshot_age = (
                max(0.0, (datetime.now(timezone.utc) - latest.observed_at).total_seconds())
                if latest is not None else float("inf")
            )
            watchdog_due = (
                portfolio_process.returncode is None
                and portfolio_uptime >= max(1.0, float(startup_grace_seconds))
                and snapshot_age > max(1.0, float(portfolio_watchdog_seconds))
            )
            if watchdog_due:
                print(
                    f"==> portfolio watchdog snapshot_age={snapshot_age:.1f}s; terminating wedged child",
                    flush=True,
                )
                await _terminate_process(portfolio_process)
                record_portfolio_watchdog_fallback(store)
                if restart_delay_seconds > 0:
                    await asyncio.sleep(restart_delay_seconds)
                await launch("portfolio")
                latest = ledger.latest_snapshot()
                snapshot_age = (
                    max(0.0, (datetime.now(timezone.utc) - latest.observed_at).total_seconds())
                    if latest is not None else float("inf")
                )

            store.record_worker_heartbeat(
                worker_id=SUPERVISOR_WORKER_ID,
                state="running",
                detail={
                    "paper_only": True,
                    "process_isolation": True,
                    "research_pid": processes["research"].pid,
                    "portfolio_pid": processes["portfolio"].pid,
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
            detail={"paper_only": True, "process_isolation": True},
        )
