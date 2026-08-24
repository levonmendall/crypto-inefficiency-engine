from __future__ import annotations

import signal
import threading
from contextlib import contextmanager
from typing import Iterator


class ControlCycleDeadlineExceeded(TimeoutError):
    """Raised when one synchronous canonical-control cycle exceeds its wall-clock budget."""


def hard_control_deadline_supported() -> bool:
    """Return whether this process can enforce a wall-clock deadline on the main thread."""

    return bool(
        threading.current_thread() is threading.main_thread()
        and hasattr(signal, "SIGALRM")
        and hasattr(signal, "ITIMER_REAL")
        and hasattr(signal, "setitimer")
    )


@contextmanager
def hard_control_cycle_deadline(seconds: float) -> Iterator[bool]:
    """Interrupt synchronous control work without leaving an executor thread behind.

    Canonical control is a dedicated Linux process in production. Keeping durable
    reconciliation on that process's main thread lets a process-local real-time alarm
    interrupt Python work, while PostgreSQL ``statement_timeout`` remains the primary
    bound for SQL already executing in the database. Unlike ``asyncio.to_thread``
    cancellation, no reconciliation thread survives after this context exits.
    """

    budget = max(0.001, float(seconds))
    if not hard_control_deadline_supported():
        yield False
        return

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_delay, previous_interval = signal.getitimer(signal.ITIMER_REAL)

    def _raise_deadline(_signum, _frame) -> None:
        raise ControlCycleDeadlineExceeded(
            f"canonical control cycle exceeded {budget:.3f}s wall-clock deadline"
        )

    signal.signal(signal.SIGALRM, _raise_deadline)
    signal.setitimer(signal.ITIMER_REAL, budget)
    try:
        yield True
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_delay > 0.0:
            signal.setitimer(signal.ITIMER_REAL, previous_delay, previous_interval)


def install_control_pool_checkout_timeout(store, *, timeout_seconds: float) -> bool:
    """Bound PostgreSQL QueuePool waits below the canonical-control cycle deadline.

    PostgreSQL statement/lock timeouts begin only after a connection is checked out.
    A saturated SQLAlchemy QueuePool can otherwise wait longer than the whole control
    budget. This process-local adjustment does not alter other workers or database
    semantics; it only bounds how long canonical control waits for a connection.
    """

    engine = store.engine
    if str(getattr(engine.dialect, "name", "")) != "postgresql":
        return False
    pool = getattr(engine, "pool", None)
    if pool is None or not hasattr(pool, "_timeout"):
        return False
    pool._timeout = max(0.25, float(timeout_seconds))
    return True
