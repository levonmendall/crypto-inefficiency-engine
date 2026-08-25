from __future__ import annotations

import random
from typing import Any

from sqlalchemy.exc import OperationalError


_PATCH_MARKER = "_cie_portfolio_operational_recovery_installed"
_ORIGINAL_ATTR = "_cie_original_run_cycle_before_operational_recovery"
_RETRYABLE_SQLSTATES = {"57P03"}
_RETRYABLE_MESSAGE_MARKERS = (
    "database system is not yet accepting connections",
    "database system is starting up",
    "database system is in recovery mode",
    "consistent recovery state has not been yet reached",
    "cannot connect now",
    "server closed the connection unexpectedly",
    "connection reset",
    "connection refused",
    "connection terminated unexpectedly",
)


def _postgres_sqlstate(exc: Exception) -> str | None:
    original = getattr(exc, "orig", None)
    for candidate in (original, getattr(original, "diag", None)):
        if candidate is None:
            continue
        for attr in ("sqlstate", "pgcode"):
            value = getattr(candidate, attr, None)
            if isinstance(value, str) and value:
                return value.upper()
    return None


def is_retryable_postgres_operational_error(exc: Exception) -> bool:
    """Identify connection/recovery failures that are safe to retry fail-closed.

    PostgreSQL SQLSTATE class 08 is a connection exception and 57P03 is
    ``cannot_connect_now`` (including startup/recovery windows). If a driver exposes
    a different explicit SQLSTATE, do not override it by fuzzy message matching.
    Older/driver-specific exceptions without SQLSTATE fall back to a deliberately
    small set of connection/recovery messages observed from PostgreSQL/psycopg.
    """

    if not isinstance(exc, OperationalError):
        return False
    sqlstate = _postgres_sqlstate(exc)
    if sqlstate is not None:
        return sqlstate.startswith("08") or sqlstate in _RETRYABLE_SQLSTATES
    message = str(exc).lower()
    return any(marker in message for marker in _RETRYABLE_MESSAGE_MARKERS)


def operational_recovery_delay_seconds(
    attempt: int,
    *,
    base_seconds: float = 1.0,
    maximum_seconds: float = 30.0,
    jitter_fraction: float = 0.2,
    random_value: float | None = None,
) -> float:
    """Return capped exponential backoff with bounded positive jitter."""

    attempt = max(0, min(int(attempt), 20))
    base_seconds = max(0.0, float(base_seconds))
    maximum_seconds = max(base_seconds, float(maximum_seconds))
    jitter_fraction = max(0.0, min(float(jitter_fraction), 1.0))
    exponential = min(maximum_seconds, base_seconds * (2**attempt))
    sample = random.random() if random_value is None else float(random_value)
    sample = max(0.0, min(sample, 1.0))
    jitter = exponential * jitter_fraction * sample
    return min(maximum_seconds, exponential + jitter)


def recycle_pool_after_operational_error(store: Any, exc: Exception) -> bool:
    """Invalidate pooled DB connections only for a genuine SQLAlchemy OperationalError.

    The failed portfolio cycle remains failed/fail-closed and is not replayed here.
    Disposing the pool only ensures the next normal canonical cycle checks out a fresh
    PostgreSQL connection rather than inheriting a broken connection after a transient
    network/backend interruption.
    """

    if not isinstance(exc, OperationalError):
        return False
    engine = getattr(store, "engine", None)
    dispose = getattr(engine, "dispose", None)
    if not callable(dispose):
        return False
    try:
        dispose()
    except Exception:
        return False
    return True


def install_portfolio_operational_recovery_runtime(portfolio_class: type[Any]) -> None:
    """Wrap the active canonical portfolio cycle without changing allocation semantics."""

    if bool(getattr(portfolio_class, _PATCH_MARKER, False)):
        return
    original = getattr(portfolio_class, "run_cycle")
    if not callable(original):
        raise TypeError("portfolio class run_cycle is not callable")

    async def recovered_run_cycle(self: Any):
        try:
            return await original(self)
        except OperationalError as exc:
            recycled = recycle_pool_after_operational_error(self.store, exc)
            try:
                self.store.record_worker_heartbeat(
                    worker_id="canonical-portfolio-db-recovery",
                    state="success" if recycled else "degraded",
                    error_type=type(exc).__name__,
                    detail={
                        "stage": "operational_error_pool_recycle",
                        "pool_recycled": recycled,
                        "cycle_replayed": False,
                        "next_cycle_uses_normal_cadence": True,
                        "qualification_thresholds_unchanged": True,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
                    },
                )
            except Exception:
                pass
            raise

    setattr(portfolio_class, _ORIGINAL_ATTR, original)
    portfolio_class.run_cycle = recovered_run_cycle
    setattr(portfolio_class, _PATCH_MARKER, True)
