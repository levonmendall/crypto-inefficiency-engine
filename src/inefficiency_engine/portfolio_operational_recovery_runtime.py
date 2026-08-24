from __future__ import annotations

from typing import Any, Awaitable, Callable

from sqlalchemy.exc import OperationalError


_PATCH_MARKER = "_cie_portfolio_operational_recovery_installed"
_ORIGINAL_ATTR = "_cie_original_run_cycle_before_operational_recovery"


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
