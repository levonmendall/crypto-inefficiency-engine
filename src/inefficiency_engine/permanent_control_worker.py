from __future__ import annotations

import asyncio
import gc
import os
import time

from inefficiency_engine import __version__
from inefficiency_engine.bounded_strategy_evidence_runtime import (
    install_control_database_timeouts,
)
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.config import Settings
from inefficiency_engine.control_cycle_runtime import (
    ControlExecutorSupervisor,
    install_control_pool_checkout_timeout,
)
from inefficiency_engine.dashboard_projection import ResearchDashboardProjectionLedger
from inefficiency_engine.durable_control_alpha import DurableControlAlphaFactoryService
from inefficiency_engine.durable_control_bridge import (
    DurableControlQualifiedOpportunityBridgePublisher,
)
from inefficiency_engine.durable_control_cache import (
    ensure_durable_control_cache_schema,
)
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityAllLaneOperatingCertificationService as OperatingCertificationService,
    EvidenceVelocityLaneSuccessAllocationForwardCertificationService as AllocationForwardCertificationService,
    EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService as UnifiedPaperAllocatorService,
)
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.universal_service import UniversalOpportunityService


CONTROL_WORKER_ID = "canonical-control-operating-loop"
DEFAULT_CONTROL_INTERVAL_SECONDS = 30.0
DEFAULT_CONTROL_CYCLE_DEADLINE_SECONDS = 25.0
DEFAULT_CONTROL_PARENT_HEARTBEAT_SECONDS = 2.0
DEFAULT_CONTROL_EXECUTOR_TERMINATE_GRACE_SECONDS = 2.0


def _interval_seconds() -> float:
    try:
        value = float(
            os.getenv(
                "CIE_CONTROL_INTERVAL_SECONDS",
                str(DEFAULT_CONTROL_INTERVAL_SECONDS),
            )
        )
    except ValueError:
        value = DEFAULT_CONTROL_INTERVAL_SECONDS
    return max(5.0, value)


def _deadline_seconds() -> float:
    """Bound one database-only control cycle below its normal publication cadence."""

    try:
        value = float(
            os.getenv(
                "CIE_CONTROL_CYCLE_DEADLINE_SECONDS",
                str(DEFAULT_CONTROL_CYCLE_DEADLINE_SECONDS),
            )
        )
    except ValueError:
        value = DEFAULT_CONTROL_CYCLE_DEADLINE_SECONDS
    return max(5.0, value)


def _parent_heartbeat_seconds() -> float:
    try:
        value = float(
            os.getenv(
                "CIE_CONTROL_PARENT_HEARTBEAT_SECONDS",
                str(DEFAULT_CONTROL_PARENT_HEARTBEAT_SECONDS),
            )
        )
    except ValueError:
        value = DEFAULT_CONTROL_PARENT_HEARTBEAT_SECONDS
    return max(0.25, value)


def _executor_terminate_grace_seconds() -> float:
    try:
        value = float(
            os.getenv(
                "CIE_CONTROL_EXECUTOR_TERMINATE_GRACE_SECONDS",
                str(DEFAULT_CONTROL_EXECUTOR_TERMINATE_GRACE_SECONDS),
            )
        )
    except ValueError:
        value = DEFAULT_CONTROL_EXECUTOR_TERMINATE_GRACE_SECONDS
    return max(0.0, value)


def _build_control_services(settings, store):
    """Build the durable control graph without granting acquisition authority."""

    service = OpportunityService(settings=settings, evidence_store=store)
    # This control-specific alpha factory rejects any promotion path that would need
    # a provider request. Current executable cost must come from the persisted source
    # snapshot or the candidate remains fail-closed.
    factory = DurableControlAlphaFactoryService(service, store)
    universal = UniversalOpportunityService(service)
    composite_service = CexDexCompositeEvidenceService(service, universal=universal)
    promotion = CexDexPaperPromotionService(service, composite_service, store)
    unified = UnifiedPaperAllocatorService(service, promotion, factory)
    qualified_bridge = DurableControlQualifiedOpportunityBridgePublisher(
        service,
        store,
        unified,
    )
    allocation_certification = AllocationForwardCertificationService(service, unified, store)
    operating_certification = OperatingCertificationService(
        service,
        store,
        factory,
        allocation_certification,
        version=__version__,
    )
    research_projection = ResearchDashboardProjectionLedger(store)
    return operating_certification, qualified_bridge, research_projection


async def _run() -> None:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("permanent control worker requires durable evidence persistence")

    interval = _interval_seconds()
    deadline = _deadline_seconds()
    # Parent database work is limited to small append-only heartbeat writes. These
    # process-local bounds prevent health publication from inheriting the executor's
    # potentially expensive connection/query behavior.
    parent_statement_timeout_seconds = min(2.0, max(1.0, deadline / 25.0))
    parent_lock_timeout_seconds = min(1.0, parent_statement_timeout_seconds)
    parent_pool_checkout_timeout_seconds = min(1.0, parent_statement_timeout_seconds)
    parent_pool_checkout_timeout_enforced = install_control_pool_checkout_timeout(
        store,
        timeout_seconds=parent_pool_checkout_timeout_seconds,
    )
    parent_database_timeouts_enforced = install_control_database_timeouts(
        store,
        statement_timeout_seconds=parent_statement_timeout_seconds,
        lock_timeout_seconds=parent_lock_timeout_seconds,
    )
    ensure_durable_control_cache_schema(store)
    supervisor = ControlExecutorSupervisor(
        deadline_seconds=deadline,
        heartbeat_interval_seconds=_parent_heartbeat_seconds(),
        terminate_grace_seconds=_executor_terminate_grace_seconds(),
    )
    sequence = 0

    def record_parent_heartbeat(**kwargs) -> bool:
        try:
            store.record_worker_heartbeat(**kwargs)
        except Exception:
            return False
        return True

    while True:
        sequence += 1
        started = time.monotonic()

        def record_running(telemetry: dict[str, object]) -> None:
            record_parent_heartbeat(
                    worker_id=CONTROL_WORKER_ID,
                    state="running",
                    cycle_id=str(telemetry.get("executor_cycle_id") or "") or None,
                    detail={
                        "sequence": sequence,
                        "stage": "reconciliation_executor",
                        **telemetry,
                        "runtime_plane": "canonical-control-parent",
                        "permanent_process": True,
                        "parent_process": True,
                        "provider_requests_allowed": False,
                        "provider_requests_used": 0,
                        "current_execution_cost_source": "persisted_order_books_only",
                        "missing_current_executable_depth_policy": "fail_closed",
                        "cycle_deadline_seconds": deadline,
                        "external_process_deadline_enforced": True,
                        "database_statement_timeout_enforced": (
                            parent_database_timeouts_enforced
                        ),
                        "database_statement_timeout_seconds": (
                            parent_statement_timeout_seconds
                        ),
                        "database_lock_timeout_seconds": parent_lock_timeout_seconds,
                        "database_pool_checkout_timeout_enforced": (
                            parent_pool_checkout_timeout_enforced
                        ),
                        "database_pool_checkout_timeout_seconds": (
                            parent_pool_checkout_timeout_seconds
                        ),
                        "strategy_evidence_read_mode": (
                            "durable_exact_bootstrap_plus_incremental_tail"
                        ),
                        "mechanism_evidence_read_mode": (
                            "durable_exact_history_plus_incremental_tail"
                        ),
                        "reconciliation_executor_threads": 0,
                        "mechanism_forward_dependency": False,
                        "disposable_research_dependency": False,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
                    },
                )

        result = supervisor.run_cycle(
            sequence=sequence,
            heartbeat=record_running,
            environment={"CIE_CONTROL_CACHE_NAMESPACE": "canonical-control"},
        )
        telemetry = result.telemetry()
        if result.ok:
            control = result.payload.get("control")
            control = dict(control) if isinstance(control, dict) else {}
            errors = control.get("control_plane_errors")
            error_map = errors if isinstance(errors, dict) else {}
            state = "degraded" if error_map else "success"
            error_type = next(iter(error_map.values()), None)
            alpha_diagnostics = result.payload.get("alpha_durable_promotion")
            if not isinstance(alpha_diagnostics, dict):
                alpha_diagnostics = {"provider_requests_used": 0}
            record_parent_heartbeat(
                worker_id=CONTROL_WORKER_ID,
                state=state,
                error_type=str(error_type) if error_type else None,
                cycle_id=result.executor_cycle_id,
                detail={
                    "sequence": sequence,
                    **control,
                    **telemetry,
                    "stage": "durable_control_complete",
                    "runtime_plane": "canonical-control-parent",
                    "permanent_process": True,
                    "parent_process": True,
                    "provider_requests_allowed": False,
                    "provider_requests_used": 0,
                    "current_execution_cost_source": "persisted_order_books_only",
                    "missing_current_executable_depth_policy": "fail_closed",
                    "cycle_deadline_seconds": deadline,
                    "cycle_runtime_seconds": result.executor_runtime_seconds,
                    "external_process_deadline_enforced": True,
                    "database_statement_timeout_enforced": (
                        parent_database_timeouts_enforced
                    ),
                    "database_statement_timeout_seconds": (
                        parent_statement_timeout_seconds
                    ),
                    "database_lock_timeout_seconds": parent_lock_timeout_seconds,
                    "database_pool_checkout_timeout_enforced": (
                        parent_pool_checkout_timeout_enforced
                    ),
                    "database_pool_checkout_timeout_seconds": (
                        parent_pool_checkout_timeout_seconds
                    ),
                    "strategy_evidence_read_mode": (
                        "durable_exact_bootstrap_plus_incremental_tail"
                    ),
                    "mechanism_evidence_read_mode": (
                        "durable_exact_history_plus_incremental_tail"
                    ),
                    "reconciliation_executor_threads": 0,
                    "alpha_durable_promotion": alpha_diagnostics,
                    "mechanism_forward_dependency": False,
                    "disposable_research_dependency": False,
                    "qualification_thresholds_unchanged": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        else:
            failure_stage = (
                "reconciliation_executor_timeout"
                if result.error_type == "ControlExecutorDeadlineExceeded"
                else "reconciliation_executor_failed"
            )
            record_parent_heartbeat(
                worker_id=CONTROL_WORKER_ID,
                state="degraded",
                error_type=result.error_type,
                cycle_id=result.executor_cycle_id,
                detail={
                    "sequence": sequence,
                    **telemetry,
                    "stage": failure_stage,
                    "message": (
                        f"control executor failed at {result.executor_last_stage}"
                    ),
                    "control_plane_errors": {
                        "control_executor": result.error_type
                    },
                    "cycle_timeout_recoverable": True,
                    "retrying": True,
                    "runtime_plane": "canonical-control-parent",
                    "permanent_process": True,
                    "parent_process": True,
                    "provider_requests_allowed": False,
                    "provider_requests_used": 0,
                    "current_execution_cost_source": "persisted_order_books_only",
                    "missing_current_executable_depth_policy": "fail_closed",
                    "cycle_deadline_seconds": deadline,
                    "cycle_runtime_seconds": result.executor_runtime_seconds,
                    "external_process_deadline_enforced": True,
                    "database_statement_timeout_enforced": (
                        parent_database_timeouts_enforced
                    ),
                    "database_statement_timeout_seconds": (
                        parent_statement_timeout_seconds
                    ),
                    "database_lock_timeout_seconds": parent_lock_timeout_seconds,
                    "database_pool_checkout_timeout_enforced": (
                        parent_pool_checkout_timeout_enforced
                    ),
                    "database_pool_checkout_timeout_seconds": (
                        parent_pool_checkout_timeout_seconds
                    ),
                    "strategy_evidence_read_mode": (
                        "durable_exact_bootstrap_plus_incremental_tail"
                    ),
                    "mechanism_evidence_read_mode": (
                        "durable_exact_history_plus_incremental_tail"
                    ),
                    "reconciliation_executor_threads": 0,
                    "orphan_reconciliation_processes": 0,
                    "mechanism_forward_dependency": False,
                    "disposable_research_dependency": False,
                    "qualification_thresholds_unchanged": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        sleep_seconds = max(0.25, interval - (time.monotonic() - started))
        gc.collect()
        await asyncio.sleep(sleep_seconds)


def main() -> int:
    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
