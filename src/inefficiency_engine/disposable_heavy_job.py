from __future__ import annotations

import argparse
import asyncio
import os
import uuid

from inefficiency_engine.config import Settings
from inefficiency_engine.disposable_research_worker import (
    RESEARCH_WORKER_ID,
    run_disposable_research_cycle,
)
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.heavy_work_lease import HeavyWorkLeaseLedger, HeavyWorkLeaseUnavailable
from inefficiency_engine.history_batch_job import maintain_history_batch_once
from inefficiency_engine.instance_memory import instance_memory_snapshot
from inefficiency_engine.service import OpportunityService


HEAVY_WORKER_ID = "disposable-heavy-work"
TEMPORARY_ADMISSION_EXIT_CODE = 75


def _research_completion_state(store) -> tuple[str, str | None, dict[str, object]]:
    """Propagate the research worker's truthful subsystem state one level upward."""
    try:
        heartbeat = store.latest_worker_heartbeat(RESEARCH_WORKER_ID)
    except Exception:
        heartbeat = None
    if heartbeat is None:
        return (
            "degraded",
            "ResearchHeartbeatUnavailable",
            {"research_worker_state": "unavailable"},
        )
    state = str(getattr(heartbeat, "state", "") or "unknown")
    error_type = getattr(heartbeat, "error_type", None)
    detail = getattr(heartbeat, "detail", {}) or {}
    propagated = {
        "research_worker_state": state,
        "research_worker_error_type": error_type,
        "research_subsystem_error_count": int(detail.get("subsystem_error_count") or 0)
        if isinstance(detail, dict)
        else 0,
        "research_subsystem_error_keys": list(detail.get("subsystem_error_keys") or [])
        if isinstance(detail, dict)
        else [],
    }
    if state in {"degraded", "error", "stopped"}:
        return "degraded", str(error_type or "ResearchSubsystemDegraded"), propagated
    return "success", None, propagated


def child_memory_admission_reason(memory) -> str | None:
    """Reject only when the already-started child is at the hard terminate boundary.

    The combined supervisor is the pre-spawn admission authority and deliberately
    checks ``start_blocked`` before creating this process. Re-applying that same
    threshold after Python/import startup double-counts the child's own bootstrap
    footprint and can create an infinite spawn -> code 75 -> retry loop. Once this
    child exists, the only safe local admission failure is the harder aggregate
    terminate boundary; the supervisor continues monitoring that boundary while the
    job runs.
    """

    if getattr(memory, "terminate_required", False):
        return "InstanceMemoryTerminateBlocked"
    return None


async def _run(job: str) -> int:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("disposable heavy work requires durable evidence persistence")

    owner = f"{job}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    lease = HeavyWorkLeaseLedger(store)
    try:
        context = lease.lease(owner)
        context.__enter__()
    except HeavyWorkLeaseUnavailable as exc:
        store.record_worker_heartbeat(
            worker_id=HEAVY_WORKER_ID,
            state="degraded",
            error_type=type(exc).__name__,
            detail={
                "job": job,
                "owner": owner,
                "lease_acquired": False,
                "temporary_admission_failure": True,
                "paper_only": True,
            },
        )
        return TEMPORARY_ADMISSION_EXIT_CODE

    try:
        before = instance_memory_snapshot()
        memory_block = child_memory_admission_reason(before)
        if memory_block is not None:
            store.record_worker_heartbeat(
                worker_id=HEAVY_WORKER_ID,
                state="degraded",
                error_type=memory_block,
                detail={
                    "job": job,
                    "owner": owner,
                    "lease_acquired": True,
                    "memory": before.as_dict(),
                    "temporary_admission_failure": True,
                    "pre_spawn_admission_owned_by_supervisor": True,
                    "paper_only": True,
                },
            )
            return TEMPORARY_ADMISSION_EXIT_CODE

        sequence = lease.next_sequence(job)
        store.record_worker_heartbeat(
            worker_id=HEAVY_WORKER_ID,
            state="running",
            detail={
                "job": job,
                "sequence": sequence,
                "owner": owner,
                "lease_acquired": True,
                "memory_before": before.as_dict(),
                "memory_start_blocked_after_import": bool(before.start_blocked),
                "pre_spawn_admission_owned_by_supervisor": True,
                "disposable_process": True,
                "paper_only": True,
            },
        )

        completion_state = "success"
        completion_error_type: str | None = None
        propagated_detail: dict[str, object] = {}
        if job == "research":
            service = OpportunityService(settings=settings, evidence_store=store)
            result = await run_disposable_research_cycle(
                service,
                store,
                sequence=sequence,
            )
            completion_state, completion_error_type, propagated_detail = (
                _research_completion_state(store)
            )
            result_detail: dict[str, object] = {
                "cycles_attempted": result.cycles_attempted,
                "cycles_succeeded": result.cycles_succeeded,
                "cycles_failed": result.cycles_failed,
                **propagated_detail,
            }
        elif job == "history":
            payload = await maintain_history_batch_once(store, settings=settings)
            result_detail = {
                "batch_size": payload.get("batch_size", 0),
                "batch_assets": payload.get("batch_assets", []),
                "asset_count": payload.get("asset_count", 0),
                "complete_asset_count": payload.get("complete_asset_count", 0),
                "overall_coverage_fraction": payload.get("overall_coverage_fraction", 0.0),
            }
        else:
            raise ValueError(f"unknown disposable job: {job}")

        after = instance_memory_snapshot()
        store.record_worker_heartbeat(
            worker_id=HEAVY_WORKER_ID,
            state=completion_state,
            error_type=completion_error_type,
            detail={
                "job": job,
                "sequence": sequence,
                "owner": owner,
                "lease_acquired": True,
                "memory_before": before.as_dict(),
                "memory_after": after.as_dict(),
                "process_exit_reclaims_heap": True,
                "disposable_process": True,
                "paper_only": True,
                **result_detail,
            },
        )
        # A degraded research cycle is deliberately visible in durable telemetry but
        # is not process-fatal: the supervisor must keep scheduling independent
        # disposable cycles so transient providers can recover automatically.
        return 0
    except Exception as exc:
        try:
            store.record_worker_heartbeat(
                worker_id=HEAVY_WORKER_ID,
                state="error",
                error_type=type(exc).__name__,
                detail={
                    "job": job,
                    "owner": owner,
                    "message": str(exc)[:500],
                    "memory": instance_memory_snapshot().as_dict(),
                    "disposable_process": True,
                    "paper_only": True,
                },
            )
        except Exception:
            pass
        return 1
    finally:
        context.__exit__(None, None, None)


def main() -> int:
    parser = argparse.ArgumentParser(prog="cie-heavy")
    parser.add_argument("job", choices=["research", "history"])
    args = parser.parse_args()
    return asyncio.run(_run(args.job))


if __name__ == "__main__":
    raise SystemExit(main())
