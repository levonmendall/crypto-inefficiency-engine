from __future__ import annotations

import argparse
import asyncio
import os
import uuid

from inefficiency_engine.config import Settings
from inefficiency_engine.disposable_research_worker import run_disposable_research_cycle
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.heavy_work_lease import HeavyWorkLeaseLedger, HeavyWorkLeaseUnavailable
from inefficiency_engine.history_batch_job import maintain_history_batch_once
from inefficiency_engine.instance_memory import instance_memory_snapshot
from inefficiency_engine.service import OpportunityService


HEAVY_WORKER_ID = "disposable-heavy-work"


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
                "paper_only": True,
            },
        )
        return 75

    try:
        before = instance_memory_snapshot()
        if before.start_blocked:
            store.record_worker_heartbeat(
                worker_id=HEAVY_WORKER_ID,
                state="degraded",
                error_type="InstanceMemoryStartBlocked",
                detail={
                    "job": job,
                    "owner": owner,
                    "lease_acquired": True,
                    "memory": before.as_dict(),
                    "paper_only": True,
                },
            )
            return 75

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
                "disposable_process": True,
                "paper_only": True,
            },
        )

        if job == "research":
            service = OpportunityService(settings=settings, evidence_store=store)
            result = await run_disposable_research_cycle(
                service,
                store,
                sequence=sequence,
            )
            result_detail: dict[str, object] = {
                "cycles_attempted": result.cycles_attempted,
                "cycles_succeeded": result.cycles_succeeded,
                "cycles_failed": result.cycles_failed,
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
            state="success",
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
