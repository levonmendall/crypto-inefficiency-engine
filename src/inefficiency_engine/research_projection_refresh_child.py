from __future__ import annotations

from inefficiency_engine.config import Settings
from inefficiency_engine.dashboard_projection import (
    DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
    ResearchDashboardProjectionLedger,
)
from inefficiency_engine.evidence import build_evidence_store


WORKER_ID = DASHBOARD_RESEARCH_PROJECTION_WORKER_ID


def _publish_kwargs(settings: Settings) -> dict[str, object]:
    return {
        "forward_target": max(1, int(settings.alpha_min_forward_samples)),
        "settled_target": max(
            5,
            int(getattr(settings, "operating_certification_min_settled_trials", 20)),
        ),
        "shadow_horizons_seconds": tuple(
            getattr(settings, "shadow_horizons_seconds", (60.0,)) or (60.0,)
        ),
        "shadow_cycle_interval_seconds": float(settings.shadow_cycle_interval_seconds),
        "alpha_evidence_every_cycles": max(1, int(settings.alpha_evidence_every_cycles)),
        "heartbeat_stale_seconds": float(settings.worker_heartbeat_stale_seconds),
    }


def main() -> int:
    """Publish one presentation-only research projection, then exit completely.

    The long-lived portfolio process retains its existing publisher. This independent
    one-shot path is a liveness backstop: the parent supervisor can terminate the entire
    interpreter if a projection read wedges, so a stuck thread cannot make dashboard
    research freshness silently expire. No provider calls, qualification, allocation or
    execution authority are introduced.
    """

    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        return 2
    try:
        projection = ResearchDashboardProjectionLedger(store)
        payload = projection.publish(**_publish_kwargs(settings))
        store.record_worker_heartbeat(
            worker_id=WORKER_ID,
            state="success",
            detail={
                "projection_observed_at": payload.get("observed_at"),
                "publication_stage": "disposable_persisted_refresh",
                "publication_owner": "research-projection-refresh-supervisor",
                "disposable_process": True,
                "process_exit_reclaims_heap": True,
                "research_computation": False,
                "provider_calls": False,
                "presentation_only": True,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            },
        )
        return 0
    except Exception as exc:
        try:
            store.record_worker_heartbeat(
                worker_id=WORKER_ID,
                state="degraded",
                error_type=type(exc).__name__,
                detail={
                    "publication_stage": "disposable_persisted_refresh",
                    "publication_owner": "research-projection-refresh-supervisor",
                    "message": str(exc)[:500],
                    "retrying": True,
                    "disposable_process": True,
                    "process_exit_reclaims_heap": True,
                    "research_computation": False,
                    "provider_calls": False,
                    "presentation_only": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
