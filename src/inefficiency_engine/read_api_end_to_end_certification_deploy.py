from __future__ import annotations

from datetime import datetime, timezone

from fastapi import HTTPException

from inefficiency_engine import read_api_active_volume_deploy as active
from inefficiency_engine import read_api_lane_history_ui_deploy as inner
from inefficiency_engine.critical_evidence_recovery import (
    DEFAULT_ALPHA_FORWARD_RECOVERY_STALE_SECONDS,
    _alpha_forward_status,
)


app = inner.app


def _worker(workers: object, name: str) -> dict[str, object]:
    if not isinstance(workers, dict):
        return {}
    row = workers.get(name)
    return dict(row) if isinstance(row, dict) else {}


def _fresh_worker(row: dict[str, object], *, allowed_states: set[str]) -> bool:
    return bool(
        row.get("available")
        and not row.get("stale")
        and str(row.get("state") or "") in allowed_states
    )


def end_to_end_certification_payload() -> dict[str, object]:
    """Return a fail-closed production certification from durable runtime truth.

    Certification proves that the paper pipeline can move truthful evidence through
    every authority boundary. It deliberately does *not* require a profitable candidate,
    allocation, position or trade: correct economic/statistical rejection is a healthy
    end-to-end result. Historical or presentation data can never create qualification.
    """

    try:
        ready = dict(active.deployment_readiness())
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "end-to-end certification readiness is unavailable",
                "error_type": type(exc).__name__,
            },
        ) from exc

    runtime = ready.get("runtime_heartbeats")
    workers = runtime.get("workers") if isinstance(runtime, dict) else {}
    control = _worker(workers, "canonical_control")
    portfolio = _worker(workers, "portfolio")
    source = _worker(workers, "permanent_source")
    mechanism = _worker(workers, "mechanism_forward")
    source_snapshot = _worker(workers, "source_coverage_snapshot")
    research_projection = _worker(workers, "research_projection")
    index_maintenance = _worker(workers, "runtime_index_maintenance")

    store = active._store()  # noqa: SLF001 - production read-plane composition
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    alpha_forward = _alpha_forward_status(
        store,
        now=datetime.now(timezone.utc),
        stale_after_seconds=DEFAULT_ALPHA_FORWARD_RECOVERY_STALE_SECONDS,
    )

    checks = {
        "database_ready": bool(ready.get("database_ok")),
        "release_identity_available": bool(ready.get("release_commit")),
        "paper_only": bool(ready.get("paper_only")) and not bool(ready.get("live_execution")),
        "source_worker_current": _fresh_worker(
            source,
            allowed_states={"starting", "running", "success"},
        ),
        "thirteen_lane_source_snapshot_current": bool(
            source_snapshot.get("available")
            and not source_snapshot.get("handoff_stale")
            and not source_snapshot.get("stale")
            and source_snapshot.get("persisted_complete_snapshot") is True
            and int(source_snapshot.get("lane_count") or 0) == 13
        ),
        "structural_forward_worker_current": _fresh_worker(
            mechanism,
            allowed_states={"running", "success"},
        ),
        "alpha_forward_cycle_current": bool(
            alpha_forward.get("available")
            and not alpha_forward.get("recovery_required")
        ),
        "cycle_history_serving_target_certified": bool(
            control.get("cycle_history_cache_complete")
            or control.get("historical_cache_complete")
        ),
        "canonical_control_current": _fresh_worker(
            control,
            allowed_states={"success"},
        ),
        "operating_reconciliation_complete": bool(
            control.get("operating_reconciliation_complete")
        ),
        "qualified_bridge_publication_complete": bool(
            control.get("qualified_bridge_publication_complete")
        ),
        "research_projection_current": _fresh_worker(
            research_projection,
            allowed_states={"success"},
        ),
        "paper_portfolio_worker_current": _fresh_worker(
            portfolio,
            allowed_states={"running", "success"},
        ),
    }
    blockers = [name for name, passed in checks.items() if not passed]
    certified = not blockers

    index_advisory = {
        "available": bool(index_maintenance.get("available")),
        "state": index_maintenance.get("state"),
        "error_type": index_maintenance.get("error_type"),
        "background_indexes_complete": index_maintenance.get("background_indexes_complete"),
        "control_gate_released": index_maintenance.get("control_gate_released"),
        "certification_authority": False,
    }

    return {
        "certified": certified,
        "status": "certified" if certified else "blocked",
        "release_commit": ready.get("release_commit"),
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
        "blockers": blockers,
        "alpha_forward": alpha_forward,
        "source_coverage": {
            "lane_count": source_snapshot.get("lane_count"),
            "sufficient_lane_count": source_snapshot.get("sufficient_lane_count"),
            "forward_test_eligible_lane_count": source_snapshot.get(
                "forward_test_eligible_lane_count"
            ),
            "allocation_source_qualified_lane_count": source_snapshot.get(
                "allocation_source_qualified_lane_count"
            ),
            "all_lanes_required_to_be_profitable": False,
            "all_lanes_required_to_be_source_sufficient": False,
            "fail_closed_lane_gaps_allowed": True,
        },
        "control": {
            "state": control.get("state"),
            "error_type": control.get("error_type"),
            "cycle_history_cache_complete": control.get("cycle_history_cache_complete"),
            "cycle_history_cache_progress": control.get("cycle_history_cache_progress"),
            "operating_reconciliation_complete": control.get(
                "operating_reconciliation_complete"
            ),
            "qualified_bridge_publication_complete": control.get(
                "qualified_bridge_publication_complete"
            ),
        },
        "runtime_index_maintenance": index_advisory,
        "trade_required_for_certification": False,
        "positive_candidate_required_for_certification": False,
        "economic_rejection_is_valid": True,
        "qualification_thresholds_unchanged": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }


@app.get("/v3/operations/end-to-end-certification")
def end_to_end_certification():
    return end_to_end_certification_payload()


__all__ = ["app", "end_to_end_certification", "end_to_end_certification_payload"]
