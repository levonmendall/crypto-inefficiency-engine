from __future__ import annotations

from datetime import datetime, timezone

import inefficiency_engine.read_api_end_to_end_certification_deploy as certification


def _worker(state="success", *, stale=False, **extra):
    return {"available": True, "state": state, "stale": stale, **extra}


def _current_alpha_worker():
    observed_at = datetime.now(timezone.utc).isoformat()
    return _worker(
        "running",
        observed_at=observed_at,
        critical_evidence_recovery={
            "workers": {
                "alpha_forward": {
                    "worker_id": "shadow-research-auxiliary",
                    "signal": "alpha_forward_evidence_cycle_id",
                    "available": True,
                    "observed_at": observed_at,
                    "state": "running",
                    "cycle_id": "alpha-current",
                    "recovery_after_seconds": 1200.0,
                }
            }
        },
    )


def test_background_serving_target_is_visible_without_falsely_certifying_control(monkeypatch):
    progress = {
        "complete": True,
        "serving_scan_id": "scan-certified",
        "incomplete_pair_count": 0,
        "durable_checkpoint_persisted": True,
    }
    ready = {
        "status": "ready",
        "database_ok": True,
        "release_commit": "repair-sha",
        "paper_only": True,
        "live_execution": False,
        "runtime_heartbeats": {
            "workers": {
                "canonical_control": _worker(
                    "degraded",
                    error_type="CycleHistoryCacheRebuilding",
                    cycle_history_cache_complete=False,
                    historical_cache_complete=False,
                    operating_reconciliation_complete=False,
                    qualified_bridge_publication_complete=False,
                ),
                "portfolio": _worker("running"),
                "permanent_source": _worker("running"),
                "mechanism_forward": _worker("success"),
                "research": _current_alpha_worker(),
                "source_coverage_snapshot": _worker(
                    persisted_complete_snapshot=True,
                    lane_count=13,
                    handoff_stale=False,
                    sufficient_lane_count=8,
                    forward_test_eligible_lane_count=9,
                    allocation_source_qualified_lane_count=8,
                ),
                "research_projection": _worker("success"),
                "runtime_index_maintenance": _worker("success"),
                "source_history_migration": _worker(
                    "success",
                    stage="canonical_history_ready",
                    complete=True,
                    compact_certification_summary=True,
                    checkpoint_heartbeat_id=100,
                    lane_count=13,
                    snapshot_count=1300,
                ),
                "cycle_history_backfill": _worker(
                    "success",
                    stage="certified_target_available",
                    cache_complete=True,
                    first_certified_target_pending=False,
                    progress=progress,
                ),
            }
        },
    }

    monkeypatch.setattr(certification.active, "deployment_readiness", lambda: ready)

    payload = certification.end_to_end_certification_payload()

    assert payload["checks"]["cycle_history_serving_target_certified"] is True
    assert payload["cycle_history_backfill"]["cache_complete"] is True
    assert payload["cycle_history_backfill"]["serving_scan_id"] == "scan-certified"
    assert payload["control"]["cycle_history_cache_progress"] == progress
    assert payload["checks"]["canonical_control_current"] is False
    assert payload["checks"]["operating_reconciliation_complete"] is False
    assert payload["checks"]["qualified_bridge_publication_complete"] is False
    assert payload["certified"] is False
    assert "canonical_control_current" in payload["blockers"]


def test_background_heartbeat_is_diagnostic_only_when_stale():
    status = certification._cycle_history_backfill_status_from_worker(
        {
            "available": True,
            "stale": True,
            "state": "success",
            "error_type": None,
            "observed_at": "2026-01-01T00:00:00+00:00",
            "age_seconds": 10000.0,
            "cache_complete": True,
            "progress": {"complete": True, "serving_scan_id": "old-scan"},
        }
    )

    assert status["stale"] is True
    assert status["cache_complete"] is True
    assert status["certification_authority"] is False
