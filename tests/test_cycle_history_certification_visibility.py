from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import inefficiency_engine.read_api_end_to_end_certification_deploy as certification


def _worker(state="success", *, stale=False, **extra):
    return {"available": True, "state": state, "stale": stale, **extra}


def test_background_serving_target_is_visible_without_falsely_certifying_control(monkeypatch):
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
            }
        },
    }
    progress = {
        "complete": True,
        "serving_scan_id": "scan-certified",
        "incomplete_pair_count": 0,
        "durable_checkpoint_persisted": True,
    }
    heartbeat = SimpleNamespace(
        worker_id="cycle-history-background-backfill",
        observed_at=datetime.now(timezone.utc),
        state="success",
        error_type=None,
        detail={
            "stage": "certified_target_available",
            "cache_complete": True,
            "first_certified_target_pending": False,
            "progress": progress,
        },
    )

    class FakeStore:
        def latest_worker_heartbeat(self, worker_id):
            assert worker_id == "cycle-history-background-backfill"
            return heartbeat

    monkeypatch.setattr(certification.active, "deployment_readiness", lambda: ready)
    monkeypatch.setattr(certification.active, "_store", lambda: FakeStore())
    monkeypatch.setattr(
        certification,
        "_alpha_forward_status",
        lambda store, now, stale_after_seconds: {
            "available": True,
            "recovery_required": False,
        },
    )
    monkeypatch.setattr(
        certification,
        "_source_history_status",
        lambda store: {
            "available": True,
            "migration_complete": True,
            "lane_count": 13,
        },
    )

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


def test_background_heartbeat_is_diagnostic_only_when_stale(monkeypatch):
    old = datetime(2026, 1, 1, tzinfo=timezone.utc)
    heartbeat = SimpleNamespace(
        observed_at=old,
        state="success",
        error_type=None,
        detail={
            "cache_complete": True,
            "progress": {"complete": True, "serving_scan_id": "old-scan"},
        },
    )

    class FakeStore:
        def latest_worker_heartbeat(self, _worker_id):
            return heartbeat

    status = certification._cycle_history_backfill_status(FakeStore())

    assert status["stale"] is True
    assert status["cache_complete"] is True
    assert status["certification_authority"] is False
