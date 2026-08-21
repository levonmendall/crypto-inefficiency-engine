from __future__ import annotations

from datetime import datetime, timezone

from inefficiency_engine.production_dashboard_fastpath import (
    operating_projection_freshness,
    reconcile_mechanism_runtime_truth,
    research_projection_freshness,
)
from inefficiency_engine.render_combined import (
    choose_heavy_job,
    research_memory_starvation_exceeded,
    research_watchdog_reason,
)


def _now() -> datetime:
    return datetime(2026, 8, 21, 17, 50, tzinfo=timezone.utc)


def test_research_projection_uses_actual_wall_clock_for_staleness():
    stale = research_projection_freshness(
        {"observed_at": "2026-08-20T21:13:00+00:00"},
        now=_now(),
        stale_seconds=900,
    )
    assert stale["stale"] is True
    assert float(stale["age_seconds"]) > 20 * 3600

    fresh = research_projection_freshness(
        {"observed_at": "2026-08-21T17:45:00+00:00"},
        now=_now(),
        stale_seconds=900,
    )
    assert fresh["stale"] is False


def test_operating_projection_is_independently_freshness_checked():
    result = operating_projection_freshness(
        {
            "observed_at": "2026-08-21T17:49:00+00:00",
            "source_operating_observed_at": "2026-08-20T21:13:00+00:00",
        },
        now=_now(),
        stale_seconds=1800,
    )
    assert result["stale"] is True
    assert float(result["age_seconds"]) > 20 * 3600


def test_overdue_lane_cannot_keep_claiming_forward_collector_healthy():
    payload = {
        "mechanisms": [
            {
                "mechanism_id": "trend_momentum",
                "state": "statistical_failure",
                "primary_reason": (
                    "current candidates do not pass the complete gate · "
                    "forward collector healthy; persistence healthy; "
                    "next expected ~Aug 20 21:13 UTC"
                ),
                "next_action": "continue forward testing without lowering thresholds",
                "forward_evidence_worker_healthy": True,
                "forward_evidence_worker_state": "healthy_current",
                "forward_evidence_next_expected_at": "2026-08-20T21:13:00+00:00",
                "forward_evidence_expected_interval_seconds": 900.0,
            }
        ]
    }
    reconciled = reconcile_mechanism_runtime_truth(
        payload,
        now=_now(),
        research_freshness={"stale": False, "age_seconds": 60.0},
        operating_freshness={"stale": False, "age_seconds": 60.0},
    )
    row = reconciled["mechanisms"][0]
    assert row["state"] == "statistical_failure"
    assert row["forward_evidence_worker_healthy"] is False
    assert row["forward_evidence_worker_state"] == "stalled"
    assert float(row["forward_evidence_overdue_seconds"]) > 20 * 3600
    assert "forward collector healthy" not in row["primary_reason"]
    assert "overdue" in row["primary_reason"]
    assert "without lowering thresholds" in row["next_action"]


def test_research_watchdog_detects_stale_or_missing_publication_after_grace():
    stale_payload = {
        "runtime_heartbeats": {
            "workers": {
                "research": {
                    "available": True,
                    "state": "success",
                    "age_seconds": 7200.0,
                }
            }
        }
    }
    reason = research_watchdog_reason(
        stale_payload,
        runtime_age_seconds=1000.0,
        startup_grace_seconds=180.0,
        heartbeat_stale_seconds=900.0,
    )
    assert reason is not None
    assert "research heartbeat" in reason

    fresh_payload = {
        "runtime_heartbeats": {
            "workers": {
                "research": {
                    "available": True,
                    "state": "degraded",
                    "age_seconds": 120.0,
                }
            }
        }
    }
    assert research_watchdog_reason(
        fresh_payload,
        runtime_age_seconds=1000.0,
        startup_grace_seconds=180.0,
        heartbeat_stale_seconds=900.0,
    ) is None

    missing = {"runtime_heartbeats": {"workers": {}}}
    assert research_watchdog_reason(
        missing,
        runtime_age_seconds=120.0,
        startup_grace_seconds=180.0,
        heartbeat_stale_seconds=900.0,
    ) is None
    assert research_watchdog_reason(
        missing,
        runtime_age_seconds=181.0,
        startup_grace_seconds=180.0,
        heartbeat_stale_seconds=900.0,
    ) is not None


def test_overdue_research_preempts_history_and_memory_starvation_is_bounded():
    assert choose_heavy_job(
        due_research=False,
        due_history=True,
        research_overdue=True,
    ) == "research"
    assert choose_heavy_job(
        due_research=True,
        due_history=True,
        research_overdue=False,
    ) == "research"
    assert choose_heavy_job(
        due_research=False,
        due_history=True,
        research_overdue=False,
    ) == "history"

    assert research_memory_starvation_exceeded(
        research_overdue=True,
        blocked_since=100.0,
        now=699.0,
        limit_seconds=600.0,
    ) is False
    assert research_memory_starvation_exceeded(
        research_overdue=True,
        blocked_since=100.0,
        now=700.0,
        limit_seconds=600.0,
    ) is True
    assert research_memory_starvation_exceeded(
        research_overdue=False,
        blocked_since=100.0,
        now=1000.0,
        limit_seconds=600.0,
    ) is False
