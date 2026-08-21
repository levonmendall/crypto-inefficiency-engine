from __future__ import annotations

from inefficiency_engine.render_combined import (
    research_heartbeat_marker,
    research_job_stalled,
)


def test_long_research_runtime_is_allowed_while_progress_is_current():
    assert research_job_stalled(
        heavy_name="research",
        last_progress_at=950.0,
        now=1000.0,
        timeout_seconds=300.0,
    ) is False


def test_research_is_killed_only_after_progress_stalls():
    assert research_job_stalled(
        heavy_name="research",
        last_progress_at=699.0,
        now=1000.0,
        timeout_seconds=300.0,
    ) is True
    assert research_job_stalled(
        heavy_name="research",
        last_progress_at=700.0,
        now=1000.0,
        timeout_seconds=300.0,
    ) is False
    assert research_job_stalled(
        heavy_name="history",
        last_progress_at=0.0,
        now=1000.0,
        timeout_seconds=300.0,
    ) is False


def test_research_heartbeat_marker_uses_durable_observed_at():
    health = {
        "runtime_heartbeats": {
            "workers": {
                "research": {
                    "available": True,
                    "observed_at": "2026-08-21T22:00:00+00:00",
                    "age_seconds": 5.0,
                }
            }
        }
    }

    assert research_heartbeat_marker(health) == "2026-08-21T22:00:00+00:00"
    assert research_heartbeat_marker({"runtime_heartbeats": {"workers": {}}}) is None


def test_unavailable_research_heartbeat_does_not_count_as_progress():
    health = {
        "runtime_heartbeats": {
            "workers": {
                "research": {
                    "available": False,
                    "observed_at": "2026-08-21T22:00:00+00:00",
                }
            }
        }
    }

    assert research_heartbeat_marker(health) is None
