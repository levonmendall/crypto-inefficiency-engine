from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine.disposable_heavy_job import child_memory_admission_reason
from inefficiency_engine.render_combined import (
    recovery_failure_exceeded,
    research_job_timed_out,
    research_watchdog_reason,
)


def _health(*, age_seconds: float = 30.0, state: str = "success") -> dict[str, object]:
    return {
        "runtime_heartbeats": {
            "workers": {
                "research": {
                    "available": True,
                    "state": state,
                    "age_seconds": age_seconds,
                }
            }
        }
    }


def _dashboard(*, age_seconds: float = 30.0, stale: bool = False) -> dict[str, object]:
    observed = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
    return {
        "research_projection_observed_at": observed.isoformat(),
        "research_projection_stale": stale,
        "research_projection_freshness": {
            "available": True,
            "observed_at": observed.isoformat(),
            "age_seconds": age_seconds,
            "stale": stale,
        },
    }


def test_child_does_not_reapply_supervisor_start_block_threshold() -> None:
    memory = SimpleNamespace(start_blocked=True, terminate_required=False)
    assert child_memory_admission_reason(memory) is None


def test_child_still_rejects_hard_aggregate_terminate_boundary() -> None:
    memory = SimpleNamespace(start_blocked=True, terminate_required=True)
    assert child_memory_admission_reason(memory) == "InstanceMemoryTerminateBlocked"


def test_research_watchdog_requires_fresh_projection_not_just_worker_heartbeat() -> None:
    reason = research_watchdog_reason(
        _health(age_seconds=20.0),
        _dashboard(age_seconds=1200.0, stale=True),
        runtime_age_seconds=1000.0,
        startup_grace_seconds=60.0,
        heartbeat_stale_seconds=600.0,
    )
    assert reason is not None
    assert "projection" in reason
    assert "stale" in reason


def test_research_watchdog_accepts_current_worker_and_projection() -> None:
    assert research_watchdog_reason(
        _health(age_seconds=20.0, state="degraded"),
        _dashboard(age_seconds=30.0, stale=False),
        runtime_age_seconds=1000.0,
        startup_grace_seconds=60.0,
        heartbeat_stale_seconds=600.0,
    ) is None


def test_research_watchdog_rejects_missing_projection() -> None:
    reason = research_watchdog_reason(
        _health(age_seconds=20.0),
        None,
        runtime_age_seconds=1000.0,
        startup_grace_seconds=60.0,
        heartbeat_stale_seconds=600.0,
    )
    assert reason == "research dashboard projection could not be read"


def test_repeated_failed_recovery_eventually_escalates() -> None:
    assert not recovery_failure_exceeded(
        research_overdue=True,
        failed_since=100.0,
        now=699.0,
        limit_seconds=600.0,
    )
    assert recovery_failure_exceeded(
        research_overdue=True,
        failed_since=100.0,
        now=700.0,
        limit_seconds=600.0,
    )


def test_stuck_research_disposable_has_hard_runtime_bound() -> None:
    assert not research_job_timed_out(
        heavy_name="research",
        heavy_started_at=100.0,
        now=399.0,
        timeout_seconds=300.0,
    )
    assert research_job_timed_out(
        heavy_name="research",
        heavy_started_at=100.0,
        now=401.0,
        timeout_seconds=300.0,
    )
    assert not research_job_timed_out(
        heavy_name="history",
        heavy_started_at=100.0,
        now=1000.0,
        timeout_seconds=300.0,
    )
