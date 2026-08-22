from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

from inefficiency_engine.render_combined import (
    API_APP,
    child_commands,
    heavy_commands,
    portfolio_watchdog_reason,
)


def test_combined_runtime_keeps_only_portfolio_and_api_permanent():
    commands = child_commands("12345")

    assert set(commands) == {"portfolio", "api"}
    assert commands["portfolio"] == [
        sys.executable,
        "-m",
        "inefficiency_engine.lightweight_portfolio_worker",
    ]
    assert commands["api"] == [
        sys.executable,
        "-m",
        "uvicorn",
        API_APP,
        "--host",
        "0.0.0.0",
        "--port",
        "12345",
    ]


def test_combined_runtime_makes_research_and_history_disposable_and_mutually_scheduled():
    commands = heavy_commands()

    assert set(commands) == {"research", "history"}
    assert commands["research"] == [
        sys.executable,
        "-m",
        "inefficiency_engine.disposable_heavy_job",
        "research",
    ]
    assert commands["history"] == [
        sys.executable,
        "-m",
        "inefficiency_engine.disposable_heavy_job",
        "history",
    ]


def test_combined_runtime_uses_canonical_card_history_read_plane():
    assert API_APP == "inefficiency_engine.read_api_card_history_deploy:app"
    assert child_commands("10000")["api"][3] == API_APP


def _health_row(*, observed_at: datetime, age_seconds: float, state: str = "success"):
    return {
        "runtime_heartbeats": {
            "workers": {
                "portfolio": {
                    "worker_id": "canonical-portfolio-operating-loop",
                    "available": True,
                    "state": state,
                    "error_type": None,
                    "observed_at": observed_at.isoformat(),
                    "age_seconds": age_seconds,
                    "stale": age_seconds > 180.0,
                }
            }
        }
    }


def test_portfolio_watchdog_allows_startup_grace_before_first_heartbeat():
    started = datetime(2026, 8, 21, 17, 0, tzinfo=timezone.utc)
    payload = {"runtime_heartbeats": {"workers": {}}}

    assert portfolio_watchdog_reason(
        payload,
        process_started_at=started,
        process_age_seconds=120.0,
    ) is None
    assert "has not published" in portfolio_watchdog_reason(
        payload,
        process_started_at=started,
        process_age_seconds=181.0,
    )


def test_portfolio_watchdog_restarts_cycle_stuck_beyond_outer_timeout_margin():
    started = datetime(2026, 8, 21, 17, 0, tzinfo=timezone.utc)
    observed = started + timedelta(seconds=10)
    payload = _health_row(observed_at=observed, age_seconds=211.0, state="running")

    reason = portfolio_watchdog_reason(
        payload,
        process_started_at=started,
        process_age_seconds=240.0,
    )

    assert reason is not None
    assert "remained running" in reason


def test_portfolio_watchdog_allows_normal_five_minute_sleep_but_restarts_long_freeze():
    started = datetime(2026, 8, 21, 17, 0, tzinfo=timezone.utc)
    observed = started + timedelta(seconds=10)

    assert portfolio_watchdog_reason(
        _health_row(observed_at=observed, age_seconds=450.0, state="degraded"),
        process_started_at=started,
        process_age_seconds=460.0,
    ) is None

    reason = portfolio_watchdog_reason(
        _health_row(observed_at=observed, age_seconds=601.0, state="degraded"),
        process_started_at=started,
        process_age_seconds=611.0,
    )
    assert reason is not None
    assert "heartbeat is" in reason


def test_portfolio_watchdog_requires_heartbeat_from_current_child_after_restart():
    started = datetime(2026, 8, 21, 17, 10, tzinfo=timezone.utc)
    old_observed = started - timedelta(minutes=2)

    reason = portfolio_watchdog_reason(
        _health_row(observed_at=old_observed, age_seconds=300.0, state="success"),
        process_started_at=started,
        process_age_seconds=181.0,
    )

    assert reason is not None
    assert "previous process heartbeat" in reason
