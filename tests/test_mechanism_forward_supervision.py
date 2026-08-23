from __future__ import annotations

import inspect

from inefficiency_engine import permanent_mechanism_worker


def test_mechanism_forward_deadline_env_is_bounded(monkeypatch):
    monkeypatch.setenv("CIE_MECHANISM_FORWARD_DEADLINE_SECONDS", "5")
    assert permanent_mechanism_worker._forward_deadline_seconds() == 30.0

    monkeypatch.setenv("CIE_MECHANISM_FORWARD_DEADLINE_SECONDS", "75")
    assert permanent_mechanism_worker._forward_deadline_seconds() == 75.0


def test_permanent_mechanism_worker_publishes_startup_and_hard_deadlines_cycles():
    source = inspect.getsource(permanent_mechanism_worker._run)

    assert '"stage": "initializing"' in source
    assert "asyncio.wait_for" in source
    assert "MechanismForwardDeadlineExceeded" in source
    assert '"whole_cycle_deadline_seconds": deadline' in source
    assert '"supervised_by_dedicated_guard": True' in source
