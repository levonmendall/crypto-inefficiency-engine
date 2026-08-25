from __future__ import annotations

from types import SimpleNamespace

from inefficiency_engine import candidate_observatory_backfill_supervisor as supervisor
from inefficiency_engine.candidate_observatory_lane_coverage_preflight import (
    replay_is_ready_for_lane_certification,
)


class _NeverStop:
    def is_set(self) -> bool:
        return False

    def wait(self, _seconds: float) -> bool:
        raise AssertionError("terminal coverage preflight must not sleep")


class _StopOnWait:
    def __init__(self) -> None:
        self.stopped = False
        self.waits: list[float] = []

    def is_set(self) -> bool:
        return self.stopped

    def wait(self, seconds: float) -> bool:
        self.waits.append(seconds)
        self.stopped = True
        return True


def test_preflight_accepts_only_durable_complete_replay_with_live_boundary():
    ready_store = SimpleNamespace(
        latest_worker_heartbeat=lambda _worker_id: SimpleNamespace(
            detail={
                "stream_replay_complete": True,
                "live_observatory_started_at": "2026-08-21T22:00:00+00:00",
            }
        )
    )
    legacy_ready_store = SimpleNamespace(
        latest_worker_heartbeat=lambda _worker_id: SimpleNamespace(
            detail={
                "complete": True,
                "live_observatory_started_at": "2026-08-21T22:00:00+00:00",
            }
        )
    )
    incomplete_store = SimpleNamespace(
        latest_worker_heartbeat=lambda _worker_id: SimpleNamespace(
            detail={
                "stream_replay_complete": False,
                "live_observatory_started_at": "2026-08-21T22:00:00+00:00",
            }
        )
    )
    boundary_missing_store = SimpleNamespace(
        latest_worker_heartbeat=lambda _worker_id: SimpleNamespace(
            detail={"stream_replay_complete": True}
        )
    )

    assert replay_is_ready_for_lane_certification(ready_store) is True
    assert replay_is_ready_for_lane_certification(legacy_ready_store) is True
    assert replay_is_ready_for_lane_certification(incomplete_store) is False
    assert replay_is_ready_for_lane_certification(boundary_missing_store) is False


def test_terminal_coverage_preflight_runs_before_memory_admission(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(supervisor, "_api_is_bound", lambda _port: True)
    monkeypatch.setattr(
        supervisor,
        "_coverage_result",
        lambda _stop_event: (calls.append("coverage") or supervisor.COVERAGE_INCOMPLETE_EXIT_CODE, False),
    )

    def forbidden_memory():
        raise AssertionError("memory admission must not run after terminal coverage")

    monkeypatch.setattr(supervisor, "instance_memory_snapshot", forbidden_memory)
    monkeypatch.setattr(
        supervisor,
        "_run_bounded_child",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("heavy replay must not run after terminal coverage")
        ),
    )

    supervisor.run_candidate_observatory_backfill_supervisor(_NeverStop())

    assert calls == ["coverage"]


def test_not_ready_preflight_preserves_heavy_memory_gate(monkeypatch):
    stop = _StopOnWait()
    calls: list[str] = []

    monkeypatch.setattr(supervisor, "_api_is_bound", lambda _port: True)
    monkeypatch.setattr(
        supervisor,
        "_coverage_result",
        lambda _stop_event: (
            calls.append("coverage") or supervisor.COVERAGE_NOT_READY_EXIT_CODE,
            False,
        ),
    )
    monkeypatch.setattr(
        supervisor,
        "instance_memory_snapshot",
        lambda: SimpleNamespace(start_blocked=True),
    )
    monkeypatch.setattr(
        supervisor,
        "_run_bounded_child",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("memory-blocked heavy replay must not start")
        ),
    )

    supervisor.run_candidate_observatory_backfill_supervisor(stop)

    assert calls == ["coverage"]
    assert stop.waits == [supervisor.BACKFILL_MEMORY_RETRY_SECONDS]


def test_supervisor_uses_short_lived_preflight_module():
    assert supervisor.BACKFILL_COVERAGE_COMMAND[-1] == (
        "inefficiency_engine.candidate_observatory_lane_coverage_preflight"
    )
