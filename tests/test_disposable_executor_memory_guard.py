from __future__ import annotations

import json
import threading

import pytest

from inefficiency_engine import control_cycle_executor, source_coverage_snapshot_executor
from inefficiency_engine import disposable_executor_memory_guard as memory_guard
from inefficiency_engine.instance_memory import (
    DEFAULT_SOFT_RATIO,
    DEFAULT_START_BLOCK_RATIO,
    DEFAULT_TERMINATE_RATIO,
    InstanceMemorySnapshot,
)


def _snapshot(
    *,
    usage_mb: float,
    start_block_mb: float = 775.0,
    terminate_mb: float = 825.0,
) -> InstanceMemorySnapshot:
    return InstanceMemorySnapshot(
        usage_mb=usage_mb,
        limit_mb=1000.0,
        soft_mb=700.0,
        start_block_mb=start_block_mb,
        terminate_mb=terminate_mb,
        source="test",
    )


def test_disposable_executor_admission_uses_existing_start_block_threshold() -> None:
    blocked = _snapshot(usage_mb=800.0)

    with pytest.raises(memory_guard.DisposableMemoryAdmissionDeferred) as excinfo:
        with memory_guard.disposable_executor_memory_guard(
            "test-executor",
            snapshot_reader=lambda: blocked,
        ):
            pytest.fail("blocked disposable executor must not enter its heavy body")

    assert excinfo.value.snapshot is blocked
    assert "before heavy imports" in str(excinfo.value)


def test_running_disposable_executor_exits_at_existing_terminate_threshold() -> None:
    allowed = _snapshot(usage_mb=500.0)
    terminate = _snapshot(usage_mb=850.0)
    snapshots = iter((allowed, terminate))
    triggered = threading.Event()
    exit_codes: list[int] = []

    def read_snapshot() -> InstanceMemorySnapshot:
        try:
            return next(snapshots)
        except StopIteration:
            return terminate

    def fake_exit(code: int) -> None:
        exit_codes.append(code)
        triggered.set()

    with memory_guard.disposable_executor_memory_guard(
        "test-executor",
        snapshot_reader=read_snapshot,
        exit_func=fake_exit,
        poll_seconds=0.05,
    ):
        assert triggered.wait(timeout=1.0)

    assert exit_codes == [memory_guard.MEMORY_PRESSURE_EXIT_CODE]


def test_control_executor_defers_before_heavy_cycle_when_memory_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    result_path = tmp_path / "control-result.json"
    monkeypatch.setenv("CIE_CONTROL_EXECUTOR_RESULT_PATH", str(result_path))
    monkeypatch.setattr(
        memory_guard,
        "instance_memory_snapshot",
        lambda: _snapshot(usage_mb=800.0),
    )

    called = False

    def unexpected_cycle() -> dict[str, object]:
        nonlocal called
        called = True
        raise AssertionError("heavy control cycle must not start under memory pressure")

    monkeypatch.setattr(control_cycle_executor, "run_one_control_cycle", unexpected_cycle)

    assert control_cycle_executor.main() == memory_guard.MEMORY_PRESSURE_EXIT_CODE
    assert called is False
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    assert payload["error_type"] == "ControlExecutorMemoryAdmissionDeferred"
    assert payload["qualification_thresholds_unchanged"] is True
    assert payload["paper_only"] is True


def test_source_coverage_executor_defers_before_heavy_imports_when_memory_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        memory_guard,
        "instance_memory_snapshot",
        lambda: _snapshot(usage_mb=800.0),
    )

    called = False

    def unexpected_snapshot() -> int:
        nonlocal called
        called = True
        raise AssertionError("source snapshot body must not start under memory pressure")

    monkeypatch.setattr(
        source_coverage_snapshot_executor,
        "_run_source_coverage_snapshot",
        unexpected_snapshot,
    )

    assert source_coverage_snapshot_executor.main() == memory_guard.MEMORY_PRESSURE_EXIT_CODE
    assert called is False


def test_memory_threshold_defaults_are_unchanged() -> None:
    assert DEFAULT_SOFT_RATIO == 0.70
    assert DEFAULT_START_BLOCK_RATIO == 0.775
    assert DEFAULT_TERMINATE_RATIO == 0.825
