from __future__ import annotations

import inspect
import os
from types import SimpleNamespace

from inefficiency_engine import cycle_history_bucket_timeout_runtime as runtime
from inefficiency_engine.durable_control_cycle_history import _bucket_query_budget


def test_control_executor_cycle_history_slice_is_short_and_clamped(monkeypatch):
    monkeypatch.delenv("CIE_CONTROL_CYCLE_HISTORY_EXECUTOR_SLICE_SECONDS", raising=False)
    assert runtime.control_executor_cycle_history_slice_seconds() == 3.0

    monkeypatch.setenv("CIE_CONTROL_CYCLE_HISTORY_EXECUTOR_SLICE_SECONDS", "99")
    assert runtime.control_executor_cycle_history_slice_seconds() == 4.0

    monkeypatch.setenv("CIE_CONTROL_CYCLE_HISTORY_EXECUTOR_SLICE_SECONDS", "0")
    assert runtime.control_executor_cycle_history_slice_seconds() == 1.0


def test_supervisor_safe_slice_caps_one_bucket_and_restores_environment(monkeypatch):
    monkeypatch.delenv("CIE_CONTROL_CYCLE_HISTORY_BUCKET_QUERY_BUDGET", raising=False)
    monkeypatch.delenv("CIE_CONTROL_CYCLE_HISTORY_EXECUTOR_SLICE_SECONDS", raising=False)
    monkeypatch.setattr(runtime.time, "monotonic", lambda: 100.0)
    observed: dict[str, object] = {}

    def fake_advance(factory, snapshot, *, stop_at_monotonic=None):
        observed["factory"] = factory
        observed["snapshot"] = snapshot
        observed["stop_at_monotonic"] = stop_at_monotonic
        observed["query_budget"] = _bucket_query_budget()
        observed["env_budget"] = os.getenv(
            "CIE_CONTROL_CYCLE_HISTORY_BUCKET_QUERY_BUDGET"
        )
        return {"complete": False, "bucket_queries": 1}

    factory = SimpleNamespace(name="factory")
    snapshot = SimpleNamespace(scan_id="scan")
    progress = runtime.advance_control_executor_cycle_history_cache(
        fake_advance,
        factory,
        snapshot,
    )

    assert observed == {
        "factory": factory,
        "snapshot": snapshot,
        "stop_at_monotonic": 103.0,
        "query_budget": 1,
        "env_budget": "1",
    }
    assert "CIE_CONTROL_CYCLE_HISTORY_BUCKET_QUERY_BUDGET" not in os.environ
    assert progress["complete"] is False
    assert progress["control_executor_slice_seconds"] == 3.0
    assert progress["control_executor_bucket_query_cap"] == 1
    assert progress["control_executor_supervisor_safe_slice"] is True
    assert progress["external_process_deadline_unchanged"] is True
    assert progress["qualification_thresholds_unchanged"] is True
    assert progress["paper_only"] is True


def test_supervisor_safe_slice_honors_earlier_stop_and_restores_existing_budget(
    monkeypatch,
):
    monkeypatch.setenv("CIE_CONTROL_CYCLE_HISTORY_BUCKET_QUERY_BUDGET", "32")
    monkeypatch.setenv("CIE_CONTROL_CYCLE_HISTORY_EXECUTOR_SLICE_SECONDS", "4")
    monkeypatch.setattr(runtime.time, "monotonic", lambda: 100.0)
    observed: dict[str, object] = {}

    def fake_advance(_factory, _snapshot, *, stop_at_monotonic=None):
        observed["stop_at_monotonic"] = stop_at_monotonic
        observed["query_budget"] = _bucket_query_budget()
        return {"complete": False}

    runtime.advance_control_executor_cycle_history_cache(
        fake_advance,
        SimpleNamespace(),
        SimpleNamespace(),
        stop_at_monotonic=102.0,
    )

    assert observed == {"stop_at_monotonic": 102.0, "query_budget": 1}
    assert os.environ["CIE_CONTROL_CYCLE_HISTORY_BUCKET_QUERY_BUDGET"] == "32"


def test_disposable_control_runtime_installs_supervisor_safe_wrapper():
    source = inspect.getsource(runtime)
    assignment = source.index(
        "_legacy_cycle_history.advance_durable_control_cycle_history_cache = ("
    )
    wrapper = source.index("_advance_supervisor_safe_cycle_history", assignment)

    assert assignment < wrapper
    assert "CIE_CONTROL_CYCLE_DEADLINE_SECONDS" not in source
    assert "_DEFAULT_STATEMENT_TIMEOUT_SECONDS = 4.0" in source
    assert "_DEFAULT_LOCK_TIMEOUT_SECONDS = 1.0" in source
