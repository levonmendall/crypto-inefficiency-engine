from __future__ import annotations

import inspect
import os
import sys
from pathlib import Path

from inefficiency_engine import control_cycle_executor
from inefficiency_engine.control_cycle_runtime import ControlExecutorSupervisor
from inefficiency_engine.durable_control_bridge import (
    DurableControlQualifiedOpportunityBridgePublisher,
)


def _python(source: str) -> list[str]:
    return [sys.executable, "-c", source]


def test_bridge_reports_exact_diagnostic_substage_without_changing_authority():
    bridge = object.__new__(DurableControlQualifiedOpportunityBridgePublisher)
    observed: list[str] = []

    bridge.set_control_stage_reporter(observed.append)
    bridge._report_control_stage("alpha_promotion")

    assert observed == ["qualified_bridge:alpha_promotion"]

    source = inspect.getsource(
        DurableControlQualifiedOpportunityBridgePublisher.publish_latest
    )
    for stage in (
        "source_scan_selection",
        "source_freshness_gate",
        "core_cex_projection",
        "persisted_cex_dex_projection",
        "alpha_promotion",
        "canonical_settlement_filter",
        "ledger_record",
        "complete",
    ):
        assert f'_report_control_stage("{stage}")' in source


def test_bridge_substage_survives_external_executor_timeout(tmp_path: Path):
    supervisor = ControlExecutorSupervisor(
        deadline_seconds=0.15,
        heartbeat_interval_seconds=0.02,
        terminate_grace_seconds=0.05,
        workspace=tmp_path,
    )
    source = (
        "import json, os, pathlib, time; "
        "path=pathlib.Path(os.environ['CIE_CONTROL_EXECUTOR_STATUS_PATH']); "
        "path.write_text(json.dumps({"
        "'stage':'qualified_bridge:alpha_promotion',"
        "'observed_at':'2026-08-24T15:55:00+00:00',"
        "'provider_requests_allowed':False,"
        "'provider_requests_used':0,"
        "'paper_only':True})); "
        "time.sleep(3600)"
    )

    result = supervisor.run_cycle(sequence=1, command=_python(source))
    telemetry = result.telemetry()

    assert result.error_type == "ControlExecutorDeadlineExceeded"
    assert result.executor_last_stage == "qualified_bridge:alpha_promotion"
    assert telemetry["executor_last_stage_before_failure"] == (
        "qualified_bridge:alpha_promotion"
    )
    assert telemetry["provider_requests_allowed"] is False
    assert telemetry["provider_requests_used"] == 0
    assert telemetry["paper_only"] is True
    try:
        os.kill(result.executor_pid, 0)
    except ProcessLookupError:
        pass
    else:
        raise AssertionError("timed-out bridge executor remained alive")


def test_bridge_telemetry_does_not_add_historical_cache_queries():
    source = inspect.getsource(control_cycle_executor.run_one_control_cycle)
    start = source.index("def bridge_stage_reporter")
    end = source.index('stage_reporter("control_executor_starting")')
    bridge_reporter_source = source[start:end]

    assert "write_stage(stage, last_progress)" in bridge_reporter_source
    assert "_cache_status()" not in bridge_reporter_source
    assert "set_control_stage_reporter" in source
