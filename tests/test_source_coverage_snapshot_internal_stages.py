from __future__ import annotations

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.source_coverage import SourceCoveragePlane
from inefficiency_engine.source_coverage_snapshot_executor import (
    SOURCE_COVERAGE_EXECUTOR_WORKER_ID,
    _record_executor_stage,
)
from inefficiency_engine.source_coverage_snapshot_stage_runtime import (
    profile_source_coverage_snapshot,
)
from inefficiency_engine.source_runtime_safety import (
    install_source_coverage_reconciliation_runtime,
)


def test_source_coverage_snapshot_exposes_exact_internal_stages(tmp_path):
    store = EvidenceStore(tmp_path / "source-coverage-internal-stages.sqlite")
    install_source_coverage_reconciliation_runtime()

    observed: list[tuple[str, dict[str, float]]] = []

    def record(stage: str, timings: dict[str, float]) -> None:
        observed.append((stage, dict(timings)))

    with profile_source_coverage_snapshot(record) as profiler:
        snapshot = SourceCoveragePlane(store).snapshot()

    stages = [stage for stage, _ in observed]
    assert snapshot.lane_count == 13
    for required in (
        "snapshot_start",
        "table_discovery",
        "source_observation_latest",
        "provider_status_latest",
        "admission_latest",
        "lane_reconciliation",
        "dynamic_priority",
        "snapshot_persist",
        "snapshot_complete",
    ):
        assert required in stages

    assert stages.index("table_discovery") < stages.index("source_observation_latest")
    assert stages.index("source_observation_latest") < stages.index("provider_status_latest")
    assert stages.index("provider_status_latest") < stages.index("admission_latest")
    assert stages.index("dynamic_priority") < stages.index("snapshot_persist")
    assert any(stage.startswith("table_candidate:") for stage in stages)
    assert all(value >= 0.0 for value in profiler.timings().values())


def test_executor_heartbeat_persists_stage_timings(tmp_path):
    store = EvidenceStore(tmp_path / "source-coverage-stage-timings.sqlite")
    _record_executor_stage(
        store,
        stage="dynamic_priority",
        stage_timings_seconds={
            "table_discovery": 0.25,
            "source_observation_latest": 1.5,
        },
    )

    heartbeat = store.latest_worker_heartbeat(SOURCE_COVERAGE_EXECUTOR_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.detail["stage"] == "dynamic_priority"
    assert heartbeat.detail["stage_timings_seconds"] == {
        "table_discovery": 0.25,
        "source_observation_latest": 1.5,
    }
    assert heartbeat.detail["provider_requests_allowed"] is False
    assert heartbeat.detail["provider_requests_used"] == 0
    assert heartbeat.detail["qualification_thresholds_unchanged"] is True
