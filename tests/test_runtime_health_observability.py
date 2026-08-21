from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine import read_api_active_volume_deploy as deploy


NOW = datetime.now(timezone.utc)


class _Store:
    def __init__(self, rows):
        self.rows = rows

    def latest_worker_heartbeat(self, worker_id):
        return self.rows.get(worker_id)


def test_lane_fast_summary_reports_production_connectivity_separately():
    summary = deploy._lane_summary_from_payload({"mechanisms": {"mechanisms": []}})
    assert summary["lane_count"] == 13
    assert summary["architecture_executable_count"] == 13
    assert summary["production_evidence_connected_count"] == 12
    assert summary["decision_grade_outcome_qualified_count"] == 0
    assert summary["paper_execution_capable_count"] == 0
    assert summary["all_lanes_paper_execution_capable"] is False
    assert summary["all_lanes_production_evidence_connected"] is False
    assert summary["production_evidence_disconnected_lanes"] == [
        "capital_location_settlement"
    ]


def test_runtime_heartbeat_payload_exposes_degraded_and_stale_without_raising(monkeypatch):
    rows = {
        "canonical-portfolio-operating-loop": SimpleNamespace(
            state="success",
            error_type=None,
            observed_at=NOW,
        ),
        "shadow-research-auxiliary": SimpleNamespace(
            state="degraded",
            error_type="ResearchSubsystemDegraded",
            observed_at=NOW - timedelta(hours=1),
        ),
    }
    monkeypatch.setattr(deploy, "_store", lambda: _Store(rows))
    monkeypatch.setattr(
        deploy._base_deploy._base,
        "settings",
        SimpleNamespace(worker_heartbeat_stale_seconds=180.0),
    )

    payload = deploy._runtime_heartbeats()
    assert payload["diagnostic_only"] is True
    assert payload["liveness_authority"] is False
    assert payload["workers"]["portfolio"]["state"] == "success"
    assert payload["workers"]["research"]["state"] == "degraded"
    assert payload["workers"]["research"]["stale"] is True
    assert payload["workers"]["research"]["error_type"] == "ResearchSubsystemDegraded"


def test_unobserved_worker_is_visible_not_assumed_healthy(monkeypatch):
    monkeypatch.setattr(deploy, "_store", lambda: _Store({}))
    payload = deploy._runtime_heartbeats()
    assert payload["workers"]["alpha_l2_sampling"]["available"] is False
    assert payload["workers"]["alpha_l2_sampling"]["state"] == "unobserved"
