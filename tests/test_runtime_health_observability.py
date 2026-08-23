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


def test_runtime_contract_includes_permanent_source_and_universe_routing():
    assert deploy._RUNTIME_HEARTBEATS["permanent_source"] == "canonical-source-operating-loop"
    assert deploy._RUNTIME_HEARTBEATS["volume_universe"] == "volume-universe-lightweight-refresh"
    assert deploy._RUNTIME_HEARTBEATS["market_universe_routing"] == "market-universe-routing"


def test_runtime_heartbeat_payload_exposes_degraded_and_stale_without_raising(monkeypatch):
    rows = {
        "canonical-portfolio-operating-loop": SimpleNamespace(
            state="success",
            error_type=None,
            observed_at=NOW,
        ),
        "canonical-source-operating-loop": SimpleNamespace(
            state="degraded",
            error_type="ProviderSubsystemDegraded",
            observed_at=NOW - timedelta(seconds=20),
        ),
        "market-universe-routing": SimpleNamespace(
            state="degraded",
            error_type="VolumeUniverseUnavailableError",
            observed_at=NOW - timedelta(seconds=10),
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
    assert payload["worker_specific_staleness"] is True
    assert payload["workers"]["portfolio"]["state"] == "success"
    assert payload["workers"]["permanent_source"]["state"] == "degraded"
    assert payload["workers"]["permanent_source"]["stale"] is False
    assert payload["workers"]["market_universe_routing"]["error_type"] == "VolumeUniverseUnavailableError"
    assert payload["workers"]["research"]["state"] == "degraded"
    assert payload["workers"]["research"]["stale"] is True
    assert payload["workers"]["research"]["error_type"] == "ResearchSubsystemDegraded"


def test_runtime_liveness_windows_match_worker_cadence_without_relaxing_source_owner(monkeypatch):
    rows = {
        "canonical-portfolio-operating-loop": SimpleNamespace(
            state="success",
            error_type=None,
            observed_at=NOW - timedelta(seconds=240),
        ),
        "canonical-source-operating-loop": SimpleNamespace(
            state="running",
            error_type=None,
            observed_at=NOW - timedelta(seconds=181),
        ),
        "shadow-research-auxiliary": SimpleNamespace(
            state="success",
            error_type=None,
            observed_at=NOW - timedelta(seconds=300),
        ),
        "alpha-l2-research-sampling": SimpleNamespace(
            state="success",
            error_type=None,
            observed_at=NOW - timedelta(seconds=300),
        ),
    }
    monkeypatch.setattr(deploy, "_store", lambda: _Store(rows))
    monkeypatch.setattr(
        deploy._base_deploy._base,
        "settings",
        SimpleNamespace(worker_heartbeat_stale_seconds=180.0),
    )

    payload = deploy._runtime_heartbeats()

    portfolio = payload["workers"]["portfolio"]
    source = payload["workers"]["permanent_source"]
    research = payload["workers"]["research"]
    l2 = payload["workers"]["alpha_l2_sampling"]

    assert portfolio["stale_after_seconds"] == 600.0
    assert portfolio["stale"] is False
    assert source["stale_after_seconds"] == 180.0
    assert source["stale"] is True
    assert research["stale_after_seconds"] == 600.0
    assert research["stale"] is False
    assert l2["stale_after_seconds"] == 600.0
    assert l2["stale"] is False


def test_unobserved_worker_is_visible_not_assumed_healthy(monkeypatch):
    monkeypatch.setattr(deploy, "_store", lambda: _Store({}))
    payload = deploy._runtime_heartbeats()
    assert payload["workers"]["permanent_source"]["available"] is False
    assert payload["workers"]["permanent_source"]["state"] == "unobserved"
    assert payload["workers"]["permanent_source"]["stale_after_seconds"] == 180.0
    assert payload["workers"]["market_universe_routing"]["available"] is False
    assert payload["workers"]["alpha_l2_sampling"]["available"] is False
    assert payload["workers"]["alpha_l2_sampling"]["state"] == "unobserved"
    assert payload["workers"]["alpha_l2_sampling"]["stale_after_seconds"] == 600.0
