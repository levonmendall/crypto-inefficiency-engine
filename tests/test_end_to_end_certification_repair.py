from __future__ import annotations

import inspect
from datetime import datetime, timezone
from types import SimpleNamespace

import inefficiency_engine.read_api_end_to_end_certification_deploy as certification
import inefficiency_engine.research_projection_refresh_child as projection_child
import inefficiency_engine.research_projection_supervisor as projection_supervisor


def _worker(state="success", *, stale=False, **extra):
    return {
        "available": True,
        "state": state,
        "stale": stale,
        **extra,
    }


def _alpha_research_worker(*, stale=False):
    observed_at = (
        "2026-01-01T00:00:00+00:00"
        if stale
        else datetime.now(timezone.utc).isoformat()
    )
    return _worker(
        "running",
        observed_at=observed_at,
        critical_evidence_recovery={
            "workers": {
                "alpha_forward": {
                    "worker_id": "shadow-research-auxiliary",
                    "signal": "alpha_forward_evidence_cycle_id",
                    "available": True,
                    "observed_at": observed_at,
                    "state": "running",
                    "cycle_id": "alpha-test",
                    "recovery_after_seconds": 1200.0,
                }
            }
        },
    )


def _source_history_worker(*, complete=True):
    return _worker(
        "success" if complete else "running",
        stage="canonical_history_ready" if complete else "canonical_history_archive_migrating",
        complete=complete,
        compact_certification_summary=complete,
        checkpoint_heartbeat_id=100 if complete else 50,
        lane_count=13 if complete else 0,
        snapshot_count=1300 if complete else 0,
    )


def _ready_payload(*, sufficient_lane_count=6):
    workers = {
        "canonical_control": _worker(
            cycle_history_cache_complete=True,
            historical_cache_complete=True,
            operating_reconciliation_complete=True,
            qualified_bridge_publication_complete=True,
        ),
        "portfolio": _worker("running"),
        "permanent_source": _worker("running"),
        "mechanism_forward": _worker("success"),
        "research": _alpha_research_worker(),
        "source_coverage_snapshot": _worker(
            persisted_complete_snapshot=True,
            lane_count=13,
            handoff_stale=False,
            sufficient_lane_count=sufficient_lane_count,
            forward_test_eligible_lane_count=7,
            allocation_source_qualified_lane_count=6,
        ),
        "research_projection": _worker("success"),
        "runtime_index_maintenance": _worker(
            "degraded",
            error_type="ProgrammingError",
            control_gate_released=True,
            background_indexes_complete=False,
        ),
        "source_history_migration": _source_history_worker(),
    }
    return {
        "status": "ready",
        "database_ok": True,
        "release_commit": "abc123",
        "paper_only": True,
        "live_execution": False,
        "runtime_heartbeats": {"workers": workers},
    }


def test_certification_allows_economic_rejection_without_trade(monkeypatch):
    monkeypatch.setattr(certification.active, "deployment_readiness", _ready_payload)

    payload = certification.end_to_end_certification_payload()

    assert payload["certified"] is True
    assert payload["operationally_certified"] is True
    assert payload["blockers"] == []
    assert payload["trade_required_for_certification"] is False
    assert payload["positive_candidate_required_for_certification"] is False
    assert payload["economic_rejection_is_valid"] is True
    assert payload["canonical_source_history"]["migration_complete"] is True
    assert payload["source_coverage"]["sufficient_lane_count"] == 6
    assert payload["full_13_lane_evidence_scope_complete"] is False
    assert payload["source_coverage"]["fail_closed_lane_gaps_allowed_for_operational_certification"] is True
    assert payload["runtime_index_maintenance"]["certification_authority"] is False
    assert payload["qualification_thresholds_unchanged"] is True
    assert payload["live_execution_authority"] is False
    assert payload["certification_post_readiness_database_reads"] == 0


def test_all_lane_scope_is_reported_separately_from_pipeline_operation(monkeypatch):
    monkeypatch.setattr(
        certification.active,
        "deployment_readiness",
        lambda: _ready_payload(sufficient_lane_count=13),
    )

    payload = certification.end_to_end_certification_payload()

    assert payload["certified"] is True
    assert payload["full_13_lane_evidence_scope_complete"] is True
    assert payload["source_coverage"]["sufficient_lane_count"] == 13


def test_certification_fails_closed_on_stale_alpha_and_incomplete_control(monkeypatch):
    ready = _ready_payload()
    workers = ready["runtime_heartbeats"]["workers"]
    control = workers["canonical_control"]
    control.update(
        {
            "state": "degraded",
            "cycle_history_cache_complete": False,
            "historical_cache_complete": False,
            "operating_reconciliation_complete": False,
            "qualified_bridge_publication_complete": False,
        }
    )
    workers["research"] = _alpha_research_worker(stale=True)
    workers["source_history_migration"] = _source_history_worker(complete=False)
    monkeypatch.setattr(certification.active, "deployment_readiness", lambda: ready)

    payload = certification.end_to_end_certification_payload()

    assert payload["certified"] is False
    assert "canonical_source_history_migrated" in payload["blockers"]
    assert "alpha_forward_cycle_current" in payload["blockers"]
    assert "cycle_history_serving_target_certified" in payload["blockers"]
    assert "canonical_control_current" in payload["blockers"]
    assert "operating_reconciliation_complete" in payload["blockers"]
    assert "qualified_bridge_publication_complete" in payload["blockers"]


def test_projection_refresh_child_is_persisted_only_and_non_authoritative(monkeypatch):
    recorded = []

    class FakeStore:
        def record_worker_heartbeat(self, **kwargs):
            recorded.append(kwargs)

    class FakeLedger:
        def __init__(self, store):
            self.store = store

        def publish(self, **kwargs):
            assert kwargs["forward_target"] >= 1
            assert kwargs["settled_target"] >= 5
            return {"observed_at": "2026-08-26T01:00:00+00:00"}

    settings = SimpleNamespace(
        evidence_db_path="unused",
        alpha_min_forward_samples=30,
        operating_certification_min_settled_trials=20,
        shadow_horizons_seconds=(1.0, 5.0, 15.0, 30.0, 60.0),
        shadow_cycle_interval_seconds=30.0,
        alpha_evidence_every_cycles=10,
        worker_heartbeat_stale_seconds=180.0,
    )
    monkeypatch.setattr(projection_child.Settings, "from_env", lambda: settings)
    monkeypatch.setattr(projection_child, "build_evidence_store", lambda _path: FakeStore())
    monkeypatch.setattr(projection_child, "ResearchDashboardProjectionLedger", FakeLedger)

    assert projection_child.main() == 0
    assert recorded[-1]["state"] == "success"
    detail = recorded[-1]["detail"]
    assert detail["provider_calls"] is False
    assert detail["research_computation"] is False
    assert detail["presentation_only"] is True
    assert detail["allocation_authority"] is False
    assert detail["live_execution_authority"] is False
    assert detail["paper_only"] is True


def test_projection_supervisor_has_killable_deadline_and_bounded_cadence():
    source = inspect.getsource(projection_supervisor.run_research_projection_supervisor)

    assert projection_supervisor.REFRESH_EXECUTOR_DEADLINE_SECONDS == 45.0
    assert projection_supervisor.REFRESH_INTERVAL_SECONDS == 60.0
    assert "subprocess.Popen(REFRESH_COMMAND)" in source
    assert "_terminate(child)" in source
    assert "instance_memory_snapshot" in source


def test_production_liveness_composes_certification_route():
    from inefficiency_engine import read_api_liveness_deploy as liveness

    payload = liveness.liveness_payload()
    assert payload["end_to_end_certification_endpoint"] == (
        "/v3/operations/end-to-end-certification"
    )
    assert payload["liveness_database_independent"] is True
