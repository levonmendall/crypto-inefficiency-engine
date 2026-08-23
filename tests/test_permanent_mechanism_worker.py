from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.permanent_mechanism_worker import (
    mechanism_forward_funnel,
    refresh_canonical_control_plane,
)


NOW = datetime(2026, 8, 23, 15, 0, tzinfo=timezone.utc)


class FakeExecution:
    def readiness_summary(self):
        return {
            "maker_rebate": {
                "forward_outcome_count": 11,
                "incremental_qualified_cohort_count": 1,
                "full_qualified_cohort_count": 0,
                "currently_qualified": True,
                "current_promoted_candidate_count": 1,
            },
            "liquidation": {
                "forward_outcome_count": 7,
                "incremental_qualified_cohort_count": 0,
                "full_qualified_cohort_count": 1,
                "currently_qualified": True,
                "current_promoted_candidate_count": 2,
            },
        }


def test_mechanism_forward_funnel_reports_durable_qualification_progress():
    cycle = SimpleNamespace(
        current_specs=5,
        trials_recorded=3,
        outcomes_matured=2,
        promoted_candidates=3,
    )

    funnel = mechanism_forward_funnel(FakeExecution(), cycle)

    assert funnel["mechanism_count"] == 2
    assert funnel["forward_outcome_count"] == 18
    assert funnel["incremental_qualified_cohort_count"] == 1
    assert funnel["full_qualified_cohort_count"] == 1
    assert funnel["currently_qualified_mechanism_count"] == 2
    assert funnel["current_promoted_candidate_count"] == 3
    assert funnel["cycle_promoted_candidate_count"] == 3


class FakeStore:
    def __init__(self, events):
        self.events = events

    def latest_worker_heartbeat(self, worker_id):
        assert worker_id == "canonical-portfolio-operating-loop"
        return SimpleNamespace(detail={"portfolio_nav_usd": 251_000.0})

    def record_worker_heartbeat(self, **kwargs):
        self.events.append(("heartbeat", kwargs["worker_id"], kwargs["state"]))


class FakeOperating:
    def __init__(self, events, *, fail=False):
        self.events = events
        self.fail = fail

    def reconcile_latest_runtime_truth(self):
        self.events.append(("reconcile",))
        if self.fail:
            raise RuntimeError("reconciliation failed")
        return SimpleNamespace(snapshot_id="operating-1", observed_at=NOW)


class FakeBridge:
    def __init__(self, events):
        self.events = events
        self._latest_scan = lambda: "persisted-old-scan"

    async def publish_latest(self, *, total_capital_usd):
        self.events.append(("bridge", total_capital_usd, self._latest_scan()))
        return SimpleNamespace(
            candidates=[SimpleNamespace(candidate_id="candidate-1")],
            observed_at=NOW,
        )


class FakeProjection:
    def __init__(self, events):
        self.events = events

    def publish(self, **kwargs):
        self.events.append(("projection", kwargs["forward_target"], kwargs["settled_target"]))
        return {"observed_at": NOW.isoformat()}


def _settings():
    return SimpleNamespace(
        alpha_min_forward_samples=30,
        operating_certification_min_settled_trials=20,
        shadow_horizons_seconds=(1.0, 5.0, 30.0),
        shadow_cycle_interval_seconds=30.0,
        alpha_evidence_every_cycles=10,
        worker_heartbeat_stale_seconds=180.0,
    )


@pytest.mark.asyncio
async def test_permanent_control_plane_reconciles_before_bridge_and_projection():
    events = []
    bridge_snapshot = object()
    bridge = FakeBridge(events)

    result = await refresh_canonical_control_plane(
        store=FakeStore(events),
        operating_certification=FakeOperating(events),
        qualified_bridge=bridge,
        research_projection=FakeProjection(events),
        settings=_settings(),
        bridge_snapshot=bridge_snapshot,
    )

    assert result["control_plane_healthy"] is True
    assert result["operating_reconciliation_complete"] is True
    assert result["qualified_bridge_publication_complete"] is True
    assert result["qualified_bridge_candidate_count"] == 1
    assert result["research_projection_publication_complete"] is True
    assert [row[0] for row in events if row[0] != "heartbeat"] == [
        "reconcile",
        "bridge",
        "projection",
    ]
    bridge_event = next(row for row in events if row[0] == "bridge")
    assert bridge_event[1] == 251_000.0
    assert bridge_event[2] is bridge_snapshot
    assert bridge._latest_scan() == "persisted-old-scan"


@pytest.mark.asyncio
async def test_reconciliation_failure_fails_closed_before_bridge_publication():
    events = []

    result = await refresh_canonical_control_plane(
        store=FakeStore(events),
        operating_certification=FakeOperating(events, fail=True),
        qualified_bridge=FakeBridge(events),
        research_projection=FakeProjection(events),
        settings=_settings(),
        bridge_snapshot=object(),
    )

    assert result["control_plane_healthy"] is False
    assert result["operating_reconciliation_complete"] is False
    assert result["qualified_bridge_publication_complete"] is False
    assert result["research_projection_publication_complete"] is False
    assert result["control_plane_errors"] == {
        "operating_reconciliation": "RuntimeError"
    }
    assert [row[0] for row in events] == ["reconcile"]
