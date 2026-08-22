from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from inefficiency_engine.critical_evidence_recovery import (
    ALPHA_L2_WORKER_ID,
    DEFAULT_CRITICAL_EVIDENCE_RECOVERY_STALE_SECONDS,
    MECHANISM_FORWARD_WORKER_ID,
    SOURCE_REFRESH_WORKER_ID,
    critical_evidence_recovery_status,
)


NOW = datetime(2026, 8, 22, 15, 0, tzinfo=timezone.utc)


class FakeStore:
    def __init__(self, rows=None):
        self.rows = dict(rows or {})

    def latest_worker_heartbeat(self, worker_id):
        return self.rows.get(worker_id)


def _heartbeat(*, age_seconds: float, state: str = "success", error_type=None):
    return SimpleNamespace(
        observed_at=NOW - timedelta(seconds=age_seconds),
        state=state,
        error_type=error_type,
    )


def test_default_recovery_window_matches_production_freshness_budget():
    assert DEFAULT_CRITICAL_EVIDENCE_RECOVERY_STALE_SECONDS == 180.0


def test_unobserved_critical_workers_force_bounded_recovery():
    status = critical_evidence_recovery_status(FakeStore(), now=NOW)

    assert status["source_refresh_required"] is True
    assert status["alpha_forward_required"] is True
    assert status["any_required"] is True
    assert status["workers"]["source_refresh"]["reason"] == "unobserved"
    assert status["workers"]["alpha_l2_sampling"]["reason"] == "unobserved"
    assert status["workers"]["mechanism_forward"]["reason"] == "unobserved"
    assert status["dashboard_freshness_aligned"] is True
    assert status["qualification_thresholds_unchanged"] is True
    assert status["allocation_authority"] is False


def test_stale_source_worker_forces_source_recovery_at_dashboard_sla():
    store = FakeStore(
        {
            SOURCE_REFRESH_WORKER_ID: _heartbeat(age_seconds=181),
            ALPHA_L2_WORKER_ID: _heartbeat(age_seconds=120),
            MECHANISM_FORWARD_WORKER_ID: _heartbeat(age_seconds=120),
        }
    )

    status = critical_evidence_recovery_status(store, now=NOW)

    assert status["source_refresh_required"] is True
    assert status["alpha_forward_required"] is False
    assert status["workers"]["source_refresh"]["reason"] == "grossly_stale"
    assert status["workers"]["source_refresh"]["recovery_after_seconds"] == 180.0


def test_fresh_degraded_heartbeat_suppresses_immediate_retry():
    store = FakeStore(
        {
            SOURCE_REFRESH_WORKER_ID: _heartbeat(
                age_seconds=60,
                state="degraded",
                error_type="ProviderUnavailable",
            ),
            ALPHA_L2_WORKER_ID: _heartbeat(
                age_seconds=60,
                state="degraded",
                error_type="AlphaL2SampleEmpty",
            ),
            MECHANISM_FORWARD_WORKER_ID: _heartbeat(
                age_seconds=60,
                state="degraded",
                error_type="NoForwardEvidence",
            ),
        }
    )

    status = critical_evidence_recovery_status(store, now=NOW)

    assert status["source_refresh_required"] is False
    assert status["alpha_forward_required"] is False
    assert status["any_required"] is False
    assert all(row["reason"] == "current_enough" for row in status["workers"].values())


def test_degraded_heartbeat_recovers_once_it_is_stale_for_dashboard():
    store = FakeStore(
        {
            SOURCE_REFRESH_WORKER_ID: _heartbeat(
                age_seconds=181,
                state="degraded",
                error_type="ProviderUnavailable",
            ),
            ALPHA_L2_WORKER_ID: _heartbeat(age_seconds=60),
            MECHANISM_FORWARD_WORKER_ID: _heartbeat(age_seconds=60),
        }
    )

    status = critical_evidence_recovery_status(store, now=NOW)

    assert status["source_refresh_required"] is True
    assert status["workers"]["source_refresh"]["reason"] == "grossly_stale"


def test_custom_recovery_window_remains_supported():
    store = FakeStore(
        {
            SOURCE_REFRESH_WORKER_ID: _heartbeat(age_seconds=240),
            ALPHA_L2_WORKER_ID: _heartbeat(age_seconds=240),
            MECHANISM_FORWARD_WORKER_ID: _heartbeat(age_seconds=240),
        }
    )

    status = critical_evidence_recovery_status(store, now=NOW, stale_after_seconds=300)

    assert status["any_required"] is False
    assert status["stale_after_seconds"] == 300.0


def test_recovery_is_wired_ahead_of_core_shadow_without_changing_normal_cadence():
    source = Path("src/inefficiency_engine/disposable_research_worker.py").read_text()

    recovery = source.index("critical_evidence_recovery_status(store)")
    source_condition = source.index("if source_bootstrap_scheduled or source_recovery_required:")
    alpha_condition = source.index("if alpha_scheduled or alpha_recovery_required:")
    core = source.index('_record_progress("core_shadow")')

    assert recovery < source_condition < alpha_condition < core
    assert "source_bootstrap_scheduled = sequence == 1 or sequence % alpha_every == 1" in source
    assert "alpha_scheduled = _due(sequence, alpha_every, 0.75)" in source
    assert '"critical_evidence_recovery_guard": True' in source
