from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import inefficiency_engine.critical_evidence_recovery as recovery_module
from inefficiency_engine.critical_evidence_recovery import (
    ALPHA_L2_WORKER_ID,
    DEFAULT_ALPHA_FORWARD_RECOVERY_STALE_SECONDS,
    DEFAULT_CRITICAL_EVIDENCE_RECOVERY_STALE_SECONDS,
    DEFAULT_SOURCE_TRUTH_RETRY_COOLDOWN_SECONDS,
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


def _current_truth():
    return {
        "carry": {
            "source_state": "sufficient",
            "stale_source_ids": [],
        }
    }


def _stale_truth():
    return {
        "carry": {
            "source_state": "stale",
            "stale_source_ids": ["funding-primary"],
        },
        "options": {
            "source_state": "evidence_class_gap",
            "stale_source_ids": ["options-primary"],
        },
    }


def test_default_recovery_windows_match_production_freshness_contract():
    assert DEFAULT_CRITICAL_EVIDENCE_RECOVERY_STALE_SECONDS == 180.0
    assert DEFAULT_ALPHA_FORWARD_RECOVERY_STALE_SECONDS == 1200.0
    assert DEFAULT_SOURCE_TRUTH_RETRY_COOLDOWN_SECONDS == 60.0


def test_unobserved_critical_workers_force_bounded_recovery():
    status = critical_evidence_recovery_status(FakeStore(), now=NOW)

    assert status["source_refresh_required"] is True
    assert status["alpha_forward_required"] is True
    assert status["mechanism_forward_required"] is True
    assert status["any_required"] is True
    assert status["workers"]["source_refresh"]["reason"] == "unobserved"
    assert status["workers"]["alpha_l2_sampling"]["reason"] == "unobserved"
    assert status["workers"]["mechanism_forward"]["reason"] == "unobserved"
    assert status["dashboard_freshness_aligned"] is True
    assert status["source_truth_recovery_active"] is True
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


def test_fresh_l2_cannot_mask_stale_alpha_forward_completion(monkeypatch):
    monkeypatch.setattr(
        recovery_module,
        "_alpha_forward_status",
        lambda store, now, stale_after_seconds: {
            "worker_id": "shadow-research-auxiliary",
            "signal": "alpha_forward_evidence_cycle_id",
            "available": True,
            "recovery_required": True,
            "reason": "alpha_forward_marker_stale",
            "age_seconds": 6 * 24 * 60 * 60,
            "recovery_after_seconds": stale_after_seconds,
        },
    )
    store = FakeStore(
        {
            SOURCE_REFRESH_WORKER_ID: _heartbeat(age_seconds=30),
            ALPHA_L2_WORKER_ID: _heartbeat(age_seconds=5),
            MECHANISM_FORWARD_WORKER_ID: _heartbeat(age_seconds=30),
        }
    )

    status = critical_evidence_recovery_status(store, now=NOW)

    assert status["workers"]["alpha_l2_sampling"]["recovery_required"] is False
    assert status["workers"]["alpha_forward"]["reason"] == "alpha_forward_marker_stale"
    assert status["alpha_forward_required"] is True
    assert status["alpha_forward_completion_signal"].endswith("alpha_forward_evidence_cycle_id")
    assert status["qualification_thresholds_unchanged"] is True


def test_stale_mechanism_worker_does_not_force_disposable_alpha_recovery():
    store = FakeStore(
        {
            SOURCE_REFRESH_WORKER_ID: _heartbeat(age_seconds=60),
            ALPHA_L2_WORKER_ID: _heartbeat(age_seconds=60),
            MECHANISM_FORWARD_WORKER_ID: _heartbeat(age_seconds=181),
        }
    )

    status = critical_evidence_recovery_status(store, now=NOW)

    assert status["source_refresh_required"] is False
    assert status["alpha_forward_required"] is False
    assert status["mechanism_forward_required"] is True
    assert status["any_required"] is True


def test_fresh_degraded_heartbeat_suppresses_immediate_retry():
    store = FakeStore(
        {
            SOURCE_REFRESH_WORKER_ID: _heartbeat(
                age_seconds=30,
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


def test_stale_source_truth_forces_recovery_even_with_current_worker_heartbeat(monkeypatch):
    monkeypatch.setattr(recovery_module, "_read_source_truth", lambda store, now: _stale_truth())
    store = FakeStore(
        {
            SOURCE_REFRESH_WORKER_ID: _heartbeat(age_seconds=61, state="degraded"),
            ALPHA_L2_WORKER_ID: _heartbeat(age_seconds=30),
            MECHANISM_FORWARD_WORKER_ID: _heartbeat(age_seconds=30),
        }
    )

    status = critical_evidence_recovery_status(store, now=NOW)

    assert status["workers"]["source_refresh"]["recovery_required"] is False
    assert status["source_refresh_required"] is True
    assert status["source_truth"]["recovery_required"] is True
    assert status["source_truth"]["reason"] == "stale_truth_retry_due"
    assert status["source_truth"]["stale_lane_ids"] == ["carry", "options"]
    assert status["source_truth"]["stale_source_ids"] == ["funding-primary", "options-primary"]


def test_stale_source_truth_respects_short_retry_cooldown(monkeypatch):
    monkeypatch.setattr(recovery_module, "_read_source_truth", lambda store, now: _stale_truth())
    store = FakeStore(
        {
            SOURCE_REFRESH_WORKER_ID: _heartbeat(age_seconds=30, state="degraded"),
            ALPHA_L2_WORKER_ID: _heartbeat(age_seconds=30),
            MECHANISM_FORWARD_WORKER_ID: _heartbeat(age_seconds=30),
        }
    )

    status = critical_evidence_recovery_status(store, now=NOW)

    assert status["source_refresh_required"] is False
    assert status["source_truth"]["stale"] is True
    assert status["source_truth"]["recovery_required"] is False
    assert status["source_truth"]["reason"] == "stale_truth_retry_cooldown"


def test_current_source_truth_does_not_create_extra_recovery(monkeypatch):
    monkeypatch.setattr(recovery_module, "_read_source_truth", lambda store, now: _current_truth())
    store = FakeStore(
        {
            SOURCE_REFRESH_WORKER_ID: _heartbeat(age_seconds=120),
            ALPHA_L2_WORKER_ID: _heartbeat(age_seconds=120),
            MECHANISM_FORWARD_WORKER_ID: _heartbeat(age_seconds=120),
        }
    )

    status = critical_evidence_recovery_status(store, now=NOW)

    assert status["source_refresh_required"] is False
    assert status["source_truth"]["reason"] == "source_truth_current"


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
