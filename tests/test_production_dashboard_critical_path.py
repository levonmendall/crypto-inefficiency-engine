from __future__ import annotations

import pytest
from fastapi import HTTPException

from inefficiency_engine import read_api_active_volume_deploy as deploy
from inefficiency_engine import production_dashboard_fastpath as fastpath
from inefficiency_engine.canonical_paper_portfolio import CanonicalPaperPortfolioLedger
from inefficiency_engine.dashboard_projection import DashboardProjectionLedger
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.portfolio_integrity import PortfolioIntegrityLedger
from inefficiency_engine.read_evidence import ReadOnlyEvidenceStore


def _writer_with_genesis(path):
    store = EvidenceStore(path)
    ledger = CanonicalPaperPortfolioLedger(store)
    ledger.ensure_genesis()
    snapshot = ledger.current_state()
    ledger.record_snapshot(snapshot)
    PortfolioIntegrityLedger(store).ensure_initial(snapshot)
    return store


def _forbidden(message: str):
    def fail(*_args, **_kwargs):
        raise AssertionError(message)

    return fail


def test_exact_render_dashboard_reconstructs_genesis_without_heavy_request_work(
    monkeypatch,
    tmp_path,
):
    database = tmp_path / "production-fastpath.db"
    _writer_with_genesis(database)
    readonly = ReadOnlyEvidenceStore(database)

    monkeypatch.setattr(deploy, "_store", lambda: readonly)
    monkeypatch.setattr(
        deploy,
        "_lane_readiness",
        _forbidden("dashboard request must not construct 13-lane readiness"),
    )
    monkeypatch.setattr(
        deploy._base_deploy,
        "dashboard_snapshot",
        _forbidden("production route must not invoke the old multi-read dashboard path"),
    )

    payload = deploy.dashboard_snapshot()

    assert payload["portfolio"]["available"] is True
    assert payload["portfolio"]["nav_usd"] == 250_000.0
    assert payload["performance"]["current_nav_usd"] == 250_000.0
    assert payload["performance"]["cash_usd"] == 250_000.0
    assert payload["runtime"]["valuation_status"] == "cash_only"
    assert payload["positions"]["positions"] == []
    assert payload["trades"]["trades"] == []
    assert payload["critical_path_persisted_only"] is True
    assert payload["request_time_research_computation"] is False
    assert payload["dashboard_critical_path_persisted_only"] is True
    assert payload["lane_executability"]["request_time_research_computation"] is False
    assert payload["lane_executability"]["lane_count"] == 13
    assert payload["paper_only"] is True
    assert payload["live_execution_authority"] is False


def test_exact_render_dashboard_prefers_compact_projection_without_durable_rebuild(
    monkeypatch,
    tmp_path,
):
    database = tmp_path / "production-compact.db"
    writer = _writer_with_genesis(database)
    DashboardProjectionLedger(writer).publish()
    readonly = ReadOnlyEvidenceStore(database)

    monkeypatch.setattr(deploy, "_store", lambda: readonly)
    monkeypatch.setattr(
        deploy,
        "_lane_readiness",
        _forbidden("dashboard request must not construct 13-lane readiness"),
    )
    monkeypatch.setattr(
        deploy._base_deploy,
        "dashboard_snapshot",
        _forbidden("production route must not invoke the old inner dashboard path"),
    )

    payload = deploy.dashboard_snapshot()

    assert payload["portfolio"]["available"] is True
    assert payload["performance"]["current_nav_usd"] == 250_000.0
    assert payload["performance"]["cash_usd"] == 250_000.0
    assert payload["critical_path_persisted_only"] is True
    assert payload["request_time_research_computation"] is False
    assert payload.get("presentation_fallback") is not True


def test_compact_projection_read_gets_one_bounded_retry_before_reconstruction(monkeypatch):
    calls: list[tuple[int, int]] = []

    def compact(_store, *, statement_timeout_ms=fastpath.DEFAULT_COMPACT_READ_TIMEOUT_MS, lock_timeout_ms=500):
        calls.append((statement_timeout_ms, lock_timeout_ms))
        if len(calls) == 1:
            return None, None, "OperationalError"
        return {"portfolio": {"available": True}}, None, None

    monkeypatch.setattr(fastpath, "_read_compact_projections", compact)
    monkeypatch.setattr(
        fastpath,
        "build_dashboard_projection",
        _forbidden("successful compact retry must avoid durable reconstruction"),
    )

    payload = fastpath.build_production_dashboard_snapshot(object())

    assert calls == [
        (fastpath.DEFAULT_COMPACT_READ_TIMEOUT_MS, 500),
        (fastpath.RETRY_COMPACT_READ_TIMEOUT_MS, 1000),
    ]
    assert payload["compact_projection_read_retry_used"] is True
    assert payload["compact_projection_read_initial_error_type"] == "OperationalError"
    assert payload["compact_projection_read_retry_error_type"] is None
    assert payload["presentation_fallback"] if "presentation_fallback" in payload else True


def test_snapshot_503_identifies_fastpath_stage_and_error_type(monkeypatch):
    monkeypatch.setattr(deploy, "_store", lambda: object())

    def fail(*_args, **_kwargs):
        try:
            raise TimeoutError("db timeout")
        except TimeoutError as cause:
            raise RuntimeError("bounded dashboard read failed") from cause

    monkeypatch.setattr(deploy, "build_production_dashboard_snapshot", fail)

    with pytest.raises(HTTPException) as caught:
        deploy.dashboard_snapshot()

    assert caught.value.status_code == 503
    assert caught.value.detail["stage"] == "production_dashboard_fastpath"
    assert caught.value.detail["error_type"] == "RuntimeError"
    assert caught.value.detail["cause_type"] == "TimeoutError"


def test_source_connectivity_endpoint_is_independent_of_main_snapshot(monkeypatch):
    monkeypatch.setattr(deploy, "_store", lambda: object())
    monkeypatch.setattr(
        deploy,
        "read_source_connectivity",
        lambda _store: {
            "available": True,
            "summary": {"configured": 2, "healthy": 1, "stale": 1},
            "sources": [{"source_id": "a", "state": "healthy"}],
        },
    )

    payload = deploy.source_connectivity()

    assert payload["available"] is True
    assert payload["summary"]["stale"] == 1
    assert payload["diagnostic_only"] is True
    assert payload["live_execution_authority"] is False or "live_execution_authority" not in payload
