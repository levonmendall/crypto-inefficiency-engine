from __future__ import annotations

from inefficiency_engine import read_api_active_volume_deploy as deploy
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
