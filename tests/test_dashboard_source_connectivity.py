from datetime import datetime, timedelta, timezone

from inefficiency_engine.dashboard_source_connectivity import read_source_connectivity
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.source_coverage import SourceCoverageLedger, SourceCoverageObservation
from inefficiency_engine.source_coverage_catalog import SOURCES


NOW = datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)


def _record(
    ledger: SourceCoverageLedger,
    *,
    lane_id: str,
    source_id: str,
    classes: list[str],
    observed_at: datetime = NOW,
    healthy: bool = True,
    error_type: str | None = None,
) -> None:
    ledger.record(
        SourceCoverageObservation(
            source_id=source_id,
            lane_id=lane_id,
            observed_at=observed_at,
            healthy=healthy,
            item_count=4 if healthy else 0,
            evidence_classes=classes,
            authoritative=True,
            commercial_use_permitted=True,
            point_in_time=True,
            economic_fields_complete=True,
            forward_testable_evidence=True,
            error_type=error_type,
        )
    )


def test_connectivity_reports_every_configured_source_without_provider_calls(monkeypatch, tmp_path):
    monkeypatch.delenv("CIE_TOKENOMIST_API_KEY", raising=False)
    monkeypatch.delenv("CIE_BYBIT_PUBLIC_ENABLED", raising=False)
    store = EvidenceStore(tmp_path / "source-connectivity.sqlite")
    ledger = SourceCoverageLedger(store)
    _record(
        ledger,
        lane_id="volatility",
        source_id="bybit-options",
        classes=["option_quotes", "option_greeks"],
    )
    _record(
        ledger,
        lane_id="microstructure",
        source_id="public-trade-flow",
        classes=["trade_flow"],
        observed_at=NOW - timedelta(days=3),
    )
    _record(
        ledger,
        lane_id="liquidation_distress",
        source_id="bybit-liquidations",
        classes=["liquidation_events", "liquidation_pressure"],
        healthy=False,
        error_type="ProviderUnavailable",
    )

    payload = read_source_connectivity(store, now=NOW)
    rows = {row["source_id"]: row for row in payload["sources"]}

    assert payload["available"] is True
    assert payload["summary"]["configured"] == len(SOURCES)
    assert len(rows) == len(SOURCES)
    assert rows["bybit-options"]["state"] == "healthy"
    assert rows["bybit-options"]["admitted"] is True
    assert rows["public-trade-flow"]["state"] == "stale"
    assert rows["public-trade-flow"]["admitted"] is False
    assert rows["bybit-liquidations"]["state"] == "failed"
    assert rows["bybit-liquidations"]["error_type"] == "ProviderUnavailable"
    assert rows["snapshot-governance"]["state"] == "unobserved"
    assert rows["tokenomist-unlocks"]["state"] == "credential_required"
    assert rows["tokenomist-unlocks"]["credential_env"] == "CIE_TOKENOMIST_API_KEY"
    assert rows["tokenomist-unlocks"]["credential_configured"] is False
    assert payload["live_execution_authority"] is False
    assert payload["allocation_authority"] is False


def test_connectivity_distinguishes_policy_disabled_and_endogenous_waiting(monkeypatch, tmp_path):
    monkeypatch.setenv("CIE_BYBIT_PUBLIC_ENABLED", "false")
    monkeypatch.delenv("CIE_TOKENOMIST_API_KEY", raising=False)
    store = EvidenceStore(tmp_path / "source-connectivity-policy.sqlite")
    SourceCoverageLedger(store)

    payload = read_source_connectivity(store, now=NOW)
    rows = {row["source_id"]: row for row in payload["sources"]}

    for source_id in (
        "bybit-market",
        "bybit-l2",
        "bybit-funding",
        "bybit-catalog",
        "bybit-options",
        "bybit-liquidations",
        "bybit-distress",
    ):
        assert rows[source_id]["state"] == "not_applicable"
        assert rows[source_id]["status_reason"] == "disabled_by_runtime_provider_policy"

    for source_id in (
        "internal-maker-shadow",
        "internal-opportunity-history",
        "internal-transfer-telemetry",
    ):
        assert rows[source_id]["state"] == "awaiting_endogenous"
        assert rows[source_id]["status_reason"] == "generated_only_after_governed_activity"

    assert payload["summary"]["not_applicable"] == 7
    assert payload["summary"]["awaiting_endogenous"] == 3
    assert payload["summary"]["connectivity_configured"] == len(SOURCES) - 7 - 3


def test_connectivity_never_exposes_credential_value(monkeypatch, tmp_path):
    secret = "not-for-output"
    monkeypatch.setenv("CIE_TOKENOMIST_API_KEY", secret)
    store = EvidenceStore(tmp_path / "source-connectivity-secret.sqlite")
    SourceCoverageLedger(store)

    payload = read_source_connectivity(store, now=NOW)
    tokenomist = next(row for row in payload["sources"] if row["source_id"] == "tokenomist-unlocks")

    assert tokenomist["credential_configured"] is True
    assert tokenomist["credential_env"] == "CIE_TOKENOMIST_API_KEY"
    assert secret not in repr(payload)
